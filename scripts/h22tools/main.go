// Command h22tools is helper22's out-of-band evidence tool (NOT product code).
// It reads Postgres and MinIO directly, bypassing the deployments under test.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"encoding/xml"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/signer"
)

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "h22tools:", err)
		os.Exit(1)
	}
}

func nowMS() string { return time.Now().UTC().Format("2006-01-02T15:04:05.000Z") }

func minioClient() *minio.Client {
	ep := os.Getenv("AHP_S3_ENDPOINT")
	if ep == "" {
		fatal(fmt.Errorf("AHP_S3_ENDPOINT unset"))
	}
	secure := strings.HasPrefix(ep, "https://")
	if i := strings.Index(ep, "://"); i >= 0 {
		ep = ep[i+3:]
	}
	c, err := minio.New(ep, &minio.Options{
		Creds:  credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""),
		Secure: secure,
		Region: os.Getenv("AHP_S3_REGION"),
	})
	fatal(err)
	return c
}

func cmdMinioLS(ctx context.Context) {
	c := minioClient()
	bucket := os.Getenv("AHP_EXEC_ARTIFACT_BUCKET")
	n, bytes := 0, int64(0)
	for obj := range c.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true, WithMetadata: true}) {
		if obj.Err != nil {
			fatal(obj.Err)
		}
		n++
		bytes += obj.Size
		fmt.Printf("OBJ\t%s\tsize=%d\tetag=%s\tmod=%s\n",
			obj.Key, obj.Size, obj.ETag, obj.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"))
	}
	fmt.Printf("TOTAL\tobjects_and_versions=%d\tbytes=%d\tt=%s\n", n, bytes, nowMS())
}

func cmdMinioSha(args []string) {
	c := minioClient()
	bucket := os.Getenv("AHP_EXEC_ARTIFACT_BUCKET")
	opts := minio.GetObjectOptions{}
	if len(args) > 1 && args[1] != "" {
		opts.VersionID = args[1]
	}
	o, err := c.GetObject(ctx0(), bucket, args[0], opts)
	fatal(err)
	defer o.Close()
	h := sha256.New()
	n, err := io.Copy(h, o)
	fatal(err)
	fmt.Printf("SHA\t%s\tsha256=%s\tsize=%d\tt=%s\n", args[0], hex.EncodeToString(h.Sum(nil)), n, nowMS())
}

func cmdMinioMk() {
	c := minioClient()
	bucket := os.Getenv("AHP_EXEC_ARTIFACT_BUCKET")
	ok, err := c.BucketExists(ctx0(), bucket)
	fatal(err)
	if !ok {
		fatal(c.MakeBucket(ctx0(), bucket, minio.MakeBucketOptions{Region: os.Getenv("AHP_S3_REGION")}))
		fmt.Println("BUCKET\tcreated", bucket)
	} else {
		fmt.Println("BUCKET\texists", bucket)
	}
	fatal(c.EnableVersioning(ctx0(), bucket))
	fmt.Println("VERSIONING\tenabled", bucket)
}

// cmdVersions lists ALL object versions out-of-band via the raw S3 ?versions
// API (minio-go v7.4.1 has no wrapper), signed with the repo's own SigV4 signer.
func cmdVersions() {
	ep := os.Getenv("AHP_S3_ENDPOINT")
	bucket := os.Getenv("AHP_EXEC_ARTIFACT_BUCKET")
	u := ep + "/" + bucket + "?versions"
	req, err := http.NewRequest("GET", u, nil)
	fatal(err)
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	req.Header.Set("X-Amz-Date", time.Now().UTC().Format("20060102T150405Z"))
	signed := signer.SignV4(*req, os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), "", os.Getenv("AHP_S3_REGION"))
	resp, err := http.DefaultClient.Do(signed)
	fatal(err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	fatal(err)
	if resp.StatusCode != 200 {
		fatal(fmt.Errorf("versions http %d: %s", resp.StatusCode, string(body)[:400]))
	}
	var out struct {
		Version []struct {
			Key          string `xml:"Key"`
			VersionID    string `xml:"VersionId"`
			IsLatest     bool   `xml:"IsLatest"`
			LastModified string `xml:"LastModified"`
			ETag         string `xml:"ETag"`
			Size         int64  `xml:"Size"`
		} `xml:"Version"`
	}
	fatal(xml.Unmarshal(body, &out))
	n := 0
	for _, v := range out.Version {
		n++
		fmt.Printf("VER\t%s\tvid=%s\tlatest=%v\tsize=%d\tetag=%s\tmod=%s\n",
			v.Key, v.VersionID, v.IsLatest, v.Size, strings.Trim(v.ETag, "\""), v.LastModified)
	}
	fmt.Printf("VERSIONS_TOTAL\t%d\tt=%s\n", n, nowMS())
}

func ctx0() context.Context {
	c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	_ = cancel
	return c
}

func cmdPG(args []string) {
	dsn := args[0]
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	fatal(err)
	defer conn.Close(ctx)
	for _, stmt := range args[1:] {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		rows, err := conn.Query(ctx, stmt)
		fatal(err)
		fmt.Printf("-- SQL\t%s\n", strings.ReplaceAll(stmt, "\n", " "))
		cds := rows.FieldDescriptions()
		hdr := make([]string, len(cds))
		for i, cd := range cds {
			hdr[i] = string(cd.Name)
		}
		fmt.Println("COL\t" + strings.Join(hdr, "\t"))
		n := 0
		for rows.Next() {
			vals, err := rows.Values()
			fatal(err)
			out := make([]string, len(vals))
			for i, v := range vals {
				switch t := v.(type) {
				case time.Time:
					out[i] = t.UTC().Format("2006-01-02T15:04:05.000Z")
				case []byte:
					out[i] = fmt.Sprintf("%q", string(t))
				default:
					out[i] = fmt.Sprint(t)
				}
			}
			fmt.Println("ROW\t" + strings.Join(out, "\t"))
			n++
		}
		fatal(rows.Err())
		fmt.Printf("-- ROWS\t%d\n", n)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: h22tools pg <dsn> <sql>... | minio-ls | minio-sha <key> [vid] | minio-mk")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "pg":
		cmdPG(os.Args[2:])
	case "minio-ls":
		cmdMinioLS(context.Background())
	case "minio-sha":
		cmdMinioSha(os.Args[2:])
	case "minio-versions":
		cmdVersions()
	case "minio-mk":
		cmdMinioMk()
	default:
		fatal(fmt.Errorf("unknown subcommand %s", os.Args[1]))
	}
}
