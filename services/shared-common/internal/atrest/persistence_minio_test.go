//go:build ahp_data

package atrest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Object-storage byte-check against the real MinIO in the lane. A dedicated,
// uniquely named bucket is created and removed; no neighbor bucket is touched and
// nothing prints plaintext or keys.

const minioSentinel = "S3-PLAINTEXT-MARKER-4e19"

func minioClient(t *testing.T) (*minio.Client, string) {
	t.Helper()
	endpoint := os.Getenv("AHP_S3_ENDPOINT")
	access := os.Getenv("AHP_MINIO_ROOT_USER")
	secret := os.Getenv("AHP_MINIO_ROOT_PASSWORD")
	if endpoint == "" || access == "" || secret == "" {
		t.Fatal("AHP_S3_ENDPOINT/AHP_MINIO_ROOT_USER/AHP_MINIO_ROOT_PASSWORD must be set")
	}
	host, secure := parseS3(endpoint)
	if host == "" {
		t.Fatal("AHP_S3_ENDPOINT must be a URL")
	}
	client, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Region: os.Getenv("AHP_S3_REGION"),
		Secure: secure,
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	var rnd [4]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatal(err)
	}
	bucket := "ahp-atrest-lab-" + hex.EncodeToString(rnd[:])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: os.Getenv("AHP_S3_REGION")}); err != nil {
		t.Fatalf("make bucket: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		for obj := range client.ListObjects(cctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
			_ = client.RemoveObject(cctx, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
		}
		_ = client.RemoveBucket(cctx, bucket)
	})
	return client, bucket
}

func parseS3(raw string) (host string, secure bool) {
	for _, pfx := range []string{"https://", "http://"} {
		if len(raw) >= len(pfx) && raw[:len(pfx)] == pfx {
			rest := raw[len(pfx):]
			for i := 0; i < len(rest); i++ {
				if rest[i] == '/' {
					rest = rest[:i]
					break
				}
			}
			return rest, pfx == "https://"
		}
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] == '/' {
			return raw[:i], false
		}
	}
	return raw, false
}

func TestMinIOSealedObjectByteCheck(t *testing.T) {
	client, bucket := minioClient(t)
	auth := restartAuthority(t, testTenant)
	codec, _ := NewCodec(auth)
	ctx := context.Background()

	plaintext := "artifact-bytes " + minioSentinel
	env, err := codec.Seal(ctx, testTenant, "artifact.bytes", []byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	key := "sealed/blob"
	if _, err := client.PutObject(ctx, bucket, key, bytes.NewReader(env), int64(len(env)), minio.PutObjectOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Read the raw object back — this is exactly what a disk/backup thief sees.
	obj, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	stored, err := io.ReadAll(obj)
	obj.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if bytes.Contains(stored, []byte(minioSentinel)) {
		t.Fatal("persisted object bytes contain the plaintext marker")
	}
	if bytes.Equal(stored, []byte(plaintext)) {
		t.Fatal("persisted object bytes equal plaintext")
	}
	if got, err := codec.Open(ctx, testTenant, "artifact.bytes", stored); err != nil || string(got) != plaintext {
		t.Fatalf("open stored object: got=%q err=%v", got, err)
	}
}

// Overwriting a single byte of the stored object must make it unreadable,
// modelling a tampered or corrupted blob at rest.
func TestMinIOTamperedObjectRejected(t *testing.T) {
	client, bucket := minioClient(t)
	auth := restartAuthority(t, testTenant)
	codec, _ := NewCodec(auth)
	ctx := context.Background()

	env, err := codec.Seal(ctx, testTenant, "artifact.bytes", []byte("artifact-bytes "+minioSentinel))
	if err != nil {
		t.Fatal(err)
	}
	key := "sealed/blob"
	if _, err := client.PutObject(ctx, bucket, key, bytes.NewReader(env), int64(len(env)), minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	tampered := append([]byte(nil), env...)
	tampered[len(tampered)-1] ^= 0x01
	if _, err := client.PutObject(ctx, bucket, key, bytes.NewReader(tampered), int64(len(tampered)), minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	obj, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stored, _ := io.ReadAll(obj)
	obj.Close()
	if _, err := codec.Open(ctx, testTenant, "artifact.bytes", stored); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("want ErrAuthFailed for tampered object, got %v", err)
	}
}
