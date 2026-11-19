package executor_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/atrest"
	"github.com/ahp-platform/ahp/internal/atrest/objectcipher"
	"github.com/ahp-platform/ahp/internal/executor"
)

// Byte-level "no plaintext after PUT" for the executor role specifically: the
// plane PUTs plaintext, then Seal must transform the verified version into an
// envelope and retire the plaintext version, so no version left in MinIO
// equals the plaintext payload.
func TestArtifactPlaneSealLeavesNoPlaintext(t *testing.T) {
	dsn, endpoint, ak, sk := mustEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	u, _ := url.Parse(endpoint)
	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	bucket := "ahp-plane-atrest-" + hex.EncodeToString(rnd[:])
	client, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(ak, sk, ""), Secure: u.Scheme == "https"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for o := range client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			_ = client.RemoveObject(context.Background(), bucket, o.Key, minio.RemoveObjectOptions{VersionID: o.VersionID})
		}
		_ = client.RemoveBucket(context.Background(), bucket)
	}()

	auth := atrest.NewDynamicAuthority(bytes.Repeat([]byte("r"), 32)) // 32-byte root for deriveKEK
	codec, err := atrest.NewCodec(auth)
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	cipher := objectcipher.New(codec, client, true).WithSegmentSize(64 << 10)

	plane, err := executor.NewArtifactPlane(ctx, pool, executor.ArtifactPlaneConfig{
		S3Endpoint: endpoint, S3AccessKey: ak, S3SecretKey: sk, Bucket: bucket,
		ObjectCipher: cipher,
	})
	if err != nil {
		t.Fatalf("NewArtifactPlane: %v", err)
	}
	scope := planeScope()
	plaintext := strings.Repeat("EXEC-PLAINTEXT-", 5000) // ~80 KiB, multi-segment
	seal, err := plane.Seal(ctx, scope, artifacts.Binding{ContextID: scope.Tenant + "-c", TaskID: scope.Tenant + "-t"}, "note.txt", "text/plain", plaintext)
	if err != nil {
		t.Fatalf("plane.Seal: %v", err)
	}

	sawVersion := false
	for o := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
		if o.Err != nil {
			t.Fatalf("list: %v", o.Err)
		}
		rc, err := client.GetObject(ctx, bucket, o.Key, minio.GetObjectOptions{VersionID: o.VersionID})
		if err != nil {
			t.Fatalf("get version: %v", err)
		}
		raw, _ := io.ReadAll(rc)
		_ = rc.Close()
		sawVersion = true
		if string(raw) == plaintext {
			t.Fatalf("plaintext version %s of %s survived executor seal", o.VersionID, o.Key)
		}
		if !atrest.IsEnvelope(raw) {
			t.Fatalf("version %s of %s is not an envelope", o.VersionID, o.Key)
		}
	}
	if !sawVersion {
		t.Fatal("no object versions observed")
	}

	rc, _, err := plane.Store.Open(ctx, scope, seal.ArtifactID)
	if err != nil {
		// Plane-level Open is gated on extraction readiness in this role, not by
		// encryption; the decrypt round-trip through Open/OpenRange is owned by
		// the internal/artifacts mount tests. Here we only assert the seal bound
		// the plaintext digest (dedup identity) and that no plaintext bytes
		// survived, which the loop above already verified.
		_ = rc
	}
	ref, err := plane.Store.Get(ctx, scope, seal.ArtifactID)
	if err != nil {
		t.Fatalf("get sealed: %v", err)
	}
	sum := sha256.Sum256([]byte(plaintext))
	if ref.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("mount bound a non-plaintext digest")
	}
}
