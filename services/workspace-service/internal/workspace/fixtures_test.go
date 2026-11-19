package workspace

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// env wires a real artifact store + workspace store over real PostgreSQL and
// MinIO, with the membership hook closed between them. It fails (not skips)
// when the environment is missing so the suite cannot pass vacuously.
type env struct {
	pool   *pgxpool.Pool
	arts   *artifacts.Store
	ws     *Store
	tenant string
	minio  *minio.Client
	bucket string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL must be set for workspace integration tests")
	}
	raw := os.Getenv("AHP_S3_ENDPOINT")
	if raw == "" {
		t.Fatal("AHP_S3_ENDPOINT must be set for workspace integration tests")
	}
	if os.Getenv("AHP_MINIO_ROOT_USER") == "" || os.Getenv("AHP_MINIO_ROOT_PASSWORD") == "" {
		t.Fatal("MinIO credentials must be configured for workspace integration tests")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""),
		Secure: u.Scheme == "https",
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatal(err)
	}
	bucket := "ahp-go-ws-" + hex.EncodeToString(rnd[:])
	tenant := "t-" + hex.EncodeToString(rnd[:])

	allow := func(context.Context, contracts.Scope, string, string) error { return nil }
	var wsRef *Store
	art, err := artifacts.New(pool, client, bucket, allow,
		artifacts.WithWorkspaceAccess(func(ctx context.Context, s contracts.Scope, w string) error {
			return wsRef.IsMember(ctx, s, w)
		}))
	if err != nil {
		t.Fatalf("artifacts New: %v", err)
	}
	if err := art.Migrate(ctx); err != nil {
		t.Fatalf("artifacts Migrate: %v", err)
	}
	ws, err := New(pool, art, allow)
	if err != nil {
		t.Fatalf("workspace New: %v", err)
	}
	wsRef = ws
	if err := ws.Migrate(ctx); err != nil {
		t.Fatalf("workspace Migrate: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for obj := range client.ListObjects(c, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if obj.Err != nil {
				break
			}
			_ = client.RemoveObject(c, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
		}
		_ = client.RemoveBucket(c, bucket)
		_, _ = pool.Exec(c, `DROP SCHEMA IF EXISTS ahp_workspace CASCADE`)
	})
	return &env{pool: pool, arts: art, ws: ws, tenant: tenant, minio: client, bucket: bucket}
}

func (e *env) scope(subject string) contracts.Scope {
	return contracts.Scope{Tenant: e.tenant, Deployment: "d-1", Subject: subject}
}

// put uploads bytes to a presigned URL.
func put(t *testing.T, signed, body string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, signed, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT failed: %T", err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("PUT status %d", resp.StatusCode)
	}
}

func shaHex(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

// uploadSeal uploads+seals an artifact and returns a File row for path.
func (e *env) uploadSeal(t *testing.T, subject, key, body string, set labels.Set) File {
	t.Helper()
	ctx := context.Background()
	sc := e.scope(subject)
	up, err := e.arts.BeginUpload(ctx, sc, key, artifacts.UploadRequest{
		Name: key, MIME: "application/octet-stream", SHA256: shaHex(body), Size: int64(len(body)), Labels: set,
	})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	put(t, up.PresignedURL, body)
	ref, err := e.arts.Seal(ctx, sc, up.ID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return File{Path: strings.TrimPrefix(key, "/"), ArtifactID: ref.ID, Mode: 0o644, Digest: ref.SHA256}
}

// share publishes each artifact to the workspace under its owner's rights —
// the only way a subject-private artifact can ever enter a workspace. Tests use
// it when they intend to exercise overlap/CAS/checks gates, not the rights gate.
func (e *env) share(t *testing.T, subject, ws string, files ...File) {
	t.Helper()
	for _, f := range files {
		if err := e.arts.PublishToWorkspace(context.Background(), e.scope(subject), f.ArtifactID, ws); err != nil {
			t.Fatalf("PublishToWorkspace(%s): %v", f.ArtifactID, err)
		}
	}
}

func mustErr(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got nil", code)
	}
	if got := contracts.Code(err); string(got) != code {
		t.Fatalf("error code = %s, want %s (err=%v)", got, code, err)
	}
}

func mustOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

var _ = bytes.MinRead
