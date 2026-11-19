package memory

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// env wires a real Memory store over real PostgreSQL + pgvector, with a real
// Artifacts store for evidence publication checks. It fails (never skips) when
// the environment is missing so the suite cannot pass vacuously.
type env struct {
	pool     *pgxpool.Pool
	arts     *artifacts.Store
	mem      *Store
	tenant   string
	members  map[string][]string // subject -> live workspaces
	ceilings map[string]labels.Set
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL must be set for memory integration tests")
	}
	raw := os.Getenv("AHP_S3_ENDPOINT")
	if raw == "" {
		t.Fatal("AHP_S3_ENDPOINT must be set for memory integration tests")
	}
	if os.Getenv("AHP_MINIO_ROOT_USER") == "" || os.Getenv("AHP_MINIO_ROOT_PASSWORD") == "" {
		t.Fatal("MinIO credentials must be configured for memory integration tests")
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
	bucket := "ahp-go-mem-" + hex.EncodeToString(rnd[:])
	tenant := "t-" + hex.EncodeToString(rnd[:])

	allow := func(context.Context, contracts.Scope, string, string) error { return nil }
	art, err := artifacts.New(pool, client, bucket, allow)
	if err != nil {
		t.Fatalf("artifacts New: %v", err)
	}
	if err := art.Migrate(ctx); err != nil {
		t.Fatalf("artifacts Migrate: %v", err)
	}

	e := &env{pool: pool, arts: art, tenant: tenant, members: map[string][]string{}, ceilings: map[string]labels.Set{}}
	mem, err := New(pool, art, allow, embedText,
		WithWorkspaces(func(_ context.Context, s contracts.Scope) ([]string, error) {
			return append([]string(nil), e.members[s.Subject]...), nil
		}),
		WithLabels(func(_ context.Context, s contracts.Scope) (labels.Set, error) {
			return e.ceilings[s.Subject], nil
		}),
	)
	if err != nil {
		t.Fatalf("memory New: %v", err)
	}
	if err := mem.Migrate(ctx); err != nil {
		t.Fatalf("memory Migrate: %v", err)
	}
	e.mem = mem
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
		_, _ = pool.Exec(c, `DROP SCHEMA IF EXISTS ahp_memory CASCADE`)
		_, _ = pool.Exec(c, `DROP SCHEMA IF EXISTS ahp_artifacts CASCADE`)
	})
	return e
}

func (e *env) scope(subject string) contracts.Scope {
	return contracts.Scope{Tenant: e.tenant, Deployment: "d-1", Subject: subject}
}

// setCeiling sets a subject's live label ceiling (the labels the writing task
// carries), which bounds every record that subject writes.
func (e *env) setCeiling(subject string, set labels.Set) { e.ceilings[subject] = set }

// addWorkspace grants a subject live membership in a workspace.
func (e *env) addWorkspace(subject, ws string) {
	for _, w := range e.members[subject] {
		if w == ws {
			return
		}
	}
	e.members[subject] = append(e.members[subject], ws)
}

// embedText is a deterministic content hash projected to a mean-zero vector in
// [-1,1]^64: identical text yields cosine 1, unrelated text yields ~0, so both
// the exact-digest dedup and the pgvector signal are exercised deterministically.
func embedText(_ context.Context, text string) ([]float32, error) {
	sum := sha256.Sum256([]byte(text))
	v := make([]float32, EmbeddingDim)
	for i := range v {
		v[i] = float32(sum[i%len(sum)])/255.0*2.0 - 1.0
	}
	return v, nil
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

var _ = labels.TaintSecret
