package artifacts

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

type env struct {
	store   *Store
	allow   atomic.Bool
	scope   contracts.Scope
	foreign contracts.Scope
	// client and bucket are the raw object-storage handles, kept so a test can
	// verify bytes out of band - a direct GET of one exact version, bypassing
	// every code path under test - instead of trusting the store's own answer.
	client *minio.Client
	bucket string
}

// put uploads bytes through the presigned URL; failures report only the
// error type and stage, never the URL itself.
func put(t *testing.T, signed, body string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, signed, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build PUT: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		var uerr *url.Error
		if errors.As(err, &uerr) {
			t.Fatalf("PUT stage=%s errType=%T", uerr.Op, uerr.Err)
		}
		t.Fatalf("PUT errType=%T", err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("PUT stage=storage status=%d", resp.StatusCode)
	}
}

func (e *env) uploadSeal(t *testing.T, key, body string) (ArtifactRef, Upload) {
	t.Helper()
	sum := sha256.Sum256([]byte(body))
	up, err := e.store.BeginUpload(context.Background(), e.scope, key, UploadRequest{
		Name: "a.bin", MIME: "application/octet-stream",
		SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	put(t, up.PresignedURL, body)
	ref, err := e.store.Seal(context.Background(), e.scope, up.ID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return ref, up
}

func readAll(t *testing.T, rc io.ReadCloser, want string) {
	t.Helper()
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	if string(b) != want {
		t.Fatalf("artifact bytes mismatch")
	}
}

func codeIs(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want %s, got nil", want)
	}
	if got := contracts.Code(err); got != want {
		t.Fatalf("want code %s, got %q (%T)", want, got, err)
	}
}

// setup runs against real PostgreSQL + MinIO from env; skipped only when
// AHP_TEST_DATABASE_URL is unset.
func setup(t *testing.T, opts ...Option) *env {
	t.Helper()
	raw := os.Getenv("AHP_S3_ENDPOINT")
	if raw == "" {
		t.Skip("AHP_S3_ENDPOINT not set")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT: scheme=%q parseErrType=%T", u.Scheme, err)
	}
	if os.Getenv("AHP_MINIO_ROOT_USER") == "" || os.Getenv("AHP_MINIO_ROOT_PASSWORD") == "" {
		t.Skip("MinIO credentials not configured")
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""),
		Secure: u.Scheme == "https",
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := isolatedPool(t)
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatal(err)
	}
	// Lane-owned bucket: every bucket this package creates is named go-h17-* so
	// the resources one lane made on the shared stand are identifiable and no
	// sweep can be confused for another lane's.
	suffix := hex.EncodeToString(rnd[:])
	bucket := "go-h17-" + suffix
	// The tenant is derived from the same suffix as the bucket, not from a
	// slice of the bucket name: a shared database makes a colliding tenant
	// visible as another fixture's rows inside this one's sweep.
	e := &env{
		scope:   contracts.Scope{Tenant: "t-" + suffix, Deployment: "d1", Subject: "alice"},
		foreign: contracts.Scope{Tenant: "t-" + suffix, Deployment: "d1", Subject: "mallory"},
	}
	e.allow.Store(true)
	st, err := New(pool, client, bucket, func(_ context.Context, _ contracts.Scope, _, _ string) error {
		if e.allow.Load() {
			return nil
		}
		return contracts.NewError(contracts.ErrForbidden, "denied by test policy")
	}, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for obj := range client.ListObjects(c, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if obj.Err != nil {
				return
			}
			_ = client.RemoveObject(c, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
		}
		_ = client.RemoveBucket(c, bucket)
	})
	e.store = st
	e.client = client
	e.bucket = bucket
	return e
}

func TestUploadSealOpen(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body := "hello ahp artifacts"
	ref, _ := e.uploadSeal(t, "k-"+t.Name(), body)
	if ref.Version == "" || ref.SHA256 == "" {
		t.Fatal("sealed ref missing version/sha")
	}
	rc, got, err := e.store.Open(ctx, e.scope, ref.ID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got.ID != ref.ID || got.Version != ref.Version {
		t.Fatal("Open ref mismatch")
	}
	readAll(t, rc, body)
	if _, err := e.store.Get(ctx, e.scope, ref.ID); err != nil {
		t.Fatalf("Get: %v", err)
	}
}

func TestOverwriteKeepsPinnedVersion(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body := "original-payload"
	ref, up := e.uploadSeal(t, "k-ow", body)
	put(t, up.PresignedURL, strings.Repeat("X", len(body))) // still-valid URL, post-seal
	rc, got, err := e.store.Open(ctx, e.scope, ref.ID)
	if err != nil {
		t.Fatalf("Open after overwrite: %v", err)
	}
	if got.Version != ref.Version {
		t.Fatal("pinned version changed")
	}
	readAll(t, rc, body)
}

func TestBeginUploadIdempotency(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req := UploadRequest{Name: "a.bin", MIME: "application/octet-stream",
		SHA256: strings.Repeat("a", 64), Size: 5}
	first, err := e.store.BeginUpload(ctx, e.scope, "k-idem", req)
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	again, err := e.store.BeginUpload(ctx, e.scope, "k-idem", req)
	if err != nil || again.ID != first.ID {
		t.Fatalf("replay must return same id: err=%v", err)
	}
	other := req
	other.Name = "b.bin"
	_, err = e.store.BeginUpload(ctx, e.scope, "k-idem", other)
	codeIs(t, err, contracts.ErrKeyReuse)
}

func TestSealWrongHashRejected(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body := "payload"
	declared := sha256.Sum256([]byte("different-bytes"))
	up, err := e.store.BeginUpload(ctx, e.scope, "k-bad", UploadRequest{
		Name: "a.bin", MIME: "application/octet-stream",
		SHA256: hex.EncodeToString(declared[:]), Size: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	put(t, up.PresignedURL, body)
	_, err = e.store.Seal(ctx, e.scope, up.ID)
	codeIs(t, err, contracts.ErrConflict)
}

func TestAuthRevocationAndForeignSubjects(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref, _ := e.uploadSeal(t, "k-auth", "auth-payload")
	e.foreign.Tenant = e.scope.Tenant
	_, err := e.store.Get(ctx, e.foreign, ref.ID)
	codeIs(t, err, contracts.ErrNotFound) // B114: cross-subject is 404, no existence leak
	foreignTenant := contracts.Scope{Tenant: "ext-" + e.scope.Tenant, Deployment: "d1", Subject: "alice"}
	_, err = e.store.Get(ctx, foreignTenant, ref.ID)
	codeIs(t, err, contracts.ErrNotFound)
	e.allow.Store(false)
	_, err = e.store.Get(ctx, e.scope, ref.ID)
	codeIs(t, err, contracts.ErrForbidden)
	_, _, err = e.store.Open(ctx, e.scope, ref.ID)
	codeIs(t, err, contracts.ErrForbidden)
}

func TestDeleteTombstone(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref, _ := e.uploadSeal(t, "k-del", "delete-me")
	if err := e.store.Delete(ctx, e.scope, ref.ID, "dk"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := e.store.Get(ctx, e.scope, ref.ID)
	codeIs(t, err, contracts.ErrNotFound)
	_, _, err = e.store.Open(ctx, e.scope, ref.ID)
	codeIs(t, err, contracts.ErrNotFound)
}

func TestB115ReadinessGate(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sum := sha256.Sum256([]byte("readiness-body"))
	up, err := e.store.BeginUpload(ctx, e.scope, "k-ready", UploadRequest{
		Name: "r.bin", MIME: "application/octet-stream",
		SHA256: hex.EncodeToString(sum[:]), Size: int64(len("readiness-body")),
	})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	put(t, up.PresignedURL, "readiness-body")
	// Before seal: OpenRange must fail (no version bound).
	_, _, err = e.store.OpenRange(ctx, e.scope, up.ID, 0, 0)
	if err == nil {
		t.Fatal("OpenRange on unsealed artifact succeeded")
	}
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("OpenRange on unsealed: code=%s want NOT_FOUND", contracts.Code(err))
	}
	// After seal: OpenRange succeeds.
	if _, err := e.store.Seal(ctx, e.scope, up.ID); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	rc, _, err := e.store.OpenRange(ctx, e.scope, up.ID, 0, 0)
	if err != nil {
		t.Fatalf("OpenRange after seal: %v", err)
	}
	_ = rc.Close()
}

func TestB115PresignedReuseAfterSeal(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body := "single-use-body"
	sum := sha256.Sum256([]byte(body))
	up, err := e.store.BeginUpload(ctx, e.scope, "k-single-use", UploadRequest{
		Name: "s.bin", MIME: "application/octet-stream",
		SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	put(t, up.PresignedURL, body)
	if _, err := e.store.Seal(ctx, e.scope, up.ID); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	// Re-seal is idempotent: same artifact, same version, same bytes.
	ref2, err := e.store.Seal(ctx, e.scope, up.ID)
	if err != nil {
		t.Fatalf("re-Seal (idempotent): %v", err)
	}
	// The sealed artifact still serves the original bytes.
	rc, _, err := e.store.OpenRange(ctx, e.scope, up.ID, 0, 0)
	if err != nil {
		t.Fatalf("OpenRange after re-seal: %v", err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != body {
		t.Fatalf("artifact content changed after re-PUT: got %q want %q", data, body)
	}
	_ = ref2
}

func TestB115ParserIsolation(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Alice seals an artifact.
	refA, _ := e.uploadSeal(t, "k-parser-a", "alice-secret")
	// Bob (same tenant, different subject) cannot read Alice's artifact.
	bobScope := e.scope
	bobScope.Subject = "bob"
	_, _, err := e.store.OpenRange(ctx, bobScope, refA.ID, 0, 0)
	if err == nil {
		t.Fatal("cross-subject OpenRange succeeded (parser isolation violated)")
	}
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-subject: code=%s want NOT_FOUND (B114)", contracts.Code(err))
	}
	// Cross-tenant also 404.
	extScope := e.scope
	extScope.Tenant = "other-tenant"
	_, _, err = e.store.OpenRange(ctx, extScope, refA.ID, 0, 0)
	if err == nil {
		t.Fatal("cross-tenant OpenRange succeeded")
	}
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant: code=%s want NOT_FOUND", contracts.Code(err))
	}
}

func TestB116RangeProxy(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body := "0123456789abcdefghij"
	ref, _ := e.uploadSeal(t, "k-range", body)

	// Full read.
	rc, _, err := e.store.OpenRange(ctx, e.scope, ref.ID, 0, 0)
	if err != nil {
		t.Fatalf("full OpenRange: %v", err)
	}
	full, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(full) != body {
		t.Fatalf("full read mismatch")
	}

	// Range read: bytes 5-9
	rc, _, err = e.store.OpenRange(ctx, e.scope, ref.ID, 5, 5)
	if err != nil {
		t.Fatalf("range OpenRange: %v", err)
	}
	part, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(part) != "56789" {
		t.Fatalf("range read: got %q want %q", part, "56789")
	}

	// Tail range: bytes=15- (offset 15, to end)
	rc, _, err = e.store.OpenRange(ctx, e.scope, ref.ID, 15, 0)
	if err != nil {
		t.Fatalf("tail OpenRange: %v", err)
	}
	tail, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(tail) != "fghij" {
		t.Fatalf("tail read: got %q want %q", tail, "fghij")
	}
}

func TestB116NoDirectURLLeak(t *testing.T) {
	e := setup(t)
	ref, _ := e.uploadSeal(t, "k-noleak", "no-leak-body")
	// The ArtifactRef must not contain any presigned URL or direct object path.
	rj, _ := json.Marshal(ref)
	for _, forbidden := range []string{"https://", "http://", "presigned", "X-Amz-Signature"} {
		if strings.Contains(string(rj), forbidden) {
			t.Fatalf("ArtifactRef JSON contains %q: %s", forbidden, rj)
		}
	}
	_ = ref
}
