package executor_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// realEnv is the same discipline as mustEnv, for the binding legs: an absent
// real dependency skips the leg with the machine-readable SKIP_ENV token naming
// the exact variables, and the token says out loud that a skip is not coverage.
// This replaces an earlier "red unless AHP_ALLOW_SKIP_REAL_DB=1" rule from the
// lane's own pinning: a red default suite says nothing about the product and it
// is what made the wide gate unreadable (director instruction 2026-11-10, after
// measuring 10 red legs in this package on a runner without the environment).
// What did NOT change: a skip may never be counted as acceptance.
func realEnv(t *testing.T) (dsn, endpoint, ak, sk string) {
	t.Helper()
	if reason := realDepsSkipReason(); reason != "" {
		t.Log(reason)
		t.Skip(reason)
	}
	dsn = os.Getenv("AHP_TEST_DATABASE_URL")
	endpoint = os.Getenv("AHP_S3_ENDPOINT")
	ak = os.Getenv("AHP_MINIO_ROOT_USER")
	sk = os.Getenv("AHP_MINIO_ROOT_PASSWORD")
	return
}

func newBoundPlane(t *testing.T) (*executor.ArtifactPlane, contracts.Scope) {
	t.Helper()
	dsn, endpoint, ak, sk := realEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pg pool: %v", err)
	}
	t.Cleanup(pool.Close)
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT")
	}
	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	bucket := "ahp-exec-bind-" + hex.EncodeToString(rnd[:])
	client, err := minio.New(u.Host, &minio.Options{
		Creds: credentials.NewStaticV4(ak, sk, ""), Secure: u.Scheme == "https",
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	plane, err := executor.NewArtifactPlane(ctx, pool, executor.ArtifactPlaneConfig{
		S3Endpoint: endpoint, S3AccessKey: ak, S3SecretKey: sk, Bucket: bucket,
	})
	if err != nil {
		t.Fatalf("NewArtifactPlane: %v", err)
	}
	t.Cleanup(func() {
		c, cn := context.WithTimeout(context.Background(), 20*time.Second)
		defer cn()
		for o := range client.ListObjects(c, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			_ = client.RemoveObject(c, bucket, o.Key, minio.RemoveObjectOptions{VersionID: o.VersionID})
		}
		_ = client.RemoveBucket(c, bucket)
	})
	scope := contracts.Scope{Tenant: "t-bind-" + hex.EncodeToString(rnd[:]), Deployment: "d1", Subject: "exec-agent"}
	return plane, scope
}

// TestArtifactSealRefusesMissingBinding is the fail-closed core: exec-plane
// artifacts without a context (or task) binding are REFUSED, not silently
// written -- the foreman's defect was BeginUpload without a binding, which put
// every exec artifact outside the context deletion cascade. These refusals need
// no storage at all, so a nil plane must refuse before touching anything.
func TestArtifactSealRefusesMissingBinding(t *testing.T) {
	ctx := context.Background()
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	var plane *executor.ArtifactPlane

	for _, tc := range []struct {
		name string
		b    artifacts.Binding
	}{
		{"no context no task", artifacts.Binding{}},
		{"no context", artifacts.Binding{TaskID: "task-1"}},
		{"no task", artifacts.Binding{ContextID: "ctx-1"}},
		{"malformed context", artifacts.Binding{ContextID: "has/slash", TaskID: "task-1"}},
		{"malformed task", artifacts.Binding{ContextID: "ctx-1", TaskID: " has space"}},
	} {
		_, err := plane.Seal(ctx, scope, tc.b, "a.txt", "text/plain", "body")
		if contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("%s: want INVALID, got %v", tc.name, err)
		}
	}
}

// TestBoundSealSurvivesOnTheToolDoor pins the ingress gate: the model-callable
// write_artifact tool seals through the SAME binding path, and an attempt whose
// turn context carries no context id fails with INVALID instead of writing an
// orphan row.
func TestBoundSealSurvivesOnTheToolDoor(t *testing.T) {
	plane, scope := newBoundPlane(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	args, _ := json.Marshal(map[string]string{"name": "answer.txt", "mime": "text/plain", "content": "hello"})

	// The tool door without a context on the attempt: refused, no orphan.
	unbound := executor.WithTurn(ctx, executor.Turn{Scope: scope, TaskID: "task-tool-1"})
	if _, err := plane.Tools.Execute(unbound, "write_artifact", args); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("tool without context binding: want INVALID, got %v", err)
	}

	// The same tool with the full binding: sealed, and the row is reachable by
	// the context -- that is exactly the reachability the cascade depends on.
	bound := executor.WithTurn(ctx, executor.Turn{Scope: scope, TaskID: "task-tool-2", ContextID: "ctx-tool-1"})
	out, err := plane.Tools.Execute(bound, "write_artifact", args)
	if err != nil {
		t.Fatalf("tool with binding must seal: %v", err)
	}
	var sealed struct {
		ArtifactID string `json:"artifact_id"`
	}
	if err := json.Unmarshal(out, &sealed); err != nil || sealed.ArtifactID == "" {
		t.Fatalf("tool result must carry the artifact id: %s err=%v", out, err)
	}
	listed, err := plane.Store.ListByContext(ctx, scope, "ctx-tool-1")
	if err != nil {
		t.Fatalf("ListByContext: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != sealed.ArtifactID {
		t.Fatalf("bound seal must be reachable by context, got %v", listed)
	}
}

// TestBoundSealCascadeAndIdempotency is the Go-level twin of the stand leg:
// run -> artifact through the exec plane -> thread delete -> the artifact is
// gone for metadata AND download, while a replay of the same bytes under the
// same task does NOT mint a second object.
func TestBoundSealCascadeAndIdempotency(t *testing.T) {
	plane, scope := newBoundPlane(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	b := artifacts.Binding{ContextID: "ctx-cascade-1", TaskID: "task-cascade-1"}
	first, err := plane.Seal(ctx, scope, b, "proof.txt", "text/plain", "restart-proof-FINAL")
	if err != nil {
		t.Fatalf("bound Seal: %v", err)
	}
	if _, err := plane.Store.Get(ctx, scope, first.ArtifactID); err != nil {
		t.Fatalf("sealed artifact must be readable before the cascade: %v", err)
	}
	replay, err := plane.Seal(ctx, scope, b, "proof.txt", "text/plain", "restart-proof-FINAL")
	if err != nil {
		t.Fatalf("replay Seal: %v", err)
	}
	if replay.ArtifactID != first.ArtifactID {
		t.Fatalf("same (task, sha) must replay one artifact: %s vs %s", first.ArtifactID, replay.ArtifactID)
	}

	n, err := plane.Store.DeleteByContext(ctx, scope, "ctx-cascade-1")
	if err != nil {
		t.Fatalf("DeleteByContext: %v", err)
	}
	if n.Tombstoned < 1 {
		t.Fatalf("cascade must catch the exec-plane artifact, result=%+v", n)
	}
	if _, err := plane.Store.Get(ctx, scope, first.ArtifactID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("metadata after cascade: want NOT_FOUND, got %v", err)
	}
	if _, _, err := plane.Store.Open(ctx, scope, first.ArtifactID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("download after cascade: want NOT_FOUND, got %v", err)
	}
}
