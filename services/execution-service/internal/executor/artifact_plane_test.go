package executor_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The idempotency of a seal is the property the acceptance rule is written
// against: "a repeated call with the same (task, sha) must not create a second
// object". It was previously asserted only by reading internal/artifacts/store.go
// -- which is a reading, not a measurement. Here it is measured against real
// PostgreSQL and real object storage, counting the objects that exist.

func newPlane(t *testing.T) (*executor.ArtifactPlane, *minio.Client, string, *pgxpool.Pool) {
	t.Helper()
	dsn, endpoint, ak, sk := mustEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	u, _ := url.Parse(endpoint)
	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	bucket := "ahp-plane-" + hex.EncodeToString(rnd[:])
	client, err := minio.New(u.Host, &minio.Options{
		Creds: credentials.NewStaticV4(ak, sk, ""), Secure: u.Scheme == "https",
	})
	if err != nil {
		t.Fatal(err)
	}
	plane, err := executor.NewArtifactPlane(ctx, pool, executor.ArtifactPlaneConfig{
		S3Endpoint: endpoint, S3AccessKey: ak, S3SecretKey: sk, Bucket: bucket,
		ParserCommand: artifacts.ParserCommand{Path: os.Getenv(executor.EnvArtifactParser)},
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
	return plane, client, bucket, pool
}

func listKeys(t *testing.T, client *minio.Client, bucket string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var keys []string
	for o := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
		if o.Err != nil {
			t.Fatalf("list objects: %v", o.Err)
		}
		keys = append(keys, o.Key)
	}
	return keys
}

func planeScope() contracts.Scope {
	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	sfx := hex.EncodeToString(rnd[:])
	return contracts.Scope{Tenant: "t-plane-" + sfx, Deployment: "d1", Subject: "u-plane-" + sfx}
}

// Exercise the production environment constructor with the real sandboxed
// parser. Missing configuration and refused content must stay unreadable.
func TestArtifactPlaneFromEnvPreservesParserReadinessGate(t *testing.T) {
	// This leg measures the REAL sandboxed parser: it must reject malformed JSON
	// and refuse a relative command, which no stand-in can do. The product's
	// parser installs a Linux seccomp filter before it will look at untrusted
	// content, so on another platform the only honest answer is a named skip --
	// not a red suite, and not a stub pretending to be a sandbox.
	if reason := trustedParserSkipReason(); reason != "" {
		t.Log(reason)
		t.Skip(reason)
	}
	parser := os.Getenv(executor.EnvArtifactParser)
	for _, tc := range []struct {
		name, parser, content string
		readable              bool
	}{
		{"configured", parser, `{"ok":true}`, true},
		{"unconfigured", "", `{"ok":true}`, false},
		{"malformed-json", parser, `{"broken":`, false},
		{"relative-command", "artifact-parser", `{"ok":true}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, bucket, pool := newPlane(t)
			t.Setenv(executor.EnvArtBucket, bucket)
			t.Setenv(executor.EnvArtifactParser, tc.parser)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			plane, err := executor.ArtifactPlaneFromEnv(ctx, pool)
			if err != nil {
				t.Fatal(err)
			}
			scope := planeScope()
			ref, err := plane.Seal(ctx, scope, artifacts.Binding{ContextID: scope.Tenant + "-context", TaskID: scope.Tenant + "-task"}, "document.json", "application/json", tc.content)
			if err != nil {
				t.Fatal(err)
			}
			body, _, err := plane.Store.Open(ctx, scope, ref.ArtifactID)
			if !tc.readable {
				if body != nil {
					body.Close()
				}
				if contracts.Code(err) != contracts.ErrForbidden {
					t.Fatalf("pending/quarantined bytes were readable: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer body.Close()
			got, err := io.ReadAll(body)
			if err != nil || string(got) != tc.content {
				t.Fatalf("configured parser changed original bytes: %v", err)
			}
		})
	}
}

// Same task, same bytes: the same artifact is replayed and the bucket still
// holds exactly one object.
//
// The idempotency half needs no parser, and it is the half the acceptance rule
// is written against. The reopen half depends on whether extraction could run
// here, and BOTH answers are pinned rather than assumed -- a measured run on
// this base with PostgreSQL + MinIO but no parser produced
// "FORBIDDEN: artifact is quarantined or awaiting extraction" at the reopen
// step (internal/artifacts/ingestion.go checkContent), and that is the
// fail-closed content gate, not a replay defect: the replay itself had already
// returned the same artifact id, the same sha/size and exactly one object.
func TestSealReplayDoesNotCreateASecondObject(t *testing.T) {
	t.Run("reopen_after_extraction_ready", func(t *testing.T) { replayLeg(t, contractParser(t)) })
	t.Run("reopen_without_extraction_hits_the_content_gate", func(t *testing.T) { replayLeg(t, "") })
}

func replayLeg(t *testing.T, parser string) {
	t.Helper()
	// The plane reads the parser from the environment at construction, so the
	// branch is selected the way a deployment would select it.
	t.Setenv(executor.EnvArtifactParser, parser)
	plane, client, bucket, _ := newPlane(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scope := planeScope()
	taskID := "task-replay-" + hex.EncodeToString([]byte("abcd"))
	content := "тот же самый ответ того же хода"

	first, err := plane.Seal(ctx, scope, artifacts.Binding{ContextID: "ctx-" + taskID, TaskID: taskID}, "answer.txt", "text/plain", content)
	if err != nil {
		t.Fatalf("first seal: %v", err)
	}
	second, err := plane.Seal(ctx, scope, artifacts.Binding{ContextID: "ctx-" + taskID, TaskID: taskID}, "answer.txt", "text/plain", content)
	if err != nil {
		t.Fatalf("replayed seal: %v", err)
	}
	if second.ArtifactID != first.ArtifactID {
		t.Fatalf("a replay minted a new artifact: %q then %q", first.ArtifactID, second.ArtifactID)
	}
	if second.SHA256 != first.SHA256 || second.Size != first.Size {
		t.Fatalf("replay changed the seal: %+v vs %+v", first, second)
	}
	keys := listKeys(t, client, bucket)
	distinct := map[string]bool{}
	for _, k := range keys {
		distinct[strings.TrimPrefix(k, "/")] = true
	}
	if len(distinct) != 1 {
		t.Fatalf("objects in bucket = %d (%v), want exactly 1: a retry must not duplicate the artifact", len(distinct), keys)
	}

	rc, ref, err := plane.Store.Open(ctx, scope, first.ArtifactID)
	if parser == "" {
		// No extraction could run, so the bytes of a supported text type stay
		// behind the content gate. Pinned as a code AND a reason, because "any
		// error" would pass for a tenant leak or a missing row.
		if rc != nil {
			_ = rc.Close()
		}
		if code := contracts.Code(err); code != contracts.ErrForbidden {
			t.Fatalf("an unextracted text artifact must answer FORBIDDEN, got %v (%v)", code, err)
		}
		if !strings.Contains(err.Error(), "quarantined or awaiting extraction") {
			t.Fatalf("the refusal must name the content gate, got %v", err)
		}
		t.Log("reopen_branch=content_gate -- extraction cannot run without a parser; the read-back " +
			"after extraction is NOT covered by this branch, it is the other branch")
		return
	}
	if err != nil {
		t.Fatalf("reopen the sealed artifact: %v", err)
	}
	defer rc.Close()
	raw, _ := io.ReadAll(rc)
	if string(raw) != content {
		t.Fatalf("sealed bytes = %q, want %q", raw, content)
	}
	if ref.SHA256 != first.SHA256 {
		t.Fatalf("stored sha = %s, want %s", ref.SHA256, first.SHA256)
	}
}

// Different bytes under the same task are a different artifact -- the key is
// (task, sha), not task alone, so a retried attempt with a NEW answer cannot
// overwrite the previous one.
func TestSealDifferentBytesAreDistinctArtifacts(t *testing.T) {
	plane, client, bucket, _ := newPlane(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scope := planeScope()
	taskID := "task-two-answers"
	a, err := plane.Seal(ctx, scope, artifacts.Binding{ContextID: "ctx-" + taskID, TaskID: taskID}, "answer.txt", "text/plain", "первый ответ")
	if err != nil {
		t.Fatalf("seal A: %v", err)
	}
	b, err := plane.Seal(ctx, scope, artifacts.Binding{ContextID: "ctx-" + taskID, TaskID: taskID}, "answer.txt", "text/plain", "второй ответ")
	if err != nil {
		t.Fatalf("seal B: %v", err)
	}
	if a.ArtifactID == b.ArtifactID || a.SHA256 == b.SHA256 {
		t.Fatalf("different content must not collide onto one artifact: %+v vs %+v", a, b)
	}
	if got := len(listKeys(t, client, bucket)); got != 2 {
		t.Fatalf("objects = %d, want 2", got)
	}
}

// The task id is part of the key on purpose: two tasks that produced identical
// bytes must each own a readable artifact rather than share one object.
func TestSealSameBytesDifferentTasksDoNotCollide(t *testing.T) {
	plane, _, _, _ := newPlane(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scope := planeScope()
	content := "одинаковый ответ"
	a, err := plane.Seal(ctx, scope, artifacts.Binding{ContextID: "ctx-task-one", TaskID: "task-one"}, "answer.txt", "text/plain", content)
	if err != nil {
		t.Fatalf("seal one: %v", err)
	}
	b, err := plane.Seal(ctx, scope, artifacts.Binding{ContextID: "ctx-task-two", TaskID: "task-two"}, "answer.txt", "text/plain", content)
	if err != nil {
		t.Fatalf("seal two: %v", err)
	}
	if a.ArtifactID == b.ArtifactID {
		t.Fatal("two tasks shared one artifact: one of them would read the other's object")
	}
}

// An empty answer is not an artifact, and a foreign scope cannot read a seal it
// does not own.
func TestSealRejectsEmptyContentAndForeignRead(t *testing.T) {
	plane, _, _, _ := newPlane(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scope := planeScope()
	if _, err := plane.Seal(ctx, scope, artifacts.Binding{ContextID: "ctx-task-empty", TaskID: "task-empty"}, "answer.txt", "text/plain", ""); err == nil {
		t.Fatal("an empty artifact was accepted")
	}
	sealed, err := plane.Seal(ctx, scope, artifacts.Binding{ContextID: "ctx-task-foreign", TaskID: "task-foreign"}, "answer.txt", "text/plain", "чужого не брать")
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	foreign := contracts.Scope{Tenant: scope.Tenant + "-other", Deployment: scope.Deployment, Subject: scope.Subject}
	if _, _, err := plane.Store.Open(ctx, foreign, sealed.ArtifactID); err == nil {
		t.Fatal("a foreign scope read an artifact it does not own")
	}
}
