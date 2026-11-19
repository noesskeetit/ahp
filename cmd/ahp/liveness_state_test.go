//go:build ahp_data

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// W-1: the storage plane must know, from durable facts only, that something
// alive still uses an artifact -- and it must refuse to collect it even when the
// row carries no pin, no legal hold, and its retention floor is long past.
//
// Every claim below is checked against the table and the bucket directly (a raw
// SQL row lookup and a raw version listing), never against a log line, and the
// control leg demonstrates the defect itself: with no live-reference authority
// mounted the sweep removes exactly the row a live run still needs.

const (
	livenessBucketPrefix = "ahp-hive-h23w"
	livenessTenantPrefix = "ahp_hive_h23w"
)

type livenessEnv struct {
	pool   *pgxpool.Pool
	client *minio.Client
	bucket string
	scope  contracts.Scope
	seed   *artifacts.Store
	state  *state.Store
	live   *stateLiveness
}

func newLivenessEnv(t *testing.T, ctx context.Context) *livenessEnv {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("AHP_DATABASE_URL")
	}
	raw := os.Getenv("AHP_S3_ENDPOINT")
	if dsn == "" || raw == "" || os.Getenv("AHP_MINIO_ROOT_USER") == "" ||
		os.Getenv("AHP_MINIO_ROOT_PASSWORD") == "" {
		t.Skip("live-reference tests need AHP_TEST_DATABASE_URL and MinIO credentials: bring the isolated test stand up (test PG + MinIO), then set them; not a silent pass")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT: %v", err)
	}
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
		t.Fatalf("minio: %v", err)
	}
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	suffix := hex.EncodeToString(rnd[:])
	bucket := livenessBucketPrefix + "-" + suffix
	scope := contracts.Scope{
		Tenant:     livenessTenantPrefix + "-" + suffix,
		Deployment: "d-h23w",
		Subject:    "subject-h23w",
	}
	if err := pool.QueryRow(ctx, `SELECT 1`).Scan(new(int)); err != nil {
		t.Fatalf("database unreachable: %v", err)
	}
	seed, err := artifacts.New(pool, client, bucket,
		func(context.Context, contracts.Scope, string, string) error { return nil })
	if err != nil {
		t.Fatalf("seed store: %v", err)
	}
	if err := seed.Migrate(ctx); err != nil {
		t.Fatalf("artifacts migrate: %v", err)
	}
	allow := func(context.Context, contracts.Scope, string, string) error { return nil }
	ws, err := workspace.New(pool, seed, allow)
	if err != nil {
		t.Fatal(err)
	}
	if err = ws.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	mem, err := memory.New(pool, seed, allow, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = mem.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("state migrate: %v", err)
	}
	// The execution plane must exist for the probe to answer "this conversation
	// is finished": a deployment that does not run it is a separate case, covered
	// by TestAbsentPlaneLeavesTheOtherAuthoritiesInCharge.
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS ahp_exec`); err != nil {
		t.Fatalf("exec schema: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS ahp_exec.tasks (
		task_id text PRIMARY KEY, tenant text NOT NULL, deployment text NOT NULL,
		subject text NOT NULL, caller text NOT NULL, context_id text NOT NULL,
		kind text NOT NULL DEFAULT '', parent_task text NOT NULL DEFAULT '',
		state text NOT NULL, status_message jsonb, artifacts jsonb NOT NULL DEFAULT '[]'::jsonb,
		metadata jsonb, status_version bigint NOT NULL DEFAULT 1, fencing_token bigint NOT NULL DEFAULT 1,
		lease_owner text NOT NULL DEFAULT '', lease_expires_at timestamptz,
		cancel_requested boolean NOT NULL DEFAULT false, attempts integer NOT NULL DEFAULT 0,
		created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
		terminal_at timestamptz)`); err != nil {
		t.Fatalf("exec tasks: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS ahp_exec.contexts (
		context_id text PRIMARY KEY, tenant text NOT NULL, deployment text NOT NULL,
		subject text NOT NULL, caller text NOT NULL, created_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		t.Fatalf("exec contexts: %v", err)
	}
	// The identity schemas of this shared test database belong to other tenants'
	// journeys, so the probe is pointed at a lane-private copy of the grant tables
	// instead of reading rows this test did not write.
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS ahp_identity_h23w`); err != nil {
		t.Fatalf("identity schema: %v", err)
	}
	for _, table := range []string{"grants_root", "grants_child"} {
		if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS ahp_identity_h23w.`+table+` (
			id text PRIMARY KEY, tenant text NOT NULL, task text NOT NULL, context_id text NOT NULL,
			status text NOT NULL DEFAULT 'active', expires_at timestamptz NOT NULL,
			revoked_at timestamptz)`); err != nil {
			t.Fatalf("identity %s: %v", table, err)
		}
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS ahp_identity_h23w.contexts (
		id text PRIMARY KEY, tenant text NOT NULL)`); err != nil {
		t.Fatalf("identity contexts: %v", err)
	}
	t.Setenv("AHP_IDENTITY_SCHEMA", "ahp_identity_h23w")
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		for obj := range client.ListObjects(c, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if obj.Err != nil {
				break
			}
			_ = client.RemoveObject(c, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
		}
		_ = client.RemoveBucket(c, bucket)
		for _, q := range []string{
			`DELETE FROM ahp_artifacts.artifacts WHERE tenant = $1`,
			`DELETE FROM ahp_artifacts.gc_events WHERE tenant = $1`,
			`DELETE FROM ahp_artifacts.gc_pins WHERE tenant = $1`,
			`DELETE FROM ahp_exec.tasks WHERE tenant = $1`,
			`DELETE FROM ahp_exec.contexts WHERE tenant = $1`,
			`DELETE FROM ahp_identity_h23w.grants_root WHERE tenant = $1`,
			`DELETE FROM ahp_identity_h23w.grants_child WHERE tenant = $1`,
		} {
			_, _ = pool.Exec(c, q, scope.Tenant)
		}
		for _, q := range []string{
			`DELETE FROM ahp_state.runs r USING ahp_state.threads t WHERE r.thread_id = t.id AND t.tenant = $1`,
			`DELETE FROM ahp_state.threads WHERE tenant = $1`,
		} {
			_, _ = pool.Exec(c, q, scope.Tenant)
		}
	})
	return &livenessEnv{pool: pool, client: client, bucket: bucket, scope: scope, seed: seed,
		state: st, live: newStateLiveness(pool)}
}

// sealTombstonedExpired seals a body bound to a conversation and a task, deletes
// it and pushes its retention floor into the past: the exact shape a sweep takes
// unless something tells it the row is still used.
func (e *livenessEnv) sealTombstonedExpired(t *testing.T, ctx context.Context, contextID, taskID, body string) (string, string) {
	t.Helper()
	id := sealArtifact(t, ctx, e.seed, e.scope,
		artifacts.Binding{ContextID: contextID, TaskID: taskID}, body)
	object := e.objectOf(t, ctx, id)
	if err := e.seed.Delete(ctx, e.scope, id, "del-"+id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	past := time.Now().UTC().Add(-time.Hour)
	if err := e.seed.SetRetention(ctx, e.scope, id, &past); err != nil {
		t.Fatalf("SetRetention: %v", err)
	}
	return id, object
}

func (e *livenessEnv) objectOf(t *testing.T, ctx context.Context, id string) string {
	t.Helper()
	var object string
	if err := e.pool.QueryRow(ctx, `SELECT object FROM ahp_artifacts.artifacts WHERE id = $1`, id).Scan(&object); err != nil {
		t.Fatalf("read object of %s: %v", id, err)
	}
	return object
}

func (e *livenessEnv) rowPresent(t *testing.T, ctx context.Context, id string) bool {
	t.Helper()
	var deleted bool
	err := e.pool.QueryRow(ctx, `SELECT deleted FROM ahp_artifacts.artifacts WHERE id = $1`, id).Scan(&deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return false
	}
	if err != nil {
		t.Fatalf("row lookup %s: %v", id, err)
	}
	return true
}

func (e *livenessEnv) versions(t *testing.T, object string) int {
	t.Helper()
	n := 0
	for obj := range e.client.ListObjects(context.Background(), e.bucket,
		minio.ListObjectsOptions{Prefix: object, Recursive: true, WithVersions: true}) {
		if obj.Err != nil {
			t.Fatalf("list versions of %s: %v", object, obj.Err)
		}
		if obj.Key == object && obj.VersionID != "" {
			n++
		}
	}
	return n
}

// openRun creates the durable lane and one run that has not finished.
func (e *livenessEnv) openRun(t *testing.T, ctx context.Context, contextID, taskID string) string {
	t.Helper()
	thread, err := e.state.CreateThread(ctx, e.scope, contextID, "thread-"+contextID)
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := e.state.CreateRun(ctx, e.scope, thread.ID, taskID, "run-"+taskID, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	return run.ID
}

func (e *livenessEnv) finishRun(t *testing.T, ctx context.Context, runID string) {
	t.Helper()
	tag, err := e.pool.Exec(ctx, `UPDATE ahp_state.runs SET status = 'done' WHERE id = $1`, runID)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("finish run: %v rows=%d", err, tag.RowsAffected())
	}
}

func (e *livenessEnv) sweep(t *testing.T, ctx context.Context, key string, opts ...artifacts.GCOption) artifacts.GCReport {
	t.Helper()
	gc, err := artifacts.NewGC(e.seed, opts...)
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}
	report, err := gc.RunOnce(ctx, e.scope, artifacts.GCPolicy{
		Key:                key,
		StagedGrace:        time.Millisecond,
		TombstoneRetention: time.Millisecond,
		OrphanGrace:        time.Millisecond,
		BatchSize:          50,
	})
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	return report
}

// (1a) The defect, demonstrated rather than asserted: with no live-reference
// authority mounted -- the state of the world before W-1 -- the sweep collects a
// tombstoned artifact that a running run still needs.
func TestSweepCollectsLiveRunArtifactWithoutAuthority(t *testing.T) {
	ctx := context.Background()
	e := newLivenessEnv(t, ctx)
	runID := e.openRun(t, ctx, "ctx-"+t.Name(), "task-"+t.Name())
	id, object := e.sealTombstonedExpired(t, ctx, "ctx-"+t.Name(), runID, "live run body")
	if got := e.versions(t, object); got != 1 {
		t.Fatalf("the sealed body must exist before the sweep: versions=%d", got)
	}
	e.sweep(t, ctx, "no-authority")
	if e.rowPresent(t, ctx, id) {
		t.Fatal("the pre-W-1 sweep was expected to collect this row; if it no longer does, this test must be retired rather than left to prove nothing")
	}
	if got := e.versions(t, object); got != 0 {
		t.Fatalf("the bytes of the collected row must be gone: versions=%d", got)
	}
}

// (1b) With the production authority mounted, the same row survives: no pin, no
// legal hold, retention floor an hour in the past, and a run in 'running'.
// (1c) After the run terminates, the same authority lets the sweep take it.
func TestLiveRunReferenceSurvivesSweep(t *testing.T) {
	ctx := context.Background()
	e := newLivenessEnv(t, ctx)
	runID := e.openRun(t, ctx, "ctx-"+t.Name(), "task-"+t.Name())
	id, object := e.sealTombstonedExpired(t, ctx, "ctx-"+t.Name(), runID, "protected body")

	report := e.sweep(t, ctx, "with-authority",
		artifacts.WithLiveness(e.live), artifacts.WithReferenceScan(e.live))
	if !e.rowPresent(t, ctx, id) {
		t.Fatalf("a run in 'running' must keep its artifact: report=%+v", report)
	}
	if report.SkippedLive < 1 {
		t.Fatalf("the sweep must report the refusal it made: %+v", report)
	}
	if got := e.versions(t, object); got != 1 {
		t.Fatalf("protected bytes must still be there: versions=%d", got)
	}
	// The refusal is not an accident of one marker kind: the same run id asked
	// about as a run, and its conversation asked about as a thread, both answer
	// alive from the same durable row.
	for _, kind := range []string{artifacts.PinKindTask, artifacts.PinKindRun} {
		answer, err := e.live.RefsAlive(ctx, e.scope.Tenant, []artifacts.Ref{{Kind: kind, ID: runID}})
		if err != nil {
			t.Fatalf("RefsAlive(%s): %v", kind, err)
		}
		if !answer[artifacts.Ref{Kind: kind, ID: runID}] {
			t.Fatalf("kind %s of a running run must be alive", kind)
		}
	}

	e.finishRun(t, ctx, runID)
	report = e.sweep(t, ctx, "after-terminal",
		artifacts.WithLiveness(e.live), artifacts.WithReferenceScan(e.live))
	if e.rowPresent(t, ctx, id) {
		t.Fatalf("a finished run must not hold its artifact forever: report=%+v", report)
	}
	if got := e.versions(t, object); got != 0 {
		t.Fatalf("the collected artifact's bytes must be gone: versions=%d", got)
	}
}

// (2) A conversation whose A2A task reached a terminal state, with no run and no
// live credential, is settled as finished -- the sweep collects it. This is the
// leg that shows the probe is decisive, not merely permissive.
func TestTerminalConversationIsCollected(t *testing.T) {
	ctx := context.Background()
	e := newLivenessEnv(t, ctx)
	contextID := "ctxterm-" + t.Name()
	if _, err := e.pool.Exec(ctx,
		`INSERT INTO ahp_exec.contexts (context_id, tenant, deployment, subject, caller) VALUES ($1,$2,'d-h23w','subject-h23w','caller')`,
		contextID, e.scope.Tenant); err != nil {
		t.Fatalf("exec context: %v", err)
	}
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_exec.tasks (task_id, tenant, deployment, subject, caller, context_id, state, terminal_at)
VALUES ($1,$2,'d-h23w','subject-h23w','caller',$3,'TASK_STATE_COMPLETED', now())`,
		"taskterm-"+t.Name(), e.scope.Tenant, contextID); err != nil {
		t.Fatalf("exec task: %v", err)
	}
	answer, err := e.live.RefsAlive(ctx, e.scope.Tenant, []artifacts.Ref{{Kind: artifacts.PinKindContext, ID: contextID}})
	if err != nil {
		t.Fatalf("RefsAlive: %v", err)
	}
	if alive, known := answer[artifacts.Ref{Kind: artifacts.PinKindContext, ID: contextID}]; !known || alive {
		t.Fatalf("a completed conversation must be answered known-dead: known=%v alive=%v", known, alive)
	}
	id, object := e.sealTombstonedExpired(t, ctx, contextID, "taskterm-"+t.Name(), "terminal body")
	report := e.sweep(t, ctx, "terminal-conversation",
		artifacts.WithLiveness(e.live), artifacts.WithReferenceScan(e.live))
	if e.rowPresent(t, ctx, id) {
		t.Fatalf("nothing alive refers to this artifact any more: report=%+v", report)
	}
	if got := e.versions(t, object); got != 0 {
		t.Fatalf("bytes must be gone after the sweep: versions=%d", got)
	}
}

// (3) A durable turn frame that names the artifact keeps it, even though the
// conversation itself is finished: the reference and the marker are different
// questions, and the frame is the answer to the first.
func TestTurnFrameReferenceSurvivesSweep(t *testing.T) {
	ctx := context.Background()
	e := newLivenessEnv(t, ctx)
	contextID := "ctxff-" + t.Name()
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_exec.contexts (context_id, tenant, deployment, subject, caller) VALUES ($1,$2,'d-h23w','subject-h23w','caller')`,
		contextID, e.scope.Tenant); err != nil {
		t.Fatalf("exec context: %v", err)
	}
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_exec.tasks (task_id, tenant, deployment, subject, caller, context_id, state, terminal_at)
VALUES ($1,$2,'d-h23w','subject-h23w','caller',$3,'TASK_STATE_FAILED', now())`,
		"taskff-"+t.Name(), e.scope.Tenant, contextID); err != nil {
		t.Fatalf("exec task: %v", err)
	}
	id, object := e.sealTombstonedExpired(t, ctx, contextID, "taskff-"+t.Name(), "framed body")

	// The message of the finished conversation still carries the artifact id.
	thread, err := e.state.CreateThread(ctx, e.scope, contextID+"-frame", "thread-frame-"+contextID)
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	frame := json.RawMessage(`{"kind":"tool_result","artifact_id":"` + id + `"}`)
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_state.stream_events (id, thread_id, run_id, key, kind, sequence, payload, digest, created_at)
VALUES ($2,$1,'run-frame','k-frame','frame',1,$3,'digest-frame', now())`,
		thread.ID, "frame-"+id, frame); err != nil {
		t.Fatalf("stream event: %v", err)
	}
	report := e.sweep(t, ctx, "frame-protects",
		artifacts.WithLiveness(e.live), artifacts.WithReferenceScan(e.live))
	if !e.rowPresent(t, ctx, id) {
		t.Fatalf("a durable frame naming the artifact must protect it: report=%+v", report)
	}
	if got := e.versions(t, object); got != 1 {
		t.Fatalf("protected bytes must survive: versions=%d", got)
	}

	// Remove the frame: nothing refers to it now, so the same policy collects it.
	if _, err := e.pool.Exec(ctx, `DELETE FROM ahp_state.stream_events WHERE thread_id = $1`, thread.ID); err != nil {
		t.Fatalf("remove frame: %v", err)
	}
	if _, err := e.pool.Exec(ctx, `DELETE FROM ahp_state.runs r USING ahp_state.threads t WHERE r.thread_id = t.id AND t.id = $1`, thread.ID); err != nil {
		t.Fatalf("cleanup runs: %v", err)
	}
	if _, err := e.pool.Exec(ctx, `DELETE FROM ahp_state.threads WHERE id = $1`, thread.ID); err != nil {
		t.Fatalf("cleanup thread: %v", err)
	}
	report = e.sweep(t, ctx, "frame-gone",
		artifacts.WithLiveness(e.live), artifacts.WithReferenceScan(e.live))
	if e.rowPresent(t, ctx, id) {
		t.Fatalf("with the frame gone the row is collectable: report=%+v", report)
	}
}

// (4) Fail-closed: a probe that cannot answer must protect the batch, never
// delete on a guess. The engine owns this rule; this pins that the production
// probe is wired through it rather than around it.
func TestUnreadableProbeProtects(t *testing.T) {
	ctx := context.Background()
	e := newLivenessEnv(t, ctx)
	contextID := "ctxerr-" + t.Name()
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_exec.contexts (context_id, tenant, deployment, subject, caller) VALUES ($1,$2,'d-h23w','subject-h23w','caller')`,
		contextID, e.scope.Tenant); err != nil {
		t.Fatalf("exec context: %v", err)
	}
	id, object := e.sealTombstonedExpired(t, ctx, contextID, "taskerr-"+t.Name(), "fail closed body")
	failing := artifacts.ReferenceScanFunc(func(context.Context, string, []artifacts.ArtifactQuery) (map[string]bool, error) {
		return nil, errors.New("probe unavailable")
	})
	report := e.sweep(t, ctx, "probe-error",
		artifacts.WithLiveness(e.live), artifacts.WithReferenceScan(failing))
	if !e.rowPresent(t, ctx, id) {
		t.Fatalf("a failing probe must never delete: report=%+v", report)
	}
	if got := e.versions(t, object); got != 1 {
		t.Fatalf("bytes must be intact: versions=%d", got)
	}
}

// (5) A plane the deployment does not run has no opinion; it must not silence
// the authorities that do exist, and it must not be an error.
func TestAbsentOwnerPlaneFailsClosed(t *testing.T) {
	ctx := context.Background()
	e := newLivenessEnv(t, ctx)
	t.Setenv("AHP_MEMORY_SCHEMA", "ahp_memory_missing_h23w")
	live := newStateLiveness(e.pool)
	referenced, err := live.ArtifactsReferenced(ctx, e.scope.Tenant,
		[]artifacts.ArtifactQuery{{ID: "art-unknown", Digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}})
	if err == nil {
		t.Fatalf("missing memory owner must fail closed, got references=%v", referenced)
	}
	// A typo in a schema name is still refused: the probe validates identifiers
	// instead of letting one become SQL.
	t.Setenv("AHP_STATE_SCHEMA", "ahp_state; DROP TABLE ahp_artifacts.artifacts")
	if _, err := newStateLiveness(e.pool).RefsAlive(ctx, e.scope.Tenant,
		[]artifacts.Ref{{Kind: artifacts.PinKindContext, ID: "x"}}); err == nil {
		t.Fatal("an invalid schema identifier must be refused, not interpolated")
	}
}
