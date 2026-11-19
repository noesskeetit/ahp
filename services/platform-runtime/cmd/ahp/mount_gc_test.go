//go:build ahp_data

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The background GC scheduler gate tests. As in the storage-GC suite, every
// claim about bytes is checked out of band -- a raw version listing of one exact
// object key through the MinIO client, and a raw SQL read of the metadata table
// -- so a passing test is evidence about the object store and the rows, not
// about the scheduler's own log lines. Every resource these tests touch is named
// with the h26 lane prefix so nothing on the shared stand is confused for another
// lane's work.

// Lane prefixes. S3 bucket names forbid underscores, so the bucket uses a
// hyphenated prefix while the Postgres tenant (a contracts identifier) keeps the
// h26 lane marker with underscores; both carry the h26 marker so nothing on the
// shared stand is mistaken for another lane's work.
const (
	h26BucketPrefix = "ahp-hive-h26"
	h26TenantPrefix = "ahp_hive_h26"
)

type gcSchedEnv struct {
	pool   *pgxpool.Pool
	client *minio.Client
	bucket string
	scope  contracts.Scope
	seed   *artifacts.Store // full-authorize store used only to seed fixtures
}

// gcSchedTestEnv is the scheduler-test harness: real PostgreSQL and real MinIO
// from the environment (skipped when absent), a lane-owned bucket and tenant, and
// a permissive-authorize seed store. The scheduler under test builds its own
// gc-only store from the environment and sweeps the same tenant.
func gcSchedTestEnv(t *testing.T, ctx context.Context) *gcSchedEnv {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("AHP_DATABASE_URL")
	}
	raw := os.Getenv("AHP_S3_ENDPOINT")
	if dsn == "" || raw == "" || os.Getenv("AHP_MINIO_ROOT_USER") == "" ||
		os.Getenv("AHP_MINIO_ROOT_PASSWORD") == "" {
		t.Skip("gc scheduler tests need AHP_TEST_DATABASE_URL and MinIO credentials")
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
	bucket := h26BucketPrefix + "-" + suffix
	scope := contracts.Scope{Tenant: h26TenantPrefix + "-" + suffix, Deployment: "d-h26", Subject: "subject-h26"}
	seed, err := artifacts.New(pool, client, bucket,
		func(context.Context, contracts.Scope, string, string) error { return nil })
	if err != nil {
		t.Fatalf("seed store: %v", err)
	}
	if err := seed.Migrate(ctx); err != nil {
		t.Fatalf("seed migrate: %v", err)
	}
	// Collection needs answers from every configured owner, including owners
	// with no references. Provision the real schemas instead of treating an
	// absent service as proof of no references.
	allow := func(context.Context, contracts.Scope, string, string) error { return nil }
	if err := state.New(pool).Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := identity.New(pool).Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	execution, err := gateway.NewStore(pool, "ahp_exec")
	if err != nil {
		t.Fatal(err)
	}
	if err = execution.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
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
	// The object store is versioned by the stand; the sweep deletes every version
	// of a purged key, so cleanup removes the same way a sweep would and the
	// bucket is then empty enough to drop.
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
	})
	return &gcSchedEnv{pool: pool, client: client, bucket: bucket, scope: scope, seed: seed}
}

// configureScheduler points the scheduler's environment at this test's bucket and
// tenant and collapses every GC floor to milliseconds, exactly as fastPolicy does
// in the storage suite -- only the age floors change; every refusal rule stays a
// production code path.
func (e *gcSchedEnv) configureScheduler(t *testing.T, enable bool, tenants ...string) {
	t.Helper()
	t.Setenv("AHP_S3_BUCKET", e.bucket)
	t.Setenv("AHP_GC_SCHEDULER_INTERVAL", "20ms")
	t.Setenv("AHP_GC_SCHEDULER_PASS_TIMEOUT", "30s")
	t.Setenv("AHP_GC_SCHEDULER_STAGED_GRACE", "1ms")
	t.Setenv("AHP_GC_SCHEDULER_TOMBSTONE_RETENTION", "1ms")
	t.Setenv("AHP_GC_SCHEDULER_ORPHAN_GRACE", "1ms")
	if enable {
		t.Setenv("AHP_ENABLE_GC_SCHEDULER", "1")
		t.Setenv("AHP_GC_SCHEDULER_TENANTS", strings.Join(tenants, ","))
	} else {
		t.Setenv("AHP_ENABLE_GC_SCHEDULER", "")
	}
}

func gcSchedServer(pool *pgxpool.Pool) *server {
	return &server{pool: pool, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// tombstoneExpired seals a body, deletes it (tombstone) and sets an explicit
// past retain_until, which is the shape eligibleSQL collects under any policy.
// It returns the artifact id and its object key (captured before deletion, while
// the row still names it) so a test can watch the bytes out of band.
func (e *gcSchedEnv) tombstoneExpired(t *testing.T, ctx context.Context, scope contracts.Scope, body string) (string, string) {
	t.Helper()
	id := sealArtifact(t, ctx, e.seed, scope, artifacts.Binding{ContextID: "ctx-" + t.Name()}, body)
	object := e.objectOf(t, id)
	if err := e.seed.Delete(ctx, scope, id, "del-"+t.Name()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	past := time.Now().UTC().Add(-time.Hour)
	if err := e.seed.SetRetention(ctx, scope, id, &past); err != nil {
		t.Fatalf("SetRetention: %v", err)
	}
	return id, object
}

func (e *gcSchedEnv) objectOf(t *testing.T, id string) string {
	t.Helper()
	var object string
	if err := e.pool.QueryRow(ctx0(), `SELECT object FROM ahp_artifacts.artifacts WHERE id = $1`, id).Scan(&object); err != nil {
		t.Fatalf("read object of %s: %v", id, err)
	}
	return object
}

// rowGone asks the table directly (bypassing the read gate that hides tombstones)
// whether a metadata row still exists.
func (e *gcSchedEnv) rowGone(t *testing.T, id string) bool {
	t.Helper()
	var deleted bool
	err := e.pool.QueryRow(ctx0(), `SELECT deleted FROM ahp_artifacts.artifacts WHERE id = $1`, id).Scan(&deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return true
	}
	if err != nil {
		t.Fatalf("row lookup %s: %v", id, err)
	}
	return false
}

// versions counts the live object versions of one key straight from the bucket,
// ignoring delete markers. Zero means the bytes are gone -- the only honest way
// to show a sweep did (or did not) remove them.
func (e *gcSchedEnv) versions(t *testing.T, object string) int {
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

func ctx0() context.Context { return context.Background() }

// waitFor polls cond until it is true or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// (a) With the flag on, an expired tombstone is removed by the scheduler's own
// clock -- no external GC call is made anywhere in this test.
func TestGCSchedulerPurgesExpiredGarbageOnTick(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	env := gcSchedTestEnv(t, ctx)
	env.configureScheduler(t, true, env.scope.Tenant)

	id, object := env.tombstoneExpired(t, ctx, env.scope, "expired-tombstone-must-be-collected")
	if env.rowGone(t, id) || env.versions(t, object) == 0 {
		t.Fatalf("fixture not in place before the scheduler ran: gone=%v versions=%d", env.rowGone(t, id), env.versions(t, object))
	}
	// A crashed writer left a pin after its transaction ended. Only the
	// scheduler's State settlement proof may retire it; expiry alone cannot.
	if _, err := env.pool.Exec(ctx, `INSERT INTO ahp_artifacts.checkpoint_pins(operation_id,tenant,artifact_id) VALUES($1,$2,$3)`, contracts.NewID(), env.scope.Tenant, id); err != nil {
		t.Fatal(err)
	}

	s := gcSchedServer(env.pool)
	if err := s.tryMountGCScheduler(ctx); err != nil {
		t.Fatalf("tryMountGCScheduler: %v", err)
	}
	if s.gcDrainOnShutdown == nil {
		t.Fatalf("scheduler mounted without a shutdown drain hook")
	}

	if !waitFor(t, 30*time.Second, func() bool { return env.rowGone(t, id) && env.versions(t, object) == 0 }) {
		t.Fatalf("scheduler never collected the expired tombstone: gone=%v versions=%d", env.rowGone(t, id), env.versions(t, object))
	}
	var remainingPins int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_artifacts.checkpoint_pins WHERE tenant=$1`, env.scope.Tenant).Scan(&remainingPins); err != nil || remainingPins != 0 {
		t.Fatalf("scheduled reconciliation left pins=%d err=%v", remainingPins, err)
	}
	// Stop the loop; a completed drain confirms no pass is left dangling.
	cancel()
	done := make(chan struct{})
	go func() { s.gcDrainOnShutdown(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("drain hung with no pass expected in flight")
	}
}

// (b) With the flag off the mount is a no-op: no sweep runs and the behavior is
// identical to today's on-demand-only plane. An expired tombstone survives.
func TestGCSchedulerDisabledWhenFlagOff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	env := gcSchedTestEnv(t, ctx)
	env.configureScheduler(t, false)

	id, object := env.tombstoneExpired(t, ctx, env.scope, "must-survive-because-scheduler-is-off")

	s := gcSchedServer(env.pool)
	if err := s.tryMountGCScheduler(ctx); err != nil {
		t.Fatalf("tryMountGCScheduler (off) should be a silent no-op, got: %v", err)
	}
	if s.gcDrainOnShutdown != nil {
		t.Fatalf("flag-off mount must not install a drain hook")
	}
	// Several intervals pass with the (would-be) ticker nominally at 20ms.
	time.Sleep(400 * time.Millisecond)
	if env.rowGone(t, id) {
		t.Fatalf("scheduler swept garbage while disabled: row for %s is gone", id)
	}
	if env.versions(t, object) == 0 {
		t.Fatalf("scheduler removed bytes while disabled")
	}
	cancel()
}

// (b') Enabled but with no tenant named leaves the scheduler idle: a collector
// with an unbounded blast radius is refused, and nothing is swept.
func TestGCSchedulerIdleWithoutTenants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	env := gcSchedTestEnv(t, ctx)
	t.Setenv("AHP_ENABLE_GC_SCHEDULER", "1")
	t.Setenv("AHP_GC_SCHEDULER_TENANTS", "")
	t.Setenv("AHP_S3_BUCKET", env.bucket)

	id, object := env.tombstoneExpired(t, ctx, env.scope, "must-survive-idle-scheduler")
	s := gcSchedServer(env.pool)
	if err := s.tryMountGCScheduler(ctx); err != nil {
		t.Fatalf("enabled-but-idle mount should return nil, got: %v", err)
	}
	if s.gcDrainOnShutdown != nil {
		t.Fatalf("idle scheduler must not install a drain hook")
	}
	time.Sleep(400 * time.Millisecond)
	if env.rowGone(t, id) || env.versions(t, object) == 0 {
		t.Fatalf("idle scheduler swept: gone=%v versions=%d", env.rowGone(t, id), env.versions(t, object))
	}
	cancel()
}

// (c) A GC pass never overlaps itself: while one pass holds the single-flight
// guard, further ticks are refused, and the wrapped sweep -- which calls the REAL
// gc.RunOnce against real garbage -- is never entered concurrently.
func TestGCSchedulerPassNeverOverlapsItself(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	env := gcSchedTestEnv(t, ctx)

	// Seed one expired tombstone the real sweep will collect.
	t.Setenv("AHP_S3_BUCKET", env.bucket)
	id, object := env.tombstoneExpired(t, ctx, env.scope, "expired-under-single-flight")

	gc, err := artifacts.NewGC(env.seed)
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}
	var inFlight, maxInFlight atomic.Int32
	realSweep := gc.RunOnce
	wrapped := func(c context.Context, scope contracts.Scope, p artifacts.GCPolicy) (artifacts.GCReport, error) {
		cur := inFlight.Add(1)
		for {
			m := maxInFlight.Load()
			if cur <= m || maxInFlight.CompareAndSwap(m, cur) {
				break
			}
		}
		defer inFlight.Add(-1)
		// Hold the pass long enough that the other startPass calls in this test
		// necessarily arrive while it is still running.
		time.Sleep(150 * time.Millisecond)
		return realSweep(c, scope, p)
	}

	sched := &gcScheduler{
		cfg: gcSchedulerConfig{
			tenants:      []string{env.scope.Tenant},
			passTimeout:  30 * time.Second,
			sweepSubject: gcSchedulerIdentity,
			policy: artifacts.GCPolicy{
				StagedGrace:        time.Millisecond,
				TombstoneRetention: time.Millisecond,
				OrphanGrace:        time.Millisecond,
				BatchSize:          50,
			},
		},
		sweep:  wrapped,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	started := 0
	for i := 0; i < 6; i++ {
		if sched.startPass(context.Background()) {
			started++
		}
	}
	if started != 1 {
		t.Fatalf("expected exactly one pass to start while busy, got %d", started)
	}
	// The guard is held while the (slow) first pass runs, which is why five of
	// the six synchronous startPass calls above were refused. The concurrency
	// ceiling itself is asserted after the pass drains.
	if !sched.running.Load() {
		t.Fatalf("guard released while a slow pass was still expected in flight")
	}

	// This test owns admission and will not Add until Wait returns. Waiting
	// for one pass is distinct from drain, which permanently closes admission.
	sched.wg.Wait()

	if got := maxInFlight.Load(); got != 1 {
		t.Fatalf("real sweep overlapped: max in-flight = %d", got)
	}
	if !env.rowGone(t, id) || env.versions(t, object) != 0 {
		t.Fatalf("single-flight pass did not collect garbage: gone=%v versions=%d", env.rowGone(t, id), env.versions(t, object))
	}
	// After a pass finishes the guard is released and a new pass can start.
	if !sched.startPass(context.Background()) {
		t.Fatalf("guard was not released after the pass drained")
	}
	sched.drain(context.Background())
	cancel()
}

// (d) A sweep run by the scheduler spares everything the deletion plane refuses:
// a live sealed artifact, a tombstone whose retention has not elapsed, a held
// tombstone, and -- crucially -- another tenant's expired tombstone. A genuinely
// expired tombstone in the swept tenant IS collected, so the survivals below are
// evidence the sweep actually ran, not that it silently did nothing.
func TestGCSchedulerSparesProtectedAndForeignArtifacts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	env := gcSchedTestEnv(t, ctx)
	// Only THIS tenant is in the scheduler's list.
	env.configureScheduler(t, true, env.scope.Tenant)

	// A: live sealed artifact, never tombstoned.
	keepLive := sealArtifact(t, ctx, env.seed, env.scope, artifacts.Binding{ContextID: "ctx-live"}, "live-sealed-bytes")
	objLive := env.objectOf(t, keepLive)

	// B: tombstone with a FUTURE retain_until (retention floor not elapsed).
	keepRetention := sealArtifact(t, ctx, env.seed, env.scope, artifacts.Binding{ContextID: "ctx-ret"}, "retained-tombstone-bytes")
	objRetention := env.objectOf(t, keepRetention)
	if err := env.seed.Delete(ctx, env.scope, keepRetention, "del-ret"); err != nil {
		t.Fatalf("Delete B: %v", err)
	}
	future := time.Now().UTC().Add(time.Hour)
	if err := env.seed.SetRetention(ctx, env.scope, keepRetention, &future); err != nil {
		t.Fatalf("SetRetention B: %v", err)
	}

	// C: tombstone under legal hold.
	keepHold := sealArtifact(t, ctx, env.seed, env.scope, artifacts.Binding{ContextID: "ctx-hold"}, "held-tombstone-bytes")
	objHold := env.objectOf(t, keepHold)
	if err := env.seed.Delete(ctx, env.scope, keepHold, "del-hold"); err != nil {
		t.Fatalf("Delete C: %v", err)
	}
	if err := env.seed.SetLegalHold(ctx, env.scope, keepHold, true); err != nil {
		t.Fatalf("SetLegalHold C: %v", err)
	}
	past := time.Now().UTC().Add(-time.Hour)
	if err := env.seed.SetRetention(ctx, env.scope, keepHold, &past); err != nil {
		t.Fatalf("SetRetention C: %v", err)
	}

	// D: expired tombstone in the swept tenant -- must be collected.
	goneID, objGone := env.tombstoneExpired(t, ctx, env.scope, "expired-tombstone-in-swept-tenant")

	// F: expired tombstone in a FOREIGN tenant not named to the scheduler.
	foreign := contracts.Scope{Tenant: h26TenantPrefix + "-foreign-" + contracts.NewID(), Deployment: "d-f", Subject: "s-f"}
	foreignID, objForeign := env.tombstoneExpired(t, ctx, foreign, "expired-tombstone-in-foreign-tenant")

	s := gcSchedServer(env.pool)
	if err := s.tryMountGCScheduler(ctx); err != nil {
		t.Fatalf("tryMountGCScheduler: %v", err)
	}
	defer func() { cancel(); s.gcDrainOnShutdown(context.Background()) }()

	// Wait until the sweep has demonstrably run (D collected), then check the rest.
	if !waitFor(t, 30*time.Second, func() bool { return env.rowGone(t, goneID) && env.versions(t, objGone) == 0 }) {
		t.Fatalf("scheduler never collected the in-scope expired tombstone: gone=%v versions=%d", env.rowGone(t, goneID), env.versions(t, objGone))
	}
	// Give the same pass any remaining per-tenant work a moment to settle.
	time.Sleep(150 * time.Millisecond)

	if env.rowGone(t, keepLive) || env.versions(t, objLive) == 0 {
		t.Fatalf("live sealed artifact was swept")
	}
	if env.rowGone(t, keepRetention) || env.versions(t, objRetention) == 0 {
		t.Fatalf("tombstone inside its retention window was swept")
	}
	if env.rowGone(t, keepHold) || env.versions(t, objHold) == 0 {
		t.Fatalf("legally held tombstone was swept")
	}
	if env.rowGone(t, foreignID) || env.versions(t, objForeign) == 0 {
		t.Fatalf("a foreign tenant's artifact was swept -- cross-tenant blast radius")
	}
}

func TestGCSchedulerDrainClosesAdmissionAndWaits(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	sched := &gcScheduler{cfg: gcSchedulerConfig{tenants: []string{"drain-test"}, passTimeout: time.Second}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), sweep: func(context.Context, contracts.Scope, artifacts.GCPolicy) (artifacts.GCReport, error) {
		close(started)
		<-finish
		return artifacts.GCReport{}, nil
	}}
	if !sched.startPass(context.Background()) {
		t.Fatal("first pass rejected")
	}
	<-started
	// A cancelled drain still closes admission permanently, without pretending
	// the in-flight pass has finished.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	sched.drain(canceled)
	if sched.startPass(context.Background()) {
		t.Fatal("shutdown admitted another pass")
	}
	waiting := make(chan struct{})
	go func() { sched.drain(context.Background()); close(waiting) }()
	select {
	case <-waiting:
		t.Fatal("drain returned before its in-flight pass")
	default:
	}
	close(finish)
	select {
	case <-waiting:
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not finish")
	}
	if sched.startPass(context.Background()) {
		t.Fatal("completed drain reopened admission")
	}
	sched.drain(context.Background())
}
