//go:build ahp_data

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// This file mounts the background garbage-collection scheduler for the storage
// plane. The deletion plane itself (internal/artifacts.GC, mounted as an HTTP
// face by the data role) only runs when somebody calls it: retention is then
// enforced for as long as an operator happens to remember to poke it. The PDF's
// operator-facing obligation ("retention и удаление по чату, сотруднику и tenant",
// с. 48) is a standing one, so the sweep has to run on its own clock too.
//
// The mount mirrors the accepted trigger dispatcher (mount_w2a.go): an explicit
// operator flag that is OFF by default, an interval read from the environment,
// and a single background goroutine that stops when the context is cancelled.
// With AHP_ENABLE_GC_SCHEDULER unset the mount is never reached and the default
// build is byte-identical; the untagged build resolves to a stub that reports
// the feature is not built.
//
// Two properties distinguish a garbage collector from a trigger ticker and shape
// the loop below:
//
//   - Overlap: a slow sweep must never start a second sweep of the same tenant
//     while the first is still running. The engine is already safe against two
//     concurrent sweeps (every collection is a guarded single-row statement), but
//     a scheduler that piled passes up would still waste work and could outlive
//     its interval, so a single-flight guard refuses a pass while one is in
//     flight and a dropped tick simply means the next pass waits.
//   - Graceful shutdown: cancelling the context stops the scheduler from starting
//     new passes, but a pass already running is not abandoned half-way -- that
//     would be the exact crash-window the collector is written to survive (row
//     gone, bytes not yet). The in-flight pass therefore runs on a context
//     detached from cancellation, bounded by its own timeout, and the role waits
//     for it before the process lets go of the connection pool.

// gcSchedulerIdentity is the scope Deployment/Subject the scheduler sweeps under.
// RunOnce takes the swept tenant from the verified scope and nowhere else, and
// uses only the tenant for the sweep itself -- Deployment and Subject appear
// solely in the audit trail, where they name the scheduler as the actor rather
// than impersonating a workload.
const gcSchedulerIdentity = "gc-scheduler"

// gcSchedulerConfig is the operator-supplied shape of one scheduler instance.
type gcSchedulerConfig struct {
	tenants      []string
	interval     time.Duration
	passTimeout  time.Duration
	policy       artifacts.GCPolicy
	sweepSubject string // audit identity (Deployment/Subject); defaults to gcSchedulerIdentity
}

// tryMountGCScheduler builds an isolated artifact store for the sweep and starts
// its ticker. It is untagged-safe: in the untagged build the stub (mount_gc_stub.go)
// answers errFeatureNotBuilt instead.
//
// The store is deliberately NOT the data plane's store. The data plane's
// authorizer allowlists the actions the artifact tier performs over user data
// (upload, seal, read, delete, ...) and, by design, does not include "gc" --
// a request handler must not be able to ask that store to sweep. The scheduler
// therefore opens its own store whose authorizer admits exactly one action,
// "gc", and nothing else; RunOnce then authorizes once per pass against it.
// Construction reuses the same object-store environment the data plane uses so
// an operator configures one bucket, not two, and the same connection pool.
func (s *server) tryMountGCScheduler(ctx context.Context) error {
	if envOr("AHP_ENABLE_GC_SCHEDULER", "") != "1" {
		// Off by default: nothing to build, nothing to start. Checked here as
		// well as in serveAll so the mount is self-guarding and testable in
		// isolation without driving the whole composition root.
		return nil
	}
	cfg, err := s.gcSchedulerConfig()
	if err != nil {
		return err
	}
	if len(cfg.tenants) == 0 {
		// The operator enabled the scheduler but named no tenant. A collector
		// with an unbounded blast radius is a security property the engine itself
		// refuses (RunOnce sweeps the caller's tenant, never "all"), so the honest
		// behaviour is to stay idle and say so, not to guess a tenant list.
		s.logger.Warn("gc scheduler enabled but no tenants named (set AHP_GC_SCHEDULER_TENANTS); staying idle")
		return nil
	}

	dataPool, readPool, closePools, err := checkpointOwnerPools(ctx, s.pool)
	if err != nil {
		return err
	}
	keepPools := false
	defer func() {
		if !keepPools {
			closePools()
		}
	}()
	owner := &server{pool: dataPool, logger: s.logger}
	store, err := owner.newGCSweepStore(ctx)
	if err != nil {
		return err
	}
	// The collector runs on the same durable answer the HTTP face gets: a row
	// some live plane still names is never garbage, with or without a pin and
	// past any retention floor. Failures inside the probe protect the batch.
	liveness := newStateLiveness(readPool)
	artifacts.WithReferenceKeeper(liveness)(store)
	artifacts.WithCheckpointSettlement(state.New(readPool).CheckpointOperationSettled)(store)
	gc, err := artifacts.NewGC(store,
		artifacts.WithLiveness(liveness),
		artifacts.WithReferenceScan(liveness))
	if err != nil {
		return fmt.Errorf("gc scheduler engine: %w", err)
	}

	sched := &gcScheduler{
		cfg:    cfg,
		sweep:  gc.RunOnce,
		logger: s.logger,
	}
	// Detach from cancellation the moment the pass starts, and hand the process a
	// drain hook so shutdown waits for an in-flight pass instead of dropping it
	// (and the still-open pool it uses) mid-collect.
	s.gcDrainOnShutdown = func(ctx context.Context) { sched.drain(ctx); closePools() }
	keepPools = true

	go sched.loop(ctx)
	s.logger.Info("gc scheduler mounted",
		"tenants", len(cfg.tenants), "interval", cfg.interval.String(), "pass_timeout", cfg.passTimeout.String())
	return nil
}

// gcScheduler runs one guarded sweep pass per tick over a fixed tenant list.
type gcScheduler struct {
	cfg   gcSchedulerConfig
	sweep func(context.Context, contracts.Scope, artifacts.GCPolicy) (artifacts.GCReport, error)
	// sweep is a seam: production wires gc.RunOnce; a test can wrap it to make
	// a pass slow enough to observe the single-flight guard. The wrapped call
	// still runs the real sweep, so the guard is proven against real Postgres.
	logger *slog.Logger

	running   atomic.Bool    // single-flight: at most one pass in flight
	wg        sync.WaitGroup // in-flight passes, drained on shutdown
	admission sync.Mutex     // closes Add before any shutdown Wait
	stopping  bool
}

// loop is the ticker: one goroutine, stops on ctx, never overlaps itself.
func (g *gcScheduler) loop(ctx context.Context) {
	t := time.NewTicker(g.cfg.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.startPass(ctx)
		}
	}
}

// startPass launches one sweep pass unless one is already running. The guard is
// the whole overlap story: a tick that arrives during a slow pass is a no-op, so
// the next pass begins only after the current one has released the guard.
//
// The pass runs on a context detached from cancellation and bounded by
// passTimeout: shutdown stops the loop from scheduling further passes but does
// not abandon this one mid-way, and passTimeout keeps a wedged pass from living
// forever after cancellation.
func (g *gcScheduler) startPass(parent context.Context) bool {
	g.admission.Lock()
	defer g.admission.Unlock()
	if g.stopping || parent.Err() != nil {
		return false
	}
	if !g.running.CompareAndSwap(false, true) {
		g.logger.Debug("gc scheduler pass skipped: previous pass still running")
		return false
	}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		defer g.running.Store(false)
		passCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), g.cfg.passTimeout)
		defer cancel()
		g.runPass(passCtx)
	}()
	return true
}

// runPass sweeps every configured tenant once. It is called only by startPass,
// which holds the single-flight guard, so a tenant is never swept twice at once.
// A per-tenant sweep error is logged and the pass moves on -- one tenant's bad
// object-store call must not stop the others.
func (g *gcScheduler) runPass(ctx context.Context) {
	tenant := g.cfg.sweepSubject
	if tenant == "" {
		tenant = gcSchedulerIdentity
	}
	for _, t := range g.cfg.tenants {
		if ctx.Err() != nil {
			g.logger.Warn("gc scheduler pass cut short", "reason", ctx.Err().Error())
			return
		}
		scope := contracts.Scope{Tenant: t, Deployment: tenant, Subject: tenant}
		rep, err := g.sweep(ctx, scope, g.cfg.policy)
		if err != nil {
			g.logger.Warn("gc scheduler sweep failed", "tenant", t, "err", err)
			continue
		}
		if rep.PurgedTombstoned > 0 || rep.CollectedStaged > 0 || rep.VersionsRemoved > 0 {
			g.logger.Info("gc scheduler swept",
				"tenant", t,
				"purged_tombstoned", rep.PurgedTombstoned,
				"collected_staged", rep.CollectedStaged,
				"versions_removed", rep.VersionsRemoved,
				"skipped_sealed", rep.SkippedSealed,
				"skipped_live_reference", rep.SkippedLive,
				"skipped_legal_hold", rep.SkippedHold,
			)
		}
	}
}

// drain waits for a pass already in flight so shutdown does not drop it (and the
// pool it still needs) mid-collect. Bounded by passTimeout so a wedged pass
// cannot hold the process open forever.
func (g *gcScheduler) drain(ctx context.Context) {
	g.admission.Lock()
	g.stopping = true
	g.admission.Unlock()
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(g.cfg.passTimeout):
		g.logger.Warn("gc scheduler drain timed out with a pass still running")
	case <-ctx.Done():
	}
}

// gcSchedulerConfig reads the scheduler's shape from the environment. Every
// duration falls back to a production-conservative default when unset or
// malformed: a typo must not silently turn a collector into a shredder.
func (s *server) gcSchedulerConfig() (gcSchedulerConfig, error) {
	cfg := gcSchedulerConfig{
		tenants:      envListOr("AHP_GC_SCHEDULER_TENANTS"),
		interval:     15 * time.Minute,
		passTimeout:  10 * time.Minute,
		sweepSubject: gcSchedulerIdentity,
	}
	if raw := strings.TrimSpace(envOr("AHP_GC_SCHEDULER_INTERVAL", "")); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return cfg, fmt.Errorf("gc scheduler: bad AHP_GC_SCHEDULER_INTERVAL %q", raw)
		}
		cfg.interval = d
	}
	if raw := strings.TrimSpace(envOr("AHP_GC_SCHEDULER_PASS_TIMEOUT", "")); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return cfg, fmt.Errorf("gc scheduler: bad AHP_GC_SCHEDULER_PASS_TIMEOUT %q", raw)
		}
		cfg.passTimeout = d
	}
	if raw := strings.TrimSpace(os.Getenv("AHP_GC_SCHEDULER_STAGED_GRACE")); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			return cfg, fmt.Errorf("gc scheduler: bad AHP_GC_SCHEDULER_STAGED_GRACE %q", raw)
		}
		cfg.policy.StagedGrace = d
	}
	if raw := strings.TrimSpace(os.Getenv("AHP_GC_SCHEDULER_TOMBSTONE_RETENTION")); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			return cfg, fmt.Errorf("gc scheduler: bad AHP_GC_SCHEDULER_TOMBSTONE_RETENTION %q", raw)
		}
		cfg.policy.TombstoneRetention = d
	}
	if raw := strings.TrimSpace(os.Getenv("AHP_GC_SCHEDULER_ORPHAN_GRACE")); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			return cfg, fmt.Errorf("gc scheduler: bad AHP_GC_SCHEDULER_ORPHAN_GRACE %q", raw)
		}
		cfg.policy.OrphanGrace = d
	}
	if envOr("AHP_GC_SCHEDULER_COLLECT_UNPINNED_VERSIONS", "") == "1" {
		cfg.policy.CollectUnpinnedVersions = true
	}
	if dep := strings.TrimSpace(envOr("AHP_GC_SCHEDULER_IDENTITY", "")); dep != "" {
		cfg.sweepSubject = dep
	}
	return cfg, nil
}

// newGCSweepStore builds the scheduler's own artifact store over the configured
// object store, admitting only the "gc" action. It runs the idempotent
// migration so the sweep finds the metadata schema in place.
func (s *server) newGCSweepStore(ctx context.Context) (*artifacts.Store, error) {
	raw := envOr("AHP_S3_ENDPOINT", "")
	if raw == "" {
		return nil, fmt.Errorf("gc scheduler: AHP_S3_ENDPOINT is required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("gc scheduler: bad AHP_S3_ENDPOINT")
	}
	bucket := envOr("AHP_S3_BUCKET", "")
	if bucket == "" {
		return nil, fmt.Errorf("gc scheduler: AHP_S3_BUCKET is required")
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(envOr("AHP_MINIO_ROOT_USER", ""), envOr("AHP_MINIO_ROOT_PASSWORD", ""), ""),
		Secure: u.Scheme == "https",
		Region: envOr("AHP_S3_REGION", ""),
	})
	if err != nil {
		return nil, fmt.Errorf("gc scheduler: object store: %w", err)
	}
	// Fail-closed authorizer: the sweep store can do one thing and nothing else.
	// scope.Validate keeps a malformed tenant from ever reaching a query.
	gcAuthorize := func(_ context.Context, scope contracts.Scope, action, _ string) error {
		if err := scope.Validate(); err != nil {
			return err
		}
		if action != "gc" {
			return contracts.NewError(contracts.ErrForbidden, "ahp: gc scheduler store admits only the gc action")
		}
		return nil
	}
	store, err := artifacts.New(s.pool, client, bucket, gcAuthorize)
	if err != nil {
		return nil, fmt.Errorf("gc scheduler: store: %w", err)
	}
	if err := store.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("gc scheduler: migrate: %w", err)
	}
	return store, nil
}
