package state

// Startup on a pool that can only ever hold ONE connection (MaxConns=1) is the
// shape a constrained deployment gets, and it is the shape the Compute bootstrap
// measured: State.Migrate takes a session-scoped advisory lock on one pooled
// connection and used to run every child migration through the pool. With one
// connection available the child asked for a second that can never exist, and
// startup hung until the caller's deadline instead of ever applying DDL. The
// observation is recorded in the Compute lane's pitfalls note ("State.Migrate
// держит session advisory lock на одном pool connection и запускает дочерние
// migrations через тот же pool; MaxConns=1 исчерпывает pool до deadline") and was
// left open. These legs close it on a fresh, isolated database.

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// singleConnectionPool returns a pool that can hold exactly one connection,
// pointed at this run's isolated database. MaxConns=1 is the assertion: any code
// path that needs two simultaneous connections fails here, not in a customer's
// constrained deployment.
func singleConnectionPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg := isolatedConfig(t)
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("single-connection pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestMigrateSucceedsOnASingleConnectionPool is the direct reproduction. Before
// the fix it fails by deadline (the child migration waits for a connection the
// lock holder never returns); after it, the whole State schema comes up on one
// connection.
func TestMigrateSucceedsOnASingleConnectionPool(t *testing.T) {
	pool := singleConnectionPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	started := time.Now()
	if err := New(pool).Migrate(ctx); err != nil {
		t.Fatalf("Migrate on a MaxConns=1 pool failed after %s: %v "+
			"(a deadline here means a migration asked for a second connection while holding the advisory lock)",
			time.Since(started).Round(time.Millisecond), err)
	}

	// Anti-vacuous: the DDL really ran. Every surface Migrate owns must exist.
	surviving := []string{
		"ahp_state.threads", "ahp_state.runs", "ahp_state.checkpoints",
		"ahp_state.checkpoint_parts", "ahp_state.kv", "ahp_state.stream_events",
		"ahp_state.inbox_events", "ahp_state.intents", "ahp_state.legalhold",
		"ahp_state.run_completions",
	}
	for _, table := range surviving {
		var present bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&present); err != nil {
			t.Fatalf("probe %s: %v", table, err)
		}
		if !present {
			t.Errorf("%s is missing after Migrate on one connection: startup left a half-built schema", table)
		}
	}

	// The pool is still usable and the single connection came back clean: a real
	// round trip on the same one-connection pool.
	scope := contracts.Scope{Tenant: "t-pool1", Deployment: "d-pool1", Subject: "s-pool1"}
	store := New(pool)
	thread, err := store.CreateThread(ctx, scope, "ctx-pool1", "k-pool1")
	if err != nil {
		t.Fatalf("CreateThread after single-connection migration: %v", err)
	}
	if got, err := store.GetThread(ctx, scope, thread.ID); err != nil || got.ID != thread.ID {
		t.Fatalf("GetThread after migration: %v (%+v)", err, got)
	}
}

// TestMigrateReleasesItsAdvisoryLock: a session lock that outlived Migrate would
// hang the next migrator forever. A separate one-connection pool must be able to
// migrate the same database immediately afterwards.
func TestMigrateReleasesItsAdvisoryLock(t *testing.T) {
	first := singleConnectionPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := New(first).Migrate(ctx); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}

	second := singleConnectionPool(t)
	migrateCtx, cancelSecond := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelSecond()
	if err := New(second).Migrate(migrateCtx); err != nil {
		t.Fatalf("second migrator was left waiting by a session advisory lock: %v", err)
	}
}

// TestConcurrentMigratorsQueueInsteadOfDeadlocking is the multi-process startup
// shape: two migrators, each with its own single-connection pool, must both
// finish. The advisory lock is allowed to serialise them; what it must never do
// again is make either one wait on its own pool.
func TestConcurrentMigratorsQueueInsteadOfDeadlocking(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	const migrators = 3
	// The pools are built in the test goroutine: a fixture failure must fail the
	// test, and t.Fatal from inside a worker goroutine is not legal.
	pools := make([]*pgxpool.Pool, migrators)
	for i := range pools {
		pools[i] = singleConnectionPool(t)
	}
	errs := make(chan error, migrators)
	for _, pool := range pools {
		go func(pool *pgxpool.Pool) { errs <- New(pool).Migrate(ctx) }(pool)
	}
	for i := 0; i < migrators; i++ {
		if err := <-errs; err != nil {
			t.Errorf("a concurrent single-connection migrator failed: %v", err)
		}
	}
}
