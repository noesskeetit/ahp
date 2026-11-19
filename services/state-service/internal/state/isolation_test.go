package state

// Per-run database isolation for the state package suite (ported from the
// tests/identity acceptance suite, lane vm2-a2a-multinode, and mirrored in
// internal/identity/isolation_test.go). TEST-ONLY — the product is not touched.
//
// WHY THIS EXISTS. Every fixture here migrates in place against whichever
// database AHP_TEST_DATABASE_URL names, and this package's migrations are DDL
// (CREATE SCHEMA/TABLE/INDEX, ALTER TABLE) that takes AccessExclusive locks.
// On a shared development database that DDL races the DML of any other run of
// this suite — a second run of the same package, another worktree, a `go test`
// retry — and PostgreSQL resolves the race by aborting one side:
//
//	errorcodes_test.go: Migrate: state: migrate: ERROR: deadlock detected (SQLSTATE 40P01)
//	kv_test.go:         PutKV(user-1): UNAVAILABLE: state: the database aborted this transaction (SQLSTATE 40P01)
//
// Both are the same event: 40P01/40001 is an availability abort to
// internal/state/store.go (it wore CONFLICT until the lock-order fix, which made a
// lost lock cycle look like a decision about the request), so the same test passes,
// then fails, then passes on the SAME binary with the SAME code. The isolation the
// product respects is the DATABASE, so a test-binary run gets its own fresh
// database and the DDL has no other tenant to deadlock with.
//
// SHARED BY EVERY ENTRY POINT. isolatedPool, isolatedConfig and laneDSN all
// resolve through isolateRunDatabase (sync.Once), so the store, the crash
// test's blocker connection and its victim pool are guaranteed to sit in the
// same database — a test that split across two databases would block nothing
// and measure nothing.
//
// SAFE BY CONSTRUCTION. If provisioning fails (a role without CREATEDB, a
// managed Postgres that forbids CREATE DATABASE, a parse error), every helper
// falls back to the originally configured database, so the suite is never
// redder than it was before this file. Each fixture keeps its own
// skip/fail-without-a-database semantics. The created database is dropped once,
// at process exit, after every test has closed its pool (via t.Cleanup).

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

var (
	isolateOnce sync.Once
	isolateCfg  *pgxpool.Config // run pool config; nil only if the base DSN could not be parsed
	isolateName string          // created database name, empty if none was created
	isolateNote string          // one-line reason, logged by the first caller
	isolateDSN  string          // maintenance DSN (the originally configured database)
)

// stateDSN resolves the configured database the way these fixtures always did:
// AHP_TEST_DATABASE_URL first, DATABASE_URL as the legacy fallback.
func stateDSN() string {
	if dsn := os.Getenv("AHP_TEST_DATABASE_URL"); dsn != "" {
		return dsn
	}
	return os.Getenv("DATABASE_URL")
}

// isolateSafeID keeps a generated name to characters that are safe to splice
// into CREATE DATABASE, which cannot take a bind parameter.
func isolateSafeID(s string) string {
	var b []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			b = append(b, c)
		}
	}
	return string(b)
}

// isolateRunDatabase provisions a fresh database once for this test-binary run
// and records the config every caller should use. isolateCfg is always non-nil
// after a successful parse — the fresh-database config on success, the original
// on any fallback — so a caller can never accidentally split across databases.
func isolateRunDatabase(baseDSN string) {
	isolateOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cfg, err := pgxpool.ParseConfig(baseDSN)
		if err != nil {
			isolateNote = "fallback (parse dsn)" // DSN carries credentials: never printed
			return                               // isolateCfg stays nil; callers fall back to the configured database
		}
		isolateCfg = cfg // provisional fallback: the originally configured database
		isolateDSN = baseDSN
		maint, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			isolateNote = "fallback (maint connect)"
			return
		}
		defer maint.Close()
		if err := maint.Ping(ctx); err != nil {
			isolateNote = "fallback (maint unreachable)"
			return
		}
		name := "ahp_sttest_" + isolateSafeID(contracts.NewID())
		if _, err := maint.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
			isolateNote = "fallback (create db, needs CREATEDB)"
			return
		}
		// ConnConfig.ConnString() is cached at parse time (pgx conn.go), so a new
		// DSN string would still point at the original database. Point the pool at
		// the fresh database through ConnConfig.Database, which pgx sends as the
		// startup "database" parameter (pgconn startup params).
		runCfg := cfg.Copy()
		runCfg.ConnConfig.Database = name
		isolateCfg = runCfg
		isolateName = name
		isolateNote = "provisioned fresh database " + name
	})
}

// isolatedPool opens a pool bound to this run's isolated database, skipping when
// no database is configured (the fixtures' own pre-existing behaviour), and
// registers pool.Close with t.Cleanup.
func isolatedPool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	dsn := stateDSN()
	if dsn == "" {
		t.Skip("set AHP_TEST_DATABASE_URL or DATABASE_URL to run the state tests")
	}
	isolateRunDatabase(dsn)
	cfg := isolateCfg
	if cfg == nil {
		t.Skip("state: the configured database could not be parsed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Skipf("state: PostgreSQL not reachable (%v)", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("state: PostgreSQL not reachable (%v)", err)
	}
	t.Cleanup(pool.Close)
	return pool, isolateNote
}

// isolatedConfig returns a private copy of this run's database config for
// callers that must build their own connection or pool (the crash/recovery lane
// needs a blocker connection and a single-connection victim pool). It fails
// rather than skipping: that lane's test asserts it ran live, exactly as its
// laneDSN helper did before.
func isolatedConfig(t *testing.T) *pgxpool.Config {
	t.Helper()
	dsn := stateDSN()
	if dsn == "" {
		t.Fatal("set AHP_TEST_DATABASE_URL to run the crash/recovery test")
	}
	isolateRunDatabase(dsn)
	if isolateCfg == nil {
		// pgconn.ParseConfigError embeds the connection string by design, so the
		// credential-carrying text is never printed - only the fact of failure.
		t.Fatal("the lane DSN could not be parsed (not printed: it carries credentials)")
	}
	return isolateCfg.Copy()
}

// dropRunDatabase removes the per-run database if one was created. Run from
// TestMain after every test has closed its pool (via t.Cleanup), so no
// connection still references it.
func dropRunDatabase() {
	if isolateName == "" {
		return
	}
	base := isolateDSN
	if base == "" {
		base = stateDSN()
	}
	if base == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	maint, err := pgxpool.New(ctx, base)
	if err != nil {
		return
	}
	defer maint.Close()
	_, _ = maint.Exec(ctx, `DROP DATABASE IF EXISTS "`+isolateName+`"`)
}

// TestMain runs the suite and then tears down the per-run database. It exists
// only to bound the fresh database to one test-binary invocation.
func TestMain(m *testing.M) {
	code := m.Run()
	dropRunDatabase()
	os.Exit(code)
}
