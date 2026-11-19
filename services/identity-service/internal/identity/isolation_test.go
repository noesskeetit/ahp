package identity

// Per-run database isolation for the identity package suite (ported from the
// tests/identity acceptance suite, lane vm2-a2a-multinode). TEST-ONLY — the
// product is not touched.
//
// WHY THIS EXISTS. identity.New has no schema parameter and the schema name is
// baked into the DDL (internal/identity/store.go: `CREATE SCHEMA IF NOT EXISTS
// ahp_identity`), so package tests cannot be isolated by schema. Grants,
// contexts and tokens are keyed on ids that are unique across the whole schema,
// and some ids in this package are literal strings ("ctx-child", "task-create",
// ...). Against a shared development database the second run therefore collides
// with rows the first run left behind — the failure is not a product bug but a
// dirty substrate (e.g. "context id belongs to another scope" from Delegate,
// because a previous run's tenant already owns that context id). The isolation
// the product actually respects is the DATABASE, so each test-binary run gets a
// fresh database and starts clean; two runs in a row on the same configured
// server are then both green.
//
// SHARED BY EVERY ENTRY POINT. newStore is the only pool this package opens, so
// every DB-backed test in the package goes through isolatedPool and lands in
// the same fresh database.
//
// SAFE BY CONSTRUCTION. If provisioning fails for any reason (a role without
// CREATEDB, a managed Postgres that forbids CREATE DATABASE, a parse error),
// callers fall back to the original configured database, so the suite is never
// redder than it was before this file. The created database is dropped once, at
// process exit, after every test has closed its pool (via t.Cleanup).

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
			isolateNote = "fallback (parse dsn): " + err.Error()
			return // isolateCfg stays nil; isolatedPool falls back to baseDSN
		}
		isolateCfg = cfg // provisional fallback: the originally configured database
		isolateDSN = baseDSN
		maint, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			isolateNote = "fallback (maint connect): " + err.Error()
			return
		}
		defer maint.Close()
		if err := maint.Ping(ctx); err != nil {
			isolateNote = "fallback (maint unreachable): " + err.Error()
			return
		}
		// sanitizeID keeps the name to [a-z0-9]; CREATE DATABASE cannot take a
		// bind parameter, so the name is composed only from safe characters.
		name := "ahp_idpkg_" + sanitizeID(contracts.NewID())
		if _, err := maint.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
			isolateNote = "fallback (create db, needs CREATEDB): " + err.Error()
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

// isolatedPool opens a pool bound to this run's isolated database. It skips
// when no database is configured, exactly as the pre-isolation helper did, and
// registers pool.Close with t.Cleanup.
func isolatedPool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("identity: AHP_TEST_DATABASE_URL not set")
	}
	isolateRunDatabase(dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var (
		pool *pgxpool.Pool
		err  error
	)
	if isolateCfg != nil {
		pool, err = pgxpool.NewWithConfig(ctx, isolateCfg)
	} else {
		pool, err = pgxpool.New(ctx, dsn)
	}
	if err != nil {
		t.Skipf("identity: PostgreSQL not reachable (%v)", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("identity: PostgreSQL not reachable (%v)", err)
	}
	t.Cleanup(pool.Close)
	return pool, isolateNote
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
		base = os.Getenv("AHP_TEST_DATABASE_URL")
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
