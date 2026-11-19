package artifacts

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// PER-RUN DATABASE ISOLATION for this package.
//
// The artifact suite migrates ahp_artifacts once per fixture and then sweeps,
// cascades and GCs against it. Against the shared stand database that is two
// problems at once: rows another lane left behind become candidates in this
// lane's sweep, and DDL races make a run fail for reasons that have nothing to
// do with the code under test. The isolation the plane actually respects is the
// database - tenant scoping keeps a GC inside one tenant, not inside one
// test - so every test-binary run gets a fresh database and drops it at exit.
//
// SAFE BY CONSTRUCTION: if provisioning fails (a role without CREATEDB, a
// managed Postgres that forbids CREATE DATABASE, an unparseable DSN) every
// helper falls back to the configured database, so the suite is never redder
// than it was before this file. Each fixture keeps its own skip semantics.
var (
	artIsolateOnce sync.Once
	artIsolateDSN  string // run DSN: the fresh database, or the configured one on fallback
	artIsolateName string // created database name, empty if none was created
	artMaintDSN    string // the originally configured database, for the drop
)

// artifactsDSN resolves the configured database the way these fixtures always
// did: AHP_TEST_DATABASE_URL first, DATABASE_URL as the legacy fallback.
func artifactsDSN() string {
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

// isolateRunDatabase provisions one fresh database per test-binary run. The
// credential-carrying DSN is never printed; only the fact of a failure is.
func isolateRunDatabase(baseDSN string) {
	artIsolateOnce.Do(func() {
		artIsolateDSN = baseDSN
		artMaintDSN = baseDSN
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cfg, err := pgxpool.ParseConfig(baseDSN)
		if err != nil {
			return
		}
		maint, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			return
		}
		defer maint.Close()
		if err := maint.Ping(ctx); err != nil {
			return
		}
		name := "ahp_h17art_" + isolateSafeID(contracts.NewID())
		if _, err := maint.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
			return
		}
		// ConnConfig.ConnString() is cached at parse time (pgx conn.go), so the
		// fresh database is selected through ConnConfig.Database - which pgx
		// sends as the startup "database" parameter - rather than by splicing a
		// new DSN string together.
		fresh := cfg.Copy()
		fresh.ConnConfig.Database = name
		artRunCfg = fresh
		artIsolateName = name
	})
}

// artRunCfg is the pool config of this run's database (nil only when the base
// DSN could not be parsed, in which case every fixture skips itself as before).
var artRunCfg *pgxpool.Config

// isolatedPool returns the pool of this run's database, created once per test
// binary and shared by every fixture. A pool per fixture would be natural in a
// suite this size, and it is also how the suite managed to exhaust the stand's
// connection limit: the GC fixtures call it from inside helpers and goroutines,
// so it is one bounded pool (MaxConns below) with a run-scoped Close instead.
func isolatedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := artifactsDSN()
	if dsn == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	isolateRunDatabase(dsn)
	return sharedRunPool(t)
}

var (
	sharedPoolOnce sync.Once
	sharedPool     *pgxpool.Pool
	sharedPoolErr  error
)

func sharedRunPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	sharedPoolOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cfg := artRunCfg
		if cfg == nil {
			parsed, err := pgxpool.ParseConfig(artIsolateDSN)
			if err != nil {
				sharedPoolErr = err
				return
			}
			cfg = parsed
		}
		run := cfg.Copy()
		// Bounded: the GC suite runs sweeps and helpers concurrently, and an
		// unbounded pool on a many-core box outruns the stand's max_connections.
		run.MaxConns = 12
		pool, err := pgxpool.NewWithConfig(ctx, run)
		if err != nil {
			sharedPoolErr = err
			return
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			sharedPoolErr = err
			return
		}
		sharedPool = pool
	})
	if sharedPoolErr != nil || sharedPool == nil {
		t.Skipf("artifacts: PostgreSQL not reachable (%T)", sharedPoolErr)
	}
	return sharedPool
}

// dropRunDatabase removes the per-run database, once, after every test has
// closed its pool.
func dropRunDatabase() {
	if sharedPool != nil {
		sharedPool.Close()
	}
	if artIsolateName == "" || artMaintDSN == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	maint, err := pgxpool.New(ctx, artMaintDSN)
	if err != nil {
		return
	}
	defer maint.Close()
	_, _ = maint.Exec(ctx, `DROP DATABASE IF EXISTS "`+artIsolateName+`"`)
}

// TestMain bounds the fresh database to one test-binary invocation.
func TestMain(m *testing.M) {
	code := m.Run()
	dropRunDatabase()
	os.Exit(code)
}
