package integration_test

// Per-run database isolation for the compute live lane — the pattern already
// accepted for internal/identity and internal/state (see
// internal/identity/isolation_test.go, internal/state/isolation_test.go).
// TEST-ONLY: the product is not touched.
//
// WHY THIS EXISTS. The lane connects through the one configured test database,
// so this package and its neighbours (tests/execution above all) run their DDL
// and their mass deletes against the SAME tables. Two suites migrating and
// scanning at once make PostgreSQL pick one of them as the deadlock victim
// (40P01, deadlocks detected in the store's scan queries), and rows one run
// retires are visible to the other: the incarnation-retirement assertion in
// TestB79 saw "v2 not retired" because a foreign run held the table it was
// competing with. Neither is a product behaviour worth reporting; both are a
// dirty shared substrate.
//
// WHAT ISOLATION MEANS HERE. The substrate the lane genuinely respects is the
// DATABASE, so one test-binary run gets a fresh database and everything in it —
// including the real compute-host process the smoke test spawns, which is handed
// the same run DSN — lands there. Two runs in a row, or this package alongside
// another isolated package, are then both clean.
//
// SAFE BY CONSTRUCTION. If provisioning fails for any reason (a role without
// CREATEDB, a managed Postgres that forbids CREATE DATABASE, an unparsable DSN),
// callers fall back to the configured database, so the suite is never redder
// than it was before this file. The live discipline is unchanged: no DSN at all
// is still a hard FAIL, never a skip. The created database is dropped once, at
// process exit, after every test has closed its pool (t.Cleanup) and the smoke
// service has been terminated.
//
// SECRETS. The DSN carries credentials, so no error text produced from parsing
// or connecting is ever printed here; only the kind of failure is reported.

import (
	"context"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

var (
	isolateOnce  sync.Once
	isolateRun   string // run DSN every DB-backed fixture connects through; empty until provisioned or fallen back
	isolateName  string // created database name, empty if none was created
	isolateNote  string // one-line reason, logged by the first caller
	isolateMaint string // maintenance DSN (the originally configured database)
)

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

// dsnWithDatabase rewrites a postgres URL onto another database. A keyword/value
// DSN is not rewritten: this lane is configured with URLs, and guessing how to
// splice a name into an unknown format is how a test ends up connected to a
// database someone else is using.
func dsnWithDatabase(base, database string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return "", contracts.NewError(contracts.ErrInvalid, "isolation: configured DSN is not a postgres URL")
	}
	u.Path = "/" + database
	return u.String(), nil
}

// isolateRunDatabase provisions a fresh database once for this test-binary run.
// isolateRun ends up as the run database's DSN on success and as the configured
// DSN on every fallback, so a caller can never accidentally split across
// databases.
func isolateRunDatabase(baseDSN string) {
	isolateOnce.Do(func() {
		isolateMaint = baseDSN
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cfg, err := pgxpool.ParseConfig(baseDSN)
		if err != nil {
			isolateNote = "fallback (dsn not parsable)"
			isolateRun = baseDSN
			return
		}
		isolateRun = baseDSN // provisional fallback: the configured database
		maint, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			isolateNote = "fallback (maintenance pool)"
			return
		}
		defer maint.Close()
		if err := maint.Ping(ctx); err != nil {
			isolateNote = "fallback (maintenance unreachable)"
			return
		}
		name := "ahp_cmpkg_" + isolateSafeID(contracts.NewID())
		if _, err := maint.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
			isolateNote = "fallback (create db, needs CREATEDB)"
			return
		}
		runDSN, err := dsnWithDatabase(baseDSN, name)
		if err != nil {
			_, _ = maint.Exec(ctx, `DROP DATABASE IF EXISTS "`+name+`"`)
			isolateNote = "fallback (dsn form)"
			return
		}
		isolateRun = runDSN
		isolateName = name
		isolateNote = "provisioned fresh database " + name
	})
}

// isolatedDSN returns this run's database DSN. It is silent about the base DSN
// (credentials) and logs the provisioning outcome once, so a log line proves
// whether the run was isolated or fell back.
func isolatedDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL must be set: the compute lane suite runs live, no offline fallback")
	}
	isolateRunDatabase(dsn)
	if isolateNote != "" {
		t.Log("compute lane database isolation: " + isolateNote)
	}
	if isolateRun == "" {
		return dsn
	}
	return isolateRun
}

// dropRunDatabase removes the per-run database if one was created. Called from
// TestMain after every test closed its pool and the smoke service exited, so no
// connection still references it.
func dropRunDatabase() {
	if isolateName == "" || isolateMaint == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	maint, err := pgxpool.New(ctx, isolateMaint)
	if err != nil {
		return
	}
	defer maint.Close()
	for attempt := 0; attempt < 5; attempt++ {
		if _, err := maint.Exec(ctx, `DROP DATABASE IF EXISTS "`+isolateName+`"`); err == nil {
			return
		}
		time.Sleep(time.Second)
	}
}
