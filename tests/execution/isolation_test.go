package execution_test

// Per-run database isolation for the execution lane — the pattern accepted for
// internal/identity, internal/state and internal/compute/integration (see
// internal/identity/isolation_test.go, internal/state/isolation_test.go).
// TEST-ONLY: the product is not touched.
//
// WHY THIS EXISTS. Every DB-backed fixture here opens its pool through
// requireRealDB, which handed out the one configured database. Running alongside
// internal/compute/integration (go test -p 2) put two suites' migrations,
// bulk deletes and periodic scans on the same tables: PostgreSQL resolved the
// cycle by killing one of them (40P01 deadlocks detected in a scan), and rows a
// neighbour retired or created changed what a test could see. Neither is the
// product's behaviour; both are a shared substrate being used by two tenants.
//
// WHAT ISOLATION MEANS HERE. One test-binary run gets a fresh database, and
// because requireRealDB is the only place this package resolves a DSN, every
// pool in the package lands there.
//
// SAFE BY CONSTRUCTION. Provisioning failure (no CREATEDB, a managed Postgres
// that forbids CREATE DATABASE, an unparsable or non-URL DSN) falls back to the
// configured database, so the suite is never redder than before. The fail-closed
// discipline is untouched: no configured DSN is still a hard FAIL unless the
// explicit AHP_ALLOW_SKIP_REAL_DB=1 opt-out downgrades it to SKIP_EXPLICIT.
//
// SECRETS. The DSN carries credentials; no error produced while parsing or
// connecting to it is printed, only the kind of failure.

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
	isolateRun   string // run DSN every pool connects through; empty until provisioned or fallen back
	isolateName  string // created database name, empty if none was created
	isolateNote  string // one-line reason, logged by the first caller
	isolateMaint string // maintenance DSN (the originally configured database)
)

// executionDSN resolves the configured database the way this lane always did:
// AHP_TEST_DATABASE_URL first, DATABASE_URL as the legacy fallback.
func executionDSN() string {
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

// dsnWithDatabase rewrites a postgres URL onto another database. A keyword/value
// DSN is not rewritten: guessing how to splice a name into an unknown format is
// how a test ends up connected to a database someone else is using.
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
// isolateRun ends up as the fresh database's DSN on success and as the
// configured DSN on every fallback, so no fixture can split across databases.
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
		name := "ahp_exetest_" + isolateSafeID(contracts.NewID())
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

// dropRunDatabase removes the per-run database if one was created, from TestMain
// after every test closed its pool.
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

// TestMain exists only to bound the per-run database to one test-binary run.
func TestMain(m *testing.M) {
	code := m.Run()
	dropRunDatabase()
	os.Exit(code)
}
