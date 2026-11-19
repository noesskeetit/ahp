package artifactsapi

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// PER-RUN DATABASE ISOLATION for the Artifact API suite, for the same reason
// internal/artifacts has it: these fixtures migrate ahp_artifacts and then
// delete, cascade and sweep through HTTP, and against a shared stand database
// another lane's rows are inside this suite's DDL and cascade reach. The
// isolation the plane respects is the database, so one fresh database per test
// binary, dropped at exit, with a bounded shared pool. Provisioning failure
// falls back to the configured database so the suite is never redder than it
// was.

var (
	isolateOnce sync.Once
	apiRunCfg   *pgxpool.Config
	apiRunName  string
	apiMaintDSN string

	poolOnce sync.Once
	pool     *pgxpool.Pool
	poolErr  error
)

func apiDSN() string {
	if dsn := os.Getenv("AHP_TEST_DATABASE_URL"); dsn != "" {
		return dsn
	}
	return os.Getenv("DATABASE_URL")
}

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

func isolateRunDatabase(baseDSN string) {
	isolateOnce.Do(func() {
		apiMaintDSN = baseDSN
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
		name := "ahp_h17api_" + isolateSafeID(contracts.NewID())
		if _, err := maint.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
			return
		}
		fresh := cfg.Copy()
		fresh.ConnConfig.Database = name
		apiRunCfg = fresh
		apiRunName = name
	})
}

// isolatedPool returns the shared, bounded pool of this run's database.
func isolatedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := apiDSN()
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL must be set for artifactsapi integration tests")
	}
	isolateRunDatabase(dsn)
	poolOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cfg := apiRunCfg
		if cfg == nil {
			parsed, err := pgxpool.ParseConfig(dsn)
			if err != nil {
				poolErr = err
				return
			}
			cfg = parsed
		}
		run := cfg.Copy()
		run.MaxConns = 12
		p, err := pgxpool.NewWithConfig(ctx, run)
		if err != nil {
			poolErr = err
			return
		}
		if err := p.Ping(ctx); err != nil {
			p.Close()
			poolErr = err
			return
		}
		pool = p
	})
	if poolErr != nil || pool == nil {
		// The DSN carries credentials, so only the failure type is reported.
		t.Fatalf("artifactsapi: PostgreSQL not reachable (%T)", poolErr)
	}
	return pool
}

func dropRunDatabase() {
	if pool != nil {
		pool.Close()
	}
	if apiRunName == "" || apiMaintDSN == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	maint, err := pgxpool.New(ctx, apiMaintDSN)
	if err != nil {
		return
	}
	defer maint.Close()
	_, _ = maint.Exec(ctx, `DROP DATABASE IF EXISTS "`+apiRunName+`"`)
}

func TestMain(m *testing.M) {
	code := m.Run()
	dropRunDatabase()
	os.Exit(code)
}
