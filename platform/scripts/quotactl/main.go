// Command quotactl publishes and audits provider quotas on the Model Gateway's
// ledger (ahp_model.provider_quotas).
//
// It exists because "is this tenant bounded?" has to be answerable from the
// database, not from a remembered environment variable: the gate treats a tenant
// with no row as unlimited, so the operator needs the pair (tenant served,
// effective limit) and a non-zero exit when any of them is unbounded.
//
//	quotactl -action apply -file infra/quotas/modelrun-tenant-limits.sql
//	quotactl -action sync  -providers infra/model/providers.json
//	quotactl -action audit
//	quotactl -action list
//	quotactl -action set  -tenant t1 -rpm 30 -tpm 200000
//	quotactl -action delete -tenant t1
//
// The DSN comes from -dsn, AHP_QUOTA_DSN, AHP_DATABASE_URL or
// AHP_TEST_DATABASE_URL, in that order, and is never printed.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/model/routing"
)

func main() {
	var (
		dsn       = flag.String("dsn", firstEnv("AHP_QUOTA_DSN", "AHP_DATABASE_URL", "AHP_TEST_DATABASE_URL"), "PostgreSQL DSN (never printed)")
		action    = flag.String("action", "audit", "apply | sync | audit | list | set | delete")
		file      = flag.String("file", "infra/quotas/modelrun-tenant-limits.sql", "seed file for -action apply")
		provider  = flag.String("provider", "modelrun", "provider the limits belong to")
		tenant    = flag.String("tenant", "", "tenant for -action set/delete (\"\" = provider-wide default)")
		rpm       = flag.Int64("rpm", 0, "requests per window for -action set")
		tpm       = flag.Int64("tpm", 0, "tokens per window for -action set")
		window    = flag.Int("window", 60, "window length in seconds")
		unbounded = flag.Bool("unbounded-only", false, "-action audit: print only tenants without a finite limit\n")
		providers = flag.String("providers", "infra/model/providers.json", "-action sync: the provider config whose declared quota to publish")
	)
	flag.Parse()

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "quotactl: no DSN (-dsn or AHP_QUOTA_DSN/AHP_DATABASE_URL/AHP_TEST_DATABASE_URL)")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := openPool(ctx, *dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "quotactl: connect failed (dsn not shown): %v\n", err)
		os.Exit(2)
	}
	defer pool.Close()

	store := model.NewQuotaStore(pool)
	if err := store.Migrate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "quotactl: migrate: %v\n", err)
		os.Exit(2)
	}

	switch *action {
	case "apply":
		if err := applyFile(ctx, pool, *file); err != nil {
			fmt.Fprintf(os.Stderr, "quotactl: apply: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("applied %s\n", *file)
		if err := runAudit(ctx, store, *provider, *unbounded); err != nil {
			fmt.Fprintf(os.Stderr, "quotactl: audit after apply: %v\n", err)
			os.Exit(1)
		}
	case "sync":
		// Publish what the committed provider config DECLARES, so the gate and the
		// file cannot drift: the routing table is reviewed, the database is not.
		tbl, err := routing.LoadFile(*providers)
		if err != nil {
			fmt.Fprintf(os.Stderr, "quotactl: sync: %v\n", err)
			os.Exit(1)
		}
		declared := tbl.DeclaredQuotas()
		if len(declared) == 0 {
			fmt.Fprintf(os.Stderr, "quotactl: sync: %s declares no quota for any provider; refusing to publish an empty set\n", *providers)
			os.Exit(1)
		}
		for _, q := range declared {
			if err := store.Set(ctx, model.ProviderQuota{
				Provider: q.Provider, Tenant: "", RPM: q.RPM, TPM: q.TPM, WindowSeconds: q.WindowSeconds,
			}); err != nil {
				fmt.Fprintf(os.Stderr, "quotactl: sync %s: %v\n", q.Provider, err)
				os.Exit(1)
			}
			fmt.Printf("published provider=%s tenant=<any> rpm=%d tpm=%d window=%d source=%s\n",
				q.Provider, q.RPM, q.TPM, q.WindowSeconds, *providers)
		}
		for _, q := range declared {
			if err := runAudit(ctx, store, q.Provider, *unbounded); err != nil {
				fmt.Fprintf(os.Stderr, "quotactl: sync audit %s: %v\n", q.Provider, err)
				os.Exit(1)
			}
		}
	case "audit":
		if err := runAudit(ctx, store, *provider, *unbounded); err != nil {
			fmt.Fprintf(os.Stderr, "quotactl: %v\n", err)
			os.Exit(1)
		}
	case "list":
		rows, err := store.ConfiguredLimits(ctx, *provider)
		if err != nil {
			fmt.Fprintf(os.Stderr, "quotactl: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("provider|tenant|rpm|tpm|window_seconds")
		for _, q := range rows {
			label := q.Tenant
			if label == "" {
				label = "<any>"
			}
			fmt.Printf("%s|%s|%d|%d|%d\n", q.Provider, label, q.RPM, q.TPM, q.WindowSeconds)
		}
	case "set":
		if err := store.Set(ctx, model.ProviderQuota{
			Provider: *provider, Tenant: *tenant, RPM: *rpm, TPM: *tpm, WindowSeconds: *window,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "quotactl: set: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("set provider=%s tenant=%s rpm=%d tpm=%d window=%d\n", *provider, displayTenant(*tenant), *rpm, *tpm, *window)
	case "delete":
		if err := store.Delete(ctx, *provider, *tenant); err != nil {
			fmt.Fprintf(os.Stderr, "quotactl: delete: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("deleted provider=%s tenant=%s (that tenant is unlimited until a row exists)\n", *provider, displayTenant(*tenant))
	default:
		fmt.Fprintf(os.Stderr, "quotactl: unknown -action %q\n", *action)
		os.Exit(2)
	}
}

// openPool connects with the simple query protocol, which is what lets -action
// apply send a multi-statement seed (including a DO block) in one round trip.
func openPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func applyFile(ctx context.Context, pool *pgxpool.Pool, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, string(raw))
	return err
}

// runAudit prints the audit and refuses (error) when any served tenant is not
// bounded, so a script can gate a redeploy on it.
func runAudit(ctx context.Context, store *model.QuotaStore, provider string, unboundedOnly bool) error {
	audits, err := store.Audit(ctx, provider)
	if err != nil {
		return err
	}
	// The count line comes first and is always printed: a deployment that has
	// served hundreds of tenants has to be summarised before it is enumerated,
	// and the exit code -- not a wall of rows -- is what a script gates on.
	bad := model.NonFiniteTenants(audits)
	fmt.Printf("audit: %d tenant(s) served by %q, %d without a finite limit\n", len(audits), provider, len(bad))
	if !unboundedOnly || len(bad) > 0 {
		fmt.Println("provider|tenant|source|rpm|tpm|window_seconds|finite")
		for _, a := range audits {
			if unboundedOnly && a.Finite {
				continue
			}
			fmt.Printf("%s|%s|%s|%d|%d|%d|%t\n", a.Provider, a.Tenant, a.Source, a.RPM, a.TPM, a.WindowSeconds, a.Finite)
		}
	}
	if len(audits) == 0 {
		return fmt.Errorf("no tenant has been served by provider %q yet: an empty audit is not a bounded deployment", provider)
	}
	if len(bad) > 0 {
		return fmt.Errorf("tenant(s) without a finite effective limit: %s", strings.Join(bad, ", "))
	}
	fmt.Printf("audit ok: %d tenant(s) served, all bounded\n", len(audits))
	return nil
}

func displayTenant(t string) string {
	if t == "" {
		return "<any>"
	}
	return t
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}
