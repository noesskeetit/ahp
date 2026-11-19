package model

// The committed operator seed (infra/quotas/modelrun-tenant-limits.sql) is
// configuration the platform is accepted on, so it gets the same treatment as
// code: applied for real against PostgreSQL, and refused when it would leave a
// tenant unbounded.
//
// Legs:
//   - the file applies cleanly and publishes a FINITE limit for every tenant it
//     names, with the acceptance tenant winning over the provider-wide default;
//   - a tenant that only inherits the default is still bounded (that is what the
//     floor is for), and the audit says where each limit came from;
//   - re-applying is a no-op (idempotent UPSERTs), because a re-deploy re-applies;
//   - the file's own self-check refuses a seed that would leave an observed tenant
//     unbounded, and the refusal is atomic -- the previous limits survive;
//   - a tenant's own row is what ENFORCES, not the default: a tight tenant limit
//     refuses at the tenant's bound even while the default would allow more.
//
// Isolation: a fresh database per test where CREATE DATABASE is permitted, so
// publishing the real provider's rows here cannot disturb other legs. When the
// server forbids CREATE DATABASE the configured database is used instead, which
// never makes the suite redder than it already is.

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

const quotaSeedRelPath = "../../infra/quotas/modelrun-tenant-limits.sql"

// quotaSeedDatabase gives one test its own database when it can have one.
// Returns a connected pool whose ahp_model schema is empty.
func quotaSeedDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	requireModelDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	base := os.Getenv("AHP_TEST_DATABASE_URL")
	dsn := base
	if u, err := url.Parse(base); err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		name := "h21seed" + randHex(5)
		// Only [a-z0-9] reaches CREATE DATABASE, which takes no bind parameter.
		safe := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
				return r
			}
			return -1
		}, name)
		if safe != "" {
			if maint, merr := pgxpool.New(ctx, base); merr == nil {
				_, cerr := maint.Exec(ctx, `CREATE DATABASE "`+safe+`"`)
				maint.Close()
				if cerr == nil {
					u.Path = "/" + safe
					dsn = u.String()
					t.Cleanup(func() {
						c, cc := context.WithTimeout(context.Background(), 20*time.Second)
						defer cc()
						m, e := pgxpool.New(c, base)
						if e == nil {
							_, _ = m.Exec(c, `DROP DATABASE IF EXISTS "`+safe+`" WITH (FORCE)`)
							m.Close()
						}
					})
				} else {
					t.Logf("quota seed leg: CREATE DATABASE not permitted here, using the configured database")
				}
			}
		}
	}

	// Simple protocol: the seed is a multi-statement script (including a DO block)
	// and must go over the wire as one query, exactly like scripts/quotactl sends it.
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func readQuotaSeed(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(quotaSeedRelPath))
	if err != nil {
		t.Fatalf("read committed seed: %v", err)
	}
	return string(raw)
}

func limitsByTenant(t *testing.T, store *QuotaStore, provider string) map[string]ProviderQuota {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rows, err := store.ConfiguredLimits(ctx, provider)
	if err != nil {
		t.Fatalf("configured limits: %v", err)
	}
	out := map[string]ProviderQuota{}
	for _, q := range rows {
		out[q.Tenant] = q
	}
	return out
}

func TestProviderQuotaSeedBoundsEveryTenantItNames(t *testing.T) {
	pool := quotaSeedDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Mirror scripts/quotactl: the gateway owns its schema, the seed is configuration.
	store := NewQuotaStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("quota migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, readQuotaSeed(t)); err != nil {
		t.Fatalf("committed seed must apply cleanly: %v", err)
	}

	got := limitsByTenant(t, store, "modelrun")
	want := map[string][2]int64{
		"":           {20, 120000}, // provider-wide floor, finite on purpose
		"djourney":   {30, 200000}, // acceptance tenant
		"quotaprobe": {5, 20000},   // verifier probes: tight by design
		"dh21probe":  {5, 20000},
		"h20probe":   {5, 20000},
	}
	for tenant, w := range want {
		q, ok := got[tenant]
		if !ok {
			t.Fatalf("seed published no limit row for tenant %q; rows: %d", tenant, len(got))
		}
		if q.RPM != w[0] || q.TPM != w[1] {
			t.Fatalf("tenant %q limits = rpm %d / tpm %d, want %d / %d", tenant, q.RPM, q.TPM, w[0], w[1])
		}
		if q.RPM <= 0 || q.TPM <= 0 {
			t.Fatalf("tenant %q has an unbounded dimension (rpm=%d tpm=%d): a seed row of 0 means unlimited, not \"configured\"", tenant, q.RPM, q.TPM)
		}
	}

	// The acceptance tenant must resolve to its OWN row, and a tenant nobody named
	// must resolve to the finite floor rather than to nothing.
	own, found, err := store.Limits(ctx, "modelrun", "djourney")
	if err != nil || !found || own.Tenant != "djourney" {
		t.Fatalf("djourney effective limit = %+v found=%v err=%v, want its own row", own, found, err)
	}
	inherited, found, err := store.Limits(ctx, "modelrun", "a-tenant-nobody-seeded")
	if err != nil || !found {
		t.Fatalf("un-named tenant limit found=%v err=%v, want the provider-wide floor", found, err)
	}
	if inherited.RPM <= 0 || inherited.TPM <= 0 {
		t.Fatalf("floor is unbounded: %+v", inherited)
	}

	// Audit view: after real admissions for both kinds of tenant, every served
	// tenant is finite and the source column says which row decided.
	for _, tenant := range []string{"djourney", "a-tenant-nobody-seeded"} {
		if _, err := store.Admit(ctx, "modelrun", tenant, "seed-op-"+tenant, 1, 100); err != nil {
			t.Fatalf("admit for %q: %v", tenant, err)
		}
	}
	audits, err := store.Audit(ctx, "modelrun")
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	byTenant := map[string]TenantQuotaAudit{}
	for _, a := range audits {
		byTenant[a.Tenant] = a
	}
	if a := byTenant["djourney"]; a.Source != QuotaSourceTenant || !a.Finite {
		t.Fatalf("djourney audit = %+v, want source=tenant finite=true", a)
	}
	if a := byTenant["a-tenant-nobody-seeded"]; a.Source != QuotaSourceProviderDefault || !a.Finite {
		t.Fatalf("inheriting tenant audit = %+v, want source=provider-default finite=true", a)
	}
	if bad := NonFiniteTenants(audits); len(bad) > 0 {
		t.Fatalf("audit found unbounded tenants after a clean seed: %v", bad)
	}
}

func TestProviderQuotaSeedReApplyIsANoOp(t *testing.T) {
	pool := quotaSeedDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seed := readQuotaSeed(t)
	if _, err := pool.Exec(ctx, seed); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	store := NewQuotaStore(pool)
	before := limitsByTenant(t, store, "modelrun")

	// A window counter is real spend; a re-apply must not disturb it either.
	if _, err := store.Admit(ctx, "modelrun", "djourney", "reapply-op", 1, 50); err != nil {
		t.Fatalf("admit: %v", err)
	}
	if _, err := pool.Exec(ctx, seed); err != nil {
		t.Fatalf("second apply must succeed: %v", err)
	}
	after := limitsByTenant(t, store, "modelrun")
	if len(before) != len(after) {
		t.Fatalf("re-apply changed row count %d -> %d", len(before), len(after))
	}
	for tenant, q := range before {
		g := after[tenant]
		if g.RPM != q.RPM || g.TPM != q.TPM || g.WindowSeconds != q.WindowSeconds {
			t.Fatalf("re-apply changed tenant %q: %+v -> %+v", tenant, q, g)
		}
	}
}

func TestQuotaAuditNamesAnUnboundedTenant(t *testing.T) {
	pool := quotaSeedDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, readQuotaSeed(t)); err != nil {
		t.Fatalf("seed apply: %v", err)
	}
	store := NewQuotaStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// A row exists but leaves rpm unbounded. This is the shape an operator can
	// accidentally publish ("rpm 0 = I did not set it"), and the audit must call it
	// unbounded rather than "configured".
	if err := store.Set(ctx, ProviderQuota{Provider: "modelrun", Tenant: "seed-zero-rpm", RPM: 0, TPM: 5000, WindowSeconds: 60}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := store.Admit(ctx, "modelrun", "seed-zero-rpm", "zero-rpm-op", 1, 10); err != nil {
		t.Fatalf("admit: %v", err)
	}

	// And a tenant the gateway has served under a provider with no rows at all:
	// no limit row means no window, which is unlimited, and the audit learns the
	// tenant from the gateway's own budget ledger.
	budgets := NewBudgetStore(pool)
	if err := budgets.Migrate(ctx); err != nil {
		t.Fatalf("budgets migrate: %v", err)
	}
	ghostScope := contracts.Scope{Tenant: "seed-ghost-tenant", Subject: "s-ghost", Deployment: "d-ghost"}
	if _, err := budgets.Root(ctx, ghostScope, "task-ghost", "ghost-key", 100); err != nil {
		t.Fatalf("budget root: %v", err)
	}

	audits, err := store.Audit(ctx, "modelrun")
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	bad := NonFiniteTenants(audits)
	joined := strings.Join(bad, ",")
	if !strings.Contains(joined, "seed-zero-rpm") {
		t.Fatalf("audit missed a tenant whose rpm is 0: %v", bad)
	}
	// Under a provider that HAS a floor, a tenant with no row of its own is bounded
	// by inheritance -- and the audit must say so by name. Invisibility of
	// inheritance is how a deployment ends up with every tenant sharing one number
	// nobody intended to share.
	ghost := auditFor(t, audits, "seed-ghost-tenant")
	if ghost.Source != QuotaSourceProviderDefault {
		t.Fatalf("a tenant with no row of its own must report inheritance, got %+v", ghost)
	}
	if !ghost.Finite {
		t.Fatalf("a tenant covered by the provider floor must be reported bounded: %+v", ghost)
	}
	for _, a := range audits {
		if a.Tenant == "djourney" && !a.Finite {
			t.Fatalf("the audit must not report a bounded tenant as unbounded: %+v", a)
		}
	}

	// The same tenant under a provider nobody seeded: no row, no floor, unlimited.
	// That is precisely the state the order refuses to accept, so the audit has to
	// name it rather than report an empty (and trivially "clean") list.
	other, err := store.Audit(ctx, "modelrun-unseeded")
	if err != nil {
		t.Fatalf("audit of an unseeded provider: %v", err)
	}
	if !strings.Contains(strings.Join(NonFiniteTenants(other), ","), "seed-ghost-tenant") {
		t.Fatalf("a tenant served under a provider with no limit rows must be reported unbounded: %v", other)
	}
	if a := auditFor(t, other, "seed-ghost-tenant"); a.Source != QuotaSourceNone {
		t.Fatalf("source must be %q with no row and no floor, got %+v", QuotaSourceNone, a)
	}
}

func TestSeedSelfCheckRefusesAnUnboundedTenantAtomically(t *testing.T) {
	pool := quotaSeedDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seed := readQuotaSeed(t)
	if _, err := pool.Exec(ctx, seed); err != nil {
		t.Fatalf("seed apply: %v", err)
	}
	store := NewQuotaStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// Make the acceptance tenant a tenant the gateway has actually served, so the
	// seed's own rule has something to protect.
	if _, err := store.Admit(ctx, "modelrun", "djourney", "selfcheck-op", 1, 10); err != nil {
		t.Fatalf("admit: %v", err)
	}

	// A corrupted seed that would leave that served tenant with an unbounded rpm.
	// The file's DO block is the thing under test: it must refuse, not publish.
	corrupted := strings.Replace(seed,
		"VALUES ('modelrun', 'djourney', 30, 200000, 60, now())",
		"VALUES ('modelrun', 'djourney', 0, 200000, 60, now())", 1)
	if corrupted == seed {
		t.Fatal("corruption did not apply: the seed's acceptance-tenant line changed shape")
	}
	errMsg := ""
	if _, err := pool.Exec(ctx, corrupted); err != nil {
		errMsg = err.Error()
	}
	if !strings.Contains(errMsg, "no finite effective limit") {
		t.Fatalf("self-check must refuse an unbounded tenant, got err=%q", errMsg)
	}
	// The whole script is one transaction: the refusal must have left the good
	// limits standing, not half-applied them.
	got := limitsByTenant(t, store, "modelrun")
	if q := got["djourney"]; q.RPM != 30 {
		t.Fatalf("refused seed changed djourney rpm to %d, want the previous 30 (apply must be atomic)", q.RPM)
	}
}

func TestSeedTenantLimitEnforcesUnderTheDefault(t *testing.T) {
	pool := quotaSeedDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, readQuotaSeed(t)); err != nil {
		t.Fatalf("seed apply: %v", err)
	}
	store := NewQuotaStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Tenant bound of 1 request/minute while the provider-wide floor allows 20:
	// enforcement must follow the tenant's number. Resolution and enforcement
	// agreeing is the property; testing only resolution would pass on a gate that
	// consulted the default.
	if err := store.Set(ctx, ProviderQuota{Provider: "modelrun", Tenant: "seed-tight", RPM: 1, TPM: 1000, WindowSeconds: 60}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := store.Admit(ctx, "modelrun", "seed-tight", "tight-1", 1, 10); err != nil {
		t.Fatalf("first admit: %v", err)
	}
	_, err := store.Admit(ctx, "modelrun", "seed-tight", "tight-2", 1, 10)
	if err == nil {
		t.Fatal("second admit must be refused at the tenant's own bound of 1, whatever the floor allows")
	}
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("refusal code = %v, want ErrConflict", contracts.Code(err))
	}
	// The reason, not the prose: an operator scripts on the stable code, and a
	// message reworded next release must not turn this leg red or, worse, a
	// refusal into an unlabelled one.
	if got := contracts.ReasonOf(err); got != ReasonProviderRPMExhausted {
		t.Fatalf("refusal reason = %q, want %q; err=%v", got, ReasonProviderRPMExhausted, err)
	}
	// And the refusal left nothing charged: the window still holds exactly one.
	audits, err := store.Audit(ctx, "modelrun")
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	for _, a := range audits {
		if a.Tenant == "seed-tight" && !a.Finite {
			t.Fatalf("tight tenant must be reported finite: %+v", a)
		}
	}
}

func auditFor(t *testing.T, audits []TenantQuotaAudit, tenant string) TenantQuotaAudit {
	t.Helper()
	for _, a := range audits {
		if a.Tenant == tenant {
			return a
		}
	}
	t.Fatalf("audit did not report tenant %q at all: %+v", tenant, audits)
	return TenantQuotaAudit{}
}
