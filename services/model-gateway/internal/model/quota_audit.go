package model

import (
	"context"
	"fmt"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Operator-facing quota audit.
//
// The gate answers "may this operation run"; this answers the question an operator
// has to ask BEFORE a load test: "is every tenant this gateway serves actually
// bounded?" A limit nobody can enumerate is not configuration, it is a rumour --
// and because the absence of a row means unlimited, the only honest statement
// about a deployment is the pair (tenants the gateway has actually served, the
// effective limit each one resolves to).
//
// "Finite" is deliberately strict: a row whose rpm or tpm is 0 leaves that
// dimension unbounded (see Admit), and a tenant resolving to no row at all is
// unbounded in both. Either is reported as not finite rather than rounded up to
// "configured", because the acceptance question is exactly "can this tenant spend
// without limit".
type TenantQuotaAudit struct {
	Provider string `json:"provider"`
	Tenant   string `json:"tenant"`
	// Source names the row that decided the limit: "tenant", "provider-default"
	// or "none". It is the field that shows a tenant silently inheriting the
	// provider-wide default instead of carrying its own limit.
	Source        string `json:"source"`
	RPM           int64  `json:"rpm"`
	TPM           int64  `json:"tpm"`
	WindowSeconds int    `json:"window_seconds"`
	Finite        bool   `json:"finite"`
}

// Quota sources as reported. Constants because an operator script branches on
// them, and a typo in a string literal is a silent pass.
const (
	QuotaSourceTenant          = "tenant"
	QuotaSourceProviderDefault = "provider-default"
	QuotaSourceNone            = "none"
)

// The gateway's own books, most authoritative first: window counters it booked,
// generations it accepted, budgets it opened. The first one belongs to the quota
// plane and is therefore always present; the other two belong to planes a given
// deployment may not have mounted at all.
const (
	observedLedgerQuotaWindows = "ahp_model.quota_windows"
	observedLedgerGenerations  = "ahp_model.generations"
	observedLedgerBudgets      = "ahp_model.budgets"
)

// observedTenantsQuery lists every tenant the gateway has actually served, so an
// audit cannot be satisfied by rows an operator typed that no caller ever resolves
// to. It is assembled from the ledgers that exist (see observedLedgers): a plane
// that is not mounted here must not turn the audit into an error, and must not
// silently drop a tenant that IS visible in another ledger either.
func observedTenantsQuery(ledgers []string) string {
	union := make([]string, 0, len(ledgers))
	for _, l := range ledgers {
		if l == observedLedgerQuotaWindows {
			// The window ledger is provider-scoped: a tenant bounded on some other
			// provider has not been served by THIS provider's windows.
			union = append(union, "  SELECT tenant FROM "+l+
				" WHERE provider = $1 AND tenant <> ''")
			continue
		}
		union = append(union, "  SELECT tenant FROM "+l+" WHERE tenant <> ''")
	}
	return `
WITH observed AS (
` + strings.Join(union, "\n  UNION\n") + `
),
eff AS (
  SELECT o.tenant,
         (SELECT q.tenant FROM ahp_model.provider_quotas q
           WHERE q.provider = $1 AND q.tenant = ANY(ARRAY[o.tenant, ''])
           ORDER BY (q.tenant = o.tenant) DESC LIMIT 1) AS source_tenant,
         (SELECT q.rpm FROM ahp_model.provider_quotas q
           WHERE q.provider = $1 AND q.tenant = ANY(ARRAY[o.tenant, ''])
           ORDER BY (q.tenant = o.tenant) DESC LIMIT 1) AS rpm,
         (SELECT q.tpm FROM ahp_model.provider_quotas q
           WHERE q.provider = $1 AND q.tenant = ANY(ARRAY[o.tenant, ''])
           ORDER BY (q.tenant = o.tenant) DESC LIMIT 1) AS tpm,
         (SELECT q.window_seconds FROM ahp_model.provider_quotas q
           WHERE q.provider = $1 AND q.tenant = ANY(ARRAY[o.tenant, ''])
           ORDER BY (q.tenant = o.tenant) DESC LIMIT 1) AS window_seconds
    FROM observed o
)
SELECT tenant,
       CASE WHEN source_tenant IS NULL THEN 'none'
            WHEN source_tenant = tenant THEN 'tenant'
            ELSE 'provider-default' END,
       coalesce(rpm, -1), coalesce(tpm, -1), coalesce(window_seconds, 0)
  FROM eff
 ORDER BY tenant`
}

// Audit reports, per tenant this gateway has served, the limit that tenant
// actually resolves to and whether it is finite. An empty result means the gateway
// has served no tenant yet -- that is not "bounded", and the caller says so
// instead of passing quietly.
func (s *QuotaStore) Audit(ctx context.Context, provider string) ([]TenantQuotaAudit, error) {
	if provider == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "quota: provider required for an audit")
	}
	ledgers, err := s.observedLedgers(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, observedTenantsQuery(ledgers), provider)
	if err != nil {
		return nil, fmt.Errorf("model: quota audit: %w", err)
	}
	defer rows.Close()

	var out []TenantQuotaAudit
	for rows.Next() {
		var a TenantQuotaAudit
		if err := rows.Scan(&a.Tenant, &a.Source, &a.RPM, &a.TPM, &a.WindowSeconds); err != nil {
			return nil, fmt.Errorf("model: quota audit scan: %w", err)
		}
		a.Provider = provider
		a.Finite = a.Source != QuotaSourceNone && a.RPM > 0 && a.TPM > 0
		out = append(out, a)
	}
	return out, rows.Err()
}

// observedLedgers returns the tenant ledgers present in this database, most
// authoritative first. A missing plane is reported as absent rather than as an
// error: auditing a gateway that only runs the quota plane is still a valid audit
// of that gateway.
func (s *QuotaStore) observedLedgers(ctx context.Context) ([]string, error) {
	var generations, budgets bool
	err := s.pool.QueryRow(ctx, `
SELECT to_regclass($1) IS NOT NULL, to_regclass($2) IS NOT NULL`,
		observedLedgerGenerations, observedLedgerBudgets).Scan(&generations, &budgets)
	if err != nil {
		return nil, fmt.Errorf("model: quota audit: probe ledgers: %w", err)
	}
	ledgers := []string{observedLedgerQuotaWindows}
	if generations {
		ledgers = append(ledgers, observedLedgerGenerations)
	}
	if budgets {
		ledgers = append(ledgers, observedLedgerBudgets)
	}
	return ledgers, nil
}

// ConfiguredLimits lists every limit row published for one provider, so an
// operator can see what was seeded independently of who has called.
func (s *QuotaStore) ConfiguredLimits(ctx context.Context, provider string) ([]ProviderQuota, error) {
	if provider == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "quota: provider required")
	}
	rows, err := s.pool.Query(ctx, `
SELECT provider, tenant, rpm, tpm, window_seconds
  FROM ahp_model.provider_quotas WHERE provider = $1 ORDER BY tenant`, provider)
	if err != nil {
		return nil, fmt.Errorf("model: configured limits: %w", err)
	}
	defer rows.Close()

	var out []ProviderQuota
	for rows.Next() {
		var q ProviderQuota
		if err := rows.Scan(&q.Provider, &q.Tenant, &q.RPM, &q.TPM, &q.WindowSeconds); err != nil {
			return nil, fmt.Errorf("model: configured limits scan: %w", err)
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

// NonFiniteTenants names the tenants an audit found without a finite effective
// limit: the list an operator has to act on, and the reason an apply script exits
// non-zero instead of reporting green over an unbounded tenant.
func NonFiniteTenants(audits []TenantQuotaAudit) []string {
	var out []string
	for _, a := range audits {
		if !a.Finite {
			out = append(out, a.Tenant)
		}
	}
	return out
}
