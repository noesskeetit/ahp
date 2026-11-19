package memory

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

func tenantFenceKey(tenant string) string { return "memory-tenant-erasure/" + tenant }
func (s *Store) FenceTenant(ctx context.Context, scope contracts.Scope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := s.authorize(ctx, scope, deleteSubjectAction, "tenant:"+scope.Tenant); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, tenantFenceKey(scope.Tenant)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO ahp_memory.erased_tenants(tenant) VALUES($1) ON CONFLICT DO NOTHING`, scope.Tenant)
		return err
	})
}
func denyErasedTenant(ctx context.Context, tx pgx.Tx, tenant string) error {
	var erased bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_memory.erased_tenants WHERE tenant=$1)`, tenant).Scan(&erased); err != nil {
		return err
	}
	if erased {
		return contracts.NewError(contracts.ErrForbidden, "memory: tenant erased")
	}
	return nil
}
func (s *Store) TenantScopes(ctx context.Context, scope contracts.Scope) ([]contracts.Scope, error) {
	if err := s.authorize(ctx, scope, deleteSubjectAction, "tenant:"+scope.Tenant); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT deployment,subject FROM ahp_memory.memories WHERE tenant=$1 AND state<>'deleted' ORDER BY deployment,subject`, scope.Tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []contracts.Scope
	for rows.Next() {
		target := contracts.Scope{Tenant: scope.Tenant}
		if err = rows.Scan(&target.Deployment, &target.Subject); err != nil {
			return nil, err
		}
		out = append(out, target)
	}
	return out, rows.Err()
}
