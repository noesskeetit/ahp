package workspace

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func tenantFenceKey(tenant string) string { return "workspace-tenant-erasure/" + tenant }
func (s *Store) FenceTenant(ctx context.Context, scope contracts.Scope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := s.authorize(ctx, scope, "purge", "tenant:"+scope.Tenant); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, tenantFenceKey(scope.Tenant)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO ahp_workspace.erased_tenants(tenant) VALUES($1) ON CONFLICT DO NOTHING`, scope.Tenant)
		return err
	})
}
func (s *Store) DeleteTenant(ctx context.Context, scope contracts.Scope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := s.authorize(ctx, scope, "purge", "tenant:"+scope.Tenant); err != nil {
		return err
	}
	// FK cascades remove candidates, revisions, memberships and reference pins.
	// Unlike subject deletion, tenant erasure has unambiguous ownership of all.
	_, err := s.pool.Exec(ctx, `DELETE FROM ahp_workspace.workspaces WHERE tenant=$1 AND EXISTS(SELECT 1 FROM ahp_workspace.erased_tenants WHERE tenant=$1)`, scope.Tenant)
	return err
}
func (s *Store) beginWritable(ctx context.Context, scope contracts.Scope) (pgx.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, tenantFenceKey(scope.Tenant)); err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	var erased bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_workspace.erased_tenants WHERE tenant=$1)`, scope.Tenant).Scan(&erased)
	if err == nil && erased {
		err = contracts.NewError(contracts.ErrForbidden, "workspace: tenant erased")
	}
	if err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}
func (s *Store) writeExec(ctx context.Context, scope contracts.Scope, sql string, args ...any) (pgconn.CommandTag, error) {
	tx, err := s.beginWritable(ctx, scope)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, sql, args...)
	if err != nil {
		return tag, err
	}
	return tag, tx.Commit(ctx)
}
func (s *Store) AuthorizeTenantDeletion(ctx context.Context, scope contracts.Scope) error {
	return s.authorize(ctx, scope, "purge", "tenant:"+scope.Tenant)
}
