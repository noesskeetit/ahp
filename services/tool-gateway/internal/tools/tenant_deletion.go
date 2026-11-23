package tools

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// FenceTenant rejects new effects and personal OAuth lifecycle writes permanently.
func (s *Service) FenceTenant(ctx context.Context, tenant string) error {
	if err := s.ledger.FenceTenant(ctx, tenant); err != nil {
		return err
	}
	if s.cfg.OAuth != nil {
		return s.cfg.OAuth.FenceTenant(ctx, tenant)
	}
	// An OAuth table left by a previous configuration cannot silently escape cleanup.
	var exists bool
	if err := s.pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", s.schema+".oauth_secrets").Scan(&exists); err != nil {
		return err
	}
	if exists {
		return contracts.NewError(contracts.ErrUnavailable, "tools: OAuth owner configuration required for tenant cleanup")
	}
	return nil
}

// DeleteTenant preserves unresolved effects and erases only this owner's data.
func (s *Service) DeleteTenant(ctx context.Context, tenant string) error {
	if err := s.FenceTenant(ctx, tenant); err != nil {
		return err
	}
	if err := s.ledger.DeleteTenant(ctx, tenant); err != nil {
		return err
	}
	if s.cfg.OAuth != nil {
		if err := s.cfg.OAuth.DeleteTenant(ctx, tenant); err != nil {
			return err
		}
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, table := range []string{"approvals", "guardian_log", "tool_policy", "tool_policy_audit"} {
			if _, err := tx.Exec(ctx, "DELETE FROM "+s.schema+"."+table+" WHERE tenant=$1", tenant); err != nil {
				return err
			}
		}
		return s.deleteToolLeaseRows(ctx, tx, tenant, "")
	})
}

func (s *Service) tenantWrite(ctx context.Context, scope contracts.Scope) (pgx.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if err = s.ledger.CheckTenantWritable(ctx, tx, scope.Tenant); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	if err = s.ledger.CheckSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}
