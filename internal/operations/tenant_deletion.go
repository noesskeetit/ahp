package operations

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

func (s *Store) tenantFenceKey(tenant string) string { return s.schema + "/tenant-erasure/" + tenant }

// CheckTenantWritable joins an owner's transaction to the durable tenant barrier.
// Call before locking effect or content rows. Terminal reconciliation does not use it.
func (s *Store) CheckTenantWritable(ctx context.Context, tx pgx.Tx, tenant string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, s.tenantFenceKey(tenant)); err != nil {
		return err
	}
	var erased bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+quoteIdentifier(s.schema)+`.erased_tenants WHERE tenant=$1)`, tenant).Scan(&erased); err != nil {
		return err
	}
	if erased {
		return contracts.NewError(contracts.ErrForbidden, "operations: tenant erased")
	}
	return nil
}

// FenceTenant prevents admission and dispatch. A dispatched effect remains
// ambiguous until its original executor or an explicit reconciler proves outcome.
func (s *Store) FenceTenant(ctx context.Context, tenant string) error {
	if err := (contracts.Scope{Tenant: tenant, Deployment: "control", Subject: "control"}).Validate(); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.tenantFenceKey(tenant)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+quoteIdentifier(s.schema)+`.erased_tenants(tenant) VALUES($1) ON CONFLICT DO NOTHING`, tenant); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE `+quoteIdentifier(s.schema)+`.operations SET status=CASE WHEN status='ACCEPTED' THEN 'FENCED' ELSE 'SIDE_EFFECT_UNKNOWN' END,terminal=(status='ACCEPTED'),version=version+1,updated_at=now() WHERE tenant=$1 AND status IN ('ACCEPTED','DISPATCHED')`, tenant)
		return err
	})
}

// DeleteTenant removes only settled effect metadata and keeps the permanent fence.
func (s *Store) DeleteTenant(ctx context.Context, tenant string) error {
	return s.DeleteTenantChecked(ctx, tenant, nil)
}

// DeleteTenantChecked removes terminal receipts in the same transaction as the
// owner's content; erase must only use the supplied transaction.
func (s *Store) DeleteTenantChecked(ctx context.Context, tenant string, erase func(context.Context, pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.tenantFenceKey(tenant)); err != nil {
			return err
		}
		var fenced, pending bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+quoteIdentifier(s.schema)+`.erased_tenants WHERE tenant=$1),EXISTS(SELECT 1 FROM `+quoteIdentifier(s.schema)+`.operations WHERE tenant=$1 AND NOT terminal)`, tenant).Scan(&fenced, &pending); err != nil {
			return err
		}
		if !fenced || pending {
			return contracts.NewReasoned(contracts.ErrUnavailable, "tenant_effects_pending", "operations: tenant fence or external outcome pending")
		}
		if erase != nil {
			if err := erase(ctx, tx); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `DELETE FROM `+quoteIdentifier(s.schema)+`.operations WHERE tenant=$1`, tenant)
		return err
	})
}
