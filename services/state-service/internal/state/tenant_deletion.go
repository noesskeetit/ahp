package state

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

func tenantFenceKey(tenant string) string { return "state-tenant-erasure/" + tenant }

// FenceTenant drains every subject writer before committing the permanent barrier.
func (s *Store) FenceTenant(ctx context.Context, tenant string) error {
	if err := (contracts.Scope{Tenant: tenant, Deployment: "control", Subject: "control"}).Validate(); err != nil {
		return err
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, tenantFenceKey(tenant)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO ahp_state.erased_tenants(tenant) VALUES($1) ON CONFLICT DO NOTHING`, tenant)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE ahp_state.runs r SET status='canceled',wait_for='',deadline=NULL,payload=payload || '{"close_reason":"tenant_deleted"}'::jsonb,updated_at=clock_timestamp() FROM ahp_state.threads t WHERE r.thread_id=t.id AND t.tenant=$1 AND r.status IN ('pending','running','waiting')`, tenant); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE ahp_state.threads SET epoch=epoch+1,lease_epoch=NULL,lease_run_id=NULL,lease_worker=NULL,lease_expires=NULL WHERE tenant=$1`, tenant)
		return err
	})
	if err == nil {
		s.dropTailsForTenant(tenant)
	}
	return err
}
func (s *Store) TenantScopes(ctx context.Context, tenant string) ([]contracts.Scope, error) {
	rows, err := s.pool.Query(ctx, `SELECT deployment,subject FROM ahp_state.threads WHERE tenant=$1 UNION SELECT deployment,subject FROM ahp_state.kv WHERE tenant=$1 ORDER BY deployment,subject`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []contracts.Scope
	for rows.Next() {
		scope := contracts.Scope{Tenant: tenant}
		if err = rows.Scan(&scope.Deployment, &scope.Subject); err != nil {
			return nil, err
		}
		out = append(out, scope)
	}
	return out, rows.Err()
}

// TenantEffectsSettled prevents deleting the only durable evidence of a
// dispatched or unknown effect while its executor is being revoked.
func (s *Store) TenantEffectsSettled(ctx context.Context, tenant string) error {
	var pending bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.intents i JOIN ahp_state.runs r ON r.id=i.run_id JOIN ahp_state.threads t ON t.id=r.thread_id WHERE t.tenant=$1 AND i.status IN ('dispatched','unknown'))`, tenant).Scan(&pending)
	if err != nil {
		return err
	}
	if pending {
		return contracts.NewReasoned(contracts.ErrUnavailable, "tenant_state_effects_pending", "state: external outcome requires reconciliation")
	}
	return nil
}
