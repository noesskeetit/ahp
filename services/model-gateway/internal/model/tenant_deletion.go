package model

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// The trigger joins every insertion (including aliases, quota upserts and
// reservations) to the same barrier as the effect ledger. UPDATE settlement is
// deliberately allowed: a provider bill must remain reconcilable after fencing.
const tenantGuardDDL = `;CREATE TABLE IF NOT EXISTS ahp_model.erased_tenants(tenant text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS ahp_model.erased_subjects(tenant text NOT NULL, subject text NOT NULL,PRIMARY KEY(tenant,subject));
CREATE OR REPLACE FUNCTION ahp_model.guard_tenant_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target text; owner_subject text;
BEGIN
 target := to_jsonb(NEW)->>'tenant';
 owner_subject := to_jsonb(NEW)->>'subject';
 IF target IS NULL THEN SELECT tenant,subject INTO target,owner_subject FROM ahp_model.budgets WHERE id=to_jsonb(NEW)->>'budget_id'; END IF;
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('ahp_model/tenant-erasure/'||target,0));
 IF EXISTS(SELECT 1 FROM ahp_model.erased_tenants WHERE tenant=target) THEN RAISE EXCEPTION 'model: tenant erased' USING ERRCODE='42501'; END IF;
 IF owner_subject IS NOT NULL THEN
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('ahp_model/subject-erasure/'||target||'/'||owner_subject,0));
 IF EXISTS(SELECT 1 FROM ahp_model.erased_subjects WHERE tenant=target AND subject=owner_subject) THEN RAISE EXCEPTION 'model: subject erased' USING ERRCODE='42501'; END IF;
 END IF;
 RETURN NEW;
END $$;`

func tenantInsertGuards(tables ...string) string {
	ddl := tenantGuardDDL
	for _, table := range tables {
		ddl += `CREATE OR REPLACE TRIGGER tenant_insert_guard BEFORE INSERT ON ahp_model.` + table + ` FOR EACH ROW EXECUTE FUNCTION ahp_model.guard_tenant_insert();`
	}
	return ddl
}

// FenceTenant stops admission and requests normal generation cancellation.
func (s *Service) FenceTenant(ctx context.Context, tenant string) error {
	if s.ledger == nil {
		return contracts.NewError(contracts.ErrUnavailable, "model: durable effect owner unavailable")
	}
	if err := s.ledger.FenceTenant(ctx, tenant); err != nil {
		return err
	}
	if s.files != nil {
		if err := s.files.FenceTenant(ctx, tenant); err != nil {
			return err
		}
	}
	rows, err := s.gens.pool.Query(ctx, `SELECT id,tenant,deployment,subject FROM ahp_model.generations WHERE tenant=$1 AND status IN ('PENDING','RUNNING','STREAMING')`, tenant)
	if err != nil {
		return err
	}
	type target struct {
		id    string
		scope contracts.Scope
	}
	var targets []target
	for rows.Next() {
		var t target
		if err = rows.Scan(&t.id, &t.scope.Tenant, &t.scope.Deployment, &t.scope.Subject); err != nil {
			rows.Close()
			return err
		}
		targets = append(targets, t)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, t := range targets {
		if _, err = s.Cancel(ctx, t.scope, t.id); err != nil {
			return err
		}
	}
	return nil
}

// DeleteTenant preserves unsettled billing and unknown provider effects.
func (s *Service) DeleteTenant(ctx context.Context, tenant string) error {
	if s.ledger == nil {
		return contracts.NewError(contracts.ErrUnavailable, "model: durable effect owner unavailable")
	}
	if err := s.FenceTenant(ctx, tenant); err != nil {
		return err
	}
	if err := s.deleteProviderFiles(ctx, tenant); err != nil {
		return err
	}
	if err := s.recoverErasureGenerations(ctx, tenant, ""); err != nil {
		return err
	}
	return s.ledger.DeleteTenantChecked(ctx, tenant, func(ctx context.Context, tx pgx.Tx) error {
		var pending bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_model.generations WHERE tenant=$1 AND status IN ('PENDING','RUNNING','STREAMING')) OR EXISTS(SELECT 1 FROM ahp_model.budget_reservations r JOIN ahp_model.budgets b ON b.id=r.budget_id WHERE b.tenant=$1 AND NOT r.settled)`, tenant).Scan(&pending); err != nil {
			return err
		}
		if pending {
			return contracts.NewReasoned(contracts.ErrUnavailable, "tenant_model_pending", "model: generation or settlement pending")
		}
		for _, q := range []string{
			`DELETE FROM ahp_model.budget_task_aliases WHERE tenant=$1`,
			`DELETE FROM ahp_model.budget_reservations WHERE budget_id IN (SELECT id FROM ahp_model.budgets WHERE tenant=$1)`,
			`DELETE FROM ahp_model.generations WHERE tenant=$1`,
			`DELETE FROM ahp_model.budgets WHERE tenant=$1`,
		} {
			if _, err := tx.Exec(ctx, q, tenant); err != nil {
				return err
			}
		}
		for _, table := range []string{"provider_quotas", "quota_windows", "quota_admits", "deployment_labels"} {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, "ahp_model."+table).Scan(&exists); err != nil {
				return err
			}
			if exists {
				if _, err := tx.Exec(ctx, `DELETE FROM ahp_model.`+table+` WHERE tenant=$1`, tenant); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
