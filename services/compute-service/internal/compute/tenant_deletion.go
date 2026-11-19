package compute

import (
	"context"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

const tenantGuardDDL = `CREATE TABLE IF NOT EXISTS ahp_compute.erased_tenants(tenant text PRIMARY KEY);
CREATE OR REPLACE FUNCTION ahp_compute.guard_tenant_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target text;
BEGIN
 target:=to_jsonb(NEW)->>'tenant';
 IF target IS NULL THEN SELECT tenant INTO target FROM ahp_compute.computers WHERE id=to_jsonb(NEW)->>'computer_id'; END IF;
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('ahp_compute/tenant-erasure/'||target,0));
 IF EXISTS(SELECT 1 FROM ahp_compute.erased_tenants WHERE tenant=target) THEN RAISE EXCEPTION 'compute: tenant erased' USING ERRCODE='42501'; END IF;
 RETURN NEW; END $$;`

func (s *Store) tenantGuards(ctx context.Context) error {
	q := tenantGuardDDL
	for _, table := range []string{"computers", "incarnations", "volume_fences", "activity_leases", "commands", "snapshots", "env_revisions", "workspace_revisions", "state_revisions", "cost_ledger", "image_versions"} {
		q += `CREATE OR REPLACE TRIGGER tenant_insert_guard BEFORE INSERT ON ahp_compute.` + table + ` FOR EACH ROW EXECUTE FUNCTION ahp_compute.guard_tenant_insert();`
	}
	return s.migrateSubjectGuards(ctx, q)
}
func (s *Store) checkTenantWritable(ctx context.Context, tx pgx.Tx, tenant string) error {
	ledger, err := operations.New(s.pool, "ahp_compute")
	if err != nil {
		return err
	}
	return ledger.CheckTenantWritable(ctx, tx, tenant)
}

// FenceTenant prevents new computers, incarnations, leases and command dispatch.
func (s *Store) FenceTenant(ctx context.Context, tenant string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "ahp_compute/tenant-erasure/"+tenant); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO ahp_compute.erased_tenants VALUES($1) ON CONFLICT DO NOTHING`, tenant); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE ahp_compute.commands SET cancel_requested=true,status=CASE WHEN status='accepted' THEN 'cancelled' ELSE status END,updated_at=clock_timestamp() WHERE tenant=$1 AND status IN ('accepted','dispatched','side_effect_unknown')`, tenant)
		return err
	})
}
func (s *Store) tenantPending(ctx context.Context, tx pgx.Tx, tenant string) error {
	var fenced, pending bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_compute.erased_tenants WHERE tenant=$1),
 EXISTS(SELECT 1 FROM ahp_compute.commands WHERE tenant=$1 AND status IN ('accepted','dispatched','side_effect_unknown')) OR
 EXISTS(SELECT 1 FROM ahp_compute.env_revisions e JOIN ahp_compute.computers c ON c.id=e.computer_id WHERE c.tenant=$1 AND e.status='pending') OR
 EXISTS(SELECT 1 FROM ahp_compute.incarnations i JOIN ahp_compute.computers c ON c.id=i.computer_id WHERE c.tenant=$1 AND i.status<>'stopped') OR
 EXISTS(SELECT 1 FROM ahp_compute.activity_leases l JOIN ahp_compute.computers c ON c.id=l.computer_id WHERE c.tenant=$1 AND l.expires>clock_timestamp())`, tenant).Scan(&fenced, &pending)
	if err != nil {
		return err
	}
	if !fenced || pending {
		return contracts.NewReasoned(contracts.ErrUnavailable, "tenant_compute_pending", "compute: active execution or outcome pending")
	}
	return nil
}

// DeleteTenant is the metadata-only owner. Filesystem-bearing Computers need
// the runner's cleanup, and remain pending if that owner is unavailable.
func (s *Store) DeleteTenant(ctx context.Context, tenant string) error {
	return s.deleteTenant(ctx, tenant, false)
}
func (s *Store) deleteTenant(ctx context.Context, tenant string, filesRemoved bool) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "ahp_compute/tenant-erasure/"+tenant); err != nil {
			return err
		}
		if err := s.tenantPending(ctx, tx, tenant); err != nil {
			return err
		}
		if !filesRemoved {
			var files bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_compute.snapshots s JOIN ahp_compute.computers c ON c.id=s.computer_id WHERE c.tenant=$1) OR EXISTS(SELECT 1 FROM ahp_compute.computers WHERE tenant=$1 AND profile<>'ephemeral')`, tenant).Scan(&files); err != nil {
				return err
			}
			if files {
				return contracts.NewError(contracts.ErrUnavailable, "compute: filesystem cleanup owner required")
			}
		}
		return s.purgeTenantRows(ctx, tx, tenant)
	})
}

func (s *Store) purgeTenantRows(ctx context.Context, tx pgx.Tx, tenant string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM ahp_compute.computers WHERE tenant=$1`, tenant); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `DELETE FROM ahp_compute.image_versions WHERE tenant=$1`, tenant)
	return err
}
