package registry

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

const tenantDeletionSchema = `CREATE TABLE IF NOT EXISTS ahp_registry.erased_tenants(tenant text PRIMARY KEY);`

func tenantFenceKey(tenant string) string { return "registry-tenant-erasure/" + tenant }
func (s *Store) FenceTenant(ctx context.Context, tenant string) error {
	if err := requireID("tenant", tenant); err != nil {
		return err
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, tenantFenceKey(tenant)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO ahp_registry.erased_tenants(tenant) VALUES($1) ON CONFLICT DO NOTHING`, tenant)
		return err
	})
}
func checkTenantWritable(ctx context.Context, tx pgx.Tx, tenant string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, tenantFenceKey(tenant)); err != nil {
		return err
	}
	var erased bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_registry.erased_tenants WHERE tenant=$1)`, tenant).Scan(&erased); err != nil {
		return err
	}
	if erased {
		return contracts.NewError(contracts.ErrForbidden, "registry: tenant erased")
	}
	return nil
}

func (s *Store) tenantGuards(ctx context.Context, tx pgx.Tx) error {
	q := `CREATE OR REPLACE FUNCTION ahp_registry.guard_tenant_write() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('registry-tenant-erasure/'||NEW.tenant,0));
 IF EXISTS(SELECT 1 FROM ahp_registry.erased_tenants WHERE tenant=NEW.tenant) THEN RAISE EXCEPTION 'registry: tenant erased' USING ERRCODE='42501'; END IF;
 RETURN NEW; END $$;`
	for _, table := range []string{"installations", "task_pins", "configs", "deployments", "system_deployments", "model_routes", "runtimes"} {
		q += `CREATE OR REPLACE TRIGGER tenant_write_guard BEFORE INSERT OR UPDATE ON ahp_registry.` + table + ` FOR EACH ROW EXECUTE FUNCTION ahp_registry.guard_tenant_write();`
	}
	_, err := tx.Exec(ctx, q)
	return err
}

// DeleteTenant removes the installation and configuration catalog after all
// version-pinned own_tables hooks have acknowledged their durable operation.
func (s *Store) DeleteTenant(ctx context.Context, tenant string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var fenced bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_registry.erased_tenants WHERE tenant=$1)`, tenant).Scan(&fenced); err != nil {
			return err
		}
		if !fenced {
			return contracts.NewError(contracts.ErrForbidden, "registry: tenant barrier required")
		}
		if _, err := tx.Exec(ctx, `DELETE FROM ahp_registry.skill_revisions WHERE publisher_id IN (SELECT publisher_id FROM ahp_registry.skill_owners WHERE tenant=$1)`, tenant); err != nil {
			return err
		}
		for _, table := range []string{"skill_owners", "dot_pins", "profiles", "system_deployments", "deployments", "task_pins", "configs", "model_routes", "model_route_generations", "runtimes", "installations"} {
			if _, err := tx.Exec(ctx, `DELETE FROM ahp_registry.`+table+` WHERE tenant=$1`, tenant); err != nil {
				return err
			}
		}
		return nil
	})
}
