package triggers

import (
	"context"
	"fmt"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

func (s *Service) tenantGuards(ctx context.Context) error {
	q := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %[1]s.guard_tenant_insert() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('%[2]s/tenant-erasure/'||NEW.tenant,0));
 IF EXISTS(SELECT 1 FROM %[1]s.erased_tenants WHERE tenant=NEW.tenant) THEN RAISE EXCEPTION 'triggers: tenant erased' USING ERRCODE='42501'; END IF;
 RETURN NEW; END $$;
 CREATE OR REPLACE TRIGGER tenant_insert_guard BEFORE INSERT ON %[1]s.triggers FOR EACH ROW EXECUTE FUNCTION %[1]s.guard_tenant_insert();`, quoteIdent(s.schema), s.schema)
	return s.migrateSubjectGuards(ctx, q)
}

// FenceTenant revokes every source before any frozen inventory is deleted.
func (s *Service) FenceTenant(ctx context.Context, tenant string) error {
	if err := s.ledger.FenceTenant(ctx, tenant); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `UPDATE `+quoteIdent(s.schema)+`.triggers SET status='REVOKED',version=version+1,updated_at=now() WHERE tenant=$1 AND status<>'REVOKED'`, tenant)
	return err
}

// DeleteTenant leaves any unknown outbound dispatch in its durable ledger.
func (s *Service) DeleteTenant(ctx context.Context, tenant string) error {
	if err := s.ledger.DeleteTenant(ctx, tenant); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var live bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+quoteIdent(s.schema)+`.triggers WHERE tenant=$1 AND status<>'REVOKED')`, tenant).Scan(&live); err != nil {
			return err
		}
		if live {
			return contracts.NewError(contracts.ErrUnavailable, "triggers: sources not revoked")
		}
		_, err := tx.Exec(ctx, `DELETE FROM `+quoteIdent(s.schema)+`.triggers WHERE tenant=$1`, tenant)
		return err
	})
}
