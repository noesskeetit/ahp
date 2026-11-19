package identity

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RevokeTenant is a trusted control-plane kill switch, including subjects that
// have never existed. Every live token check consults this permanent tombstone.
func (s *Store) RevokeTenant(ctx context.Context, tenant string) error {
	if err := (contracts.Scope{Tenant: tenant, Deployment: "control", Subject: "control"}).Validate(); err != nil {
		return err
	}
	return s.runTxDetached(func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "identity-tenant-erasure/"+tenant); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO ahp_identity.erased_tenants(tenant) VALUES($1) ON CONFLICT DO NOTHING`, tenant); err != nil {
			return err
		}
		for _, table := range []string{"grants_child", "grants_root"} {
			if _, err := tx.Exec(ctx, `UPDATE ahp_identity.`+table+` SET status='revoked',epoch=epoch+1,revoked_at=clock_timestamp() WHERE tenant=$1 AND status='active'`, tenant); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE ahp_identity.browser_sessions SET revoked_at=clock_timestamp() WHERE tenant=$1 AND revoked_at IS NULL`, tenant); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE ahp_identity.browser_token_bindings b SET revoked_at=clock_timestamp() FROM ahp_identity.browser_sessions s WHERE b.session_id=s.session_id AND s.tenant=$1 AND b.revoked_at IS NULL`, tenant)
		return err
	})
}

func (s *Store) tenantGuards(ctx context.Context, conn *pgxpool.Conn) error {
	q := `CREATE OR REPLACE FUNCTION ahp_identity.guard_tenant_insert() RETURNS trigger LANGUAGE plpgsql AS $$
 DECLARE target text;
 BEGIN
 target:=to_jsonb(NEW)->>'tenant';
 IF target IS NULL AND to_jsonb(NEW)?'user_id' THEN SELECT tenant INTO target FROM ahp_identity.browser_users WHERE user_id=to_jsonb(NEW)->>'user_id'; END IF;
 IF target IS NULL AND to_jsonb(NEW)?'session_id' THEN SELECT tenant INTO target FROM ahp_identity.browser_sessions WHERE session_id=to_jsonb(NEW)->>'session_id'; END IF;
 IF target IS NULL AND to_jsonb(NEW)?'grant_id' THEN SELECT tenant INTO target FROM ahp_identity.grants_root WHERE id=to_jsonb(NEW)->>'grant_id'; IF target IS NULL THEN SELECT tenant INTO target FROM ahp_identity.grants_child WHERE id=to_jsonb(NEW)->>'grant_id'; END IF; END IF;
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('identity-tenant-erasure/'||target,0));
 IF EXISTS(SELECT 1 FROM ahp_identity.erased_tenants WHERE tenant=target) THEN RAISE EXCEPTION 'identity: tenant erased' USING ERRCODE='42501'; END IF;
 RETURN NEW; END $$;`
	for _, table := range []string{"subjects", "resource_rules", "contexts", "context_label_events", "grants_root", "grants_child", "issued_tokens", "browser_users", "browser_credentials", "browser_grants", "browser_sessions", "browser_token_bindings"} {
		q += `CREATE OR REPLACE TRIGGER tenant_insert_guard BEFORE INSERT ON ahp_identity.` + table + ` FOR EACH ROW EXECUTE FUNCTION ahp_identity.guard_tenant_insert();`
	}
	_, err := conn.Exec(ctx, q)
	return err
}

// DeleteTenant removes revoked identity data while retaining the permanent
// tenant tombstone consulted by every live task and browser authorization.
func (s *Store) DeleteTenant(ctx context.Context, tenant string) error {
	return s.runTxDetached(func(ctx context.Context, tx pgx.Tx) error {
		// The context label journal is append-only; an erasure is the one
		// operation allowed to remove events, and it says so inside its own
		// transaction so the rule stays a database rule for everyone else.
		if _, err := tx.Exec(ctx, `SET LOCAL ahp_identity.erasing = 'on'`); err != nil {
			return err
		}
		var fenced bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_identity.erased_tenants WHERE tenant=$1)`, tenant).Scan(&fenced); err != nil {
			return err
		}
		if !fenced {
			return contracts.NewError(contracts.ErrForbidden, "identity: tenant revocation required")
		}
		for _, q := range []string{
			`DELETE FROM ahp_identity.resource_rules WHERE tenant=$1`,
			`DELETE FROM ahp_identity.browser_token_bindings WHERE session_id IN (SELECT session_id FROM ahp_identity.browser_sessions WHERE tenant=$1)`,
			`DELETE FROM ahp_identity.browser_sessions WHERE tenant=$1`,
			`DELETE FROM ahp_identity.browser_credentials WHERE user_id IN (SELECT user_id FROM ahp_identity.browser_users WHERE tenant=$1)`,
			`DELETE FROM ahp_identity.browser_grants WHERE user_id IN (SELECT user_id FROM ahp_identity.browser_users WHERE tenant=$1)`,
			`DELETE FROM ahp_identity.browser_users WHERE tenant=$1`,
			`DELETE FROM ahp_identity.issued_tokens WHERE grant_id IN (SELECT id FROM ahp_identity.grants_child WHERE tenant=$1 UNION SELECT id FROM ahp_identity.grants_root WHERE tenant=$1)`,
			`DELETE FROM ahp_identity.grants_child WHERE tenant=$1`,
			`DELETE FROM ahp_identity.grants_root WHERE tenant=$1`,
			`DELETE FROM ahp_identity.contexts WHERE tenant=$1`,
			`DELETE FROM ahp_identity.subjects WHERE tenant=$1`,
		} {
			if _, err := tx.Exec(ctx, q, tenant); err != nil {
				return err
			}
		}
		return nil
	})
}
