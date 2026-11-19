package postgres

import (
	"context"
	"github.com/ahp-platform/ahp/internal/memory/longterm"
	"github.com/jackc/pgx/v5"
)

func tenantLock(tenant string) string { return "ahp-longterm-tenant/" + tenant }
func (s *Store) FenceTenant(ctx context.Context, tenant string) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, tenantLock(tenant)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO ahp_longterm.erased_tenants(tenant) VALUES($1) ON CONFLICT DO NOTHING`, tenant)
		return err
	})
}
func (s *Store) TenantOwners(ctx context.Context, tenant string) ([]memory.Identity, error) {
	rows, err := s.Pool.Query(ctx, `SELECT subject,deployment FROM ahp_longterm.owner_directory WHERE tenant=$1 ORDER BY subject`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []memory.Identity{}
	for rows.Next() {
		id := memory.Identity{TenantID: tenant}
		if err := rows.Scan(&id.UserID, &id.ActorID); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
func (t *transaction) RegisterIdentity(id memory.Identity) error {
	if id.Owner() != t.owner {
		return memory.Fail(403, "owner_mismatch", "owner directory scope mismatch")
	}
	if _, err := t.tx.Exec(t.ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, tenantLock(id.TenantID)); err != nil {
		return err
	}
	var frozen bool
	if err := t.tx.QueryRow(t.ctx, `SELECT EXISTS(SELECT 1 FROM ahp_longterm.erased_tenants WHERE tenant=$1)`, id.TenantID).Scan(&frozen); err != nil {
		return err
	}
	if frozen {
		return memory.Fail(410, "tenant_deleted", "memory tenant is fenced")
	}
	_, err := t.tx.Exec(t.ctx, `INSERT INTO ahp_longterm.owner_directory(owner_key,tenant,subject,deployment) VALUES($1,$2,$3,$4) ON CONFLICT(owner_key) DO NOTHING`, t.owner, id.TenantID, id.UserID, id.ActorID)
	return err
}
func (t *transaction) ForgetIdentity() error {
	_, err := t.tx.Exec(t.ctx, `DELETE FROM ahp_longterm.owner_directory WHERE owner_key=$1`, t.owner)
	return err
}
func (t *transaction) TenantFrozen() (bool, error) {
	var tenant string
	err := t.tx.QueryRow(t.ctx, `SELECT tenant FROM ahp_longterm.owner_directory WHERE owner_key=$1`, t.owner).Scan(&tenant)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if _, err = t.tx.Exec(t.ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, tenantLock(tenant)); err != nil {
		return false, err
	}
	var frozen bool
	err = t.tx.QueryRow(t.ctx, `SELECT EXISTS(SELECT 1 FROM ahp_longterm.erased_tenants WHERE tenant=$1)`, tenant).Scan(&frozen)
	return frozen, err
}
