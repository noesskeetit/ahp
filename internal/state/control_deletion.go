package state

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

const controlDeletionSchema = `CREATE TABLE IF NOT EXISTS ahp_state.control_deletions (
 id text PRIMARY KEY,tenant text NOT NULL,actor text NOT NULL,op_key text NOT NULL,subject text NOT NULL,
 plan jsonb NOT NULL,steps text[] NOT NULL DEFAULT '{}',completed boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(),UNIQUE(tenant,actor,op_key)
);
ALTER TABLE ahp_state.control_deletions ADD COLUMN IF NOT EXISTS service_authorized boolean NOT NULL DEFAULT false;
ALTER TABLE ahp_state.control_deletions ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE ahp_state.control_deletions ADD COLUMN IF NOT EXISTS kind text NOT NULL DEFAULT 'subject' CHECK(kind IN ('subject','tenant'));`

type ControlDeletion struct {
	ID, Tenant, Actor, Key, Subject string
	Kind                            string
	Plan                            json.RawMessage
	Steps                           []string
	Completed                       bool
	ServiceAuthorized               bool
}

func (s *Store) GetControlDeletion(ctx context.Context, tenant, actor, key string) (ControlDeletion, error) {
	d := ControlDeletion{Tenant: tenant, Actor: actor, Key: key}
	err := s.pool.QueryRow(ctx, `SELECT id,subject,plan,steps,completed,service_authorized,kind FROM ahp_state.control_deletions WHERE tenant=$1 AND actor=$2 AND op_key=$3`, tenant, actor, key).Scan(&d.ID, &d.Subject, &d.Plan, &d.Steps, &d.Completed, &d.ServiceAuthorized, &d.Kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, contracts.NewError(contracts.ErrNotFound, "state: control deletion not found")
	}
	return d, err
}
func (s *Store) BeginControlDeletion(ctx context.Context, scope contracts.Scope, actor, key string, plan json.RawMessage, service ...bool) (ControlDeletion, error) {
	if err := scope.Validate(); err != nil {
		return ControlDeletion{}, err
	}
	if err := validIdentifier(key, "deletion key"); err != nil {
		return ControlDeletion{}, err
	}
	authorized := len(service) == 1 && service[0]
	_, err := s.pool.Exec(ctx, `INSERT INTO ahp_state.control_deletions(id,tenant,actor,op_key,subject,plan,service_authorized) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, contracts.NewID(), scope.Tenant, actor, key, scope.Subject, plan, authorized)
	if err != nil {
		return ControlDeletion{}, err
	}
	d, err := s.GetControlDeletion(ctx, scope.Tenant, actor, key)
	if err == nil && (d.Subject != scope.Subject || d.Kind != "subject") {
		return d, contracts.NewError(contracts.ErrKeyReuse, "state: deletion key reused for another subject")
	}
	return d, err
}
func (s *Store) AckControlDeletion(ctx context.Context, d ControlDeletion, step string, complete bool) error {
	_, err := s.pool.Exec(ctx, `UPDATE ahp_state.control_deletions SET steps=CASE WHEN $4=ANY(steps) THEN steps ELSE array_append(steps,$4) END,completed=completed OR $5 WHERE id=$1 AND tenant=$2 AND actor=$3`, d.ID, d.Tenant, d.Actor, step, complete)
	return err
}

// SubjectScopes is the owner API used for a tenant-wide control-plane erasure.
func (s *Store) SubjectScopes(ctx context.Context, tenant, subject string) ([]contracts.Scope, error) {
	rows, err := s.pool.Query(ctx, `SELECT deployment FROM ahp_state.threads WHERE tenant=$1 AND subject=$2 UNION SELECT deployment FROM ahp_state.kv WHERE tenant=$1 AND subject=$2 ORDER BY deployment`, tenant, subject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []contracts.Scope
	for rows.Next() {
		scope := contracts.Scope{Tenant: tenant, Subject: subject}
		if err = rows.Scan(&scope.Deployment); err != nil {
			return nil, err
		}
		out = append(out, scope)
	}
	return out, rows.Err()
}
func (s *Store) SubjectHeld(ctx context.Context, tenant, subject string) (bool, error) {
	var held bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.legalhold WHERE tenant=$1 AND (subject='' OR subject=$2))`, tenant, subject).Scan(&held)
	return held, err
}

// LockSubjectControl serializes control jobs and legalhold changes. The
// returned release must run even on a partial result; no lock crosses retry.
func (s *Store) LockSubjectControl(ctx context.Context, tenant, subject string) (func(), error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, "state-control-tenant/"+tenant); err != nil {
		_ = tx.Rollback(context.Background())
		return nil, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "state-control/"+tenant+"/"+subject); err != nil {
		_ = tx.Rollback(context.Background())
		return nil, err
	}
	return func() { _ = tx.Rollback(context.Background()) }, nil
}
func (s *Store) UpdateControlDeletionPlan(ctx context.Context, d ControlDeletion, plan json.RawMessage) error {
	_, err := s.pool.Exec(ctx, `UPDATE ahp_state.control_deletions SET plan=$4 WHERE id=$1 AND tenant=$2 AND actor=$3 AND NOT completed`, d.ID, d.Tenant, d.Actor, plan)
	return err
}

// PendingControlDeletions returns only intents admitted with service authority.
// Legacy jobs lacking that explicit authority can only resume after human auth.
func (s *Store) PendingControlDeletions(ctx context.Context, limit int) ([]ControlDeletion, error) {
	if limit < 1 || limit > 100 {
		limit = 32
	}
	rows, err := s.pool.Query(ctx, `WITH selected AS (
 SELECT id FROM ahp_state.control_deletions WHERE service_authorized AND NOT completed AND next_attempt_at<=now()
 ORDER BY next_attempt_at,created_at,id LIMIT $1 FOR UPDATE SKIP LOCKED
 ) UPDATE ahp_state.control_deletions d SET next_attempt_at=now()+interval '5 seconds' FROM selected s WHERE d.id=s.id
 RETURNING d.id,d.tenant,d.actor,d.op_key,d.subject,d.plan,d.steps,d.completed,d.service_authorized,d.kind`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ControlDeletion
	for rows.Next() {
		var d ControlDeletion
		if err = rows.Scan(&d.ID, &d.Tenant, &d.Actor, &d.Key, &d.Subject, &d.Plan, &d.Steps, &d.Completed, &d.ServiceAuthorized, &d.Kind); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) BeginTenantControlDeletion(ctx context.Context, scope contracts.Scope, actor, key string, plan json.RawMessage) (ControlDeletion, error) {
	if err := scope.Validate(); err != nil {
		return ControlDeletion{}, err
	}
	if err := validIdentifier(key, "deletion key"); err != nil {
		return ControlDeletion{}, err
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO ahp_state.control_deletions(id,tenant,actor,op_key,subject,plan,kind,service_authorized) VALUES($1,$2,$3,$4,'',$5,'tenant',true) ON CONFLICT DO NOTHING`, contracts.NewID(), scope.Tenant, actor, key, plan)
	if err != nil {
		return ControlDeletion{}, err
	}
	d, err := s.GetControlDeletion(ctx, scope.Tenant, actor, key)
	if err == nil && d.Kind != "tenant" {
		return d, contracts.NewError(contracts.ErrKeyReuse, "state: deletion key belongs to another operation")
	}
	return d, err
}
func (s *Store) LockTenantControl(ctx context.Context, tenant string) (func(), error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "state-control-tenant/"+tenant); err != nil {
		_ = tx.Rollback(context.Background())
		return nil, err
	}
	return func() { _ = tx.Rollback(context.Background()) }, nil
}
func (s *Store) TenantHeld(ctx context.Context, tenant string) (bool, error) {
	var held bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.legalhold WHERE tenant=$1)`, tenant).Scan(&held)
	return held, err
}
