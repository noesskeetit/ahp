// Package postgres stores individually addressable typed records as JSONB rows.
// PostgreSQL owns transactions, owner locks and the indexed wake-up schedule.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ Pool *pgxpool.Pool }

// UsePool joins AHP's existing composition without opening another DB owner.
func UsePool(pool *pgxpool.Pool) *Store { return &Store{Pool: pool} }

func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database configuration: %w", err)
	}
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool}, nil
}
func (s *Store) Close()                         { s.Pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

func (s *Store) Within(ctx context.Context, owner string, fn func(memory.Tx) error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx, "INSERT INTO ahp_longterm.memory_owners(owner_key) VALUES($1) ON CONFLICT DO NOTHING", owner); err != nil {
		return err
	}
	var locked string
	if err := tx.QueryRow(ctx, "SELECT owner_key FROM ahp_longterm.memory_owners WHERE owner_key=$1 FOR UPDATE", owner).Scan(&locked); err != nil {
		return err
	}
	if err := fn(&transaction{ctx: ctx, tx: tx, owner: owner}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) Due(ctx context.Context, at time.Time, limit int) ([]string, error) {
	rows, err := s.Pool.Query(ctx, "SELECT owner_key FROM ahp_longterm.memory_owners WHERE due_at <= $1 ORDER BY due_at LIMIT $2", at, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var owner string
		if err := rows.Scan(&owner); err != nil {
			return nil, err
		}
		out = append(out, owner)
	}
	return out, rows.Err()
}

type transaction struct {
	ctx   context.Context
	tx    pgx.Tx
	owner string
}

func (t *transaction) Get(kind, id string, into any) error {
	var data []byte
	err := t.tx.QueryRow(t.ctx, "SELECT body FROM ahp_longterm.memory_records WHERE owner_key=$1 AND kind=$2 AND record_id=$3", t.owner, kind, id).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return memory.ErrMissing
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, into)
}
func (t *transaction) Put(kind, id string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(t.ctx, `INSERT INTO ahp_longterm.memory_records(owner_key,kind,record_id,body) VALUES($1,$2,$3,$4::jsonb)
ON CONFLICT(owner_key,kind,record_id) DO UPDATE SET body=EXCLUDED.body,updated_at=now()`, t.owner, kind, id, data)
	return err
}
func (t *transaction) Delete(kind, id string) error {
	_, err := t.tx.Exec(t.ctx, "DELETE FROM ahp_longterm.memory_records WHERE owner_key=$1 AND kind=$2 AND record_id=$3", t.owner, kind, id)
	return err
}
func (t *transaction) DeleteKind(kind string) error {
	_, err := t.tx.Exec(t.ctx, "DELETE FROM ahp_longterm.memory_records WHERE owner_key=$1 AND kind=$2", t.owner, kind)
	return err
}
func (t *transaction) List(kind string) ([]json.RawMessage, error) {
	rows, err := t.tx.Query(t.ctx, "SELECT body FROM ahp_longterm.memory_records WHERE owner_key=$1 AND kind=$2 ORDER BY record_id", t.owner, kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []json.RawMessage{}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		out = append(out, json.RawMessage(b))
	}
	return out, rows.Err()
}
func (t *transaction) Schedule(at time.Time) error {
	var due any
	if !at.IsZero() {
		due = at
	}
	_, err := t.tx.Exec(t.ctx, "UPDATE ahp_longterm.memory_owners SET due_at=$2 WHERE owner_key=$1", t.owner, due)
	return err
}

// Migrate uses an advisory lock to serialize replicas of the composition.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(746281093)"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS ahp_longterm"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS ahp_longterm.memory_schema_version(version integer PRIMARY KEY);
CREATE TABLE IF NOT EXISTS ahp_longterm.memory_owners(owner_key text PRIMARY KEY,due_at timestamptz);
CREATE INDEX IF NOT EXISTS memory_owners_due ON ahp_longterm.memory_owners(due_at) WHERE due_at IS NOT NULL;
CREATE TABLE IF NOT EXISTS ahp_longterm.memory_records(
 owner_key text NOT NULL REFERENCES ahp_longterm.memory_owners(owner_key),
 kind text NOT NULL,record_id text NOT NULL,body jsonb NOT NULL,
 updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(owner_key,kind,record_id));
CREATE TABLE IF NOT EXISTS ahp_longterm.erased_tenants(tenant text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS ahp_longterm.owner_directory(
 owner_key text PRIMARY KEY REFERENCES ahp_longterm.memory_owners(owner_key) ON DELETE CASCADE,
 tenant text NOT NULL,subject text NOT NULL,deployment text NOT NULL);
CREATE INDEX IF NOT EXISTS owner_directory_tenant ON ahp_longterm.owner_directory(tenant,subject);
INSERT INTO ahp_longterm.memory_schema_version(version) VALUES(1) ON CONFLICT DO NOTHING;`); err != nil {
		return err
	}
	var directoryMigrated bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_longterm.memory_schema_version WHERE version=2)`).Scan(&directoryMigrated); err != nil {
		return err
	}
	if directoryMigrated {
		return tx.Commit(ctx)
	}
	// Upgrade pre-directory banks from their retained server identities. An
	// erased subject has no turn/job records and must never be re-registered.
	if _, err := tx.Exec(ctx, `INSERT INTO ahp_longterm.owner_directory(owner_key,tenant,subject,deployment)
 SELECT DISTINCT ON(owner_key) owner_key,body->'identity'->>'tenant_id',body->'identity'->>'user_id',body->'identity'->>'actor_id'
 FROM ahp_longterm.memory_records r WHERE kind IN ('turn','job')
 AND COALESCE(body->'identity'->>'tenant_id','')<>'' AND COALESCE(body->'identity'->>'user_id','')<>''
 AND COALESCE(body->'identity'->>'actor_id','')<>''
 AND NOT EXISTS(SELECT 1 FROM ahp_longterm.memory_records f WHERE f.owner_key=r.owner_key AND f.kind='fence' AND f.record_id='subject' AND f.body='true'::jsonb)
 ORDER BY owner_key,updated_at DESC ON CONFLICT(owner_key) DO NOTHING`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO ahp_longterm.memory_schema_version(version) VALUES(2)`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
