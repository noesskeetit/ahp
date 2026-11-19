package state

import (
	"context"
	"fmt"
)

// kvDDL is the idempotent, non-destructive DDL for the scoped KV part of the
// ahp_state schema. Scope (tenant, deployment, subject) is stored as server
// columns on every row and is part of the primary key, so a caller-supplied
// namespace can never address another subject's data.
//
// Deletions keep a tombstone row (deleted = true) that carries the last
// version, so a delete/recreate cycle can never hand out a version that an
// earlier writer already saw (no ABA on the CAS counter).
const kvDDL = `CREATE SCHEMA IF NOT EXISTS ahp_state;

CREATE TABLE IF NOT EXISTS ahp_state.kv (
    tenant        text        NOT NULL,
    deployment    text        NOT NULL,
    subject       text        NOT NULL,
    namespace     text[]      NOT NULL DEFAULT '{}',
    key           text        NOT NULL,
    value         jsonb       NOT NULL,
    version       bigint      NOT NULL,
    deleted       boolean     NOT NULL DEFAULT false,
    thread_id     text,
    updated_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT kv_pk PRIMARY KEY (tenant, deployment, subject, namespace, key),
    CONSTRAINT kv_value_size CHECK (octet_length(value::text) <= 1048576),
    -- A tombstone carries no payload: version is preserved, value is cleared.
    CONSTRAINT kv_tombstone_shape CHECK (deleted OR octet_length(value::text) > 0),
    CONSTRAINT kv_thread_fk FOREIGN KEY (thread_id)
        REFERENCES ahp_state.threads (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS kv_scope_ns_idx
    ON ahp_state.kv (tenant, deployment, subject, namespace)
    WHERE deleted = false;

-- Upgrade path for a kv table that predates the tombstone column.
ALTER TABLE ahp_state.kv ADD COLUMN IF NOT EXISTS deleted boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS ahp_state.kv_ops (
    tenant          text        NOT NULL,
    deployment      text        NOT NULL,
    subject         text        NOT NULL,
    operation_key   text        NOT NULL,
    request_digest  text        NOT NULL,
    result          jsonb       NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT kv_ops_pk PRIMARY KEY (tenant, deployment, subject, operation_key)
);
`

// MigrateKV applies the KV DDL on top of the core migration. It is idempotent
// and non-destructive: existing rows are never modified or removed.
// MigrateKV applies the kv DDL on the store's own pool. Migrate calls the same body on
// the connection that already holds the migrate advisory lock, which is what
// keeps a MaxConns=1 pool from deadlocking at startup.
func (s *Store) MigrateKV(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return invalid("state: store is not initialised")
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	return s.migrateKV(ctx, conn)
}

func (s *Store) migrateKV(ctx context.Context, ex ddlExec) error {
	if s == nil || s.pool == nil {
		return invalid("state: store is not initialised")
	}
	if _, err := ex.Exec(ctx, kvDDL+kvOwnerCatalogDDL); err != nil {
		return fmt.Errorf("state: migrate kv: %w", err)
	}
	if len(s.kvOwnerPolicies) > 0 {
		q, ok := ex.(kvPolicyConnection)
		if !ok {
			return invalid("owner policy requires a query-capable migration connection")
		}
		return s.registerKVPolicies(ctx, q)
	}
	return nil
}
