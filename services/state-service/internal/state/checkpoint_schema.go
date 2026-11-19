package state

import (
	"context"
	"fmt"
)

// checkpointDDL is the idempotent, non-destructive DDL for the checkpoint part
// of the ahp_state schema. Every statement is CREATE ... IF NOT EXISTS and
// nothing existing is ever altered or dropped. Scope is never stored on these
// tables: it is derived by joining checkpoints to ahp_state.threads.
//
// id is the INTERNAL storage key: for rows created after the isolation fix it
// is a deterministic digest of (thread, namespace, public id), so a caller
// chosen public id can never collide with another thread or namespace.
// external_id is the public checkpoint id and is unique per
// (thread, namespace). checkpoint_parts and checkpoint_writes keep the global
// internal id as their foreign key.
const checkpointDDL = `CREATE SCHEMA IF NOT EXISTS ahp_state;

CREATE TABLE IF NOT EXISTS ahp_state.checkpoints (
    id          text        NOT NULL,
    external_id text,
    thread_id   text        NOT NULL,
    namespace   text        NOT NULL DEFAULT '',
    parent_id   text,
    version     bigint      NOT NULL,
    metadata    jsonb       NOT NULL,
    body_digest text        NOT NULL,
    key         text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT checkpoints_pk PRIMARY KEY (id),
    CONSTRAINT checkpoints_thread_fk FOREIGN KEY (thread_id)
        REFERENCES ahp_state.threads (id) ON DELETE CASCADE,
    CONSTRAINT checkpoints_parent_fk FOREIGN KEY (parent_id)
        REFERENCES ahp_state.checkpoints (id) ON DELETE CASCADE,
    CONSTRAINT checkpoints_thread_ns_key_key UNIQUE (thread_id, namespace, key)
);

CREATE UNIQUE INDEX IF NOT EXISTS checkpoints_thread_ns_external_key
    ON ahp_state.checkpoints (thread_id, namespace, external_id);

CREATE TABLE IF NOT EXISTS ahp_state.checkpoint_blobs (
    thread_id  text        NOT NULL,
    namespace  text        NOT NULL DEFAULT '',
    channel    text        NOT NULL,
    version    text        NOT NULL,
    data       bytea       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT checkpoint_blobs_pk PRIMARY KEY (thread_id, namespace, channel, version),
    CONSTRAINT checkpoint_blobs_thread_fk FOREIGN KEY (thread_id)
        REFERENCES ahp_state.threads (id) ON DELETE CASCADE,
    CONSTRAINT checkpoint_blobs_size CHECK (octet_length(data) <= 1048576)
);

CREATE TABLE IF NOT EXISTS ahp_state.checkpoint_parts (
    checkpoint_id text   NOT NULL,
    channel       text   NOT NULL,
    version       text   NOT NULL,
    CONSTRAINT checkpoint_parts_pk PRIMARY KEY (checkpoint_id, channel),
    CONSTRAINT checkpoint_parts_ck_fk FOREIGN KEY (checkpoint_id)
        REFERENCES ahp_state.checkpoints (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ahp_state.checkpoint_writes (
    checkpoint_id text   NOT NULL,
    thread_id     text   NOT NULL,
    task          text   NOT NULL,
    idx           integer NOT NULL,
    channel       text   NOT NULL,
    data          bytea  NOT NULL,
    CONSTRAINT checkpoint_writes_pk PRIMARY KEY (checkpoint_id, task, idx),
    CONSTRAINT checkpoint_writes_thread_fk FOREIGN KEY (thread_id)
        REFERENCES ahp_state.threads (id) ON DELETE CASCADE
);
`

// checkpointVersionUpgrade converts legacy bigint version columns of local
// tables to opaque text. ALTER COLUMN TYPE is non-destructive: existing rows
// keep their values, rendered via version::text. The data_type guard makes the
// statement a no-op once the column is already text, so migration stays
// idempotent.
const checkpointVersionUpgrade = `
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
              WHERE table_schema = 'ahp_state' AND table_name = 'checkpoint_blobs'
                AND column_name = 'version' AND data_type <> 'text') THEN
    ALTER TABLE ahp_state.checkpoint_blobs
      ALTER COLUMN version TYPE text USING version::text;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns
              WHERE table_schema = 'ahp_state' AND table_name = 'checkpoint_parts'
                AND column_name = 'version' AND data_type <> 'text') THEN
    ALTER TABLE ahp_state.checkpoint_parts
      ALTER COLUMN version TYPE text USING version::text;
  END IF;
END $$;
`

// checkpointIsolationUpgrade backfills external_id for rows created before the
// public/internal id split existed. It is non-destructive: no column, table or
// constraint is dropped, the internal id of an existing row is never rewritten
// (checkpoint_parts and checkpoint_writes keep referencing it), and the
// statement is a no-op once every row carries an external_id.
//
// The legacy schema enforced UNIQUE (thread_id, namespace, id), so within one
// scope the backfill is injective and cannot violate the new unique index.
const checkpointIsolationUpgrade = `
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM ahp_state.checkpoints WHERE external_id IS NULL) THEN
    UPDATE ahp_state.checkpoints SET external_id = id WHERE external_id IS NULL;
  END IF;
END $$;
`

// checkpointWritesUpgrade relaxes the pending-writes table so a write may be
// recorded before its checkpoint header has been flushed. The LangGraph
// checkpoint executor flushes put_writes and put concurrently and gives no
// ordering guarantee between them, so a hard foreign key from a write to the
// checkpoint row rejected legitimate pending writes with NOT_FOUND whenever the
// executor happened to flush the write first; the reference checkpoint's own
// MemorySaver tolerates writes ahead of the header. The constraint is replaced
// by a thread foreign key so thread/subject cascade deletion (control plane)
// still reaches the writes once the checkpoint key is gone. The block is
// idempotent and non-destructive: the legacy checkpoint FK is dropped only if
// it still exists, thread_id is added only once, existing writes are backfilled
// from their checkpoint row, and the thread FK is added only when missing. On a
// database created from the current checkpointDDL every step is a no-op.
const checkpointWritesUpgrade = `
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_constraint
              WHERE conrelid = 'ahp_state.checkpoint_writes'::regclass
                AND conname = 'checkpoint_writes_ck_fk') THEN
    ALTER TABLE ahp_state.checkpoint_writes
      DROP CONSTRAINT checkpoint_writes_ck_fk;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema = 'ahp_state'
                    AND table_name = 'checkpoint_writes'
                    AND column_name = 'thread_id') THEN
    ALTER TABLE ahp_state.checkpoint_writes ADD COLUMN thread_id text;
  END IF;
  UPDATE ahp_state.checkpoint_writes w SET thread_id = c.thread_id
    FROM ahp_state.checkpoints c
   WHERE w.checkpoint_id = c.id AND w.thread_id IS NULL;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                  WHERE conrelid = 'ahp_state.checkpoint_writes'::regclass
                    AND conname = 'checkpoint_writes_thread_fk') THEN
    ALTER TABLE ahp_state.checkpoint_writes
      ADD CONSTRAINT checkpoint_writes_thread_fk FOREIGN KEY (thread_id)
        REFERENCES ahp_state.threads (id) ON DELETE CASCADE;
  END IF;
  CREATE INDEX IF NOT EXISTS checkpoint_writes_thread_idx
    ON ahp_state.checkpoint_writes (thread_id);
END $$;
`

// Empty encoding is legacy opaque data. No payload prefix is ever interpreted
// by the migration; existing rows and their request fingerprints stay unchanged.
const checkpointEncodingUpgrade = `
ALTER TABLE ahp_state.checkpoints ADD COLUMN IF NOT EXISTS metadata_encoding text NOT NULL DEFAULT '';
ALTER TABLE ahp_state.checkpoint_blobs ADD COLUMN IF NOT EXISTS encoding text NOT NULL DEFAULT '';
ALTER TABLE ahp_state.checkpoint_writes ADD COLUMN IF NOT EXISTS encoding text NOT NULL DEFAULT '';
`

// MigrateCheckpoints applies the checkpoint DDL on top of the core migration.
// It is idempotent and non-destructive: existing rows are never removed and no
// table is dropped; the only rewrites are the lossless bigint-to-text version
// upgrade and the external_id backfill of pre-isolation rows. Encoding columns
// default to empty without inspecting or rewriting legacy bytes. checkpoint_parts
// rows only reference existing blobs; that existence is verified in the Go
// transaction that inserts them, not by the schema.
// MigrateCheckpoints applies the checkpoints DDL on the store's own pool. Migrate calls the same body on
// the connection that already holds the migrate advisory lock, which is what
// keeps a MaxConns=1 pool from deadlocking at startup.
func (s *Store) MigrateCheckpoints(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return invalid("state: store is not initialised")
	}
	return s.migrateCheckpoints(ctx, s.pool)
}

func (s *Store) migrateCheckpoints(ctx context.Context, ex ddlExec) error {
	if s == nil || s.pool == nil {
		return invalid("state: store is not initialised")
	}
	if _, err := ex.Exec(ctx, checkpointDDL); err != nil {
		return fmt.Errorf("state: migrate checkpoints: %w", err)
	}
	if _, err := ex.Exec(ctx, checkpointVersionUpgrade); err != nil {
		return fmt.Errorf("state: migrate checkpoints version upgrade: %w", err)
	}
	if _, err := ex.Exec(ctx, checkpointIsolationUpgrade); err != nil {
		return fmt.Errorf("state: migrate checkpoints isolation backfill: %w", err)
	}
	if _, err := ex.Exec(ctx, checkpointWritesUpgrade); err != nil {
		return fmt.Errorf("state: migrate checkpoints pending-write forward reference: %w", err)
	}
	if _, err := ex.Exec(ctx, checkpointEncodingUpgrade); err != nil {
		return fmt.Errorf("state: migrate checkpoint encoding: %w", err)
	}
	return nil
}
