CREATE SCHEMA IF NOT EXISTS ahp_journal;

CREATE TABLE IF NOT EXISTS ahp_journal.sessions (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    deployment_id text NOT NULL,
    subject_id text NOT NULL,
    harness_type text NOT NULL,
    format_version integer NOT NULL CHECK (format_version > 0),
    status text NOT NULL DEFAULT 'IDLE'
        CHECK (status IN ('ACTIVE', 'IDLE', 'WAITING_INPUT', 'INTERRUPTED', 'DELETED')),
    lease_owner text,
    lease_epoch bigint NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at timestamptz,
    head_seq bigint NOT NULL DEFAULT 0 CHECK (head_seq >= 0),
    leaf_entry_id text,
    append_parent_id text,
    parent_session_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, session_id),
    CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL))
) PARTITION BY HASH (tenant_id, session_id);

CREATE INDEX IF NOT EXISTS sessions_scope_idx
    ON ahp_journal.sessions (tenant_id, deployment_id, subject_id, session_id)
    WHERE status <> 'DELETED';

ALTER TABLE ahp_journal.sessions
    ADD COLUMN IF NOT EXISTS turn_input_seq bigint,
    ADD COLUMN IF NOT EXISTS queued_at timestamptz;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='ahp_journal.sessions'::regclass AND conname='sessions_queue_state_check'
    ) THEN
        ALTER TABLE ahp_journal.sessions DROP CONSTRAINT IF EXISTS sessions_status_check;
        ALTER TABLE ahp_journal.sessions ADD CONSTRAINT sessions_queue_state_check CHECK (
            status IN ('QUEUED','ACTIVE','WAITING_INPUT','IDLE','INTERRUPTED','DELETED')
            AND ((queued_at IS NULL) = (turn_input_seq IS NULL))
            AND (status <> 'QUEUED' OR turn_input_seq IS NOT NULL)
            AND (turn_input_seq IS NULL OR (status IN ('QUEUED','ACTIVE') AND turn_input_seq > 0))
        );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS sessions_queue_idx
    ON ahp_journal.sessions (tenant_id, deployment_id, subject_id, queued_at, session_id)
    WHERE status='QUEUED';

CREATE INDEX IF NOT EXISTS sessions_recovery_idx
    ON ahp_journal.sessions (tenant_id, deployment_id, subject_id, lease_expires_at, session_id)
    WHERE status='ACTIVE' AND turn_input_seq IS NOT NULL;

CREATE INDEX IF NOT EXISTS sessions_sweeper_idx
    ON ahp_journal.sessions (tenant_id, lease_expires_at, session_id)
    WHERE status='ACTIVE' AND turn_input_seq IS NOT NULL;

ALTER TABLE ahp_journal.sessions
    ADD COLUMN IF NOT EXISTS updated_at timestamptz,
    ADD COLUMN IF NOT EXISTS agent_id text,
    ADD COLUMN IF NOT EXISTS native_key text,
    ADD COLUMN IF NOT EXISTS title text,
    ADD COLUMN IF NOT EXISTS prompt_ref text,
    ADD COLUMN IF NOT EXISTS summary_metadata jsonb,
    ADD COLUMN IF NOT EXISTS head_checkpoint_id text,
    ADD COLUMN IF NOT EXISTS deleted_at timestamptz,
    ADD COLUMN IF NOT EXISTS legal_hold boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS blob_stage_at timestamptz,
    ADD COLUMN IF NOT EXISTS retention_started_at timestamptz,
    ADD COLUMN IF NOT EXISTS retention_swept_at timestamptz;

UPDATE ahp_journal.sessions SET updated_at=created_at WHERE updated_at IS NULL;
ALTER TABLE ahp_journal.sessions
    ALTER COLUMN updated_at SET DEFAULT clock_timestamp(),
    ALTER COLUMN updated_at SET NOT NULL;

-- Pre-upgrade tombstones carry no deletion time. Age these from migration.
UPDATE ahp_journal.sessions SET deleted_at=clock_timestamp()
    WHERE status='DELETED' AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS sessions_retention_idx
    ON ahp_journal.sessions (tenant_id, deleted_at, session_id)
    WHERE status='DELETED' AND retention_swept_at IS NULL;

CREATE INDEX IF NOT EXISTS sessions_catalog_idx
    ON ahp_journal.sessions (tenant_id, deployment_id, subject_id, harness_type, updated_at DESC, session_id DESC)
    WHERE status <> 'DELETED';

CREATE TABLE IF NOT EXISTS ahp_journal.session_events (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    seq bigint NOT NULL CHECK (seq > 0),
    event_uid text NOT NULL,
    parent_entry_id text,
    event_type text NOT NULL,
    role text NOT NULL,
    codec text NOT NULL,
    inline_payload bytea,
    blob_ref jsonb,
    operation_key text,
    request_digest bytea,
    result_head_seq bigint,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, session_id, seq),
    UNIQUE (tenant_id, session_id, event_uid),
    FOREIGN KEY (tenant_id, session_id)
        REFERENCES ahp_journal.sessions (tenant_id, session_id) ON DELETE CASCADE,
    CHECK (num_nonnulls(inline_payload, blob_ref) = 1),
    CHECK (octet_length(inline_payload) <= 262144),
    CHECK (num_nonnulls(operation_key, request_digest, result_head_seq) IN (0, 3)),
    CHECK (octet_length(request_digest) = 32 AND result_head_seq >= seq)
) PARTITION BY HASH (tenant_id, session_id);

CREATE UNIQUE INDEX IF NOT EXISTS session_events_operation_key
    ON ahp_journal.session_events (tenant_id, session_id, operation_key)
    WHERE operation_key IS NOT NULL;

-- The FIFO projection is opt-in for newly admitted Dots sessions. Native
-- session rows and their immutable event payloads need no backfill.
ALTER TABLE ahp_journal.sessions
    ADD COLUMN IF NOT EXISTS dots_fifo boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS dots_run_sequence bigint NOT NULL DEFAULT 0;

ALTER TABLE ahp_journal.session_events
    ADD COLUMN IF NOT EXISTS dots_run_id text,
    ADD COLUMN IF NOT EXISTS dots_run_sequence bigint,
    ADD COLUMN IF NOT EXISTS dots_task_id text,
    ADD COLUMN IF NOT EXISTS dots_closed_at timestamptz;

CREATE UNIQUE INDEX IF NOT EXISTS dots_run_order_idx
    ON ahp_journal.session_events (tenant_id, session_id, dots_run_sequence)
    WHERE dots_run_sequence IS NOT NULL;

ALTER TABLE ahp_journal.session_events
    ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS is_compacted boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS content_text text,
    ADD COLUMN IF NOT EXISTS otel_trace_id text,
    ADD COLUMN IF NOT EXISTS otel_span_id text;

ALTER TABLE ahp_journal.session_events
    ADD COLUMN IF NOT EXISTS search_tsv tsvector GENERATED ALWAYS AS (
        CASE WHEN role IN ('user', 'assistant', 'compaction')
             THEN to_tsvector('simple'::regconfig, left(content_text, 32768))
        END
    ) STORED;

CREATE INDEX IF NOT EXISTS session_events_fts_idx
    ON ahp_journal.session_events USING GIN (search_tsv)
    WHERE role IN ('user', 'assistant', 'compaction');

CREATE TABLE IF NOT EXISTS ahp_journal.checkpoints (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    checkpoint_ns text NOT NULL,
    checkpoint_id text NOT NULL,
    parent_checkpoint_id text,
    snapshot_seq bigint NOT NULL CHECK (snapshot_seq >= 0),
    metadata jsonb NOT NULL,
    channel_versions jsonb NOT NULL,
    codec text NOT NULL,
    inline_payload bytea,
    blob_ref jsonb,
    operation_key text NOT NULL,
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, session_id, checkpoint_ns, checkpoint_id),
    UNIQUE (tenant_id, session_id, operation_key),
    FOREIGN KEY (tenant_id, session_id)
        REFERENCES ahp_journal.sessions (tenant_id, session_id) ON DELETE CASCADE,
    CHECK (num_nonnulls(inline_payload, blob_ref) = 1),
    CHECK (octet_length(inline_payload) <= 262144)
) PARTITION BY HASH (tenant_id, session_id);

ALTER TABLE ahp_journal.checkpoints
    ADD COLUMN IF NOT EXISTS canonical_request_digest bytea
        CHECK (octet_length(canonical_request_digest) = 32),
    ADD COLUMN IF NOT EXISTS chat_event_id text;

CREATE TABLE IF NOT EXISTS ahp_journal.checkpoint_writes (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    checkpoint_ns text NOT NULL,
    checkpoint_id text NOT NULL,
    task_id text NOT NULL,
    task_path text NOT NULL DEFAULT '',
    idx integer NOT NULL,
    channel text NOT NULL,
    codec text NOT NULL,
    inline_payload bytea,
    blob_ref jsonb,
    PRIMARY KEY (tenant_id, session_id, checkpoint_ns, checkpoint_id, task_id, idx),
    -- LangGraph can call put_writes before it calls put for the same ID.
    -- Bind to the leased session even when the checkpoint header is absent.
    FOREIGN KEY (tenant_id, session_id)
        REFERENCES ahp_journal.sessions (tenant_id, session_id) ON DELETE CASCADE,
    CHECK (num_nonnulls(inline_payload, blob_ref) = 1),
    CHECK (octet_length(inline_payload) <= 262144)
) PARTITION BY HASH (tenant_id, session_id);

CREATE TABLE IF NOT EXISTS ahp_journal.scoped_kv (
    tenant_id text NOT NULL,
    deployment_id text NOT NULL,
    scope_type text NOT NULL CHECK (scope_type IN ('session', 'user', 'app')),
    scope_owner text NOT NULL,
    namespace text NOT NULL,
    key text NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    inline_payload bytea,
    blob_ref jsonb,
    PRIMARY KEY (tenant_id, deployment_id, scope_type, scope_owner, namespace, key),
    CHECK (num_nonnulls(inline_payload, blob_ref) = 1),
    CHECK (octet_length(inline_payload) <= 262144)
) PARTITION BY HASH (tenant_id);

ALTER TABLE ahp_journal.scoped_kv
    ADD COLUMN IF NOT EXISTS deleted boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT clock_timestamp();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='ahp_journal.scoped_kv'::regclass AND conname='scoped_kv_scope_check'
    ) THEN
        ALTER TABLE ahp_journal.scoped_kv DROP CONSTRAINT IF EXISTS scoped_kv_scope_type_check;
        ALTER TABLE ahp_journal.scoped_kv ADD CONSTRAINT scoped_kv_scope_check
            CHECK (scope_type IN ('session','user','app','store'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='ahp_journal.scoped_kv'::regclass AND conname='scoped_kv_blob_session_only'
    ) THEN
        ALTER TABLE ahp_journal.scoped_kv ADD CONSTRAINT scoped_kv_blob_session_only
            CHECK (scope_type='session' OR blob_ref IS NULL);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ahp_journal.tool_intents (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    call_id text NOT NULL,
    tool_name text NOT NULL,
    args_digest bytea NOT NULL CHECK (octet_length(args_digest) = 32),
    idempotency_key text NOT NULL,
    gateway_operation_id text,
    status text NOT NULL CHECK (status IN ('PREPARED', 'DISPATCHED', 'COMPLETED', 'OUTCOME_UNKNOWN')),
    response_payload bytea,
    response_blob_ref jsonb,
    PRIMARY KEY (tenant_id, session_id, call_id),
    UNIQUE (tenant_id, session_id, idempotency_key),
    FOREIGN KEY (tenant_id, session_id)
        REFERENCES ahp_journal.sessions (tenant_id, session_id) ON DELETE CASCADE,
    CHECK (octet_length(response_payload) <= 262144),
    CHECK (num_nonnulls(response_payload, response_blob_ref) <= 1)
) PARTITION BY HASH (tenant_id, session_id);

ALTER TABLE ahp_journal.tool_intents
    ADD COLUMN IF NOT EXISTS prepared_epoch bigint CHECK (prepared_epoch > 0);

CREATE INDEX IF NOT EXISTS tool_intents_unresolved_idx
    ON ahp_journal.tool_intents (tenant_id, session_id, call_id)
    WHERE status IN ('PREPARED', 'DISPATCHED');

-- Lineage is validated by the leased writer. Parent row references would
-- otherwise prevent bounded retention of the original history.
DO $$
DECLARE
    relation regclass;
    old_constraint text;
BEGIN
    FOREACH relation IN ARRAY ARRAY['ahp_journal.session_events'::regclass, 'ahp_journal.checkpoints'::regclass]
    LOOP
        SELECT conname INTO old_constraint FROM pg_constraint
        WHERE conrelid=relation AND confrelid=relation AND contype='f' AND conparentid=0;
        IF old_constraint IS NOT NULL THEN
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', relation, old_constraint);
        END IF;
    END LOOP;
END $$;

DO $partitions$
DECLARE
    relation text;
    remainder integer;
BEGIN
    FOREACH relation IN ARRAY ARRAY[
        'sessions', 'session_events', 'checkpoints',
        'checkpoint_writes', 'scoped_kv', 'tool_intents'
    ] LOOP
        FOR remainder IN 0..3 LOOP
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS ahp_journal.%I PARTITION OF ahp_journal.%I
                 FOR VALUES WITH (MODULUS 4, REMAINDER %s)',
                relation || '_p' || remainder, relation, remainder
            );
        END LOOP;
    END LOOP;
END
$partitions$;

CREATE INDEX IF NOT EXISTS sessions_global_turn_sweeper_idx
    ON ahp_journal.sessions (lease_expires_at, tenant_id)
    WHERE status='ACTIVE' AND turn_input_seq IS NOT NULL;

CREATE INDEX IF NOT EXISTS sessions_global_retention_sweeper_idx
    ON ahp_journal.sessions (deleted_at, tenant_id)
    WHERE status='DELETED' AND retention_swept_at IS NULL AND NOT legal_hold;

CREATE INDEX IF NOT EXISTS sessions_global_stage_sweeper_idx
    ON ahp_journal.sessions (blob_stage_at, tenant_id)
    WHERE blob_stage_at IS NOT NULL AND NOT legal_hold AND status <> 'DELETED';
