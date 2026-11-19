-- State API durable schema (task 02a: threads, runs, leases).
--
-- Every statement here is idempotent and non-destructive: re-running the
-- migration on a database that already carries the schema is a no-op and
-- never drops or rewrites existing rows. All tables live in the dedicated
-- ahp_state schema owned exclusively by the State API service.

CREATE SCHEMA IF NOT EXISTS ahp_state;

-- One thread per (tenant, deployment, subject, context_id): the durable
-- conversation/execution lane a FIFO sequence of runs is executed on.
CREATE TABLE IF NOT EXISTS ahp_state.threads (
    id             text PRIMARY KEY,
    tenant         text NOT NULL,
    deployment     text NOT NULL,
    subject        text NOT NULL,
    context_id     text NOT NULL,
    epoch          bigint NOT NULL DEFAULT 0,
    created_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    -- Current single lease holder of the thread. A lease always covers the
    -- run named here; clearing the lease clears both columns.
    lease_run_id   text,
    lease_worker   text,
    lease_epoch    bigint,
    lease_expires  timestamptz,
    -- Idempotency: keys are scoped by tenant/deployment/subject and the
    -- canonical body fingerprint includes the context id.
    idem_key       text NOT NULL,
    body_digest    text NOT NULL,
    CONSTRAINT threads_scope_key_unique UNIQUE (tenant, deployment, subject, idem_key),
    CONSTRAINT threads_context_unique UNIQUE (tenant, deployment, subject, context_id),
    -- A lease is all-or-nothing.
    CONSTRAINT threads_lease_coherent CHECK (
        (lease_run_id IS NULL AND lease_worker IS NULL AND lease_epoch IS NULL AND lease_expires IS NULL)
        OR
        (lease_run_id IS NOT NULL AND lease_worker IS NOT NULL AND lease_epoch IS NOT NULL AND lease_expires IS NOT NULL)
    ),
    CONSTRAINT threads_epoch_nonnegative CHECK (epoch >= 0)
);

CREATE INDEX IF NOT EXISTS threads_lookup_idx
    ON ahp_state.threads (tenant, deployment, subject, id);

-- Coarse per-thread sequence high-water for the results-only retention experiment.
-- In results-only the individual model_delta fragments are never written to
-- stream_events, so MAX(sequence) no longer advances past them. This single column
-- (not one marker per fragment) is advanced at every boundary flush to the highest
-- sequence ever handed to the volatile tail, so durable records (step results, the
-- terminal status frame) and every reopened tail continue strictly ABOVE the live
-- cursors already served and never reset or collide with them. It stays 0 in the
-- chunk/step/turn journal modes, where every sequence-changing GREATEST below is a
-- numeric no-op and no behaviour changes.
ALTER TABLE ahp_state.threads
    ADD COLUMN IF NOT EXISTS stream_high_water bigint NOT NULL DEFAULT 0;

-- One run: a queued (or executing, or finished) unit of work inside a thread.
-- sequence is dense and monotone per thread; the unique index is what makes
-- concurrent CreateRun calls produce distinct, ordered positions.
CREATE TABLE IF NOT EXISTS ahp_state.runs (
    id            text PRIMARY KEY,
    thread_id     text NOT NULL REFERENCES ahp_state.threads (id) ON DELETE CASCADE,
    task_id       text NOT NULL,
    status        text NOT NULL,
    wait_for      text NOT NULL DEFAULT '',
    sequence      bigint NOT NULL,
    payload       jsonb NOT NULL,
    body_digest   text NOT NULL,
    deadline      timestamptz,
    created_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    -- Idempotency: keys are scoped by thread and the canonical body
    -- fingerprint covers task id and payload.
    idem_key      text NOT NULL,
    CONSTRAINT runs_status_enum CHECK (
        status IN ('pending', 'running', 'waiting', 'done', 'failed', 'canceled')
    ),
    CONSTRAINT runs_thread_sequence_unique UNIQUE (thread_id, sequence),
    CONSTRAINT runs_thread_idem_unique UNIQUE (thread_id, idem_key),
    -- Only a waiting run carries a wait target; only a waiting run may carry
    -- a wake deadline. A waiting run without a deadline sleeps until an inbox
    -- event wakes it, so the deadline stays optional.
    CONSTRAINT runs_wait_for_shape CHECK (
        (status = 'waiting' AND wait_for <> '') OR (status <> 'waiting' AND wait_for = '')
    ),
    CONSTRAINT runs_deadline_shape CHECK (
        status = 'waiting' OR deadline IS NULL
    )
);

CREATE INDEX IF NOT EXISTS runs_thread_order_idx
    ON ahp_state.runs (thread_id, sequence);

-- Durable scoped thread listing (chat/project list, PDF 3.2): the indexed
-- access path for ListThreads. Threads are scoped by tenant/deployment/subject
-- (all taken from the token) and ordered newest-first by (created_at DESC,
-- id DESC) for stable deterministic keyset pagination. This index serves both
-- the scope equality filter and the ORDER BY / keyset bound, so listing stays
-- an index scan rather than a sort, and is stable under concurrent insert.
CREATE INDEX IF NOT EXISTS threads_scope_order_idx
    ON ahp_state.threads (tenant, deployment, subject, created_at DESC, id DESC);

-- Transport ancestry is separate from the opaque business payload and idempotency digest.
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ahp_state' AND table_name='runs' AND column_name='traceparent') THEN
  ALTER TABLE ahp_state.runs ADD COLUMN traceparent text NOT NULL DEFAULT '';
 END IF;
END $$;
