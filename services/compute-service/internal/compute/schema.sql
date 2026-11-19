-- Compute durable schema (task 12 / MS-5: Computers, incarnations, volume
-- fencing, activity leases, command ledger, snapshots, environment and
-- workspace revisions).
--
-- Every statement is idempotent and non-destructive: re-running the migration
-- is a no-op that never drops or rewrites existing rows. All tables live in the
-- dedicated ahp_compute schema owned exclusively by the Compute service.

CREATE SCHEMA IF NOT EXISTS ahp_compute;

-- A Computer is the durable logical compute unit. For personal and project
-- profiles it outlives incarnation churn; ephemeral Computers exist only to
-- drive a single session.
CREATE TABLE IF NOT EXISTS ahp_compute.computers (
    id           text PRIMARY KEY,
    tenant       text NOT NULL,
    deployment   text NOT NULL,
    subject      text NOT NULL,
    class        text NOT NULL,
    profile      text NOT NULL,
    status       text NOT NULL,
    volume_id    text NOT NULL,
    workspace_id text NOT NULL DEFAULT '',
    realm        text NOT NULL DEFAULT '',
    incarnation  bigint NOT NULL DEFAULT 0,
    quota        jsonb  NOT NULL DEFAULT '{}'::jsonb,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    idem_key     text NOT NULL,
    body_digest  text NOT NULL,
    CONSTRAINT computers_scope_key_unique UNIQUE (tenant, deployment, subject, idem_key),
    CONSTRAINT computers_class_check CHECK (class IN ('S','C','X')),
    CONSTRAINT computers_profile_check CHECK (profile IN ('ephemeral','project','personal')),
    CONSTRAINT computers_status_check CHECK (status IN ('provisioning','active','suspended','destroyed')),
    -- A personal Computer is shared by one Dot's chats: at most one
    -- non-destroyed personal Computer per (tenant, deployment, subject, realm), and only
    -- class S may own one.
    CONSTRAINT computers_personal_trusted CHECK (profile <> 'personal' OR class = 'S'),
    CONSTRAINT computers_incarnation_nonneg CHECK (incarnation >= 0)
);

-- PDF 3.6 keys personal Computers by (tenant, user, dot, realm). Deployment is
-- the Dot dimension. Install the complete key BEFORE removing the old stricter
-- index so upgrades retain uniqueness throughout and preserve all existing rows.
CREATE UNIQUE INDEX IF NOT EXISTS computers_personal_by_dot_active_idx
    ON ahp_compute.computers (tenant, deployment, subject, realm)
    WHERE profile = 'personal' AND status <> 'destroyed';
DROP INDEX IF EXISTS ahp_compute.computers_personal_active_idx;

-- Project Computers are pinned to a workspace: at most one non-destroyed
-- project Computer per (tenant, deployment, subject, workspace_id).
CREATE UNIQUE INDEX IF NOT EXISTS computers_project_active_idx
    ON ahp_compute.computers (tenant, deployment, subject, workspace_id)
    WHERE profile = 'project' AND status <> 'destroyed';

CREATE INDEX IF NOT EXISTS computers_lookup_idx
    ON ahp_compute.computers (tenant, deployment, subject, id);

-- One incarnation is a single disposable VM. It is never returned to the pool.
CREATE TABLE IF NOT EXISTS ahp_compute.incarnations (
    id          text PRIMARY KEY,
    computer_id text NOT NULL REFERENCES ahp_compute.computers (id) ON DELETE CASCADE,
    seq         bigint NOT NULL,
    status      text NOT NULL,
    backend     text NOT NULL DEFAULT '',
    node        text NOT NULL DEFAULT '',
    vmgenid     text NOT NULL DEFAULT '',
    fence       bigint NOT NULL DEFAULT 0,
    acquired_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    stopped_at  timestamptz,
    CONSTRAINT incarnations_status_check CHECK (status IN ('acquiring','running','stopped')),
    CONSTRAINT incarnations_seq_positive CHECK (seq >= 1),
    CONSTRAINT incarnations_computer_seq_unique UNIQUE (computer_id, seq)
);

-- Volume fencing: one monotonically increasing attachment epoch per volume.
-- Every attach bumps the fence; a holder presenting a stale fence loses.
CREATE TABLE IF NOT EXISTS ahp_compute.volume_fences (
    computer_id text NOT NULL REFERENCES ahp_compute.computers (id) ON DELETE CASCADE,
    volume_id   text NOT NULL,
    fence       bigint NOT NULL DEFAULT 0,
    incarnation text NOT NULL DEFAULT '',
    updated_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (computer_id, volume_id),
    CONSTRAINT volume_fences_nonneg CHECK (fence >= 0)
);

-- Activity leases keep a Computer awake. When no unexpired lease is held the
-- Computer is allowed to sleep (no incarnation running). Background commands
-- hold a lease so they survive idle; processes do not survive sleep.
CREATE TABLE IF NOT EXISTS ahp_compute.activity_leases (
    id          text PRIMARY KEY,
    computer_id text NOT NULL REFERENCES ahp_compute.computers (id) ON DELETE CASCADE,
    incarnation bigint NOT NULL DEFAULT 0,
    kind        text NOT NULL,
    holder      text NOT NULL,
    expires     timestamptz NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT activity_leases_kind_check CHECK (kind IN ('command','pty','browser','export','writer')),
    -- An exclusive writer lease is unique per Computer at a time.
    CONSTRAINT activity_leases_writer_unique UNIQUE (computer_id, kind, holder)
);

CREATE INDEX IF NOT EXISTS activity_leases_active_idx
    ON ahp_compute.activity_leases (computer_id, expires);

-- The durable command ledger. Idempotency is scoped to the Computer and the
-- body fingerprint covers the command and cwd, so replaying a key with the same
-- body returns the original record and a different body is KEY_REUSE_MISMATCH.
CREATE TABLE IF NOT EXISTS ahp_compute.commands (
    id               text PRIMARY KEY,
    tenant           text NOT NULL,
    deployment       text NOT NULL,
    subject          text NOT NULL,
    computer_id      text NOT NULL REFERENCES ahp_compute.computers (id) ON DELETE CASCADE,
    incarnation      bigint NOT NULL DEFAULT 0,
    key              text NOT NULL,
    execution_id     text NOT NULL DEFAULT '',
    status           text NOT NULL,
    command          text NOT NULL,
    cwd              text NOT NULL DEFAULT '',
    stdout           text NOT NULL DEFAULT '',
    stderr           text NOT NULL DEFAULT '',
    exit_code        integer NOT NULL DEFAULT 0,
    has_exit         boolean NOT NULL DEFAULT false,
    output_truncated boolean NOT NULL DEFAULT false,
    output_seq       bigint  NOT NULL DEFAULT 0,
    cancel_requested boolean NOT NULL DEFAULT false,
    error            text NOT NULL DEFAULT '',
    body_digest      text NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT commands_scope_key_unique UNIQUE (tenant, deployment, subject, computer_id, key),
    CONSTRAINT commands_status_check CHECK (status IN
        ('accepted','dispatched','succeeded','failed','cancelled','lost','side_effect_unknown','fenced'))
);

CREATE INDEX IF NOT EXISTS commands_lookup_idx
    ON ahp_compute.commands (computer_id, id);

-- Filesystem / memory snapshots.
CREATE TABLE IF NOT EXISTS ahp_compute.snapshots (
    id          text PRIMARY KEY,
    computer_id text NOT NULL REFERENCES ahp_compute.computers (id) ON DELETE CASCADE,
    kind        text NOT NULL,
    path        text NOT NULL,
    digest      text NOT NULL,
    size_bytes  bigint NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT snapshots_kind_check CHECK (kind IN ('directory','memory'))
);

-- Environment / package revisions. Installing is an exclusive operation that
-- yields a new revision; failures roll back to the parent.
CREATE TABLE IF NOT EXISTS ahp_compute.env_revisions (
    id          text PRIMARY KEY,
    computer_id text NOT NULL REFERENCES ahp_compute.computers (id) ON DELETE CASCADE,
    revision    bigint NOT NULL,
    spec        text NOT NULL DEFAULT '',
    status      text NOT NULL,
    parent_id   text NOT NULL DEFAULT '',
    error       text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT env_revisions_status_check CHECK (status IN ('pending','applied','rolled_back','failed')),
    CONSTRAINT env_revisions_computer_rev_unique UNIQUE (computer_id, revision),
    CONSTRAINT env_revisions_rev_positive CHECK (revision >= 1)
);

-- Copy-on-write workspace revisions for project Computers. Commit performs
-- optimistic concurrency against expected_head; mismatches raise a conflict
-- branch rather than overwrite.
CREATE TABLE IF NOT EXISTS ahp_compute.workspace_revisions (
    id          text PRIMARY KEY,
    computer_id text NOT NULL REFERENCES ahp_compute.computers (id) ON DELETE CASCADE,
    revision    bigint NOT NULL,
    base_rev    bigint NOT NULL,
    digest      text NOT NULL,
    branch      text NOT NULL DEFAULT 'main',
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT workspace_revisions_computer_rev_unique UNIQUE (computer_id, revision),
    CONSTRAINT workspace_revisions_rev_positive CHECK (revision >= 1)
);

-- Computer state revisions (PDF 3.6: the personal Computer's state is stored
-- as revisions; a typed patch verifies the file hash and appends a derivative
-- revision; derivative branches let an employee work in parallel).
CREATE TABLE IF NOT EXISTS ahp_compute.state_revisions (
    id          text PRIMARY KEY,
    computer_id text NOT NULL REFERENCES ahp_compute.computers (id) ON DELETE CASCADE,
    seq         bigint NOT NULL,
    branch      text NOT NULL DEFAULT 'main',
    parent_id   text NOT NULL DEFAULT '',
    base_seq    bigint NOT NULL DEFAULT 0,
    tree        jsonb NOT NULL DEFAULT '{}'::jsonb,
    digest      text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT state_revisions_computer_branch_seq_unique
        UNIQUE (computer_id, branch, seq),
    CONSTRAINT state_revisions_seq_nonneg CHECK (seq >= 0)
);

CREATE INDEX IF NOT EXISTS state_revisions_branch_head_idx
    ON ahp_compute.state_revisions (computer_id, branch, seq);

-- Cost accounting (PDF 3.6: a background command holds the VM and is
-- accounted in the Computer's cost). Each awake window is one row: opened
-- when a sleeping Computer is woken (or first activated), closed at sleep.
CREATE TABLE IF NOT EXISTS ahp_compute.cost_ledger (
    id          text PRIMARY KEY,
    computer_id text NOT NULL REFERENCES ahp_compute.computers (id) ON DELETE CASCADE,
    kind        text NOT NULL DEFAULT 'active',
    started_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    ended_at    timestamptz,
    CONSTRAINT cost_ledger_kind_check CHECK (kind IN ('active'))
);

-- At most one open (unended) awake window per Computer.
CREATE UNIQUE INDEX IF NOT EXISTS cost_ledger_open_idx
    ON ahp_compute.cost_ledger (computer_id) WHERE ended_at IS NULL;

-- B79: Image version registry (§4.5 Versions/Drain/Compat/Rollback).
-- Each version is an image@sha256 digest. A task (Computer) is pinned to
-- the version active at creation time; publishing a new version does not
-- migrate existing tasks.
CREATE TABLE IF NOT EXISTS ahp_compute.image_versions (
    id              text PRIMARY KEY,
    tenant          text NOT NULL,
    deployment      text NOT NULL,
    agent           text NOT NULL,
    digest          text NOT NULL,
    status          text NOT NULL DEFAULT 'active',
    drain_deadline  timestamptz,
    state_compat    boolean NOT NULL DEFAULT true,
    published_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT image_versions_status_check
        CHECK (status IN ('active','drain','retired','rolled_back')),
    CONSTRAINT image_versions_digest_unique UNIQUE (tenant, deployment, agent, digest)
);

-- At most one active version per (tenant, deployment, agent) at any time.
CREATE UNIQUE INDEX IF NOT EXISTS image_versions_active_idx
    ON ahp_compute.image_versions (tenant, deployment, agent)
    WHERE status = 'active';

-- B79: Pin a Computer to the image version that was active at creation.
ALTER TABLE ahp_compute.computers
    ADD COLUMN IF NOT EXISTS version_digest text NOT NULL DEFAULT '';

-- B79: State schema version for forward-compat checks on resume.
-- When a Computer is resumed after a version migration, the state's
-- schema_version must be compatible with the target version's expected
-- schema; otherwise the resume is refused with a clear reason.
ALTER TABLE ahp_compute.state_revisions
    ADD COLUMN IF NOT EXISTS schema_version integer NOT NULL DEFAULT 1;

-- Output is captured bounded (4 MiB per stream). A log that hit that cap has to say
-- so: without the flag an observer cannot tell "the command printed this" from "the
-- platform threw the rest away", which is the difference between a partial log and a
-- silently falsified one.
ALTER TABLE ahp_compute.commands
    ADD COLUMN IF NOT EXISTS output_truncated boolean NOT NULL DEFAULT false;

-- DEF-COMPUTE-6: a dispatch has to be bounded. Without a bound a command whose runner
-- died stays 'dispatched' forever: nothing is executing it, nothing will ever report
-- it, and the export guard - which refuses a source holding a non-quiescent command -
-- blocks the Computer's owner for good. The bound is dispatch_deadline; heartbeat_at is
-- the proof of life the holder renews while the command runs; backend_started_at records
-- the instant a backend was actually invoked, which is the last moment before an effect
-- becomes possible and is therefore what separates 'lost' (nothing was ever reached)
-- from 'side_effect_unknown' (something was reached and never answered). terminal_reason
-- says who closed the row and on what evidence, so a reconciled verdict is never an
-- anonymous status change.
ALTER TABLE ahp_compute.commands
    ADD COLUMN IF NOT EXISTS dispatch_deadline timestamptz,
    ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz,
    ADD COLUMN IF NOT EXISTS backend_started_at timestamptz,
    ADD COLUMN IF NOT EXISTS terminal_reason text NOT NULL DEFAULT '';

-- The reconciliation sweep wants a partial index on the dispatched tail of the ledger.
-- It is deliberately NOT created here: a plain CREATE INDEX takes a SHARE lock on
-- ahp_compute.commands, which both blocks every writer and is blocked by them, and this
-- file runs on EVERY process start - so a lock-hungry statement here can hang a boot for
-- as long as another connection holds the ledger. Store.EnsureSweepIndex creates it
-- CONCURRENTLY off the startup path instead, where a wait costs a slower sweep and never
-- a downtime. The sweep is correct without the index (it scans and takes LIMIT); the
-- index only makes it cheap on a large ledger.
