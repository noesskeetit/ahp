-- Identity API durable schema (task 03: grants, delegation tree, label journal).
--
-- Every statement here is idempotent and non-destructive: re-running the
-- migration on a database that already carries the schema is a no-op and
-- never drops, truncates or rewrites existing rows. All objects live in the
-- dedicated ahp_identity schema owned exclusively by the identity service.
--
-- Invariants the database itself enforces:
--   * grants_root / grants_child are subtypes of one logical grant table, so a
--     root can never be forged to point at a child and vice versa. A root has
--     depth 0, an empty parent and root = id; a child has depth >= 1, a parent
--     and a distinct root.
--   * The grant idempotency key is (tenant, deployment, subject, task): a
--     foreign scope misses the row entirely and is reported NOT_FOUND.
--   * A subject tombstone is a foreign key with ON DELETE RESTRICT, so while a
--     subject is revoked no grant of that subject can be inserted at all.
--   * Context journal labels only accumulate; a trigger rejects any UPDATE
--     that would weaken them.
--   * The context owner key (tenant, deployment, subject, id) is what makes a
--     private child context unreadable from a parent scope.

CREATE SCHEMA IF NOT EXISTS ahp_identity;
CREATE TABLE IF NOT EXISTS ahp_identity.erased_tenants (tenant text PRIMARY KEY);

-- Revoked subjects. A row here is a hard stop: while it exists no grant of
-- that (tenant, subject) may be inserted and no new root may be created.
CREATE TABLE IF NOT EXISTS ahp_identity.subjects (
    tenant     text        NOT NULL,
    subject    text        NOT NULL,
    revoked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT subjects_pk PRIMARY KEY (tenant, subject)
);

-- One label journal row per (scope, context).
CREATE TABLE IF NOT EXISTS ahp_identity.contexts (
    id                 text        NOT NULL,
    tenant             text        NOT NULL,
    deployment         text        NOT NULL,
    subject            text        NOT NULL,
    labels_subjects    text[]      NOT NULL DEFAULT '{}',
    labels_workspaces  text[]      NOT NULL DEFAULT '{}',
    labels_taints      text[]      NOT NULL DEFAULT '{}',
    created_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT contexts_pk PRIMARY KEY (id),
    CONSTRAINT contexts_scope_key_unique UNIQUE (tenant, deployment, subject, id),
    CONSTRAINT contexts_labels_subjects_sorted_ck CHECK (
        labels_subjects = ARRAY(SELECT unnest(labels_subjects) ORDER BY 1)),
    CONSTRAINT contexts_labels_workspaces_sorted_ck CHECK (
        labels_workspaces = ARRAY(SELECT unnest(labels_workspaces) ORDER BY 1)),
    CONSTRAINT contexts_labels_taints_sorted_ck CHECK (
        labels_taints = ARRAY(SELECT unnest(labels_taints) ORDER BY 1))
);

CREATE INDEX IF NOT EXISTS contexts_scope_idx
    ON ahp_identity.contexts (tenant, deployment, subject);

-- Monotonic label journal: an UPDATE may only add labels, never weaken or
-- re-key a journal row.
CREATE OR REPLACE FUNCTION ahp_identity.contexts_labels_monotonic() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    IF NEW.id             <> OLD.id
        OR NEW.tenant     <> OLD.tenant
        OR NEW.deployment <> OLD.deployment
        OR NEW.subject    <> OLD.subject THEN
        RAISE EXCEPTION 'identity: a context journal row may not be re-keyed'
            USING ERRCODE = '23514';
    END IF;
    IF NOT (OLD.labels_subjects   <@ NEW.labels_subjects
        AND OLD.labels_workspaces <@ NEW.labels_workspaces
        AND OLD.labels_taints     <@ NEW.labels_taints) THEN
        RAISE EXCEPTION 'identity: context labels may only accumulate'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$fn$;

DROP TRIGGER IF EXISTS contexts_labels_monotonic ON ahp_identity.contexts;
CREATE TRIGGER contexts_labels_monotonic
    BEFORE UPDATE ON ahp_identity.contexts
    FOR EACH ROW EXECUTE FUNCTION ahp_identity.contexts_labels_monotonic();

-- Root grants: depth 0, no parent, root = id.
CREATE TABLE IF NOT EXISTS ahp_identity.grants_root (
    id                text        NOT NULL,
    root              text        NOT NULL,
    parent            text        NOT NULL DEFAULT '',
    tenant            text        NOT NULL,
    deployment        text        NOT NULL,
    subject           text        NOT NULL,
    task              text        NOT NULL,
    context_id        text        NOT NULL,
    status            text        NOT NULL DEFAULT 'active',
    epoch             bigint      NOT NULL DEFAULT 1,
    depth             integer     NOT NULL DEFAULT 0,
    actors            text[]      NOT NULL DEFAULT '{}',
    capabilities      text[]      NOT NULL DEFAULT '{}',
    labels_subjects   text[]      NOT NULL DEFAULT '{}',
    labels_workspaces text[]      NOT NULL DEFAULT '{}',
    labels_taints     text[]      NOT NULL DEFAULT '{}',
    max_depth         integer     NOT NULL,
    max_fanout        integer     NOT NULL,
    max_active        integer     NOT NULL,
    allow_self        boolean     NOT NULL DEFAULT false,
    body_digest       text        NOT NULL,
    issued_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at        timestamptz NOT NULL,
    revoked_at        timestamptz,
    CONSTRAINT grants_root_pk PRIMARY KEY (id),
    CONSTRAINT grants_root_scope_task_unique UNIQUE (tenant, deployment, subject, task),
    CONSTRAINT grants_root_status_ck CHECK (status IN ('active', 'revoked')),
    CONSTRAINT grants_root_depth_ck CHECK (depth = 0),
    CONSTRAINT grants_root_root_ck CHECK (root = id),
    CONSTRAINT grants_root_parent_ck CHECK (parent = ''),
    CONSTRAINT grants_root_epoch_ck CHECK (epoch >= 1),
    CONSTRAINT grants_root_depth_bounds_ck CHECK (max_depth BETWEEN 1 AND 16),
    CONSTRAINT grants_root_fanout_bounds_ck CHECK (max_fanout BETWEEN 1 AND 1024),
    CONSTRAINT grants_root_active_bounds_ck CHECK (max_active BETWEEN 1 AND 100000),
    CONSTRAINT grants_root_expiry_ck CHECK (expires_at > issued_at),
    CONSTRAINT grants_root_revoked_shape_ck CHECK (
        (status = 'active' AND revoked_at IS NULL) OR
        (status = 'revoked' AND revoked_at IS NOT NULL)
    ),
    CONSTRAINT grants_root_context_fk FOREIGN KEY (context_id)
        REFERENCES ahp_identity.contexts (id) ON DELETE RESTRICT,
    CONSTRAINT grants_root_subject_fk FOREIGN KEY (tenant, subject)
        REFERENCES ahp_identity.subjects (tenant, subject) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS grants_root_context_idx
    ON ahp_identity.grants_root (context_id);
CREATE INDEX IF NOT EXISTS grants_root_subject_idx
    ON ahp_identity.grants_root (tenant, subject);

-- Delegated grants: depth >= 1, always a parent and a distinct root. The
-- parent and root foreign keys make an orphaned or forged chain impossible.
CREATE TABLE IF NOT EXISTS ahp_identity.grants_child (
    id                text        NOT NULL,
    root              text        NOT NULL,
    parent            text        NOT NULL,
    tenant            text        NOT NULL,
    deployment        text        NOT NULL,
    subject           text        NOT NULL,
    task              text        NOT NULL,
    context_id        text        NOT NULL,
    status            text        NOT NULL DEFAULT 'active',
    epoch             bigint      NOT NULL DEFAULT 1,
    depth             integer     NOT NULL,
    actors            text[]      NOT NULL DEFAULT '{}',
    capabilities      text[]      NOT NULL DEFAULT '{}',
    labels_subjects   text[]      NOT NULL DEFAULT '{}',
    labels_workspaces text[]      NOT NULL DEFAULT '{}',
    labels_taints     text[]      NOT NULL DEFAULT '{}',
    max_depth         integer     NOT NULL,
    max_fanout        integer     NOT NULL,
    max_active        integer     NOT NULL,
    allow_self        boolean     NOT NULL DEFAULT false,
    body_digest       text        NOT NULL,
    issued_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at        timestamptz NOT NULL,
    revoked_at        timestamptz,
    CONSTRAINT grants_child_pk PRIMARY KEY (id),
    CONSTRAINT grants_child_scope_task_unique UNIQUE (tenant, deployment, subject, task),
    CONSTRAINT grants_child_status_ck CHECK (status IN ('active', 'revoked')),
    CONSTRAINT grants_child_depth_ck CHECK (depth BETWEEN 1 AND 16),
    CONSTRAINT grants_child_shape_ck CHECK (root <> '' AND parent <> '' AND root <> id),
    CONSTRAINT grants_child_root_fk FOREIGN KEY (root)
        REFERENCES ahp_identity.grants_root (id) ON DELETE RESTRICT,
    CONSTRAINT grants_child_parent_fk FOREIGN KEY (parent)
        REFERENCES ahp_identity.grants_child (id) ON DELETE RESTRICT,
    CONSTRAINT grants_child_epoch_ck CHECK (epoch >= 1),
    CONSTRAINT grants_child_depth_bounds_ck CHECK (max_depth BETWEEN 1 AND 16),
    CONSTRAINT grants_child_fanout_bounds_ck CHECK (max_fanout BETWEEN 1 AND 1024),
    CONSTRAINT grants_child_active_bounds_ck CHECK (max_active BETWEEN 1 AND 100000),
    CONSTRAINT grants_child_expiry_ck CHECK (expires_at > issued_at),
    CONSTRAINT grants_child_revoked_shape_ck CHECK (
        (status = 'active' AND revoked_at IS NULL) OR
        (status = 'revoked' AND revoked_at IS NOT NULL)
    ),
    CONSTRAINT grants_child_context_fk FOREIGN KEY (context_id)
        REFERENCES ahp_identity.contexts (id) ON DELETE RESTRICT,
    CONSTRAINT grants_child_subject_fk FOREIGN KEY (tenant, subject)
        REFERENCES ahp_identity.subjects (tenant, subject) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS grants_child_parent_idx
    ON ahp_identity.grants_child (parent);
CREATE INDEX IF NOT EXISTS grants_child_root_idx
    ON ahp_identity.grants_child (root);
CREATE INDEX IF NOT EXISTS grants_child_context_idx
    ON ahp_identity.grants_child (context_id);
CREATE INDEX IF NOT EXISTS grants_child_subject_idx
    ON ahp_identity.grants_child (tenant, subject);

-- Minted task tokens, keyed by the grant that authorised them. The token
-- itself is never stored: only the workload it was minted for and the expiry
-- that was granted, so an audit can re-check a mint without credentials.
CREATE TABLE IF NOT EXISTS ahp_identity.issued_tokens (
    grant_id    text        NOT NULL,
    workload    text        NOT NULL,
    expires_at  timestamptz NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT issued_tokens_pk PRIMARY KEY (grant_id),
    CONSTRAINT issued_tokens_grant_fk FOREIGN KEY (grant_id)
        REFERENCES ahp_identity.grants_child (id) ON DELETE CASCADE
);
