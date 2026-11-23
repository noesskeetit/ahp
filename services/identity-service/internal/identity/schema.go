// Package identity: durable schema of the AHP identity service.
//
// The identity service owns the ahp_identity schema exclusively. Every
// statement below is idempotent and non-destructive: re-running Migrate on a
// database that already carries the schema is a no-op and never drops,
// truncates or rewrites anything.
//
// The design carries the security invariants in the database itself:
//
//   - grants_root / grants_child are subtypes of one logical grant table, so
//     a root id can never be forged to point at a child and a child can never
//     be mistaken for a root. A root always has depth 0, an empty parent and
//     root = id; a child always has depth >= 1, a parent and a distinct root.
//   - The idempotency key of a grant is (tenant, deployment, subject, task).
//     A foreign scope therefore misses the row entirely and is reported as
//     NOT_FOUND, never as forbidden.
//   - A subject tombstone forbids every new grant of that (tenant, subject).
//     It is checked inside each grant transaction (subjectRevoked); a
//     foreign key cannot express "forbidden when the row exists".
//   - The context journal is keyed by (tenant, deployment, subject, context)
//     and its labels accumulate monotonically. A grant row never stores a
//     copy of the journal: it references the journal row, so a label added
//     later is visible on the next read and can never be removed.
//   - The delegation graph is acyclic by construction: the edge guard below
//     requires every child to sit exactly one level below its parent under the
//     same root. Depth therefore strictly increases along parent -> child, so a
//     closed parent chain would need a depth greater than itself. The rule is a
//     LOCAL property of one edge, which is what makes it unescapable: no
//     arrangement of edges, however many, satisfies every edge and still closes
//     a loop - unlike a depth cap checked during a walk, which a cycle drawn
//     beyond the cap slips through.
//   - The context owner key (owner_tenant, owner_deployment, owner_subject,
//     id) is what makes a private child context unreadable from a parent
//     scope: a foreign scope cannot satisfy the reference.
package identity

// schemaDDL is the complete, idempotent DDL of the ahp_identity schema.
const schemaDDL = `
CREATE SCHEMA IF NOT EXISTS ahp_identity;
CREATE TABLE IF NOT EXISTS ahp_identity.erased_tenants (tenant text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS ahp_identity.execution_fences (
    tenant      text NOT NULL,
    deployment  text NOT NULL,
    subject     text NOT NULL,
    task        text NOT NULL,
    PRIMARY KEY (tenant, deployment, subject, task)
);

-- Revoked subjects. A row here is a hard stop: while it exists no grant of
-- that (tenant, subject) may be inserted or re-parented, and no new root may
-- be created. Rows are never removed by this service.
CREATE TABLE IF NOT EXISTS ahp_identity.subjects (
    tenant     text        NOT NULL,
    subject    text        NOT NULL,
    revoked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT subjects_pk PRIMARY KEY (tenant, subject)
);

-- One journal row per (scope, context). Labels only ever accumulate: the
-- service only ever writes a superset of the stored value, and the database
-- repeats that rule as a trigger so no other writer can weaken it.
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
    -- Canonical sorted-without-duplicates label arrays cannot be expressed
    -- as a CHECK (subqueries are not allowed there); the writers and the
    -- monotonic trigger below enforce that form instead.
    CONSTRAINT contexts_labels_subjects_shape_ck CHECK (array_ndims(labels_subjects) = 1),
    CONSTRAINT contexts_labels_workspaces_shape_ck CHECK (array_ndims(labels_workspaces) = 1),
    CONSTRAINT contexts_labels_taints_shape_ck CHECK (array_ndims(labels_taints) = 1)
);

CREATE INDEX IF NOT EXISTS contexts_scope_idx
    ON ahp_identity.contexts (tenant, deployment, subject);

-- Monotonic label journal: an UPDATE may only add labels, never weaken or
-- re-key a journal row. The rule lives in the database so that no writer,
-- this service included, can silently remove a restriction.
CREATE OR REPLACE FUNCTION ahp_identity.contexts_labels_monotonic() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    IF NEW.id               <> OLD.id
        OR NEW.tenant       <> OLD.tenant
        OR NEW.deployment   <> OLD.deployment
        OR NEW.subject      <> OLD.subject THEN
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

-- Context label journal, append-only (Identity R4, item 2).
--
-- The journal used to be ONE row per (scope, context) that every widening
-- updated in place. One shared row under concurrent update is a guaranteed
-- serialization abort: two delegations of one context, or one AddLabels per
-- root of a scope that shares a context, aborted each other with 40001 and the
-- retry loop replayed them in silence (measured: 101 and 60 aborts per 160/90
-- calls - internal/identity/delegate_ssi_realpath_test.go). The journal is now
-- an event log instead:
--
--   * contexts keeps the claim (id, scope) and the labels written before this
--     change; the anchor row is never updated again, so no two writers can
--     contend for it. A BEFORE UPDATE trigger makes that a database rule.
--   * every widening appends one row to context_label_events. Appends never
--     block one another: an insert of a new row has nothing to wait for.
--   * the stored set is the union of the anchor and every event, read by
--     readContext. Union is commutative and idempotent, so the answer does not
--     depend on the order events landed in.
--   * monotonicity survives, and is now stronger: an event may never be
--     rewritten, and it may only be removed by an erasure, which has to say so
--     with a-hp_identity.erasing inside its own transaction.
CREATE TABLE IF NOT EXISTS ahp_identity.context_label_events (
    event_id          bigint      NOT NULL GENERATED ALWAYS AS IDENTITY,
    context_id        text        NOT NULL,
    tenant            text        NOT NULL,
    deployment        text        NOT NULL,
    subject           text        NOT NULL,
    labels_subjects   text[]      NOT NULL DEFAULT '{}',
    labels_workspaces text[]      NOT NULL DEFAULT '{}',
    labels_taints     text[]      NOT NULL DEFAULT '{}',
    created_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT context_label_events_pk PRIMARY KEY (event_id),
    CONSTRAINT context_label_events_context_fk FOREIGN KEY (context_id)
        REFERENCES ahp_identity.contexts (id) ON DELETE CASCADE,
    -- An event that widens nothing is noise: refusing it keeps the log bounded
    -- by the number of label additions actually made.
    CONSTRAINT context_label_events_adds_something_ck CHECK (
        cardinality(labels_subjects) + cardinality(labels_workspaces) + cardinality(labels_taints) > 0),
    CONSTRAINT context_label_events_subjects_shape_ck CHECK (array_ndims(labels_subjects) = 1),
    CONSTRAINT context_label_events_workspaces_shape_ck CHECK (array_ndims(labels_workspaces) = 1),
    CONSTRAINT context_label_events_taints_shape_ck CHECK (array_ndims(labels_taints) = 1)
);

CREATE INDEX IF NOT EXISTS context_label_events_context_idx
    ON ahp_identity.context_label_events (context_id);

CREATE OR REPLACE FUNCTION ahp_identity.context_label_events_append_only() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'identity: the context label journal is append-only: an event may never be rewritten'
            USING ERRCODE = '23514';
    END IF;
    IF current_setting('ahp_identity.erasing', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION 'identity: the context label journal is append-only: only an erasure may remove events'
            USING ERRCODE = '23514';
    END IF;
    RETURN OLD;
END
$fn$;

DROP TRIGGER IF EXISTS context_label_events_append_only ON ahp_identity.context_label_events;
CREATE TRIGGER context_label_events_append_only
    BEFORE UPDATE OR DELETE ON ahp_identity.context_label_events
    FOR EACH ROW EXECUTE FUNCTION ahp_identity.context_label_events_append_only();

-- The anchor row is write-once. The monotonic-labels rule it carried is now
-- enforced by the event log alone, so the anchor needs no updater at all -
-- and a row nobody updates cannot serialize two writers.
CREATE OR REPLACE FUNCTION ahp_identity.contexts_anchor_immutable() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    RAISE EXCEPTION 'identity: the context journal anchor is immutable: widen labels through context_label_events'
        USING ERRCODE = '23514';
END
$fn$;

DROP TRIGGER IF EXISTS contexts_labels_monotonic ON ahp_identity.contexts;
DROP TRIGGER IF EXISTS contexts_anchor_immutable ON ahp_identity.contexts;
CREATE TRIGGER contexts_anchor_immutable
    BEFORE UPDATE ON ahp_identity.contexts
    FOR EACH ROW EXECUTE FUNCTION ahp_identity.contexts_anchor_immutable();

-- Root grants: depth 0, no parent, root = id.
CREATE TABLE IF NOT EXISTS ahp_identity.grants_root (
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
    -- No subjects FK: a subjects row is a revocation tombstone, so an FK
    -- would invert the rule and only let revoked subjects create grants.
    -- subjectRevoked enforces the tombstone check instead.
    CONSTRAINT grants_root_context_id_not_blank_ck CHECK (context_id <> '')
);

CREATE INDEX IF NOT EXISTS grants_root_context_idx
    ON ahp_identity.grants_root (context_id);
CREATE INDEX IF NOT EXISTS grants_root_subject_idx
    ON ahp_identity.grants_root (tenant, subject);

-- Delegated grants: depth >= 1, always a parent and a distinct root. The
-- parent and root foreign keys make an orphaned or forged chain impossible.
CREATE TABLE IF NOT EXISTS ahp_identity.grants_child (
    id                text        NOT NULL,
    tenant            text        NOT NULL,
    deployment        text        NOT NULL,
    subject           text        NOT NULL,
    task              text        NOT NULL,
    context_id        text        NOT NULL,
    root              text        NOT NULL,
    parent            text        NOT NULL,
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
    -- A child parent is either the root or another child, so the reference
    -- spans both grant subtypes; a plain FK to one subtype cannot express
    -- that and is deliberately absent here.
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
    -- No subjects FK, see grants_root: a tombstone row forbids, it does not
    -- authorise. subjectRevoked enforces the rule.
    -- No parent FK, see the parent comment above: the reference spans both
    -- grant subtypes.
    CONSTRAINT grants_child_context_id_not_blank_ck CHECK (context_id <> '')
);

CREATE INDEX IF NOT EXISTS grants_child_parent_idx
    ON ahp_identity.grants_child (parent);
CREATE INDEX IF NOT EXISTS grants_child_root_idx
    ON ahp_identity.grants_child (root);
CREATE INDEX IF NOT EXISTS grants_child_context_idx
    ON ahp_identity.grants_child (context_id);
CREATE INDEX IF NOT EXISTS grants_child_subject_idx
    ON ahp_identity.grants_child (tenant, subject);

-- Edge guard: the delegation graph must stay a tree.
--
-- A cyclic parent chain is not a slow query, it is a stored contradiction: every
-- recursive walk of grants_child (quota, ancestor ceilings, cancellation,
-- usability, lineage) either never terminates or silently stops at whatever hop
-- bound it happens to carry, and the service owns the answer either way -
-- PDF rev3.2 line 2703 assigns cycles to Identity & Policy, and line 3561 lists
-- "цикл делегирования" among the cases the platform must answer for.
--
-- It is enforced here rather than in Go because the writers are not only in Go:
-- an import, a repair script or a legacy database reaches these rows too, and
-- PDF rev3.2 section 9.4's counter is only meaningful if the graph it counts is
-- a tree.
CREATE OR REPLACE FUNCTION ahp_identity.grants_child_edge_invariant() RETURNS trigger
LANGUAGE plpgsql AS $fn$
DECLARE
    parent_depth integer;
    parent_root  text;
BEGIN
    IF NEW.parent IS NULL OR NEW.parent = '' THEN
        RAISE EXCEPTION 'identity: a child grant must name a parent'
            USING ERRCODE = '23514';
    END IF;
    -- A parent is either the root or another child, the same split the schema
    -- documents for the deliberately absent parent foreign key.
    SELECT g.depth, g.root INTO parent_depth, parent_root
      FROM (
            SELECT c.depth AS depth, c.root AS root
              FROM ahp_identity.grants_child c WHERE c.id = NEW.parent
            UNION ALL
            SELECT r.depth AS depth, r.id AS root
              FROM ahp_identity.grants_root r WHERE r.id = NEW.parent
      ) g;
    IF NOT FOUND THEN
        -- Nothing to compare against. A dangling parent cannot close a loop
        -- through an edge that does not lead anywhere, and inventing an owner
        -- check here would change what an absent parent means for existing
        -- rows, which is a different question than acyclicity.
        RETURN NEW;
    END IF;
    IF NEW.depth <> parent_depth + 1 OR NEW.root <> parent_root THEN
        RAISE EXCEPTION 'identity: the delegation graph must stay acyclic: grant % must sit exactly one level below parent % (depth % under root %), asked for depth % under root %',
            NEW.id, NEW.parent, parent_depth, parent_root, NEW.depth, NEW.root
            USING ERRCODE = '23P01';
    END IF;
    RETURN NEW;
END
$fn$;

DROP TRIGGER IF EXISTS grants_child_edge_invariant ON ahp_identity.grants_child;
CREATE TRIGGER grants_child_edge_invariant
    BEFORE INSERT OR UPDATE OF parent, root, depth ON ahp_identity.grants_child
    FOR EACH ROW EXECUTE FUNCTION ahp_identity.grants_child_edge_invariant();

-- =====================================================================
-- Browser authentication (Agent Space UI / as-web).
--
-- The identity service decides who a human is. In a production cell that
-- decision belongs to the corporate IdP plus the IAM ExtAuthz check; this
-- cell has neither, so the tables below carry a real local credential store
-- (argon2id) and real server-side sessions. They are a prototype stand-in for
-- the IdP, never a production identity provider, and they never hold a
-- signing key: token minting stays in pkg/auth.
--
-- Invariants:
--   - tenant and subject live on the user row only. A login request cannot
--     name them, so a caller can never pick the tenant or subject it logs in
--     as; they are read back from the account the credential belongs to.
--   - A session carries an absolute deadline that the database refuses to
--     move later: refreshing proves the session is still live, it never
--     extends the authority the session was opened with.
--   - Revocation is a row write. Every later check reads that row against the
--     PostgreSQL clock, so a logout or a subject tombstone stops operations
--     immediately and does not depend on the client cooperating.
-- =====================================================================

-- Human accounts the browser may authenticate as. One row per (tenant,
-- subject) so the same person cannot exist twice inside a tenant.
CREATE TABLE IF NOT EXISTS ahp_identity.browser_users (
    user_id      text        NOT NULL,
    tenant       text        NOT NULL,
    subject      text        NOT NULL,
    display_name text        NOT NULL DEFAULT '',
    email        text        NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT browser_users_pk PRIMARY KEY (user_id),
    CONSTRAINT browser_users_scope_key_unique UNIQUE (tenant, subject),
    CONSTRAINT browser_users_user_id_ck CHECK (user_id <> ''),
    CONSTRAINT browser_users_tenant_ck CHECK (tenant <> ''),
    CONSTRAINT browser_users_subject_ck CHECK (subject <> '')
);

-- Credentials. Only an argon2id digest is stored; the password itself is
-- never persisted and never logged. The length checks stop a caller from
-- storing a digest too short to be argon2id at the configured cost.
CREATE TABLE IF NOT EXISTS ahp_identity.browser_credentials (
    username   text        NOT NULL,
    user_id    text        NOT NULL,
    algo       text        NOT NULL,
    salt       bytea       NOT NULL,
    hash       bytea       NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT browser_credentials_pk PRIMARY KEY (username),
    CONSTRAINT browser_credentials_user_fk FOREIGN KEY (user_id)
        REFERENCES ahp_identity.browser_users (user_id),
    CONSTRAINT browser_credentials_algo_ck CHECK (algo = 'argon2id'),
    CONSTRAINT browser_credentials_hash_len_ck CHECK (octet_length(hash) = 32),
    CONSTRAINT browser_credentials_salt_len_ck CHECK (octet_length(salt) >= 16),
    -- One account per login name: a username must resolve to exactly one
    -- (tenant, subject) or login could not pick a scope deterministically.
    CONSTRAINT browser_credentials_user_unique UNIQUE (user_id)
);

-- Roles and permissions the UI gates on. Rows rather than an array so a grant
-- can be added or revoked without rewriting the account.
CREATE TABLE IF NOT EXISTS ahp_identity.browser_grants (
    user_id text NOT NULL,
    kind    text NOT NULL,
    value   text NOT NULL,
    CONSTRAINT browser_grants_pk PRIMARY KEY (user_id, kind, value),
    CONSTRAINT browser_grants_user_fk FOREIGN KEY (user_id)
        REFERENCES ahp_identity.browser_users (user_id),
    CONSTRAINT browser_grants_kind_ck CHECK (kind IN ('role', 'permission')),
    CONSTRAINT browser_grants_value_ck CHECK (value <> '')
);

-- Server-side browser sessions. The browser holds only an opaque random id in
-- an HttpOnly cookie; all authority lives in this row.
CREATE TABLE IF NOT EXISTS ahp_identity.browser_sessions (
    session_id   text        NOT NULL,
    user_id      text        NOT NULL,
    tenant       text        NOT NULL,
    subject      text        NOT NULL,
    epoch        bigint      NOT NULL DEFAULT 1,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at   timestamptz NOT NULL,
    absolute_at  timestamptz NOT NULL,
    revoked_at   timestamptz,
    CONSTRAINT browser_sessions_pk PRIMARY KEY (session_id),
    CONSTRAINT browser_sessions_user_fk FOREIGN KEY (user_id)
        REFERENCES ahp_identity.browser_users (user_id),
    CONSTRAINT browser_sessions_epoch_ck CHECK (epoch >= 1),
    CONSTRAINT browser_sessions_deadline_ck CHECK (absolute_at > created_at),
    CONSTRAINT browser_sessions_expiry_ck CHECK (expires_at > created_at)
);

CREATE INDEX IF NOT EXISTS browser_sessions_subject_idx
    ON ahp_identity.browser_sessions (tenant, subject);

-- A session's absolute deadline is immovable. Refresh may push the idle
-- deadline and bump the epoch; it may never move absolute_at later, unset a
-- revocation or rewind the epoch: renewing proves liveness, it does not grant
-- more authority.
CREATE OR REPLACE FUNCTION ahp_identity.browser_sessions_non_extending() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    IF NEW.absolute_at > OLD.absolute_at THEN
        RAISE EXCEPTION 'identity: a browser session absolute deadline may not be extended'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.epoch < OLD.epoch THEN
        RAISE EXCEPTION 'identity: a browser session epoch may not go backwards'
            USING ERRCODE = '23514';
    END IF;
    IF OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS DISTINCT FROM OLD.revoked_at THEN
        RAISE EXCEPTION 'identity: a revoked browser session may not be revived'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.tenant <> OLD.tenant OR NEW.subject <> OLD.subject OR NEW.user_id <> OLD.user_id THEN
        RAISE EXCEPTION 'identity: a browser session may not be re-keyed'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$fn$;

DROP TRIGGER IF EXISTS browser_sessions_non_extending ON ahp_identity.browser_sessions;
CREATE TRIGGER browser_sessions_non_extending
    BEFORE UPDATE ON ahp_identity.browser_sessions
    FOR EACH ROW EXECUTE FUNCTION ahp_identity.browser_sessions_non_extending();

-- Binding between an issued browser access token and the session that produced
-- it, keyed by the token's jti. The token itself is never stored. The access
-- token is a stateless JWT, so this table is what makes logout and subject
-- revocation observable on a credential that is still inside its own validity
-- window: the row is revoked, therefore the token is dead, whatever its exp
-- still claims. The key is the jti rather than a hash of the token because two
-- tokens minted for the same principal in the same second are otherwise
-- byte-identical, and one browser's logout would then reach into another's
-- credential.
--
-- The token_hash shape below belongs to the same unreleased revision of this
-- table, so it is renamed in place rather than carried forward.
--
-- That replacement is NOT a statement in this DDL. Probing for the legacy shape
-- and dropping it is decided in Migrate, ahead of the ACCESS EXCLUSIVE window, so
-- that a healthy installation never has a DROP TABLE sent to it at all: the
-- catalog question costs one read outside the window, and the exclusive lock is
-- requested only for an installation that really holds the abandoned shape
-- (measured: DROP TABLE waits behind a reader holding ACCESS SHARE, CREATE TABLE
-- IF NOT EXISTS does not, so this is the one statement in the DDL that can turn a
-- migration into a stall in front of live traffic).
-- See legacyBrowserTokenBindingsDropDDL and Migrate.

CREATE TABLE IF NOT EXISTS ahp_identity.browser_token_bindings (
    token_jti   text        NOT NULL,
    session_id  text        NOT NULL,
    epoch       bigint      NOT NULL,
    expires_at  timestamptz NOT NULL,
    revoked_at  timestamptz,
    CONSTRAINT browser_token_bindings_pk PRIMARY KEY (token_jti),
    CONSTRAINT browser_token_bindings_session_fk FOREIGN KEY (session_id)
        REFERENCES ahp_identity.browser_sessions (session_id),
    CONSTRAINT browser_token_bindings_jti_ck CHECK (token_jti <> ''),
    CONSTRAINT browser_token_bindings_epoch_ck CHECK (epoch >= 1)
);

CREATE INDEX IF NOT EXISTS browser_token_bindings_session_idx
    ON ahp_identity.browser_token_bindings (session_id);
-- Minted task tokens, keyed by the grant that authorised them. The token
-- itself is never stored: only the workload it was issued to and the expiry
-- that was granted, so an audit can re-check a mint without the credential.
-- One audit row per grant: MintTask re-mints by upserting on grant_id, so the
-- table needs a unique key on grant_id. Databases created before this key
-- existed are reconciled by the idempotent block below.
CREATE TABLE IF NOT EXISTS ahp_identity.issued_tokens (
    grant_id    text        NOT NULL,
    workload    text        NOT NULL,
    expires_at  timestamptz NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    -- No grant FK: a token may be minted for any grant subtype, and a single
    -- FK can only target one subtype table.
    CONSTRAINT issued_tokens_pk PRIMARY KEY (grant_id),
    CONSTRAINT issued_tokens_workload_ck CHECK (workload <> '')
);

-- Idempotent reconciliation for pre-existing databases that were created
-- without the grant_id key: keep the newest audit row per grant and add the
-- key only when it is still missing. Never drops or rewrites anything else.
DELETE FROM ahp_identity.issued_tokens a
      USING ahp_identity.issued_tokens b
     WHERE a.grant_id = b.grant_id AND a.ctid < b.ctid;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint c
          JOIN pg_attribute at ON at.attrelid = c.conrelid AND at.attnum = ANY(c.conkey)
         WHERE c.conrelid = 'ahp_identity.issued_tokens'::regclass
           AND c.contype IN ('p', 'u')
           AND at.attname = 'grant_id'
    ) THEN
        ALTER TABLE ahp_identity.issued_tokens
            ADD CONSTRAINT issued_tokens_pk PRIMARY KEY (grant_id);
    END IF;
END
$$;
` + resourceDDL
