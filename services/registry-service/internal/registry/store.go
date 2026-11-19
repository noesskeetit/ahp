// Package registry: PostgreSQL storage for the ahp_registry schema.
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema is the exclusive schema of this service.
const Schema = "ahp_registry"

// Environments an installation may target. Stage, dev and prod are separate
// installation rows and never share a version pointer or a pin.
const (
	EnvironmentDev   = "dev"
	EnvironmentStage = "stage"
	EnvironmentProd  = "prod"
)

// Store is the registry record owner. It is safe for concurrent use.
type Store struct {
	pool        *pgxpool.Pool
	evalGate    PublishGate
	taskSubject TaskSubjectResolver
	// supply, when set by WithSupplyChain, turns on deploy admission: an
	// image-bearing version deploys only on a verified CI supply chain.
	supply *TrustRoots
	// supplyElection records WHY the supply chain is or is not enforced. It is
	// never inferred from absence: a Store that was given neither trust roots nor
	// an explicit opt-out treats every provenance document as unverifiable and
	// refuses to act on it (fail closed).
	supplyElection string
	// supplyReason keeps the operator's stated reason for an explicit opt-out
	// visible in diagnostics; it is never used to authorise anything.
	supplyReason string
	// prober, when wired with WithPromoteReadinessGate, is consulted BEFORE a
	// promote switches traffic and by AutoRollbackIfUnready.
	prober ReadinessProber
	// readinessGateEnvs lists the environments whose traffic switching must be
	// probed first; empty means the gate is never active.
	readinessGateEnvs map[string]bool
}

// PublishGate is the publication precondition the PDF makes mandatory:
// "оценки перед публикацией" (rev 3.2, с. 42/48; the pipeline pairs Registry's
// "ревизии · публикация" with Evals' "оценки · трассы без данных"). A version
// digest may be published only when a passing evaluation verdict exists for that
// exact digest. internal/evals.Store satisfies it.
//
// It is a hard product rule, not an operator option: a Store built without a gate
// publishes nothing (fail-closed), so a deployment that forgets to wire the evals
// plane refuses publication instead of silently releasing an un-evaluated version.
type PublishGate interface {
	RequirePassedVerdict(ctx context.Context, digest string) error
}

// Option configures a Store at construction.
type Option func(*Store)

// WithEvalGate installs the publication gate. Without it the store refuses every
// PutVersion, because publishing without an evaluation verdict is never allowed.
func WithEvalGate(g PublishGate) Option {
	return func(s *Store) { s.evalGate = g }
}

// New builds a registry store on an existing pool. The pool must be able to
// create the ahp_registry schema when Migrate is called. Pass WithEvalGate to
// wire the mandatory evals precondition; a store built without it fails closed.
func New(pool *pgxpool.Pool, opts ...Option) *Store {
	s := &Store{pool: pool}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Supply chain modes reported by SupplyChainMode.
const (
	// SupplyChainModeRoots: the operator installed trust roots, so every
	// provenance document is verified cryptographically.
	SupplyChainModeRoots = "roots"
	// SupplyChainModeSynthetic: the operator explicitly elected a SYNTHETIC
	// stand. Nothing is verified, and the mode is deliberately named so no
	// evidence, log or report can mistake it for real PKI/KMS verification.
	SupplyChainModeSynthetic = "synthetic"
	// SupplyChainModeUnset: nothing was configured, so nothing that depends on
	// verifying provenance is allowed to proceed.
	SupplyChainModeUnset = "unset"
)

// SupplyChainMode reports how this Store handles deploy provenance, so an
// operator face can log the truth instead of a caller guessing from behaviour.
func (s *Store) SupplyChainMode() string {
	switch {
	case s.supply != nil:
		return SupplyChainModeRoots
	case s.supplyElection == SupplyChainModeSynthetic:
		return SupplyChainModeSynthetic
	default:
		return SupplyChainModeUnset
	}
}

// SyntheticElection returns the operator's stated reason for running the
// synthetic stand, or "" when no such stand was elected. Evidence must carry
// this value when it reports synthetic results, so synthetic is never read as
// real supply-chain verification.
func (s *Store) SyntheticElection() string {
	if s.supplyElection != SupplyChainModeSynthetic {
		return ""
	}
	return s.supplyReason
}

// WithSyntheticSupplyChain elects a SYNTHETIC stand: provenance documents are
// accepted without cryptographic verification, for local fixtures and test
// harnesses that have no operator PKI.
//
// It is not a way to weaken the product gate quietly. It has to be named, the
// mode stays observable through SupplyChainMode/SyntheticElection, and a Store
// built with neither trust roots nor this election refuses every operation that
// would otherwise rest on an unchecked attestation -- instead of accepting an
// attestation because its string is not empty.
func WithSyntheticSupplyChain(reason string) Option {
	return func(s *Store) {
		if strings.TrimSpace(reason) == "" {
			reason = "unspecified synthetic stand"
		}
		s.supplyElection = SupplyChainModeSynthetic
		s.supplyReason = strings.TrimSpace(reason)
	}
}

// Pool exposes the underlying pool for health checks only; callers must not
// reach into registry tables directly.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Version is one immutable manifest publication.
type Version struct {
	ID        string    `json:"id"`
	AgentID   string    `json:"agent_id"`
	Digest    string    `json:"digest"`
	Manifest  Manifest  `json:"manifest"`
	CreatedAt time.Time `json:"created_at"`
}

// Installation is a tenant-scoped deployment of one version into one
// environment.
type Installation struct {
	ID           string       `json:"id"`
	Tenant       string       `json:"tenant"`
	AgentID      string       `json:"agent_id"`
	Deployment   string       `json:"deployment"`
	Environment  string       `json:"environment"`
	VersionID    string       `json:"version_id"`
	Capabilities Capabilities `json:"capabilities"`
	Published    bool         `json:"published"`
}

// Config is the per-tenant, per-agent configuration document guarded by an
// optimistic revision counter.
type Config struct {
	ID       string          `json:"id"`
	Tenant   string          `json:"tenant"`
	AgentID  string          `json:"agent_id"`
	Revision int64           `json:"revision"`
	Data     json.RawMessage `json:"data"`
}

// registrySchemaDDL is idempotent and non-destructive: it only ever creates.
const registrySchemaDDL = `
CREATE SCHEMA IF NOT EXISTS ahp_registry;

CREATE TABLE IF NOT EXISTS ahp_registry.versions (
    id          text PRIMARY KEY,
    agent_id    text NOT NULL,
    digest      text NOT NULL,
    manifest    jsonb NOT NULL,
    -- Derived columns keep visibility and class queryable without jsonb
    -- expressions in the index, and are always rewritten on insert.
    visibility  text NOT NULL DEFAULT 'tenant',
    class       text NOT NULL DEFAULT 'C',
    created_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT versions_digest_format CHECK (digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT versions_id_format CHECK (id <> ''),
    CONSTRAINT versions_visibility_check CHECK (visibility IN ('system', 'tenant', 'catalog')),
    CONSTRAINT versions_class_check CHECK (class IN ('S', 'C'))
);
-- A digest hashes manifest content only: independent agents may publish
-- byte-identical manifests, so uniqueness is scoped to the publishing agent.
CREATE UNIQUE INDEX IF NOT EXISTS versions_agent_digest_uidx
    ON ahp_registry.versions (agent_id, digest);
CREATE INDEX IF NOT EXISTS versions_agent_idx
    ON ahp_registry.versions (agent_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ahp_registry.installations (
    id           text PRIMARY KEY,
    tenant       text NOT NULL,
    agent_id     text NOT NULL,
    deployment   text NOT NULL,
    environment  text NOT NULL,
    version_id   text NOT NULL REFERENCES ahp_registry.versions (id),
    capabilities jsonb NOT NULL DEFAULT '{}'::jsonb,
    published    boolean NOT NULL DEFAULT false,
    idem_key     text NOT NULL,
    request_digest text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT installations_environment_format
        CHECK (environment ~ '^[a-z0-9][a-z0-9._:@-]{0,63}$'),
    CONSTRAINT installations_published_visibility_check
        CHECK (NOT published OR environment = 'prod')
);
CREATE UNIQUE INDEX IF NOT EXISTS installations_tenant_key_uidx
    ON ahp_registry.installations (tenant, idem_key);
CREATE UNIQUE INDEX IF NOT EXISTS installations_slot_uidx
    ON ahp_registry.installations (tenant, agent_id, environment);
CREATE INDEX IF NOT EXISTS installations_catalog_idx
    ON ahp_registry.installations (tenant, published);

CREATE TABLE IF NOT EXISTS ahp_registry.task_pins (
    task_id         text PRIMARY KEY,
    tenant          text NOT NULL,
    installation_id text NOT NULL REFERENCES ahp_registry.installations (id),
    version_id      text NOT NULL REFERENCES ahp_registry.versions (id),
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT task_pins_tenant_format CHECK (tenant <> '')
);
CREATE INDEX IF NOT EXISTS task_pins_installation_idx
    ON ahp_registry.task_pins (installation_id, version_id);

CREATE TABLE IF NOT EXISTS ahp_registry.configs (
    id         text PRIMARY KEY,
    tenant     text NOT NULL,
    agent_id   text NOT NULL,
    revision   bigint NOT NULL DEFAULT 1,
    data       jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT configs_revision_positive CHECK (revision >= 1)
);
CREATE UNIQUE INDEX IF NOT EXISTS configs_tenant_agent_uidx
    ON ahp_registry.configs (tenant, agent_id);

-- schema_version records which non-idempotent-cost migration revisions
-- have already been applied to this database. It lets Migrate run the
-- exclusive ALTER/backfill at most once per database (see Migrate).
-- agents records who published which agent: an agent and all of its versions
-- belong to one publisher, while installations belong to the consuming tenant
-- (PDF rev 3.2, c. 42 "Установка").
CREATE TABLE IF NOT EXISTS ahp_registry.agents (
    agent_id     text PRIMARY KEY,
    publisher_id text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT agents_publisher_format CHECK (publisher_id <> '')
);
CREATE INDEX IF NOT EXISTS agents_publisher_idx ON ahp_registry.agents (publisher_id, agent_id);

-- deployments is the append-only rollout history of one installation. It is
-- what makes "rollback to the previous digest" (PDF rev 3.2, c. 42 "Версии")
-- an auditable fact of the registry rather than a client assertion.
CREATE TABLE IF NOT EXISTS ahp_registry.deployments (
    id              text PRIMARY KEY,
    tenant          text NOT NULL,
    installation_id text NOT NULL REFERENCES ahp_registry.installations (id),
    agent_id        text NOT NULL,
    environment     text NOT NULL,
    version_id      text NOT NULL REFERENCES ahp_registry.versions (id),
    digest          text NOT NULL,
    action          text NOT NULL,
    release_id      text NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT deployments_action_check CHECK (action IN ('install', 'promote', 'rollback')),
    CONSTRAINT deployments_digest_format CHECK (digest ~ '^sha256:[0-9a-f]{64}$')
);
CREATE INDEX IF NOT EXISTS deployments_installation_idx
    ON ahp_registry.deployments (tenant, installation_id, created_at DESC);

-- agent_systems / system_releases / system_deployments model a system of
-- agents as a Registry object that groups several deployments of one publisher,
-- with a release that pins member versions and a rollout that is atomic
-- (PDF rev 3.2, c. 44 "5.6. Система агентов, видимость и направление вызовов").
CREATE TABLE IF NOT EXISTS ahp_registry.agent_systems (
    system_id    text PRIMARY KEY,
    publisher_id text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT agent_systems_publisher_format CHECK (publisher_id <> '')
);

CREATE TABLE IF NOT EXISTS ahp_registry.system_releases (
    id         text PRIMARY KEY,
    system_id  text NOT NULL REFERENCES ahp_registry.agent_systems (system_id),
    digest     text NOT NULL,
    members    jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT system_releases_members_object CHECK (jsonb_typeof(members) = 'array'),
    CONSTRAINT system_releases_digest_format CHECK (digest ~ '^sha256:[0-9a-f]{64}$')
);
CREATE UNIQUE INDEX IF NOT EXISTS system_releases_digest_uidx
    ON ahp_registry.system_releases (system_id, digest);

CREATE TABLE IF NOT EXISTS ahp_registry.system_deployments (
    id         text PRIMARY KEY,
    tenant     text NOT NULL,
    system_id  text NOT NULL REFERENCES ahp_registry.agent_systems (system_id),
    release_id text NOT NULL REFERENCES ahp_registry.system_releases (id),
    environment text NOT NULL,
    installations jsonb NOT NULL,
    action     text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT system_deployments_action_check CHECK (action IN ('deploy', 'rollback'))
);
CREATE INDEX IF NOT EXISTS system_deployments_current_idx
    ON ahp_registry.system_deployments (tenant, system_id, environment, created_at DESC);

-- skill_revisions stores immutable skill bundles with the scope level a caller
-- must hold to receive them (PDF rev 3.2, c. 28 Registry owns "навыки"; c. 36
-- "Навык -- Ревизия бандла в Registry; уровни admin, workspace, user").
CREATE TABLE IF NOT EXISTS ahp_registry.skill_revisions (
    id           text PRIMARY KEY,
    skill_id     text NOT NULL,
    publisher_id text NOT NULL,
    revision     bigint NOT NULL,
    level        text NOT NULL,
    bundle       jsonb NOT NULL,
    digest       text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT skill_revisions_level_check CHECK (level IN ('admin', 'workspace', 'user')),
    CONSTRAINT skill_revisions_revision_positive CHECK (revision >= 1),
    CONSTRAINT skill_revisions_digest_format CHECK (digest ~ '^sha256:[0-9a-f]{64}$')
);
CREATE UNIQUE INDEX IF NOT EXISTS skill_revisions_number_uidx
    ON ahp_registry.skill_revisions (skill_id, revision);
CREATE UNIQUE INDEX IF NOT EXISTS skill_revisions_digest_uidx
    ON ahp_registry.skill_revisions (skill_id, digest);

-- model_routes are the tenant's model route documents: the Registry owns
-- "маршруты моделей" (PDF rev 3.2, c. 28 Registry row), a manifest names a
-- route in capabilities.models, and ResolveConfig hands the resolved route to
-- the task. Revision counters make writes compare-and-swap.
CREATE TABLE IF NOT EXISTS ahp_registry.model_routes (
    id         text PRIMARY KEY,
    tenant     text NOT NULL,
    route      text NOT NULL,
    revision   bigint NOT NULL DEFAULT 1,
    document   jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT model_routes_revision_positive CHECK (revision >= 1)
);
CREATE UNIQUE INDEX IF NOT EXISTS model_routes_tenant_route_uidx
    ON ahp_registry.model_routes (tenant, route);

-- rollouts records every version replacement of an installation together with
-- the drain window the replaced version's manifest declared. That window is
-- what the Registry must honour: the old version keeps serving the tasks pinned
-- to it, but not past drain_until (PDF rev 3.2, c. 41/42). drain_window and
-- drain_until stay NULL when the manifest declared no window, which means the
-- Registry has no deadline to invent and keeps serving pinned tasks until they
-- finish.
CREATE TABLE IF NOT EXISTS ahp_registry.rollouts (
    id                text PRIMARY KEY,
    tenant            text NOT NULL,
    installation_id   text NOT NULL REFERENCES ahp_registry.installations (id),
    agent_id          text NOT NULL,
    environment       text NOT NULL,
    from_version_id   text NOT NULL REFERENCES ahp_registry.versions (id),
    to_version_id     text NOT NULL REFERENCES ahp_registry.versions (id),
    action            text NOT NULL,
    drain_window      interval,
    drain_until       timestamptz,
    compat_source     text NOT NULL DEFAULT '',
    compat_target     text NOT NULL DEFAULT '',
    created_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT rollouts_action_check CHECK (action IN ('promote', 'rollback')),
    CONSTRAINT rollouts_window_non_negative CHECK (drain_window IS NULL OR drain_window >= interval '0')
);
CREATE INDEX IF NOT EXISTS rollouts_installation_idx
    ON ahp_registry.rollouts (tenant, installation_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS ahp_registry.schema_version (
    revision   bigint PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);
`

// registryMigrationDDL upgrades tables created by an earlier DDL revision.
// CREATE TABLE IF NOT EXISTS never adds columns to an existing table, so the
// derived visibility/class columns are added here, and the legacy global
// UNIQUE(digest) index is replaced by the (agent_id, digest) pair index. The
// statements are strictly non-destructive: ADD COLUMN IF NOT EXISTS with a
// safe DEFAULT, and the only DROP targets an index, never rows.
const registryMigrationDDL = `
ALTER TABLE ahp_registry.versions
    ADD COLUMN IF NOT EXISTS visibility text NOT NULL DEFAULT 'tenant',
    ADD COLUMN IF NOT EXISTS class      text NOT NULL DEFAULT 'C';
DROP INDEX IF EXISTS ahp_registry.versions_digest_uidx;
CREATE UNIQUE INDEX IF NOT EXISTS versions_agent_digest_uidx
    ON ahp_registry.versions (agent_id, digest);

-- audience_roles is the role audience an administrator chose when publishing
-- the agent to employees (PDF rev 3.2, c. 42 "Каталог Agents Space.
-- Публикация ... с видимостью по ролям"); an empty array means every employee
-- of the tenant sees the card. publisher_disclosure is the consumer's opt-in
-- to sharing aggregates with the publisher (c. 42: "Издатель не видит ...
-- только агрегаты и оценки, которые разрешил потребитель").
ALTER TABLE ahp_registry.task_pins
    ADD COLUMN IF NOT EXISTS config_data jsonb,
    ADD COLUMN IF NOT EXISTS config_revision bigint,
    ADD COLUMN IF NOT EXISTS grant_task_id text NOT NULL DEFAULT '';

ALTER TABLE ahp_registry.installations
    ADD COLUMN IF NOT EXISTS audience_roles       jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS publisher_disclosure boolean NOT NULL DEFAULT false;

-- A task whose drain window elapsed is carried to the compatible new version
-- (PDF rev 3.2, c. 52 "Окно drain истекло: перенос на новую версию при
-- state.compat"). The origin and the moment stay on the pin, so what a task
-- actually executed remains auditable after the move.
ALTER TABLE ahp_registry.task_pins
    ADD COLUMN IF NOT EXISTS migrated_from text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS migrated_at   timestamptz;
`

// registryBackfillSQL re-derives the projected columns from the stored
// manifest. Rows whose manifest carries no usable value keep the column
// default, so the backfill never leaves a NULL or out-of-check value behind.
const registryBackfillSQL = `
UPDATE ahp_registry.versions
SET visibility = COALESCE(
        NULLIF(manifest ->> 'visibility', ''),
        'tenant');
UPDATE ahp_registry.versions
SET class = CASE
        WHEN manifest ->> 'class' IN ('S', 'C') THEN manifest ->> 'class'
        ELSE 'C'
    END;
`

// registryDDLRevision is bumped whenever registryMigrationDDL or
// registryBackfillSQL change semantics. The migration body runs ALTER TABLE
// ADD COLUMN (ACCESS EXCLUSIVE) and an UPDATE backfill that conflict with
// concurrent DML on the shared ahp_registry schema. To keep Migrate safe under
// parallel callers -- multiple test binaries sharing one database -- that
// exclusive work runs at most once per database: the first migrator applies it
// under a transaction-scoped advisory lock and records the revision in
// schema_version, while every later or concurrent Migrate observes the revision
// and skips the DDL, taking only ACCESS SHARE locks that never block workload.
const registryDDLRevision = 7

// registryMigrationLockKey namespaces the advisory lock guarding Migrate.
const registryMigrationLockKey = "ahp_registry_migration"

// Migrate creates or upgrades the ahp_registry schema. It is idempotent and
// never drops or rewrites existing records: tables are created when missing,
// the derived visibility/class projection upgrade runs once per database, and
// concurrent or repeated calls only take shared locks.
func (s *Store) Migrate(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return contracts.NewError(contracts.ErrInvalid, "registry store has no connection pool")
	}
	// Idempotent CREATE ... IF NOT EXISTS only: ACCESS SHARE once objects exist,
	// so it is compatible with in-flight DML from other callers.
	if _, err := s.pool.Exec(ctx, registrySchemaDDL+runtimeDDL+supplyChainDDL+tenantDeletionSchema+subjectDeletionSchema+profileDDL+agentTurnDDL); err != nil {
		return storageError("create registry schema", err)
	}
	// Serialise the exclusive migration against other migrators across processes.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return storageError("begin registry migration", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		registryMigrationLockKey); err != nil {
		return storageError("lock registry migration", err)
	}
	var applied int64
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM ahp_registry.schema_version WHERE revision = $1`,
		registryDDLRevision).Scan(&applied); err != nil {
		return storageError("read registry schema version", err)
	}
	if applied == 0 {
		if _, err := tx.Exec(ctx, registryMigrationDDL); err != nil {
			return storageError("upgrade registry schema", err)
		}
		if _, err := tx.Exec(ctx, registryBackfillSQL); err != nil {
			return storageError("backfill registry projections", err)
		}
		// Trigger replacement is migration DDL too. Keep it under the same
		// transaction lock/version gate so concurrent starts do not update
		// pg_proc tuples or take table locks on every Migrate call.
		if err := s.tenantGuards(ctx, tx); err != nil {
			return storageError("migrate registry tenant guards", err)
		}
		if err := s.subjectGuards(ctx, tx); err != nil {
			return storageError("migrate subject pins", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO ahp_registry.schema_version (revision) VALUES ($1)
			 ON CONFLICT (revision) DO NOTHING`,
			registryDDLRevision); err != nil {
			return storageError("record registry schema version", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return storageError("commit registry migration", err)
	}
	return nil
}

// PutVersion stores an immutable manifest version.
//
// The stored digest is recomputed from the canonical manifest; a caller that
// supplies a different digest is rejected. A digest hashes content only, so it
// is unique per agent, not globally: independent agents may publish identical
// manifests and each gets its own version row. Repeating the same agent, digest
// and manifest is idempotent and returns the original version. Reusing one
// agent's digest for a different manifest is a KEY_REUSE_MISMATCH.
//
// The derived visibility and class columns are rewritten from the manifest on
// every insert so the catalog query never trusts a stale projection.
func (s *Store) PutVersion(ctx context.Context, publisherID, agentID, digest string, manifest Manifest) (Version, error) {
	if err := requireID("agent id", agentID); err != nil {
		return Version{}, err
	}
	if err := ValidateVersionDigest(digest); err != nil {
		return Version{}, err
	}
	if err := ValidateManifest(manifest); err != nil {
		return Version{}, err
	}
	canonical, err := contracts.Digest(manifest)
	if err != nil {
		return Version{}, contracts.NewError(contracts.ErrInvalid, "manifest cannot be canonicalised: "+err.Error())
	}
	if canonical != digest {
		return Version{}, contracts.NewError(contracts.ErrKeyReuse, "digest does not match the canonical manifest")
	}
	doc, err := json.Marshal(manifest)
	if err != nil {
		return Version{}, contracts.NewError(contracts.ErrInvalid, "manifest cannot be stored: "+err.Error())
	}
	// Only the publisher of an agent may publish its versions (PDF rev 3.2,
	// c. 42: "Агент и его версии принадлежат издателю"). The manifest is
	// validated before this check so a malformed manifest never leaks whether
	// an agent name exists.
	publisher, err := s.publisherOf(ctx, nil, agentID)
	if err != nil {
		return Version{}, err
	}
	if publisher != publisherID {
		return Version{}, contracts.NewError(contracts.ErrForbidden, "agent belongs to another publisher")
	}

	// "Оценки перед публикацией": no version may be published without a passing
	// evaluation verdict for this exact digest. This runs last among the checks --
	// after the manifest/digest validation and publisher ownership -- so a
	// malformed manifest or a foreign publisher still fails for its own reason and
	// only a well-formed, owned publication is subject to the evals precondition.
	if err := s.requireEvalVerdict(ctx, digest); err != nil {
		return Version{}, err
	}

	const insert = `
INSERT INTO ahp_registry.versions (id, agent_id, digest, manifest, visibility, class)
VALUES ($1, $2, $3, $4::jsonb, $5, $6)
ON CONFLICT DO NOTHING`
	tag, err := s.pool.Exec(ctx, insert, contracts.NewID(), agentID, digest, doc,
		manifest.Visibility, manifest.Class)
	if err != nil {
		return Version{}, storageError("store manifest version", err)
	}
	if tag.RowsAffected() == 1 {
		// The row exists; read it back so created_at comes from the database.
		v, err := s.findVersionByAgentDigest(ctx, agentID, digest)
		if err != nil {
			return Version{}, err
		}
		return v, nil
	}

	// A row for this agent and digest (or, far less likely, this id) already
	// exists: decide whether the call is an idempotent replay or a mismatch.
	existing, err := s.findVersionByAgentDigest(ctx, agentID, digest)
	if err != nil {
		if contracts.Code(err) == contracts.ErrNotFound {
			return Version{}, contracts.NewError(contracts.ErrConflict, "manifest version already exists")
		}
		return Version{}, err
	}
	if !sameManifest(existing.Manifest, manifest) {
		return Version{}, contracts.NewError(contracts.ErrKeyReuse,
			"digest is already published for a different manifest")
	}
	return existing, nil
}

// requireEvalVerdict enforces the publication precondition. A store without a
// gate denies publication outright (fail-closed): an unwired evals plane must
// never degrade into "publish allowed".
func (s *Store) requireEvalVerdict(ctx context.Context, digest string) error {
	if s.evalGate == nil {
		return contracts.NewError(contracts.ErrConflict,
			"registry: publishing requires a passing evaluation verdict for the version digest (no evals gate wired; fail closed)")
	}
	return s.evalGate.RequirePassedVerdict(ctx, digest)
}

// GetVersion loads one immutable version.
func (s *Store) GetVersion(ctx context.Context, id string) (Version, error) {
	if err := requireID("version id", id); err != nil {
		return Version{}, err
	}
	const q = `SELECT id, agent_id, digest, manifest, created_at
	           FROM ahp_registry.versions WHERE id = $1`
	return scanVersion(s.pool.QueryRow(ctx, q, id))
}

// findVersionByAgentDigest loads the version one agent published for a digest.
func (s *Store) findVersionByAgentDigest(ctx context.Context, agentID, digest string) (Version, error) {
	const q = `SELECT id, agent_id, digest, manifest, created_at
	           FROM ahp_registry.versions WHERE agent_id = $1 AND digest = $2`
	return scanVersion(s.pool.QueryRow(ctx, q, agentID, digest))
}

// Install creates a tenant installation of an immutable version.
//
// granted must be a subset of the manifest capability ceiling. The installation
// is bound to the caller tenant and to one environment; a class C shared
// artifact published across tenants is refused. The idempotency key is scoped
// to the tenant: a repeat with an identical request returns the original
// installation, a repeat with a different body is a KEY_REUSE_MISMATCH.
func (s *Store) Install(ctx context.Context, tenant, agentID, versionID, environment, key string, granted Capabilities) (Installation, error) {
	if err := requireID("tenant", tenant); err != nil {
		return Installation{}, err
	}
	if err := requireID("agent id", agentID); err != nil {
		return Installation{}, err
	}
	if err := requireID("version id", versionID); err != nil {
		return Installation{}, err
	}
	if err := requireEnvironment(environment); err != nil {
		return Installation{}, err
	}
	if err := requireID("idempotency key", key); err != nil {
		return Installation{}, err
	}
	if err := validateCapabilitiesShape(granted); err != nil {
		return Installation{}, err
	}

	reqDigest, err := installRequestDigest(agentID, versionID, environment, granted)
	if err != nil {
		return Installation{}, contracts.NewError(contracts.ErrInvalid, "install request cannot be canonicalised")
	}

	var out Installation
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, tenant); err != nil {
			return err
		}
		// Serialise concurrent calls that share the tenant idempotency key so
		// a racing duplicate replays instead of tripping the unique index.
		if _, err := tx.Exec(ctx,
			`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
			"ahp_registry/install/"+tenant+"/"+key); err != nil {
			return storageError("lock install key", err)
		}

		if existing, err := findInstallationByKey(ctx, tx, tenant, key); err == nil {
			if existing.requestDigest != reqDigest {
				return contracts.NewError(contracts.ErrKeyReuse,
					"idempotency key was already used for a different install request")
			}
			out = existing.Installation
			return nil
		} else if contracts.Code(err) != contracts.ErrNotFound {
			return err
		}

		version, err := lockVersion(ctx, tx, versionID)
		if err != nil {
			return err
		}
		if version.AgentID != agentID {
			return contracts.NewError(contracts.ErrConflict, "version belongs to a different agent")
		}
		if err := requireGrantedSubset(version.Manifest.Capabilities, granted); err != nil {
			return err
		}
		if err := requireInstallationAllowed(version.Manifest, tenant); err != nil {
			return err
		}
		// Deploy admission ("SBOM и подпись" then "проверка ИБ", rev 3.2 c.41):
		// once the operator installs trust roots, an image-bearing version
		// installs only on a verified supply chain with a current evaluation
		// verdict, and only into stage/prod with a current security verdict.
		if err := s.requireDeployAdmission(ctx, tx, version, environment); err != nil {
			return err
		}

		// One installation per tenant, agent and environment.
		var slotDigest string
		row := tx.QueryRow(ctx, `
SELECT request_digest FROM ahp_registry.installations
WHERE tenant = $1 AND agent_id = $2 AND environment = $3`, tenant, agentID, environment)
		switch err := row.Scan(&slotDigest); {
		case err == nil:
			return contracts.NewError(contracts.ErrConflict, "agent is already installed in this environment")
		case !errors.Is(err, pgx.ErrNoRows):
			return storageError("check install slot", err)
		}

		id := contracts.NewID()
		doc, err := json.Marshal(granted)
		if err != nil {
			return contracts.NewError(contracts.ErrInvalid, "capabilities cannot be stored")
		}
		const ins = `
INSERT INTO ahp_registry.installations
    (id, tenant, agent_id, deployment, environment, version_id, capabilities, published, idem_key, request_digest)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, false, $8, $9)`
		if _, err := tx.Exec(ctx, ins, id, tenant, agentID, agentID, environment, versionID, doc, key, reqDigest); err != nil {
			if isUniqueViolation(err) {
				return contracts.NewError(contracts.ErrConflict, "installation already exists for this key or slot")
			}
			return storageError("insert installation", err)
		}
		if derr := recordDeployment(ctx, tx, tenant, id, agentID, environment,
			version, "install", ""); derr != nil {
			return derr
		}
		out = Installation{
			ID:           id,
			Tenant:       tenant,
			AgentID:      agentID,
			Deployment:   agentID,
			Environment:  environment,
			VersionID:    versionID,
			Capabilities: granted,
			Published:    false,
		}
		return nil
	})
	if err != nil {
		return Installation{}, err
	}
	return out, nil
}

// GetInstallation loads one installation of the caller tenant. An installation
// belonging to another tenant is reported as NOT_FOUND: existence is never
// leaked to a non-owner.
func (s *Store) GetInstallation(ctx context.Context, tenant, id string) (Installation, error) {
	if err := requireID("tenant", tenant); err != nil {
		return Installation{}, err
	}
	if err := requireID("installation id", id); err != nil {
		return Installation{}, err
	}
	const q = `SELECT id, tenant, agent_id, deployment, environment, version_id, capabilities, published
	           FROM ahp_registry.installations WHERE tenant = $1 AND id = $2`
	return scanInstallation(s.pool.QueryRow(ctx, q, tenant, id))
}

// Promote moves an installation to another version of the same agent. The
// granted capabilities are kept and must still fit the new manifest ceiling.
// Pins taken by running tasks are never rewritten.
func (s *Store) Promote(ctx context.Context, tenant, installationID, versionID string) (Installation, error) {
	if err := requireID("tenant", tenant); err != nil {
		return Installation{}, err
	}
	if err := requireID("installation id", installationID); err != nil {
		return Installation{}, err
	}
	if err := requireID("version id", versionID); err != nil {
		return Installation{}, err
	}

	// Probe the promotion target's runtime BEFORE moving the version pointer, so
	// live traffic is never switched onto a runtime that is not serving. The read
	// is deliberately outside the transaction: a readiness probe is a network
	// round-trip and must not run while the installation row is locked. A failed
	// installation read is left to the transaction below, which returns the
	// authoritative typed error.
	if probeInst, err := s.GetInstallation(ctx, tenant, installationID); err == nil {
		if gerr := s.gatePromoteReadiness(ctx, probeInst, versionID); gerr != nil {
			return Installation{}, gerr
		}
	}

	var out Installation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, tenant); err != nil {
			return err
		}
		inst, err := lockInstallation(ctx, tx, tenant, installationID)
		if err != nil {
			return err
		}
		version, err := lockVersion(ctx, tx, versionID)
		if err != nil {
			return err
		}
		if version.AgentID != inst.AgentID {
			return contracts.NewError(contracts.ErrConflict, "promotion target belongs to a different agent")
		}
		if err := requireGrantedSubset(version.Manifest.Capabilities, inst.Capabilities); err != nil {
			return contracts.NewError(contracts.ErrConflict,
				"granted capabilities exceed the promotion target ceiling: "+err.Error())
		}
		if err := requireInstallationAllowed(version.Manifest, tenant); err != nil {
			return err
		}
		// "Оценки перед публикацией" governs promotion, not only first
		// publication: the target version must carry a verdict that is fresh
		// under the CURRENT eval policy (row 24). This runs unconditionally --
		// for manifest-only versions and with the supply chain off as well --
		// and re-derives everything from the target digest at decision time, so
		// mutating the promotion request cannot name its way past the gate.
		if err := s.requireEvalVerdict(ctx, version.Digest); err != nil {
			return err
		}
		// Promotion carries the same supply-chain guarantee as a fresh install:
		// a superseded (failed/stale) eval or security verdict blocks moving an
		// installation onto a new image.
		if err := s.requireDeployAdmission(ctx, tx, version, inst.Environment); err != nil {
			return err
		}
		previous, err := getVersion(ctx, tx, inst.VersionID)
		if err != nil {
			return err
		}
		const q = `
UPDATE ahp_registry.installations SET version_id = $1, updated_at = now()
WHERE tenant = $2 AND id = $3`
		if _, err := tx.Exec(ctx, q, versionID, tenant, installationID); err != nil {
			return storageError("promote installation", err)
		}
		if derr := recordDeployment(ctx, tx, tenant, installationID, inst.AgentID,
			inst.Environment, version, "promote", ""); derr != nil {
			return derr
		}
		// The replaced version now owes a drain: record the window ITS manifest
		// declared, so ResolveConfig keeps serving pinned tasks up to that
		// deadline and decides by the declared compatibility afterwards.
		if derr := recordRollout(ctx, tx, tenant, installationID, inst.AgentID,
			inst.Environment, previous, version, "promote"); derr != nil {
			return derr
		}
		inst.VersionID = versionID
		out = inst.Installation
		return nil
	})
	if err != nil {
		return Installation{}, err
	}
	return out, nil
}

// Pin binds a task to an immutable version. The first pin wins and is returned
// unchanged for every later call, so a task keeps its version across promote,
// rollback and republish. A pin can only be taken through a tenant installation,
// so a caller cannot pin a version of another tenant's artifact.
func (s *Store) Pin(ctx context.Context, tenant, installationID, taskID string) (Version, error) {
	return s.pin(ctx, tenant, "", installationID, taskID)
}

// PinForSubject captures the verified caller alongside the immutable task snapshot.
func (s *Store) PinForSubject(ctx context.Context, scope contracts.Scope, installationID, taskID string) (Version, error) {
	if err := scope.Validate(); err != nil {
		return Version{}, err
	}
	return s.pin(ctx, scope.Tenant, scope.Subject, installationID, taskID)
}
func (s *Store) pin(ctx context.Context, tenant, subject, installationID, taskID string) (Version, error) {
	if err := requireID("tenant", tenant); err != nil {
		return Version{}, err
	}
	if err := requireID("installation id", installationID); err != nil {
		return Version{}, err
	}
	if err := requireID("task id", taskID); err != nil {
		return Version{}, err
	}

	var out Version
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, tenant); err != nil {
			return err
		}
		if err := checkSubjectWritable(ctx, tx, tenant, subject); err != nil {
			return err
		}
		// The installation row is the serialisation point: it is locked by both
		// Pin and Promote, so a pin always observes a stable version pointer.
		inst, err := lockInstallation(ctx, tx, tenant, installationID)
		if err != nil {
			return err
		}

		var pinnedVersion, pinnedInstallation, pinnedSubject string
		row := tx.QueryRow(ctx, `
SELECT version_id, installation_id, subject FROM ahp_registry.task_pins WHERE tenant = $1 AND task_id = $2`, tenant, taskID)
		switch err := row.Scan(&pinnedVersion, &pinnedInstallation, &pinnedSubject); {
		case err == nil:
			if pinnedInstallation != installationID || pinnedSubject != subject {
				return contracts.NewError(contracts.ErrConflict, "task belongs to another installation")
			}
			v, err := getVersion(ctx, tx, pinnedVersion)
			if err != nil {
				return err
			}
			out = v
			return nil
		case !errors.Is(err, pgx.ErrNoRows):
			return storageError("read task pin", err)
		}

		configData, configRevision := json.RawMessage(`{}`), int64(0)
		config, cerr := getConfig(ctx, tx, tenant, inst.AgentID)
		if cerr == nil {
			configData, configRevision = config.Data, config.Revision
		} else if contracts.Code(cerr) != contracts.ErrNotFound {
			return cerr
		}
		// A new pin decides what a task STARTS on, so the version it names has to
		// clear the same admission the rollout cleared. Without this, a version
		// that reached the installation row by any other route -- a direct write
		// behind the Registry's back, or a verdict that failed after the promote --
		// would still take new tasks.
		v, err := getVersion(ctx, tx, inst.VersionID)
		if err != nil {
			return err
		}
		if err := s.requireDeployAdmission(ctx, tx, v, inst.Environment); err != nil {
			return err
		}
		const ins = `
INSERT INTO ahp_registry.task_pins (task_id, tenant, installation_id, version_id, config_data, config_revision,subject)
VALUES ($1, $2, $3, $4, $5::jsonb, $6,$7)`
		if _, err := tx.Exec(ctx, ins, taskID, tenant, inst.ID, inst.VersionID, configData, configRevision, subject); err != nil {
			if isUniqueViolation(err) {
				return contracts.NewError(contracts.ErrConflict, "task is already pinned")
			}
			return storageError("insert task pin", err)
		}
		out = v
		return nil
	})
	if err != nil {
		return Version{}, err
	}
	return out, nil
}

// PinnedVersion returns the version a task is pinned to, or NOT_FOUND when the
// task has never been pinned.
func (s *Store) PinnedVersion(ctx context.Context, tenant, taskID string) (Version, error) {
	if err := requireID("tenant", tenant); err != nil {
		return Version{}, err
	}
	if err := requireID("task id", taskID); err != nil {
		return Version{}, err
	}
	const q = `SELECT v.id, v.agent_id, v.digest, v.manifest, v.created_at
	           FROM ahp_registry.task_pins p JOIN ahp_registry.versions v ON v.id = p.version_id
	           WHERE p.tenant = $1 AND p.task_id = $2`
	return scanVersion(s.pool.QueryRow(ctx, q, tenant, taskID))
}

// CountPinnedTasks reports how many tasks are pinned to a version through one
// installation. A deploy controller uses it to decide whether a drain may end.
func (s *Store) CountPinnedTasks(ctx context.Context, tenant, installationID, versionID string) (int, error) {
	if err := requireID("tenant", tenant); err != nil {
		return 0, err
	}
	if err := requireID("installation id", installationID); err != nil {
		return 0, err
	}
	if err := requireID("version id", versionID); err != nil {
		return 0, err
	}
	const q = `SELECT count(*) FROM ahp_registry.task_pins
	           WHERE tenant = $1 AND installation_id = $2 AND version_id = $3`
	var n int
	if err := s.pool.QueryRow(ctx, q, tenant, installationID, versionID).Scan(&n); err != nil {
		return 0, storageError("count pinned tasks", err)
	}
	return n, nil
}

// Publish flips catalog visibility of an installation. Only a prod installation
// may become visible: stage and dev artifacts never leak into a catalog.
func (s *Store) Publish(ctx context.Context, tenant, installationID string, visible bool) error {
	if err := requireID("tenant", tenant); err != nil {
		return err
	}
	if err := requireID("installation id", installationID); err != nil {
		return err
	}
	return s.setCatalogPublication(ctx, tenant, installationID, visible, nil)
}

// Catalog lists the published installations of one tenant, and only that
// tenant's. An installation always belongs to a single consuming tenant: the
// manifest catalog visibility flag governs whether the agent package may be
// installed by other tenants, never whether another tenant's installation row
// is visible. Foreign installation ids, deployments and grants never leak.
// Unpublished rows are invisible to everyone but their owner, which
// GetInstallation serves directly.
func (s *Store) Catalog(ctx context.Context, tenant string) ([]Installation, error) {
	if err := requireID("tenant", tenant); err != nil {
		return nil, err
	}
	// Only published rows of the caller tenant are ever listed.
	const q = `
SELECT i.id, i.tenant, i.agent_id, i.deployment, i.environment, i.version_id, i.capabilities, i.published
FROM ahp_registry.installations i
WHERE i.published AND i.tenant = $1
ORDER BY i.agent_id, i.environment, i.id`
	rows, err := s.pool.Query(ctx, q, tenant)
	if err != nil {
		return nil, storageError("read catalog", err)
	}
	defer rows.Close()

	out := make([]Installation, 0, 8)
	for rows.Next() {
		inst, err := scanInstallation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, inst)
	}
	if err := rows.Err(); err != nil {
		return nil, storageError("read catalog", err)
	}
	return out, nil
}

// PutConfig writes the tenant configuration document for an agent using
// compare-and-swap on the revision counter. expectedRevision must be 0 for the
// first write; a stale expectation is a CONFLICT carrying the current revision
// so the caller can re-read and retry.
func (s *Store) PutConfig(ctx context.Context, tenant, agentID string, expectedRevision int64, data json.RawMessage) (Config, error) {
	if agentID == DotAgentID {
		if err := validateDotConfig(data); err != nil {
			return Config{}, err
		}
	}
	if err := requireID("tenant", tenant); err != nil {
		return Config{}, err
	}
	if err := requireID("agent id", agentID); err != nil {
		return Config{}, err
	}
	if expectedRevision < 0 {
		return Config{}, contracts.NewError(contracts.ErrInvalid, "expected revision must not be negative")
	}
	if len(data) == 0 {
		return Config{}, contracts.NewError(contracts.ErrInvalid, "configuration data must not be empty")
	}
	var probe any
	if err := json.Unmarshal(data, &probe); err != nil {
		return Config{}, contracts.NewError(contracts.ErrInvalid, "configuration data is not valid JSON")
	}
	if probe == nil {
		return Config{}, contracts.NewError(contracts.ErrInvalid, "configuration data must not be null")
	}

	var out Config
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
			"ahp_registry/config/"+tenant+"/"+agentID); err != nil {
			return storageError("lock config", err)
		}
		current, err := getConfig(ctx, tx, tenant, agentID)
		switch {
		case err == nil:
			if current.Revision != expectedRevision {
				return contracts.NewError(contracts.ErrConflict, fmt.Sprintf(
					"configuration revision is %d, expected %d", current.Revision, expectedRevision))
			}
			const q = `
UPDATE ahp_registry.configs SET data = $1::jsonb, revision = revision + 1, updated_at = now()
WHERE tenant = $2 AND agent_id = $3 AND revision = $4
RETURNING id, tenant, agent_id, revision, data`
			return scanConfig(tx.QueryRow(ctx, q, string(data), tenant, agentID, expectedRevision), &out)
		case contracts.Code(err) == contracts.ErrNotFound:
			if expectedRevision != 0 {
				return contracts.NewError(contracts.ErrConflict,
					"configuration does not exist yet, expected revision must be 0")
			}
			const q = `
INSERT INTO ahp_registry.configs (id, tenant, agent_id, revision, data)
VALUES ($1, $2, $3, 1, $4::jsonb)
RETURNING id, tenant, agent_id, revision, data`
			return scanConfig(tx.QueryRow(ctx, q, contracts.NewID(), tenant, agentID, string(data)), &out)
		default:
			return err
		}
	})
	if err != nil {
		return Config{}, err
	}
	return out, nil
}

// GetConfig reads the current configuration document of an agent for a tenant.
func (s *Store) GetConfig(ctx context.Context, tenant, agentID string) (Config, error) {
	if err := requireID("tenant", tenant); err != nil {
		return Config{}, err
	}
	if err := requireID("agent id", agentID); err != nil {
		return Config{}, err
	}
	var out Config
	if err := scanConfig(s.pool.QueryRow(ctx, configSelect, tenant, agentID), &out); err != nil {
		return Config{}, err
	}
	return out, nil
}

const configSelect = `SELECT id, tenant, agent_id, revision, data
                      FROM ahp_registry.configs WHERE tenant = $1 AND agent_id = $2`

// getConfig reads a configuration row inside a transaction.
func getConfig(ctx context.Context, tx pgx.Tx, tenant, agentID string) (Config, error) {
	var out Config
	if err := scanConfig(tx.QueryRow(ctx, configSelect, tenant, agentID), &out); err != nil {
		return Config{}, err
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Row helpers
// ---------------------------------------------------------------------------

// installationRow is an installation plus its idempotency request digest.
type installationRow struct {
	Installation
	requestDigest string
}

// inTx runs fn inside a transaction and commits it when fn returns nil.
func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return storageError("begin transaction", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return storageError("commit transaction", err)
	}
	return nil
}

func getVersion(ctx context.Context, tx pgx.Tx, id string) (Version, error) {
	const q = `SELECT id, agent_id, digest, manifest, created_at
	           FROM ahp_registry.versions WHERE id = $1`
	return scanVersion(tx.QueryRow(ctx, q, id))
}

// lockVersion takes a shared row lock so a concurrent idempotent replay of the
// same version cannot interleave with an install.
func lockVersion(ctx context.Context, tx pgx.Tx, id string) (Version, error) {
	const q = `SELECT id, agent_id, digest, manifest, created_at
	           FROM ahp_registry.versions WHERE id = $1 FOR SHARE`
	return scanVersion(tx.QueryRow(ctx, q, id))
}

// lockInstallation loads and locks one installation of the caller tenant. Both
// Pin and Promote take this lock, which is what makes a pin stable.
func lockInstallation(ctx context.Context, tx pgx.Tx, tenant, id string) (installationRow, error) {
	var row installationRow
	const q = `
SELECT id, tenant, agent_id, deployment, environment, version_id, capabilities, published, request_digest
FROM ahp_registry.installations WHERE tenant = $1 AND id = $2 FOR UPDATE`
	err := tx.QueryRow(ctx, q, tenant, id).Scan(
		&row.ID, &row.Tenant, &row.AgentID, &row.Deployment, &row.Environment,
		&row.VersionID, &row.Capabilities, &row.Published, &row.requestDigest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return installationRow{}, contracts.NewError(contracts.ErrNotFound, "installation not found")
		}
		return installationRow{}, storageError("lock installation", err)
	}
	return row, nil
}

// findInstallationByKey resolves a tenant-scoped idempotency key.
func findInstallationByKey(ctx context.Context, tx pgx.Tx, tenant, key string) (installationRow, error) {
	var row installationRow
	const q = `
SELECT id, tenant, agent_id, deployment, environment, version_id, capabilities, published, request_digest
FROM ahp_registry.installations WHERE tenant = $1 AND idem_key = $2`
	err := tx.QueryRow(ctx, q, tenant, key).Scan(
		&row.ID, &row.Tenant, &row.AgentID, &row.Deployment, &row.Environment,
		&row.VersionID, &row.Capabilities, &row.Published, &row.requestDigest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return installationRow{}, contracts.NewError(contracts.ErrNotFound, "installation not found")
		}
		return installationRow{}, storageError("read install key", err)
	}
	return row, nil
}

// scanner is satisfied by pgx rows, so one scan helper serves queries and
// catalog iteration.
type scanner interface {
	Scan(dest ...any) error
}

func scanVersion(row scanner) (Version, error) {
	var (
		v   Version
		doc []byte
		cts time.Time
	)
	err := row.Scan(&v.ID, &v.AgentID, &v.Digest, &doc, &cts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Version{}, contracts.NewError(contracts.ErrNotFound, "manifest version not found")
		}
		return Version{}, storageError("read manifest version", err)
	}
	if err := json.Unmarshal(doc, &v.Manifest); err != nil {
		return Version{}, contracts.NewError(contracts.ErrConflict, "stored manifest is unreadable")
	}
	v.CreatedAt = cts.UTC()
	return v, nil
}

func scanInstallation(row scanner) (Installation, error) {
	var (
		inst Installation
		doc  []byte
	)
	err := row.Scan(&inst.ID, &inst.Tenant, &inst.AgentID, &inst.Deployment,
		&inst.Environment, &inst.VersionID, &doc, &inst.Published)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Installation{}, contracts.NewError(contracts.ErrNotFound, "installation not found")
		}
		return Installation{}, storageError("read installation", err)
	}
	if len(doc) > 0 {
		if err := json.Unmarshal(doc, &inst.Capabilities); err != nil {
			return Installation{}, contracts.NewError(contracts.ErrConflict, "stored capabilities are unreadable")
		}
	}
	return inst, nil
}

func scanConfig(row scanner, out *Config) error {
	var doc []byte
	err := row.Scan(&out.ID, &out.Tenant, &out.AgentID, &out.Revision, &doc)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrNotFound, "configuration not found")
		}
		return storageError("read configuration", err)
	}
	out.Data = json.RawMessage(doc)
	return nil
}

// ---------------------------------------------------------------------------
// Invariants enforced in Go on top of the schema
// ---------------------------------------------------------------------------

// installRequestDigest is the canonical digest of an install request body.
func installRequestDigest(agentID, versionID, environment string, granted Capabilities) (string, error) {
	return contracts.Digest(map[string]any{
		"agent_id":    agentID,
		"version_id":  versionID,
		"environment": environment,
		"granted":     granted,
	})
}

// sameManifest compares two manifests by canonical digest.
func sameManifest(a, b Manifest) bool {
	da, err := contracts.Digest(a)
	if err != nil {
		return false
	}
	db, err := contracts.Digest(b)
	if err != nil {
		return false
	}
	return da == db
}

// requireGrantedSubset rejects granted capabilities outside the manifest
// ceiling: a grant can only ever be narrowed, never widened.
func requireGrantedSubset(ceiling, granted Capabilities) error {
	for _, pair := range []struct {
		field string
		want  []string
		allow []string
	}{
		{"models", granted.Models, ceiling.Models},
		{"tools", granted.Tools, ceiling.Tools},
		{"memory", granted.Memory, ceiling.Memory},
		{"compute", granted.Compute, ceiling.Compute},
		{"calls", granted.Calls, ceiling.Calls},
	} {
		for _, g := range pair.want {
			if !containsString(pair.allow, g) {
				return contracts.NewError(contracts.ErrInvalid,
					"granted "+pair.field+" capability "+g+" is not declared by the manifest")
			}
		}
	}
	if granted.SelfTasks > ceiling.SelfTasks {
		return contracts.NewError(contracts.ErrInvalid, "granted self_tasks exceeds the manifest ceiling")
	}
	return nil
}

// requireInstallationAllowed enforces installation isolation. The tenant
// argument always comes from verified identity, so a caller cannot install into
// another tenant by claiming a different tenant in the request body.
func requireInstallationAllowed(m Manifest, tenant string) error {
	if m.Visibility == VisibilitySystem {
		return contracts.NewError(contracts.ErrForbidden, "system artifacts are not tenant installable")
	}
	if m.Class == ClassC && m.Audience == AudienceShared && m.Visibility == VisibilityCatalog {
		return contracts.NewError(contracts.ErrForbidden,
			"class C shared artifacts cannot be published across tenants")
	}
	if tenant == "" {
		return contracts.NewError(contracts.ErrForbidden, "installation requires a verified tenant")
	}
	return nil
}

// validateCapabilitiesShape checks a granted capability set on its own, without
// a ceiling; the ceiling is checked against the stored manifest.
func validateCapabilitiesShape(c Capabilities) error {
	return validateCapabilities(Manifest{Capabilities: c})
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// requireID accepts only bounded, path-safe identifiers.
func requireID(field, value string) error {
	if value == "" {
		return contracts.NewError(contracts.ErrInvalid, field+" must not be empty")
	}
	if len(value) > 128 {
		return contracts.NewError(contracts.ErrInvalid, field+" exceeds 128 bytes")
	}
	for i := 0; i < len(value); i++ {
		switch c := value[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '-', c == '_', c == ':', c == '@':
		default:
			return contracts.NewError(contracts.ErrInvalid, field+" contains an unsafe identifier character")
		}
	}
	return nil
}

// requireEnvironment checks the environment identifier.
func requireEnvironment(env string) error {
	if err := requireID("environment", env); err != nil {
		return err
	}
	if strings.ToLower(env) != env {
		return contracts.NewError(contracts.ErrInvalid, "environment must be lowercase")
	}
	return nil
}

// storageError maps a driver failure onto the shared error vocabulary without
// echoing query parameters or document content.
func storageError(op string, err error) error {
	if err == nil {
		return nil
	}
	if code := contracts.Code(err); code != "" {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.NewError(contracts.ErrNotFound, op+": not found")
	}
	if isUniqueViolation(err) {
		return contracts.NewError(contracts.ErrConflict, op+": already exists")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505", "23514", "40001":
			return contracts.NewError(contracts.ErrConflict, op+": storage conflict")
		case "23503":
			return contracts.NewError(contracts.ErrNotFound, op+": referenced record is missing")
		}
	}
	return fmt.Errorf("registry: %s: %w", op, err)
}

// isUniqueViolation reports a PostgreSQL unique constraint violation.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
