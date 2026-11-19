// Package identity implements the AHP delegation authority: grants, the actor
// chain, capability narrowing, the task/context label journal and subtree
// cancellation.
//
// The store owns the ahp_identity schema exclusively and never touches SQL
// belonging to another service. Every decision about whether a workload may
// act is taken here, against the PostgreSQL clock, from rows this package
// wrote; no caller-supplied timestamp is ever trusted.
//
// Invariants enforced transactionally inside PostgreSQL:
//
//   - Scope (tenant, deployment, subject) filters every operation. A foreign
//     scope reports NOT_FOUND, so existence never leaks across tenants.
//   - A grant is idempotent under (tenant, deployment, subject, task). The
//     canonical body fingerprint decides whether a repeat is a replay
//     (returns the stored grant) or a key reuse (KEY_REUSE_MISMATCH).
//   - Delegation may only narrow: requested capabilities must be a subset of
//     the parent's, expiry may not pass the parent's, depth may not exceed
//     the parent's ceiling, and the actor chain may not repeat an actor
//     unless the caller explicitly requested a self-delegation.
//   - Fan-out and active-subtree limits are counted under the root advisory
//     lock, the same lock subtree cancellation takes. A child can therefore
//     never appear in a subtree that was cancelled, and a cancellation can
//     never miss a child that was already created.
//   - Evaluate re-checks the whole ancestor chain for status and expiry on
//     every call and matches the requested capability exactly: a wildcard is
//     never authorised.
//   - Labels only accumulate. AddLabels propagates a set to the target
//     context and to every ancestor grant and context in one transaction, so
//     a private child can never be read through an untainted parent.
//
// Evaluate is the current-grant half of authorisation. Resource-level IAM is
// a separate adapter wired by its own service; nothing here validates
// corporate IAM.
package identity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Grant lifecycle statuses. The strings are part of the persisted format.
const (
	StatusActive  = "active"
	StatusRevoked = "revoked"
)

// MaxTreeDepth is the deepest delegation level AHP allows (the root is level
// 0). It is the hard ceiling of any requested depth limit and matches the
// registry manifest ceiling.
const MaxTreeDepth = 16

// Default and hard-ceiling tree limits. A root carries these as ceilings for
// its whole tree, never as targets.
const (
	// DefaultMaxDepth is the depth ceiling of a root whose caller did not
	// state one.
	DefaultMaxDepth = 4
	// DefaultMaxFanout bounds the direct children of one grant by default.
	DefaultMaxFanout = 32
	// DefaultMaxActive bounds the live subtree of one root by default.
	DefaultMaxActive = 256
	// MaxTreeFanout is the hard ceiling of a requested fan-out limit.
	MaxTreeFanout = 1024
	// MaxTreeActive is the hard ceiling of a requested active-subtree limit.
	MaxTreeActive = 100000
)

// maxTxAttempts is the SAFETY VALVE of runTxMode's replay loop, not its
// stopping rule. The loop stops when a retriable abort is no longer retriable by
// CLASS (only 40001/40P01 and an undecidable context claim ever re-enter it) or
// when the request's replay budget in TIME has run out (txReplayBudget). A count
// used to be the stopping rule, and it was the wrong instrument: at ~140-160
// legitimate concurrent delegations a writer could exhaust 16 attempts while its
// caller's own deadline was minutes away, so the service answered a correct
// request with CONFLICT - contention became an outage at a number rather than at
// a time. The valve stays because a store that aborts instantaneously forever
// must not spin unboundedly even inside its budget.
const maxTxAttempts = 4096

// The replay budget and backoff shape of runTxMode. The budget is the stopping
// rule (bounded time, not a blind count); the ceiling keeps one wait from
// eating the budget, and the floor keeps a hot contention window from becoming
// a busy spin. Values are measured, not guessed: at 8x25 delegations over one
// scope the observed abort storm clears in well under 2s of replay, while a
// 16-attempt counter was giving up inside ~1s of it. 5s is the number that held
// under a whole package's worth of concurrent load on one database, where 2s did
// not (10 of 600 legitimate delegations were still refused at 2s).
const (
	txReplayBudget     = 5 * time.Second
	minTxReplayBackoff = 5 * time.Millisecond
	maxTxReplayBackoff = 50 * time.Millisecond
)

// MaxTokenTTL is the hard ceiling of a minted task token. A grant that lives
// MaxTokenTTL is the hard ceiling of a minted task token. A grant that lives
// longer never produces a longer token.
const MaxTokenTTL = 15 * time.Minute

// maxCapabilities bounds the capability set of one grant.
const maxCapabilities = 256

// lockSpace namespaces the identity advisory locks so they can never collide
// with an advisory lock taken by another service on the same cluster.
const lockSpace = int64(0x69646e74) // "idnt"

// Store is the identity authority backed by PostgreSQL.
type Store struct {
	pool      *pgxpool.Pool
	resources ResourceEvaluator
}

// New returns a Store on the given pool. The pool must point at a database
// the identity service may create its own schema in.
func New(pool *pgxpool.Pool, options ...StoreOption) *Store {
	s := &Store{pool: pool}
	for _, option := range options {
		option(s)
	}
	return s
}

// schemaMigrateLockKey guards the identity schema DDL. A fixed, service-wide
// advisory-lock key lets every migrator (any connection, process or test
// package sharing the database) serialize on one lock.
const schemaMigrateLockKey int64 = 720261029

// schemaVersion records, in ahp_identity.schema_version, which revision of
// schemaDDL the database has had applied. Bump it whenever schemaDDL changes
// in a way an already-migrated database must re-apply; Migrate then re-runs
// the idempotent DDL once more under the advisory lock and records the new
// version. It is monotonic and never decreases, so a re-run is always safe.
//
// Version 2 adds the browser authentication tables (accounts, argon2id
// credentials, sessions and access-token bindings) that the Agent Space UI
// login flow needs. The migration is additive: nothing existing is altered.
//
// Version 8 adds ahp_identity.context_label_events with its append-only
// trigger. The table arrived in schemaDDL with 1152a8ea and the version was not
// raised, so every database already stamped 7 -- the canonical build and the
// live stand included -- was told its schema was current while the journal it
// now reads did not exist, and every context widening failed with 42P01. The
// DDL and the guards it re-runs are idempotent, so this transition is a no-op
// wherever the table is already present.
const schemaVersion int64 = 8

// sentinelTableDDL creates the migration sentinel. It is safe to run on every
// migrate: once the table exists the statement takes only ACCESS SHARE, which
// is compatible with the row-level DML another test package may hold open, so
// it can never block or deadlock against that DML.
const sentinelTableDDL = `CREATE TABLE IF NOT EXISTS ahp_identity.schema_version (
    component   text        NOT NULL,
    version     bigint      NOT NULL,
    migrated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT schema_version_pk PRIMARY KEY (component)
)`

// sentinelVersionSQL records that schemaDDL up to the bound version has been
// applied, so no later migrator repeats the lock-heavy parts of the DDL.
const sentinelVersionSQL = `
INSERT INTO ahp_identity.schema_version (component, version)
VALUES ('ddl', $1)
ON CONFLICT (component) DO UPDATE
    SET version = EXCLUDED.version, migrated_at = clock_timestamp()`

// Migrate idempotently brings the ahp_identity schema up to schemaVersion. It
// never alters, truncates or drops anything that already exists.
//
// The DDL is idempotent (every object statement is CREATE ... IF NOT EXISTS)
// but it is not safe to issue concurrently with a second writer: the trigger
// drop/recreate and the audit-table reconciliation inside schemaDDL take
// ACCESS EXCLUSIVE, and a session advisory lock serializes migrate-vs-migrate
// but cannot shield that DDL from another test package's in-flight DML on the
// shared schema — which is exactly the deadlock_detected (SQLSTATE 40P01) a
// naive per-test Migrate produces when two packages run in parallel.
//
// The fix is to apply schemaDDL at most once per database. Under the advisory
// lock Migrate reads the recorded version from the sentinel table and runs the
// full DDL only when it is missing or stale. The first migrator to arrive does
// the DDL while every other migrator is still blocked in Migrate (never in
// DML), records the version, and releases; from then on every Migrate sees the
// version and issues only the sentinel-table IF-NOT-EXISTS and a read, neither
// of which conflicts with concurrent DML. The invariant — one writer of the
// ACCESS EXCLUSIVE DDL at a time, never alongside DML — is what makes a
// parallel multi-package test run free of migration deadlocks.
func (s *Store) Migrate(ctx context.Context) error {
	if s.pool == nil {
		return contracts.NewError(contracts.ErrInvalid, "identity: store has no pool")
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("identity: migrate acquire: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", schemaMigrateLockKey); err != nil {
		return fmt.Errorf("identity: migrate lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", schemaMigrateLockKey)
	}()
	// The schema has to exist before the sentinel can live in it. The
	// statement is a catalog check once the schema is present and takes no
	// table locks.
	if _, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS ahp_identity"); err != nil {
		return fmt.Errorf("identity: migrate create schema: %w", err)
	}
	if _, err := conn.Exec(ctx, sentinelTableDDL); err != nil {
		return fmt.Errorf("identity: migrate sentinel table: %w", err)
	}
	var current int64
	err = conn.QueryRow(ctx,
		"SELECT version FROM ahp_identity.schema_version WHERE component = 'ddl'").Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		current = 0
	} else if err != nil {
		return fmt.Errorf("identity: migrate read version: %w", err)
	}
	if current < schemaVersion {
		// The revision's DDL is about to take ACCESS EXCLUSIVE on tables live
		// traffic uses, so the one replacement it performs is decided before that
		// window opens: a catalog read that locks nothing says whether this
		// installation still holds the abandoned token_hash shape of
		// browser_token_bindings. Healthy installations never have a DROP TABLE
		// sent to them at all; only an installation carrying the retired shape
		// pays for the exclusive lock, and it pays once, at the front.
		legacy, err := holdsLegacyBrowserTokenBindings(ctx, conn)
		if err != nil {
			return err
		}
		if legacy {
			if err := retireLegacyBrowserTokenBindings(ctx, conn); err != nil {
				return err
			}
		}
		if _, err := conn.Exec(ctx, schemaDDL); err != nil {
			return fmt.Errorf("identity: migrate schema ahp_identity: %w", err)
		}
		// Use the migration lease: acquiring from the pool here deadlocks a
		// single-connection pool. Trigger DDL also belongs behind the version
		// gate so later Migrate calls do not lock tables used by live traffic.
		if err := s.tenantGuards(ctx, conn); err != nil {
			return fmt.Errorf("identity: migrate tenant guards: %w", err)
		}
		if err := s.subjectGuards(ctx, conn); err != nil {
			return fmt.Errorf("identity: migrate subject guards: %w", err)
		}
		if _, err := conn.Exec(ctx, sentinelVersionSQL, schemaVersion); err != nil {
			return fmt.Errorf("identity: migrate record version: %w", err)
		}
	}
	return nil
}

// Grant is one delegation authority. A root grant has Depth 0, an empty
// Parent and Root equal to its own ID. Labels is the accumulated visibility
// set of the grant's context journal, read through the journal, so a label
// added after the grant was created is visible here immediately.
type Grant struct {
	ID         string   `json:"id"`
	Tenant     string   `json:"tenant"`
	Deployment string   `json:"deployment"`
	Subject    string   `json:"subject"`
	Task       string   `json:"task"`
	Root       string   `json:"root"`
	Context    string   `json:"context"`
	Parent     string   `json:"parent,omitempty"`
	Status     string   `json:"status"`
	Epoch      int64    `json:"epoch"`
	Depth      int      `json:"depth"`
	Actors     []string `json:"actors"`
	// Capabilities is the exact set this grant may exercise. It lives on the
	// grant, never in a token, so revocation is immediate.
	Capabilities []string   `json:"capabilities"`
	Labels       labels.Set `json:"labels"`
	ExpiresAt    time.Time  `json:"expires_at"`

	// Delegation ceiling carried by this grant. Children may not exceed it.
	MaxDepth  int  `json:"max_depth"`
	MaxFanout int  `json:"max_fanout"`
	MaxActive int  `json:"max_active"`
	AllowSelf bool `json:"allow_self"`

	// bodyDigest is the canonical fingerprint of the request that created
	// this grant; it is never part of the exported document.
	bodyDigest string
}

// Scope projects the grant onto the platform isolation scope.
func (g Grant) Scope() contracts.Scope {
	return contracts.Scope{Tenant: g.Tenant, Deployment: g.Deployment, Subject: g.Subject}
}

// RootRequest creates the top of a delegation tree. Deployment must equal the
// caller scope deployment: a request body never chooses where it runs.
//
// MaxDepth, MaxFanout, MaxActive and AllowSelf are tree ceilings supplied by
// a trusted service after manifest and tenant authorisation, never by a
// public API body: this package trusts the caller of CreateRoot to have
// authorised them. Zero means the package default; values outside the
// documented ranges are rejected.
type RootRequest struct {
	Task         string        `json:"task"`
	Context      string        `json:"context"`
	Deployment   string        `json:"deployment"`
	Capabilities []string      `json:"capabilities"`
	TTL          time.Duration `json:"ttl"`
	// MaxDepth is the deepest delegation level the tree may reach, 1..16.
	// Zero selects DefaultMaxDepth.
	MaxDepth int `json:"max_depth"`
	// MaxFanout bounds the direct children of any grant in the tree, 1..1024.
	// Zero selects DefaultMaxFanout.
	MaxFanout int `json:"max_fanout"`
	// MaxActive bounds the live subtree of the root, 1..100000. Zero selects
	// DefaultMaxActive.
	MaxActive int  `json:"max_active"`
	AllowSelf bool `json:"allow_self"`
}

// normalizeLimits replaces zero limits with their defaults and rejects
// anything outside the hard ranges. It runs before the body digest is
// computed, so an omitted limit and an explicit default are the same request.
func normalizeLimits(r RootRequest) (maxDepth, maxFanout, maxActive int, err error) {
	maxDepth, maxFanout, maxActive = r.MaxDepth, r.MaxFanout, r.MaxActive
	switch {
	case maxDepth == 0:
		maxDepth = DefaultMaxDepth
	case maxDepth < 0 || maxDepth > MaxTreeDepth:
		return 0, 0, 0, invalid("identity: max_depth must be between 1 and 16")
	}
	switch {
	case maxFanout == 0:
		maxFanout = DefaultMaxFanout
	case maxFanout < 0 || maxFanout > MaxTreeFanout:
		return 0, 0, 0, invalid("identity: max_fanout must be between 1 and 1024")
	}
	switch {
	case maxActive == 0:
		maxActive = DefaultMaxActive
	case maxActive < 0 || maxActive > MaxTreeActive:
		return 0, 0, 0, invalid("identity: max_active must be between 1 and 100000")
	}
	return maxDepth, maxFanout, maxActive, nil
}

// DelegateRequest delegates from an active parent grant.
type DelegateRequest struct {
	Task         string        `json:"task"`
	Context      string        `json:"context"`
	Deployment   string        `json:"deployment"`
	Capabilities []string      `json:"capabilities"`
	TTL          time.Duration `json:"ttl"`
	MaxDepth     int           `json:"max_depth"`
	MaxFanout    int           `json:"max_fanout"`
	MaxActive    int           `json:"max_active"`
	AllowSelf    bool          `json:"allow_self"`
}

// grantColumns is the shared read projection of both grant subtypes.
const grantColumns = `id, root, parent, tenant, deployment, subject, task, context_id,
    status, epoch, depth, actors, capabilities,
    labels_subjects, labels_workspaces, labels_taints,
    max_depth, max_fanout, max_active, allow_self, body_digest, issued_at, expires_at`

// findGrant reads one grant by its scope task key. found is false for an
// absent key and for a key that lives in another scope.
func findGrant(ctx context.Context, tx pgx.Tx, scope contracts.Scope, task string) (Grant, bool, error) {
	const q = `SELECT 'root' AS kind, ` + grantColumns + `
	      FROM ahp_identity.grants_root
	     WHERE tenant = $1 AND deployment = $2 AND subject = $3 AND task = $4
	    UNION ALL
	    SELECT 'child', ` + grantColumns + `
	      FROM ahp_identity.grants_child
	     WHERE tenant = $1 AND deployment = $2 AND subject = $3 AND task = $4`
	return scanGrants(tx.Query(ctx, q, scope.Tenant, scope.Deployment, scope.Subject, task))
}

// findGrantByID reads one grant by primary key from either subtype.
func findGrantByID(ctx context.Context, tx pgx.Tx, id string) (Grant, bool, error) {
	const q = `SELECT 'root' AS kind, ` + grantColumns + `
	      FROM ahp_identity.grants_root WHERE id = $1
	    UNION ALL
	    SELECT 'child', ` + grantColumns + `
	      FROM ahp_identity.grants_child WHERE id = $1`
	return scanGrants(tx.Query(ctx, q, id))
}

// scanGrants materialises the union query. The scope task key is unique
// inside each subtype, so more than one row means the schema was altered.
func scanGrants(rows pgx.Rows, err error) (Grant, bool, error) {
	if err != nil {
		return Grant{}, false, fmt.Errorf("identity: read grant: %w", err)
	}
	defer rows.Close()
	var found []Grant
	for rows.Next() {
		var (
			kind             string
			g                Grant
			subj, ws, taints []string
		)
		if err := rows.Scan(&kind, &g.ID, &g.Root, &g.Parent, &g.Tenant, &g.Deployment,
			&g.Subject, &g.Task, &g.Context, &g.Status, &g.Epoch, &g.Depth,
			&g.Actors, &g.Capabilities, &subj, &ws, &taints,
			&g.MaxDepth, &g.MaxFanout, &g.MaxActive, &g.AllowSelf, &g.bodyDigest,
			new(ignoredTime), &g.ExpiresAt); err != nil {
			return Grant{}, false, fmt.Errorf("identity: scan grant: %w", err)
		}
		g.Labels = labels.Set{Subjects: subj, Workspaces: ws, Taints: taints}
		found = append(found, g)
	}
	if err := rows.Err(); err != nil {
		return Grant{}, false, fmt.Errorf("identity: scan grant: %w", err)
	}
	if len(found) == 0 {
		return Grant{}, false, nil
	}
	if len(found) > 1 {
		return Grant{}, false, contracts.NewError(contracts.ErrConflict,
			"identity: task id is ambiguous across grant subtypes")
	}
	return found[0], true, nil
}

// ignoredTime discards a column that is stored but not exported.
type ignoredTime struct{}

// Scan satisfies pgx.Scanner without retaining the value.
func (ignoredTime) Scan(any) error { return nil }

// lockRoot takes the transaction-scoped advisory lock that serialises child
// creation, fan-out accounting, labelling, token minting and cancellation for
// one delegation tree.
func lockRoot(ctx context.Context, tx pgx.Tx, root string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, hashtext($2))`,
		lockSpace, root); err != nil {
		return fmt.Errorf("identity: lock root: %w", err)
	}
	return nil
}

// lockTaskKey serialises root creation for one idempotency key
// (tenant, deployment, subject, task) for the rest of the transaction, so
// concurrent identical creates read/insert strictly one after another and a
// mismatched concurrent body is reported as KEY_REUSE. A serialization or
// deadlock abort behind that lock is retried by runTxMode in a fresh
// transaction, so concurrent identical creates converge on one grant.
func lockTaskKey(ctx context.Context, tx pgx.Tx, scope contracts.Scope, task string) error {
	key := newHash()
	key.str("taskkey")
	key.str(scope.Tenant)
	key.str(scope.Deployment)
	key.str(scope.Subject)
	key.str(task)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, hashtext($2))`,
		lockSpace, key.hexSum()); err != nil {
		return fmt.Errorf("identity: lock task key: %w", err)
	}
	return nil
}

// subjectRevoked reports whether the scope subject carries a tombstone.
func subjectRevoked(ctx context.Context, tx pgx.Tx, scope contracts.Scope) (bool, error) {
	var found bool
	err := tx.QueryRow(ctx, `
	    SELECT EXISTS (SELECT 1 FROM ahp_identity.subjects
	                    WHERE tenant = $1 AND subject = $2) OR EXISTS(SELECT 1 FROM ahp_identity.erased_tenants WHERE tenant=$1)`,
		scope.Tenant, scope.Subject).Scan(&found)
	if err != nil {
		return false, fmt.Errorf("identity: read subject tombstone: %w", err)
	}
	return found, nil
}

// checkUsable verifies a grant and every one of its ancestors is active and
// unexpired according to the PostgreSQL clock. A revoked or expired level
// invalidates the whole chain below it.
func checkUsable(ctx context.Context, tx pgx.Tx, g Grant) error {
	if g.Status != StatusActive {
		return forbidden("identity: grant is revoked")
	}
	// The tombstone outranks live rows: a subject revoked concurrently with
	// (or after) its grants were written must deny even though every grant
	// in the chain still reads active.
	revoked, err := subjectRevoked(ctx, tx, g.Scope())
	if err != nil {
		return err
	}
	if revoked {
		return forbidden("identity: subject is revoked")
	}
	// UNION, not UNION ALL (D-ID-2): the ancestor walk must be both COMPLETE and
	// terminating. Truncating it - by a hop bound or by a depth predicate that a
	// corrupt row defeats - could hide a revoked ancestor and let a token be
	// minted under authority that no longer exists. Deduplicating the projected
	// rows makes the recursion reach a fixed point instead, which terminates on a
	// cyclic parent chain without ever dropping a row a healthy chain contains.
	rows, err := tx.Query(ctx, `
	    WITH RECURSIVE chain(id, status, expires_at, parent) AS (
	        SELECT c.id, c.status, c.expires_at, c.parent
	          FROM ahp_identity.grants_child c WHERE c.id = $1
	        UNION
	        SELECT c.id, c.status, c.expires_at, c.parent
	          FROM ahp_identity.grants_child c JOIN chain h ON h.parent = c.id
	    )
	    SELECT id, status, clock_timestamp() < expires_at FROM chain
	    UNION ALL
	    SELECT id, status, clock_timestamp() < expires_at
	      FROM ahp_identity.grants_root WHERE id = $2`,
		g.ID, g.Root)
	if err != nil {
		return fmt.Errorf("identity: walk ancestors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id, status string
			alive      bool
		)
		if err := rows.Scan(&id, &status, &alive); err != nil {
			return fmt.Errorf("identity: scan ancestors: %w", err)
		}
		if status != StatusActive {
			return forbidden("identity: an ancestor grant is revoked")
		}
		if !alive {
			return forbidden("identity: an ancestor grant has expired")
		}
	}
	return rows.Err()
}

// validateTask and validateContext bound every identifier that becomes part
// of an idempotency key.
func validateTask(task string) error {
	if task == "" {
		return invalid("identity: task id is required")
	}
	if len(task) > 128 {
		return invalid("identity: task id exceeds 128 bytes")
	}
	for i := 0; i < len(task); i++ {
		if task[i] < 0x20 || task[i] == 0x7f {
			return invalid("identity: task id contains control characters")
		}
	}
	return nil
}

func validateContext(contextID string) error {
	if contextID == "" {
		return invalid("identity: context id is required")
	}
	if len(contextID) > 128 {
		return invalid("identity: context id exceeds 128 bytes")
	}
	for i := 0; i < len(contextID); i++ {
		if contextID[i] < 0x20 || contextID[i] == 0x7f {
			return invalid("identity: context id contains control characters")
		}
	}
	return nil
}

// normalizeCapabilities sorts, de-duplicates and bounds a capability set.
// Wildcards are rejected at the door: AHP capabilities are exact names.
func normalizeCapabilities(in []string) ([]string, error) {
	if len(in) > maxCapabilities {
		return nil, invalid("identity: at most 256 capabilities may be requested")
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, c := range in {
		if c == "" {
			return nil, invalid("identity: capability must not be empty")
		}
		if len(c) > 128 {
			return nil, invalid("identity: capability exceeds 128 bytes")
		}
		if strings.ContainsAny(c, "*?%") {
			return nil, invalid("identity: capability wildcards are not supported")
		}
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	sortStrings(out)
	return out, nil
}

// normalizeTTL bounds a requested lifetime.
func normalizeTTL(ttl time.Duration) (time.Duration, error) {
	if ttl <= 0 {
		return 0, invalid("identity: ttl must be positive")
	}
	if ttl > 24*time.Hour {
		return 0, invalid("identity: ttl must not exceed 24h")
	}
	return ttl, nil
}

// validateLabelSet rejects malformed label sets before they reach SQL.
// AddLabels is a trusted, service-internal entry point, so only shape and the
// known taint vocabulary are enforced here.
func validateLabelSet(s labels.Set) error {
	for _, v := range s.Subjects {
		if v == "" || len(v) > 128 {
			return invalid("identity: label subject is malformed")
		}
	}
	for _, v := range s.Workspaces {
		if v == "" || len(v) > 128 {
			return invalid("identity: label workspace is malformed")
		}
	}
	for _, v := range s.Taints {
		switch v {
		case labels.TaintExternal, labels.TaintUntrustedExternal, labels.TaintSecret:
		default:
			return invalid("identity: unknown taint " + quote(v))
		}
	}
	return nil
}

// isNoRows, isUniqueViolation, isCheckViolation and isForeignKeyViolation
// classify the PostgreSQL errors this package turns into typed platform
// errors. Messages never carry row content.
func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

func isUniqueViolation(err error) bool { return sqlState(err) == "23505" }
func isCheckViolation(err error) bool  { return sqlState(err) == "23514" }

// isEdgeInvariantViolation reports the rejection raised by the grants_child edge
// guard (ahp_identity.grants_child_edge_invariant). It uses exclusion_violation
// (23P01) because no other constraint in this schema raises it, so the class is
// never ambiguous with a check (23514) or unique (23505) violation.
func isEdgeInvariantViolation(err error) bool { return sqlState(err) == "23P01" }
func isForeignKeyViolation(err error) bool {
	code := sqlState(err)
	return code == "23503" || strings.HasPrefix(code, "23503")
}

func sqlState(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func invalid(msg string) error   { return contracts.NewError(contracts.ErrInvalid, msg) }
func forbidden(msg string) error { return contracts.NewError(contracts.ErrForbidden, msg) }
