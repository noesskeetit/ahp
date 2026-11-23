// Package identity: delegation of an existing grant to a child task.
//
// Delegate stays inside the parent deployment. DelegateToTarget additionally
// accepts target policy resolved by a trusted gateway/service, intersected
// with tenant installation permissions. The store enforces every narrowing
// rule itself, because a grant is the authority: a child may
// never hold more than its parent, live longer than its parent, delegate
// deeper than its parent allows, repeat an actor in the chain (unless the
// target manifest allows a self call), or push the subtree past any ancestor
// quota. The self-call allowance inherits as a ceiling, never as a grant: a
// child's allow_self is only ever read through its own parent, so repeating an
// actor at depth N requires the allowance to have been granted at depth N-1.
// A subtree can therefore never grow the right to repeat an actor that its own
// ancestors did not have (PDF rev3.2, стр.30/64: «актор-цепочка: повтор деплоя — отказ»).
// All counting runs against the PostgreSQL clock under the root
// advisory lock, the same lock cancellation holds, so a child can never be
// born inside a cancelled subtree.

package identity

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// Delegate delegates from the active parent grant identified by parentTask
// inside parentScope to a new child task.
//
// The child always lives in the parent scope: a request carries no subject,
// and its deployment must equal the request deployment, so delegation can
// never cross a tenant, deployment or subject boundary. Caps must be a
// non-empty exact subset of the parent caps, the effective expiry is
// min(clock + ttl, parent.ExpiresAt), and the child ceilings default to (and
// may never exceed) the parent ceilings. The child task id is the idempotency
// key inside the scope: an identical body under the same parent replays the
// stored grant, any other reuse of the key reports KEY_REUSE_MISMATCH.
func (s *Store) Delegate(ctx context.Context, parentScope contracts.Scope, parentTask string, r DelegateRequest) (Grant, error) {
	if r.Deployment != "" && r.Deployment != parentScope.Deployment {
		return Grant{}, forbidden("identity: delegate request deployment does not match the parent scope")
	}
	return s.delegate(ctx, parentScope, parentTask, r, nil)
}

func (s *Store) delegate(ctx context.Context, parentScope contracts.Scope, parentTask string, r DelegateRequest, target *DelegationTarget) (Grant, error) {
	if err := parentScope.Validate(); err != nil {
		return Grant{}, err
	}
	if err := validateTask(parentTask); err != nil {
		return Grant{}, err
	}
	if r.Deployment == "" {
		return Grant{}, invalid("identity: delegate request deployment is required")
	}
	childScope := parentScope
	childScope.Deployment = r.Deployment
	if err := childScope.Validate(); err != nil {
		return Grant{}, err
	}
	if err := validateTask(r.Task); err != nil {
		return Grant{}, err
	}
	if err := validateContext(r.Context); err != nil {
		return Grant{}, err
	}
	caps, err := normalizeCapabilities(r.Capabilities)
	if err != nil {
		return Grant{}, err
	}
	if err := s.requireContainers(ctx, childScope, caps, "", 0); err != nil {
		return Grant{}, err
	}
	ttl, err := normalizeTTL(r.TTL)
	if err != nil {
		return Grant{}, err
	}
	if r.MaxDepth < 0 || r.MaxDepth > MaxTreeDepth {
		return Grant{}, invalid("identity: delegate max_depth must be between 1 and 16")
	}
	if r.MaxFanout < 0 || r.MaxFanout > MaxTreeFanout {
		return Grant{}, invalid("identity: max_fanout must be between 1 and 1024")
	}
	if r.MaxActive < 0 || r.MaxActive > MaxTreeActive {
		return Grant{}, invalid("identity: max_active must be between 1 and 100000")
	}
	// The replay fingerprint binds the original normalized request, never
	// the clock-clamped expiry, so a replay at any later instant returns the
	// original grant instead of a key reuse.
	digest, err := delegateDigest(parentScope, parentTask, r, caps, ttl)
	if err != nil {
		return Grant{}, err
	}

	if target != nil {
		d := newHash()
		d.str("delegate-to-target")
		d.str(digest)
		d.list(target.Capabilities)
		d.int(target.MaxDepth)
		d.int(target.MaxFanout)
		d.int(target.MaxActive)
		d.flag(target.AllowSelf)
		digest = d.hexSum()
	}

	var out Grant
	err = s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))", "identity-subject-erasure/"+parentScope.Tenant+"/"+parentScope.Subject); err != nil {
			return err
		}
		// Child task key first, then the root lock: a uniform lock order
		// across every delegate keeps concurrent delegations deadlock-free.
		if err := lockTaskKey(ctx, tx, childScope, r.Task); err != nil {
			return err
		}
		if err := requireUnfencedExecution(ctx, tx, childScope, r.Task); err != nil {
			return err
		}
		parent, found, err := findGrant(ctx, tx, parentScope, parentTask)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrNotFound, "identity: parent grant not found")
		}
		if err := lockRoot(ctx, tx, parent.Root); err != nil {
			return err
		}
		// Re-read under the root lock: cancellation and labelling take the
		// same lock, so this row is the live one.
		parent, found, err = findGrant(ctx, tx, parentScope, parentTask)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrNotFound, "identity: parent grant not found")
		}

		// --- live authority, before any replay short-circuit ---
		if err := checkUsable(ctx, tx, parent); err != nil {
			return err
		}
		var alive bool
		if err := tx.QueryRow(ctx,
			`SELECT clock_timestamp() < $1::timestamptz`, parent.ExpiresAt).Scan(&alive); err != nil {
			return fmt.Errorf("identity: check parent expiry: %w", err)
		}
		if !alive {
			return forbidden("identity: parent grant has expired")
		}
		revoked, err := subjectRevoked(ctx, tx, parentScope)
		if err != nil {
			return err
		}
		if revoked {
			return forbidden("identity: subject is revoked and may not delegate")
		}

		// --- idempotency: replay is checked before any quota, so a replay
		// of an existing child never consumes fan-out or active budget ---
		existing, found, err := findGrant(ctx, tx, childScope, r.Task)
		if err != nil {
			return err
		}
		if found {
			if existing.Parent != parent.ID || existing.bodyDigest != digest {
				return contracts.NewError(contracts.ErrKeyReuse,
					"identity: child task id was already used with a different canonical request or parent")
			}
			out = existing
			return nil
		}

		// --- narrowing rules on the live parent ---
		if len(caps) == 0 || !capsSubset(parent.Capabilities, caps) {
			return forbidden("identity: delegation may only narrow the parent capability set")
		}
		if parent.Depth+1 > parent.MaxDepth {
			return forbidden("identity: delegation exceeds the parent depth ceiling")
		}
		maxDepth, maxFanout, maxActive, err := childLimits(r, parent)
		if err != nil {
			return err
		}
		if target != nil {
			if r.MaxDepth > target.MaxDepth || r.MaxFanout > target.MaxFanout || r.MaxActive > target.MaxActive {
				return forbidden("identity: delegation exceeds target manifest ceilings")
			}
			maxDepth = min(maxDepth, target.MaxDepth)
			maxFanout = min(maxFanout, target.MaxFanout)
			maxActive = min(maxActive, target.MaxActive)
			if parent.Depth+1 > maxDepth {
				return forbidden("identity: delegation exceeds target depth ceiling")
			}
		}
		actors := append(append([]string{}, parent.Actors...), r.Deployment)
		if contains(parent.Actors, r.Deployment) {
			if r.Deployment != parent.Deployment || !parent.AllowSelf || target != nil && !target.AllowSelf {
				return forbidden("identity: delegation would repeat an actor in the chain")
			}
		}

		// --- context journal: own id, foreign claim is invisible ---
		if _, found, err := readContext(ctx, tx, childScope, r.Context); err != nil {
			return err
		} else if !found {
			if _, err := ensureContext(ctx, tx, childScope, r.Context, labels.Set{}); err != nil {
				// Only a decided foreign claim may say so. A duplicate-key violation
				// used to be folded into this answer as well, which is how D-ID-3
				// misreported a same-scope claim race as a foreign tenant: the arbiter
				// named one constraint while another one fired, and the error class
				// was chosen by "is it 23505" instead of by who owns the journal.
				if isContextForeignClaim(err) {
					return contracts.NewError(contracts.ErrNotFound,
						"identity: context id belongs to another scope")
				}
				return err
			}
		}

		// --- quotas under the root lock, counted on the SQL clock ---
		var activeNow bool
		if err := tx.QueryRow(ctx, `
		    SELECT status = 'active' AND clock_timestamp() < expires_at
		      FROM ahp_identity.grants_root WHERE id = $1`, parent.Root).Scan(&activeNow); err != nil {
			return fmt.Errorf("identity: read root state: %w", err)
		}
		if !activeNow {
			return forbidden("identity: root grant is not active")
		}
		// A tree that is not a tree has no size, and this transaction is about
		// to grant authority based on that size. Deny and tell the operator to
		// repair, rather than hand out a child on a graph whose ceilings cannot
		// be evaluated.
		if err := assertGrantGraphAcyclic(ctx, tx, parent.Root); err != nil {
			return err
		}

		// The live subtree is counted while the walk visits it, not by a second
		// pass over grants_child afterwards, and it is counted as DISTINCT NODES.
		//
		// Distinct nodes is what the ceiling is a ceiling over: PDF rev3.2
		// section 9.4 puts the limit on "active tasks" counted on atomic
		// counters, and cmd/ahp/runtime_authority.go binds that field
		// (limits.active_tasks) straight onto MaxActive. A task is one node of
		// the tree. The anchor "root = $1" already yields every descendant, and
		// the recursive term reaches a node again through every step of its
		// parent chain, so an undistinct count charges a node at depth d exactly
		// d times: measured on a 2+4+8 tree, 34 for 14 live tasks
		// (internal/identity/delegation_quota_cardinality_test.go, D-ID-1). The
		// quota then UNDER-issues - a legitimate personal-Dot delegation is
		// refused with "the root active-subtree limit is exhausted" while the
		// tree is at half its ceiling. Keeping the anchor rather than narrowing
		// it to "parent = root" is deliberate: a row that carries this root but a
		// broken parent chain still occupies the tree and must still be charged,
		// never silently dropped from a security ceiling.
		//
		// The walk itself stays the shape R4 established, with one addition for
		// D-ID-2: the recursive term only follows an edge that deepens the tree
		// (c.depth = h.depth + 1). With depth pinned to 1..16 by
		// grants_child_depth_ck, that makes the walk terminate by itself - no hop
		// bound to outguess the data, and no dependence on an instance
		// statement_timeout that this service does not set and cannot assume.
		// On a graph where some parent chain loops, the edges that complete the
		// loop are simply not followed, so the count stays the count of the
		// rows that carry this root. Counting the finished id list by
		// joining back to grants_child gave PostgreSQL a hash join with a Seq Scan
		// of the whole table (measured plan:
		// R4_QUOTA_PLAN ... -> Seq Scan on grants_child c  Filter: (status = 'active' AND clock_timestamp() < expires_at)),
		// and under SERIALIZABLE a scan is a predicate over everything it scanned.
		// The predicate then covered EVERY tree in the schema, while the advisory
		// root lock only ever excludes one tree - so two delegations in unrelated
		// trees aborted each other (measured: 120 40001s over 103 delegations, 3
		// of them after the caller-visible retry budget was exhausted). Carrying
		// the liveness flag inside the recursive term keeps the walk on
		// grants_child_root_idx and grants_child_parent_idx, so the predicate
		// spans one tree, which is exactly what the root lock already protects.
		// clock_timestamp() is evaluated per visited row either way, so the quota
		// answers the same question it always asked: live on the SQL clock, at
		// this instant.
		var rootActive, subtreeActive, siblings int
		if err := tx.QueryRow(ctx, `
		    WITH RECURSIVE subtree(id, depth, live) AS (
		        SELECT c.id, c.depth, (c.status = 'active' AND clock_timestamp() < c.expires_at)
		          FROM ahp_identity.grants_child c
		         WHERE c.root = $1
		        UNION ALL
		        SELECT c.id, c.depth, (c.status = 'active' AND clock_timestamp() < c.expires_at)
		          FROM ahp_identity.grants_child c
		          JOIN subtree h ON c.parent = h.id AND c.depth = h.depth + 1
		    )
		    SELECT
		        (SELECT count(*) FROM ahp_identity.grants_root g
		          WHERE g.id = $1 AND g.status = 'active' AND clock_timestamp() < g.expires_at),
		        (SELECT count(DISTINCT id) FILTER (WHERE live) FROM subtree),
		        (SELECT count(*) FROM ahp_identity.grants_child c
		         WHERE c.parent = $2 AND c.status = 'active'
		           AND clock_timestamp() < c.expires_at)`,
			parent.Root, parent.ID).Scan(&rootActive, &subtreeActive, &siblings); err != nil {
			return fmt.Errorf("identity: count delegation quotas: %w", err)
		}
		if rootActive >= parent.MaxActive {
			return forbidden("identity: the root active-subtree limit is exhausted")
		}
		if err := enforceAncestorActive(ctx, tx, parent.ID, parent.MaxActive); err != nil {
			return err
		}
		if siblings >= parent.MaxFanout {
			return forbidden("identity: the parent fan-out limit is exhausted")
		}
		if subtreeActive+1 > parent.MaxActive {
			return forbidden("identity: the root active-subtree limit is exhausted")
		}

		// --- child journal: parent labels + child context labels + grant labels ---
		parentJournal, _, err := readContext(ctx, tx, parentScope, parent.Context)
		if err != nil {
			return err
		}
		childJournal, err := ensureContext(ctx, tx, childScope, r.Context, parentJournal)
		if err != nil {
			if isContextForeignClaim(err) {
				return contracts.NewError(contracts.ErrNotFound,
					"identity: context id belongs to another scope")
			}
			return err
		}
		stored := nonnilSet(labels.Merge(parent.Labels, childJournal))

		id := contracts.NewID()
		// The child is written and read back in one statement. Reading it again
		// by (scope, task) is a second predicate on the busiest key range of the
		// scope - the same range every concurrent sibling delegates into - and a
		// row this transaction has just written cannot be anything but the row
		// RETURNING describes. Under SERIALIZABLE that redundant read was a
		// measured share of the abort surface (R4 measurement,
		// internal/identity/delegate_ssi_realpath_test.go).
		const q = `INSERT INTO ahp_identity.grants_child
    (id, root, parent, tenant, deployment, subject, task, context_id,
     status, epoch, depth, actors, capabilities,
     labels_subjects, labels_workspaces, labels_taints,
     max_depth, max_fanout, max_active, allow_self, body_digest,
     issued_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
        'active', 1, $9, $10, $11, $12, $13, $14,
        $15, $16, $17, $18, $19, clock_timestamp(),
        LEAST(clock_timestamp() + make_interval(secs => $20), $21::timestamptz))
RETURNING 'child' AS kind, ` + grantColumns
		created, found, err := scanGrants(tx.Query(ctx, q,
			id, parent.Root, parent.ID, childScope.Tenant, childScope.Deployment,
			parentScope.Subject, r.Task, r.Context,
			parent.Depth+1, actors, caps,
			stored.Subjects, stored.Workspaces, stored.Taints,
			maxDepth, maxFanout, maxActive, r.AllowSelf, digest,
			ttlSeconds(ttl), parent.ExpiresAt,
		))
		if err != nil {
			if isUniqueViolation(err) {
				return contracts.NewError(contracts.ErrConflict,
					"identity: child task was created concurrently; read it and retry")
			}
			if isEdgeInvariantViolation(err) {
				return forbidden("identity: delegation would break the acyclicity of the grant graph")
			}
			if isCheckViolation(err) || isForeignKeyViolation(err) {
				return forbidden("identity: delegation violates a stored grant constraint")
			}
			return fmt.Errorf("identity: create child grant: %w", err)
		}
		if !found {
			return contracts.NewError(contracts.ErrConflict, "identity: child grant vanished during creation")
		}
		out = created
		return nil
	})
	if err != nil {
		return Grant{}, err
	}
	return out, nil
}

// childLimits resolves the child ceilings: zero inherits the parent, a stated
// value must be positive and may never exceed the parent ceiling. The hard
// ceilings are re-checked so a stored parent can never be widened.
func childLimits(r DelegateRequest, parent Grant) (maxDepth, maxFanout, maxActive int, err error) {
	if r.MaxDepth == 0 {
		maxDepth = parent.MaxDepth
	} else {
		maxDepth = r.MaxDepth
	}
	if r.MaxFanout == 0 {
		maxFanout = parent.MaxFanout
	} else {
		maxFanout = r.MaxFanout
	}
	if r.MaxActive == 0 {
		maxActive = parent.MaxActive
	} else {
		maxActive = r.MaxActive
	}
	switch {
	case maxDepth < 1 || maxDepth > parent.MaxDepth || maxDepth > MaxTreeDepth:
		return 0, 0, 0, forbidden("identity: child max_depth may only narrow the parent ceiling")
	case maxFanout < 1 || maxFanout > parent.MaxFanout || maxFanout > MaxTreeFanout:
		return 0, 0, 0, forbidden("identity: child max_fanout may only narrow the parent ceiling")
	case maxActive < 1 || maxActive > parent.MaxActive || maxActive > MaxTreeActive:
		return 0, 0, 0, forbidden("identity: child max_active may only narrow the parent ceiling")
	}
	return maxDepth, maxFanout, maxActive, nil
}

// assertGrantGraphAcyclic reports whether the stored tree under one root still
// satisfies the edge invariant - every child exactly one level below its parent,
// under the same root.
//
// A violation is not a bad request, it is damaged stored state: something wrote
// grants_child without going through Delegate (an import, a repair script, a
// database restored from before the edge guard). It is therefore UNAVAILABLE,
// the code this package reserves for "a dependency is not provisioned or needs
// repair", not FORBIDDEN, which would tell the caller that its own delegation
// was refused on policy. The predicate is bounded: rows carrying this root
// through grants_child_root_idx, joined to their parent by primary key.
func assertGrantGraphAcyclic(ctx context.Context, tx pgx.Tx, rootID string) error {
	var broken bool
	if err := tx.QueryRow(ctx, `
	    SELECT EXISTS (
	        SELECT 1
	          FROM ahp_identity.grants_child c
	          JOIN ahp_identity.grants_child p ON p.id = c.parent
	         WHERE c.root = $1
	           AND (c.depth <> p.depth + 1 OR c.root <> p.root))`, rootID).Scan(&broken); err != nil {
		return fmt.Errorf("identity: check grant graph: %w", err)
	}
	if broken {
		return contracts.NewError(contracts.ErrUnavailable,
			"identity: the delegation graph is corrupt: a grant under this root does not sit exactly one level below its parent, which means the parent chain contains a cycle; the stored grants must be repaired")
	}
	return nil
}

// enforceAncestorActive rejects the child when the prospective subtree of
// any ancestor grant of parentID (the parent itself included) would exceed
// that ancestor's own MaxActive ceiling. The child is not yet stored, so the
// prospective count is live count + 1.
func enforceAncestorActive(ctx context.Context, tx pgx.Tx, parentID string, newMaxActive int) error {
	rows, err := tx.Query(ctx, `
	    WITH RECURSIVE anc(ord, id) AS (
	        SELECT 0, $1::text
	        UNION ALL
	        SELECT a.ord + 1, coalesce(nullif(c.parent, ''), c.root)
	          FROM anc a
	          JOIN ahp_identity.grants_child c ON c.id = a.id
	         WHERE a.ord < 32
	    ),
	    subs(ord, id, depth) AS (
	        SELECT a.ord, a.id, coalesce(c.depth, r.depth)
	          FROM anc a
	          LEFT JOIN ahp_identity.grants_child c ON c.id = a.id
	          LEFT JOIN ahp_identity.grants_root r ON r.id = a.id
	        UNION ALL
	        SELECT s.ord + 1, c.id, c.depth
	          FROM subs s JOIN ahp_identity.grants_child c
	            ON c.parent = s.id AND c.depth = s.depth + 1
	    ),
	    live(ord, id) AS (
	        SELECT s.ord, s.id FROM subs s
	          JOIN ahp_identity.grants_child c ON c.id = s.id
	         WHERE c.status = 'active' AND clock_timestamp() < c.expires_at
	        UNION ALL
	        SELECT s.ord, s.id FROM subs s
	          JOIN ahp_identity.grants_root g ON g.id = s.id
	         WHERE g.status = 'active' AND clock_timestamp() < g.expires_at
	    ),
	    counts(ord, n) AS (
	        SELECT ord, count(*) FROM live GROUP BY ord
	    )
	    SELECT a.id, coalesce(cn.n, 0) + 1,
	           coalesce(c.max_active, g.max_active)
	      FROM anc a
	      LEFT JOIN counts cn ON cn.ord = a.ord
	      LEFT JOIN ahp_identity.grants_child c ON c.id = a.id
	      LEFT JOIN ahp_identity.grants_root g ON g.id = a.id`,
		parentID)
	if err != nil {
		return fmt.Errorf("identity: count ancestor subtrees: %w", err)
	}
	type limit struct {
		id     string
		n, max int
	}
	var over []limit
	for rows.Next() {
		var l limit
		if err := rows.Scan(&l.id, &l.n, &l.max); err != nil {
			rows.Close()
			return fmt.Errorf("identity: scan ancestor subtree counts: %w", err)
		}
		if l.n > l.max {
			over = append(over, l)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("identity: count ancestor subtrees: %w", err)
	}
	if len(over) > 0 {
		return forbidden("identity: an ancestor active-subtree limit is exhausted")
	}
	return nil
}

// capsSubset reports whether every wanted capability is held by the parent,
// exactly: wildcards never match and order is irrelevant (both sides are
// canonical).
func capsSubset(held, wanted []string) bool {
	set := make(map[string]struct{}, len(held))
	for _, c := range held {
		set[c] = struct{}{}
	}
	for _, c := range wanted {
		if _, ok := set[c]; !ok {
			return false
		}
	}
	return true
}

// contains reports whether v is in list.
func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
