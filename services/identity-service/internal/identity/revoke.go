// Package identity: subtree cancellation and subject revocation.
//
// Both operations are TRUSTED CONTROLPLANE entry points: they are never
// exposed over any agent-reachable HTTP route. They run in a bounded
// detached transaction, so a cancelled caller context can never leave half
// a subtree live, and the caller can still abort before the transaction
// opens.

package identity

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// revokeActiveSQL flips every row the given subtree CTE names from active to
// revoked: revoked_at takes the PostgreSQL clock and the epoch advances only
// for rows that were still active, so a repeated call changes nothing.
const revokeActiveSQL = `
	    UPDATE ahp_identity.%s g
	       SET status = '` + StatusRevoked + `',
	           epoch = CASE WHEN g.status = '` + StatusActive + `'
	                        THEN g.epoch + 1 ELSE g.epoch END,
	           revoked_at = clock_timestamp()
	     WHERE g.status = '` + StatusActive + `'
	       AND g.id IN (SELECT id FROM subtree)`

// subtreeFromRoot enumerates every child of one root, at any depth.
//
// Two properties cancellation depends on, both from D-ID-2:
//
//   - It is COMPLETE. The anchor is the root column, so a row is named by the
//     root it carries rather than by whatever its parent column claims. A
//     tangled parent chain cannot hide a descendant from a root cancellation.
//   - It TERMINATES on its own. The combinator is UNION, not UNION ALL: the
//     projected set is a set of ids, so the recursion reaches a fixed point
//     even if the parent chain loops. UNION ALL over a cyclic graph never ends,
//     and this service sets no statement_timeout of its own - measured, the
//     caller just waited out its 30s deadline and got an unclassified
//     "context deadline exceeded" instead of an answer.
//
// Cancellation is the operation the platform reaches for when something is
// already wrong (PDF rev3.2 line 2378: отмена хода отзывает поддерево), so it
// must not be the operation a corrupt graph can deadlock.
const subtreeFromRoot = `
	    WITH RECURSIVE subtree(id) AS (
	        SELECT c.id FROM ahp_identity.grants_child c WHERE c.root = $1
	        UNION
	        SELECT c.id FROM ahp_identity.grants_child c
	          JOIN subtree s ON c.parent = s.id
	    )`

// subtreeFromChild enumerates one child and every descendant below it - exactly
// its descendants, because a child cancellation must leave the root and the
// siblings live. The same UNION fixed point makes that walk terminate on a
// cyclic graph too: the walk then names the loop as well, which is the honest
// answer for a graph whose rows are mutually each other's descendants.
const subtreeFromChild = `
	    WITH RECURSIVE subtree(id) AS (
	        SELECT $1::text
	        UNION
	        SELECT c.id FROM ahp_identity.grants_child c
	          JOIN subtree s ON c.parent = s.id
	    )`

// CancelSubtree revokes one task and its whole descendant subtree atomically.
//
// The scope task key must address a grant; a foreign or absent key is
// NOT_FOUND. Under the root advisory lock the target and every recursive
// descendant flip to revoked with revoked_at from the PostgreSQL clock and an
// epoch bump only where the row was still active, so repeating the call is a
// no-op. Cancelling a child leaves the root and its siblings live; cancelling
// the root takes the entire tree. A descendant call denies immediately:
// cancellation is the control plane's decision and the agent need not
// cooperate.
func (s *Store) CancelSubtree(ctx context.Context, scope contracts.Scope, task string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := validateTask(task); err != nil {
		return err
	}
	// The caller may still abort before the detached transaction opens.
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.runTxDetached(func(ctx context.Context, tx pgx.Tx) error {
		g, found, err := findGrant(ctx, tx, scope, task)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrNotFound, "identity: grant not found")
		}
		if err := lockRoot(ctx, tx, g.Root); err != nil {
			return err
		}
		// Re-read under the root lock: a concurrent delegate could otherwise
		// slip a fresh child into the subtree between read and lock.
		g, found, err = findGrant(ctx, tx, scope, task)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrNotFound, "identity: grant not found")
		}
		return revokeSubtreeTx(ctx, tx, g)
	})
}

// RevokeSubject is the trusted control-plane kill switch for one subject.
//
// It inserts the (tenant, subject) tombstone idempotently and revokes every
// grant of that subject across all deployments of the tenant in one
// transaction. The tombstone additionally makes checkUsable deny any
// concurrent root of that subject whose rows were written before this call.
// Other subjects of the same tenant are untouched.
func (s *Store) RevokeSubject(ctx context.Context, tenant, subject string) error {
	if tenant == "" || subject == "" {
		return invalid("identity: tenant and subject are required")
	}
	if err := (contracts.Scope{Tenant: tenant, Deployment: "x", Subject: subject}).Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.runTxDetached(func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "identity-subject-erasure/"+tenant+"/"+subject); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
		    INSERT INTO ahp_identity.subjects (tenant, subject)
		    VALUES ($1, $2) ON CONFLICT (tenant, subject) DO NOTHING`,
			tenant, subject); err != nil {
			return fmt.Errorf("identity: revoke subject tombstone: %w", err)
		}
		for _, table := range []string{"grants_child", "grants_root"} {
			if _, err := tx.Exec(ctx, `
			    UPDATE ahp_identity.`+table+`
			       SET status = '`+StatusRevoked+`',
			           epoch = CASE WHEN status = '`+StatusActive+`'
			                        THEN epoch + 1 ELSE epoch END,
			           revoked_at = clock_timestamp()
			     WHERE tenant = $1 AND subject = $2
			       AND status = '`+StatusActive+`'`,
				tenant, subject); err != nil {
				return fmt.Errorf("identity: revoke subject grants: %w", err)
			}
		}
		// The subject's browser sessions die in the same transaction: a
		// tombstone that left a live cookie and a live access token behind
		// would let a revoked subject keep acting until the token expired on
		// its own, which is exactly the cooperation this rule refuses to
		// require.
		if err := revokeBrowserSubjectsTx(ctx, tx, tenant, subject); err != nil {
			return err
		}
		return nil
	})
}

// denyIfSubjectRevoked rejects label additions and delegation for a subject
// the control plane has revoked, so a revoked subject can never widen its
// footprint even while its already-written rows still read active.
func denyIfSubjectRevoked(ctx context.Context, tx pgx.Tx, scope contracts.Scope) error {
	revoked, err := subjectRevoked(ctx, tx, scope)
	if err != nil {
		return err
	}
	if revoked {
		return forbidden("identity: subject is revoked")
	}
	return nil
}

// revokeSubtreeTx flips the target and every descendant of its tree to
// revoked under the already-held root lock. Roots live in grants_root and
// descendants in grants_child, so a root target needs both statements; a
// child target only ever touches the child table.
func revokeSubtreeTx(ctx context.Context, tx pgx.Tx, target Grant) error {
	if tableOf(target) == "grants_root" {
		if _, err := tx.Exec(ctx,
			`WITH subtree(id) AS (SELECT $1::text)`+fmt.Sprintf(revokeActiveSQL, "grants_root"),
			target.ID); err != nil {
			return fmt.Errorf("identity: cancel subtree root: %w", err)
		}
		if _, err := tx.Exec(ctx, subtreeFromRoot+
			fmt.Sprintf(revokeActiveSQL, "grants_child"), target.ID); err != nil {
			return fmt.Errorf("identity: cancel subtree descendants: %w", err)
		}
		return nil
	}
	if _, err := tx.Exec(ctx, subtreeFromChild+
		fmt.Sprintf(revokeActiveSQL, "grants_child"), target.ID); err != nil {
		return fmt.Errorf("identity: cancel subtree: %w", err)
	}
	// No acyclicity refusal here on purpose, and the reason is the transaction:
	// returning an error from this function rolls the revocation back, so
	// "refuse to cancel a corrupt tree" actually means "leave the corrupt tree
	// running". Revocation is the fail-safe direction and must always land; the
	// corruption is reported where a repair can act on it - by Delegate, which
	// refuses to issue new authority over a tree it cannot measure.
	return nil
}

// tableOf maps a grant onto its subtype table. The schema pins parent to ”
// for roots and to a non-empty id for children; depth is not part of the
// read projection.
func tableOf(g Grant) string {
	if g.Parent == "" {
		return "grants_root"
	}
	return "grants_child"
}

// CancelDescendant allows an authenticated live parent to stop only a proper
// descendant. Re-checking lineage under the root lock excludes concurrent
// delegation/revocation races and makes already-revoked children retryable.
func (s *Store) CancelDescendant(ctx context.Context, scope contracts.Scope, parentTask, childTask string) error {
	return s.CancelDescendantInDeployment(ctx, scope, parentTask, childTask, "")
}

// CancelDescendantInDeployment disambiguates a task name in a cross-deployment
// tree. An empty target preserves the unique-descendant lookup used by Space.
func (s *Store) CancelDescendantInDeployment(ctx context.Context, scope contracts.Scope, parentTask, childTask, deployment string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := validateTask(parentTask); err != nil {
		return err
	}
	if err := validateTask(childTask); err != nil {
		return err
	}
	if parentTask == childTask && (deployment == "" || deployment == scope.Deployment) {
		return forbidden("identity: parent cancellation requires a proper descendant")
	}
	if deployment != "" {
		targetScope := scope
		targetScope.Deployment = deployment
		if err := targetScope.Validate(); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.runTxDetached(func(ctx context.Context, tx pgx.Tx) error {
		parent, ok, err := findGrant(ctx, tx, scope, parentTask)
		if err != nil {
			return err
		}
		if !ok {
			return forbidden("identity: parent grant not found")
		}
		if err := lockRoot(ctx, tx, parent.Root); err != nil {
			return err
		}
		parent, ok, err = findGrant(ctx, tx, scope, parentTask)
		if err != nil {
			return err
		}
		if !ok {
			return forbidden("identity: parent grant not found")
		}
		if err := checkUsable(ctx, tx, parent); err != nil {
			return err
		}
		// The descendant may reside in another deployment, but never another
		// tenant/subject or root. Ambiguous task names fail closed.
		rows, err := tx.Query(ctx, `SELECT deployment FROM ahp_identity.grants_child WHERE tenant=$1 AND subject=$2 AND root=$3 AND task=$4 AND ($5='' OR deployment=$5)`, scope.Tenant, scope.Subject, parent.Root, childTask, deployment)
		if err != nil {
			return err
		}
		var deployments []string
		for rows.Next() {
			var d string
			if err := rows.Scan(&d); err != nil {
				rows.Close()
				return err
			}
			deployments = append(deployments, d)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(deployments) != 1 {
			return forbidden("identity: target is not one unambiguous descendant")
		}
		childScope := scope
		childScope.Deployment = deployments[0]
		child, ok, err := findGrant(ctx, tx, childScope, childTask)
		if err != nil {
			return err
		}
		if !ok {
			return forbidden("identity: child grant not found")
		}
		var descendant bool
		err = tx.QueryRow(ctx, `WITH RECURSIVE ancestors(ord,id) AS (
    SELECT 0,$1::text UNION ALL SELECT a.ord+1,coalesce(nullif(c.parent,''),c.root)
    FROM ancestors a JOIN ahp_identity.grants_child c ON c.id=a.id WHERE a.ord<32)
   SELECT EXISTS(SELECT 1 FROM ancestors WHERE id=$2 AND ord>0)`, child.ID, parent.ID).Scan(&descendant)
		if err != nil {
			return err
		}
		if !descendant {
			return forbidden("identity: target is not a descendant of this parent")
		}
		return revokeSubtreeTx(ctx, tx, child)
	})
}
