// Package identity: transaction plumbing and the context label journal.
//
// Labels only ever accumulate. The journal of a context is the single source of
// truth a grant's Labels is read from, which is what makes a label added after
// the fact visible on the next read and impossible to remove.
//
// The journal is an append-only event log (ahp_identity.context_label_events)
// behind an immutable claim row (ahp_identity.contexts). It used to be one row
// updated in place, which made every widening of a shared context a
// read-modify-write on one row: two transactions sharing a context serialized
// and the database aborted one of them with 40001 on every busy moment. An
// append has no such partner, and the stored value is the union of every event,
// which is order-independent and idempotent.

package identity

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/jackc/pgx/v5"
)

// txFn is one unit of identity work. It runs inside a serialized transaction
// and must not retain the transaction. A serialization or deadlock abort
// re-runs it from scratch in a fresh transaction, so it must be safe to
// repeat and must reassign (never accumulate into) its captured outputs.
type txFn func(ctx context.Context, tx pgx.Tx) error

// runTx runs fn inside a SERIALIZABLE transaction.
//
// Cancellation and subject revocation use it through a detached context (see
// runTxDetached): a subtree must not survive because the caller's request
// context happened to be cancelled half-way through the revocation, and the
// write is a single transaction that either commits fully or not at all.
func (s *Store) runTx(ctx context.Context, fn txFn) error {
	return s.runTxMode(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable}, fn)
}

// runTxDetached runs fn exactly like runTx but ignores cancellation of the
// caller context once the transaction has started. The caller can still abort
// before the transaction opens; after that point the operation is
// non-cooperative by design.
func (s *Store) runTxDetached(fn txFn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.runTxMode(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable}, fn)
}

// runTxMode runs fn inside one transaction, retrying only the PostgreSQL
// states that mean "the database deliberately aborted this transaction":
// 40001 serialization_failure and 40P01 deadlock_detected. Every attempt
// begins a brand-new transaction; nothing else is ever retried, so an
// unknown connection or commit outcome is surfaced to the caller instead of
// silently replaying a write whose fate is unclear. fn may reassign its own
// outputs: a retry simply runs it again from scratch.
func (s *Store) runTxMode(ctx context.Context, opts pgx.TxOptions, fn txFn) error {
	if s.pool == nil {
		return contracts.NewError(contracts.ErrInvalid, "identity: store has no pool")
	}
	// The budget is TIME, and the class test decides whether an error is replayed
	// at all. Deadline precedence matters twice over: a caller that has run out of
	// time gets its own context error back (an honest cancellation, not a
	// CONFLICT that tells the caller to replay a request nobody is waiting for),
	// and a caller with no deadline at all still gets a bounded wait rather than
	// an unbounded one.
	budget := txReplayBudgetFromContext(ctx)
	deadline := time.Now().Add(budget)
	delay := minTxReplayBackoff
	for attempt := 1; ; attempt++ {
		err := s.runTxOnce(ctx, opts, fn)
		if err == nil {
			return nil
		}
		if !retryableAbort(err) {
			return err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		remaining := time.Until(deadline)
		if remaining <= 0 || attempt >= maxTxAttempts {
			return exhaustedBudget(attempt, budget, err)
		}
		// Randomized backoff, capped below the budget so a single wait can never
		// consume it. When several transactions sharing a scope abort together, an
		// identical backoff makes them wake and collide again in lockstep; full
		// jitter (anywhere in [minimum, 2*delay]) spreads them so contending
		// writers interleave instead of re-colliding.
		wait := jitteredBackoff(delay, minTxReplayBackoff)
		if wait > remaining {
			wait = remaining
		}
		if err := sleepBackoff(ctx, wait); err != nil {
			return err
		}
		if delay *= 2; delay > maxTxReplayBackoff {
			delay = maxTxReplayBackoff
		}
	}
}

// txReplayBudgetFromContext resolves the replay budget for one request. A caller
// with a deadline gets whichever is smaller - its own remaining time or the
// service budget - so this loop can never outlive the request that owns it.
func txReplayBudgetFromContext(ctx context.Context) time.Duration {
	budget := txReplayBudget
	if dl, ok := ctx.Deadline(); ok {
		if left := time.Until(dl); left < budget {
			return left
		}
	}
	return budget
}

// errRetryBudgetExhausted marks a CONFLICT that means "we stopped replaying for
// you", as opposed to the policy meaning of CONFLICT ("your write collided with
// committed state"). Without a sentinel in the chain, a caller that wants to
// replay at its own level cannot tell the two apart from the code alone.
var errRetryBudgetExhausted = errors.New("identity: replay budget exhausted")

// exhaustedBudget converts a retriable abort the replay budget could not clear
// into the contract code that means "replay it unchanged" (D-ID-3), carrying how
// much replay it actually bought: attempts and the time budget, never row
// content.
//
// Handing the driver's raw 40001 to a caller is not a contract at all: the value
// carries no code for callers to branch on, it reads like a database outage, and
// in a batch of concurrent delegations it looked like a refusal of the
// delegation itself. CONFLICT is the code this package's own vocabulary defines
// for "your write collided with committed state; replay unchanged" - the database
// detail stays in the message, which carries identifiers and a SQLSTATE. The
// budget decision itself lives in runTxMode; this only decides what the last
// attempt reports.
func exhaustedBudget(attempts int, budget time.Duration, err error) error {
	detail := "a concurrent abort"
	if code := sqlState(err); code != "" {
		detail = "SQLSTATE " + code
	} else if isContextClaimRace(err) {
		detail = "a context journal claimed concurrently"
	}
	return &retryBudgetError{contracts.NewError(contracts.ErrConflict, fmt.Sprintf(
		"identity: the database aborted this write %d times as concurrent within a replay budget of %s (%s); replay the request unchanged",
		attempts, budget, detail))}
}

// retryBudgetError is the CONFLICT of an exhausted replay budget. It reports
// exactly like a contracts.Error on the wire and carries errRetryBudgetExhausted
// for errors.Is, so the two meanings of CONFLICT stay tellable apart without
// changing what a client that only reads `code` sees.
type retryBudgetError struct{ err error }

func (e *retryBudgetError) Error() string { return e.err.Error() }
func (e *retryBudgetError) Unwrap() error { return e.err }
func (e *retryBudgetError) Is(target error) bool {
	return target == errRetryBudgetExhausted
}

// runTxOnce is a single begin/run/commit cycle.
func (s *Store) runTxOnce(ctx context.Context, opts pgx.TxOptions, fn txFn) error {
	tx, err := s.pool.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("identity: begin transaction: %w", err)
	}
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		if sqlState(err) == "42501" {
			return forbidden("identity: tenant write is forbidden")
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("identity: commit: %w", err)
	}
	return nil
}

// jitteredBackoff spreads retries of concurrently aborted transactions over
// time. Full jitter, floored at minimum: the wait is uniform in
// [minimum, 2*d], so losers stop retrying in lockstep while the wait stays
// bounded and never degenerates into a busy spin.
func jitteredBackoff(d, minimum time.Duration) time.Duration {
	if d <= minimum {
		return minimum
	}
	span := d + d/2 // up to 2*d, floored at minimum
	if span <= minimum {
		return minimum
	}
	return minimum + time.Duration(rand.Int63n(int64(span-minimum)+1))
}

// retryableAbort reports whether err proves that this transaction may be
// replayed unchanged: the two PostgreSQL states that mean the database rolled it
// back (40001 serialization_failure, 40P01 deadlock_detected), or a context
// claim this transaction could not decide.
func retryableAbort(err error) bool {
	if isContextClaimRace(err) {
		return true
	}
	switch sqlState(err) {
	case "40001", "40P01":
		return true
	default:
		return false
	}
}

// sleepBackoff waits d unless the context is done first, in which case the
// context error is returned so the caller aborts instead of retrying.
func sleepBackoff(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// nonNil normalises a nil string slice for pgx: a nil Go slice binds as SQL
// NULL, which the NOT NULL label columns reject, while an empty non-nil slice
// binds as the empty array '{}'.
func nonNil(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}

// nonnilSet normalises every component of a label set for SQL binding.
// labels.Merge returns nil for empty components, so this must run on every
// value that reaches a NOT NULL label column.
func nonnilSet(s labels.Set) labels.Set {
	return labels.Set{Subjects: nonNil(s.Subjects), Workspaces: nonNil(s.Workspaces), Taints: nonNil(s.Taints)}
}

// errContextForeignClaim reports that a context id is already claimed by
// another scope. It replaces the unique-violation path the in-place journal used
// to rely on: the anchor row is now claimed with ON CONFLICT DO NOTHING, so a
// losing race and a foreign claim are told apart by reading the claim back
// instead of by which constraint happened to fire.
var errContextForeignClaim = errors.New("identity: context id belongs to another scope")

// isContextForeignClaim reports errContextForeignClaim, however wrapped.
func isContextForeignClaim(err error) bool { return errors.Is(err, errContextForeignClaim) }

// errContextClaimRace reports a context id that is claimed, by somebody, but
// whose owner this transaction cannot determine from its own snapshot.
//
// It is not the same answer as errContextForeignClaim and must never be
// collapsed into it. Foreign is a verdict: the id belongs to another scope, the
// caller can do nothing, and the reply is NOT_FOUND. A race is the absence of
// an answer: the claim may well belong to THIS scope and have committed a
// moment ago, invisible to a snapshot taken before it. Answering a race with the
// foreign verdict is what D-ID-3 is about -
//
//	NOT_FOUND: identity: context id belongs to another scope
//
// sent a caller to hunt a tenant that does not exist, while the delegation it
// asked for was legal. A replay settles both cases correctly: the same-scope
// claim becomes readable and succeeds, the foreign one stays NOT_FOUND.
var errContextClaimRace = errors.New("identity: context journal was claimed concurrently")

// isContextClaimRace reports errContextClaimRace, however wrapped.
func isContextClaimRace(err error) bool { return errors.Is(err, errContextClaimRace) }

// contextJournalUnion builds the stored label set of the context row aliased as
// c: the labels the anchor carried before the journal became append-only, union
// every appended event, in the canonical sorted de-duplicated form.
func contextJournalUnion(column string) string {
	// The events are matched to their journal by owner as well as by id.
	// contexts_pk keys on the id alone, so two scopes can hold the same id, and an
	// event carrying the other scope's columns would otherwise widen this
	// journal's labels with someone else's taint. No product path writes such a
	// row today (a foreign claim returns before the append, and the transaction
	// rolls back), so this is a stored property rather than a live defect fix:
	// internal/identity/context_claim_race_test.go pins it with a row written
	// outside the service, the same way the append-only promise is pinned.
	return `ARRAY(SELECT DISTINCT u FROM unnest(c.` + column + ` || COALESCE((
		           SELECT array_agg(x)
		             FROM ahp_identity.context_label_events e, unnest(e.` + column + `) x
		            WHERE e.context_id = c.id
		              AND e.tenant = c.tenant
		              AND e.deployment = c.deployment
		              AND e.subject = c.subject), '{}'::text[])
		     ) u ORDER BY u)`
}

// readContextOwner answers who owns a context id, without the caller-scope
// filter readContext applies. It exists to tell a foreign claim apart from a
// claim this transaction simply cannot see yet.
func readContextOwner(ctx context.Context, tx pgx.Tx, contextID string) (contracts.Scope, bool, error) {
	var scope contracts.Scope
	err := tx.QueryRow(ctx, `
	    SELECT tenant, deployment, subject FROM ahp_identity.contexts WHERE id = $1`,
		contextID).Scan(&scope.Tenant, &scope.Deployment, &scope.Subject)
	if err != nil {
		if isNoRows(err) {
			return contracts.Scope{}, false, nil
		}
		return contracts.Scope{}, false, fmt.Errorf("identity: read context owner: %w", err)
	}
	return scope, true, nil
}

// ensureContext claims the journal of (scope, contextID) on first use, appends
// initial as one widening event, and returns the stored canonical union.
//
// Nothing here updates a row that another transaction can be writing: the claim
// is an insert-with-conflict-skip and the widening is an append. That is the
// whole point - a shared context journal used to be the second source of
// serialization aborts in this service after the quota count.
func ensureContext(ctx context.Context, tx pgx.Tx, scope contracts.Scope, contextID string, initial labels.Set) (labels.Set, error) {
	merged := nonnilSet(labels.Merge(initial))
	// nonnilSet turns a nil component into an empty array, so emptiness is a
	// cardinality question, never a nil one.
	addsLabels := len(merged.Subjects)+len(merged.Workspaces)+len(merged.Taints) > 0

	stored, found, err := readContext(ctx, tx, scope, contextID)
	if err != nil {
		return labels.Set{}, err
	}
	if found && !addsLabels {
		// Read-only outcome on a journal that exists: no write at all. That
		// matters more than it looks - the claim is an insert into a shared
		// unique index, and a speculative insertion is a write even when the
		// conflict turns it into a no-op. Measured, the unconditional claim put
		// every delegation of a shared context in contention with every other
		// one: all 90 aborts of the subject-revocation race landed on it.
		return stored, nil
	}
	if !found {
		// The arbiter is the id, which is the constraint that actually fires. The
		// shape this replaced arbitrated on (tenant, deployment, subject, id) - the
		// scope key - while contexts_pk keys on the id alone, so a duplicate id
		// from another scope did not match the arbiter, escaped as a raw 23505 on
		// contexts_pk, and the caller was told "belongs to another scope" from a
		// duplicate-key branch instead of from a decision (D-ID-3, 8 of 1920
		// concurrent delegations in Fermat's run). With the arbiter on the id, a
		// conflict is always skipped and the owner is asked directly.
		res, err := tx.Exec(ctx, `
		    INSERT INTO ahp_identity.contexts (id, tenant, deployment, subject)
		    VALUES ($1, $2, $3, $4)
		    ON CONFLICT (id) DO NOTHING`,
			contextID, scope.Tenant, scope.Deployment, scope.Subject)
		if err != nil {
			return labels.Set{}, fmt.Errorf("identity: claim context journal: %w", err)
		}
		if res.RowsAffected() == 0 {
			owner, visible, err := readContextOwner(ctx, tx, contextID)
			if err != nil {
				return labels.Set{}, err
			}
			switch {
			case !visible:
				// Somebody holds the id and this snapshot cannot see them yet, and
				// no event may be appended to a journal whose owner is unknown.
				return labels.Set{}, errContextClaimRace
			case owner != scope:
				return labels.Set{}, errContextForeignClaim
			}
			// Owned by this scope and visible: read it back below.
		}
	}
	if addsLabels {
		if _, err := appendContextEvent(ctx, tx, scope, contextID, merged); err != nil {
			return labels.Set{}, err
		}
	}
	stored, found, err = readContext(ctx, tx, scope, contextID)
	if err != nil {
		return labels.Set{}, err
	}
	if !found {
		owner, visible, err := readContextOwner(ctx, tx, contextID)
		if err != nil {
			return labels.Set{}, err
		}
		if visible && owner != scope {
			// Claimed, visible, and not ours: invisible, NOT_FOUND, no leak of who
			// owns it - the same answer a foreign claim gave when the journal was
			// one row.
			return labels.Set{}, errContextForeignClaim
		}
		// Claimed but not visible to this snapshot: replay decides.
		return labels.Set{}, errContextClaimRace
	}
	return stored, nil
}

// appendContextEvent widens the journal by one immutable event.
func appendContextEvent(ctx context.Context, tx pgx.Tx, scope contracts.Scope, contextID string, merged labels.Set) (labels.Set, error) {
	if _, err := tx.Exec(ctx, `
	    INSERT INTO ahp_identity.context_label_events
	        (context_id, tenant, deployment, subject,
	         labels_subjects, labels_workspaces, labels_taints)
	    VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		contextID, scope.Tenant, scope.Deployment, scope.Subject,
		merged.Subjects, merged.Workspaces, merged.Taints); err != nil {
		return labels.Set{}, fmt.Errorf("identity: append context journal: %w", err)
	}
	return merged, nil
}

// readContext reads one journal row inside the caller scope. A context owned
// by another scope is invisible: found is false, so the caller reports
// NOT_FOUND and existence never leaks.
func readContext(ctx context.Context, tx pgx.Tx, scope contracts.Scope, contextID string) (labels.Set, bool, error) {
	var subj, ws, taints []string
	err := tx.QueryRow(ctx, `
	    SELECT `+contextJournalUnion("labels_subjects")+`,
	           `+contextJournalUnion("labels_workspaces")+`,
	           `+contextJournalUnion("labels_taints")+`
	      FROM ahp_identity.contexts c
	     WHERE c.tenant = $1 AND c.deployment = $2 AND c.subject = $3 AND c.id = $4`,
		scope.Tenant, scope.Deployment, scope.Subject, contextID,
	).Scan(&subj, &ws, &taints)
	if err != nil {
		if isNoRows(err) {
			return labels.Set{}, false, nil
		}
		return labels.Set{}, false, fmt.Errorf("identity: read context journal: %w", err)
	}
	return labels.Set{Subjects: subj, Workspaces: ws, Taints: taints}, true, nil
}

// accumulateGrantLabels widens the label columns stored on one grant row.
// The WHERE clause only admits a strict superset, so a concurrent writer can
// never lose labels and a repeated call is a no-op.
func accumulateGrantLabels(ctx context.Context, tx pgx.Tx, grantID string, added labels.Set) error {
	merged := nonnilSet(labels.Merge(added))
	for _, table := range []string{"grants_child", "grants_root"} {
		res, err := tx.Exec(ctx, `
		    UPDATE ahp_identity.`+table+`
		       SET labels_subjects = ARRAY(SELECT DISTINCT unnest(
	                   labels_subjects || $2::text[]) ORDER BY 1),
		           labels_workspaces = ARRAY(SELECT DISTINCT unnest(
	                   labels_workspaces || $3::text[]) ORDER BY 1),
		           labels_taints = ARRAY(SELECT DISTINCT unnest(
	                   labels_taints || $4::text[]) ORDER BY 1)
		     WHERE id = $1
		       AND NOT (labels_subjects @> $2::text[]
		                AND labels_workspaces @> $3::text[]
		                AND labels_taints @> $4::text[])`,
			grantID, merged.Subjects, merged.Workspaces, merged.Taints)
		if err != nil {
			return fmt.Errorf("identity: accumulate grant labels: %w", err)
		}
		if res.RowsAffected() > 0 {
			return nil
		}
	}
	return nil
}

// ancestorChain returns the grant itself followed by every ancestor up to the
// root, in bottom-up order. It is the propagation path of a label addition:
// under-tainting an ancestor would let a private child be read through it.
func ancestorChain(ctx context.Context, tx pgx.Tx, g Grant) ([]Grant, error) {
	rows, err := tx.Query(ctx, `
	    WITH RECURSIVE chain(ord, id) AS (
	        SELECT 0, $1::text
	        UNION ALL
	        SELECT c.ord + 1, coalesce(nullif(n.parent, ''), n.id)
	          FROM chain c
	          JOIN ahp_identity.grants_child n ON n.id = c.id
	         WHERE c.ord < 32
	    )
	    SELECT id FROM chain ORDER BY ord`, g.ID)
	if err != nil {
		return nil, fmt.Errorf("identity: walk chain: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("identity: scan chain: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("identity: walk chain: %w", err)
	}

	out := make([]Grant, 0, len(ids))
	for _, id := range ids {
		node, found, err := findGrantByID(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, contracts.NewError(contracts.ErrConflict,
				"identity: delegation chain is broken at a missing grant")
		}
		out = append(out, node)
	}
	return out, nil
}
