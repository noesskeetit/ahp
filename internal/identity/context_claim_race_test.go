// Package identity: a context journal claimed by THIS scope must never be
// reported as belonging to another one.
//
// D-ID-3 (found by Fermat's independent run,
// reports/mac-director/identity-r4-concurrency-fermat-20261110.md): 8 of 1920
// concurrent delegations came back as
//
//	NOT_FOUND: identity: context id belongs to another scope
//
// with the raw database error being a duplicate-key violation on
// contexts_pk (the id alone), while the ON CONFLICT arbiter the code relied on
// named contexts_scope_key_unique. A wrong error class is worse than an error:
// it sends the reader after a foreign tenant that does not exist, and in the
// space service this exact masking pattern was already classified as a defect
// worth its own number.
//
// The shape does not need luck to reproduce. Under SERIALIZABLE a transaction
// reads from the snapshot taken at its first statement, so:
//
//	A: read the journal  -> absent          (A's snapshot is now fixed)
//	B: claim the journal -> committed        (same tenant, same deployment, same subject)
//	A: claim the journal -> the id exists, but A cannot see who owns it
//
// That last step is a race inside one scope, and today it is answered as a
// foreign claim. The two are distinguishable, and the answer matters: a race
// must be replayed, a foreign claim must stay NOT_FOUND.
package identity

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// TestSameScopeContextClaimIsNotReportedAsForeignScope is the deterministic
// D-ID-3 leg: no scheduling luck, the two transactions are driven by hand.
func TestSameScopeContextClaimIsNotReportedAsForeignScope(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	contextID := "ctx-race-" + sanitizeID(contracts.NewID())

	serializable := pgx.TxOptions{IsoLevel: pgx.Serializable}
	txEarly, err := s.pool.BeginTx(ctx, serializable)
	if err != nil {
		t.Fatalf("begin early transaction: %v", err)
	}
	defer func() { _ = txEarly.Rollback(ctx) }()

	// The early transaction establishes its snapshot here and sees no journal.
	if _, found, err := readContext(ctx, txEarly, scope, contextID); err != nil || found {
		t.Fatalf("substrate: the journal must be absent at this point (found=%v err=%v)", found, err)
	}

	txLate, err := s.pool.BeginTx(ctx, serializable)
	if err != nil {
		t.Fatalf("begin claiming transaction: %v", err)
	}
	if _, err := ensureContext(ctx, txLate, scope, contextID, labels.Set{}); err != nil {
		_ = txLate.Rollback(ctx)
		t.Fatalf("substrate: claim the journal: %v", err)
	}
	if err := txLate.Commit(ctx); err != nil {
		t.Fatalf("substrate: commit the claim: %v", err)
	}

	// Same scope, same id, snapshot fixed before the claim landed.
	_, err = ensureContext(ctx, txEarly, scope, contextID, labels.Set{})
	t.Logf("D-ID-3 same-scope claim over a concurrently created journal: foreignClaim=%v retriable=%v err=%v",
		isContextForeignClaim(err), retryableAbort(err), err)
	if isContextForeignClaim(err) {
		t.Errorf("D-ID-3: a context journal created by THIS scope in a transaction that committed after our snapshot was reported as \"%s\". It is a claim race inside one scope, not a foreign owner: a caller chasing a foreign tenant can never resolve it, and a legitimate delegation of a personal Dot is refused with a NOT_FOUND that names a scope which does not exist",
			err)
	}
	if err != nil && !retryableAbort(err) {
		t.Errorf("D-ID-3: a claim this transaction cannot yet decide must be replayed, not returned as a verdict; got a terminal error: %v", err)
	}
}

// TestForeignContextClaimStillReportsNotFound is the control that keeps the fix
// honest: a journal that really belongs to another scope must stay invisible and
// must still be answered as a foreign claim, so that the race path cannot become
// a way to probe or widen someone else's journal.
func TestForeignContextClaimStillReportsNotFound(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	owner := uniqueScope(t)
	intruder := uniqueScope(t)
	if intruder.Tenant == owner.Tenant {
		t.Fatal("substrate: the intruder must be in another tenant")
	}
	contextID := "ctx-foreign-" + sanitizeID(contracts.NewID())

	// The owner claims the journal through the product, so the claim is durable
	// and committed before the intruder looks at it.
	if _, err := s.CreateRoot(ctx, owner, RootRequest{
		Task:         "owner-root-" + sanitizeID(contracts.NewID()),
		Context:      contextID,
		Deployment:   owner.Deployment,
		Capabilities: []string{"read"},
		TTL:          time.Hour,
	}); err != nil {
		t.Fatalf("substrate: owner claims the journal: %v", err)
	}
	_, err := ensureContext(ctx, mustTx(t, s), intruder, contextID, labels.Set{})
	t.Logf("D-ID-3 control: intruder claim of a foreign journal: foreignClaim=%v err=%v", isContextForeignClaim(err), err)
	if !isContextForeignClaim(err) {
		t.Errorf("D-ID-3: a genuine foreign claim must stay \"%s\", got %v", "identity: context id belongs to another scope", err)
	}
	if err != nil && retryableAbort(err) {
		t.Errorf("D-ID-3: a genuine foreign claim is a verdict, not a race; making it retriable would let one scope hammer another scope's journal id forever")
	}
	// The answer must be a decision, not a leaked constraint violation. This is
	// the mechanism D-ID-3 was made of: the claim arbitrated on
	// (tenant, deployment, subject, id) while contexts_pk keys on the id alone, so
	// a duplicate id from another scope missed the arbiter, escaped as 23505 on
	// contexts_pk, and the "is it a duplicate key" branch produced the
	// foreign-scope sentence. With the arbiter on the id the conflict is skipped
	// and the owner is asked.
	if isUniqueViolation(err) {
		t.Errorf("D-ID-3: the foreign claim was produced by a raw duplicate-key violation (SQLSTATE %s) rather than by reading who owns the journal: %v", sqlState(err), err)
	}
}

// TestConcurrentSameScopeDelegatesSharingFreshContextNeverReportForeignScope is
// the caller-visible shape of D-ID-3: one subject, one deployment, one context
// id that does not exist yet, and a wall of delegations claiming it at once.
//
// Every answer must be a delegation, a CONFLICT the caller may replay unchanged,
// or a forbidden ceiling. NOT_FOUND naming another scope is never a correct
// answer here, because there is no other scope in the test.
func TestConcurrentSameScopeDelegatesSharingFreshContextNeverReportForeignScope(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	root := createRootForTest(t, s, scope, "race-root", "ctx-race-root-"+sanitizeID(contracts.NewID()))
	shared := "ctx-shared-race-" + sanitizeID(contracts.NewID())

	const workers, rounds = 16, 15
	var (
		ok            atomic.Int64
		conflict      atomic.Int64
		foreignClaims atomic.Int64
		other         atomic.Int64
	)
	var firstForeign string
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			callerCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			for r := 0; r < rounds; r++ {
				_, err := s.Delegate(callerCtx, scope, root.Task, DelegateRequest{
					Task:         fmt.Sprintf("race-w%d-r%d", w, r),
					Context:      shared,
					Deployment:   scope.Deployment,
					Capabilities: []string{"read"},
					TTL:          time.Hour,
					AllowSelf:    true,
				})
				switch {
				case err == nil:
					ok.Add(1)
				case isContextForeignClaim(err) || contracts.Code(err) == contracts.ErrNotFound:
					foreignClaims.Add(1)
					mu.Lock()
					if firstForeign == "" {
						firstForeign = err.Error()
					}
					mu.Unlock()
				case contracts.Code(err) == contracts.ErrConflict:
					conflict.Add(1)
				default:
					other.Add(1)
					mu.Lock()
					if other.Load() == 1 {
						t.Logf("D-ID-3 unexpected answer: %v", err)
					}
					mu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()

	calls := int64(workers) * int64(rounds)
	t.Logf("D-ID-3 wall of %d same-scope delegations over one fresh context id: admitted=%d replayable-conflict=%d false-foreign-NOT_FOUND=%d other=%d",
		calls, ok.Load(), conflict.Load(), foreignClaims.Load(), other.Load())
	if foreignClaims.Load() > 0 {
		t.Errorf("D-ID-3: %d of %d delegations inside ONE scope were answered with \"%s\" - there is no other scope in this test. First: %s",
			foreignClaims.Load(), calls, "identity: context id belongs to another scope", firstForeign)
	}
	if other.Load() > 0 {
		t.Errorf("D-ID-3: %d delegations came back with an error that is neither success nor a replayable CONFLICT", other.Load())
	}
	if ok.Load() == 0 {
		t.Errorf("D-ID-3: nothing was admitted at all - the ceiling under max_active=%d would be a different story, but nothing worked", MaxTreeActive)
	}
}

// TestForeignScopedJournalEventsDoNotWidenAJournal is defence in depth, and the
// distinction matters: no product path writes such a row, because a foreign
// claim returns before the append and the surrounding transaction rolls back.
//
// The reason the rule is still pinned here is the key. A journal is addressed by
// context id (contexts_pk), while its scope columns are a separate unique key,
// so the same id can legitimately exist in two scopes. If the union of a journal
// ever matched events by id alone, one scope's labels would widen another
// scope's grants - a taint crossing a tenant boundary. That must be a property of
// the read, not a habit of the callers, for the same reason the append-only
// promise is pinned with a row written outside the service.
func TestForeignScopedJournalEventsDoNotWidenAJournal(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	owner := uniqueScope(t)
	stranger := uniqueScope(t)
	contextID := "ctx-mixed-" + sanitizeID(contracts.NewID())

	if _, err := s.CreateRoot(ctx, owner, RootRequest{
		Task:         "owner-" + sanitizeID(contracts.NewID()),
		Context:      contextID,
		Deployment:   owner.Deployment,
		Capabilities: []string{"read"},
		TTL:          time.Hour,
	}); err != nil {
		t.Fatalf("substrate: owner claims the journal: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `
	    INSERT INTO ahp_identity.context_label_events
	        (context_id, tenant, deployment, subject,
	         labels_subjects, labels_workspaces, labels_taints)
	    VALUES ($1,$2,$3,$4,'{attacker-subject}','{attacker-workspace}','{attacker-taint}')`,
		contextID, stranger.Tenant, stranger.Deployment, stranger.Subject); err != nil {
		t.Fatalf("substrate: write a stranger-scoped event: %v", err)
	}

	stored, found, err := readContext(ctx, mustTx(t, s), owner, contextID)
	if err != nil || !found {
		t.Fatalf("read the owner's journal: found=%v err=%v", found, err)
	}
	for _, tc := range []struct {
		name string
		have []string
		want string
	}{
		{name: "subjects", have: stored.Subjects, want: "attacker-subject"},
		{name: "workspaces", have: stored.Workspaces, want: "attacker-workspace"},
		{name: "taints", have: stored.Taints, want: "attacker-taint"},
	} {
		for _, got := range tc.have {
			if got == tc.want {
				t.Errorf("D-ID-3: an event owned by another scope widened the %s of this journal (%v): labels cross a scope boundary through the context id", tc.name, tc.have)
			}
		}
	}
	t.Logf("D-ID-3 defence in depth: owner journal after a stranger-scoped event row: subjects=%v workspaces=%v taints=%v",
		stored.Subjects, stored.Workspaces, stored.Taints)
}
