// Package identity: the delegation graph must be acyclic, and a corrupt graph
// must fail as a contract instead of as a hang.
//
// Requirement (ahp-agents-space-architecture (2) (1).pdf, rev 3.2): extracted
// line 2703 - "Глубину, циклы, fan-out, резервы бюджета и отзыв ведут gateway,
// Identity & Policy" - cycles are this service's job, and extracted line 3561
// lists "цикл делегирования и fan-out сверх лимита" as a case the platform must
// answer for. An answer delivered by a statement timeout is not an answer: the
// server-side statement_timeout is an instance setting this service does not
// own, and on an instance that does not set one the walk never ends at all.
//
// D-ID-2 (found by Fermat's independent run,
// reports/mac-director/identity-r4-concurrency-fermat-20261110.md): a cyclic
// parent chain makes "WITH RECURSIVE ... UNION ALL" over grants_child spin
// forever; it was stopped only by statement_timeout=2s (SQLSTATE 57014) after
// 2.006888583s.
package identity

import (
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// edgeInvariantTrigger is the database-level guard that makes a cycle
// unrepresentable: every edge must deepen the tree by exactly one level below
// one root. It is a LOCAL property of an edge, so no arrangement of edges can
// satisfy every edge and still close a loop - going round a cycle would require
// a depth strictly greater than itself. That is why the guard is placed on the
// edge rather than on the walk: a walk-time depth cap can be escaped by drawing
// a cycle the cap does not reach, while this rule cannot be escaped by any
// number of edges.
const edgeInvariantTrigger = "grants_child_edge_invariant"

// forgeCycleEdge reparents a grant onto one of its own descendants, which is
// exactly how a cycle enters a grants_child table: through a writer that does
// not go through Delegate (an import, a repair script, a legacy database).
// The guard is temporarily taken off the table with DDL - the only way to
// obtain a pre-guard row, and itself an owner-only, logged operation; there is
// no session flag or other in-application escape hatch.
func forgeCycleEdge(t *testing.T, s *Store, childID, newParentID string) {
	t.Helper()
	ctx := testContext(t)
	disable := `ALTER TABLE ahp_identity.grants_child DISABLE TRIGGER ` + edgeInvariantTrigger
	if _, err := s.pool.Exec(ctx, disable); err != nil {
		t.Logf("D-ID-2 substrate: %s: guard %s could not be disabled (absent = no guard exists yet): %v",
			childID, edgeInvariantTrigger, err)
	} else {
		t.Cleanup(func() {
			c := testContext(t)
			if _, err := s.pool.Exec(c, `ALTER TABLE ahp_identity.grants_child ENABLE TRIGGER `+edgeInvariantTrigger); err != nil {
				t.Fatalf("D-ID-2: re-enable guard: %v", err)
			}
		})
	}
	if _, err := s.pool.Exec(ctx, `UPDATE ahp_identity.grants_child SET parent = $2 WHERE id = $1`,
		childID, newParentID); err != nil {
		t.Fatalf("D-ID-2: forge %s -> %s: %v", childID, newParentID, err)
	}
}

// depthViolations counts rows whose depth does not follow their parent, the
// local signature every cycle must leave: round a loop and some edge fails to
// deepen.
func depthViolations(t *testing.T, s *Store, rootID string) int {
	t.Helper()
	ctx := testContext(t)
	var n int
	if err := s.pool.QueryRow(ctx, `
	    SELECT count(*) FROM ahp_identity.grants_child c
	      JOIN ahp_identity.grants_child p ON p.id = c.parent
	     WHERE c.root = $1 AND c.depth <> p.depth + 1`, rootID).Scan(&n); err != nil {
		t.Fatalf("D-ID-2: count depth violations: %v", err)
	}
	return n
}

// TestDelegationCycleCannotBeForgedIntoTheGraph is the D-ID-2 prevention leg.
//
// A single edge and a three-edge detour are both attempted, because a guard
// that only looks at "did this one insert create a loop" is escapable by
// drawing the loop across several writes. Here every individual edge is
// rejected on its own merits.
func TestDelegationCycleCannotBeForgedIntoTheGraph(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)

	for _, tc := range []struct {
		name  string
		chain []int
	}{
		{name: "one edge", chain: []int{1, 1}},
		{name: "three edge detour", chain: []int{1, 1, 1, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope := uniqueScope(t)
			rootID, byLevel := seedGrantTree(t, s, scope, "cyc", tc.chain, 1000)
			top := byLevel[0][0]
			deepest := byLevel[len(byLevel)-1][0]
			var parentBefore string
			if err := s.pool.QueryRow(ctx, `SELECT parent FROM ahp_identity.grants_child WHERE id = $1`, top).Scan(&parentBefore); err != nil {
				t.Fatalf("read parent: %v", err)
			}

			_, err := s.pool.Exec(ctx, `UPDATE ahp_identity.grants_child SET parent = $2 WHERE id = $1`, top, deepest)
			if err == nil {
				n := depthViolations(t, s, rootID)
				t.Fatalf("D-ID-2: the graph accepted an edge that closes a loop (%s -> %s); %d rows now violate depth = parent.depth + 1 and every recursive walk of this tree hangs until the instance statement_timeout stops it",
					top, deepest, n)
			}
			code := sqlState(err)
			t.Logf("D-ID-2 %s: reparenting %s onto its own descendant %s was rejected by the database: SQLSTATE %s: %s",
				tc.name, top, deepest, code, err)
			if !strings.Contains(err.Error(), "cycle") && !strings.Contains(err.Error(), "acyclic") {
				t.Errorf("D-ID-2: rejection must name the acyclicity rule it enforces, got SQLSTATE %s: %v", code, err)
			}
			var parentAfter string
			if err := s.pool.QueryRow(ctx, `SELECT parent FROM ahp_identity.grants_child WHERE id = $1`, top).Scan(&parentAfter); err != nil {
				t.Fatalf("read parent after: %v", err)
			}
			if parentAfter != parentBefore {
				t.Errorf("D-ID-2: the rejected write still moved the edge: parent %q -> %q", parentBefore, parentAfter)
			}
			if n := depthViolations(t, s, rootID); n != 0 {
				t.Errorf("D-ID-2: %d rows violate the edge invariant after a rejected write", n)
			}
		})
	}
}

// TestDelegationCycleFailsWithContractErrorNotTimeout is the D-ID-2 leg that
// matters to a caller: a tree that ALREADY carries a cycle (written before the
// guard existed, or by a writer outside the service) must be answered with a
// contract, quickly.
//
// Today the answer is a hang terminated by whatever statement_timeout the
// instance happens to have, surfacing as SQLSTATE 57014 - which reads like a
// timeout, tells the caller nothing about the delegation graph, and would run
// forever on an instance that sets no timeout.
func TestDelegationCycleFailsWithContractErrorNotTimeout(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	rootID, byLevel := seedGrantTree(t, s, scope, "cycq", []int{1, 1}, 1000)
	top, deepest := byLevel[0][0], byLevel[1][0]
	forgeCycleEdge(t, s, top, deepest)
	if n := depthViolations(t, s, rootID); n == 0 {
		t.Fatal("D-ID-2 substrate: forging the cycle left no invariant violation - the substrate is wrong")
	}
	t.Logf("D-ID-2 substrate: cycle forged under root %s (%s -> %s), %d rows violate the edge invariant",
		rootID, top, deepest, depthViolations(t, s, rootID))

	const budget = time.Second
	started := time.Now()
	_, err := s.Delegate(ctx, scope, "task-"+sanitizeID(top), DelegateRequest{
		Task:         "under-cycle-" + sanitizeID(contracts.NewID()),
		Context:      "ctx-" + sanitizeID(contracts.NewID()),
		Deployment:   scope.Deployment,
		Capabilities: []string{"read"},
		TTL:          time.Hour,
		AllowSelf:    true,
	})
	elapsed := time.Since(started)
	t.Logf("D-ID-2 verdict: delegate over a cyclic tree answered in %s: code=%q sqlstate=%q err=%v",
		elapsed, contracts.Code(err), sqlState(err), err)

	if err == nil {
		t.Fatal("D-ID-2: a delegation over a cyclic delegation graph was admitted; the walk cannot know the size of the tree it is standing in")
	}
	if state := sqlState(err); state == "57014" {
		t.Errorf("D-ID-2: the cycle was reported as a statement timeout (SQLSTATE 57014) after %s. A hang stopped by an instance setting is not a contract answer, and on an instance without statement_timeout the walk never ends", elapsed)
	}
	if elapsed > budget {
		t.Errorf("D-ID-2: the caller waited %s for an answer about a corrupt graph; the contract failure must be immediate (budget %s)", elapsed, budget)
	}
	if code := contracts.Code(err); code != contracts.ErrUnavailable {
		t.Errorf("D-ID-2: a corrupt delegation graph must be %s (the operator has to repair stored state), got %q: %v",
			contracts.ErrUnavailable, code, err)
	}
	if !strings.Contains(err.Error(), "delegation graph is corrupt") {
		t.Errorf("D-ID-2: the error must say what is wrong with the delegation graph, got: %v", err)
	}
}

// TestCancelSubtreeOverCorruptGraphIsFailSafe is the D-ID-2 control-plane leg.
//
// PDF rev3.2 (extracted line 2378): "Отмена хода Dot отзывает поддерево". A
// corrupt parent chain must not turn cancellation into a hang - cancellation is
// the operation the platform reaches for when something is already wrong. The
// walk must terminate and it must stay fail-safe: every row carrying the root is
// revoked, never fewer.
func TestCancelSubtreeOverCorruptGraphIsFailSafe(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	rootID, byLevel := seedGrantTree(t, s, scope, "cycc", []int{2, 4, 8}, 1000)
	forgeCycleEdge(t, s, byLevel[0][0], byLevel[2][0])

	const budget = 1500 * time.Millisecond
	started := time.Now()
	rootTask := "root-task-" + sanitizeID(rootID)
	// The seeded root task is prefixed, not task-prefixed: read it back rather
	// than assume the seed's naming.
	if err := s.pool.QueryRow(ctx, `SELECT task FROM ahp_identity.grants_root WHERE id = $1`, rootID).Scan(&rootTask); err != nil {
		t.Fatalf("read root task: %v", err)
	}
	err := s.CancelSubtree(ctx, scope, rootTask)
	elapsed := time.Since(started)
	t.Logf("D-ID-2 cancel verdict: %s over a cyclic tree returned in %s: %v", rootID, elapsed, err)
	if elapsed > budget {
		t.Errorf("D-ID-2: cancelling a cyclic tree took %s - the descendant walk does not terminate on its own and was stopped by the instance statement_timeout (budget %s)", elapsed, budget)
	}
	if err != nil {
		t.Errorf("D-ID-2: cancelling the root of a corrupt tree must still revoke the tree (fail-safe), got: %v", err)
	}
	var live int
	if err := s.pool.QueryRow(ctx, `
	    SELECT count(*) FROM ahp_identity.grants_child
	     WHERE root = $1 AND status = 'active'`, rootID).Scan(&live); err != nil {
		t.Fatalf("count live children: %v", err)
	}
	if live != 0 {
		var total int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_identity.grants_child WHERE root = $1`, rootID).Scan(&total); err != nil {
			t.Fatalf("count children: %v", err)
		}
		t.Errorf("D-ID-2: %d of %d rows under the cancelled root are still active - the walk lost rows to the cycle", live, total)
	}
}

// TestCancelChildSubtreeOverCorruptGraphIsComplete covers the other half of the
// cancellation answer. A child cancel must not revoke uncles and siblings - PDF
// rev3.2 keeps "cancelling a child leaves the root and its siblings live" - but
// it must still terminate and still revoke the target, because a refusal to
// cancel would roll the revocation back and leave a corrupt tree running, which
// is the fail-open direction.
func TestCancelChildSubtreeOverCorruptGraphIsComplete(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	rootID, byLevel := seedGrantTree(t, s, scope, "cyck", []int{2, 4, 8}, 1000)
	mid := byLevel[1][0]
	forged := byLevel[0][0]
	forgeCycleEdge(t, s, forged, byLevel[2][0])

	var midTask string
	if err := s.pool.QueryRow(ctx, `SELECT task FROM ahp_identity.grants_child WHERE id = $1`, mid).Scan(&midTask); err != nil {
		t.Fatalf("read child task: %v", err)
	}
	started := time.Now()
	err := s.CancelSubtree(ctx, scope, midTask)
	elapsed := time.Since(started)
	t.Logf("D-ID-2 child-cancel verdict over a cyclic tree: %s in %s: err=%v", mid, elapsed, err)
	if elapsed > 1500*time.Millisecond {
		t.Errorf("D-ID-2: cancelling a child of a cyclic tree took %s - the descendant walk must reach a fixed point on its own", elapsed)
	}
	if err != nil {
		t.Errorf("D-ID-2: cancelling a child must land; an error here rolls the revocation back and leaves the tree running: %v", err)
	}
	var status string
	if err := s.pool.QueryRow(ctx, `SELECT status FROM ahp_identity.grants_child WHERE id = $1`, mid).Scan(&status); err != nil {
		t.Fatalf("read child status: %v", err)
	}
	if status != StatusRevoked {
		t.Errorf("D-ID-2: the cancelled child is %q, want %s", status, StatusRevoked)
	}
	// Its own descendants went with it.
	var liveKids int
	if err := s.pool.QueryRow(ctx, `
	    SELECT count(*) FROM ahp_identity.grants_child
	     WHERE parent = $1 AND status = '`+StatusActive+`'`, mid).Scan(&liveKids); err != nil {
		t.Fatalf("count descendants: %v", err)
	}
	if liveKids != 0 {
		t.Errorf("D-ID-2: %d descendants of the cancelled child are still active", liveKids)
	}
	// And the walk did not decide to revoke the whole tenant: the root stands.
	var rootStatus string
	if err := s.pool.QueryRow(ctx, `SELECT status FROM ahp_identity.grants_root WHERE id = $1`, rootID).Scan(&rootStatus); err != nil {
		t.Fatalf("read root status: %v", err)
	}
	if rootStatus != StatusActive {
		t.Errorf("D-ID-2: cancelling a child revoked its root (%q); ancestors are not the target", rootStatus)
	}
}
