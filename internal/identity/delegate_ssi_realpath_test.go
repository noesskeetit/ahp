//go:build r4_open_measure

package identity

// R4-OPEN MEASUREMENT legs. These legs assert an invariant the product does not
// hold yet, so they are NOT part of the default package run: run them with
//   go test -tags "ahp_exec ahp_data state_integration r4_open_measure" ./internal/identity/
//
// Defect reference: reports/mac-director/STATE-LEASE-DEADLOCK-ANALYSIS-20261110T0200Z.md s4
// and reports/mac-director/identity-delegation-defects-20261110.md s6 (R4-OPEN-1/2).
// What is still open is abort FREQUENCY under SSI, not correctness: a quota range
// read races sibling inserts of the same scope, and an advisory lock is invisible
// to the serialization graph, so it orders writes without ordering snapshots.
// Closing R4-OPEN-1/2 needs either a stored per-root counter with a materialised
// expiry status (blocked by grants_child_status_ck admitting only active/revoked)
// or a session-level root lock taken before the snapshot opens.
//
// The availability invariant that DOES belong to the default run - no caller may
// be handed a raw 40001/40P01, and legitimate load must not surface CONFLICT -
// lives in r4_open_measure_off_test.go and delegate_ssi_available_test.go.

// R4 measurement and RED invariants on REAL product entry points, against real
// PostgreSQL, with a wire-level abort counter.
//
// Why a wire counter: every Identity write runs SERIALIZABLE
// (internal/identity/journal.go:32-34) and runTxMode replays 40001/40P01 in
// silence up to maxTxAttempts (journal.go:53-79), so a caller never observes an
// abort no matter how often the database raises one. Counting at the wire is the
// only way to see what the retry loop absorbs; counting returned errors would
// report zero for a store that aborts on every attempt but the last.
//
// What is measured, in the words of the analysis this work closes
// (reports/mac-director/STATE-LEASE-DEADLOCK-ANALYSIS-20261110T0200Z.md, s4):
//
//   shared context journal - ensureContext UPSERTs one shared row
//   (internal/identity/journal.go:151-176) with no WHERE guard, so it writes the
//   row even when the journal already contains every label. Two transactions
//   that both write one row serialize: one of them is aborted by the database.
//
//   tenant-wide revocation vs the quota count - RevokeTenant writes
//   grants_child by tenant under the tenant-erasure lock
//   (internal/identity/tenant_deletion.go:18-27), while CreateRoot/Delegate hold
//   the SHARED lock of the subject-erasure key only (delegate.go:112, root.go:72).
//   The tenant key is taken by the insert guard trigger, which fires after the
//   quota has already been counted, so a revocation can commit inside the window
//   of a live range read - the read/write hazard R4 is about.
//
// Each test states the invariant the eventual fix must deliver; the numbers
// printed are the before-side of the before/after evidence.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// TestConcurrentDelegatesSharingOneContextJournalDoNotAbort is the item-2
// invariant: delegations that reference the same context journal must not be
// serialized by the database. Two independent roots in one scope are used on
// purpose - their root advisory locks differ, so nothing in the product
// serialises them and the only shared object is the journal row itself.
func TestConcurrentDelegatesSharingOneContextJournalDoNotAbort(t *testing.T) {
	s, counted := tracedStore(t)
	scope := uniqueScope(t)
	sharedContext := "ctx-shared-" + sanitizeID(contracts.NewID())
	roots := []Grant{
		createRootForTest(t, s, scope, "root-a", sharedContext),
		createRootForTest(t, s, scope, "root-b", sharedContext),
	}

	const workers, rounds = 8, 20
	var calls, failures atomic.Int64
	started := time.Now()
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			parent := roots[worker%len(roots)]
			parentTask := parent.Task
			for round := 0; round < rounds; round++ {
				calls.Add(1)
				_, err := s.Delegate(ctx, scope, parentTask, DelegateRequest{
					Task:         fmt.Sprintf("child-w%d-r%d", worker, round),
					Context:      sharedContext,
					Deployment:   scope.Deployment,
					Capabilities: []string{"read"},
					TTL:          time.Hour,
					AllowSelf:    true,
				})
				if err != nil {
					failures.Add(1)
					noRetryableSurface(t, "shared_context_delegate", err)
					t.Errorf("shared_context_delegate: worker %d round %d: %v", worker, round, err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	reportAborts(t, "shared_context_delegate", counted, calls.Load(), failures.Load(), time.Since(started))
	if failures.Load() != 0 {
		t.Fatalf("shared_context_delegate: %d of %d delegations failed", failures.Load(), calls.Load())
	}
	assertJournalNeverUpdated(t, s, "shared_context_delegate")
	if aborted := counted.total(); aborted != 0 {
		t.Errorf("R4-OPEN-1 shared_context_delegate: %d of %d delegations were aborted by the database and replayed in silence (%d of them 40001). The journal no longer serialises them; what remains is the quota read racing sibling inserts of the same scope, which the root advisory lock cannot cover because an advisory lock orders writes without ordering snapshots. Closing this needs variant (i) or (ii), not another index.",
			aborted, calls.Load(), counted.serialization.Load())
	}
}

// TestConcurrentAddLabelsOnSharedContextJournalKeepsEveryLabel is the item-2
// invariant on the widening path: concurrent AddLabels calls that share a
// context journal must converge on the union of everything added, with no
// serialization aborts between the roots that share the row.
func TestConcurrentAddLabelsOnSharedContextJournalKeepsEveryLabel(t *testing.T) {
	s, counted := tracedStore(t)
	scope := uniqueScope(t)
	sharedContext := "ctx-lbl-" + sanitizeID(contracts.NewID())
	roots := []Grant{
		createRootForTest(t, s, scope, "root-a", sharedContext),
		createRootForTest(t, s, scope, "root-b", sharedContext),
	}

	const workers, rounds = 6, 15
	var calls, failures atomic.Int64
	started := time.Now()
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			parentTask := roots[worker%len(roots)].Task
			for round := 0; round < rounds; round++ {
				calls.Add(1)
				_, err := s.AddLabels(ctx, scope, parentTask, labels.Set{
					Subjects: []string{fmt.Sprintf("s-%d-%d", worker, round)},
				})
				if err != nil {
					failures.Add(1)
					noRetryableSurface(t, "shared_context_addlabels", err)
					t.Errorf("shared_context_addlabels: worker %d round %d: %v", worker, round, err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	reportAborts(t, "shared_context_addlabels", counted, calls.Load(), failures.Load(), time.Since(started))
	if failures.Load() != 0 {
		t.Fatalf("shared_context_addlabels: %d of %d calls failed", failures.Load(), calls.Load())
	}
	assertJournalNeverUpdated(t, s, "shared_context_addlabels")
	if aborted := counted.total(); aborted != 0 {
		t.Errorf("R4-OPEN-1 shared_context_addlabels: %d database aborts over %d calls (%d of them 40001). No journal row is updated any more; the residual is the widening of two roots racing each other's reads of the shared scope.",
			aborted, calls.Load(), counted.serialization.Load())
	}

	// Semantics: labels are monotone, so every label any worker added must still
	// be readable. The read goes through the product's own Get, not through a
	// query of the storage shape: the contract is "a grant carries its
	// journal's labels", and that is what must survive the rewrite of the
	// journal's storage, on the old tree and the new one alike.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	have := map[string]bool{}
	for _, root := range roots {
		grant, err := s.Get(ctx, scope, root.Task)
		if err != nil {
			t.Fatalf("Get(%s): %v", root.Task, err)
		}
		for _, label := range grant.Labels.Subjects {
			have[label] = true
		}
	}
	for worker := 0; worker < workers; worker++ {
		for round := 0; round < rounds; round++ {
			want := fmt.Sprintf("s-%d-%d", worker, round)
			if !have[want] {
				t.Errorf("label %q was lost: the journal no longer answers with everything that was added to it", want)
			}
		}
	}
}

// TestDelegateRacingTenantRevocationNeverExposesSerializationFailure is the
// item-1 invariant: a control-plane tenant revocation is a writer of the very
// range the delegate quota counts, and it does not participate in the root
// advisory protocol. The grant plane may deny a delegation because the tenant
// was revoked - that is a stated product decision - but the database must never
// have to abort the delegation, and a caller must never be handed a raw 40001.
func TestDelegateRacingTenantRevocationNeverExposesSerializationFailure(t *testing.T) {
	s, counted := tracedStoreRole(t, "delegate")
	control, controlCounted := tracedStoreRole(t, "revoke-tenant")
	scope := uniqueScope(t)
	root := createRootForTest(t, s, scope, "root-task", "ctx-race-"+sanitizeID(contracts.NewID()))

	const workers, rounds = 6, 25
	var calls, failures, denials atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		time.Sleep(150 * time.Millisecond)
		if err := control.RevokeTenant(ctx, scope.Tenant); err != nil {
			t.Errorf("RevokeTenant: %v", err)
		}
		close(stop)
	}()

	started := time.Now()
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			for round := 0; round < rounds; round++ {
				select {
				case <-stop:
					return
				default:
				}
				calls.Add(1)
				_, err := s.Delegate(ctx, scope, root.Task, DelegateRequest{
					Task:         fmt.Sprintf("race-w%d-r%d", worker, round),
					Context:      fmt.Sprintf("ctx-race-w%d-r%d", worker, round),
					Deployment:   scope.Deployment,
					Capabilities: []string{"read"},
					TTL:          time.Hour,
					AllowSelf:    true,
				})
				if err == nil {
					continue
				}
				failures.Add(1)
				noRetryableSurface(t, "tenant_revoke_race", err)
				// After revocation a delegation is expected to be denied; a
				// denial is a stated decision, recorded separately from a
				// database abort.
				if code := contracts.Code(err); code == contracts.ErrForbidden || code == contracts.ErrNotFound {
					denials.Add(1)
					continue
				}
				t.Errorf("tenant_revoke_race: worker %d round %d: unexpected %v", worker, round, err)
				return
			}
		}(worker)
	}
	wg.Wait()
	reportAborts(t, "tenant_revoke_race/delegate_side", counted, calls.Load(), failures.Load(), time.Since(started))
	reportAborts(t, "tenant_revoke_race/revoker_side", controlCounted, 1, 0, time.Since(started))
	t.Logf("R4_REALPATH shape=tenant_revoke_race stated_denials=%d", denials.Load())
	if aborted := counted.total(); aborted != 0 {
		t.Errorf("R4-OPEN-2 tenant_revoke_race: %d database aborts over the race (%d of them 40001): RevokeTenant commits inside the window of a live quota read, and no advisory lock it holds can exclude the reader, because advisory locks are invisible to the serialization graph",
			aborted, counted.serialization.Load())
	}
}

// TestDelegateRacingSubjectRevocationNeverExposesSerializationFailure is the
// control for the tenant-race shape above. RevokeSubject writes the very same
// range (internal/identity/revoke.go:113-135) and it is already guarded: it takes
// the EXCLUSIVE half of the subject-erasure advisory key that CreateRoot and
// Delegate hold in SHARED mode as their first statement (delegate.go:112,
// root.go:72). If this shape is clean while the tenant shape aborts, the fix is
// the missing half of that same lock pair - not a different quota data structure.
func TestDelegateRacingSubjectRevocationNeverExposesSerializationFailure(t *testing.T) {
	s, counted := tracedStoreRole(t, "delegate")
	control, controlCounted := tracedStoreRole(t, "revoke-subject")
	scope := uniqueScope(t)
	root := createRootForTest(t, s, scope, "root-task", "ctx-sracerace-"+sanitizeID(contracts.NewID()))

	const workers, rounds = 6, 25
	var calls, failures, denials atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		time.Sleep(150 * time.Millisecond)
		if err := control.RevokeSubject(ctx, scope.Tenant, scope.Subject); err != nil {
			t.Errorf("RevokeSubject: %v", err)
		}
		close(stop)
	}()

	started := time.Now()
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			for round := 0; round < rounds; round++ {
				select {
				case <-stop:
					return
				default:
				}
				calls.Add(1)
				_, err := s.Delegate(ctx, scope, root.Task, DelegateRequest{
					Task:         fmt.Sprintf("srace-w%d-r%d", worker, round),
					Context:      fmt.Sprintf("ctx-srace-w%d-r%d", worker, round),
					Deployment:   scope.Deployment,
					Capabilities: []string{"read"},
					TTL:          time.Hour,
					AllowSelf:    true,
				})
				if err == nil {
					continue
				}
				failures.Add(1)
				noRetryableSurface(t, "subject_revoke_race", err)
				if code := contracts.Code(err); code == contracts.ErrForbidden || code == contracts.ErrNotFound {
					denials.Add(1)
					continue
				}
				t.Errorf("subject_revoke_race: worker %d round %d: unexpected %v", worker, round, err)
				return
			}
		}(worker)
	}
	wg.Wait()
	reportAborts(t, "subject_revoke_race/delegate_side", counted, calls.Load(), failures.Load(), time.Since(started))
	reportAborts(t, "subject_revoke_race/revoker_side", controlCounted, 1, 0, time.Since(started))
	t.Logf("R4_REALPATH shape=subject_revoke_race stated_denials=%d", denials.Load())
	if aborted := counted.total(); aborted != 0 {
		t.Errorf("R4-OPEN-2 subject_revoke_race: %d database aborts over the race (%d of them 40001), and the revoker itself aborted %d times. The shared/exclusive advisory pair orders the two writers but not their snapshots, so it prevents interleaved writes without preventing a serialization abort - the measurement of the guard this service already trusts",
			aborted, counted.serialization.Load(), controlCounted.total())
	}
}

// TestConcurrentDelegatesOnPopulatedSubstrate separates a real predicate defect
// from a planner artifact of an almost empty table.
//
// The aborts the small shapes produce are attributed by statement, and every
// one of them is a predicate conflict: an INSERT into grants_child aborted by a
// concurrent quota count, an UPDATE of one root aborted by a concurrent
// findGrant. A range read only conflicts that widely when the plan scans more
// than the rows it needs - and on a table of a dozen rows PostgreSQL seq-scans
// regardless of the indexes, which makes the predicate cover the whole relation.
// Production does not look like that, so the same shapes are measured again on a
// populated substrate with ANALYZE'd statistics, and the plan is printed. An
// abort that survives a plan that uses an index is a defect; one that disappears
// with it is a fixture artifact, and the report has to say which it is.
func TestConcurrentDelegatesOnPopulatedSubstrate(t *testing.T) {
	s, counted := tracedStoreRole(t, "delegate-populated")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Noise: unrelated tenants, roots and scopes, enough rows that the planner
	// has a reason to use an index and enough statistics for it to know.
	const noise = 3000
	for batch := 0; batch < noise/100; batch++ {
		for i := 0; i < 100; i++ {
			n := batch*100 + i
			scopeID := fmt.Sprintf("noise%d", n)
			if _, err := s.pool.Exec(ctx, `
			    INSERT INTO ahp_identity.contexts (id, tenant, deployment, subject)
			    VALUES ($1, $2, $2, $3) ON CONFLICT (id) DO NOTHING`,
				"ctx-"+scopeID, "t-"+scopeID, "s-"+scopeID); err != nil {
				t.Fatalf("seed context: %v", err)
			}
			if _, err := s.pool.Exec(ctx, `
			    INSERT INTO ahp_identity.grants_root
			        (id, root, parent, tenant, deployment, subject, task, context_id, status, depth,
			         max_depth, max_fanout, max_active, body_digest, expires_at)
			    VALUES ($1, $1, '', $2, $2, $3, $1, $4, 'active', 0,
			            4, 1024, 100000, 'noise-digest', clock_timestamp() + interval '30 minutes')
			    ON CONFLICT (id) DO NOTHING`,
				"root-"+scopeID, "t-"+scopeID, "s-"+scopeID, "ctx-"+scopeID); err != nil {
				t.Fatalf("seed root: %v", err)
			}
			if _, err := s.pool.Exec(ctx, `
			    INSERT INTO ahp_identity.grants_child
			        (id, root, parent, tenant, deployment, subject, task, context_id, status, depth,
			         max_depth, max_fanout, max_active, body_digest, expires_at)
			    VALUES ($1, $2, $2, $3, $3, $4, $1, $5, 'active', 1,
			            4, 1024, 100000, 'noise-digest', clock_timestamp() + interval '30 minutes')
			    ON CONFLICT (id) DO NOTHING`,
				"child-"+scopeID, "root-"+scopeID, "t-"+scopeID, "s-"+scopeID, "ctx-"+scopeID); err != nil {
				t.Fatalf("seed child: %v", err)
			}
		}
	}
	if _, err := s.pool.Exec(ctx, `ANALYZE ahp_identity.grants_child; ANALYZE ahp_identity.grants_root; ANALYZE ahp_identity.contexts`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	// The plan the quota check actually gets on this substrate.
	rows, err := s.pool.Query(ctx, `
	    EXPLAIN
	    WITH RECURSIVE subtree(id) AS (
	        SELECT c.id FROM ahp_identity.grants_child c WHERE c.root = 'root-noise0'
	        UNION ALL
	        SELECT c.id FROM ahp_identity.grants_child c JOIN subtree h ON c.parent = h.id)
	    SELECT count(*) FROM ahp_identity.grants_child c JOIN subtree s ON s.id = c.id
	     WHERE c.status = 'active' AND clock_timestamp() < c.expires_at`)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	var planLines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			rows.Close()
			t.Fatalf("explain scan: %v", err)
		}
		planLines = append(planLines, strings.TrimSpace(line))
	}
	rows.Close()
	t.Logf("R4_QUOTA_PLAN_BEFORE_FIX %s", strings.Join(planLines, " | "))

	// The plan the fixed shape gets: liveness carried inside the recursive term,
	// so no second pass over grants_child is needed.
	rows2, err := s.pool.Query(ctx, `
	    EXPLAIN
	    WITH RECURSIVE subtree(id, live) AS (
	        SELECT c.id, (c.status = 'active' AND clock_timestamp() < c.expires_at)
	          FROM ahp_identity.grants_child c WHERE c.root = 'root-noise0'
	        UNION ALL
	        SELECT c.id, (c.status = 'active' AND clock_timestamp() < c.expires_at)
	          FROM ahp_identity.grants_child c JOIN subtree h ON c.parent = h.id)
	    SELECT count(*) FROM subtree WHERE live`)
	if err != nil {
		t.Fatalf("explain after: %v", err)
	}
	var afterLines []string
	for rows2.Next() {
		var line string
		if err := rows2.Scan(&line); err != nil {
			rows2.Close()
			t.Fatalf("explain after scan: %v", err)
		}
		afterLines = append(afterLines, strings.TrimSpace(line))
	}
	rows2.Close()
	t.Logf("R4_QUOTA_PLAN_AFTER_FIX %s", strings.Join(afterLines, " | "))

	// The shape the product runs now (D-ID-1 added DISTINCT over nodes). It must
	// keep the R4 property: a walk of one tree through grants_child_root_idx and
	// grants_child_parent_idx, never a Seq Scan of grants_child - under
	// SERIALIZABLE a scan is a predicate over everything it scanned, and that is
	// what used to make unrelated trees abort each other.
	rows3, err := s.pool.Query(ctx, `
	    EXPLAIN
	    WITH RECURSIVE subtree(id, live) AS (
	        SELECT c.id, (c.status = 'active' AND clock_timestamp() < c.expires_at)
	          FROM ahp_identity.grants_child c WHERE c.root = 'root-noise0'
	        UNION ALL
	        SELECT c.id, (c.status = 'active' AND clock_timestamp() < c.expires_at)
	          FROM ahp_identity.grants_child c JOIN subtree h ON c.parent = h.id)
	    SELECT (SELECT count(DISTINCT id) FILTER (WHERE live) FROM subtree)`)
	if err != nil {
		t.Fatalf("explain product shape: %v", err)
	}
	var productLines []string
	for rows3.Next() {
		var line string
		if err := rows3.Scan(&line); err != nil {
			rows3.Close()
			t.Fatalf("explain product scan: %v", err)
		}
		productLines = append(productLines, strings.TrimSpace(line))
	}
	rows3.Close()
	productPlan := strings.Join(productLines, " | ")
	t.Logf("R4_QUOTA_PLAN_PRODUCT_SHAPE %s", productPlan)
	if strings.Contains(productPlan, "Seq Scan on ahp_identity.grants_child") ||
		strings.Contains(productPlan, "Seq Scan on grants_child") {
		t.Errorf("R4 regression: the D-ID-1 quota count degraded to a scan of the whole grants_child table, which is a predicate over every tree: %s", productPlan)
	}

	scope := uniqueScope(t)
	sharedContext := "ctx-pop-" + sanitizeID(contracts.NewID())
	roots := []Grant{
		createRootForTest(t, s, scope, "root-a", sharedContext),
		createRootForTest(t, s, scope, "root-b", sharedContext),
	}

	const workers, rounds = 8, 20
	var calls, failures atomic.Int64
	started := time.Now()
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			parentTask := roots[worker%len(roots)].Task
			for round := 0; round < rounds; round++ {
				calls.Add(1)
				if _, err := s.Delegate(ctx, scope, parentTask, DelegateRequest{
					Task:         fmt.Sprintf("pop-w%d-r%d", worker, round),
					Context:      sharedContext,
					Deployment:   scope.Deployment,
					Capabilities: []string{"read"},
					TTL:          time.Hour,
					AllowSelf:    true,
				}); err != nil {
					failures.Add(1)
					noRetryableSurface(t, "populated_delegate", err)
					t.Errorf("populated_delegate: worker %d round %d: %v", worker, round, err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	reportAborts(t, "populated_delegate", counted, calls.Load(), failures.Load(), time.Since(started))
	if failures.Load() != 0 {
		t.Fatalf("populated_delegate: %d of %d delegations failed", failures.Load(), calls.Load())
	}
	assertJournalNeverUpdated(t, s, "populated_delegate")
	if aborted := counted.total(); aborted != 0 {
		t.Errorf("R4-OPEN-1 populated_delegate: %d database aborts over %d delegations on a populated substrate (%d of them 40001). The plan above shows the quota walk reaches only grants_child_root_idx and grants_child_parent_idx, so this is not a scan the indexes should have narrowed: it is the read of one tree racing sibling inserts of the same scope",
			aborted, calls.Load(), counted.serialization.Load())
	}
}

// assertJournalNeverUpdated asks PostgreSQL itself how many times the anchor
// table was updated since the statistics were last reset. The append-only
// rewrite is only real if that number stays zero under a workload that widens
// shared context journals, because the defect this work closes was one shared
// row under concurrent update. pg_stat_statements-free and durable: the counter
// comes from the database, not from a flag in the test.
func assertJournalNeverUpdated(t *testing.T, store *Store, shape string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Statistics views are per-database and cumulative, and the whole test
	// binary shares one database, so this assertion says what it measures: no
	// UPDATE of the anchor table has happened at any point in this run.
	_, _ = store.pool.Exec(ctx, `SELECT pg_stat_force_next_flush()`)
	var updated int64
	if err := store.pool.QueryRow(ctx, `
	    SELECT COALESCE(s.n_tup_upd, 0)
	      FROM pg_stat_user_tables s
	     WHERE s.relid = 'ahp_identity.contexts'::regclass`).Scan(&updated); err != nil {
		t.Fatalf("%s: read the contexts update counter: %v", shape, err)
	}
	if updated > 0 {
		t.Errorf("%s: ahp_identity.contexts has been UPDATEd %d times in this database; the context journal is supposed to be append-only, and one shared row under concurrent update is exactly the abort source R4 item 2 names",
			shape, updated)
		return
	}
	t.Logf("R4_JOURNAL shape=%s contexts_rows_updated=0 append_only_holds=true", shape)
}

// TestLegitimateDelegationIsNeverRefusedByRetryBudget is the zero-refusal form of
// the availability invariant: no legitimate delegation is refused for contention
// at all. It is RED today (R4-OPEN-1: the quota range read races sibling inserts
// of one scope, and 1 in ~600 delegations still exhausts any bounded replay), and
// it stays in this file so the default run is not asked to carry a permanent red.
// Run it with -tags "ahp_exec ahp_data state_integration r4_open_measure".
func TestLegitimateDelegationIsNeverRefusedByRetryBudget(t *testing.T) {
	s, counted := tracedStoreRole(t, "retry-budget-never")
	seedNoise(t, s, 1500)
	var calls, exhausted int64
	for phase := 0; phase < 3; phase++ {
		c, e, _ := runDelegationPhase(t, s, fmt.Sprintf("nb%d", phase), 8, 25)
		calls += c
		exhausted += e
	}
	if counted.total() == 0 {
		t.Fatalf("the load produced no serialization contention: the leg would pass for the wrong reason")
	}
	if exhausted != 0 {
		t.Fatalf("R4-OPEN-1: %d of %d legitimate delegations were refused by replay exhaustion (wire aborts %d). No replay budget fixes this: the quota count must stop racing sibling inserts, by variant (i) a per-root counter or variant (ii) a session-level root lock taken before the snapshot opens.",
			exhausted, calls, counted.total())
	}
}
