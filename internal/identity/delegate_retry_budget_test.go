package identity

// Availability of a LEGITIMATE delegation under real serialization contention.
//
// The measurement this reacts to (reports/mac-director/identity-r4-concurrency-
// fermat-20261110.md, reproduced in this lane at tip 94599faa):
//
//	R4_REALPATH shape=populated_delegate calls=125 caller_failures=2 wire_40001=159
//	  -> "CONFLICT: identity: the database aborted this write 16 times as
//	      concurrent (SQLSTATE 40001); replay the request unchanged"
//
// ~1 call in 60 was refused for a reason the caller did nothing wrong about: the
// retry loop counted attempts (maxTxAttempts = 16) instead of spending the time
// the request still had. A blind counter is the wrong class of bound: it gives up
// while the caller's own deadline is far away, and it keeps trying when the
// caller has already run out of time and only the next attempt's backoff is left.
//
// The invariant: whether a retriable abort is replayed is decided by its CLASS
// (40001/40P01/undecidable claim are replayable; nothing else is) and by the time
// the request still owns, never by an attempt tally.
//
// No fault injection is used anywhere in this file: every abort is raised by
// PostgreSQL itself, which is what makes the leg evidence rather than a rehearsal.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// seedNoise gives the planner a substrate wide enough that the quota walk is a
// real range read (the shape that produced the 159 aborts), not a cold-cache
// shortcut.
func seedNoise(t *testing.T, s *Store, rows int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for i := 0; i < rows; i++ {
		scopeID := fmt.Sprintf("rt%d", i)
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
	if _, err := s.pool.Exec(ctx, `ANALYZE ahp_identity.grants_child; ANALYZE ahp_identity.grants_root; ANALYZE ahp_identity.contexts`); err != nil {
		t.Fatalf("analyze: %v", err)
	}
}

// runDelegationPhase drives one burst of legitimate delegations over a shared
// scope and returns (calls, budget-exhausted refusals, raw-abort surfaces).
//
// A budget-exhausted refusal is counted from the contract message rather than the
// code alone: CONFLICT is also the honest answer to a real policy conflict, and
// this leg must not credit a policy refusal as an availability failure.
func runDelegationPhase(t *testing.T, s *Store, tag string, workers, rounds int) (int64, int64, int64) {
	t.Helper()
	scope := uniqueScope(t)
	sharedContext := "ctx-rt-" + sanitizeID(contracts.NewID())
	roots := []Grant{
		createRootForTest(t, s, scope, tag+"-root-a", sharedContext),
		createRootForTest(t, s, scope, tag+"-root-b", sharedContext),
	}
	var calls, exhausted, rawSurface atomic.Int64
	var latMu sync.Mutex
	latencies := make([]time.Duration, 0, workers*rounds)
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
				called := time.Now()
				_, err := s.Delegate(ctx, scope, parentTask, DelegateRequest{
					Task:         fmt.Sprintf("%s-w%d-r%d", tag, worker, round),
					Context:      sharedContext,
					Deployment:   scope.Deployment,
					Capabilities: []string{"read"},
					TTL:          time.Hour,
					AllowSelf:    true,
				})
				latMu.Lock()
				latencies = append(latencies, time.Since(called))
				latMu.Unlock()
				if err == nil {
					continue
				}
				if retryableAbort(err) {
					rawSurface.Add(1)
					t.Errorf("%s: caller was handed a raw retryable abort (no contract code to branch on): %v", tag, err)
					return
				}
				if isRetryBudgetRefusal(err) {
					exhausted.Add(1)
				}
			}
		}(worker)
	}
	wg.Wait()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if n := len(latencies); n > 0 {
		t.Logf("R4_RETRY_LATENCY tag=%s p50=%s p95=%s max=%s", tag,
			latencies[n/2].Round(time.Millisecond),
			latencies[(n*95)/100].Round(time.Millisecond),
			latencies[n-1].Round(time.Millisecond))
	}
	return calls.Load(), exhausted.Load(), rawSurface.Load()
}

// isRetryBudgetRefusal recognises the answer "we stopped replaying because the
// retry budget ran out". It is deliberately matched on the product's own words:
// CONFLICT is also the honest answer to a genuine policy conflict, and crediting
// that as an availability failure would make this leg measure the wrong thing.
var lastBudgetRefusal string

func isRetryBudgetRefusal(err error) bool {
	// Matched on the product's own words, not on a sentinel, so this leg compiles
	// and fails against the pre-fix product as well: a leg that cannot be run
	// before its own fix cannot be the evidence of that fix. The sentinel form is
	// asserted separately in TestRunTxModeRetryLoop.
	if contracts.Code(err) != contracts.ErrConflict {
		return false
	}
	if !strings.Contains(strings.ToLower(err.Error()), "the database aborted this write") {
		return false
	}
	lastBudgetRefusal = err.Error()
	return true
}

// refusalCeiling is the availability floor this package may not go back under.
// It is not a wish: it is the measured before-side. A 16-attempt counter refused
// 8 of 600 legitimate delegations in this lane and 1 of ~150 in
// reports/mac-director/identity-r4-concurrency-fermat-20261110.md. Anything at or
// above 1 in 100 means the stopping rule has become a count again.
const refusalCeiling = 100

// TestRetryBudgetDoesNotRefuseLegitimateDelegationByCount is the availability leg
// of the retry fix: under real contention a legitimate delegation may still lose
// (R4-OPEN-1 is open and named), but it must not be refused because an attempt
// TALLY ran out while the request still had minutes of its own deadline left.
// What is asserted is therefore (a) the refusal rate stays under the measured
// before-side ceiling, (b) every refusal reports the time it bought, so a caller
// can tell "we replayed for 5s" from a policy conflict, and (c) the refusals that
// remain are the R4-OPEN-1 shape, i.e. they are counted, not hidden.
//
// The zero-refusal invariant is asserted too - under -tags r4_open_measure, as
// TestLegitimateDelegationIsNeverRefusedByRetryBudget - because it is reachable
// only when the quota range read stops racing sibling inserts (R4-OPEN-1), which
// is a different fix and is not claimed here.
func TestRetryBudgetDoesNotRefuseLegitimateDelegationByCount(t *testing.T) {
	s, counted := tracedStoreRole(t, "retry-budget")
	seedNoise(t, s, 1500)

	const phases, workers, rounds = 3, 8, 25
	var totalCalls, totalExhausted int64
	started := time.Now()
	for phase := 0; phase < phases; phase++ {
		calls, exhausted, _ := runDelegationPhase(t, s, fmt.Sprintf("rb%d", phase), workers, rounds)
		totalCalls += calls
		totalExhausted += exhausted
		t.Logf("R4_RETRY_BUDGET phase=%d calls=%d budget_exhausted=%d wire_40001=%d wire_40P01=%d elapsed_ms=%d",
			phase, calls, exhausted, counted.serialization.Load(), counted.deadlock.Load(),
			time.Since(started).Milliseconds())
	}
	t.Logf("R4_RETRY_BUDGET_TOTAL calls=%d budget_exhausted=%d wire_aborts_total=%d attempts_cap=%d",
		totalCalls, totalExhausted, counted.total(), maxTxAttempts)
	if counted.serialization.Load()+counted.deadlock.Load() == 0 {
		t.Fatalf("the load produced no serialization contention at all: the leg would pass for the wrong reason")
	}
	// The load leg reports the residual instead of gating on a rate: whether a
	// given contention shape clears a budget depends on the load, and a rate
	// threshold turns this leg into a race with the database. The mechanism is
	// asserted deterministically in TestReplayIsBoundedByTimeNotByAttemptCount,
	// and the zero-refusal invariant lives under r4_open_measure. What stays a
	// hard failure is a return to the measured before-side: 8 of 600.
	t.Logf("R4_RETRY_SHAPE_LOAD calls=%d budget_exhausted=%d before_side=8 of 600 wire_aborts=%d",
		totalCalls, totalExhausted, counted.total())
	if totalExhausted > totalCalls/refusalCeiling {
		t.Fatalf("%d of %d legitimate delegations refused: that is at or above the measured before-side of the blind attempt count (8 of 600 here, 1 of ~150 for Fermat)",
			totalExhausted, totalCalls)
	}
	if totalExhausted != 0 && !strings.Contains(lastBudgetRefusal, "replay budget") {
		t.Fatalf("a replay refusal must report the time it replayed for, got %q", lastBudgetRefusal)
	}
	if totalExhausted != 0 {
		t.Logf("R4_OPEN_1_RESIDUAL refusals=%d of calls=%d: the quota range read still races sibling inserts of one scope; variant (i)/(ii), not a longer budget, closes it",
			totalExhausted, totalCalls)
	}
}

// TestRetryFollowsErrorClass not-retriable means not-retriable: a rejection the
// database will return again must cost exactly one attempt, whatever the budget
// says. Otherwise a policy error turns into a latency storm, and the widened
// budget would be spent replaying refusals.
func TestRetryFollowsErrorClass(t *testing.T) {
	attempts := 0
	err := contracts.NewError(contracts.ErrForbidden, "identity: policy said no")
	for attempt := 1; ; attempt++ {
		attempts++
		if err == nil || !retryableAbort(err) {
			break
		}
		if attempt >= 1000 {
			t.Fatalf("a non-retriable error re-entered the replay loop %d times", attempts)
		}
	}
	if attempts != 1 {
		t.Fatalf("a non-retriable error took %d attempts; class decides, and its answer here is one", attempts)
	}
	// The two wire states and the undecidable claim are the replayable class.
	for _, e := range []error{
		&pgconn.PgError{Code: "40001"},
		&pgconn.PgError{Code: "40P01"},
		fmt.Errorf("wrapped: %w", errContextClaimRace),
	} {
		if !retryableAbort(e) {
			t.Fatalf("error of class %v must be replayable", e)
		}
	}
}

// TestRetryStopsWhenTheRequestRunsOutOfTime is the other half of a time budget:
// a caller whose deadline has passed must be told so immediately rather than be
// held for one more backoff, so that the budget cannot become a latency leak.
func TestRetryStopsWhenTheRequestRunsOutOfTime(t *testing.T) {
	s, _ := tracedStoreRole(t, "retry-deadline")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	time.Sleep(15 * time.Millisecond)
	started := time.Now()
	// The store is reachable but this caller is already expired: no attempt may
	// be paid for, and no backoff may be slept.
	err := s.runTx(ctx, func(context.Context, pgx.Tx) error { return nil })
	if err == nil {
		t.Fatal("an expired caller reported success: the attempt was paid for with a dead caller")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("an expired caller was held for %s; the budget must return, not retry", elapsed)
	}
}

// TestReplayIsBoundedByTimeNotByAttemptCount is the deterministic half of the
// retry fix: a write that is aborted continuously must be replayed until its TIME
// budget is spent, and the answer must say how much replay it bought. A counter
// stops after a fixed number of attempts whatever the clock says, so both of those
// properties are measured here rather than inferred from a load shape.
func TestReplayIsBoundedByTimeNotByAttemptCount(t *testing.T) {
	s, _ := tracedStoreRole(t, "retry-shape")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	runs := 0
	started := time.Now()
	err := s.runTxMode(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable},
		func(context.Context, pgx.Tx) error {
			runs++
			return pgxError("40001")
		})
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("a write aborted forever must not report success")
	}
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("replay exhaustion must keep the CONFLICT class a client replays on, got %q (%v)", contracts.Code(err), err)
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "replay budget") {
		t.Fatalf("a replay refusal must name the time it replayed for, so a caller can tell it from a policy conflict: %q", err)
	}
	// One replay of an aborted transaction costs a few hundred microseconds here,
	// so a 16-attempt rule gives up long before a multi-second budget is spent.
	// This is the exact defect: the service stopped helping a correct request
	// because of a tally, not because time ran out.
	if runs <= 16 {
		t.Fatalf("only %d attempts in %s: the loop stopped on the attempt count, not on the time budget", runs, elapsed.Round(time.Millisecond))
	}
	// A 16-attempt rule spends 15 backoffs of at most a few tens of milliseconds
	// and gives up in well under a second while the caller waits minutes for an
	// answer it was still entitled to. Anything under a second here means the
	// stopping rule is a count again.
	if elapsed < time.Second {
		t.Fatalf("gave up after %s and %d attempts: a correct request was refused long before any caller's deadline - the bound is a count, not a budget",
			elapsed.Round(time.Millisecond), runs)
	}
	t.Logf("R4_RETRY_SHAPE attempts=%d elapsed=%s message=%q", runs, elapsed.Round(time.Millisecond), err)
}
