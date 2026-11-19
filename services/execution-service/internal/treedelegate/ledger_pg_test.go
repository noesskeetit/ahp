package treedelegate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// These run against the real Postgres the suite is configured with. The rules under
// test are about ATOMICITY and about what the ledger ends up holding, and neither
// of those is observable against a mock: a fake budget store would happily let two
// children spend the same unit.

func newLedger(t *testing.T) (*Ledger, *pgxpool.Pool, contracts.Scope) {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	store := model.NewBudgetStore(pool)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	l, err := New(store)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	// A tenant/subject of its own per test: the ledger is idempotent per
	// (tenant,subject,deployment,task,key), so isolation comes from the scope and
	// nothing a sibling test wrote can move these numbers.
	u := fmt.Sprint(time.Now().UnixNano())
	scope := contracts.Scope{Tenant: "ttdeleg" + u, Subject: "subj-" + u, Deployment: "dep-a"}
	return l, pool, scope
}

func mustRun(t *testing.T, l *Ledger, scope contracts.Scope, bound int64) *Run {
	t.Helper()
	r, err := l.ForRun(scope, "run-"+scope.Tenant, bound)
	if err != nil {
		t.Fatalf("ForRun: %v", err)
	}
	return r
}

func treeTotals(t *testing.T, r *Run) model.TreeTotals {
	t.Helper()
	snap, err := r.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	return snap.Totals
}

func rootBudget(t *testing.T, r *Run) model.Budget {
	t.Helper()
	b, err := r.EnsureRoot(context.Background())
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	return b
}

func charge(t *testing.T, r *Run, steps ...int) {
	t.Helper()
	for _, s := range steps {
		if err := r.ChargeStep(context.Background(), s); err != nil {
			t.Fatalf("charge step %d: %v", s, err)
		}
	}
}

func reserve(t *testing.T, r *Run, ref string, requested int64) Reservation {
	t.Helper()
	res, err := r.ReserveDelegation(context.Background(), contracts.Scope{}, ref, requested)
	if err != nil {
		t.Fatalf("reserve %s: %v", ref, err)
	}
	return res
}

func settle(t *testing.T, r *Run, res Reservation, childTask string, steps int64, reported bool) {
	t.Helper()
	err := r.SettleDelegation(context.Background(), res, Outcome{
		ChildTaskID:   childTask,
		ReachedChild:  true,
		StepsConsumed: steps,
		Reported:      reported,
	})
	if err != nil {
		t.Fatalf("settle %s: %v", res.Ref, err)
	}
}

// Criterion (1) atomicity + (4) parallel children cannot spend each other's
// reserve: N goroutines race for one unit of remainder. Exactly one wins. A reserve
// that is not a single locked transaction against the parent would let several
// through, and the tree would end up holding more than it was ever granted.
func TestConcurrentReservesCannotOverspendOneRemainder(t *testing.T) {
	ctx := context.Background()
	l, _, scope := newLedger(t)
	r := mustRun(t, l, scope, 1)

	const racers = 8
	var (
		wg      sync.WaitGroup
		granted int64
		refused int64
	)
	errs := make([]error, racers)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := r.ReserveDelegation(ctx, contracts.Scope{}, fmt.Sprintf("race-%d", i), 1)
			if err != nil {
				errs[i] = err
				atomic.AddInt64(&refused, 1)
				return
			}
			if res.Granted != 1 {
				errs[i] = fmt.Errorf("granted %d, want exactly 1", res.Granted)
			}
			atomic.AddInt64(&granted, 1)
		}(i)
	}
	close(start)
	wg.Wait()

	if granted != 1 {
		t.Fatalf("%d reservations took a remainder of 1 (refused %d): %v", granted, refused, errs)
	}
	if refused != racers-1 {
		t.Fatalf("expected %d refusals, got %d", racers-1, refused)
	}
	root := rootBudget(t, r)
	if root.Reserved != 1 || root.Spent != 0 {
		t.Fatalf("root after the race = spent %d reserved %d, want spent 0 reserved 1", root.Spent, root.Reserved)
	}
	if got := treeTotals(t, r).Committed; got > root.Limit {
		t.Fatalf("tree committed %d exceeds its limit %d", got, root.Limit)
	}
}

// Criterion (2) the remainder returns when the child finishes. The child was
// admitted with 4 of a run's 5 remaining steps and used 1; the three it did not
// use must be delegable again, and the tree must end at its own truth: one step by
// the run, one step by the child, no hold left.
func TestUnspentRemainderReturnsAfterTheChildFinishes(t *testing.T) {
	l, _, scope := newLedger(t)
	r := mustRun(t, l, scope, 5)

	charge(t, r, 0) // the run's own model step
	first := reserve(t, r, "deleg-first", 5)
	if first.Granted != 4 {
		t.Fatalf("first grant = %d, want the remainder 4", first.Granted)
	}
	if root := rootBudget(t, r); root.Reserved != 4 {
		t.Fatalf("root reserved %d while the child ran, want 4", root.Reserved)
	}
	settle(t, r, first, "child-task-1", 1, true)

	root := rootBudget(t, r)
	if root.Spent != 2 {
		t.Fatalf("root spent %d after the child used 1 of 4, want 2 (own step + child step)", root.Spent)
	}
	if root.Reserved != 0 {
		t.Fatalf("root still holds %d after the child finished; the remainder never came back", root.Reserved)
	}

	second := reserve(t, r, "deleg-second", 5)
	if second.Granted != 3 {
		t.Fatalf("second grant = %d, want the returned remainder 3", second.Granted)
	}
}

// Criterion (3) an exhausted reserve is a REFUSAL. Not a child admitted with a
// quiet zero, not an overrun: the delegation does not happen and no node is added.
func TestExhaustedRemainderRefusesTheDelegation(t *testing.T) {
	l, _, scope := newLedger(t)
	r := mustRun(t, l, scope, 3)
	charge(t, r, 0, 1, 2)

	before := treeTotals(t, r).Nodes
	res, err := r.ReserveDelegation(context.Background(), contracts.Scope{}, "deleg-denied", 3)
	if !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("reserve on an exhausted run = %+v, %v; want ErrNoCapacity", res, err)
	}
	if res.Granted != 0 {
		t.Fatalf("a refusal must grant nothing, granted %d", res.Granted)
	}
	if after := treeTotals(t, r).Nodes; after != before {
		t.Fatalf("a refused delegation still added a budget node (%d -> %d)", before, after)
	}
}

// Criterion (4) in the literal sense the spec uses it ("параллельные дети тратят
// только свой резерв"): two live children, each bounded by its own grant; the
// second child cannot reach the first one's unspent share, and while either is
// live the run cannot take another step from the money it lent out.
func TestParallelChildrenSpendOnlyTheirOwnReserve(t *testing.T) {
	l, _, scope := newLedger(t)
	r := mustRun(t, l, scope, 4)

	a := reserve(t, r, "deleg-a", 2) // asks for 2, gets 2
	b := reserve(t, r, "deleg-b", 5) // asks for everything left, gets what is left
	if a.Granted != 2 || b.Granted != 2 {
		t.Fatalf("grants = %d/%d, want 2/2", a.Granted, b.Granted)
	}
	if err := r.ChargeStep(context.Background(), 0); err == nil {
		t.Fatal("the run took a step out of budget it had already lent to two children")
	}
	settle(t, r, a, "child-a", 2, true) // A spends its whole reserve
	settle(t, r, b, "child-b", 1, true) // B spends one, returns one

	root := rootBudget(t, r)
	if root.Spent != 3 || root.Reserved != 0 {
		t.Fatalf("root = spent %d reserved %d, want spent 3 reserved 0", root.Spent, root.Reserved)
	}
	totals := treeTotals(t, r)
	if totals.Spent != 3 {
		t.Fatalf("tree spent = %d, want 3 (2 by child A, 1 by child B, charged once each)", totals.Spent)
	}
	if totals.Committed > root.Limit {
		t.Fatalf("tree committed %d above limit %d", totals.Committed, root.Limit)
	}
}

// No double charge: a retried physical attempt of one step is a replay, not a
// second unit of spend. Same rule for one child settled twice.
func TestReplayedStepAndReplayedSettlementDoNotDoubleCharge(t *testing.T) {
	l, _, scope := newLedger(t)
	r := mustRun(t, l, scope, 3)

	charge(t, r, 0)
	charge(t, r, 0) // the same step index again: a physical retry
	if root := rootBudget(t, r); root.Spent != 1 {
		t.Fatalf("root spent %d after replaying one step, want 1", root.Spent)
	}

	res := reserve(t, r, "deleg-replay", 3)
	settle(t, r, res, "child-replay", 2, true)
	if root := rootBudget(t, r); root.Spent != 3 || root.Reserved != 0 {
		t.Fatalf("root = spent %d reserved %d, want spent 3 reserved 0", root.Spent, root.Reserved)
	}
	// Converging the same child twice is a REPLAY, not a second charge: a process
	// that died between settling the tree and noticing its own success has to be
	// allowed to come back and do it again. What it must not be able to do is
	// re-decide the number.
	if err := r.SettleDelegation(context.Background(), res, Outcome{
		ChildTaskID: "child-replay", ReachedChild: true, StepsConsumed: 2, Reported: true,
	}); err != nil {
		t.Fatalf("replayed settlement errored, though a replay must be idempotent: %v", err)
	}
	if root := rootBudget(t, r); root.Spent != 3 || root.Reserved != 0 {
		t.Fatalf("root = spent %d reserved %d after a replayed settle, want 3/0 (no double charge)", root.Spent, root.Reserved)
	}
	if err := r.SettleDelegation(context.Background(), res, Outcome{
		ChildTaskID: "child-replay", ReachedChild: true, StepsConsumed: 1, Reported: true,
	}); err == nil {
		t.Fatal("the same child was settled with a DIFFERENT spend and the tree accepted it")
	}
	if root := rootBudget(t, r); root.Spent != 3 {
		t.Fatalf("root spent %d after a contradictory settle, want 3 (the refusal must not have moved anything)", root.Spent)
	}
}

// A child that never reported is charged its full grant. Reading silence as a zero
// would return budget a child may well have burned, which is the exact hole this
// package exists to close. A delegation that never reached a child at all returns
// everything: nothing was spent.
func TestSilentChildIsChargedItsGrantAndUnreachedChildReturnsEverything(t *testing.T) {
	l, _, scope := newLedger(t)
	r := mustRun(t, l, scope, 4)

	silent := reserve(t, r, "deleg-silent", 2)
	if err := r.SettleDelegation(context.Background(), silent, Outcome{
		ChildTaskID: "child-silent", ReachedChild: true, StepsConsumed: 0, Reported: false,
	}); err != nil {
		t.Fatalf("settle silent: %v", err)
	}
	if root := rootBudget(t, r); root.Spent != 2 || root.Reserved != 0 {
		t.Fatalf("root = spent %d reserved %d after an unreported child, want 2/0 (charged the full grant)", root.Spent, root.Reserved)
	}

	never := reserve(t, r, "deleg-never", 2)
	if err := r.SettleDelegation(context.Background(), never, Outcome{ReachedChild: false}); err != nil {
		t.Fatalf("settle unreached: %v", err)
	}
	if root := rootBudget(t, r); root.Spent != 2 || root.Reserved != 0 {
		t.Fatalf("root = spent %d reserved %d after a delegation that never landed, want 2/0 (whole reserve back)", root.Spent, root.Reserved)
	}
}

// Criterion (5) out-of-band readability: a verifier with only SQL -- no product
// face, no logs, no in-memory state -- must be able to see the run's bound, what it
// charged itself, which child task holds what, and that nothing is held after the
// child finished.
func TestDelegationIsAuditableWithPlainSQL(t *testing.T) {
	l, pool, scope := newLedger(t)
	r := mustRun(t, l, scope, 3)
	charge(t, r, 0)
	res := reserve(t, r, "deleg-sql", 3)
	settle(t, r, res, "child-visible-task", 2, true)

	var rootLimit, rootSpent, rootReserved int64
	if err := pool.QueryRow(context.Background(),
		`SELECT "limit", spent, reserved FROM ahp_model.budgets WHERE id=$1`, res.ParentBudgetID,
	).Scan(&rootLimit, &rootSpent, &rootReserved); err != nil {
		t.Fatalf("read root: %v", err)
	}
	if rootLimit != 3 || rootSpent != 3 || rootReserved != 0 {
		t.Fatalf("SQL root = limit %d spent %d reserved %d, want 3/3/0", rootLimit, rootSpent, rootReserved)
	}

	var childTask string
	var childSpent int64
	if err := pool.QueryRow(context.Background(),
		`SELECT task, spent FROM ahp_model.budgets WHERE parent=$1 AND closed`, res.GrantBudgetID,
	).Scan(&childTask, &childSpent); err != nil {
		t.Fatalf("read child node: %v", err)
	}
	if childSpent != 2 {
		t.Fatalf("child node spent %d, want 2", childSpent)
	}
	if want := "treedelegate-child:" + "run-" + scope.Tenant + ":child-visible-task"; childTask != want {
		t.Fatalf("child node is named %q, want it to name the delegated task (%q)", childTask, want)
	}

	var reservations, settled int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*), count(*) FILTER (WHERE settled) FROM ahp_model.budget_reservations
		  WHERE budget_id = ANY(ARRAY(
			SELECT id FROM ahp_model.budgets
			 WHERE id = $1 OR parent = $1 OR parent IN (SELECT id FROM ahp_model.budgets WHERE parent = $1)))`,
		res.ParentBudgetID,
	).Scan(&reservations, &settled); err != nil {
		t.Fatalf("read reservations: %v", err)
	}
	if reservations == 0 || settled != reservations {
		t.Fatalf("reservations = %d with %d settled; a verifier must see every charge settled, not held", reservations, settled)
	}
}

func TestLostAckReservationReplaysAfterProcessRestart(t *testing.T) {
	l, _, scope := newLedger(t)
	r := mustRun(t, l, scope, 3)
	first := reserve(t, r, "stable-op", 3)
	resumed := mustRun(t, l, scope, 3)
	replay, err := resumed.ReserveDelegation(context.Background(), contracts.Scope{}, "stable-op", 3)
	if err != nil || replay.GrantBudgetID != first.GrantBudgetID {
		t.Fatalf("held reservation cannot reconcile after restart: %+v %v", replay, err)
	}
	if got := rootBudget(t, resumed); got.Reserved != 3 || got.Spent != 0 {
		t.Fatalf("uncertain reserve changed: %+v", got)
	}
	settle(t, resumed, replay, "actual-child", 1, true)
	if got := rootBudget(t, resumed); got.Reserved != 0 || got.Spent != 1 {
		t.Fatalf("reconciled reserve wrong: %+v", got)
	}
}
