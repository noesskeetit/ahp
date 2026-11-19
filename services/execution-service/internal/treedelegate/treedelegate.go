// Package treedelegate accounts a DELEGATED child's step budget against the
// budget tree of the run that delegated it.
//
// The PDF does not let a delegation be budget-free. A stated budget is a reserve
// taken out of the remainder of the turn, the labels and the remaining budget
// travel with the task, every task below the root holds its budget reserve from
// its parent, reserves are atomic inside the Model GW, and the tree budget is
// AHP's -- not each deployment's own guess. Before this package the delegation
// path honoured none of that: adapters/emil forwarded the parent's own
// per-attempt bound verbatim, so a run admitted with 3 steps that spent all 3 and
// then delegated handed the child a bound of 3 as well, and one subject consumed
// 5 model steps under a budget of 3 (measured on the live chain). Two deployments
// each respecting their own number is not a tree.
//
// The arithmetic is deliberately trivial and deliberately separate from the
// ledger calls (see Remaining/Grant): the invariant the acceptance rule measures
// -- A_consumed + B_consumed <= A_budget -- has to be readable in one function,
// while the atomicity comes from the ledger it is applied through
// (internal/model.BudgetStore: Allocate locks the parent row FOR UPDATE and
// refuses a carve that does not fit). Step charges settle their exact admitted
// units; token-provider overruns are retained separately by the same ledger.
//
// Units: a node of this tree counts MODEL STEPS, one unit per consumed model
// turn. The token-denominated per-task root the execution plane bills generations
// to (internal/execbridge RootBudgetKey) is a different measure and is NOT reused
// here; mixing the two in one node would make both meaningless.
package treedelegate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	// RunStepBudgetKey is the idempotency key of the step-denominated root a
	// delegating run is accounted against. Root is idempotent per
	// (tenant,subject,deployment,task,key), so the same run re-reads the same
	// node after a crash instead of opening a fresh budget.
	RunStepBudgetKey = "treedelegate:run-steps"

	// stepKeyPrefix namespaces one run's own per-step charges inside its root.
	// A charge is keyed by the step index, so a replay of a physical attempt
	// dedupes instead of double-charging the tree.
	stepKeyPrefix = "core:step:"

	// childSpendKey is the single spend record of one carved child node.
	childSpendKey = "delegated-child-spend"

	// delegateKeyPrefix namespaces the carve of one delegated task inside the
	// parent's idempotency namespace.
	delegateKeyPrefix = "delegate:"
)

// ErrNoCapacity is the honest answer to "delegate now" when the run has no
// remainder left to reserve. It is a refusal, not a zero: a child admitted with a
// budget of nothing would either be born unable to take a step or be born
// unbudgeted, and the PDF forbids both readings.
var ErrNoCapacity = errors.New("treedelegate: no budget remainder left in the delegating run")

// taskTag names the ledger task a run's step tree hangs under. It is prefixed so
// a step tree can never collide with the token-denominated root the execution
// plane opens for the same task id.
func taskTag(taskID string) string { return "treedelegate-run:" + taskID }

// childTaskTag names the ledger task one delegated task is accounted under.
// Prefixing it with the delegating task keeps two parents that delegate to the
// same child deployment in separate idempotency namespaces.
func childTaskTag(parentTaskID, childTaskID string) string {
	return "treedelegate-child:" + parentTaskID + ":" + childTaskID
}

// StepKey is the idempotency key of one consumed model step of a run.
func StepKey(step int) string { return stepKeyPrefix + strconv.Itoa(step) }

// Remaining is what the run may still commit: its bound minus what it charged
// itself and minus what is already held for children.
func Remaining(limit, spent, reserved int64) int64 { return limit - spent - reserved }

// Grant is the whole policy of a delegated reserve: the child gets the smaller of
// what was asked for and what the delegating run has left, and nothing when there
// is nothing left.
//
// requested <= 0 means the caller named no number for the child; the remainder
// then is the answer, because a delegation cannot be worth more than the run that
// makes it has left. A negative remainder is reported as no capacity, never as a
// negative grant.
func Grant(limit, spent, reserved, requested int64) (int64, error) {
	if limit <= 0 {
		return 0, ErrNoCapacity
	}
	remaining := Remaining(limit, spent, reserved)
	if remaining <= 0 {
		return 0, ErrNoCapacity
	}
	if requested <= 0 || requested > remaining {
		return remaining, nil
	}
	return requested, nil
}

// Ledger builds delegation reserves over the model gateway's budget ledger.
type Ledger struct {
	budgets *model.BudgetStore
	log     *slog.Logger
}

// New returns a Ledger over an existing budget store. A nil store is a
// configuration error, reported at construction rather than at the first
// delegation, where it would look like a model problem.
func New(budgets *model.BudgetStore) (*Ledger, error) {
	if budgets == nil {
		return nil, errors.New("treedelegate: budget store required")
	}
	return &Ledger{budgets: budgets, log: slog.Default()}, nil
}

// Run is one delegating run's step tree: the scope it runs in, the task that
// names it, and the step bound its caller stated for it.
type Run struct {
	l     *Ledger
	scope contracts.Scope
	task  string
	bound int64

	// rootID caches the root row once opened, so a child carved under another
	// deployment (which Get cannot see from the parent's scope) is still findable
	// through the parent's own tree.
	rootID string
}

// ForRun binds a ledger to the run that is delegating. bound is the per-attempt
// step budget the CALLER stated for this run; a run whose caller stated nothing
// has no remainder of the caller's making and must not be handed one invented
// here.
func (l *Ledger) ForRun(scope contracts.Scope, taskID string, bound int64) (*Run, error) {
	if taskID == "" {
		return nil, errors.New("treedelegate: run task id required")
	}
	if bound <= 0 {
		return nil, fmt.Errorf("treedelegate: run %s has no caller-stated step budget to delegate from", taskID)
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	return &Run{l: l, scope: scope, task: taskID, bound: bound}, nil
}

// EnsureRoot opens (or replays) the run's step-denominated root. Exposed because
// a caller that only accounts its own steps must be able to do that without
// delegating anything.
func (r *Run) EnsureRoot(ctx context.Context) (model.Budget, error) {
	root, err := r.l.budgets.Root(ctx, r.scope, taskTag(r.task), RunStepBudgetKey, r.bound)
	if err != nil {
		return model.Budget{}, fmt.Errorf("treedelegate: step budget for run %s: %w", r.task, err)
	}
	r.rootID = root.ID
	return root, nil
}

// ChargeStep books ONE consumed model step of the run against its own tree.
//
// The charge is a reserve-and-settle under a per-step idempotency key, which is
// what makes it both atomic and convergent: the ledger refuses a charge that does
// not fit, and a replay of the same step index replays the same settlement instead
// of charging the tree twice.
func (r *Run) ChargeStep(ctx context.Context, step int) error {
	root, err := r.EnsureRoot(ctx)
	if err != nil {
		return err
	}
	key := StepKey(step)
	if _, err := r.l.budgets.Reserve(ctx, r.scope, root.ID, key, 1); err != nil {
		return fmt.Errorf("treedelegate: charge step %d of run %s: %w", step, r.task, err)
	}
	if _, err := r.l.budgets.Settle(ctx, r.scope, root.ID, key, 1); err != nil {
		return fmt.Errorf("treedelegate: settle step %d of run %s: %w", step, r.task, err)
	}
	return nil
}

// Reservation is what one delegated reserve granted, in the terms an operator and
// an out-of-band verifier need: the numbers, the ledger rows they moved between,
// and the reference the caller used for the delegation.
type Reservation struct {
	// Ref is the caller's reference for this logical delegation.
	Ref string
	// Granted is the bound the child is admitted with: min(requested, remainder).
	Granted int64
	// Requested is what the delegating run asked for on the child's behalf.
	Requested int64
	// RemainingBefore is the run's remainder before this carve.
	RemainingBefore int64
	// ParentBudgetID and GrantBudgetID are the ledger rows this reserve lives on:
	// the out-of-band handle. ahp_model.budgets and ahp_model.budget_reservations
	// say all of this again without any product face.
	ParentBudgetID string
	GrantBudgetID  string
}

// ReserveDelegation carves a child's bound out of the delegating run's tree.
//
// requested is the number the run would otherwise have forwarded for the child
// (today: its own bound); the grant is min(requested, remainder). The carve is the
// ledger's Allocate, which locks the parent row FOR UPDATE and admits the carve
// only if it fits, so two delegations racing inside one run cannot both spend the
// same remainder and a run cannot delegate past what it has left.
//
// ref names THIS logical delegation and is mandatory: the child's task id does not
// exist yet at this moment -- the child deployment mints it when the submission
// lands -- so the hold is taken under the caller's reference and bound to the real
// task only by SettleDelegation. A retry of the same ref replays the same hold
// instead of taking a second one.
func (r *Run) ReserveDelegation(ctx context.Context, childScope contracts.Scope, ref string, requested int64) (Reservation, error) {
	if ref == "" {
		return Reservation{}, errors.New("treedelegate: delegation reference required to carve a reserve")
	}
	root, err := r.EnsureRoot(ctx)
	if err != nil {
		return Reservation{}, err
	}
	// Recover the original carve before examining remainder. A lost ACK can
	// leave the whole parent budget reserved; that must not prevent reconciliation.
	snapshot, err := r.l.budgets.TreeSnapshot(ctx, r.scope, root.ID)
	if err != nil {
		return Reservation{}, err
	}
	for _, node := range snapshot.Nodes {
		if node.Parent == root.ID && node.Task == childTaskTag(r.task, ref) && node.Key == "alloc/"+root.ID+"/"+delegateKeyPrefix+ref {
			return Reservation{Ref: ref, Granted: node.Limit, Requested: requested,
				RemainingBefore: Remaining(root.Limit, root.Spent, root.Reserved), ParentBudgetID: root.ID, GrantBudgetID: node.ID}, nil
		}
	}
	granted, err := Grant(root.Limit, root.Spent, root.Reserved, requested)
	if err != nil {
		return Reservation{}, fmt.Errorf("treedelegate: run %s (bound %d, spent %d, held %d): %w",
			r.task, root.Limit, root.Spent, root.Reserved, err)
	}
	cscope := childScope
	if cscope.Validate() != nil {
		cscope = r.scope
	}
	grant, err := r.l.budgets.Allocate(ctx, r.scope, root.ID, cscope,
		childTaskTag(r.task, ref), delegateKeyPrefix+ref, granted)
	if err != nil {
		return Reservation{}, fmt.Errorf("treedelegate: carve reserve %s of run %s: %w", ref, r.task, err)
	}
	res := Reservation{
		Ref:             ref,
		Granted:         granted,
		Requested:       requested,
		RemainingBefore: Remaining(root.Limit, root.Spent, root.Reserved),
		ParentBudgetID:  root.ID,
		GrantBudgetID:   grant.ID,
	}
	// The audit line for one movement of budget: which tree it came out of, how
	// much of that tree was left, and which row holds it now. No secret is here,
	// and every number is readable again from ahp_model without the product.
	r.l.log.Info("treedelegate: reserve carved",
		"run_task", r.task, "delegation", ref,
		"run_bound", root.Limit, "run_spent", root.Spent, "run_held", root.Reserved,
		"requested", requested, "granted", granted,
		"parent_budget", root.ID, "grant_budget", grant.ID)
	return res, nil
}

// Outcome is what the delegated child actually did, as the tree is told about it.
type Outcome struct {
	// ChildTaskID is the task the child deployment minted. It names the ledger
	// node the child's spend is booked to, which is the join a verifier needs
	// between the A-side tree and the B-side task.
	ChildTaskID string
	// ReachedChild says the submission landed at all. A delegation that never
	// reached a child spent nothing and its whole hold goes back.
	ReachedChild bool
	// StepsConsumed is the child's own report (A2A metadata steps_consumed).
	StepsConsumed int64
	// Reported distinguishes "the child said zero" from "the child never said".
	Reported bool
}

// SettleDelegation converges one carve with what the child actually consumed.
//
// An unreported child is charged its FULL grant, because accounting fails closed:
// handing capacity back on silence would return budget a child may well have
// spent, which is the exact hole this package exists to close. A child that
// reported is charged what it reported, and the unused part of the grant returns
// to the run.
//
// The spend is booked on a node named by the REAL child task id, so the tree reads
// one node per delegated task. Close then propagates that spend into the grant and
// into the run's root and releases each hold on the way up, so the root ends at
// spent = own steps + child's actual steps with no hold left: one charge per unit
// of work, no double counting, and the tree cannot read the same step twice.
func (r *Run) SettleDelegation(ctx context.Context, res Reservation, out Outcome) error {
	if res.GrantBudgetID == "" {
		return errors.New("treedelegate: reservation has no grant budget to settle")
	}
	var actual int64
	switch {
	case !out.ReachedChild:
	case !out.Reported || out.StepsConsumed > res.Granted:
		actual = res.Granted
	default:
		actual = out.StepsConsumed
	}
	if actual > 0 {
		if out.ChildTaskID == "" {
			return errors.New("treedelegate: child task id required to book delegated spend")
		}
		grantScope, err := r.childScope(ctx, res.GrantBudgetID)
		if err != nil {
			return err
		}
		spend, err := r.l.budgets.Allocate(ctx, grantScope, res.GrantBudgetID, grantScope,
			childTaskTag(r.task, out.ChildTaskID), delegateKeyPrefix+out.ChildTaskID, actual)
		if err != nil {
			return fmt.Errorf("treedelegate: book child spend for %s under grant %s: %w",
				out.ChildTaskID, res.GrantBudgetID, err)
		}
		if _, err := r.l.budgets.Reserve(ctx, grantScope, spend.ID, childSpendKey, actual); err != nil {
			return fmt.Errorf("treedelegate: hold child spend on %s: %w", spend.ID, err)
		}
		if _, err := r.l.budgets.Settle(ctx, grantScope, spend.ID, childSpendKey, actual); err != nil {
			return fmt.Errorf("treedelegate: settle child spend on %s: %w", spend.ID, err)
		}
		if err := r.l.budgets.Close(ctx, grantScope, spend.ID); err != nil {
			return fmt.Errorf("treedelegate: close child spend %s: %w", spend.ID, err)
		}
	}
	if err := r.l.budgets.Close(ctx, r.scope, res.GrantBudgetID); err != nil {
		return fmt.Errorf("treedelegate: close grant budget %s: %w", res.GrantBudgetID, err)
	}
	r.l.log.Info("treedelegate: reserve settled",
		"run_task", r.task, "delegation", res.Ref, "child_task", out.ChildTaskID,
		"granted", res.Granted, "actual", actual, "reached_child", out.ReachedChild,
		"child_reported", out.Reported,
		"parent_budget", res.ParentBudgetID, "grant_budget", res.GrantBudgetID)
	return nil
}

// Snapshot is the out-of-band view of one run's step tree, for evidence harnesses
// and for this package's own tests: nodes plus totals, read fail-closed.
func (r *Run) Snapshot(ctx context.Context) (model.TreeSnapshot, error) {
	root, err := r.EnsureRoot(ctx)
	if err != nil {
		return model.TreeSnapshot{}, err
	}
	return r.l.budgets.TreeSnapshot(ctx, r.scope, root.ID)
}

// childScope reads the scope a carved child node lives in. Allocate may put a
// child under a different deployment than its parent, and every ledger write has
// to name the scope the row is actually under; the parent's tree is the read that
// spans deployments.
func (r *Run) childScope(ctx context.Context, childBudgetID string) (contracts.Scope, error) {
	child, err := r.l.budgets.Get(ctx, r.scope, childBudgetID)
	if err == nil {
		return child.Scope, nil
	}
	rootID := r.rootID
	if rootID == "" {
		root, rerr := r.EnsureRoot(ctx)
		if rerr != nil {
			return contracts.Scope{}, rerr
		}
		rootID = root.ID
	}
	snap, serr := r.l.budgets.TreeSnapshot(ctx, r.scope, rootID)
	if serr != nil {
		return contracts.Scope{}, fmt.Errorf("treedelegate: read child budget %s: %w (tree: %v)", childBudgetID, err, serr)
	}
	for _, n := range snap.Nodes {
		if n.ID == childBudgetID {
			return n.Scope, nil
		}
	}
	return contracts.Scope{}, fmt.Errorf("treedelegate: child budget %s not found in run %s tree", childBudgetID, r.task)
}

// LookupDelegation recovers an existing grant inside this run's tree without
// creating capacity. The HTTP owner uses this instead of trusting client budget IDs.
func (r *Run) LookupDelegation(ctx context.Context, ref string) (Reservation, error) {
	if ref == "" {
		return Reservation{}, contracts.NewError(contracts.ErrInvalid, "delegation reference required")
	}
	root, err := r.EnsureRoot(ctx)
	if err != nil {
		return Reservation{}, err
	}
	snapshot, err := r.l.budgets.TreeSnapshot(ctx, r.scope, root.ID)
	if err != nil {
		return Reservation{}, err
	}
	for _, node := range snapshot.Nodes {
		if node.Parent == root.ID && node.Task == childTaskTag(r.task, ref) && node.Key == "alloc/"+root.ID+"/"+delegateKeyPrefix+ref {
			return Reservation{Ref: ref, Granted: node.Limit, ParentBudgetID: root.ID, GrantBudgetID: node.ID}, nil
		}
	}
	return Reservation{}, contracts.NewError(contracts.ErrNotFound, "delegation grant not found")
}
