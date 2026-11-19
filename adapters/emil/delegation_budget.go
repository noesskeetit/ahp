package emil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"log/slog"
	"time"
)

// settleDelegationTimeout bounds the accounting write that returns a delegation's
// remainder. It is generous rather than instant because it crosses the same
// database a busy execution plane is billing generations against.
const settleDelegationTimeout = 10 * time.Second

// MetaDelegationRef is the A2A SendMessage metadata key carrying the reference
// under which the delegating run's budget reserve was taken. The PDF sends the
// labels and the remaining budget with the task, and a verifier has to be able to
// get from the child's task to the parent's budget node without reading either
// agent's memory: this key is that join, and the parent's ledger node is named by
// the same string.
const MetaDelegationRef = "ahp_delegation_ref"

// DelegationBudget is the host's budget tree, as the side that delegates needs it.
//
// The adapter does not know what a budget is -- it speaks A2A -- but it is the one
// place that knows a delegation is happening, so it is the one place that has to
// ask for the money before the child is admitted and report what the child spent
// afterwards. A host that wires no budget here keeps today's behavior exactly: the
// adapter forwards the run's own number and nothing is accounted.
//
// The product contract behind the interface (spec 1789-1793): a child task
// atomically reserves its share from the parent, the remainder returns when the
// child finishes, and an exhausted reserve is a REFUSAL -- never an overrun and
// never a quiet zero.
type DelegationBudget interface {
	// Reserve takes the child's share out of the delegating run's remainder and
	// reports the bound the child may be admitted with. It must refuse (non-nil
	// error) when there is nothing left to reserve.
	Reserve(ctx context.Context, ref string, requested int) (DelegationGrant, error)
	// Settle returns the unspent remainder and books the spent part. It runs after
	// the child reached a terminal state, or after a submission that never landed.
	Settle(ctx context.Context, grant DelegationGrant, out DelegationOutcome) error
}

// DelegationGrant is one taken reserve. Handle is the host's own ledger reference
// (row ids); the adapter carries it back untouched and never interprets it.
type DelegationGrant struct {
	Ref     string
	Granted int
	Handle  any
}

// DelegationOutcome is what became of one delegated task.
type DelegationOutcome struct {
	// ChildTaskID is the task id the child deployment minted ("" when the child
	// was never reached).
	ChildTaskID string
	// ReachedChild is false only when the submission itself failed: nothing could
	// have been spent, so the whole reserve goes back.
	ReachedChild bool
	// StepsConsumed and Reported carry the child's own report
	// (metadata steps_consumed). Reported=false means the child never said, which
	// is charged conservatively rather than as a convenient zero.
	StepsConsumed int
	Reported      bool
}

// NewDelegationRef names one logical delegation. The child's task id does not
// exist yet when the reserve has to be taken -- the child mints it when the
// submission lands -- so the hold is taken under this reference and bound to the
// real task at settlement. Random rather than derived: the Core loop's mutation
// journal already replays a retried tool call, so no two live delegations share a
// reference, and a derived name would only look deterministic while colliding
// across attempts of one run.
func NewDelegationRef() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A failing RNG is not a reason to delegate without a reference: the
		// caller refuses the delegation instead of taking an unaccountable hold.
		slog.Error("emil: delegation reference unavailable", "err", err)
		return ""
	}
	return "deleg-" + hex.EncodeToString(b[:])
}

// StepsConsumed reads the child's own step report off its task metadata.
//
// The second value is the honest one: the gateway journals steps_consumed only
// when the executor reported stats, and metadata that is absent must not be read
// as "the child did nothing" -- that reading would hand the parent's budget back
// to a child that may have spent all of it.
func (t *Task) StepsConsumed() (int, bool) {
	if t == nil || t.Metadata == nil {
		return 0, false
	}
	raw, ok := t.Metadata["steps_consumed"]
	if !ok {
		return 0, false
	}
	switch v := raw.(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	case int64:
		return int(v), true
	default:
		return 0, false
	}
}

// convergeDelegation tells the budget tree what the child did, so the unspent part
// of the grant returns to the run that made it.
//
// It runs under a DETACHED context with a deadline: the parent's context is very
// often already cancelled by the time this happens -- a subtree cancel is exactly
// how a delegated child gets stopped -- and an accounting write skipped because its
// context died would leave the whole grant held against a run that is already
// finished.
//
// A failure is logged at error level and deliberately not returned to the caller:
// the child's turn happened, its artifacts are real, and dropping a delivered
// result because the ledger was unreachable would trade a product outcome for an
// accounting warning. The unreturned grant stays visible -- the log names the
// reference and the amount -- and is correctable out-of-band.
func (a *Adapter) convergeDelegation(ctx context.Context, budget DelegationBudget, grant DelegationGrant, task *Task) {
	if budget == nil || task == nil || !IsTerminalState(task.Status.State) {
		return // Missing acknowledgement or a live child cannot release its reserve.
	}
	out := DelegationOutcome{ReachedChild: task != nil}
	if task != nil {
		out.ChildTaskID = task.ID
		if steps, ok := task.StepsConsumed(); ok {
			out.StepsConsumed, out.Reported = steps, true
		}
	}
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleDelegationTimeout)
	defer cancel()
	if err := budget.Settle(settleCtx, grant, out); err != nil {
		slog.Error("emil: delegation budget not converged - grant stays held",
			"delegation", grant.Ref, "child_task", out.ChildTaskID,
			"granted", grant.Granted, "err", err)
		return
	}
	slog.Info("emil: delegation budget converged",
		"delegation", grant.Ref, "child_task", out.ChildTaskID,
		"granted", grant.Granted, "spent", out.StepsConsumed, "child_reported", out.Reported)
}

// delegationRef is stable across replicas and restart for the same durable
// operation, but distinct across scopes and conversations.
func delegationRef(ra *RunAuthority) (string, error) {
	if ra == nil || ra.OperationKey == "" {
		if ra != nil && ra.Budget != nil {
			return "", fmt.Errorf("emil: budgeted delegation requires a durable operation key")
		}
		ref := NewDelegationRef()
		if ref == "" {
			return "", fmt.Errorf("emil: cannot allocate message identity")
		}
		return ref, nil
	}
	digest, err := contracts.Digest(struct {
		Scope                    contracts.Scope
		Context, Task, Operation string
	}{ra.Scope, ra.ContextID, ra.TaskID, ra.OperationKey})
	if err != nil {
		return "", err
	}
	return "deleg-" + digest, nil
}
