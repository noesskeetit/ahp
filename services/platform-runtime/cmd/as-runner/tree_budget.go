package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/modelapi"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// EnvTreeBudget turns on the platform budget tree for this runner: every model
// step a run consumes is charged against the run's own step budget in the Model
// Gateway's ledger, and every delegation reserves the child's bound out of the
// REMAINDER of that budget instead of re-announcing the parent's number.
//
// It is a flag rather than a default because it changes what number a delegated
// deployment receives: today a child of a 3-step run is admitted with 3 steps no
// matter what the parent already spent, and existing acceptance paths were
// measured against that. Enabling it is the difference between two deployments
// each respecting a number and one tree that cannot be overspent.
const EnvTreeBudget = "AHP_DELEGATE_TREE_BUDGET"

// treeBudget is this runner's handle on the platform's budget tree, seen through
// the two seams the product needs: the Core loop charges its steps here, and the
// emil adapter reserves a delegated child's bound here.
type treeBudget struct {
	client *modelapi.TreeBudgetClient
	// scope completes a run's scope from the run's own verified task token -- the
	// same attester the artifact sink uses, so accounting and artifacts can never
	// call two different runs by two different names (DEF-H10-9).
	scope runScopeAttester
}

// runTreeBudget is the process-wide budget tree, installed by main before the
// first run can be served. emilRunAuthority resolves a per-call authority out of a
// context and has no way to be handed one, which is why this is a package value
// rather than a parameter; it is written exactly once, at startup, and only read
// afterwards.
var runTreeBudget *treeBudget

// buildTreeBudget connects to the Model owner over the runner's workload mTLS.
func buildTreeBudget(logger *slog.Logger) (*treeBudget, error) {
	if !treeBudgetEnabled() {
		logger.Info("as-runner: delegate tree budget disabled")
		return nil, nil
	}
	origin := os.Getenv("AS_MODEL_URL")
	if origin == "" {
		return nil, fmt.Errorf("%s requires AS_MODEL_URL", EnvTreeBudget)
	}
	cfg := loadConfig()
	hc, err := space.NewMTLSClient(cfg.WorkloadCert, cfg.WorkloadKey, cfg.WorkloadCA)
	if err != nil {
		return nil, fmt.Errorf("as-runner: model budget mTLS: %w", err)
	}
	client, err := modelapi.NewTreeBudgetClient(origin, hc)
	if err != nil {
		return nil, err
	}
	att, err := buildRunScopeAttester()
	if err != nil {
		return nil, err
	}
	if !att.enabled() {
		return nil, fmt.Errorf("as-runner: budget tree requires task verification keys")
	}
	logger.Info("as-runner: delegate tree budget enabled through Model API")
	return &treeBudget{client: client, scope: att}, nil
}

func treeBudgetEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(EnvTreeBudget))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// ChargeStep implements core.StepAccountant: the loop says which step it consumed,
// the tree decides whether the run was allowed to take it.
//
// A run whose caller stated no bound is not accounted. The loop's own fallback
// ceiling is a fact about this process, not a budget the platform granted, and
// inventing a tree limit from it would report a caller's will that nobody expressed.
//
// The step is booked under the run's ATTESTED scope, not under whatever scope the
// request happened to carry: a browser turn carries the human principal's scope
// (no deployment), and filing a step there while the delegation leg files under the
// run's real scope splits one run across two trees. An unattestable scope refuses
// the step rather than guessing a label.
func (t *treeBudget) ChargeStep(ctx context.Context, req *space.CoreRequest, step int) error {
	if t == nil || req == nil || req.MaxSteps <= 0 {
		return nil
	}
	_, err := t.scope.complete(req.Scope, req.TaskToken)
	if err != nil {
		return fmt.Errorf("as-runner: cannot name the scope of run %s to charge step %d: %w", req.RunID, step, err)
	}
	if _, err := t.scope.attestedTask(req.TaskToken); err != nil {
		return err
	}
	return t.client.ForRun(req.TaskToken, req.RunID, int64(req.MaxSteps)).ChargeStep(ctx, step)
}

// ForRun gives the emil adapter the budget of one run, so a delegation of that run
// can only ever spend what that run has left.
//
// taskToken is the run's own task credential -- the bearer this very hop presents.
// It is what lets the parent's tree be found under the same scope the child will be
// accepted under; without it a browser-initiated parent would be looked up (and
// created) under a scope that names no deployment (DEF-H10-9).
func (t *treeBudget) ForRun(scope contracts.Scope, taskToken, taskID string, bound int) (emil.DelegationBudget, error) {
	if t == nil {
		return nil, errors.New("as-runner: no budget tree configured")
	}
	_, err := t.scope.complete(scope, taskToken)
	if err != nil {
		return nil, fmt.Errorf("as-runner: cannot name the scope of run %s to reserve a delegation: %w", taskID, err)
	}
	if _, err := t.scope.attestedTask(taskToken); err != nil {
		return nil, err
	}
	if bound <= 0 || taskID == "" {
		return nil, fmt.Errorf("as-runner: run and stated budget required")
	}
	return &delegationBudget{run: t.client.ForRun(taskToken, taskID, int64(bound))}, nil
}

// delegationBudget is the emil-facing view of one run's budget tree.
type delegationBudget struct {
	run *modelapi.TreeRun
}

// Reserve carves the child's bound out of the run's remainder. An exhausted
// remainder returns the ledger's refusal, and the adapter turns that into a
// refused delegation -- not a child admitted with a quiet zero.
func (d *delegationBudget) Reserve(ctx context.Context, ref string, requested int) (emil.DelegationGrant, error) {
	res, err := d.run.Reserve(ctx, ref, int64(requested))
	if err != nil {
		return emil.DelegationGrant{}, err
	}
	return emil.DelegationGrant{Ref: ref, Granted: int(res.Granted), Handle: res}, nil
}

// Settle converges the grant with what the child reported, returning the unspent
// remainder to the run.
func (d *delegationBudget) Settle(ctx context.Context, grant emil.DelegationGrant, out emil.DelegationOutcome) error {
	res, ok := grant.Handle.(modelapi.DelegationGrant)
	if !ok {
		return fmt.Errorf("as-runner: delegation grant %s carries an unexpected budget handle %T", grant.Ref, grant.Handle)
	}
	return d.run.Settle(ctx, res, modelapi.DelegationOutcome{
		ChildTaskID:   out.ChildTaskID,
		ReachedChild:  out.ReachedChild,
		StepsConsumed: int64(out.StepsConsumed),
		Reported:      out.Reported,
	})
}

// compile-time proof that the one seam the Core loop needs is actually implemented
// by the thing main hands it.
var _ core.StepAccountant = (*treeBudget)(nil)

// rejectedDelegationBudget prevents a configured tree from being bypassed when
// run authority cannot be attested. The adapter stops before dispatching a child.
type rejectedDelegationBudget struct{ err error }

func (d rejectedDelegationBudget) Reserve(context.Context, string, int) (emil.DelegationGrant, error) {
	return emil.DelegationGrant{}, d.err
}
func (d rejectedDelegationBudget) Settle(context.Context, emil.DelegationGrant, emil.DelegationOutcome) error {
	return d.err
}
