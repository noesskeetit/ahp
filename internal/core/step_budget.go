package core

import (
	"context"
	"fmt"

	"github.com/ahp-platform/ahp/internal/space"
)

// StepAccountant is the seam between the bounded loop and the platform's budget
// tree. The loop owns the step bound; the tree owns the budget the bound was
// carved out of, and until now nothing connected them: a run could delegate its
// whole remainder to another deployment and then keep taking steps, because the
// steps only ever lived in this process's memory.
//
// ChargeStep is called once per consumed model step, with the request that is
// running, so the implementation can name the run the step belongs to without the
// loop having to know what a budget is. Returning an error stops the turn.
type StepAccountant interface {
	ChargeStep(ctx context.Context, req *space.CoreRequest, step int) error
}

// chargeStep books one consumed model step, if this deployment asked for it.
//
// The error text deliberately says "step limit": the close-reason dictionary the
// gateway publishes (internal/gateway/budget.go ClassifyCloseReason) reads that
// phrase to mean the budget ended the run rather than the model, and a tree
// refusal IS that fact. Reporting it as a generic error would file a budget
// exhaustion under "error" and send an operator looking for a broken provider.
func (r *Runtime) chargeStep(ctx context.Context, req *space.CoreRequest, step int) error {
	if r.cfg.StepAccountant == nil {
		return nil
	}
	if err := r.cfg.StepAccountant.ChargeStep(ctx, req, step); err != nil {
		return fmt.Errorf("model step limit %d reached: budget tree refused step %d: %w", req.MaxSteps, step, err)
	}
	return nil
}
