package main

import (
	"context"
	"fmt"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
)

// scopedCoreRunner gives all Core consumers the same verified workload scope.
// The coordinator's human scope remains unchanged; the State run ID is distinct
// from the grant task ID and is never rewritten from token claims.
type scopedCoreRunner struct {
	runner    space.CoreRunner
	authority runScopeAttester
}

func (r scopedCoreRunner) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	if req == nil || r.authority.verifier == nil || r.authority.workload == "" {
		return fmt.Errorf("Core requires verified run authority")
	}
	claims, err := r.authority.verifier.Verify(req.TaskToken, r.authority.taskAudience, auth.KindTask, r.authority.workload)
	if err != nil {
		return fmt.Errorf("Core run credential is not verifiable: %w", err)
	}
	if req.Scope.Tenant != claims.Tenant || req.Scope.Subject != claims.Subject || (req.Scope.Deployment != "" && req.Scope.Deployment != claims.Deployment) || (req.ContextID != "" && req.ContextID != claims.Context) {
		return fmt.Errorf("Core request differs from verified task authority")
	}
	copy := *req
	copy.Scope = claims.Scope()
	copy.ContextID = claims.Context
	return r.runner.Run(ctx, &copy, results)
}
