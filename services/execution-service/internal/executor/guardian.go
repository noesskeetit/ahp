package executor

import (
	"context"
	"encoding/json"

	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// artifactGuardian is the execution plane's independent tool-policy decision
// path. It consults internal/tools' Guardian contract shape and permits only
// the executor's own artifact-writing tool; every other tool is denied. Keeping
// this separate from the tool implementation means a tool can never self-approve.
type artifactGuardian struct{}

func (artifactGuardian) Decide(_ context.Context, _ contracts.Scope, req tools.GuardRequest) (tools.Decision, error) {
	if req.Tool == artifactToolName {
		return tools.Decision{Allow: true, Reason: "execution-plane artifact write", Class: "internal"}, nil
	}
	return tools.Decision{Allow: false, Reason: "tool not permitted for the canonical executor", Class: "deny"}, nil
}

// guardedExecutor enforces the guardian before delegating to the real tool
// executor, and records the canonical approval digest for audit.
type guardedExecutor struct {
	inner *artifactTool
	guard artifactGuardian
}

func (g *guardedExecutor) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	digest, err := tools.ApprovalDigest(name, args)
	if err != nil {
		return nil, err
	}
	scope := turnFromCtx(ctx).scope
	decision, err := g.guard.Decide(ctx, scope, tools.GuardRequest{
		Tool:          name,
		SideEffecting: true,
		Digest:        digest,
		Arguments:     args,
	})
	if err != nil {
		return nil, err
	}
	if !decision.Allow {
		return nil, contracts.NewError(contracts.ErrForbidden, "executor: tool denied by guardian: "+decision.Reason)
	}
	return g.inner.Execute(ctx, name, args)
}
