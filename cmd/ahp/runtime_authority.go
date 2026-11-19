package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The operator configuration locates runtimes; a tenant request can select only
// an installation or an evaluation suite, never a URL, image or grant ceiling.
type runtimeAuthority struct {
	budgets interface {
		TaskBudget(context.Context, contracts.Scope, string) (model.Budget, error)
	}
	ids      *identity.Store
	registry *registry.Store
	verifier *auth.Verifier
	signer   *auth.Signer
}

func readOperatorJSON(path string, out any) error {
	if path == "" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open operator configuration: %w", err)
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	return d.Decode(out)
}

func manifestTarget(tenant, deployment string, m registry.Manifest, caps registry.Capabilities) (identity.DelegationTarget, error) {
	// State and task cancellation are the platform execution contract. Optional
	// service permissions follow the installation's effective manifest ceiling.
	permissions := []string{"state", "execution", "identity:cancel"}
	if len(caps.Models) > 0 {
		permissions = append(permissions, "model")
	}
	if len(caps.Tools) > 0 {
		permissions = append(permissions, "tools")
	}
	if len(caps.Memory) > 0 {
		permissions = append(permissions, "memory")
	}
	if len(caps.Compute) > 0 {
		permissions = append(permissions, "compute")
	}
	if len(caps.Calls) > 0 || caps.SelfTasks > 0 {
		permissions = append(permissions, "identity:delegate")
	}
	return identity.DelegationTarget{Tenant: tenant, Deployment: deployment, Capabilities: permissions,
		MaxDepth: m.Limits.Depth, MaxActive: m.Limits.ActiveTasks, AllowSelf: caps.SelfTasks > 0}, nil
}

func (a runtimeAuthority) resolve(ctx context.Context, parent contracts.Scope, deployment string) (identity.DelegationTarget, error) {
	target, err := a.registry.FindInstalledTarget(ctx, parent.Tenant, deployment)
	if err != nil {
		return identity.DelegationTarget{}, err
	}
	return manifestTarget(parent.Tenant, deployment, target.Version.Manifest, target.Installation.Capabilities)
}

func (a runtimeAuthority) parent(ctx context.Context, scope contracts.Scope, token string) (identity.Grant, error) {
	c, err := a.verifier.Verify(token, "ahp", auth.KindTask, auth.WorkloadID(scope))
	if err != nil {
		return identity.Grant{}, err
	}
	if c.Scope() != scope {
		return identity.Grant{}, contracts.NewError(contracts.ErrForbidden, "runtime: parent scope mismatch")
	}
	if err := a.ids.Authorize(ctx, c, "identity:delegate"); err != nil {
		return identity.Grant{}, err
	}
	return a.ids.Evaluate(ctx, scope, c.Task, "identity:delegate")
}

func (a runtimeAuthority) issue(ctx context.Context, scope contracts.Scope, token, task string, target identity.DelegationTarget, observe bool) (string, error) {
	return a.issueWithContext(ctx, scope, token, task, target, observe, "", "")
}

func (a runtimeAuthority) issueWithContext(ctx context.Context, scope contracts.Scope, token, task string, target identity.DelegationTarget, observe bool, conversation, installation string) (string, error) {
	parent, err := a.parent(ctx, scope, token)
	if err != nil {
		return "", err
	}
	childScope := scope
	childScope.Deployment = target.Deployment
	childContext := contracts.OperationKey(childScope, task, "runtime.context", "v1")
	if conversation != "" {
		// An internal caller cannot widen this into an arbitrary context. The
		// native context is an alias of this verified parent's conversation and
		// selected installation, stable across its separate per-turn grants.
		want := contracts.OperationKey(scope, parent.Context, "registry.agent-context", installation)
		if installation == "" || conversation != want {
			return "", contracts.NewError(contracts.ErrForbidden, "runtime: target conversation differs from the authenticated parent")
		}
		childContext = conversation
	}
	if observe {
		child, err := a.ids.Get(ctx, childScope, task)
		if err != nil {
			return "", err
		}
		if child.Parent != parent.ID || child.Root != parent.Root {
			return "", contracts.NewError(contracts.ErrForbidden, "runtime: observation requires the original parent grant")
		}
		if conversation != "" && child.Context != childContext {
			return "", contracts.NewError(contracts.ErrForbidden, "runtime: existing task belongs to another native conversation")
		}
	} else {
		if _, err = a.ids.DelegateToTarget(ctx, scope, parent.Task, identity.DelegateRequest{
			Task: task, Context: childContext, Deployment: target.Deployment,
			Capabilities: target.Capabilities, TTL: 10 * time.Minute,
		}, target); err != nil {
			return "", err
		}
	}
	if !observe && a.budgets != nil {
		if _, err := a.budgets.TaskBudget(ctx, childScope, task); err != nil {
			return "", err
		}
	}
	return a.ids.MintTask(ctx, childScope, task, a.signer, "ahp", 0)
}

func (a runtimeAuthority) IssueRuntimeToken(ctx context.Context, req registry.InvokeRequest, b registry.RuntimeBinding) (string, error) {
	v, err := a.registry.GetVersion(ctx, req.VersionID)
	if err != nil {
		return "", err
	}
	if v.Digest != b.Digest || b.Tenant != req.Scope.Tenant || req.VersionDigest != b.Digest {
		return "", contracts.NewError(contracts.ErrForbidden, "runtime: pinned version differs from target")
	}
	target, err := manifestTarget(b.Tenant, b.Deployment, v.Manifest, req.Capabilities)
	if err != nil {
		return "", err
	}
	return a.issueWithContext(ctx, req.Scope, req.Token, req.TaskID, target, req.Observe, req.ConversationContext, req.InstallationID)
}

func (a runtimeAuthority) IssueEvalToken(ctx context.Context, req evals.RunRequest, t evals.Target) (string, error) {
	var m registry.Manifest
	if err := json.Unmarshal(t.Manifest, &m); err != nil {
		return "", err
	}
	target, err := manifestTarget(req.Scope.Tenant, t.Deployment, m, m.Capabilities)
	if err != nil {
		return "", err
	}
	// The Runner supplies the target from its immutable operator suite, already
	// checked against both the candidate digest and image before this call.
	return a.issue(ctx, req.Scope, req.Token, req.TaskID, target, false)
}
