package main

import (
	"context"
	"encoding/json"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// The operator explicitly attests this image implements cmd/ahp-agent's State
// result contract. Arbitrary harness checkpoints and own_tables are unsupported.
type registryStateContract struct {
	Image    string `json:"image"`
	Protocol string `json:"protocol"`
}

const registryStateProtocol = "ahp-agent-state-v1"

type registryStateObserver struct {
	ids      *identity.Store
	registry *registry.Store
	state    *state.Store
	images   map[string]bool
}

func registryObservationAuthority(ids *identity.Store, deployments []string) transport.Authorize {
	allowed := map[string]bool{}
	for _, d := range deployments {
		allowed[d] = true
	}
	return func(ctx context.Context, c auth.Claims, capability string) error {
		if c.Kind == auth.KindTask && ids != nil {
			return ids.Authorize(ctx, c, capability)
		}
		deny := contracts.NewError(contracts.ErrForbidden, "registry owner history requires a live authorized human through an approved edge")
		if ids == nil || capability != "registry" || c.Kind != auth.KindHuman || c.JTI == "" || !allowed[c.Deployment] {
			return deny
		}
		session, err := ids.BrowserTokenActive(ctx, c.JTI)
		if err != nil || session.Tenant != c.Tenant || session.Subject != c.Subject {
			return deny
		}
		human, err := ids.BrowserUserBySubject(ctx, c.Tenant, c.Subject)
		if err != nil {
			return deny
		}
		for _, permission := range human.Permissions {
			if permission == "registry" {
				return nil
			}
		}
		return deny
	}
}
func (o registryStateObserver) ObserveOwned(ctx context.Context, owner contracts.Scope, config registry.EffectiveConfig) (json.RawMessage, error) {
	if owner.Tenant != config.Tenant || config.Task == "" {
		return nil, contracts.NewError(contracts.ErrNotFound, "invoked task not found")
	}
	b, err := o.registry.ResolveRuntime(ctx, owner.Tenant, config.Environment, config.AgentID, config.Digest)
	if err != nil {
		return nil, err
	}
	image := ""
	for _, component := range config.Manifest.Components {
		if component.A2A {
			image = component.Image
		}
	}
	if image == "" || image != b.Image {
		return nil, contracts.NewError(contracts.ErrConflict, "owner history runtime differs from pinned manifest")
	}
	if config.Manifest.State.OwnTables || !o.images[b.Image] {
		return nil, contracts.NewError(contracts.ErrUnavailable, "runtime has no declared ahp-agent-state-v1 owner history contract")
	}
	scope := owner
	scope.Deployment = b.Deployment
	// Get is deliberately a historical lookup, not Evaluate/MintTask: expired or
	// canceled execution authority cannot prevent the owner reading retained data.
	grantTask := config.GrantTask
	if grantTask == "" {
		grantTask = config.Task
	}
	grant, err := o.ids.Get(ctx, scope, grantTask)
	if err != nil {
		return nil, err
	}
	// A later independent task may restrict the same context without updating
	// this old grant's snapshot. Read the live monotonic journal on every request.
	contextLabels, found, err := o.ids.ContextLabels(ctx, scope, grant.Context)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, contracts.NewError(contracts.ErrForbidden, "owner history context label journal is missing")
	}
	effectiveLabels := labels.Merge(grant.Labels, contextLabels)
	// Source ownership is independent from permission to export to this sink.
	if err := labels.CheckWrite(effectiveLabels, labels.Audience{Kind: labels.KindPrivate, Subject: owner.Subject}); err != nil {
		return nil, contracts.NewError(contracts.ErrForbidden, "owner history labels do not admit this subject")
	}
	if len(effectiveLabels.Workspaces) > 0 && len(effectiveLabels.Subjects) == 0 {
		return nil, contracts.NewError(contracts.ErrForbidden, "workspace-restricted owner history requires a membership check")
	}
	thread, err := o.state.GetThreadByContext(ctx, scope, grant.Context)
	if err != nil {
		return nil, err
	}
	runs, err := o.state.ListRuns(ctx, scope, thread.ID)
	if err != nil {
		return nil, err
	}
	var selected *state.Run
	for i := range runs {
		if runs[i].TaskID == grantTask {
			if selected != nil {
				return nil, contracts.NewError(contracts.ErrConflict, "ambiguous State task result")
			}
			selected = &runs[i]
		}
	}
	if selected == nil {
		return nil, contracts.NewError(contracts.ErrNotFound, "invoked task result not found")
	}
	run := *selected
	var payload struct {
		Digest   string `json:"digest"`
		Revision int64  `json:"config_revision"`
	}
	if json.Unmarshal(run.Payload, &payload) != nil || payload.Digest != config.Digest || payload.Revision != config.ConfigRevision {
		return nil, contracts.NewError(contracts.ErrConflict, "State run differs from pinned runtime contract")
	}
	status := map[string]string{state.RunPending: "submitted", state.RunRunning: "working", state.RunDone: "completed", state.RunFailed: "failed", state.RunCanceled: "canceled"}[run.Status]
	if status == "" {
		return nil, contracts.NewError(contracts.ErrUnavailable, "unsupported State task status")
	}
	task := map[string]any{"id": config.Task, "kind": "task", "status": map[string]string{"state": status}, "metadata": map[string]any{"run_id": run.ID, "thread_id": thread.ID, "agent_id": config.AgentID, "version_digest": config.Digest, "image": b.Image}}
	if run.Status == state.RunDone {
		cp, err := o.state.GetCheckpoint(ctx, scope, thread.ID, run.ID, "")
		if err != nil {
			return nil, err
		}
		var meta struct {
			Image    string `json:"image"`
			Digest   string `json:"version_digest"`
			Revision int64  `json:"config_revision"`
		}
		if json.Unmarshal(cp.Metadata, &meta) != nil || meta.Image != b.Image || meta.Digest != config.Digest || meta.Revision != config.ConfigRevision {
			return nil, contracts.NewError(contracts.ErrConflict, "checkpoint differs from pinned runtime contract")
		}
		found := false
		for _, part := range cp.Parts {
			if part.Channel == "output" {
				data, err := o.state.GetPart(ctx, scope, thread.ID, cp.Namespace, part.Channel, part.Version)
				if err != nil {
					return nil, err
				}
				task["artifacts"] = []any{map[string]any{"artifactId": cp.ID, "parts": []any{map[string]any{"kind": "text", "text": string(data)}}}}
				found = true
			}
		}
		if !found {
			return nil, contracts.NewError(contracts.ErrUnavailable, "completed task has no output checkpoint part")
		}
	}
	return json.Marshal(task)
}
