package main

import (
	"context"
	"encoding/json"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func (c *stateDeletionCoordinator) ControlDeleteTenant(ctx context.Context, scope contracts.Scope, key string) (map[string]any, error) {
	principal, ok := transport.FromContext(ctx)
	if !ok || principal.Scope != scope || principal.Claims.Kind != auth.KindHuman {
		return nil, contracts.NewError(contracts.ErrForbidden, "ahp: verified tenant administrator required")
	}
	if c.control.ConsumerContext == nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "ahp: tenant erasure requires a durable service consumer")
	}
	if err := c.checkTenantOwners(); err != nil {
		return nil, err
	}
	if key == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "ahp: deletion key required")
	}
	if err := c.Ready(ctx, scope, "DeleteSubject"); err != nil {
		return nil, err
	}
	owners, ok := c.cascade.(*memoryAwareCascade)
	if !ok {
		return nil, contracts.NewError(contracts.ErrUnavailable, "ahp: tenant owners unavailable")
	}
	_, _, ws, err := owners.tenantOwners()
	if err != nil {
		return nil, err
	}
	if err = ws.AuthorizeTenantDeletion(ctx, scope); err != nil {
		return nil, err
	}
	release, err := c.state.LockTenantControl(ctx, scope.Tenant)
	if err != nil {
		return nil, err
	}
	defer release()
	job, err := c.state.GetControlDeletion(ctx, scope.Tenant, principal.Claims.Subject, key)
	if contracts.Code(err) == contracts.ErrNotFound {
		plan := subjectPlan{Scope: scope}
		plan.Held, err = c.state.TenantHeld(ctx, scope.Tenant)
		if err != nil {
			return nil, err
		}
		// Preflight hook configuration before revocation. The definitive catalog
		// and data inventory are frozen and persisted after every owner barrier.
		if !plan.Held {
			plan.Hooks, err = c.tenantHooks(ctx, scope.Tenant, nil)
			if err != nil {
				return nil, err
			}
		}
		raw, _ := json.Marshal(plan)
		job, err = c.state.BeginTenantControlDeletion(ctx, scope, principal.Claims.Subject, key, raw)
	}
	if err != nil {
		return nil, err
	}
	if job.Kind != "tenant" {
		return nil, contracts.NewError(contracts.ErrKeyReuse, "ahp: deletion key belongs to another operation")
	}
	return c.continueControlTenant(withDeletionAuthority(ctx, job), scope, job)
}

func (c *stateDeletionCoordinator) tenantHooks(ctx context.Context, tenant string, prior []plannedHook) ([]plannedHook, error) {
	targets, err := registry.New(c.state.Pool()).DeletionTargets(ctx, tenant)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, hook := range prior {
		seen[hook.Target.Installation+"/"+hook.Target.Version] = true
	}
	for _, target := range targets {
		if seen[target.Installation+"/"+target.Version] {
			continue
		}
		if c.control.ResolveHook == nil || !hasControlClientCertificate(c.control.HTTPClient) {
			return nil, contracts.NewError(contracts.ErrUnavailable, "ahp: tenant hook requires runtime resolver and mTLS")
		}
		endpoint, err := c.control.ResolveHook(ctx, tenant, target)
		if err != nil {
			return nil, err
		}
		url, err := deletionHookURL(endpoint, target.Hook)
		if err != nil {
			return nil, err
		}
		prior = append(prior, plannedHook{Target: target, URL: url})
	}
	return prior, nil
}
func (c *stateDeletionCoordinator) continueControlTenant(ctx context.Context, scope contracts.Scope, job state.ControlDeletion) (map[string]any, error) {
	if job.Kind != "tenant" || job.Tenant != scope.Tenant || !job.ServiceAuthorized || !permitsDeletion(ctx, scope) {
		return nil, contracts.NewError(contracts.ErrForbidden, "ahp: tenant deletion authority mismatch")
	}
	var plan subjectPlan
	if err := json.Unmarshal(job.Plan, &plan); err != nil {
		return nil, err
	}
	done := map[string]bool{}
	for _, step := range job.Steps {
		done[step] = true
	}
	result := func(status string) map[string]any {
		return map[string]any{"deletion_id": job.ID, "state": status, "scope_count": len(plan.Scopes), "hook_count": len(plan.Hooks), "retained_by_hold": plan.Held}
	}
	status := func() string {
		if plan.Held {
			return "completed_with_holds"
		}
		return "completed"
	}
	if job.Completed {
		return result(status()), nil
	}
	pending := func(err error) (map[string]any, error) {
		r := result("pending")
		r["code"] = contracts.ErrUnavailable
		r["message"] = "tenant deletion is pending; durable consumer will retry"
		return r, err
	}
	apply := func(step string, fn func() error) error {
		if done[step] {
			return nil
		}
		if err := fn(); err != nil {
			return err
		}
		if err := c.state.AckControlDeletion(ctx, job, step, false); err != nil {
			return err
		}
		done[step] = true
		return nil
	}
	owners, ok := c.cascade.(*memoryAwareCascade)
	if !ok {
		return pending(contracts.NewError(contracts.ErrUnavailable, "ahp: tenant owners unavailable"))
	}
	art, mem, ws, err := owners.tenantOwners()
	if err != nil {
		return pending(err)
	}
	held, err := c.state.TenantHeld(ctx, scope.Tenant)
	if err != nil {
		return pending(err)
	}
	plan.Held = plan.Held || held
	stages := []struct {
		name string
		run  func() error
	}{
		{"identity", func() error { return identity.New(c.state.Pool()).RevokeTenant(ctx, scope.Tenant) }},
		{"registry-fence", func() error { return registry.New(c.state.Pool()).FenceTenant(ctx, scope.Tenant) }},
		{"state-fence", func() error { return c.state.FenceTenant(ctx, scope.Tenant) }},
		{"artifacts-fence", func() error { return art.fenceTenant(ctx, scope) }},
		{"memory-fence", func() error { return mem.FenceTenant(ctx, scope) }},
		{"workspace-fence", func() error { return ws.FenceTenant(ctx, scope) }},
	}
	for _, step := range stages {
		if err = apply(step.name, step.run); err != nil {
			return pending(err)
		}
	}
	if err = c.checkTenantOwners(); err != nil {
		return pending(err)
	}
	for _, name := range requiredTenantOwners {
		if err = apply(name+"-fence", func() error { return c.control.TenantOwners[name].FenceTenant(ctx, scope.Tenant) }); err != nil {
			return pending(err)
		}
	}
	if !done["inventory"] {
		stateScopes, err := c.state.TenantScopes(ctx, scope.Tenant)
		if err != nil {
			return pending(err)
		}
		artScopes, err := art.tenantScopes(ctx, scope)
		if err != nil {
			return pending(err)
		}
		memoryScopes, err := mem.TenantScopes(ctx, scope)
		if err != nil {
			return pending(err)
		}
		seen := map[contracts.Scope]bool{}
		for _, s := range plan.Scopes {
			seen[s] = true
		}
		for _, s := range append(append(stateScopes, artScopes...), memoryScopes...) {
			if !seen[s] {
				seen[s] = true
				plan.Scopes = append(plan.Scopes, s)
			}
		}
		if !plan.Held {
			plan.Hooks, err = c.tenantHooks(ctx, scope.Tenant, plan.Hooks)
			if err != nil {
				return pending(err)
			}
		}
		raw, _ := json.Marshal(plan)
		if err = c.state.UpdateControlDeletionPlan(ctx, job, raw); err != nil {
			return pending(err)
		}
		if err = apply("inventory", func() error { return nil }); err != nil {
			return pending(err)
		}
	}
	if err = c.state.TenantEffectsSettled(ctx, scope.Tenant); err != nil {
		return pending(err)
	}
	if !plan.Held {
		for _, name := range requiredTenantOwners {
			if err = apply(name+"-delete", func() error { return c.control.TenantOwners[name].DeleteTenant(ctx, scope.Tenant) }); err != nil {
				return pending(err)
			}
		}
	}
	// Conservative retention: any legal hold preserves shared workspace data,
	// Memory and own_tables (those owners cannot classify held thread contents).
	if !plan.Held {
		if err = apply("workspace", func() error { return ws.DeleteTenant(ctx, scope) }); err != nil {
			return pending(err)
		}
		subjects := map[string]bool{}
		for _, target := range plan.Scopes {
			if subjects[target.Subject] {
				continue
			}
			subjects[target.Subject] = true
			if err = apply("memory:"+target.Subject, func() error {
				_, e := mem.DeleteSubject(ctx, target, target.Subject, "tenant-"+shortDeletionID(job.ID+"/"+target.Subject))
				return e
			}); err != nil {
				return pending(err)
			}
		}
	}
	for _, target := range plan.Scopes {
		step := "scope:" + target.Deployment + ":" + target.Subject
		if err = apply(step, func() error {
			held, e := c.deleteTenantScope(ctx, job.ID, target)
			if held {
				plan.Held = true
				raw, _ := json.Marshal(plan)
				if pe := c.state.UpdateControlDeletionPlan(ctx, job, raw); pe != nil {
					return pe
				}
			}
			return e
		}); err != nil {
			return pending(err)
		}
	}
	if !plan.Held {
		for _, hook := range plan.Hooks {
			if err = apply("hook:"+hook.Target.Installation+":"+hook.Target.Version, func() error { return c.invokeDeletionHook(ctx, contracts.Scope{Tenant: scope.Tenant}, job.ID, hook) }); err != nil {
				return pending(err)
			}
		}
	}
	if !plan.Held {
		if err = apply("registry-delete", func() error { return registry.New(c.state.Pool()).DeleteTenant(ctx, scope.Tenant) }); err != nil {
			return pending(err)
		}
		if err = apply("identity-delete", func() error { return identity.New(c.state.Pool()).DeleteTenant(ctx, scope.Tenant) }); err != nil {
			return pending(err)
		}
	}
	// Persist a newly observed hold even when the inventory had already committed.
	raw, _ := json.Marshal(plan)
	if err = c.state.UpdateControlDeletionPlan(ctx, job, raw); err != nil {
		return pending(err)
	}
	if err = c.state.AckControlDeletion(ctx, job, "complete", true); err != nil {
		return pending(err)
	}
	return result(status()), nil
}
func (c *stateDeletionCoordinator) deleteTenantScope(ctx context.Context, jobID string, target contracts.Scope) (bool, error) {
	key := "tenant-" + shortDeletionID(jobID+"/"+target.Deployment+"/"+target.Subject)
	if _, err := c.state.DeleteSubjectWithKey(ctx, target, key); err != nil {
		return false, err
	}
	receipt, err := c.state.GetDeletion(ctx, target, "DeleteSubject", key)
	if err != nil {
		return false, err
	}
	var counts cascadeCounters
	if receipt.HeldCount > 0 {
		counts, err = c.cascade.cascadeThread(ctx, target, receipt.ContextIDs, receipt.TaskIDs)
	} else {
		counts, err = c.cascade.cascadeSubject(ctx, target)
	}
	if err != nil {
		return false, err
	}
	if counts.Referenced > 0 {
		return counts.Held > 0, contracts.NewReasoned(contracts.ErrUnavailable, "tenant_references_retained", "ahp: another owner still references tenant artifacts")
	}
	for _, step := range []string{"memory", "artifacts", "complete"} {
		if err = c.state.CompleteDeletionStep(ctx, target, receipt.ID, step); err != nil {
			return false, err
		}
	}
	return counts.Held > 0, nil
}
