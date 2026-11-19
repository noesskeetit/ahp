package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type ControlDeletionConfig struct {
	TenantOwners map[string]TenantDeletionOwner
	// The composition declares required owners separately so losing a runtime
	// collaborator cannot silently turn erasure into a partial success.
	SubjectOwners         map[string]SubjectDeletionOwner
	RequiredSubjectOwners []string
	// ConsumerContext is the process lifetime. Its presence enables durable
	// service continuation; nil preserves fail-closed self-erasure admission.
	ConsumerContext context.Context
	// ResolveHook returns the registered, version-specific A2A endpoint. Only
	// its HTTPS origin is used; the manifest supplies a bounded relative path.
	ResolveHook func(context.Context, string, registry.DeletionTarget) (string, error)
	HTTPClient  *http.Client
}
type workspaceSubjectDeleter interface {
	AuthorizeSubjectDeletion(context.Context, contracts.Scope) error
	DeleteSubjectMemberships(context.Context, contracts.Scope, string) error
}
type subjectCascade interface {
	memoryDeletion
	subjectScopes(context.Context, contracts.Scope) ([]contracts.Scope, error)
	fenceSubject(context.Context, contracts.Scope) error
	workspaceOwner() workspaceSubjectDeleter
}
type plannedHook struct {
	Target registry.DeletionTarget `json:"target"`
	URL    string                  `json:"url"`
}
type subjectPlan struct {
	Scope  contracts.Scope   `json:"scope"`
	Scopes []contracts.Scope `json:"scopes"`
	Hooks  []plannedHook     `json:"hooks"`
	Held   bool              `json:"held"`
}

func (c *stateDeletionCoordinator) ControlDeleteSubject(ctx context.Context, scope contracts.Scope, key string) (map[string]any, error) {
	principal, ok := transport.FromContext(ctx)
	if !ok || principal.Scope != scope {
		return nil, contracts.NewError(contracts.ErrForbidden, "ahp: verified control-plane scope required")
	}
	if principal.Claims.Subject == scope.Subject && c.control.ConsumerContext == nil {
		return nil, contracts.NewReasoned(contracts.ErrForbidden, "self_deletion_requires_service_authority", "ahp: self-deletion requires a service-authorized consumer; no data was changed")
	}
	if key == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "ahp: control deletion key is required")
	}
	if err := c.Ready(ctx, scope, "DeleteSubject"); err != nil {
		return nil, err
	}
	owners, ok := c.cascade.(subjectCascade)
	if !ok || owners.workspaceOwner() == nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "ahp: subject deletion owners unavailable")
	}
	if err := owners.workspaceOwner().AuthorizeSubjectDeletion(ctx, scope); err != nil {
		return nil, err
	}
	release, err := c.state.LockSubjectControl(ctx, scope.Tenant, scope.Subject)
	if err != nil {
		return nil, err
	}
	defer release()
	job, err := c.state.GetControlDeletion(ctx, scope.Tenant, principal.Claims.Subject, key)
	var plan subjectPlan
	if contracts.Code(err) == contracts.ErrNotFound {
		plan, err = c.planSubjectDeletion(ctx, scope, owners)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(plan)
		if err != nil {
			return nil, err
		}
		job, err = c.state.BeginControlDeletion(ctx, scope, principal.Claims.Subject, key, raw, c.control.ConsumerContext != nil)
	}
	if err != nil {
		return nil, err
	}
	if principal.Claims.Subject == scope.Subject && !job.ServiceAuthorized {
		return nil, contracts.NewReasoned(contracts.ErrForbidden, "self_deletion_requires_service_authority", "ahp: legacy intent has no service authority; use a new key")
	}
	if job.ServiceAuthorized {
		ctx = withDeletionAuthority(ctx, job)
	}
	return c.continueControlSubject(ctx, scope, job, owners)
}

func (c *stateDeletionCoordinator) continueControlSubject(ctx context.Context, scope contracts.Scope, job state.ControlDeletion, owners subjectCascade) (map[string]any, error) {
	if job.Subject != scope.Subject || job.Kind != "subject" {
		return nil, contracts.NewError(contracts.ErrKeyReuse, "ahp: control deletion key belongs to another subject")
	}
	var plan subjectPlan
	if err := json.Unmarshal(job.Plan, &plan); err != nil {
		return nil, err
	}
	result := func(status string) map[string]any {
		return map[string]any{"deletion_id": job.ID, "state": status, "scope_count": len(plan.Scopes), "hook_count": len(plan.Hooks), "retained_by_hold": plan.Held}
	}
	finalStatus := func() string {
		if plan.Held {
			return "completed_with_holds"
		}
		return "completed"
	}
	var err error
	done := map[string]bool{}
	for _, step := range job.Steps {
		done[step] = true
	}
	subjectOwnerNames, ownerErr := c.subjectOwnerNames()
	if ownerErr != nil {
		return nil, ownerErr
	}
	legacyEffects := job.Completed
	for step := range done {
		if strings.HasPrefix(step, "state:") || strings.HasPrefix(step, "artifacts:") {
			legacyEffects = true
		}
	}
	if legacyEffects && (!done["state-fence"] || !done["artifacts-fence"]) {
		return nil, contracts.NewReasoned(contracts.ErrConflict, "deletion_fence_upgrade_required", "ahp: this legacy receipt predates durable write fences; submit a new operation key")
	}
	if job.Completed {
		for _, name := range subjectOwnerNames {
			if !done["owner-fence:"+name] || !done["owner:"+name] {
				return nil, contracts.NewReasoned(contracts.ErrConflict, "deletion_owner_upgrade_required", "ahp: prior completion did not include all current owners; submit a new operation key")
			}
		}
		return result(finalStatus()), nil
	}
	apply := func(name string, run func() error) error {
		if done[name] {
			return nil
		}
		if err := run(); err != nil {
			return err
		}
		if err := c.state.AckControlDeletion(ctx, job, name, false); err != nil {
			return err
		}
		done[name] = true
		return nil
	}
	pending := func(err error) (map[string]any, error) {
		r := result("pending")
		r["code"] = contracts.ErrUnavailable
		r["message"] = "subject deletion is pending; retry the same key"
		return r, err
	}
	// Runtime configuration is not part of the durable payload. Revalidate it
	// on every retry before performing any unfinished owner mutation.
	for _, hook := range plan.Hooks {
		if !done["hook:"+hook.Target.Installation+":"+hook.Target.Version] && !hasControlClientCertificate(c.control.HTTPClient) {
			return pending(contracts.NewError(contracts.ErrUnavailable, "ahp: pending hook requires an mTLS client"))
		}
	}
	held, err := c.state.SubjectHeld(ctx, scope.Tenant, scope.Subject)
	if err != nil {
		return pending(err)
	}
	if len(subjectOwnerNames) > 0 && plan.Held && !held {
		refreshed, e := c.planSubjectDeletion(ctx, scope, owners)
		if e != nil {
			return pending(e)
		}
		plan.Hooks = refreshed.Hooks
		plan.Held = false
	}
	plan.Held = plan.Held || held
	if err = apply("identity", func() error { return identity.New(c.state.Pool()).RevokeSubject(ctx, scope.Tenant, scope.Subject) }); err != nil {
		return pending(err)
	}
	for _, name := range subjectOwnerNames {
		if err = apply("owner-fence:"+name, func() error { return c.control.SubjectOwners[name].FenceSubject(ctx, scope.Tenant, scope.Subject) }); err != nil {
			return pending(err)
		}
	}
	if err = apply("state-fence", func() error { return c.state.FenceSubject(ctx, scope) }); err != nil {
		return pending(err)
	}
	if err = apply("artifacts-fence", func() error { return owners.fenceSubject(ctx, scope) }); err != nil {
		return pending(err)
	}
	// Both owners now reject new writes and every earlier admitted metadata
	// transaction has committed. Discovery is stable across new deployments.
	// Discovery runs again after revocation and on each retry. Keep every
	// earlier dependency even if another owner already removed its source row.
	stateScopes, err := c.state.SubjectScopes(ctx, scope.Tenant, scope.Subject)
	if err != nil {
		return pending(err)
	}
	artifactScopes, err := owners.subjectScopes(ctx, scope)
	if err != nil {
		return pending(err)
	}
	seen := map[string]bool{}
	for _, target := range plan.Scopes {
		seen[target.Deployment] = true
	}
	for _, target := range append(stateScopes, artifactScopes...) {
		if !seen[target.Deployment] {
			seen[target.Deployment] = true
			plan.Scopes = append(plan.Scopes, target)
		}
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return pending(err)
	}
	if err = c.state.UpdateControlDeletionPlan(ctx, job, raw); err != nil {
		return pending(err)
	}
	if err = apply("workspace", func() error { return owners.workspaceOwner().DeleteSubjectMemberships(ctx, scope, "delete-"+job.ID) }); err != nil {
		return pending(err)
	}
	if !plan.Held {
		if err = apply("memory", func() error { return owners.deleteMemorySubject(ctx, scope, "global-"+job.ID) }); err != nil {
			return pending(err)
		}
	}
	if len(subjectOwnerNames) > 0 {
		if plan.Held {
			return pending(contracts.NewReasoned(contracts.ErrUnavailable, "subject_owner_hold", "ahp: owner erasure awaits legal hold release"))
		}
		for _, name := range subjectOwnerNames {
			if name == "identity" {
				continue
			}
			if err = apply("owner:"+name, func() error { return c.control.SubjectOwners[name].DeleteSubject(ctx, scope.Tenant, scope.Subject) }); err != nil {
				return pending(err)
			}
		}
	}
	for _, target := range plan.Scopes {
		childKey := "global-" + shortDeletionID(job.ID+"/"+target.Deployment)
		if err = apply("state:"+target.Deployment, func() error { _, err := c.state.DeleteSubjectWithKey(ctx, target, childKey); return err }); err != nil {
			return pending(err)
		}
		if err = apply("artifacts:"+target.Deployment, func() error {
			receipt, err := c.state.GetDeletion(ctx, target, "DeleteSubject", childKey)
			if err != nil {
				return err
			}
			if receipt.HeldCount > 0 {
				_, err = c.cascade.cascadeThread(ctx, target, receipt.ContextIDs, receipt.TaskIDs)
			} else {
				_, err = c.cascade.cascadeSubject(ctx, target)
			}
			if err != nil {
				return err
			}
			for _, step := range []string{"memory", "artifacts", "complete"} {
				if err = c.state.CompleteDeletionStep(ctx, target, receipt.ID, step); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return pending(err)
		}
	}
	if !plan.Held {
		for _, hook := range plan.Hooks {
			name := "hook:" + hook.Target.Installation + ":" + hook.Target.Version
			if err = apply(name, func() error { return c.invokeDeletionHook(ctx, scope, job.ID, hook) }); err != nil {
				return pending(err)
			}
		}
	}
	if owner := c.control.SubjectOwners["identity"]; owner != nil {
		if err = apply("owner:identity", func() error { return owner.DeleteSubject(ctx, scope.Tenant, scope.Subject) }); err != nil {
			return pending(err)
		}
	}
	if err = c.state.AckControlDeletion(ctx, job, "complete", true); err != nil {
		return pending(err)
	}
	return result(finalStatus()), nil
}

func (c *stateDeletionCoordinator) planSubjectDeletion(ctx context.Context, scope contracts.Scope, owners subjectCascade) (subjectPlan, error) {
	plan := subjectPlan{Scope: scope}
	stateScopes, err := c.state.SubjectScopes(ctx, scope.Tenant, scope.Subject)
	if err != nil {
		return plan, err
	}
	artScopes, err := owners.subjectScopes(ctx, scope)
	if err != nil {
		return plan, err
	}
	targets, err := registry.New(c.state.Pool()).DeletionTargets(ctx, scope.Tenant)
	if err != nil {
		return plan, contracts.NewError(contracts.ErrUnavailable, "ahp: registry deletion catalog unavailable")
	}
	plan.Held, err = c.state.SubjectHeld(ctx, scope.Tenant, scope.Subject)
	if err != nil {
		return plan, err
	}
	seen := map[string]bool{}
	for _, target := range append(append(stateScopes, artScopes...), scope) {
		if !seen[target.Deployment] {
			seen[target.Deployment] = true
			plan.Scopes = append(plan.Scopes, target)
		}
	}
	for _, target := range targets {
		if plan.Held {
			continue
		}
		if c.control.ResolveHook == nil || !hasControlClientCertificate(c.control.HTTPClient) {
			return plan, contracts.NewError(contracts.ErrUnavailable, "ahp: custom-table deletion requires a runtime resolver and mTLS client")
		}
		endpoint, err := c.control.ResolveHook(ctx, scope.Tenant, target)
		if err != nil {
			return plan, err
		}
		hookURL, err := deletionHookURL(endpoint, target.Hook)
		if err != nil {
			return plan, err
		}
		plan.Hooks = append(plan.Hooks, plannedHook{Target: target, URL: hookURL})
	}
	return plan, nil
}
func shortDeletionID(in string) string {
	sum := sha256.Sum256([]byte(in))
	return hex.EncodeToString(sum[:16])
}
func deletionHookURL(endpoint, hook string) (string, error) {
	base, err := url.Parse(endpoint)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil {
		return "", contracts.NewError(contracts.ErrInvalid, "ahp: deletion runtime must have an HTTPS origin")
	}
	path, err := url.Parse(hook)
	if err != nil || path.IsAbs() || path.Host != "" || path.RawQuery != "" || path.Fragment != "" || strings.Contains(hook, "%") || strings.HasPrefix(hook, "//") || hook == "" {
		return "", contracts.NewError(contracts.ErrInvalid, "ahp: deletion hook must be a relative path")
	}
	for _, part := range strings.Split(path.Path, "/") {
		if part == ".." || part == "." {
			return "", contracts.NewError(contracts.ErrInvalid, "ahp: deletion hook path traversal refused")
		}
	}
	base.Path = "/" + strings.TrimLeft(path.Path, "/")
	base.RawPath = ""
	base.RawQuery = ""
	base.Fragment = ""
	return base.String(), nil
}
func hasControlClientCertificate(client *http.Client) bool {
	if client == nil {
		return false
	}
	tr, ok := client.Transport.(*http.Transport)
	return ok && tr.TLSClientConfig != nil && !tr.TLSClientConfig.InsecureSkipVerify && (len(tr.TLSClientConfig.Certificates) > 0 || tr.TLSClientConfig.GetClientCertificate != nil)
}
func (c *stateDeletionCoordinator) invokeDeletionHook(ctx context.Context, scope contracts.Scope, job string, hook plannedHook) error {
	if !hasControlClientCertificate(c.control.HTTPClient) {
		return contracts.NewError(contracts.ErrUnavailable, "ahp: deletion hook requires an mTLS client")
	}
	key := "delete-" + shortDeletionID(job+"/"+hook.Target.Installation+"/"+hook.Target.Version)
	payload := map[string]string{"tenant": scope.Tenant, "subject": scope.Subject, "operation_key": key, "deletion_id": job}
	if scope.Subject == "" {
		delete(payload, "subject")
		payload["deletion_scope"] = "tenant"
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hook.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	client := *c.control.HTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(req)
	if err != nil {
		return contracts.NewError(contracts.ErrUnavailable, "ahp: deletion hook unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return contracts.NewError(contracts.ErrUnavailable, "ahp: deletion hook did not acknowledge completion")
	}
	var ack struct {
		Status        string `json:"status"`
		OperationKey  string `json:"operation_key"`
		DeletionScope string `json:"deletion_scope"`
	}
	if err = json.NewDecoder(io.LimitReader(response.Body, 16384)).Decode(&ack); err != nil || ack.Status != "completed" || ack.OperationKey != key || (scope.Subject == "" && ack.DeletionScope != "tenant") {
		return fmt.Errorf("ahp: deletion hook acknowledgement does not match the operation")
	}
	return nil
}
