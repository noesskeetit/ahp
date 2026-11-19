// Package ahpbridge adapts long-term memory to AHP's existing verified
// principal, live grants, context label journal and Workspace authority.
package ahpbridge

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Grant is a projection, not a second grant store. The composition fills it
// from identity.Store.Evaluate on every check.
type Grant struct {
	ID        string
	ExpiresAt time.Time
	Labels    labels.Set
}
type Dependencies struct {
	Authorize          transport.Authorize
	Evaluate           func(context.Context, contracts.Scope, string, string) (Grant, error)
	AddLabels          func(context.Context, contracts.Scope, string, labels.Set) error
	PrivateProject     func(context.Context, contracts.Scope, string) error
	Source             func(context.Context, auth.Claims, string, string) error
	ContextProject     func(context.Context, contracts.Scope, string) (string, error)
	RuntimeDeployments []string
	ControlDeployments []string
}
type Access struct{ d Dependencies }

// Only the in-process State consumer can set this marker. HTTP callers still
// need an explicitly configured runtime deployment to publish turn events.
type stateCompletionKey struct{}

func fromStateCompletion(ctx context.Context) bool {
	trusted, _ := ctx.Value(stateCompletionKey{}).(bool)
	return trusted
}

func NewAccess(d Dependencies) (*Access, error) {
	if d.Authorize == nil || d.Evaluate == nil || d.AddLabels == nil || d.PrivateProject == nil || d.Source == nil || d.ContextProject == nil {
		return nil, memory.Fail(500, "memory_authority_missing", "Identity, Workspace and State authority are mandatory")
	}
	return &Access{d}, nil
}
func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
func (a *Access) kind(c auth.Claims) string {
	if c.CtxScope == auth.CtxScopeAuthority {
		return "console"
	}
	if contains(a.d.ControlDeployments, c.Deployment) {
		return "platform"
	}
	if contains(a.d.RuntimeDeployments, c.Deployment) {
		return "runtime"
	}
	return "agent"
}
func capability(action string) (string, error) {
	switch action {
	case "memory:read", "memory:bind":
		return "memory", nil
	case "memory:write", "runtime:turn", "runtime:run", "runtime:flush":
		return "memory", nil
	case "memory:manage":
		return "memory:review", nil
	case "project:delete", "memory:clear":
		return "memory:delete", nil
	default:
		return "", memory.Fail(403, "memory_action_denied", "unknown memory action")
	}
}
func authorityError(err error) error {
	if err == nil {
		return nil
	}
	switch contracts.Code(err) {
	case contracts.ErrForbidden:
		return memory.Fail(403, "access_denied", "AHP denied memory access")
	case contracts.ErrNotFound:
		// DEF-MEM-HTTP-CODE-1 follow-up: a 404 on any HTTP face of the platform
		// carries the canonical code (decision 2026-10-31-http-surface-no-false-200),
		// and this branch reaches the memory envelope through respond()'s typed path
		// (Service.List -> Access.Bind, GET /v1/memory/items). What is specific about
		// the refusal stays in the message, where it belongs.
		return memory.Fail(404, contracts.ErrNotFound, "AHP source or private Workspace does not exist")
	case contracts.ErrInvalid:
		return memory.Fail(400, "invalid_memory_source", "AHP rejected the memory source")
	default:
		return memory.Fail(503, "ahp_authority_unavailable", "AHP authority could not be checked")
	}
}
func (a *Access) check(ctx context.Context, c auth.Claims, req memory.AccessRequest) (Grant, error) {
	if c.Kind != auth.KindTask || c.Grant == "" || c.Context == "" || c.Scope().Validate() != nil {
		return Grant{}, memory.Fail(403, "task_identity_required", "verified AHP task authority is required")
	}
	cap, err := capability(req.Action)
	if err != nil {
		return Grant{}, err
	}
	if err := a.d.Authorize(ctx, c, "memory"); err != nil {
		return Grant{}, authorityError(err)
	}
	if cap != "memory" {
		if err := a.d.Authorize(ctx, c, cap); err != nil {
			return Grant{}, authorityError(err)
		}
	}
	ctx = transport.WithPrincipal(ctx, transport.Principal{Scope: c.Scope(), Claims: c})
	kind := a.kind(c)
	if req.Action == "memory:bind" && kind != "console" && kind != "platform" {
		return Grant{}, memory.Fail(403, "console_required", "only ingress authority may bind a conversation to a project")
	}
	if strings.HasPrefix(req.Action, "runtime:") && kind != "runtime" && kind != "platform" && !fromStateCompletion(ctx) {
		return Grant{}, memory.Fail(403, "runtime_required", "deployment is not a configured memory event producer")
	}
	if (req.Action == "memory:manage" || req.Action == "memory:clear") && kind != "console" && kind != "platform" {
		return Grant{}, memory.Fail(403, "console_required", "memory management requires ingress authority")
	}
	if req.Action == "project:delete" && kind != "platform" {
		return Grant{}, memory.Fail(403, "platform_required", "project deletion requires a control deployment")
	}
	// A scoped request may not omit its bound project to bypass use_personal.
	// Unscoped internal label binding/global-settings reads disclose no facts.
	if (req.ProjectID != "" || len(req.Scopes) > 0 || strings.HasPrefix(req.Action, "runtime:")) && kind != "console" && kind != "platform" {
		bound, err := a.d.ContextProject(ctx, c.Scope(), c.Context)
		if err != nil {
			return Grant{}, authorityError(err)
		}
		if bound != req.ProjectID {
			return Grant{}, memory.Fail(403, "wrong_project", "project differs from the durable task binding")
		}
	}
	if req.ProjectID != "" && req.Action != "memory:clear" && req.Action != "project:delete" {
		if err := a.d.PrivateProject(ctx, c.Scope(), req.ProjectID); err != nil {
			return Grant{}, authorityError(err)
		}
	}
	for _, scope := range req.Scopes {
		if !scope.Valid() {
			return Grant{}, memory.Fail(400, "invalid_scope", "unknown memory scope")
		}
		if scope == memory.Project && req.ProjectID == "" {
			return Grant{}, memory.Fail(400, "project_required", "project memory requires a private Workspace")
		}
	}
	g, err := a.d.Evaluate(ctx, c.Scope(), c.Task, "memory")
	if err != nil {
		return Grant{}, authorityError(err)
	}
	if g.ID != c.Grant || !g.ExpiresAt.After(time.Now()) {
		return Grant{}, memory.Fail(403, "grant_revoked", "memory grant is no longer valid")
	}
	write := req.Action == "memory:write" || req.Action == "runtime:turn"
	if write {
		if err := labels.CheckWrite(g.Labels, labels.Audience{Kind: labels.KindPrivate, Subject: c.Subject}); err != nil {
			return Grant{}, authorityError(err)
		}
	}
	for _, source := range req.Sources {
		if err := a.d.Source(ctx, c, req.ProjectID, source.Ref); err != nil {
			return Grant{}, authorityError(err)
		}
	}
	return g, nil
}
func encode(set labels.Set) []string {
	b, _ := json.Marshal(labels.Merge(set))
	return []string{string(b)}
}
func decode(values []string) (labels.Set, error) {
	sets := []labels.Set{}
	for _, v := range values {
		var set labels.Set
		if err := json.Unmarshal([]byte(v), &set); err != nil {
			return labels.Set{}, memory.Fail(403, "invalid_source_labels", "memory labels must use the AHP label vocabulary")
		}
		sets = append(sets, set)
	}
	return labels.Merge(sets...), nil
}
func (a *Access) Authorize(ctx context.Context, _ string, req memory.AccessRequest) (memory.Identity, error) {
	p, ok := transport.FromContext(ctx)
	if !ok || p.Scope != p.Claims.Scope() {
		return memory.Identity{}, memory.Fail(403, "verified_principal_required", "AHP transport principal is required")
	}
	g, err := a.check(ctx, p.Claims, req)
	if err != nil {
		return memory.Identity{}, err
	}
	proof, _ := json.Marshal(p.Claims)
	kind := a.kind(p.Claims)
	if fromStateCompletion(ctx) {
		kind = "runtime"
	}
	return memory.Identity{TenantID: p.Scope.Tenant, UserID: p.Scope.Subject, ActorID: p.Scope.Deployment, ActorKind: kind, GrantID: g.ID, TaskID: p.Claims.Task, ExpiresAt: g.ExpiresAt, Labels: encode(g.Labels), ContextID: p.Claims.Context, Authority: proof}, nil
}
func claims(id memory.Identity) (auth.Claims, error) {
	var c auth.Claims
	if err := json.Unmarshal(id.Authority, &c); err != nil {
		return c, memory.Fail(403, "invalid_authority", "persisted AHP authority is missing")
	}
	if c.Tenant != id.TenantID || c.Subject != id.UserID || c.Deployment != id.ActorID || c.Grant != id.GrantID || c.Task != id.TaskID || c.Context != id.ContextID {
		return c, memory.Fail(403, "invalid_authority", "memory intent authority does not match its owner")
	}
	return c, nil
}
func (a *Access) Revalidate(ctx context.Context, id memory.Identity, req memory.AccessRequest) error {
	c, err := claims(id)
	if err != nil {
		return err
	}
	_, err = a.check(ctx, c, req)
	return err
}
func (a *Access) Bind(ctx context.Context, id memory.Identity, raw []string) error {
	c, err := claims(id)
	if err != nil {
		return err
	}
	if _, err := a.check(ctx, c, memory.AccessRequest{Action: "memory:read"}); err != nil {
		return err
	}
	set, err := decode(raw)
	if err != nil {
		return err
	}
	set = labels.Merge(set, labels.Set{Subjects: []string{id.UserID}})
	return authorityError(a.d.AddLabels(ctx, c.Scope(), c.Task, set))
}

// FilterSources preserves the authoritative source scopes and validates their
// labels against the private owner sink. AHP explicitly permits workspace
// labels at a private sink; treating them as a new global-write prohibition
// would silently replace the platform policy. Both banks remain private and
// preserve every source restriction in the context journal on retrieval.
func (a *Access) FilterSources(ctx context.Context, id memory.Identity, sources []memory.Source) ([]memory.Source, error) {
	c, err := claims(id)
	if err != nil {
		return nil, err
	}
	g, err := a.d.Evaluate(ctx, c.Scope(), c.Task, "memory")
	if err != nil {
		return nil, authorityError(err)
	}
	out := make([]memory.Source, 0, len(sources))
	for _, source := range sources {
		declared, err := decode(source.Labels)
		if err != nil {
			return nil, err
		}
		set := labels.Merge(g.Labels, declared)
		if err := labels.CheckWrite(set, labels.Audience{Kind: labels.KindPrivate, Subject: c.Subject}); err != nil {
			return nil, authorityError(err)
		}
		allowed := []memory.Scope{}
		for _, scope := range source.AllowedScopes {
			if !scope.Valid() {
				return nil, memory.Fail(400, "invalid_scope", "unknown source scope")
			}
			allowed = append(allowed, scope)
		}
		source.AllowedScopes = allowed
		source.Labels = encode(set)
		out = append(out, source)
	}
	return out, nil
}
