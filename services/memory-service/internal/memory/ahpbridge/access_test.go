package ahpbridge

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type authorityFixture struct {
	access        *Access
	claims        auth.Claims
	ctx           context.Context
	revoked       bool
	projectShared bool
	set           labels.Set
	bound         labels.Set
	caps          []string
}

func fixture(t *testing.T) *authorityFixture {
	t.Helper()
	f := &authorityFixture{claims: auth.Claims{Kind: auth.KindTask, TokenUse: auth.TokenUse, Tenant: "tenant", Subject: "owner", Deployment: "runner", Task: "task", Grant: "grant", Root: "root", Context: "chat", Actors: []string{"runner"}}}
	a, err := NewAccess(Dependencies{
		Authorize: func(ctx context.Context, c auth.Claims, cap string) error {
			if f.revoked {
				return contracts.NewError(contracts.ErrForbidden, "revoked")
			}
			if c.Scope() != f.claims.Scope() || c.Task != f.claims.Task || c.Grant != f.claims.Grant {
				return contracts.NewError(contracts.ErrForbidden, "mismatch")
			}
			f.caps = append(f.caps, cap)
			return nil
		},
		Evaluate: func(context.Context, contracts.Scope, string, string) (Grant, error) {
			if f.revoked {
				return Grant{}, contracts.NewError(contracts.ErrForbidden, "revoked")
			}
			return Grant{ID: "grant", ExpiresAt: time.Now().Add(time.Hour), Labels: f.set}, nil
		},
		AddLabels: func(_ context.Context, _ contracts.Scope, _ string, set labels.Set) error {
			f.bound = labels.Merge(f.bound, set)
			return nil
		},
		PrivateProject: func(_ context.Context, _ contracts.Scope, p string) error {
			if p != "alpha" || f.projectShared {
				return contracts.NewError(contracts.ErrForbidden, "shared or foreign")
			}
			return nil
		},
		ContextProject: func(context.Context, contracts.Scope, string) (string, error) { return "alpha", nil },
		Source: func(_ context.Context, _ auth.Claims, project, ref string) error {
			if ref != "state://runs/run/input" {
				return contracts.NewError(contracts.ErrForbidden, "foreign source")
			}
			return nil
		},
		RuntimeDeployments: []string{"runner"},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.access = a
	f.ctx = transport.WithPrincipal(context.Background(), transport.Principal{Claims: f.claims, Scope: f.claims.Scope()})
	return f
}
func TestNoPrincipalOrMismatchedScopeIsRejected(t *testing.T) {
	f := fixture(t)
	if _, err := f.access.Authorize(context.Background(), "arbitrary bearer", memory.AccessRequest{Action: "memory:read"}); err == nil {
		t.Fatal("unverified bearer accepted")
	}
	wrong := f.claims.Scope()
	wrong.Subject = "other"
	ctx := transport.WithPrincipal(context.Background(), transport.Principal{Claims: f.claims, Scope: wrong})
	if _, err := f.access.Authorize(ctx, "", memory.AccessRequest{Action: "memory:read"}); err == nil {
		t.Fatal("foreign scope accepted")
	}
}
func TestLiveGrantIsRecheckedForBackgroundIntent(t *testing.T) {
	f := fixture(t)
	id, err := f.access.Authorize(f.ctx, "", memory.AccessRequest{Action: "memory:write", Scopes: []memory.Scope{memory.Project}, ProjectID: "alpha", Sources: []memory.Source{{Ref: "state://runs/run/input"}}})
	if err != nil {
		t.Fatal(err)
	}
	if id.ContextID != "chat" || id.ActorKind != "runtime" || len(id.Authority) == 0 {
		t.Fatal(id)
	}
	f.revoked = true
	if err := f.access.Revalidate(context.Background(), id, memory.AccessRequest{Action: "memory:write", ProjectID: "alpha", Scopes: []memory.Scope{memory.Project}}); err == nil {
		t.Fatal("revoked durable intent accepted")
	}
}
func TestTaskCannotChooseAnotherProjectOrSharedWorkspace(t *testing.T) {
	f := fixture(t)
	for _, p := range []string{"beta", "alpha"} {
		if p == "alpha" {
			f.projectShared = true
		}
		if _, err := f.access.Authorize(f.ctx, "", memory.AccessRequest{Action: "memory:read", ProjectID: p, Scopes: []memory.Scope{memory.Project}}); err == nil {
			t.Fatal(p)
		}
	}
}
func TestOnlySingleOwnerWorkspaceIsSupported(t *testing.T) {
	if err := RequirePrivateWorkspace("u", []Member{{"u", "owner"}}); err != nil {
		t.Fatal(err)
	}
	for _, members := range [][]Member{nil, {{"other", "owner"}}, {{"u", "editor"}}, {{"u", "owner"}, {"other", "viewer"}}} {
		if err := RequirePrivateWorkspace("u", members); err == nil {
			t.Fatal(members)
		}
	}
}
func TestWorkspaceLabelsPreserveBothPrivateOwnerBranches(t *testing.T) {
	f := fixture(t)
	f.set = labels.Set{Workspaces: []string{"alpha"}}
	id, err := f.access.Authorize(f.ctx, "", memory.AccessRequest{Action: "runtime:turn", ProjectID: "alpha", Scopes: []memory.Scope{memory.Personal, memory.Project}})
	if err != nil {
		t.Fatal(err)
	}
	sources, err := f.access.FilterSources(f.ctx, id, []memory.Source{{AllowedScopes: []memory.Scope{memory.Personal, memory.Project}}})
	if err != nil || len(sources[0].AllowedScopes) != 2 {
		t.Fatal(sources, err)
	}
	if _, err := f.access.Authorize(f.ctx, "", memory.AccessRequest{Action: "memory:write", ProjectID: "alpha", Scopes: []memory.Scope{memory.Personal}}); err != nil {
		t.Fatal("private owner sink must follow the existing AHP label policy", err)
	}
}
func TestContextLabelsUseExistingIdentityJournal(t *testing.T) {
	f := fixture(t)
	id, err := f.access.Authorize(f.ctx, "", memory.AccessRequest{Action: "memory:read"})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.access.Bind(f.ctx, id, encode(labels.Set{Subjects: []string{"owner"}})); err != nil {
		t.Fatal(err)
	}
	if len(f.bound.Subjects) != 1 || f.bound.Subjects[0] != "owner" {
		t.Fatal(f.bound)
	}
	id.UserID = "other"
	if err := f.access.Bind(f.ctx, id, nil); err == nil {
		t.Fatal("tampered stored identity accepted")
	}
}
func TestRuntimeCannotChangeGlobalSettings(t *testing.T) {
	f := fixture(t)
	if _, err := f.access.Authorize(f.ctx, "", memory.AccessRequest{Action: "memory:manage"}); err == nil {
		t.Fatal("runtime obtained console authority")
	}
}

func TestProjectTaskCannotOmitProjectToReadGlobalPersonalBank(t *testing.T) {
	f := fixture(t)
	if _, err := f.access.Authorize(f.ctx, "", memory.AccessRequest{Action: "memory:read", Scopes: []memory.Scope{memory.Personal}}); err == nil {
		t.Fatal("omitting project bypassed contextual personal policy")
	}
	if _, err := f.access.Authorize(f.ctx, "", memory.AccessRequest{Action: "memory:read", ProjectID: "alpha", Scopes: []memory.Scope{memory.Personal}}); err != nil {
		t.Fatal(err)
	}
	f.access.d.ContextProject = func(context.Context, contracts.Scope, string) (string, error) { return "", nil }
	if _, err := f.access.Authorize(f.ctx, "", memory.AccessRequest{Action: "memory:read", Scopes: []memory.Scope{memory.Personal}}); err != nil {
		t.Fatal("personal-only chat was blocked", err)
	}
}
