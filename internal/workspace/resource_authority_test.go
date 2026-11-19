package workspace

import (
	"context"
	"errors"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
)

func TestResourceIAMControlsMembershipFacadesAndRevocation(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	ids := identity.New(e.pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	policy := identity.NewSyntheticResourcePolicy(e.pool)
	ids = identity.New(e.pool, identity.WithResourceEvaluator(policy))
	e.ws.SetResourceAuthority(func(ctx context.Context, scope contracts.Scope, f MembershipFacts, action string) error {
		return ids.RequireResource(ctx, identity.ResourceRequest{Scope: scope, Resource: identity.Resource{Kind: "project", ID: f.ProjectID}, ProjectRole: f.Role, Action: action})
	})
	owner, member := e.scope("owner"), e.scope("member")
	w, err := e.ws.Create(ctx, owner, "synthetic-project")
	mustOK(t, err)
	mustOK(t, e.ws.AddMember(ctx, owner, w.Name, member.Subject))
	mustOK(t, e.ws.IsMember(ctx, member, w.Name))
	projects, err := e.ws.ListWorkspaces(ctx, member)
	mustOK(t, err)
	if len(projects) != 1 {
		t.Fatal("current membership absent")
	}
	projectIDs, err := e.ws.ListWorkspaceIDs(ctx, member)
	mustOK(t, err)
	if len(projectIDs) != 1 || projectIDs[0] != w.ID {
		t.Fatal("canonical project IDs missing")
	}
	rule := identity.ResourceRule{Tenant: member.Tenant, Subject: member.Subject, Resource: identity.Resource{Kind: "project", ID: w.ID}, Action: "read", Allowed: false}
	_, err = policy.PutRule(ctx, rule, 0)
	mustOK(t, err)
	if err = e.ws.IsMember(ctx, member, w.Name); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("Artifact membership facade bypassed IAM denial")
	}
	projects, err = e.ws.ListWorkspaces(ctx, member)
	mustOK(t, err)
	if len(projects) != 0 {
		t.Fatal("Memory workspace listing bypassed IAM denial")
	}
	projectIDs, err = e.ws.ListWorkspaceIDs(ctx, member)
	mustOK(t, err)
	if len(projectIDs) != 0 {
		t.Fatal("canonical ID listing bypassed IAM denial")
	}
	if _, err = e.ws.GetWorkspace(ctx, member, w.Name); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("project get bypassed IAM denial")
	}
	rule.Allowed = true
	_, err = policy.PutRule(ctx, rule, 1)
	mustOK(t, err)
	mustOK(t, e.ws.IsMember(ctx, member, w.Name))
	mustOK(t, e.ws.RemoveMember(ctx, owner, w.Name, member.Subject))
	if err = e.ws.IsMember(ctx, member, w.Name); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("IAM allow invented membership after revoke")
	}
	foreign := owner
	foreign.Tenant = "foreign"
	if err = e.ws.IsMember(ctx, foreign, w.Name); err == nil {
		t.Fatal("foreign tenant crossed owner filter")
	}
	e.ws.SetResourceAuthority(func(context.Context, contracts.Scope, MembershipFacts, string) error {
		return errors.New("authority unavailable")
	})
	if _, err = e.ws.ListWorkspaces(ctx, owner); err == nil {
		t.Fatal("unavailable IAM returned projects")
	}
}
