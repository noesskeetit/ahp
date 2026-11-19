package identity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestSyntheticResourcesRevokeOldGrantAndDenyForeignContainers(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	p := NewSyntheticResourcePolicy(s.pool)
	s.resources = p
	request := rootRequest("resource-task", "resource-context", scope.Deployment, time.Hour, "tools")
	if _, err := s.CreateRoot(ctx, scope, request); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("unprovisioned synthetic container accepted: %v", err)
	}
	for _, r := range []Resource{{Kind: "tenant", ID: scope.Tenant}, {Kind: "deployment", ID: scope.Deployment}} {
		if _, err := p.PutRule(ctx, ResourceRule{Tenant: scope.Tenant, Subject: scope.Subject, Resource: r, Action: "tools", Allowed: true}, 0); err != nil {
			t.Fatal(err)
		}
	}
	g, err := s.CreateRoot(ctx, scope, request)
	if err != nil {
		t.Fatal(err)
	}
	c := auth.Claims{Kind: auth.KindTask, TokenUse: auth.TokenUse, Tenant: g.Tenant, Subject: g.Subject, Deployment: g.Deployment, Task: g.Task, Root: g.Root, Grant: g.ID, Context: g.Context, Actors: g.Actors, Depth: g.Depth}
	if err := s.Authorize(ctx, c, "tools"); err != nil {
		t.Fatal(err)
	}
	rule := ResourceRule{Tenant: scope.Tenant, Subject: scope.Subject, Resource: Resource{Kind: "deployment", ID: scope.Deployment}, Action: "tools", Allowed: false}
	if _, err := p.PutRule(ctx, rule, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Authorize(ctx, c, "tools"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("old JWT survived resource revoke: %v", err)
	}
	foreign := scope
	foreign.Tenant = "foreign"
	if err := p.Require(ctx, ResourceRequest{Scope: foreign, Resource: Resource{Kind: "deployment", ID: scope.Deployment}, Action: "tools"}); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("foreign tenant accepted: %v", err)
	}
	s.resources = ResourceEvaluatorFunc(func(context.Context, ResourceRequest) error { return errors.New("authority unreachable") })
	if err := s.Authorize(ctx, c, "tools"); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("failed authority did not fail closed: %v", err)
	}
}

func TestSyntheticProjectDecisionRequiresCurrentOwnerFacts(t *testing.T) {
	s := newStore(t)
	p := NewSyntheticResourcePolicy(s.pool)
	ctx := testContext(t)
	sc := uniqueScope(t)
	r := ResourceRequest{Scope: sc, Resource: Resource{Kind: "project", ID: "project-1"}, Action: "read", ProjectRole: "viewer"}
	if err := p.Require(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.Action = "publish"
	if err := p.Require(ctx, r); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("viewer allowed to publish")
	}
	r.ProjectRole = ""
	if err := p.Require(ctx, r); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("missing membership allowed")
	}
	r.ProjectRole, r.Action = "owner", "read"
	if _, err := p.PutRule(ctx, ResourceRule{Tenant: sc.Tenant, Subject: sc.Subject, Resource: r.Resource, Action: r.Action, Allowed: false}, 0); err != nil {
		t.Fatal(err)
	}
	if err := p.Require(ctx, r); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("explicit IAM denial bypassed by ownership")
	}
}

func TestSyntheticPolicyErasurePreservesPermanentRevocation(t *testing.T) {
	for _, kind := range []string{"subject", "tenant"} {
		t.Run(kind, func(t *testing.T) {
			s := newStore(t)
			ctx := testContext(t)
			sc := uniqueScope(t)
			p := NewSyntheticResourcePolicy(s.pool)
			r := ResourceRule{Tenant: sc.Tenant, Subject: sc.Subject, Resource: Resource{Kind: "tenant", ID: sc.Tenant}, Action: "login", Allowed: true}
			if _, err := p.PutRule(ctx, r, 0); err != nil {
				t.Fatal(err)
			}
			var err error
			if kind == "subject" {
				err = s.RevokeSubject(ctx, sc.Tenant, sc.Subject)
				if err == nil {
					err = s.DeleteSubject(ctx, sc.Tenant, sc.Subject)
				}
			} else {
				err = s.RevokeTenant(ctx, sc.Tenant)
				if err == nil {
					err = s.DeleteTenant(ctx, sc.Tenant)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			var count int
			if err = s.pool.QueryRow(ctx, "SELECT count(*) FROM ahp_identity.resource_rules WHERE tenant=$1", sc.Tenant).Scan(&count); err != nil || count != 0 {
				t.Fatalf("resource rules survived erasure: count=%d error=%v", count, err)
			}
			if _, err = p.PutRule(ctx, r, 0); contracts.Code(err) != contracts.ErrForbidden {
				t.Fatalf("rule resurrected erased principal: %v", err)
			}
		})
	}
}
