//go:build ahp_data

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	ltm "github.com/ahp-platform/ahp/internal/memory/longterm"
	legacy "github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestHindsightOwnerParticipatesInTenantControlWithoutBankPerTenant(t *testing.T) {
	ctx := context.Background()
	service := ltm.New(ltm.NewMemoryStore(), nil, nil)
	id := ltm.Identity{TenantID: "tenant", UserID: "owner", ActorID: "runner", ActorKind: "console", ContextID: "chat", GrantID: "grant", TaskID: "task", ExpiresAt: time.Now().Add(time.Hour)}
	request := ltm.BindRequest{ContextID: "chat", ProjectID: "alpha"}
	if _, err := service.BindProject(ctx, id, "bind", request); err != nil {
		t.Fatal(err)
	}
	owner := hindsightSubjectOwner{service: service, authorize: func(_ context.Context, scope contracts.Scope, action, resource string) error {
		if scope.Tenant != "tenant" || action != legacy.CapabilityDeleteSubject || (resource != "tenant:tenant" && resource != "subject:owner") {
			t.Fatal(scope, action, resource)
		}
		return nil
	}}
	scope := contracts.Scope{Tenant: "tenant", Subject: "owner", Deployment: "runner"}
	if err := owner.FenceTenant(ctx, scope); err != nil {
		t.Fatal(err)
	}
	scopes, err := owner.TenantScopes(ctx, scope)
	if err != nil || len(scopes) != 1 || scopes[0] != scope {
		t.Fatal(scopes, err)
	}
	_, err = service.BindProject(ctx, id, "late", request)
	var denied *ltm.Error
	if !errors.As(err, &denied) || denied.Code != "tenant_deleted" {
		t.Fatal(err)
	}
	if _, err := owner.DeleteSubject(ctx, scope, scope.Subject, "erase"); err != nil {
		t.Fatal(err)
	}
	scopes, err = owner.TenantScopes(ctx, scope)
	if err != nil || len(scopes) != 0 {
		t.Fatal(scopes, err)
	}
}

func TestHindsightOwnerRejectsUnauthorizedTenantFenceAndWrongSubject(t *testing.T) {
	ctx := context.Background()
	service := ltm.New(ltm.NewMemoryStore(), nil, nil)
	owner := hindsightSubjectOwner{service: service, authorize: func(context.Context, contracts.Scope, string, string) error {
		return contracts.NewError(contracts.ErrForbidden, "denied")
	}}
	scope := contracts.Scope{Tenant: "tenant", Subject: "owner", Deployment: "runner"}
	if err := owner.FenceTenant(ctx, scope); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal(err)
	}
	if _, err := owner.TenantScopes(ctx, scope); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal(err)
	}
	if _, err := owner.DeleteSubject(ctx, scope, "victim", "erase"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal(err)
	}
	id := ltm.Identity{TenantID: scope.Tenant, UserID: scope.Subject, ActorID: scope.Deployment, ActorKind: "console", ContextID: "chat", GrantID: "grant", TaskID: "task", ExpiresAt: time.Now().Add(time.Hour)}
	if _, err := service.BindProject(ctx, id, "still-allowed", ltm.BindRequest{ContextID: "chat", ProjectID: "alpha"}); err != nil {
		t.Fatal("rejected control intent altered memory", err)
	}
}
