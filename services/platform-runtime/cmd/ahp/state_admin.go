package main

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type memorySubjectDeleter interface {
	AuthorizeSubjectDeletion(context.Context, contracts.Scope, string) error
	DeleteSubject(context.Context, contracts.Scope, string, ...string) (int, error)
}
type memoryAwareCascade struct {
	artifactCascade
	memory    memorySubjectDeleter
	workspace workspaceSubjectDeleter
}

// withMemoryDeletion adds the actual Memory owner API to the coordinator. It
// does not query another service's tables and stores no token in the outbox.
func withMemoryDeletion(cascade artifactCascade, memory memorySubjectDeleter, workspace ...workspaceSubjectDeleter) artifactCascade {
	if cascade == nil || memory == nil {
		return cascade
	}
	var ws workspaceSubjectDeleter
	if len(workspace) > 0 {
		ws = workspace[0]
	}
	return &memoryAwareCascade{artifactCascade: cascade, memory: memory, workspace: ws}
}
func (c *memoryAwareCascade) checkMemorySubject(ctx context.Context, scope contracts.Scope) error {
	principal, ok := transport.FromContext(ctx)
	if !ok || principal.Claims.Kind != auth.KindHuman || principal.Scope != scope {
		return contracts.NewError(contracts.ErrForbidden, "ahp: subject erasure requires its verified administrative target")
	}
	return c.memory.AuthorizeSubjectDeletion(ctx, scope, scope.Subject)
}
func (c *memoryAwareCascade) deleteMemorySubject(ctx context.Context, scope contracts.Scope, key string) error {
	if permitsDeletion(ctx, scope) {
		_, err := c.memory.DeleteSubject(ctx, scope, scope.Subject, key)
		return err
	}
	principal, ok := transport.FromContext(ctx)
	if !ok || principal.Claims.Kind != auth.KindHuman || principal.Scope != scope {
		return contracts.NewError(contracts.ErrForbidden, "ahp: subject erasure requires its verified administrative target")
	}
	_, err := c.memory.DeleteSubject(ctx, scope, scope.Subject, key)
	return err
}

func (c *memoryAwareCascade) workspaceOwner() workspaceSubjectDeleter { return c.workspace }
func (c *memoryAwareCascade) subjectScopes(ctx context.Context, scope contracts.Scope) ([]contracts.Scope, error) {
	owner, ok := c.artifactCascade.(interface {
		subjectScopes(context.Context, contracts.Scope) ([]contracts.Scope, error)
	})
	if !ok {
		return nil, contracts.NewError(contracts.ErrUnavailable, "ahp: artifact subject discovery unavailable")
	}
	return owner.subjectScopes(ctx, scope)
}

func (c *memoryAwareCascade) fenceSubject(ctx context.Context, scope contracts.Scope) error {
	owner, ok := c.artifactCascade.(interface {
		fenceSubject(context.Context, contracts.Scope) error
	})
	if !ok {
		return contracts.NewError(contracts.ErrUnavailable, "ahp: artifact write fence unavailable")
	}
	return owner.fenceSubject(ctx, scope)
}

// Tenant erasure requires all three owner interfaces; missing split-topology
// clients fail closed before the durable intent is admitted.
type tenantMemoryOwner interface {
	memorySubjectDeleter
	FenceTenant(context.Context, contracts.Scope) error
	TenantScopes(context.Context, contracts.Scope) ([]contracts.Scope, error)
}
type tenantWorkspaceOwner interface {
	workspaceSubjectDeleter
	FenceTenant(context.Context, contracts.Scope) error
	DeleteTenant(context.Context, contracts.Scope) error
	AuthorizeTenantDeletion(context.Context, contracts.Scope) error
}
type tenantArtifactOwner interface {
	fenceTenant(context.Context, contracts.Scope) error
	tenantScopes(context.Context, contracts.Scope) ([]contracts.Scope, error)
}

func (c *memoryAwareCascade) tenantOwners() (tenantArtifactOwner, tenantMemoryOwner, tenantWorkspaceOwner, error) {
	art, a := c.artifactCascade.(tenantArtifactOwner)
	mem, m := c.memory.(tenantMemoryOwner)
	ws, w := c.workspace.(tenantWorkspaceOwner)
	if !a || !m || !w {
		return nil, nil, nil, contracts.NewError(contracts.ErrUnavailable, "ahp: tenant erasure owners unavailable")
	}
	return art, mem, ws, nil
}
