package main

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TenantDeletionOwner owns the write barrier, cancellation, reconciliation and
// final removal of one service's data. Unresolved effects must return an error.
type TenantDeletionOwner interface {
	FenceTenant(context.Context, string) error
	DeleteTenant(context.Context, string) error
}

var requiredTenantOwners = []string{"compute", "gateway", "model", "tools", "triggers"}

func (s *server) registerTenantDeletionOwner(name string, owner TenantDeletionOwner) {
	if s.tenantDeletionOwners == nil {
		s.tenantDeletionOwners = map[string]TenantDeletionOwner{}
	}
	s.tenantDeletionOwners[name] = owner
}
func (c *stateDeletionCoordinator) checkTenantOwners() error {
	for _, name := range requiredTenantOwners {
		if c.control.TenantOwners[name] == nil {
			return contracts.NewError(contracts.ErrUnavailable, "ahp: tenant deletion owner unavailable: "+name)
		}
	}
	return nil
}

func snapshotTenantOwners(owners map[string]TenantDeletionOwner) map[string]TenantDeletionOwner {
	snapshot := make(map[string]TenantDeletionOwner, len(owners))
	for name, owner := range owners {
		snapshot[name] = owner
	}
	return snapshot
}
