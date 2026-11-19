package main

import (
	"context"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Administrative requests require a control workload AND a live human session.
// A human's permission alone never makes an arbitrary agent a control plane.
func stateAdminAuthority(store *identity.Store, deployments []string) transport.Authorize {
	allowed := map[string]bool{}
	for _, d := range deployments {
		allowed[d] = true
	}
	return func(ctx context.Context, c auth.Claims, capability string) error {
		deny := contracts.NewError(contracts.ErrForbidden, "state administration requires a permitted control-plane human")
		switch capability {
		case stateapi.AdminCapability, stateapi.TenantDeleteCapability:
		default:
			return deny
		}
		if store == nil || c.Kind != auth.KindHuman || c.JTI == "" || !allowed[c.Deployment] {
			return deny
		}
		session, err := store.BrowserTokenActive(ctx, c.JTI)
		if err != nil || session.Tenant != c.Tenant || session.Subject != c.Subject {
			return deny
		}
		user, err := store.BrowserUserBySubject(ctx, c.Tenant, c.Subject)
		if err != nil {
			return deny
		}
		for _, permission := range user.Permissions {
			if permission == capability {
				return store.RequireHumanContainers(ctx, c, capability)
			}
		}
		return deny
	}
}

// The composed data faces recheck the same live authority as execution/state.
// A cryptographically valid but revoked JWT cannot continue reading files.
func dataLiveAuthority(store *identity.Store) transport.Authorize {
	return func(ctx context.Context, c auth.Claims, capability string) error {
		if c.Kind == auth.KindTask {
			return store.Authorize(ctx, c, capability)
		}
		deny := contracts.NewError(contracts.ErrForbidden, "data access requires a live authorized principal")
		if c.Kind != auth.KindHuman || c.JTI == "" || capability != "workspace" {
			return deny
		}
		session, err := store.BrowserTokenActive(ctx, c.JTI)
		if err != nil || session.Tenant != c.Tenant || session.Subject != c.Subject {
			return deny
		}
		user, err := store.BrowserUserBySubject(ctx, c.Tenant, c.Subject)
		if err != nil {
			return deny
		}
		for _, p := range user.Permissions {
			if p == capability {
				return store.RequireHumanContainers(ctx, c, capability)
			}
		}
		return deny
	}
}
