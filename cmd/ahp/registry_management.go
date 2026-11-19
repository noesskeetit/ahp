package main

import (
	"context"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"net/http"
)

func attachRegistryManagement(h http.Handler, v *auth.Verifier, ids *identity.Store, deployments []string, audience string) error {
	allowed := map[string]bool{}
	for _, d := range deployments {
		allowed[d] = true
	}
	return registry.AttachManagementAuthority(h, v, audience, func(ctx context.Context, c auth.Claims, capability string) error {
		deny := contracts.NewError(contracts.ErrForbidden, "Registry administration requires current tenant administrator rights")
		if ids == nil || capability != registry.AdminCapability {
			return deny
		}
		if c.Kind == auth.KindTask {
			return ids.Authorize(ctx, c, capability)
		}
		if c.Kind != auth.KindHuman || c.JTI == "" || !allowed[c.Deployment] {
			return deny
		}
		session, err := ids.BrowserTokenActive(ctx, c.JTI)
		if err != nil || session.Tenant != c.Tenant || session.Subject != c.Subject {
			return deny
		}
		user, err := ids.BrowserUserBySubject(ctx, c.Tenant, c.Subject)
		if err != nil {
			return deny
		}
		registryAllowed := false
		for _, p := range user.Permissions {
			if p == registry.AdminCapability {
				return ids.RequireHumanContainers(ctx, c, capability)
			}
			if p == "registry" {
				registryAllowed = true
			}
		}
		if registryAllowed {
			for _, role := range user.Roles {
				if role == "admin" || role == "tenant-admin" {
					return ids.RequireHumanContainers(ctx, c, capability)
				}
			}
		}
		return deny
	})
}
