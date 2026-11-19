package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// registrySupplyChainOption resolves the deploy-admission supply-chain mode from
// operator environment, returning the trust roots (nil when disabled) and the
// resolved mode name. The Registry cannot enforce a supply chain it was not
// given roots for, and a silently-off verifier is exactly the "trust anything"
// posture the PDF pipeline forbids -- so the mode is mandatory the moment the
// registry plane is enabled and never inferred:
//
//   - AHP_REGISTRY_SUPPLY_CHAIN=enforce   -- require
//     AHP_REGISTRY_TRUST_ROOTS_PEM, a PATH (never an inlined key) to a PEM bundle
//     of operator CA roots; installs the verifier and admits images only on a
//     signature chaining to a root plus a matching SBOM.
//   - AHP_REGISTRY_SUPPLY_CHAIN=synthetic -- run a SYNTHETIC stand: no deploy
//     admission, nothing about an image is verified. The name is deliberate --
//     a synthetic result must never be reportable as real PKI verification. The
//     legacy spelling "disabled" means the same thing and is accepted for
//     existing harnesses, but the resolved mode is always reported as
//     "synthetic". Logged loudly by the caller; never the default.
//   - unset or any other value            -- construction fails closed. There is
//     no silent off, so a misconfigured front cannot deploy unchecked images.
//
// The roots are operator-controlled trust material read from the filesystem the
// operator manages; private signing keys belong to publishers and the security
// authority and never appear here or in the process environment.
func registrySupplyChainOption() (*registry.TrustRoots, string, error) {
	switch mode := strings.ToLower(strings.TrimSpace(envOr("AHP_REGISTRY_SUPPLY_CHAIN", ""))); mode {
	case "":
		return nil, "", fmt.Errorf("AHP_REGISTRY_SUPPLY_CHAIN must be 'enforce' or 'synthetic' when the registry plane is enabled; a silent off is not allowed")
	case "synthetic", "disabled":
		return nil, "synthetic", nil
	case "enforce":
		path := strings.TrimSpace(envOr("AHP_REGISTRY_TRUST_ROOTS_PEM", ""))
		if path == "" {
			return nil, "enforce", fmt.Errorf("AHP_REGISTRY_SUPPLY_CHAIN=enforce requires AHP_REGISTRY_TRUST_ROOTS_PEM pointing at an operator CA bundle")
		}
		bundle, err := os.ReadFile(path)
		if err != nil {
			return nil, "enforce", fmt.Errorf("read registry trust roots: %w", err)
		}
		roots, err := registry.NewTrustRootsFromPEM(bundle)
		if err != nil {
			return nil, "enforce", fmt.Errorf("registry trust roots: %w", err)
		}
		return roots, "enforce", nil
	default:
		return nil, "", fmt.Errorf("AHP_REGISTRY_SUPPLY_CHAIN must be 'enforce' or 'synthetic', got %q", mode)
	}
}

// registrySecurityAuthority authorizes the RecordSecurityVerdict route: a live,
// current human security administrator of the caller's tenant, expressed as a
// browser session whose user carries the registry:security-verdict /
// registry:admin permission or an admin role, presented from an allowed control
// deployment. A task credential is refused outright -- the artifact publisher's
// own runtime must never self-issue the clearance its image is gated on. The
// AttachSecurityAuthority guard already rejects KindTask before this callback
// runs; the deny here is deliberate redundancy, not the only line of defense.
func registrySecurityAuthority(store *identity.Store, deployments []string) transport.Authorize {
	allowed := map[string]bool{}
	for _, d := range deployments {
		if d != "" {
			allowed[d] = true
		}
	}
	return func(ctx context.Context, c auth.Claims, capability string) error {
		deny := contracts.NewError(contracts.ErrForbidden, "security verdicts require a current tenant security administrator")
		if store == nil || capability != registry.SecurityVerdictCapability {
			return deny
		}
		if c.Kind != auth.KindHuman || c.JTI == "" || !allowed[c.Deployment] {
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
			if p == registry.SecurityVerdictCapability || p == registry.AdminCapability {
				return store.RequireHumanContainers(ctx, c, capability)
			}
		}
		for _, role := range user.Roles {
			if role == "admin" || role == "tenant-admin" {
				return store.RequireHumanContainers(ctx, c, capability)
			}
		}
		return deny
	}
}
