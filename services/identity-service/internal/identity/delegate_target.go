package identity

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DelegationTarget is server-resolved manifest/installation policy. It must
// never be constructed from an untrusted agent's request body. Zero ceilings
// select the same bounded defaults as a root manifest.
type DelegationTarget struct {
	Tenant       string
	Deployment   string
	Capabilities []string
	MaxDepth     int
	MaxFanout    int
	MaxActive    int
	AllowSelf    bool
}

// DelegateToTarget is the trusted gateway/service entry point. The transport
// authenticates that broker separately from the live parent task. Tenant and
// subject cannot change; the target manifest and the parent are both ceilings.
// Ordinary agents continue to use Delegate, which cannot change deployment.
func (s *Store) DelegateToTarget(ctx context.Context, parentScope contracts.Scope, parentTask string, r DelegateRequest, target DelegationTarget) (Grant, error) {
	if target.Tenant != parentScope.Tenant || target.Deployment == "" || target.Deployment != r.Deployment {
		return Grant{}, forbidden("identity: target policy does not match the delegated tenant/deployment")
	}
	caps, err := normalizeCapabilities(target.Capabilities)
	if err != nil {
		return Grant{}, err
	}
	if len(caps) == 0 || !capsSubset(caps, r.Capabilities) {
		return Grant{}, forbidden("identity: delegation exceeds target manifest capabilities")
	}
	if r.AllowSelf && !target.AllowSelf {
		return Grant{}, forbidden("identity: target manifest forbids self delegation")
	}
	target.Capabilities = caps
	target.MaxDepth, target.MaxFanout, target.MaxActive, err = normalizeLimits(RootRequest{MaxDepth: target.MaxDepth, MaxFanout: target.MaxFanout, MaxActive: target.MaxActive})
	if err != nil {
		return Grant{}, err
	}
	return s.delegate(ctx, parentScope, parentTask, r, &target)
}
