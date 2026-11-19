package tools

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// PersonalOAuth returns the configured lifecycle; absence is a refusal, never fake readiness.
func (s *Service) PersonalOAuth() (*OAuthManager, error) {
	if s.cfg.OAuth == nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "oauth: personal connections not configured")
	}
	return s.cfg.OAuth, nil
}

// FenceOAuthTenant is the owner hook used by the tenant deletion coordinator.
func (s *Service) FenceOAuthTenant(ctx context.Context, tenant string) error {
	m, e := s.PersonalOAuth()
	if e != nil {
		return e
	}
	return m.FenceTenant(ctx, tenant)
}

// DeleteOAuthTenant removes personal secrets and metadata after permanent fencing.
func (s *Service) DeleteOAuthTenant(ctx context.Context, tenant string) error {
	m, e := s.PersonalOAuth()
	if e != nil {
		return e
	}
	return m.DeleteTenant(ctx, tenant)
}

// OAuthConnections exposes only a human owner's safe metadata.
func (s *Service) OAuthConnections(ctx context.Context, a auth.Claims) ([]OAuthConnection, error) {
	m, e := s.PersonalOAuth()
	if e != nil {
		return nil, e
	}
	return m.Connections(ctx, a)
}
