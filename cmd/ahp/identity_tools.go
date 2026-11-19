package main

import (
	"context"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func (s *server) toolsConnectionAuthority() tools.ConnectionAuthority {
	ids := s.identityStore()
	return func(ctx context.Context, scope contracts.Scope, connection string) error {
		p, ok := transport.FromContext(ctx)
		if !ok || p.Claims.Kind != auth.KindTask || p.Scope != scope {
			return contracts.NewError(contracts.ErrForbidden, "tools: verified scoped task required")
		}
		return ids.RequireConnection(ctx, p.Claims, connection)
	}
}

func (s *server) issueToolAssertion(ctx context.Context, scope contracts.Scope, connection string, target tools.ExchangeRegistration) (string, error) {
	p, ok := transport.FromContext(ctx)
	if !ok || p.Claims.Kind != auth.KindTask || p.Scope != scope || s.exchangeSigner == nil {
		return "", contracts.NewError(contracts.ErrForbidden, "tools: trusted owner assertion unavailable")
	}
	return s.identityStore().MintConnectionAssertion(ctx, p.Claims, identity.ConnectionTarget{Connection: connection, TokenURL: target.TokenURL, Audience: target.Audience, Resource: target.Resource, Scope: target.Scope}, s.exchangeSigner)
}
