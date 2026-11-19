package identity

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"time"
)

// ConnectionTarget is operator registration copied by the ToolGW composition.
// It cannot be selected or widened by tool arguments or an HTTP request body.
type ConnectionTarget struct{ Connection, TokenURL, Audience, Resource, Scope string }

// RequireConnection supplements the live task grant with a current connection
// decision. It is called before and after a bounded external token exchange.
func (s *Store) RequireConnection(ctx context.Context, c auth.Claims, connection string) error {
	if err := s.Authorize(ctx, c, "tools"); err != nil {
		return err
	}
	return s.RequireResource(ctx, ResourceRequest{Scope: c.Scope(), Resource: Resource{Kind: "connection", ID: connection}, Action: "use", GrantID: c.Grant})
}

// MintConnectionAssertion is an in-process owner facade, deliberately not a
// public or task HTTP API. ToolGW alone owns the injected signer and targets.
func (s *Store) MintConnectionAssertion(ctx context.Context, c auth.Claims, target ConnectionTarget, signer *auth.Signer) (string, error) {
	if err := s.RequireConnection(ctx, c, target.Connection); err != nil {
		return "", err
	}
	g, err := s.Evaluate(ctx, c.Scope(), c.Task, "tools")
	if err != nil {
		return "", err
	}
	return signer.SignExchangeAssertion(auth.ExchangeAssertion{Audience: target.TokenURL, TargetAudience: target.Audience, Resource: target.Resource, Scope: target.Scope, Connection: target.Connection, Subject: c.Subject, Tenant: c.Tenant, Deployment: c.Deployment, Task: c.Task, Grant: g.ID, GrantEpoch: g.Epoch, Actor: "ahp:tool-gateway"})
}

// ValidateConnectionAssertion is the synthetic token authority's live owner
// read after signature verification. The assertion itself cannot revive a
// cancelled/expired grant or a revoked container/connection permission.
func (s *Store) ValidateConnectionAssertion(ctx context.Context, a auth.ExchangeAssertion) (time.Time, error) {
	scope := contracts.Scope{Tenant: a.Tenant, Subject: a.Subject, Deployment: a.Deployment}
	g, err := s.Evaluate(ctx, scope, a.Task, "tools")
	if err != nil {
		return time.Time{}, err
	}
	if g.ID != a.Grant || g.Epoch != a.GrantEpoch {
		return time.Time{}, forbidden("identity: stale exchange delegation")
	}
	if err = s.requireContainers(ctx, scope, []string{"tools"}, g.ID, g.Epoch); err != nil {
		return time.Time{}, err
	}
	if err = s.RequireResource(ctx, ResourceRequest{Scope: scope, Resource: Resource{Kind: "connection", ID: a.Connection}, Action: "use", GrantID: g.ID, GrantEpoch: g.Epoch}); err != nil {
		return time.Time{}, err
	}
	return g.ExpiresAt, nil
}
