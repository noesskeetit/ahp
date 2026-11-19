// Package transport carries verified identity across HTTP boundaries:
// mTLS-bound task workloads, public human routes and strict JSON helpers.
package transport

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Principal is a verified caller placed in the request context by the
// middlewares in this package.
type Principal struct {
	Claims auth.Claims
	Scope  contracts.Scope
}

type principalKey struct{}

// WithPrincipal returns ctx carrying p.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// FromContext extracts the principal set by a middleware, if any.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
