//go:build ahp_data

package main

import (
	"context"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Task labels come from the live grant owner, never an SDK request. Human
// Workspace requests have their independent session+membership gates.
func dataArtifactGrantLabels(ids *identity.Store) artifacts.GrantLabels {
	return func(ctx context.Context, scope contracts.Scope) (labels.Set, error) {
		p, ok := transport.FromContext(ctx)
		if !ok || p.Scope != scope {
			return labels.Set{}, contracts.NewError(contracts.ErrForbidden, "artifact principal required")
		}
		if p.Claims.Kind == auth.KindHuman {
			return labels.Set{}, nil
		}
		if p.Claims.Kind != auth.KindTask {
			return labels.Set{}, contracts.NewError(contracts.ErrForbidden, "artifact task required")
		}
		grant, err := ids.Evaluate(ctx, scope, p.Claims.Task, "artifacts")
		if err != nil {
			return labels.Set{}, err
		}
		if grant.Context != p.Claims.Context {
			return labels.Set{}, contracts.NewError(contracts.ErrForbidden, "artifact grant context mismatch")
		}
		return grant.Labels, nil
	}
}
