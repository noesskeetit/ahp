package main

import (
	"context"
	"github.com/ahp-platform/ahp/internal/memory/ahpbridge"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func (a identityGrantLabels) MemoryGrant(ctx context.Context, scope contracts.Scope, task string) (ahpbridge.Grant, error) {
	if a.evaluate == nil {
		return ahpbridge.Grant{}, contracts.NewError(contracts.ErrForbidden, "Identity authority is absent")
	}
	g, err := a.evaluate.Evaluate(ctx, scope, task, "memory")
	if err != nil {
		return ahpbridge.Grant{}, err
	}
	return ahpbridge.Grant{ID: g.ID, ExpiresAt: g.ExpiresAt, Labels: g.Labels}, nil
}
func (a identityGrantLabels) AddMemoryLabels(ctx context.Context, scope contracts.Scope, task string, set labels.Set) error {
	return a.AddGrantLabels(ctx, scope, task, set)
}
