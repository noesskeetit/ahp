package main

import (
	"context"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// GrantLabels is the part of Identity the Memory mount needs to bound a write:
// read the live grant behind a task credential and report its accumulated label
// set. *identity.Store satisfies it through Evaluate.
type GrantLabels interface {
	GrantLabelSet(ctx context.Context, scope contracts.Scope, task, capability string) (labels.Set, error)
}

// memoryPlaneCapability is the coarse capability the Memory face guards every
// route with (internal/memory/memoryapi/api.go); the ceiling read asks Identity
// about the same capability the request already carries, so a grant that lost
// it cannot keep writing.
const memoryPlaneCapability = "memory"

// This seam and the capability name are shared by both builds: the tagged
// mount wires a real reader, the untagged build keeps the signature honest
// without importing internal/memory.

// identityGrantLabels adapts Identity's exported grant evaluation to the
// mount's GrantLabels seam, so the Memory ceiling reads exactly the label set
// Identity publishes for the live grant -- no second copy of the labels, no
// cached ceiling.
type identityGrantLabels struct {
	evaluate interface {
		Evaluate(ctx context.Context, scope contracts.Scope, task, capability string) (identity.Grant, error)
	}
}

func (a identityGrantLabels) GrantLabelSet(ctx context.Context, scope contracts.Scope,
	task, capability string) (labels.Set, error) {
	if a.evaluate == nil {
		return labels.Set{}, contracts.NewError(contracts.ErrForbidden,
			"ahp: memory ceiling has no identity store to read")
	}
	grant, err := a.evaluate.Evaluate(ctx, scope, task, capability)
	if err != nil {
		return labels.Set{}, err
	}
	return grant.Labels, nil
}

// GrantLabelWriter persists labels before Memory emits any source content.
type GrantLabelWriter interface {
	AddGrantLabels(context.Context, contracts.Scope, string, labels.Set) error
}

func (a identityGrantLabels) AddGrantLabels(ctx context.Context, scope contracts.Scope, task string, set labels.Set) error {
	writer, ok := a.evaluate.(interface {
		AddLabels(context.Context, contracts.Scope, string, labels.Set) (identity.Grant, error)
	})
	if !ok {
		return contracts.NewError(contracts.ErrUnavailable, "memory: Identity label journal writer unavailable")
	}
	_, err := writer.AddLabels(ctx, scope, task, set)
	return err
}
