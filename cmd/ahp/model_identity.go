//go:build ahp_exec

package main

import (
	"context"
	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

type identityContextLabels struct{ store *identity.Store }

func (j identityContextLabels) LabelsForContext(ctx context.Context, scope contracts.Scope, id string) (labels.Set, error) {
	set, found, err := j.store.ContextLabels(ctx, scope, id)
	if err != nil {
		return labels.Set{}, err
	}
	if !found {
		return labels.Set{}, execbridge.ErrTaskLabelJournalAbsent
	}
	return set, nil
}

func (j identityContextLabels) AuthorizeModelTask(ctx context.Context, scope contracts.Scope, task string) error {
	_, err := j.store.Evaluate(ctx, scope, task, "model")
	return err
}
