//go:build ahp_exec

package main

import (
	"context"

	"github.com/ahp-platform/ahp/internal/execbridge"

	"time"
)

func (s *server) configureModelBudgets(ctx context.Context, b *execbridge.Bridge) error {
	ids := s.identityStore()
	b.WithBudgetLineage(taskBudgetLineage(ids))
	completed, err := newTaskBudgetCompletion(ctx, s.pool)
	if err != nil {
		return err
	}
	b.WithBudgetCompletion(completed)

	// The owner states are durable, so a restart can replay this reconciliation.
	// Admission also runs it synchronously before requesting a new allocation.
	go func() {
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				step, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := b.ReconcileBudgets(step)
				cancel()
				if err != nil && ctx.Err() == nil {
					s.logger.Warn("model budget completion reconciliation failed", "err", err)
				}
			}
		}
	}()
	return nil
}
