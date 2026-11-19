package main

import (
	"context"
	"errors"
	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
)

type runtimeBudgetAdmitter struct {
	store      *model.BudgetStore
	ids        *identity.Store
	limit      int64
	childLimit int64
	completed  func(context.Context, model.BudgetTask) (bool, error)
}

func newRuntimeBudgetAdmitter(ctx context.Context, pool *pgxpool.Pool, ids *identity.Store) (*runtimeBudgetAdmitter, error) {
	store := model.NewBudgetStore(pool)
	if err := store.Migrate(ctx); err != nil {
		return nil, err
	}
	completed, err := newTaskBudgetCompletion(ctx, pool)
	if err != nil {
		return nil, err
	}
	return &runtimeBudgetAdmitter{store: store, ids: ids, limit: execbridge.TaskTokenLimitFromEnv(), completed: completed, childLimit: execbridge.ChildTokenLimitFromEnv()}, nil
}

func taskBudgetLineage(ids *identity.Store) execbridge.BudgetLineage {
	return func(ctx context.Context, scope contracts.Scope, task string) ([]model.BudgetTask, error) {
		chain, err := ids.TaskAncestry(ctx, scope, task)
		if err != nil {
			return nil, err
		}
		out := make([]model.BudgetTask, len(chain))
		for i, g := range chain {
			out[i] = model.BudgetTask{Scope: g.Scope(), Task: g.Task}
		}
		return out, nil
	}
}

func (a *runtimeBudgetAdmitter) TaskBudget(ctx context.Context, scope contracts.Scope, task string) (model.Budget, error) {
	chain, err := taskBudgetLineage(a.ids)(ctx, scope, task)
	if err != nil {
		return model.Budget{}, err
	}
	chain[len(chain)-1].Owner = model.BudgetOwnerState
	for i := range chain {
		chain[i].Task = "a2a-task:" + chain[i].Task
	}
	if err = a.store.CloseCompletedScope(ctx, scope, execbridge.RootBudgetKey, a.completed); err != nil {
		return model.Budget{}, err
	}
	return a.store.EnsureTaskTree(ctx, chain, execbridge.RootBudgetKey, a.limit, a.childLimit)
}

func newTaskBudgetCompletion(ctx context.Context, pool *pgxpool.Pool) (func(context.Context, model.BudgetTask) (bool, error), error) {
	states := state.New(pool)
	if err := states.Migrate(ctx); err != nil {
		return nil, err
	}
	tasks, err := gateway.NewStore(pool, envOr("AHP_EXEC_SCHEMA", "ahp_exec"))
	if err != nil {
		return nil, err
	}
	if err = tasks.Migrate(ctx); err != nil {
		return nil, err
	}
	return func(ctx context.Context, task model.BudgetTask) (bool, error) {
		id := strings.TrimPrefix(task.Task, "a2a-task:")
		switch task.Owner {
		case model.BudgetOwnerState:
			return states.TaskTerminal(ctx, task.Scope, id)
		case model.BudgetOwnerGateway:
		default:
			// Legacy/unknown ownership cannot establish completion. Deletion
			// or an unrelated protocol row never turns absence into a refund.
			return false, nil
		}
		row, err := tasks.Task(ctx, task.Scope, id)
		if contracts.Code(err) == contracts.ErrNotFound || errors.Is(err, a2a.ErrTaskNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		switch row.State {
		case a2a.TaskStateCompleted, a2a.TaskStateFailed, a2a.TaskStateCanceled, a2a.TaskStateRejected:
			return true, nil
		}
		return false, nil
	}, nil
}
