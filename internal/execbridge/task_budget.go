package execbridge

import (
	"context"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type BudgetLineage func(context.Context, contracts.Scope, string) ([]model.BudgetTask, error)

// WithBudgetLineage is installed by the composition root using Identity's live
// owner API. It is mandatory on the production model face; nil is only useful
// for isolated ledger tests that have no Identity plane.
func (b *Bridge) WithBudgetLineage(lineage BudgetLineage) *Bridge {
	b.budgetLineage = lineage
	return b
}

func (b *Bridge) WithBudgetCompletion(completed func(context.Context, model.BudgetTask) (bool, error)) *Bridge {
	b.budgetCompleted = completed
	return b
}

func (b *Bridge) ReconcileBudgets(ctx context.Context) error {
	if b.budgetCompleted == nil {
		return nil
	}
	return b.budgets.CloseCompleted(ctx, RootBudgetKey, b.budgetCompleted)
}

func (b *Bridge) reconcileBudgetScope(ctx context.Context, scope contracts.Scope) error {
	if b.budgetCompleted == nil {
		return nil
	}
	return b.budgets.CloseCompletedScope(ctx, scope, RootBudgetKey, b.budgetCompleted)
}

func (b *Bridge) taskBudgetChain(ctx context.Context, scope contracts.Scope, task string) ([]model.BudgetTask, error) {
	if b.budgetLineage == nil {
		return []model.BudgetTask{{Scope: scope, Task: taskBudgetTask(task)}}, nil
	}
	chain, err := b.budgetLineage(ctx, scope, task)
	if err != nil {
		return nil, err
	}
	if len(chain) == 0 || chain[len(chain)-1].Scope != scope || chain[len(chain)-1].Task != task {
		return nil, contracts.NewError(contracts.ErrForbidden, "model: authoritative task ancestry mismatch")
	}
	chain[len(chain)-1].Owner = model.BudgetOwnerState
	for i := range chain {
		chain[i].Task = taskBudgetTask(chain[i].Task)
	}
	return chain, nil
}

func (b *Bridge) executionBudget(ctx context.Context, in gateway.ExecInput) (model.Budget, error) {
	task := string(in.Task.ID)
	if b.budgetLineage == nil {
		return b.TaskBudget(ctx, in.Scope, task)
	}
	grantTask, _ := in.Task.Metadata[gateway.MetaGrantTask].(string)
	deployment, _ := in.Task.Metadata[gateway.MetaGrantDeployment].(string)
	if grantTask == "" || deployment == "" {
		return model.Budget{}, contracts.NewError(contracts.ErrForbidden, "model: execution has no verified grant binding")
	}
	grantScope := in.Scope
	grantScope.Deployment = deployment
	chain, err := b.taskBudgetChain(ctx, grantScope, grantTask)
	if err != nil {
		return model.Budget{}, err
	}
	if in.Task.ParentTask == "" && in.Scope == grantScope {
		// Gateway ingress can name the same authenticated run with an A2A ID.
		// This is a representation, not another delegated trust boundary.
		if err = b.reconcileBudgetScope(ctx, in.Scope); err != nil {
			return model.Budget{}, err
		}
		budget, err := b.budgets.EnsureTaskTree(ctx, chain, RootBudgetKey, b.taskTokenLimit, b.childTokenLimit)
		if err != nil {
			return model.Budget{}, err
		}
		if err = b.budgets.BindTaskAlias(ctx, in.Scope, taskBudgetTask(task), RootBudgetKey, budget.ID); err != nil {
			return model.Budget{}, err
		}
		return budget, nil
	}
	if task != grantTask || in.Scope != grantScope {
		chain = append(chain, model.BudgetTask{Scope: in.Scope, Task: taskBudgetTask(task), Owner: model.BudgetOwnerGateway})
	}
	if err = b.reconcileBudgetScope(ctx, in.Scope); err != nil {
		return model.Budget{}, err
	}
	return b.budgets.EnsureTaskTree(ctx, chain, RootBudgetKey, b.taskTokenLimit, b.childTokenLimit)
}
