package model

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// BindTaskAlias connects another protocol representation of the same run to its
// existing budget. It creates no capacity; ownership and the target are immutable.
func (s *BudgetStore) BindTaskAlias(ctx context.Context, scope contracts.Scope, task, key, budgetID string) error {
	if task == "" || key == "" {
		return contracts.NewError(contracts.ErrInvalid, "budget alias task and key required")
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err = lockBudgetKey(ctx, tx, scope, task, key); err != nil {
		return err
	}
	b, err := s.scanBudget(tx.QueryRow(ctx, `SELECT `+budgetCols+` FROM ahp_model.budgets WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4`, budgetID, scope.Tenant, scope.Subject, scope.Deployment))
	if err != nil {
		return err
	}
	if b.Task == task && b.Key == key {
		return tx.Commit(ctx)
	}
	var occupied bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_model.budgets WHERE tenant=$1 AND subject=$2 AND deployment=$3 AND task=$4 AND key=$5)`, scope.Tenant, scope.Subject, scope.Deployment, task, key).Scan(&occupied); err != nil {
		return err
	}
	if occupied {
		return contracts.NewError(contracts.ErrConflict, "task already owns an independent budget")
	}
	_, err = tx.Exec(ctx, `INSERT INTO ahp_model.budget_task_aliases(tenant,subject,deployment,task,key,budget_id) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, scope.Tenant, scope.Subject, scope.Deployment, task, key, budgetID)
	if err != nil {
		return err
	}
	var target string
	if err = tx.QueryRow(ctx, `SELECT budget_id FROM ahp_model.budget_task_aliases WHERE tenant=$1 AND subject=$2 AND deployment=$3 AND task=$4 AND key=$5`, scope.Tenant, scope.Subject, scope.Deployment, task, key).Scan(&target); err != nil {
		return err
	}
	if target != budgetID {
		return contracts.NewError(contracts.ErrConflict, "task budget alias is immutable")
	}
	return tx.Commit(ctx)
}
