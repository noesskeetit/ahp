package model

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

func lockBudgetKey(ctx context.Context, tx pgx.Tx, scope contracts.Scope, task, key string) error {
	id := contracts.OperationKey(scope, task, "model.budget", key)
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, id)
	return err
}

func rejectBudgetAlias(ctx context.Context, tx pgx.Tx, scope contracts.Scope, task, key string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_model.budget_task_aliases WHERE tenant=$1 AND subject=$2 AND deployment=$3 AND task=$4 AND key=$5)`, scope.Tenant, scope.Subject, scope.Deployment, task, key).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return contracts.NewError(contracts.ErrConflict, "task is already bound to a budget alias")
	}
	return nil
}

// Every mutation takes the root lock before row locks, including sibling
// settlement and close. Parent references are immutable through the public API.
func lockBudgetTree(ctx context.Context, tx pgx.Tx, scope contracts.Scope, id string) (Budget, error) {
	s := &BudgetStore{}
	b, err := s.scanBudget(tx.QueryRow(ctx, `SELECT `+budgetCols+` FROM ahp_model.budgets WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4`, id, scope.Tenant, scope.Subject, scope.Deployment))
	if err != nil {
		return Budget{}, err
	}
	chain := []Budget{b}
	seen := map[string]bool{id: true}
	for b.Parent != "" {
		if seen[b.Parent] || len(chain) >= maxTreeDepth {
			return Budget{}, contracts.NewError(contracts.ErrConflict, "invalid budget ancestry")
		}
		b, err = s.scanBudget(tx.QueryRow(ctx, `SELECT `+budgetCols+` FROM ahp_model.budgets WHERE id=$1 AND tenant=$2 AND subject=$3`, b.Parent, scope.Tenant, scope.Subject))
		if err != nil {
			return Budget{}, err
		}
		seen[b.ID] = true
		chain = append(chain, b)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "model.budget-tree/"+b.ID); err != nil {
		return Budget{}, err
	}
	for i := len(chain) - 1; i >= 0; i-- {
		b, err = lockBudget(ctx, tx, chain[i].Scope, chain[i].ID)
		if err != nil {
			return Budget{}, err
		}
	}
	return b, nil
}

// An unclosed child may have exceeded its provider estimate before the charge
// propagates on Close. Block new work in the entire tree until it is reconciled.
func admitBudgetTree(ctx context.Context, tx pgx.Tx, id string) error {
	var overdrawn bool
	err := tx.QueryRow(ctx, `WITH RECURSIVE ancestors AS (
 SELECT id,parent FROM ahp_model.budgets WHERE id=$1
 UNION SELECT b.id,b.parent FROM ahp_model.budgets b JOIN ancestors a ON b.id=a.parent
), tree AS (
 SELECT id,spent,reserved,"limit",closed FROM ahp_model.budgets WHERE id IN (SELECT id FROM ancestors WHERE parent IS NULL)
 UNION SELECT b.id,b.spent,b.reserved,b."limit",b.closed FROM ahp_model.budgets b JOIN tree t ON b.parent=t.id
) SELECT EXISTS(SELECT 1 FROM tree WHERE NOT closed AND spent+reserved>"limit")`, id).Scan(&overdrawn)
	if err != nil {
		return err
	}
	if overdrawn {
		return contracts.NewError(contracts.ErrConflict, "budget tree has unsettled provider overrun")
	}
	return nil
}
