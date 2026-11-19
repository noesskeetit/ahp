package model

import (
	"context"
	"fmt"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// BudgetTask is an authoritative task reference. Callers must obtain the whole
// root-to-leaf chain from Identity/gateway, never from request metadata.
const BudgetOwnerState = "state"
const BudgetOwnerGateway = "gateway"

type BudgetTask struct {
	Scope contracts.Scope
	Task  string
	Owner string
}

// EnsureTaskTree reserves every child from its parent's remaining capacity in
// one transaction. Persisted parent and scope are immutable across replay and
// process restart. A missing/changed ancestor never turns a child into a root.
func (s *BudgetStore) EnsureTaskTree(ctx context.Context, chain []BudgetTask, key string, limit int64, childPolicy ...int64) (Budget, error) {
	if len(chain) == 0 || len(chain) > maxTreeDepth || key == "" {
		return Budget{}, contracts.NewError(contracts.ErrInvalid, "bounded authoritative task ancestry required")
	}
	if err := validAmount(limit); err != nil {
		return Budget{}, err
	}
	childCap := int64(0)
	if len(childPolicy) > 1 {
		return Budget{}, contracts.NewError(contracts.ErrInvalid, "one child budget policy required")
	}
	if len(childPolicy) == 1 {
		childCap = childPolicy[0]
	}
	if childCap < 0 || childCap > maxAmount {
		return Budget{}, contracts.NewError(contracts.ErrInvalid, "invalid child budget policy")
	}
	seen := map[BudgetTask]bool{}
	for _, n := range chain {
		if err := n.Scope.Validate(); err != nil {
			return Budget{}, err
		}
		if n.Owner != "" && n.Owner != BudgetOwnerState && n.Owner != BudgetOwnerGateway {
			return Budget{}, contracts.NewError(contracts.ErrInvalid, "unknown task budget owner")
		}
		identity := n
		identity.Owner = ""
		if n.Task == "" || seen[identity] {
			return Budget{}, contracts.NewError(contracts.ErrInvalid, "invalid task ancestry")
		}
		if n.Scope.Tenant != chain[0].Scope.Tenant || n.Scope.Subject != chain[0].Scope.Subject {
			return Budget{}, contracts.NewError(contracts.ErrForbidden, "task ancestry changes owner")
		}
		seen[identity] = true
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Budget{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err = lockBudgetKey(ctx, tx, chain[0].Scope, chain[0].Task, key); err != nil {
		return Budget{}, err
	}
	var parent Budget
	for i, n := range chain {
		if i > 0 {
			if err := lockBudgetKey(ctx, tx, n.Scope, n.Task, key); err != nil {
				return Budget{}, err
			}
		}
		if err := rejectBudgetAlias(ctx, tx, n.Scope, n.Task, key); err != nil {
			return Budget{}, err
		}
		b, err := s.scanBudget(tx.QueryRow(ctx, `SELECT `+budgetCols+` FROM ahp_model.budgets WHERE tenant=$1 AND subject=$2 AND deployment=$3 AND task=$4 AND key=$5`, n.Scope.Tenant, n.Scope.Subject, n.Scope.Deployment, n.Task, key))
		if err == nil {
			if b.Parent != parent.ID {
				return Budget{}, contracts.NewError(contracts.ErrConflict, "persisted task budget ancestry mismatch")
			}
			if i == 0 {
				if b.Limit != limit {
					return Budget{}, contracts.NewError(contracts.ErrKeyReuse, fmt.Sprintf("root budget policy changed: stored=%d requested=%d", b.Limit, limit))
				}
				b, err = lockBudgetTree(ctx, tx, n.Scope, b.ID)
				if err != nil {
					return Budget{}, err
				}
			}
			if n.Owner != "" && b.Owner != n.Owner {
				if b.Owner != "" {
					return Budget{}, contracts.NewError(contracts.ErrConflict, "task budget owner is immutable")
				}
				if _, err = tx.Exec(ctx, `UPDATE ahp_model.budgets SET owner=$2 WHERE id=$1`, b.ID, n.Owner); err != nil {
					return Budget{}, err
				}
				b.Owner = n.Owner
			}
			parent = b
			continue
		}
		if contracts.Code(err) != contracts.ErrNotFound {
			return Budget{}, err
		}
		amount := limit
		if i > 0 {
			if parent.Closed {
				return Budget{}, contracts.NewError(contracts.ErrConflict, "parent budget closed")
			}
			if err = admitBudgetTree(ctx, tx, parent.ID); err != nil {
				return Budget{}, err
			}
			cap := childCap
			if cap == 0 {
				cap = max(int64(1), parent.Limit/2)
			}
			amount = min(cap, parent.Limit-parent.Spent-parent.Reserved)
			if amount <= 0 {
				return Budget{}, contracts.NewError(contracts.ErrConflict, "insufficient parent capacity")
			}
			if _, err = tx.Exec(ctx, `UPDATE ahp_model.budgets SET reserved=reserved+$2 WHERE id=$1`, parent.ID, amount); err != nil {
				return Budget{}, err
			}
		}
		dg, _ := digest("budget.task", chain[:i+1], key, limit, childCap)
		if i == 0 {
			dg, _ = digest("budget.root", n.Scope, n.Task, key, limit)
		}
		b, err = s.scanBudget(tx.QueryRow(ctx, `INSERT INTO ahp_model.budgets(id,parent,tenant,subject,deployment,task,key,"limit",digest,owner) VALUES($1,NULLIF($2,''),$3,$4,$5,$6,$7,$8,$9,$10) RETURNING `+budgetCols, contracts.NewID(), parent.ID, n.Scope.Tenant, n.Scope.Subject, n.Scope.Deployment, n.Task, key, amount, dg, n.Owner))
		if err != nil {
			return Budget{}, err
		}
		if i == 0 {
			if _, err = lockBudgetTree(ctx, tx, n.Scope, b.ID); err != nil {
				return Budget{}, err
			}
		}
		parent = b
	}
	if err = tx.Commit(ctx); err != nil {
		return Budget{}, err
	}
	return parent, nil
}

// TaskBudget reads an existing binding without creating capacity. Lifecycle and
// deletion recovery use this read: absence is never evidence of unused credit.
func (s *BudgetStore) TaskBudget(ctx context.Context, scope contracts.Scope, task, key string) (Budget, error) {
	if err := scope.Validate(); err != nil {
		return Budget{}, err
	}
	return s.scanBudget(s.pool.QueryRow(ctx, `SELECT `+budgetCols+` FROM ahp_model.budgets WHERE tenant=$1 AND subject=$2 AND deployment=$3 AND task=$4 AND key=$5`, scope.Tenant, scope.Subject, scope.Deployment, task, key))
}

// CloseCompleted returns unused child allocations only after the owning service
// proves durable terminal state. Unknown, deleted and pending work retain holds.
// Re-running this after a crash is safe: Close and its parent credit are atomic.
func (s *BudgetStore) CloseCompleted(ctx context.Context, key string, completed func(context.Context, BudgetTask) (bool, error)) error {
	return s.closeCompleted(ctx, contracts.Scope{}, key, completed)
}

// CloseCompletedScope limits synchronous admission reconciliation to the owner.
func (s *BudgetStore) CloseCompletedScope(ctx context.Context, scope contracts.Scope, key string, completed func(context.Context, BudgetTask) (bool, error)) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	return s.closeCompleted(ctx, scope, key, completed)
}

func (s *BudgetStore) closeCompleted(ctx context.Context, scope contracts.Scope, key string, completed func(context.Context, BudgetTask) (bool, error)) error {
	if completed == nil {
		return contracts.NewError(contracts.ErrInvalid, "completion proof reader required")
	}
	const batch = 128
	cursor := ""
	for {
		rows, err := s.pool.Query(ctx, `SELECT `+budgetCols+` FROM ahp_model.budgets
 WHERE key=$1 AND parent IS NOT NULL AND NOT closed AND ($2='' OR (tenant=$2 AND subject=$3))
 AND ($4='' OR id<$4) ORDER BY id DESC LIMIT 128`, key, scope.Tenant, scope.Subject, cursor)
		if err != nil {
			return err
		}
		var candidates []Budget
		for rows.Next() {
			b, err := s.scanBudget(rows)
			if err != nil {
				rows.Close()
				return err
			}
			candidates = append(candidates, b)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, b := range candidates {
			done, err := completed(ctx, BudgetTask{Scope: b.Scope, Task: b.Task, Owner: b.Owner})
			if err != nil {
				return err
			}
			if !done {
				continue
			}
			err = s.Close(ctx, b.Scope, b.ID)
			if err != nil && contracts.Code(err) != contracts.ErrConflict && contracts.Code(err) != contracts.ErrNotFound {
				return err
			}
		}
		if len(candidates) < batch {
			return nil
		}
		cursor = candidates[len(candidates)-1].ID
	}
}
