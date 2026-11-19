package state

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TaskTerminal is a lifecycle proof for budget reconciliation. No row (including
// a deleted task) is not completion. Every run for the task must be terminal.
func (s *Store) TaskTerminal(ctx context.Context, scope contracts.Scope, task string) (bool, error) {
	if err := scope.Validate(); err != nil {
		return false, err
	}
	var terminal bool
	err := s.pool.QueryRow(ctx, `SELECT count(*)>0 AND COALESCE(bool_and(r.status IN ('done','failed','canceled')),false)
 FROM ahp_state.runs r JOIN ahp_state.threads t ON t.id=r.thread_id
 WHERE t.tenant=$1 AND t.deployment=$2 AND t.subject=$3 AND r.task_id=$4`, scope.Tenant, scope.Deployment, scope.Subject, task).Scan(&terminal)
	return terminal, err
}
