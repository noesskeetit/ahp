package identity

import (
	"context"
	"fmt"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
)

// FenceExecution prevents a task grant from being created after the execution
// has been fenced and revokes a grant that already exists. Both operations
// share one transaction and the same task lock as root creation and delegation.
func (s *Store) FenceExecution(ctx context.Context, scope contracts.Scope, executionID string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if !journal.ValidExecutionID(executionID) {
		return invalid("identity: invalid execution id")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.runTxDetached(func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))",
			"identity-tenant-erasure/"+scope.Tenant); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))",
			"identity-subject-erasure/"+scope.Tenant+"/"+scope.Subject); err != nil {
			return err
		}
		revoked, err := subjectRevoked(ctx, tx, scope)
		if err != nil || revoked {
			return err
		}
		if err := lockTaskKey(ctx, tx, scope, executionID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO ahp_identity.execution_fences (tenant, deployment, subject, task)
VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
			scope.Tenant, scope.Deployment, scope.Subject, executionID); err != nil {
			return fmt.Errorf("identity: record execution fence: %w", err)
		}
		g, found, err := findGrant(ctx, tx, scope, executionID)
		if err != nil || !found {
			return err
		}
		if err := lockRoot(ctx, tx, g.Root); err != nil {
			return err
		}
		return revokeSubtreeTx(ctx, tx, g)
	})
}

func requireUnfencedExecution(ctx context.Context, tx pgx.Tx, scope contracts.Scope, task string) error {
	if !journal.ValidExecutionID(task) {
		return nil
	}
	var fenced bool
	err := tx.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM ahp_identity.execution_fences
               WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND task=$4)`,
		scope.Tenant, scope.Deployment, scope.Subject, task).Scan(&fenced)
	if err != nil {
		return err
	}
	if fenced {
		return forbidden("identity: execution is fenced")
	}
	return nil
}
