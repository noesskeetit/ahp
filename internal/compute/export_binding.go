package compute

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// This table binds request identity, not execution status. Artifact publication
// and reconciliation stay with the existing Artifacts owner. Ordinary Computer
// destruction keeps its row; subject/tenant erasure removes bindings by FK.
const exportBindingDDL = `CREATE TABLE IF NOT EXISTS ahp_compute.export_request_bindings (
 tenant text NOT NULL, deployment text NOT NULL, supplied_key text NOT NULL,
 subject text NOT NULL, computer_id text NOT NULL REFERENCES ahp_compute.computers(id) ON DELETE CASCADE,
 task_id text NOT NULL, context_id text NOT NULL, source_path text NOT NULL,
 request_version integer NOT NULL DEFAULT 1, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (tenant,deployment,supplied_key)
);`

func (s *Store) bindExportRequest(ctx context.Context, scope contracts.Scope, key, computer, task, contextID, path string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if err := s.checkOwnerWritable(ctx, tx, scope); err != nil {
			return err
		}
		if _, err := s.getComputer(ctx, tx, scope, computer); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO ahp_compute.export_request_bindings(tenant,deployment,supplied_key,subject,computer_id,task_id,context_id,source_path) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING`, scope.Tenant, scope.Deployment, key, scope.Subject, computer, task, contextID, path); err != nil {
			return err
		}
		var owner, oldComputer, oldTask, oldContext, oldPath string
		var version int
		if err := tx.QueryRow(ctx, `SELECT subject,computer_id,task_id,context_id,source_path,request_version FROM ahp_compute.export_request_bindings WHERE tenant=$1 AND deployment=$2 AND supplied_key=$3 FOR UPDATE`, scope.Tenant, scope.Deployment, key).Scan(&owner, &oldComputer, &oldTask, &oldContext, &oldPath, &version); err != nil {
			return err
		}
		if owner != scope.Subject {
			return contracts.NewError(contracts.ErrForbidden, "export operation is not available")
		}
		if oldComputer != computer || oldTask != task || oldContext != contextID || oldPath != path || version != 1 {
			return contracts.NewError(contracts.ErrKeyReuse, "export key was already bound to another request")
		}
		return nil
	})
}
