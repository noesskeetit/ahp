package statejournal

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// FindExpiredTurnTenants discovers expired native claims across all deployment grants.
func (s *Store) FindExpiredTurnTenants(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT DISTINCT tenant_id FROM ahp_journal.sessions
        WHERE status='ACTIVE' AND turn_input_seq IS NOT NULL AND NOT dots_fifo AND execution_mode='worker_pull'
          AND lease_expires_at<=statement_timestamp()
        ORDER BY tenant_id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// FindMaintenanceTenants finds tenants whose state may need a retention pass.
func (s *Store) FindMaintenanceTenants(ctx context.Context, retentionCutoff, stageCutoff time.Time) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
       SELECT tenant_id FROM ahp_journal.sessions
       WHERE status='DELETED' AND retention_swept_at IS NULL AND NOT legal_hold AND deleted_at<=$1
       UNION
       SELECT tenant_id FROM ahp_journal.sessions
       WHERE blob_stage_at<=$2 AND NOT legal_hold AND status<>'DELETED'
       ORDER BY tenant_id`, retentionCutoff, stageCutoff)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}
