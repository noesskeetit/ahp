package gateway

import (
	"context"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// TaskSubject is a trusted owner lookup for migration of legacy Registry pins.
func (s *Store) TaskSubject(ctx context.Context, tenant, task string) (string, error) {
	var subject string
	err := s.pool.QueryRow(ctx, "SELECT subject FROM "+s.t("tasks")+" WHERE tenant=$1 AND task_id=$2", tenant, task).Scan(&subject)
	if err == pgx.ErrNoRows {
		return "", contracts.NewError(contracts.ErrNotFound, "gateway: task owner unavailable")
	}
	return subject, err
}

// FenceSubject cancels every task of one owner across deployments; unknown
// effects remain recoverable in the journal rather than becoming success.
func (s *Store) FenceSubject(ctx context.Context, tenant, subject string) error {
	ledger, err := operations.New(s.pool, s.schema)
	if err != nil {
		return err
	}
	if err = ledger.FenceSubject(ctx, tenant, subject); err != nil {
		return err
	}
	if _, err = s.pool.Exec(ctx, "UPDATE "+s.t("tasks")+" SET cancel_requested=true,updated_at=now() WHERE tenant=$1 AND subject=$2 AND terminal_at IS NULL", tenant, subject); err != nil {
		return err
	}
	// The row stops because this plane erased its subject, and that fact has to be in the
	// journal at the moment the row stops: no service is around these rows to say why
	// afterwards (DEF-J4-CHILD-TERMINAL-REASON). Written by the same statement that moves the
	// state, so the transition and its reason leave through one door. The ::text cast keeps
	// jsonb_build_object from asking an untyped parameter what it is.
	_, err = s.pool.Exec(ctx, "UPDATE "+s.t("tasks")+" SET state='TASK_STATE_CANCELED',status_version=status_version+1,"+
		"metadata=coalesce(metadata,'{}'::jsonb)||jsonb_build_object('"+MetadataCloseReason+"',$3::text),"+
		"terminal_at=now() WHERE tenant=$1 AND subject=$2 AND terminal_at IS NULL "+
		"AND state IN ('TASK_STATE_SUBMITTED','TASK_STATE_INPUT_REQUIRED','TASK_STATE_AUTH_REQUIRED') AND lease_owner=''",
		tenant, subject, CloseReasonSubjectDeleted)
	return err
}

// DeleteSubject atomically erases content and terminal ledger receipts. Losing
// those receipts before pending delivery/task reconciliation would lose proof.
func (s *Store) DeleteSubject(ctx context.Context, tenant, subject string) error {
	ledger, err := operations.New(s.pool, s.schema)
	if err != nil {
		return err
	}
	if err = s.FenceSubject(ctx, tenant, subject); err != nil {
		return err
	}
	return ledger.DeleteSubjectChecked(ctx, tenant, subject, func(ctx context.Context, tx pgx.Tx) error {
		var pending bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+s.t("tasks")+" WHERE tenant=$1 AND subject=$2 AND terminal_at IS NULL) OR EXISTS(SELECT 1 FROM "+s.t("outbox")+" WHERE tenant=$1 AND subject=$2 AND state NOT IN ('DONE','FAILED') AND attempts>0)", tenant, subject).Scan(&pending); err != nil {
			return err
		}
		if pending {
			return contracts.NewReasoned(contracts.ErrUnavailable, "subject_gateway_pending", "gateway: task or delivery outcome pending")
		}
		for _, table := range []string{"child_status_inbox", "push_configs", "outbox", "task_events", "messages", "tasks", "contexts"} {
			if _, err := tx.Exec(ctx, "DELETE FROM "+s.t(table)+" WHERE tenant=$1 AND subject=$2", tenant, subject); err != nil {
				return err
			}
		}
		return nil
	})
}
