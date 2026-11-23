package statejournal

import (
	"context"
	"errors"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
)

func enqueueManagedTurn(ctx context.Context, tx pgx.Tx, scope contracts.Scope, req EnqueueRequest, key string, digest []byte, head, epoch, current int64, status string, followups int) (EnqueueReceipt, error) {
	mode := req.DeliveryMode
	if mode == "" {
		mode = journal.DeliveryTurn
	}
	canStart := status == SessionIdle || status == SessionWaitingInput || status == "INTERRUPTED"
	if canStart && req.TargetTurnSeq == 0 {
		mode = journal.DeliveryTurn
	}
	seq := head + 1
	event := EventInput{ID: key, Role: "user", Codec: "json", Payload: req.Input}
	switch mode {
	case journal.DeliveryTurn:
		if !canStart {
			return EnqueueReceipt{}, contracts.NewError(contracts.ErrConflict, "the session already has a turn")
		}
		event.Type, event.TurnSeq = "turn.input", seq
		status, current = SessionQueued, seq
	case journal.DeliverySteer:
		if status != SessionActive || (req.TargetTurnSeq != 0 && req.TargetTurnSeq != current) {
			return EnqueueReceipt{}, contracts.NewError(contracts.ErrConflict, "steer target is no longer active")
		}
		event.Type, event.TurnSeq = "turn.steer", current
	case journal.DeliveryFollowup:
		if (status != SessionQueued && status != SessionActive) || followups >= journal.MaxFollowups {
			return EnqueueReceipt{}, contracts.NewError(contracts.ErrConflict, "followup unavailable or queue is full")
		}
		event.Type = "turn.followup"
		followups++
	}
	if err := insertEvent(ctx, tx, scope.Tenant, req.SessionID, seq, event, key, digest, seq); err != nil {
		return EnqueueReceipt{}, err
	}
	receipt := EnqueueReceipt{SessionID: req.SessionID, HeadSeq: seq, Status: status, AcceptedMode: mode, FollowupCount: followups, Epoch: epoch}
	_, err := tx.Exec(ctx, `
		UPDATE ahp_journal.session_events SET admission_receipt=$4
		WHERE tenant_id=$1 AND session_id=$2 AND seq=$3`, scope.Tenant, req.SessionID, seq, receipt)
	if err != nil {
		return EnqueueReceipt{}, err
	}
	_, err = tx.Exec(ctx, `
		UPDATE ahp_journal.sessions
		SET head_seq=$3, status=$4, turn_input_seq=$5, followup_count=$6, updated_at=clock_timestamp(),
		    queued_at=CASE WHEN $7 THEN clock_timestamp() ELSE queued_at END,
		    steer_acked_seq=CASE WHEN $7 THEN $5 ELSE steer_acked_seq END,
		    cancel_requested=CASE WHEN $7 THEN false ELSE cancel_requested END
		WHERE tenant_id=$1 AND session_id=$2`,
		scope.Tenant, req.SessionID, seq, status, current, followups, mode == journal.DeliveryTurn)
	return receipt, err
}

func (s *Store) ClaimVMTurn(ctx context.Context, scope contracts.Scope, session string, req journal.VMTurnClaimRequest, ttl time.Duration) (TurnLease, error) {
	var turn TurnLease
	if err := scope.Validate(); err != nil {
		return turn, err
	}
	if !validID(session) || req.TurnSeq <= 0 || req.Generation <= 0 || ttl < time.Millisecond || ttl > 15*time.Minute ||
		req.ExecutionID != journal.DeterministicExecutionID(scope, session, req.TurnSeq, req.Generation) {
		return turn, contracts.NewError(contracts.ErrInvalid, "invalid VM claim")
	}
	err := s.pool.QueryRow(ctx, `
		WITH claimed AS (
			UPDATE ahp_journal.sessions s
			SET status='ACTIVE',lease_epoch=$6,lease_owner=$7,
			    lease_expires_at=clock_timestamp()+$8*interval '1 microsecond',updated_at=clock_timestamp()
			WHERE tenant_id=$1 AND deployment_id=$2 AND subject_id=$3 AND session_id=$4
			  AND execution_mode='vm_managed' AND status='QUEUED' AND turn_input_seq=$5
			  AND lease_epoch=$6-1
			RETURNING tenant_id,session_id,turn_input_seq,head_seq,lease_owner,lease_epoch,lease_expires_at
		)
		SELECT c.session_id,c.turn_input_seq,c.head_seq,c.lease_owner,c.lease_epoch,c.lease_expires_at,e.inline_payload,e.blob_ref
		FROM claimed c JOIN ahp_journal.session_events e
		  ON (e.tenant_id,e.session_id,e.seq)=(c.tenant_id,c.session_id,c.turn_input_seq)`,
		scope.Tenant, scope.Deployment, scope.Subject, session, req.TurnSeq, req.Generation, req.ExecutionID, ttl.Microseconds()).
		Scan(&turn.SessionID, &turn.InputSeq, &turn.HeadSeq, &turn.Worker, &turn.Epoch, &turn.ExpiresAt, &turn.Input.Inline, &turn.Input.Blob)
	if errors.Is(err, pgx.ErrNoRows) {
		err = contracts.NewError(contracts.ErrStaleEpoch, "VM turn is not ready for this execution")
	}
	return turn, err
}

func (s *Store) RequeueFencedVMTurn(ctx context.Context, scope contracts.Scope, session string, req journal.FenceRequeueRequest) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if !validID(session) || !validID(req.ExecutionID) || req.TurnSeq <= 0 || req.Epoch <= 0 {
		return contracts.NewError(contracts.ErrInvalid, "invalid fenced requeue")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE ahp_journal.sessions
		SET status='QUEUED',lease_owner=NULL,lease_expires_at=NULL,updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND deployment_id=$2 AND subject_id=$3 AND session_id=$4
		  AND execution_mode='vm_managed' AND status='ACTIVE' AND turn_input_seq=$5
		  AND lease_epoch=$6 AND lease_owner=$7 AND lease_expires_at<=statement_timestamp()`,
		scope.Tenant, scope.Deployment, scope.Subject, session, req.TurnSeq, req.Epoch, req.ExecutionID)
	if err == nil && tag.RowsAffected() != 1 {
		err = contracts.NewError(contracts.ErrConflict, "expired VM claim changed before requeue")
	}
	return err
}

func (s *Store) ListRecoverableVMSessions(ctx context.Context, scope contracts.Scope, limit int) ([]journal.VMSession, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 1000 {
		return nil, contracts.NewError(contracts.ErrInvalid, "invalid VM reconcile limit")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT session_id,turn_input_seq,lease_epoch,coalesce(lease_owner,''),status,harness_type
		FROM ahp_journal.sessions
		WHERE tenant_id=$1 AND deployment_id=$2 AND subject_id=$3 AND execution_mode='vm_managed'
		  AND ((status='QUEUED' AND queued_at<=statement_timestamp()-interval '2 seconds')
		    OR (status='ACTIVE' AND turn_input_seq IS NOT NULL AND lease_expires_at<=statement_timestamp()))
		ORDER BY updated_at,session_id LIMIT $4`, scope.Tenant, scope.Deployment, scope.Subject, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (journal.VMSession, error) {
		result := journal.VMSession{Scope: scope}
		err := row.Scan(&result.SessionID, &result.TurnSeq, &result.Epoch, &result.ExecutionID, &result.Status, &result.Harness)
		return result, err
	})
}
