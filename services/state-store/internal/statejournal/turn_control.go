package statejournal

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
)

type managedTurn struct {
	Mode      string
	Seq       int64
	Ack       int64
	Followups int
	Cancel    bool
	ExpiresAt *time.Time
}

func (turn managedTurn) validate(seq int64, ack *journal.SteerAck) error {
	if turn.Mode == journal.ExecutionVMManaged && (turn.Seq == 0 || seq != turn.Seq) {
		return contracts.NewError(contracts.ErrConflict, "stale turn_seq")
	}
	if turn.Mode != journal.ExecutionVMManaged && (ack != nil || seq != 0) {
		return contracts.NewError(contracts.ErrInvalid, "turn control requires a VM-managed session")
	}
	return nil
}

func peekControl(ctx context.Context, tx pgx.Tx, scope contracts.Scope, session string, turn managedTurn) (*journal.TurnControlDelta, error) {
	if turn.Mode != journal.ExecutionVMManaged || turn.Seq == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `
		SELECT seq,event_uid,role,codec,inline_payload
		FROM ahp_journal.session_events
		WHERE tenant_id=$1 AND session_id=$2 AND turn_seq=$3 AND event_type='turn.steer' AND is_active AND seq>$4
		ORDER BY seq LIMIT 64`, scope.Tenant, session, turn.Seq, turn.Ack)
	if err != nil {
		return nil, err
	}
	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Event, error) {
		event := Event{EventInput: EventInput{TurnSeq: turn.Seq, Type: "turn.steer"}, Active: true}
		err := row.Scan(&event.Seq, &event.ID, &event.Role, &event.Codec, &event.Payload.Inline)
		return event, err
	})
	if err != nil || (!turn.Cancel && len(events) == 0) {
		return nil, err
	}
	return &journal.TurnControlDelta{TurnSeq: turn.Seq, PendingSteer: events, CancelRequested: turn.Cancel}, nil
}

func applySteerAck(ctx context.Context, tx pgx.Tx, scope contracts.Scope, session string, turn *managedTurn, ack *journal.SteerAck) error {
	if ack == nil {
		return nil
	}
	if ack.TurnSeq != turn.Seq || ack.UpToSeq <= turn.Ack || ack.Count < 1 {
		return contracts.NewError(contracts.ErrConflict, "invalid steer acknowledgment")
	}
	var last int64
	var count int
	if err := tx.QueryRow(ctx, `
		SELECT coalesce(max(seq),0),count(*) FROM ahp_journal.session_events
		WHERE tenant_id=$1 AND session_id=$2 AND turn_seq=$3 AND event_type='turn.steer'
		  AND is_active AND seq>$4 AND seq<=$5`,
		scope.Tenant, session, turn.Seq, turn.Ack, ack.UpToSeq).Scan(&last, &count); err != nil {
		return err
	}
	if last != ack.UpToSeq || count != ack.Count {
		return contracts.NewError(contracts.ErrConflict, "steer acknowledgment skips pending input")
	}
	_, err := tx.Exec(ctx, `UPDATE ahp_journal.sessions SET steer_acked_seq=$3
		WHERE tenant_id=$1 AND session_id=$2`, scope.Tenant, session, last)
	if err == nil {
		turn.Ack = last
	}
	return err
}

func finishManagedTurn(ctx context.Context, tx pgx.Tx, scope contracts.Scope, session, outcome string, lease Lease, head int64, turn managedTurn) (*journal.TurnControlDelta, error) {
	terminal := journal.TurnCompleted{TurnSeq: turn.Seq, EndSeq: head + 1, Outcome: outcome}
	if !turn.Cancel {
		var pending bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(
			SELECT 1 FROM ahp_journal.session_events WHERE tenant_id=$1 AND session_id=$2
			AND turn_seq=$3 AND event_type='turn.steer' AND is_active AND seq>$4)`,
			scope.Tenant, session, turn.Seq, turn.Ack).Scan(&pending); err != nil {
			return nil, err
		}
		if pending {
			return nil, contracts.NewError(contracts.ErrConflict, "unacknowledged steer pending")
		}
	}
	var next *TurnLease
	if turn.Cancel {
		rows, err := tx.Query(ctx, `
			UPDATE ahp_journal.session_events SET is_active=false
			WHERE tenant_id=$1 AND session_id=$2 AND is_active
			  AND ((event_type='turn.steer' AND turn_seq=$3 AND seq>$4)
			    OR (event_type='turn.followup' AND turn_seq IS NULL))
			RETURNING seq`, scope.Tenant, session, turn.Seq, turn.Ack)
		if err != nil {
			return nil, err
		}
		terminal.CanceledControlSeqs, err = pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return nil, err
		}
		slices.Sort(terminal.CanceledControlSeqs)
		terminal.Outcome, turn.Followups = "INTERRUPTED", 0
	} else if outcome == SessionIdle && turn.Followups != 0 {
		next = &TurnLease{Lease: lease, SessionID: session, HeadSeq: terminal.EndSeq}
		err := tx.QueryRow(ctx, `
			UPDATE ahp_journal.session_events SET turn_seq=seq
			WHERE tenant_id=$1 AND session_id=$2 AND seq=(
				SELECT seq FROM ahp_journal.session_events
				WHERE tenant_id=$1 AND session_id=$2 AND event_type='turn.followup'
				  AND turn_seq IS NULL AND is_active ORDER BY seq LIMIT 1)
			RETURNING seq,inline_payload`, scope.Tenant, session).Scan(&next.InputSeq, &next.Input.Inline)
		if err != nil {
			return nil, err
		}
		terminal.NextTurnSeq = next.InputSeq
		turn.Followups--
	}

	data, err := json.Marshal(terminal)
	if err != nil {
		return nil, err
	}
	event := EventInput{ID: "turn.completed:" + strconv.FormatInt(turn.Seq, 10), Type: "turn.completed",
		TurnSeq: turn.Seq, Role: "system", Codec: "json", Payload: Payload{Inline: data}}
	if err := insertEvent(ctx, tx, scope.Tenant, session, terminal.EndSeq, event, nil, nil, nil); err != nil {
		return nil, err
	}
	if next == nil {
		_, err := tx.Exec(ctx, `UPDATE ahp_journal.sessions
			SET status=$3,head_seq=$4,turn_input_seq=NULL,queued_at=NULL,lease_owner=NULL,lease_expires_at=NULL,
			    cancel_requested=false,followup_count=$5,updated_at=clock_timestamp()
			WHERE tenant_id=$1 AND session_id=$2`,
			scope.Tenant, session, terminal.Outcome, terminal.EndSeq, turn.Followups)
		return nil, err
	}
	err = tx.QueryRow(ctx, `UPDATE ahp_journal.sessions
		SET head_seq=$3,turn_input_seq=$4,steer_acked_seq=$4,followup_count=$5,updated_at=clock_timestamp(),
		    lease_expires_at=greatest(lease_expires_at,clock_timestamp()+interval '60 seconds')
		WHERE tenant_id=$1 AND session_id=$2 RETURNING lease_expires_at`,
		scope.Tenant, session, terminal.EndSeq, next.InputSeq, turn.Followups).Scan(&next.ExpiresAt)
	if err != nil {
		return nil, err
	}
	return &journal.TurnControlDelta{TurnSeq: next.InputSeq, NextTurn: next}, nil
}

func (s *Store) HeartbeatLeaseWithControl(ctx context.Context, scope contracts.Scope, session string, lease Lease, ttl time.Duration) (Lease, *journal.TurnControlDelta, error) {
	if err := scope.Validate(); err != nil {
		return Lease{}, nil, err
	}
	if !validID(session) || !validID(lease.Worker) || lease.Epoch <= 0 || ttl < time.Millisecond || ttl > 15*time.Minute {
		return Lease{}, nil, contracts.NewError(contracts.ErrInvalid, "invalid lease heartbeat")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Lease{}, nil, err
	}
	defer tx.Rollback(ctx)
	var turn managedTurn
	err = tx.QueryRow(ctx, `UPDATE ahp_journal.sessions
		SET lease_expires_at=CASE WHEN execution_mode='vm_managed'
			THEN greatest(lease_expires_at,clock_timestamp()+$7*interval '1 microsecond')
			ELSE clock_timestamp()+$7*interval '1 microsecond' END
		WHERE tenant_id=$1 AND session_id=$2 AND deployment_id=$3 AND subject_id=$4
		  AND lease_owner=$5 AND lease_epoch=$6 AND lease_expires_at > clock_timestamp()
		  AND status <> 'DELETED'
		RETURNING lease_expires_at,execution_mode,coalesce(turn_input_seq,0),steer_acked_seq,cancel_requested`,
		scope.Tenant, session, scope.Deployment, scope.Subject, lease.Worker, lease.Epoch, ttl.Microseconds()).
		Scan(&lease.ExpiresAt, &turn.Mode, &turn.Seq, &turn.Ack, &turn.Cancel)
	if errors.Is(err, pgx.ErrNoRows) {
		err = contracts.NewError(contracts.ErrStaleEpoch, "session lease has expired or been superseded")
	}
	if err != nil {
		return Lease{}, nil, err
	}
	delta, err := peekControl(ctx, tx, scope, session, turn)
	if err != nil {
		return Lease{}, nil, err
	}
	return lease, delta, tx.Commit(ctx)
}

func (s *Store) CancelTurn(ctx context.Context, scope contracts.Scope, session string, target int64) (TurnStatus, error) {
	var status TurnStatus
	if err := scope.Validate(); err != nil {
		return status, err
	}
	if !validID(session) || target < 0 {
		return status, contracts.NewError(contracts.ErrInvalid, "invalid cancellation target")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return status, err
	}
	defer tx.Rollback(ctx)
	var turn managedTurn
	err = tx.QueryRow(ctx, `SELECT status,lease_epoch,head_seq,turn_input_seq,steer_acked_seq,followup_count
		FROM ahp_journal.sessions
		WHERE tenant_id=$1 AND session_id=$2 AND deployment_id=$3 AND subject_id=$4
		  AND execution_mode='vm_managed' AND status<>'DELETED' FOR UPDATE`,
		scope.Tenant, session, scope.Deployment, scope.Subject).
		Scan(&status.Status, &status.Epoch, &status.HeadSeq, &status.InputSeq, &turn.Ack, &turn.Followups)
	if errors.Is(err, pgx.ErrNoRows) {
		err = contracts.NewError(contracts.ErrNotFound, "VM session not found")
	}
	if err != nil {
		return status, err
	}
	if status.InputSeq == nil {
		return status, nil
	}
	turn.Seq, turn.Cancel = *status.InputSeq, true
	if target != 0 && target != turn.Seq {
		return status, contracts.NewError(contracts.ErrConflict, "stale cancellation target")
	}
	switch status.Status {
	case SessionQueued:
		if _, err := finishManagedTurn(ctx, tx, scope, session, "INTERRUPTED", Lease{}, status.HeadSeq, turn); err != nil {
			return TurnStatus{}, err
		}
		status.Status, status.HeadSeq, status.InputSeq = "INTERRUPTED", status.HeadSeq+1, nil
	case SessionActive:
		tag, err := tx.Exec(ctx, `UPDATE ahp_journal.sessions SET cancel_requested=true,updated_at=clock_timestamp()
			WHERE tenant_id=$1 AND session_id=$2 AND NOT cancel_requested`, scope.Tenant, session)
		if err != nil {
			return TurnStatus{}, err
		}
		if tag.RowsAffected() == 0 {
			return status, nil
		}
	default:
		return status, nil
	}
	if err := notifyJournal(ctx, tx, scope, session); err != nil {
		return TurnStatus{}, err
	}
	return status, tx.Commit(ctx)
}

func notifyJournal(ctx context.Context, tx pgx.Tx, scope contracts.Scope, session string) error {
	data, err := json.Marshal(journal.SessionSignal{Scope: scope, SessionID: session})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "SELECT pg_notify($1,$2)", journal.SessionSignalChannel, string(data))
	return err
}
