package statejournal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
)

const (
	SessionIdle         = "IDLE"
	SessionQueued       = "QUEUED"
	SessionActive       = "ACTIVE"
	SessionWaitingInput = "WAITING_INPUT"
)

type EnqueueRequest = journal.EnqueueRequest

type EnqueueReceipt = journal.EnqueueReceipt

type TurnLease = journal.TurnLease

type TurnStatus = journal.TurnStatus

func (s *Store) GetTurnStatus(ctx context.Context, scope contracts.Scope, session string) (TurnStatus, error) {
	var status TurnStatus
	if err := scope.Validate(); err != nil {
		return status, err
	}
	if !validID(session) {
		return status, contracts.NewError(contracts.ErrInvalid, "invalid session identifier")
	}
	err := s.pool.QueryRow(ctx, `
		SELECT status,lease_epoch,head_seq,turn_input_seq FROM ahp_journal.sessions
		WHERE tenant_id=$1 AND session_id=$2 AND deployment_id=$3 AND subject_id=$4 AND status<>'DELETED'`,
		scope.Tenant, session, scope.Deployment, scope.Subject).Scan(&status.Status, &status.Epoch, &status.HeadSeq, &status.InputSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		err = contracts.NewError(contracts.ErrNotFound, "session not found")
	}
	return status, err
}

// EnqueueTurn commits the input event and its dispatch position together. A
// retry returns the original receipt regardless of the turn's current status.
func (s *Store) EnqueueTurn(ctx context.Context, scope contracts.Scope, req EnqueueRequest) (EnqueueReceipt, error) {
	receipt, err := s.enqueueTurn(ctx, scope, req, "")
	return receipt.EnqueueReceipt, err
}

// EnqueueDotsTurn admits a run into an opt-in FIFO without replacing the head
// or resetting its lease. Only the authenticated task may claim its run.
func (s *Store) EnqueueDotsTurn(ctx context.Context, scope contracts.Scope, task string, req journal.DotsEnqueueRequest) (journal.DotsEnqueueReceipt, error) {
	if !validID(task) {
		return journal.DotsEnqueueReceipt{}, contracts.NewError(contracts.ErrInvalid, "invalid Dots task")
	}
	return s.enqueueTurn(ctx, scope, EnqueueRequest{
		SessionID: req.SessionID, Harness: "dots", FormatVersion: 1, Key: req.Key, Input: req.Input,
	}, task)
}

func (s *Store) enqueueTurn(ctx context.Context, scope contracts.Scope, req EnqueueRequest, dotTask string) (journal.DotsEnqueueReceipt, error) {
	var receipt journal.DotsEnqueueReceipt
	if err := scope.Validate(); err != nil {
		return receipt, err
	}
	if req.ExecutionMode != "" && req.ExecutionMode != journal.ExecutionPull && req.ExecutionMode != journal.ExecutionVMManaged {
		return receipt, contracts.NewError(contracts.ErrInvalid, "invalid execution mode")
	}
	if req.DeliveryMode != "" && req.DeliveryMode != journal.DeliveryTurn && req.DeliveryMode != journal.DeliverySteer && req.DeliveryMode != journal.DeliveryFollowup {
		return receipt, contracts.NewError(contracts.ErrInvalid, "invalid delivery mode")
	}
	if req.TargetTurnSeq < 0 || (req.TargetTurnSeq != 0 && req.DeliveryMode != journal.DeliverySteer) {
		return receipt, contracts.NewError(contracts.ErrInvalid, "target turn is only valid for steer")
	}
	if !validID(req.SessionID) || !validID(req.Harness) || !validID(req.Key) ||
		req.FormatVersion < 1 || req.Input.Blob != nil ||
		!validPayload(req.Input) || !json.Valid(req.Input.Inline) {
		return receipt, contracts.NewError(contracts.ErrInvalid, "invalid turn admission")
	}
	dots := dotTask != ""
	var admission any = req
	if dots {
		admission = struct {
			EnqueueRequest
			Task string
		}{req, dotTask}
	}
	data, err := json.Marshal(admission)
	if err != nil {
		return receipt, err
	}
	digest := sha256.Sum256(data)
	keyHash := sha256.Sum256([]byte(req.Key))
	key := "turn.input:" + hex.EncodeToString(keyHash[:])

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return receipt, err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `
		INSERT INTO ahp_journal.sessions
			(tenant_id,session_id,deployment_id,subject_id,harness_type,format_version,dots_fifo,execution_mode)
		VALUES ($1,$2,$3,$4,$5,$6,$7,coalesce(nullif($8,''),'worker_pull')) ON CONFLICT DO NOTHING`,
		scope.Tenant, req.SessionID, scope.Deployment, scope.Subject, req.Harness, req.FormatVersion, dots, req.ExecutionMode)
	if err != nil {
		return receipt, err
	}
	var head int64
	var status, executionMode string
	var hasLease bool
	var turnSeq, epoch int64
	var followups int
	err = tx.QueryRow(ctx, `
		SELECT head_seq,status,coalesce(lease_expires_at>clock_timestamp(),false),
		       execution_mode,coalesce(turn_input_seq,0),lease_epoch,followup_count
		FROM ahp_journal.sessions
		WHERE tenant_id=$1 AND session_id=$2 AND deployment_id=$3 AND subject_id=$4
		  AND harness_type=$5 AND format_version=$6 AND dots_fifo=$7 AND status<>'DELETED'
		  AND ($8='' OR execution_mode=$8)
		FOR UPDATE`, scope.Tenant, req.SessionID, scope.Deployment, scope.Subject, req.Harness, req.FormatVersion, dots, req.ExecutionMode).
		Scan(&head, &status, &hasLease, &executionMode, &turnSeq, &epoch, &followups)
	if errors.Is(err, pgx.ErrNoRows) {
		return receipt, contracts.NewError(contracts.ErrConflict, "session unavailable for admission")
	}
	if err != nil {
		return receipt, err
	}
	receipt.SessionID = req.SessionID
	var stored []byte
	var admissionReceipt *EnqueueReceipt
	err = tx.QueryRow(ctx, `
		SELECT result_head_seq,request_digest,coalesce(dots_run_id,''),coalesce(dots_run_sequence,0),admission_receipt
		FROM ahp_journal.session_events
		WHERE tenant_id=$1 AND session_id=$2 AND operation_key=$3`,
		scope.Tenant, req.SessionID, key).Scan(&receipt.HeadSeq, &stored, &receipt.RunID, &receipt.RunSequence, &admissionReceipt)
	switch {
	case err == nil && bytes.Equal(stored, digest[:]):
		if admissionReceipt != nil {
			receipt.EnqueueReceipt = *admissionReceipt
			receipt.Replayed = true
		}
		return receipt, nil
	case err == nil:
		return journal.DotsEnqueueReceipt{}, contracts.NewError(contracts.ErrKeyReuse, "turn key has a different input")
	case !errors.Is(err, pgx.ErrNoRows):
		return journal.DotsEnqueueReceipt{}, err
	case executionMode == journal.ExecutionVMManaged:
		admitted, err := enqueueManagedTurn(ctx, tx, scope, req, key, digest[:], head, epoch, turnSeq, status, followups)
		if err != nil {
			return journal.DotsEnqueueReceipt{}, err
		}
		receipt.EnqueueReceipt = admitted
		return receipt, tx.Commit(ctx)
	case req.DeliveryMode != "" && req.DeliveryMode != journal.DeliveryTurn:
		return journal.DotsEnqueueReceipt{}, contracts.NewError(contracts.ErrConflict, "turn control requires a VM-managed session")
	case !dots && (hasLease || (status != SessionIdle && status != SessionWaitingInput)):
		return journal.DotsEnqueueReceipt{}, contracts.NewError(contracts.ErrConflict, "the session already has a turn")
	case dots && status != SessionIdle && status != SessionQueued && status != SessionActive:
		return journal.DotsEnqueueReceipt{}, contracts.NewError(contracts.ErrConflict, "Dots session unavailable for queueing")
	}

	receipt.HeadSeq = head + 1
	event := EventInput{ID: key, Type: "turn.input", Role: "user", Codec: "json", Payload: req.Input}
	if err := insertEvent(ctx, tx, scope.Tenant, req.SessionID, receipt.HeadSeq, event, key, digest[:], receipt.HeadSeq); err != nil {
		return journal.DotsEnqueueReceipt{}, err
	}
	if dots {
		receipt.RunID = contracts.NewID()
		err = tx.QueryRow(ctx, `
			WITH advanced AS (
				UPDATE ahp_journal.sessions
				SET head_seq=$3,dots_run_sequence=dots_run_sequence+1,updated_at=clock_timestamp(),
				    status=CASE WHEN turn_input_seq IS NULL THEN 'QUEUED' ELSE status END,
				    queued_at=coalesce(queued_at,clock_timestamp()),turn_input_seq=coalesce(turn_input_seq,$3)
				WHERE tenant_id=$1 AND session_id=$2
				RETURNING dots_run_sequence
			)
			UPDATE ahp_journal.session_events e
			SET dots_run_id=$4,dots_task_id=$5,dots_run_sequence=advanced.dots_run_sequence
			FROM advanced
			WHERE e.tenant_id=$1 AND e.session_id=$2 AND e.seq=$3
			RETURNING e.dots_run_sequence`,
			scope.Tenant, req.SessionID, receipt.HeadSeq, receipt.RunID, dotTask).Scan(&receipt.RunSequence)
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE ahp_journal.sessions
			SET head_seq=$3,turn_input_seq=$3,status='QUEUED',queued_at=clock_timestamp(),updated_at=clock_timestamp(),
			    lease_owner=NULL,lease_expires_at=NULL
			WHERE tenant_id=$1 AND session_id=$2`, scope.Tenant, req.SessionID, receipt.HeadSeq)
	}
	if err != nil {
		return journal.DotsEnqueueReceipt{}, err
	}
	return receipt, tx.Commit(ctx)
}

// ClaimTurn takes a specific turn or the next ready turn for the authorised
// scope. A queued input survives lease turnover and is returned on every claim.
func (s *Store) ClaimTurn(ctx context.Context, scope contracts.Scope, session, worker string, ttl time.Duration) (TurnLease, error) {
	turn, err := s.claimTurn(ctx, scope, session, worker, ttl, false)
	return turn.TurnLease, err
}

// ClaimDotsTurn takes a Dots FIFO head bound to the verified task. Workload-only
// dispatch and minting a run's task token belong to a separate access path.
func (s *Store) ClaimDotsTurn(ctx context.Context, scope contracts.Scope, session, task string, ttl time.Duration) (journal.DotsTurnLease, error) {
	return s.claimTurn(ctx, scope, session, task, ttl, true)
}

func (s *Store) claimTurn(ctx context.Context, scope contracts.Scope, session, worker string, ttl time.Duration, dots bool) (journal.DotsTurnLease, error) {
	var turn journal.DotsTurnLease
	if err := scope.Validate(); err != nil {
		return turn, err
	}
	if (session != "" && !validID(session)) || !validID(worker) || ttl < time.Millisecond || ttl > 15*time.Minute {
		return turn, contracts.NewError(contracts.ErrInvalid, "invalid turn claim")
	}
	err := s.pool.QueryRow(ctx, `
		WITH ready AS MATERIALIZED (
			SELECT s.tenant_id,s.session_id FROM ahp_journal.sessions s
			WHERE s.tenant_id=$1 AND s.deployment_id=$2 AND s.subject_id=$3
			  AND ($4='' OR s.session_id=$4) AND s.dots_fifo=$7 AND s.execution_mode='worker_pull'
			  AND (s.status='QUEUED' OR (s.status='ACTIVE' AND s.turn_input_seq IS NOT NULL
				 AND s.lease_expires_at<=statement_timestamp()))
			  AND (NOT $7 OR EXISTS (
				SELECT 1 FROM ahp_journal.session_events e
				WHERE e.tenant_id=s.tenant_id AND e.session_id=s.session_id AND e.seq=s.turn_input_seq
				  AND e.dots_task_id=$5 AND e.dots_closed_at IS NULL
			  ))
			ORDER BY s.queued_at,s.session_id
			LIMIT 1 FOR UPDATE OF s SKIP LOCKED
		), claimed AS (
			UPDATE ahp_journal.sessions AS s
			SET status='ACTIVE',lease_epoch=s.lease_epoch+1,lease_owner=$5,
			    lease_expires_at=clock_timestamp()+$6*interval '1 microsecond'
			FROM ready
			WHERE (s.tenant_id,s.session_id)=(ready.tenant_id,ready.session_id)
			RETURNING s.tenant_id,s.session_id,s.lease_owner,s.lease_epoch,s.lease_expires_at,s.turn_input_seq,s.head_seq
		)
		SELECT c.session_id,c.lease_owner,c.lease_epoch,c.lease_expires_at,c.turn_input_seq,c.head_seq,
		       e.inline_payload,e.blob_ref,coalesce(e.dots_run_id,''),coalesce(e.dots_run_sequence,0),coalesce(e.dots_task_id,'')
		FROM claimed c JOIN ahp_journal.session_events e
		  ON (e.tenant_id,e.session_id,e.seq)=(c.tenant_id,c.session_id,c.turn_input_seq)`,
		scope.Tenant, scope.Deployment, scope.Subject, session, worker, ttl.Microseconds(), dots).
		Scan(&turn.SessionID, &turn.Worker, &turn.Epoch, &turn.ExpiresAt, &turn.InputSeq, &turn.HeadSeq,
			&turn.Input.Inline, &turn.Input.Blob, &turn.RunID, &turn.RunSequence, &turn.TaskID)
	if errors.Is(err, pgx.ErrNoRows) {
		err = contracts.NewError(contracts.ErrNotFound, "no turn ready")
	}
	return turn, err
}

func finishTurn(ctx context.Context, tx pgx.Tx, scope contracts.Scope, session, status string) error {
	tag, err := tx.Exec(ctx, `
		WITH finished AS (
			UPDATE ahp_journal.session_events e
			SET dots_closed_at=clock_timestamp()
			FROM ahp_journal.sessions s
			WHERE (e.tenant_id,e.session_id,e.seq)=(s.tenant_id,s.session_id,s.turn_input_seq)
			  AND s.tenant_id=$1 AND s.session_id=$2 AND s.dots_fifo AND s.status='ACTIVE'
			  AND $3='IDLE' AND e.dots_run_sequence IS NOT NULL AND e.dots_closed_at IS NULL
			RETURNING e.dots_run_sequence
		), next_turn AS (
			SELECT e.seq,e.created_at FROM ahp_journal.session_events e CROSS JOIN finished f
			WHERE e.tenant_id=$1 AND e.session_id=$2 AND e.dots_run_sequence>f.dots_run_sequence
			  AND e.dots_closed_at IS NULL
			ORDER BY e.dots_run_sequence LIMIT 1
		)
		UPDATE ahp_journal.sessions s
		SET status=CASE WHEN EXISTS (SELECT 1 FROM next_turn) THEN 'QUEUED' ELSE $3 END,
		    turn_input_seq=(SELECT seq FROM next_turn),queued_at=(SELECT created_at FROM next_turn),
		    lease_owner=NULL,lease_expires_at=NULL
		WHERE tenant_id=$1 AND session_id=$2 AND status='ACTIVE' AND turn_input_seq IS NOT NULL
		  AND (NOT s.dots_fifo OR EXISTS (SELECT 1 FROM finished))`, scope.Tenant, session, status)
	if err == nil && tag.RowsAffected() != 1 {
		return contracts.NewError(contracts.ErrConflict, "session has no active turn to finish")
	}
	return err
}

// RequeueExpiredTurns restores expired native claims to the durable worker queue.
// Dots claims recover their own expired FIFO head.
// It preserves the accepted input and never executes an external action.
func (s *Store) RequeueExpiredTurns(ctx context.Context, tenant string, batch int) (int64, error) {
	if !validID(tenant) || batch < 1 || batch > 1000 {
		return 0, contracts.NewError(contracts.ErrInvalid, "invalid turn recovery sweep")
	}
	tag, err := s.pool.Exec(ctx, `
		WITH expired AS (
			SELECT tenant_id,session_id FROM ahp_journal.sessions
			WHERE tenant_id=$1 AND status='ACTIVE' AND turn_input_seq IS NOT NULL AND NOT dots_fifo AND execution_mode='worker_pull'
			  AND lease_expires_at<=statement_timestamp()
			ORDER BY lease_expires_at,session_id LIMIT $2 FOR UPDATE SKIP LOCKED
		)
		UPDATE ahp_journal.sessions s SET status='QUEUED',lease_owner=NULL,lease_expires_at=NULL
		FROM expired e WHERE (s.tenant_id,s.session_id)=(e.tenant_id,e.session_id)`, tenant, batch)
	return tag.RowsAffected(), err
}

// SweepTurns runs the recovery poller for one execution scope. A caller
// may claim directly in response to an ingress signal instead. On either
// path, the database arbitrates ownership.
func (s *Store) SweepTurns(ctx context.Context, scope contracts.Scope, worker string, ttl, interval time.Duration, execute func(context.Context, TurnLease) error) error {
	if interval <= 0 || execute == nil {
		return contracts.NewError(contracts.ErrInvalid, "invalid turn sweeper configuration")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		turn, err := s.ClaimTurn(ctx, scope, "", worker, ttl)
		if contracts.Code(err) == contracts.ErrNotFound {
			select {
			case <-ctx.Done():
			case <-ticker.C:
			}
			continue
		}
		if err != nil {
			return err
		}
		if err := execute(ctx, turn); err != nil {
			return err
		}
	}
	return ctx.Err()
}
