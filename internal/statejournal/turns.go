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
	if err := scope.Validate(); err != nil {
		return EnqueueReceipt{}, err
	}
	if !validID(req.SessionID) || !validID(req.Harness) || !validID(req.Key) ||
		req.FormatVersion < 1 || req.Input.Blob != nil ||
		!validPayload(req.Input) || !json.Valid(req.Input.Inline) {
		return EnqueueReceipt{}, contracts.NewError(contracts.ErrInvalid, "invalid turn admission")
	}
	data, err := json.Marshal(req)
	if err != nil {
		return EnqueueReceipt{}, err
	}
	digest := sha256.Sum256(data)
	keyHash := sha256.Sum256([]byte(req.Key))
	key := "turn.input:" + hex.EncodeToString(keyHash[:])

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return EnqueueReceipt{}, err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `
		INSERT INTO ahp_journal.sessions
			(tenant_id,session_id,deployment_id,subject_id,harness_type,format_version)
		VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`,
		scope.Tenant, req.SessionID, scope.Deployment, scope.Subject, req.Harness, req.FormatVersion)
	if err != nil {
		return EnqueueReceipt{}, err
	}
	var head int64
	var status string
	var hasLease bool
	err = tx.QueryRow(ctx, `
		SELECT head_seq,status,coalesce(lease_expires_at>clock_timestamp(),false)
		FROM ahp_journal.sessions
		WHERE tenant_id=$1 AND session_id=$2 AND deployment_id=$3 AND subject_id=$4
		  AND harness_type=$5 AND format_version=$6 AND status<>'DELETED'
		FOR UPDATE`, scope.Tenant, req.SessionID, scope.Deployment, scope.Subject, req.Harness, req.FormatVersion).
		Scan(&head, &status, &hasLease)
	if errors.Is(err, pgx.ErrNoRows) {
		return EnqueueReceipt{}, contracts.NewError(contracts.ErrConflict, "session unavailable for admission")
	}
	if err != nil {
		return EnqueueReceipt{}, err
	}
	receipt := EnqueueReceipt{SessionID: req.SessionID}
	var stored []byte
	err = tx.QueryRow(ctx, `
		SELECT result_head_seq,request_digest
		FROM ahp_journal.session_events
		WHERE tenant_id=$1 AND session_id=$2 AND operation_key=$3`,
		scope.Tenant, req.SessionID, key).Scan(&receipt.HeadSeq, &stored)
	switch {
	case err == nil && bytes.Equal(stored, digest[:]):
		return receipt, nil
	case err == nil:
		return EnqueueReceipt{}, contracts.NewError(contracts.ErrKeyReuse, "turn key has a different input")
	case !errors.Is(err, pgx.ErrNoRows):
		return EnqueueReceipt{}, err
	case hasLease || (status != SessionIdle && status != SessionWaitingInput):
		return EnqueueReceipt{}, contracts.NewError(contracts.ErrConflict, "the session already has a turn")
	}

	receipt.HeadSeq = head + 1
	event := EventInput{ID: key, Type: "turn.input", Role: "user", Codec: "json", Payload: req.Input}
	if err := insertEvent(ctx, tx, scope.Tenant, req.SessionID, receipt.HeadSeq, event, key, digest[:], receipt.HeadSeq); err != nil {
		return EnqueueReceipt{}, err
	}
	_, err = tx.Exec(ctx, `
		UPDATE ahp_journal.sessions
		SET head_seq=$3,turn_input_seq=$3,status='QUEUED',queued_at=clock_timestamp(),updated_at=clock_timestamp(),
		    lease_owner=NULL,lease_expires_at=NULL
		WHERE tenant_id=$1 AND session_id=$2`, scope.Tenant, req.SessionID, receipt.HeadSeq)
	if err != nil {
		return EnqueueReceipt{}, err
	}
	return receipt, tx.Commit(ctx)
}

// ClaimTurn takes a specific turn or the next ready turn for the authorised
// scope. A queued input survives lease turnover and is returned on every claim.
func (s *Store) ClaimTurn(ctx context.Context, scope contracts.Scope, session, worker string, ttl time.Duration) (TurnLease, error) {
	var turn TurnLease
	if err := scope.Validate(); err != nil {
		return turn, err
	}
	if (session != "" && !validID(session)) || !validID(worker) || ttl < time.Millisecond || ttl > 15*time.Minute {
		return turn, contracts.NewError(contracts.ErrInvalid, "invalid turn claim")
	}
	err := s.pool.QueryRow(ctx, `
		WITH ready AS MATERIALIZED (
			SELECT tenant_id,session_id FROM ahp_journal.sessions
			WHERE tenant_id=$1 AND deployment_id=$2 AND subject_id=$3
			  AND ($4='' OR session_id=$4)
			  AND (status='QUEUED' OR (status='ACTIVE' AND turn_input_seq IS NOT NULL
				 AND lease_expires_at<=statement_timestamp()))
			ORDER BY queued_at,session_id
			LIMIT 1 FOR UPDATE SKIP LOCKED
		), claimed AS (
			UPDATE ahp_journal.sessions AS s
			SET status='ACTIVE',lease_epoch=s.lease_epoch+1,lease_owner=$5,
			    lease_expires_at=clock_timestamp()+$6*interval '1 microsecond'
			FROM ready
			WHERE (s.tenant_id,s.session_id)=(ready.tenant_id,ready.session_id)
			RETURNING s.tenant_id,s.session_id,s.lease_owner,s.lease_epoch,s.lease_expires_at,s.turn_input_seq,s.head_seq
		)
		SELECT c.session_id,c.lease_owner,c.lease_epoch,c.lease_expires_at,c.turn_input_seq,c.head_seq,
		       e.inline_payload,e.blob_ref
		FROM claimed c JOIN ahp_journal.session_events e
		  ON (e.tenant_id,e.session_id,e.seq)=(c.tenant_id,c.session_id,c.turn_input_seq)`,
		scope.Tenant, scope.Deployment, scope.Subject, session, worker, ttl.Microseconds()).
		Scan(&turn.SessionID, &turn.Worker, &turn.Epoch, &turn.ExpiresAt,
			&turn.InputSeq, &turn.HeadSeq, &turn.Input.Inline, &turn.Input.Blob)
	if errors.Is(err, pgx.ErrNoRows) {
		err = contracts.NewError(contracts.ErrNotFound, "no turn ready")
	}
	return turn, err
}

func finishTurn(ctx context.Context, tx pgx.Tx, scope contracts.Scope, session, status string) error {
	tag, err := tx.Exec(ctx, `
		UPDATE ahp_journal.sessions
		SET status=$3,turn_input_seq=NULL,queued_at=NULL,lease_owner=NULL,lease_expires_at=NULL
		WHERE tenant_id=$1 AND session_id=$2 AND status='ACTIVE' AND turn_input_seq IS NOT NULL`,
		scope.Tenant, session, status)
	if err == nil && tag.RowsAffected() != 1 {
		return contracts.NewError(contracts.ErrConflict, "session has no active turn to finish")
	}
	return err
}

// RequeueExpiredTurns restores expired claims to the durable worker queue.
// It preserves the accepted input and never executes an external action.
func (s *Store) RequeueExpiredTurns(ctx context.Context, tenant string, batch int) (int64, error) {
	if !validID(tenant) || batch < 1 || batch > 1000 {
		return 0, contracts.NewError(contracts.ErrInvalid, "invalid turn recovery sweep")
	}
	tag, err := s.pool.Exec(ctx, `
		WITH expired AS (
			SELECT tenant_id,session_id FROM ahp_journal.sessions
			WHERE tenant_id=$1 AND status='ACTIVE' AND turn_input_seq IS NOT NULL
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
