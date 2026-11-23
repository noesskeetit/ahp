// Package statejournal implements the compact PostgreSQL state journal.
package statejournal

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

type Store struct {
	pool      *pgxpool.Pool
	artifacts *artifacts.Store
	s3        *S3Blobs
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// NewWithArtifacts enables verified artifact references. The artifact owner
// needs the compact journal's reference scan and settlement callbacks.
func NewWithArtifacts(pool *pgxpool.Pool, owner *artifacts.Store) *Store {
	return &Store{pool: pool, artifacts: owner}
}

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('ahp_journal:migrate', 0))`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type Lease = journal.Lease

func (s *Store) CreateSession(ctx context.Context, scope contracts.Scope, session, harness string, version int) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if !validID(session) || !validID(harness) || version < 1 {
		return contracts.NewError(contracts.ErrInvalid, "invalid session, harness or format version")
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO ahp_journal.sessions AS stored
			(tenant_id, session_id, deployment_id, subject_id, harness_type, format_version)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_id, session_id) DO UPDATE SET session_id=EXCLUDED.session_id
		WHERE stored.deployment_id=EXCLUDED.deployment_id AND stored.subject_id=EXCLUDED.subject_id
		  AND stored.harness_type=EXCLUDED.harness_type AND stored.format_version=EXCLUDED.format_version AND NOT stored.dots_fifo
		  AND stored.status <> 'DELETED'`, scope.Tenant, session, scope.Deployment, scope.Subject, harness, version)
	if err == nil && tag.RowsAffected() == 0 {
		return contracts.NewError(contracts.ErrConflict, "session identifier unavailable")
	}
	return err
}

// AcquireOrCreateSession grants the first lease in the same statement that
// creates a session. A later acquisition must match the stored owner and
// harness format.
func (s *Store) AcquireOrCreateSession(ctx context.Context, scope contracts.Scope, session, harness string, version int, worker string, ttl time.Duration) (Lease, error) {
	var lease Lease
	if err := scope.Validate(); err != nil {
		return lease, err
	}
	if !validID(session) || !validID(harness) || version < 1 || !validID(worker) || ttl < time.Millisecond || ttl > 15*time.Minute {
		return lease, contracts.NewError(contracts.ErrInvalid, "invalid session, harness or lease")
	}
	err := s.pool.QueryRow(ctx, `
		INSERT INTO ahp_journal.sessions AS stored
			(tenant_id, session_id, deployment_id, subject_id, harness_type,
			 format_version, lease_owner, lease_epoch, lease_expires_at, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,1,clock_timestamp()+$8*interval '1 microsecond','ACTIVE')
		ON CONFLICT (tenant_id, session_id) DO UPDATE
		SET status='ACTIVE', lease_owner=EXCLUDED.lease_owner, lease_epoch=stored.lease_epoch+1,
		    lease_expires_at=clock_timestamp()+$8*interval '1 microsecond'
		WHERE stored.deployment_id=EXCLUDED.deployment_id AND stored.subject_id=EXCLUDED.subject_id
		  AND stored.harness_type=EXCLUDED.harness_type AND stored.format_version=EXCLUDED.format_version AND NOT stored.dots_fifo
		  AND stored.status<>'DELETED' AND stored.turn_input_seq IS NULL AND stored.execution_mode='worker_pull'
		  AND (stored.lease_expires_at IS NULL OR stored.lease_expires_at<=clock_timestamp())
		RETURNING lease_owner, lease_epoch, lease_expires_at`,
		scope.Tenant, session, scope.Deployment, scope.Subject, harness, version, worker, ttl.Microseconds()).
		Scan(&lease.Worker, &lease.Epoch, &lease.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = contracts.NewError(contracts.ErrConflict, "session unavailable for acquisition")
	}
	return lease, err
}

func (s *Store) AcquireSession(ctx context.Context, scope contracts.Scope, session, worker string, ttl time.Duration) (Lease, error) {
	var lease Lease
	if err := scope.Validate(); err != nil {
		return lease, err
	}
	if !validID(session) || !validID(worker) || ttl < time.Millisecond || ttl > 15*time.Minute {
		return lease, contracts.NewError(contracts.ErrInvalid, "invalid session, worker or lease duration")
	}
	err := s.pool.QueryRow(ctx, `
		UPDATE ahp_journal.sessions
		SET status='ACTIVE', lease_owner=$5, lease_epoch=lease_epoch+1,
		    lease_expires_at=clock_timestamp()+$6*interval '1 microsecond'
		WHERE tenant_id=$1 AND session_id=$2 AND deployment_id=$3 AND subject_id=$4
		  AND status <> 'DELETED' AND turn_input_seq IS NULL AND NOT dots_fifo AND execution_mode='worker_pull'
		  AND (lease_expires_at IS NULL OR lease_expires_at <= clock_timestamp())
		RETURNING lease_owner, lease_epoch, lease_expires_at`,
		scope.Tenant, session, scope.Deployment, scope.Subject, worker, ttl.Microseconds()).
		Scan(&lease.Worker, &lease.Epoch, &lease.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = contracts.NewError(contracts.ErrConflict, "session unavailable for acquisition")
	}
	return lease, err
}

func (s *Store) HeartbeatLease(ctx context.Context, scope contracts.Scope, session string, lease Lease, ttl time.Duration) (Lease, error) {
	if err := scope.Validate(); err != nil {
		return Lease{}, err
	}
	if !validID(session) || !validID(lease.Worker) || lease.Epoch <= 0 || ttl < time.Millisecond || ttl > 15*time.Minute {
		return Lease{}, contracts.NewError(contracts.ErrInvalid, "invalid lease heartbeat")
	}
	err := s.pool.QueryRow(ctx, `
		UPDATE ahp_journal.sessions
		SET lease_expires_at=CASE WHEN execution_mode='vm_managed'
			THEN greatest(lease_expires_at,clock_timestamp()+$7*interval '1 microsecond')
			ELSE clock_timestamp()+$7*interval '1 microsecond' END
		WHERE tenant_id=$1 AND session_id=$2 AND deployment_id=$3 AND subject_id=$4
		  AND lease_owner=$5 AND lease_epoch=$6 AND lease_expires_at > clock_timestamp()
		  AND status <> 'DELETED'
		RETURNING lease_expires_at`,
		scope.Tenant, session, scope.Deployment, scope.Subject, lease.Worker, lease.Epoch, ttl.Microseconds()).
		Scan(&lease.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = contracts.NewError(contracts.ErrStaleEpoch, "session lease has expired or been superseded")
	}
	if err != nil {
		return Lease{}, err
	}
	return lease, nil
}

func (s *Store) ReleaseLease(ctx context.Context, scope contracts.Scope, session string, lease Lease) error {
	return s.ReleaseLeaseTo(ctx, scope, session, lease, SessionIdle)
}

func (s *Store) ReleaseLeaseTo(ctx context.Context, scope contracts.Scope, session string, lease Lease, status string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if status != SessionIdle && status != SessionWaitingInput {
		return contracts.NewError(contracts.ErrInvalid, "invalid disposition for a released lease")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE ahp_journal.sessions SET lease_owner=NULL, lease_expires_at=NULL, status=$7
		WHERE tenant_id=$1 AND session_id=$2 AND deployment_id=$3 AND subject_id=$4
		  AND lease_owner=$5 AND lease_epoch=$6 AND lease_expires_at > clock_timestamp()
		  AND status <> 'DELETED' AND turn_input_seq IS NULL`, scope.Tenant, session, scope.Deployment, scope.Subject, lease.Worker, lease.Epoch, status)
	if err == nil && tag.RowsAffected() != 1 {
		err = contracts.NewError(contracts.ErrStaleEpoch, "session lease has expired or been superseded")
	}
	return err
}

func (s *Store) DeleteSession(ctx context.Context, scope contracts.Scope, session string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if !validID(session) {
		return contracts.NewError(contracts.ErrInvalid, "invalid session identifier")
	}
	// Tombstones survive retries and fence the previous worker.
	_, err := s.pool.Exec(ctx, `
		UPDATE ahp_journal.sessions
		SET status='DELETED', lease_epoch=lease_epoch+1, lease_owner=NULL, lease_expires_at=NULL,
		    turn_input_seq=NULL, queued_at=NULL, cancel_requested=false, followup_count=0, deleted_at=clock_timestamp()
		WHERE tenant_id=$1 AND session_id=$2 AND deployment_id=$3 AND subject_id=$4 AND status <> 'DELETED'`,
		scope.Tenant, session, scope.Deployment, scope.Subject)
	return err
}

type SessionPage = journal.SessionPage

func (s *Store) ListSessions(ctx context.Context, scope contracts.Scope, harness, cursor string, limit int) (SessionPage, error) {
	var page SessionPage
	if err := scope.Validate(); err != nil {
		return page, err
	}
	if (harness != "" && !validID(harness)) || (cursor != "" && !validID(cursor)) || limit < 1 || limit > 1000 {
		return page, contracts.NewError(contracts.ErrInvalid, "invalid session page")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT session_id FROM ahp_journal.sessions
		WHERE tenant_id=$1 AND deployment_id=$2 AND subject_id=$3 AND session_id>$4
		  AND ($5='' OR harness_type=$5) AND status <> 'DELETED'
		ORDER BY session_id LIMIT $6`, scope.Tenant, scope.Deployment, scope.Subject, cursor, harness, limit+1)
	if err != nil {
		return page, err
	}
	page.IDs, err = pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return SessionPage{}, err
	}
	if len(page.IDs) > limit {
		page.IDs = page.IDs[:limit]
		page.NextCursor = page.IDs[len(page.IDs)-1]
	}
	return page, nil
}

// lockSession returns the active lease and head while holding the session lock.
func lockSessionState(ctx context.Context, tx pgx.Tx, scope contracts.Scope, session string, lease Lease) (int64, managedTurn, error) {
	var head int64
	var valid bool
	var turn managedTurn
	err := tx.QueryRow(ctx, `
		WITH locked AS MATERIALIZED (
			SELECT head_seq, lease_epoch, lease_owner, lease_expires_at,
			       execution_mode, coalesce(turn_input_seq,0) AS turn_seq, steer_acked_seq, followup_count, cancel_requested
			FROM ahp_journal.sessions
			WHERE tenant_id=$1 AND session_id=$2 AND deployment_id=$3 AND subject_id=$4
			  AND status <> 'DELETED'
			FOR UPDATE
		)
		SELECT head_seq, coalesce(
		    lease_epoch=$5 AND lease_owner=$6 AND lease_expires_at > clock_timestamp(), false
		), execution_mode, turn_seq, steer_acked_seq, followup_count, cancel_requested, lease_expires_at FROM locked`,
		scope.Tenant, session, scope.Deployment, scope.Subject, lease.Epoch, lease.Worker).Scan(&head, &valid, &turn.Mode, &turn.Seq, &turn.Ack, &turn.Followups, &turn.Cancel, &turn.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, turn, contracts.NewError(contracts.ErrNotFound, "session not found")
	}
	if err != nil {
		return 0, turn, err
	}
	if !valid {
		return 0, turn, contracts.NewError(contracts.ErrStaleEpoch, "session lease has expired or been superseded")
	}
	return head, turn, nil
}

func validID(s string) bool {
	if len(s) == 0 || len(s) > 256 {
		return false
	}
	for _, char := range s {
		if char <= ' ' || char == 127 {
			return false
		}
	}
	return true
}
