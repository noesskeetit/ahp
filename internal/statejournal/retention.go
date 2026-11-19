package statejournal

import (
	"context"
	"errors"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

type expiredSession struct {
	id        string
	scope     contracts.Scope
	deletedAt time.Time
}

// SweepDeletedSessions is the compact journal phase of Artifacts GC. The
// authenticated tenant and cutoff come from the GC job. The small State
// tombstone persists to reject delayed writes and retries of session creation.
func (s *Store) SweepDeletedSessions(ctx context.Context, tenant string, cutoff time.Time, batchSize int) error {
	if !validID(tenant) || cutoff.IsZero() || batchSize < 1 || batchSize > 1000 {
		return contracts.NewError(contracts.ErrInvalid, "invalid compact journal retention sweep")
	}
	if s.artifacts == nil && s.s3 == nil {
		return contracts.NewError(contracts.ErrUnavailable, "compact journal retention requires Artifacts owner")
	}
	var cursor expiredSession
	for {
		rows, err := s.pool.Query(ctx, `
			SELECT session_id, deployment_id, subject_id, deleted_at
			FROM ahp_journal.sessions
			WHERE tenant_id=$1 AND status='DELETED' AND retention_swept_at IS NULL AND deleted_at<=$2
			  AND NOT legal_hold
			  AND (deleted_at,session_id)>($3,$4)
			ORDER BY deleted_at,session_id LIMIT $5`, tenant, cutoff, cursor.deletedAt, cursor.id, batchSize)
		if err != nil {
			return err
		}
		batch, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (expiredSession, error) {
			item := expiredSession{scope: contracts.Scope{Tenant: tenant}}
			err := r.Scan(&item.id, &item.scope.Deployment, &item.scope.Subject, &item.deletedAt)
			return item, err
		})
		if err != nil {
			return err
		}
		for _, item := range batch {
			if err := s.sweepDeletedSession(ctx, item); err != nil {
				return err
			}
			cursor = item
		}
		if len(batch) < batchSize {
			return nil
		}
	}
}

func (s *Store) sweepDeletedSession(ctx context.Context, item expiredSession) error {
	scope := item.scope
	tenant, session := scope.Tenant, item.id
	pending := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var ignored string
		err := tx.QueryRow(ctx, `
			SELECT session_id FROM ahp_journal.sessions
			WHERE tenant_id=$1 AND session_id=$2 AND status='DELETED' AND deleted_at=$3
			  AND retention_swept_at IS NULL AND NOT legal_hold FOR UPDATE SKIP LOCKED`, tenant, session, item.deletedAt).Scan(&ignored)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		pending = true
		for _, query := range []string{
			`DELETE FROM ahp_journal.session_events WHERE tenant_id=$1 AND session_id=$2`,
			`DELETE FROM ahp_journal.checkpoint_writes WHERE tenant_id=$1 AND session_id=$2`,
			`DELETE FROM ahp_journal.checkpoints WHERE tenant_id=$1 AND session_id=$2`,
			`DELETE FROM ahp_journal.tool_intents WHERE tenant_id=$1 AND session_id=$2`,
		} {
			if _, err := tx.Exec(ctx, query, tenant, session); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM ahp_journal.scoped_kv
			WHERE tenant_id=$1 AND deployment_id=$2 AND scope_type='session' AND scope_owner=$3`,
			tenant, scope.Deployment, session); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			UPDATE ahp_journal.sessions SET head_seq=0, leaf_entry_id=NULL, append_parent_id=NULL,
			    parent_session_id=NULL, agent_id=NULL, native_key=NULL, title=NULL, head_checkpoint_id=NULL,
			    prompt_ref=NULL, summary_metadata=NULL,
			    retention_started_at=coalesce(retention_started_at,clock_timestamp())
			WHERE tenant_id=$1 AND session_id=$2`, tenant, session)
		return err
	})
	if err != nil || !pending {
		return err
	}
	// Commit the reference removal before asking Artifacts to retire its own
	// rows. If it fails, the pending tombstone makes this step retryable.
	if s.s3 != nil {
		if err := s.s3.retireSession(ctx, tenant, session); err != nil {
			return err
		}
	} else {
		retired, err := s.artifacts.RetireDeletedContext(ctx, scope, session, item.deletedAt)
		if err != nil || retired.Referenced > 0 || retired.Held > 0 {
			return err
		}
	}
	_, err = s.pool.Exec(ctx, `
		UPDATE ahp_journal.sessions SET retention_swept_at=clock_timestamp()
		WHERE tenant_id=$1 AND session_id=$2 AND status='DELETED' AND deleted_at=$3
		  AND retention_swept_at IS NULL`, tenant, session, item.deletedAt)
	return err
}

// SetLegalHold protects a session and its objects until explicitly released.
// Retention that has already removed durable references cannot be undone.
func (s *Store) SetLegalHold(ctx context.Context, scope contracts.Scope, session string, held bool) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if !validID(session) {
		return contracts.NewError(contracts.ErrInvalid, "invalid session identity")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE ahp_journal.sessions SET legal_hold=$5
		WHERE tenant_id=$1 AND session_id=$2 AND deployment_id=$3 AND subject_id=$4
		  AND retention_started_at IS NULL`, scope.Tenant, session, scope.Deployment, scope.Subject, held)
	if err == nil && tag.RowsAffected() != 1 {
		return contracts.NewError(contracts.ErrNotFound, "session unavailable for retention hold")
	}
	return err
}
