package state

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// CancelWaitingRun closes an unleased pending/waiting run and journals its
// terminal event atomically. Active execution still requires the lease fence.
// callerContext must be the context attested by the authenticated task token.
//
// Lock order: the thread row first, the run row only inside it - the same rule
// the claim scan states (see claimCandidatesSQL). An earlier version locked the
// run row here before locking the thread, which put this door in the opposite
// order to every lease writer (release, heartbeat, suspend, batch all lock the
// thread before the run) and made a cancel racing a release of the same run a
// run<->thread cycle (SQLSTATE 40P01). The unlocked read below is a locator
// only: it says which thread to lock, and every decision is taken from the
// re-read that happens under that lock.
func (s *Store) CancelWaitingRun(ctx context.Context, scope contracts.Scope, runID, callerContext string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if runID == "" || callerContext == "" {
		return contracts.NewError(contracts.ErrForbidden, "run and verified conversation context are required")
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkSubjectWritable(ctx, tx, scope); err != nil {
			return err
		}
		// Locator only - no row is locked here, so nothing may be decided from it.
		var threadID string
		err := tx.QueryRow(ctx, `SELECT r.thread_id FROM ahp_state.runs r JOIN ahp_state.threads t ON t.id=r.thread_id
   WHERE r.id=$1 AND t.tenant=$2 AND t.deployment=$3 AND t.subject=$4`, runID, scope.Tenant, scope.Deployment, scope.Subject).Scan(&threadID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("run")
		}
		if err != nil {
			return err
		}
		var contextID string
		var leaseRun *string
		err = tx.QueryRow(ctx, `SELECT context_id,lease_run_id FROM ahp_state.threads WHERE id=$1 FOR UPDATE`, threadID).Scan(&contextID, &leaseRun)
		if err != nil {
			return err
		}
		if callerContext != contextID && callerContext != threadID {
			return contracts.NewError(contracts.ErrForbidden, "task context does not own this conversation")
		}
		// The decision reads the run under the thread lock, so status and lease
		// cannot move between the check and the write below.
		var status string
		err = tx.QueryRow(ctx, `SELECT r.status FROM ahp_state.runs r JOIN ahp_state.threads t ON t.id=r.thread_id
   WHERE r.id=$1 AND t.tenant=$2 AND t.deployment=$3 AND t.subject=$4 FOR UPDATE OF r`, runID, scope.Tenant, scope.Deployment, scope.Subject).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("run")
		}
		if err != nil {
			return err
		}
		if status != RunCanceled && ((status != RunPending && status != RunWaiting) || leaseRun != nil) {
			return contracts.NewError(contracts.ErrConflict, "run must be pending or waiting without an active thread lease")
		}
		payload := json.RawMessage(`{"status":"canceled"}`)
		digest, err := contracts.Digest(struct {
			Kind    string          `json:"kind"`
			Payload json.RawMessage `json:"payload"`
		}{"status", payload})
		if err != nil {
			return err
		}
		key := runID + ":cancel"
		var existingDigest []byte
		err = tx.QueryRow(ctx, `SELECT digest FROM ahp_state.stream_events WHERE thread_id=$1 AND key=$2`, threadID, key).Scan(&existingDigest)
		if err == nil {
			if !digestMatches(existingDigest, digest) {
				return ErrKeyReuse
			}
		} else if errors.Is(err, pgx.ErrNoRows) {
			_, err = tx.Exec(ctx, `INSERT INTO ahp_state.stream_events(id,thread_id,run_id,key,kind,sequence,payload,digest)
    SELECT $1,$2,$3,$4,'status',GREATEST(COALESCE(MAX(sequence),0), COALESCE((SELECT stream_high_water FROM ahp_state.threads WHERE id=$2),0))+1,$5,$6 FROM ahp_state.stream_events WHERE thread_id=$2`, contracts.NewID(), threadID, runID, key, payload, digest)
			if err != nil {
				return err
			}
		} else {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE ahp_state.runs SET status='canceled',wait_for='',deadline=NULL,
   payload=payload || '{"close_reason":"canceled"}'::jsonb,updated_at=clock_timestamp() WHERE id=$1`, runID)
		return err
	})
}
