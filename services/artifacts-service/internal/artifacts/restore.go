package artifacts

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// Restore lifts a tombstone before the collector has taken the bytes.
//
// The PDF does not enumerate an un-delete, but it does make deletion a two-step
// act - a tombstone now, the retention floor later, and legal hold able to pin
// either - and a floor that nothing can be pulled back inside of would make
// every mistaken cascade final. Restore is the other half of that window, and
// it is the operation the storage GC has to be correct against: a sweep that
// races a restore must lose, because the row it wanted is a row somebody just
// asked to keep.
//
// The bytes matter here: a tombstone never removed them (that is the GC's
// job), so a restore inside the retention window returns the same sealed
// version and the same digest, and a later download verifies exactly as it did
// before the deletion.
func (s *Store) Restore(ctx context.Context, scope contracts.Scope, id string) (ArtifactRef, error) {
	if err := scope.Validate(); err != nil {
		return ArtifactRef{}, err
	}
	if err := validateID(id); err != nil {
		return ArtifactRef{}, err
	}
	if err := s.authorize(ctx, scope, "restore", "artifact:"+id); err != nil {
		return ArtifactRef{}, err
	}
	var restored bool
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Take subject locks before the artifact row to avoid a lock cycle
		// with an admitted seal and a concurrently queued erasure.
		if err := checkArtifactWritable(ctx, tx, scope, id); err != nil {
			return err
		}
		var (
			tenant, object, doc                    string
			deleted, hold                          bool
			version, sha                           string
			size                                   int64
			contextID, taskID, deployment, subject string
		)
		err := tx.QueryRow(ctx, `
SELECT tenant, deployment, subject, object, labels, deleted, legal_hold,
       COALESCE(sha256, ''), COALESCE(version, ''), COALESCE(size, 0), context_id, task_id
  FROM ahp_artifacts.artifacts WHERE id = $1 FOR UPDATE`, id).
			Scan(&tenant, &deployment, &subject, &object, &doc, &deleted, &hold,
				&sha, &version, &size, &contextID, &taskID)
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrNotFound, "artifact not found")
		}
		if err != nil {
			return storageError("lock artifact for restore", err)
		}
		// Cross-tenant and cross-subject restores read as "no such artifact":
		// the scope of a restore comes from the caller's verified identity, and
		// the stored labels are checked below the same way any read checks them.
		if tenant != scope.Tenant {
			return contracts.NewError(contracts.ErrNotFound, "artifact not found")
		}
		var stored labels.Set
		if len(doc) > 0 {
			if err := json.Unmarshal([]byte(doc), &stored); err != nil {
				return contracts.NewError(contracts.ErrConflict, "stored labels are unreadable")
			}
		}
		if _, err := s.admit(ctx, scope, stored); err != nil {
			return err
		}
		if !deleted {
			// Restoring a live artifact is a no-op with the same answer, not an
			// error: a retried restore after a lost response must not become a
			// failure the control plane cannot distinguish.
			return nil
		}
		tag, err := tx.Exec(ctx, `
UPDATE ahp_artifacts.artifacts SET deleted = false, deleted_at = NULL
 WHERE id = $1 AND deleted`, id)
		if err != nil {
			return storageError("restore artifact", err)
		}
		if tag.RowsAffected() != 1 {
			return contracts.NewError(contracts.ErrConflict, "artifact state changed, restore aborted")
		}
		restored = true
		return s.logGCEventTx(ctx, tx, GCEvent{
			Event: GCEventRestore, Tenant: tenant, Deployment: deployment, Subject: subject,
			ArtifactID: id, Object: object, Version: version, SHA256: sha, Size: size,
			Reason: "restore",
		})
	})
	if err != nil {
		return ArtifactRef{}, err
	}
	ref, err := s.get(ctx, scope, id, false)
	if err != nil {
		return ArtifactRef{}, err
	}
	if !restored {
		return ref, nil
	}
	// A restored artifact is live again, so its retention clock restarts: the
	// next tombstone sets a new floor, and leaving the old one in place would
	// let a collector finish a deletion somebody just withdrew.
	if err := s.retainAfterRestore(ctx, id); err != nil {
		return ref, err
	}
	return ref, nil
}

// retainAfterRestore clears an elapsed retain_until. An unelapsed floor is kept
// on purpose: it was set deliberately and a restore does not get to weaken it.
func (s *Store) retainAfterRestore(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
UPDATE ahp_artifacts.artifacts SET retain_until = NULL
 WHERE id = $1 AND retain_until IS NOT NULL AND retain_until <= now()`, id)
	return storageError("restart retention", err)
}
