package artifacts

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// WithCheckpointSettlement installs the State owner proof used to reconcile
// pins whose caller crashed or lost its commit/release acknowledgement.
func WithCheckpointSettlement(proof references.CheckpointSettlement) Option {
	return func(s *Store) { s.checkpointSettlement = proof }
}

func artifactLockKey(tenant, id string) string { return "ahp_artifacts:object:" + tenant + ":" + id }

// PinCheckpointReferences is an internal owner API. Its caller has already
// acquired State's unique operation transaction lock. Pins commit before the
// State transaction may commit; neither transport cancellation nor process
// death expires them. Existing bytes must be currently readable and match the
// complete immutable descriptor, not just an attacker-chosen artifact ID.
func (s *Store) PinCheckpointReferences(ctx context.Context, scope contracts.Scope, operation string, refs []references.CheckpointArtifact) error {
	if s.keep == nil || s.checkpointSettlement == nil {
		return contracts.NewError(contracts.ErrUnavailable, "checkpoint reference and settlement owners are required")
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := requireBindingValue("checkpoint operation", operation); err != nil {
		return err
	}
	byID := make(map[string]references.CheckpointArtifact, len(refs))
	for _, ref := range refs {
		if err := validateID(ref.ID); err != nil {
			return err
		}
		if prior, ok := byID[ref.ID]; ok && !prior.Equal(ref) {
			return contracts.NewError(contracts.ErrInvalid, "conflicting checkpoint artifact descriptors")
		}
		byID[ref.ID] = ref
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	labelDigests := make(map[string]string, len(ids))
	// Authority and label hooks may call other owners. Run them before taking
	// an artifact transaction connection, then compare the locked label snapshot
	// below. This prevents a bounded owner pool from waiting recursively on itself.
	for _, id := range ids {
		current, err := s.Get(ctx, scope, id)
		if err != nil {
			return err
		}
		if err := s.checkContent(ctx, id); err != nil {
			return err
		}
		labelDigests[id], err = contracts.Digest(current.Labels)
		if err != nil {
			return err
		}
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, id := range ids {
			if err := lockMarkers(ctx, tx, []string{artifactLockKey(scope.Tenant, id)}); err != nil {
				return err
			}
			// Keep the row locked through pin commit, excluding tombstone and
			// label changes as well as GC's common object lock.
			var actual references.CheckpointArtifact
			var rawLabels []byte
			var deleted bool
			var ingestion, mime string
			err := tx.QueryRow(ctx, `SELECT id,tenant,deployment,subject,context_id,task_id,COALESCE(version,''),COALESCE(sha256,''),COALESCE(size,0),labels,deleted,ingestion_state,mime FROM ahp_artifacts.artifacts WHERE id=$1 AND tenant=$2 FOR UPDATE`, id, scope.Tenant).Scan(&actual.ID, &actual.Tenant, &actual.Deployment, &actual.Subject, &actual.ContextID, &actual.TaskID, &actual.Version, &actual.SHA256, &actual.Size, &rawLabels, &deleted, &ingestion, &mime)
			if errors.Is(err, pgx.ErrNoRows) || (err == nil && deleted) {
				return contracts.NewError(contracts.ErrNotFound, "artifact not found")
			}
			if err != nil {
				return storageError("checkpoint artifact", err)
			}
			if err := json.Unmarshal(rawLabels, &actual.Labels); err != nil {
				return storageError("checkpoint labels", err)
			}
			if !actual.Equal(byID[id]) || actual.Version == "" || actual.SHA256 == "" || actual.Size <= 0 {
				return contracts.NewError(contracts.ErrInvalid, "checkpoint artifact identity mismatch")
			}
			if ingestion == "quarantined" || (supportedText(mime) && ingestion != "ready") {
				return contracts.NewError(contracts.ErrForbidden, "checkpoint artifact is not content ready")
			}
			if err := s.checkReadScope(ctx, ArtifactRef{ID: id, ContextID: actual.ContextID}); err != nil {
				return err
			}
			labelDigest, err := contracts.Digest(actual.Labels)
			if err != nil {
				return err
			}
			if labelDigest != labelDigests[id] {
				return contracts.NewError(contracts.ErrConflict, "artifact labels changed while pinning checkpoint")
			}
			if _, err := tx.Exec(ctx, `INSERT INTO ahp_artifacts.checkpoint_pins(operation_id,tenant,artifact_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, operation, scope.Tenant, id); err != nil {
				return storageError("pin checkpoint artifact", err)
			}
		}
		return nil
	})
}

// ReleaseCheckpointReferences is called only after State proves its operation
// has settled. It never drops another operation's protection.
func (s *Store) ReleaseCheckpointReferences(ctx context.Context, operation string) error {
	if err := requireBindingValue("checkpoint operation", operation); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM ahp_artifacts.checkpoint_pins WHERE operation_id=$1`, operation)
	return storageError("release checkpoint pins", err)
}

// ReconcileCheckpointPins is bounded and tenant-scoped. Unknown or unavailable
// State outcomes keep pins. GC's subsequent locked reference scan protects
// committed references after transient pins have been released.
func (s *Store) ReconcileCheckpointPins(ctx context.Context, tenant string) error {
	if s.checkpointSettlement == nil {
		return nil
	}
	rows, err := s.pool.Query(ctx, `SELECT operation_id FROM ahp_artifacts.checkpoint_pins WHERE tenant=$1 GROUP BY operation_id ORDER BY min(checked_at),operation_id LIMIT 256`, tenant)
	if err != nil {
		return storageError("list checkpoint pins", err)
	}
	var operations []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		operations = append(operations, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, op := range operations {
		settled, err := s.checkpointSettlement(ctx, op)
		if err != nil {
			return err
		}
		if settled {
			if err := s.ReleaseCheckpointReferences(ctx, op); err != nil {
				return err
			}
		} else {
			// Rotate active operations behind unexamined ones. A bounded batch
			// of long transactions must not starve later abandoned pins forever.
			if _, err := s.pool.Exec(ctx, `UPDATE ahp_artifacts.checkpoint_pins SET checked_at=clock_timestamp() WHERE operation_id=$1 AND tenant=$2`, op, tenant); err != nil {
				return err
			}
		}
	}
	return nil
}
