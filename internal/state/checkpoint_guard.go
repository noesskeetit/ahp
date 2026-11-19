package state

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// SetCheckpointArtifacts is configured once by the composition root before
// serving. Without this owner API, new artifact descriptors fail closed.
func (s *Store) SetCheckpointArtifacts(owner references.CheckpointGuard) {
	s.checkpointArtifacts = owner
}

type checkpointAttemptKey struct{}
type checkpointAttempt struct {
	id    string
	scope contracts.Scope
	refs  []references.CheckpointArtifact
}

// markCheckpointPublication records only a newly inserted descriptor-bearing
// row. A mixed request may replay old references while adding inline rows; those
// committed references must not undergo a new artifact admission.
func markCheckpointPublication(ctx context.Context, encoding string, data []byte) error {
	if encoding == "" {
		return nil
	}
	if err := validatePayloadEncoding(encoding, data); err != nil {
		return err
	}
	attempt, _ := ctx.Value(checkpointAttemptKey{}).(*checkpointAttempt)
	if attempt == nil {
		return invalid("checkpoint artifact publication has no operation guard")
	}
	var ref references.CheckpointArtifact
	if err := json.Unmarshal(data, &ref); err != nil {
		return invalid("invalid checkpoint artifact descriptor")
	}
	attempt.refs = append(attempt.refs, ref)
	return nil
}

func checkpointReferenceContext(ctx context.Context, scope contracts.Scope, checkpoints []Checkpoint, writes []PendingWrite) (context.Context, error) {
	hasReferences := false
	appendRef := func(encoding string, data []byte) error {
		if encoding == "" {
			return nil
		}
		if err := validatePayloadEncoding(encoding, data); err != nil {
			return err
		}
		hasReferences = true
		return nil
	}
	for _, cp := range checkpoints {
		if err := appendRef(cp.MetadataEncoding, cp.Metadata); err != nil {
			return ctx, err
		}
		for _, part := range cp.Parts {
			if err := appendRef(part.Encoding, part.Data); err != nil {
				return ctx, err
			}
		}
	}
	for _, write := range writes {
		if err := appendRef(write.Encoding, write.Data); err != nil {
			return ctx, err
		}
	}
	if !hasReferences {
		return ctx, nil
	}
	return context.WithValue(ctx, checkpointAttemptKey{}, &checkpointAttempt{id: contracts.NewID(), scope: scope}), nil
}

func checkpointOperationKey(id string) string { return "ahp_state:checkpoint-reference:" + id }

// CheckpointOperationSettled proves that the single-use operation no longer
// owns its transaction lock. The lock is acquired BEFORE pins, and cannot be
// released while the transaction can still commit, even after connection loss.
// No query against absence of a checkpoint substitutes for this proof.
func (s *Store) CheckpointOperationSettled(ctx context.Context, id string) (bool, error) {
	if err := validIdentifier(id, "checkpoint operation"); err != nil {
		return false, err
	}
	var settled bool
	// Use a distinct read-owner transaction, not inTx: ctx may belong to the
	// still-running writer when a reconciler probes it.
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, checkpointOperationKey(id)).Scan(&settled)
	})
	return settled, err
}

func (s *Store) releaseCheckpointAttempt(attempt *checkpointAttempt) {
	if attempt == nil || s.checkpointArtifacts == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	settled, err := s.CheckpointOperationSettled(ctx, attempt.id)
	if err == nil && settled {
		// Failure to release retains protection. The GC reconciler retries
		// through the same settlement proof; it never expires pins by time.
		_ = s.checkpointArtifacts.ReleaseCheckpointReferences(ctx, attempt.id)
	}
}
