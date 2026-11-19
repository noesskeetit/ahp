package state

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// Preflight runs before CreateThread commits. A syntactically valid export may
// still name another context's artifact, or a revoked/inaccessible version.
// Only explicit encoded references are inspected; legacy bytes are opaque.
func (s *Store) preflightImportReferences(ctx context.Context, scope contracts.Scope, exp ControlExport, target string) error {
	check := func(data []byte, encoding string) error {
		if encoding == "" {
			return nil
		}
		var ref references.CheckpointArtifact
		if json.Unmarshal(data, &ref) != nil {
			return invalid("invalid import artifact descriptor")
		}
		if ref.Tenant != scope.Tenant || ref.Deployment != scope.Deployment || ref.Subject != scope.Subject || ref.ContextID != target {
			return contracts.NewError(contracts.ErrForbidden, "import artifact must be rehomed by its owner into the target context")
		}
		return nil
	}
	for _, cp := range exp.Checkpoints {
		if err := check(cp.Metadata, cp.MetadataEncoding); err != nil {
			return err
		}
		for _, p := range cp.Parts {
			if err := check(p.Data, p.Encoding); err != nil {
				return err
			}
		}
	}
	var writes []PendingWrite
	for _, b := range exp.Writes {
		for _, w := range b.Writes {
			if err := check(w.Data, w.Encoding); err != nil {
				return err
			}
			writes = append(writes, w)
		}
	}
	guarded, err := checkpointReferenceContext(ctx, scope, exp.Checkpoints, writes)
	if err != nil {
		return err
	}
	// Ordinary write routes mark only rows they actually insert. Preflight is
	// explicitly an admission of every incoming descriptor before any thread
	// mutation, so collect all of them into this separate one-use operation.
	for _, cp := range exp.Checkpoints {
		if err := markCheckpointPublication(guarded, cp.MetadataEncoding, cp.Metadata); err != nil {
			return err
		}
		for _, part := range cp.Parts {
			if err := markCheckpointPublication(guarded, part.Encoding, part.Data); err != nil {
				return err
			}
		}
	}
	for _, write := range writes {
		if err := markCheckpointPublication(guarded, write.Encoding, write.Data); err != nil {
			return err
		}
	}
	// Acquires the same owner proof used by publication, then releases it. The
	// actual import takes a new pin; GC or revoke between phases fails closed.
	return s.inTx(guarded, func(pgx.Tx) error { return nil })
}
