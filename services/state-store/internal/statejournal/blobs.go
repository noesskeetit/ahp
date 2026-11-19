package statejournal

import (
	"context"
	"strings"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

const blobOperationPrefix = "ahp_journal:blob-publication:"

func addBlob(refs []BlobRef, payload Payload) []BlobRef {
	if payload.Blob == nil {
		return refs
	}
	return append(refs, *payload.Blob)
}

func appendBlobs(req AppendRequest) []BlobRef {
	var refs []BlobRef
	for _, event := range req.Events {
		refs = addBlob(refs, event.Payload)
	}
	for _, change := range req.KV {
		refs = addBlob(refs, change.Payload)
	}
	return addOutcomeBlobs(refs, req.ToolOutcomes)
}

func addOutcomeBlobs(refs []BlobRef, outcomes []IntentTransition) []BlobRef {
	for _, outcome := range outcomes {
		if outcome.Result != nil {
			refs = addBlob(refs, *outcome.Result)
		}
	}
	return refs
}

func addWriteBlobs(refs []BlobRef, writes []PendingWrite) []BlobRef {
	for _, write := range writes {
		refs = addBlob(refs, write.Payload)
	}
	return refs
}

// pinBlobs runs while the State transaction can publish the reference. GC
// cannot reap the intermediary pin while that transaction is active.
func (s *Store) pinBlobs(ctx context.Context, tx pgx.Tx, scope contracts.Scope, session string, blobs []BlobRef) error {
	if len(blobs) == 0 {
		return nil
	}
	if s.s3 != nil {
		seen := make(map[BlobRef]struct{}, len(blobs))
		for _, blob := range blobs {
			if _, ok := seen[blob]; ok {
				continue
			}
			if err := s.s3.verify(ctx, scope.Tenant, session, blob); err != nil {
				return err
			}
			seen[blob] = struct{}{}
		}
		return nil
	}
	if s.artifacts == nil {
		return contracts.NewError(contracts.ErrUnavailable, "journal artifact owner is unavailable")
	}
	ctx = artifacts.WithCallerContext(ctx, session)
	seen := make(map[string]BlobRef, len(blobs))
	verified := make([]references.CheckpointArtifact, 0, len(blobs))
	for _, blob := range blobs {
		if prior, ok := seen[blob.ArtifactID]; ok {
			if prior != blob {
				return contracts.NewError(contracts.ErrInvalid, "different versions of the same artifact")
			}
			continue
		}
		exact, err := s.artifacts.Get(ctx, scope, blob.ArtifactID)
		if err != nil {
			return err
		}
		if exact.Tenant != scope.Tenant || exact.Deployment != scope.Deployment ||
			exact.Subject != scope.Subject || exact.ContextID != session {
			return contracts.NewError(contracts.ErrNotFound, "journal artifact not found")
		}
		if exact.Version != blob.Version || exact.SHA256 != blob.SHA256 || exact.Size != blob.SizeBytes {
			return contracts.NewError(contracts.ErrInvalid, "journal artifact descriptor mismatch")
		}
		seen[blob.ArtifactID] = blob
		verified = append(verified, references.CheckpointArtifact{
			ID: exact.ID, Tenant: exact.Tenant, Deployment: exact.Deployment,
			Subject: exact.Subject, ContextID: exact.ContextID, TaskID: exact.TaskID,
			Version: exact.Version, SHA256: exact.SHA256, Size: exact.Size, Labels: exact.Labels,
		})
	}
	operation := "journal-" + contracts.NewID()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, blobOperationPrefix+operation); err != nil {
		return err
	}
	return s.artifacts.PinCheckpointReferences(ctx, scope, operation, verified)
}

// BlobPublicationSettled is the Artifacts GC callback for a compact journal.
// Use a separate reader pool so GC can run while State writers hold connections.
func (s *Store) BlobPublicationSettled(ctx context.Context, operation string) (bool, error) {
	if !validID(operation) || !strings.HasPrefix(operation, "journal-") {
		return false, contracts.NewError(contracts.ErrInvalid, "unknown journal artifact operation")
	}
	var settled bool
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, blobOperationPrefix+operation).Scan(&settled)
	})
	return settled, err
}

// ArtifactsReferenced provides the durable reference answer for Artifacts GC.
// References in rows retained after a session tombstone stay protected.
func (s *Store) ArtifactsReferenced(ctx context.Context, tenant string, candidates []artifacts.ArtifactQuery) (map[string]bool, error) {
	found := make(map[string]bool)
	if len(candidates) == 0 {
		return found, nil
	}
	ids := make([]string, 0, len(candidates))
	for _, item := range candidates {
		ids = append(ids, item.ID)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM unnest($2::text[]) a(id) WHERE
		     EXISTS (SELECT FROM ahp_journal.session_events e WHERE e.tenant_id=$1 AND e.blob_ref->>'artifact_id'=a.id)
		  OR EXISTS (SELECT FROM ahp_journal.checkpoints c WHERE c.tenant_id=$1 AND c.blob_ref->>'artifact_id'=a.id)
		  OR EXISTS (SELECT FROM ahp_journal.checkpoint_writes w WHERE w.tenant_id=$1 AND w.blob_ref->>'artifact_id'=a.id)
		  OR EXISTS (SELECT FROM ahp_journal.scoped_kv k WHERE k.tenant_id=$1 AND k.blob_ref->>'artifact_id'=a.id)
		  OR EXISTS (SELECT FROM ahp_journal.tool_intents t WHERE t.tenant_id=$1 AND t.response_blob_ref->>'artifact_id'=a.id)`,
		tenant, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		found[id] = true
	}
	return found, rows.Err()
}

// ArtifactLiveness answers whether a staged upload has a live writer. A
// committed blob is protected separately by ArtifactsReferenced.
func (s *Store) ArtifactLiveness(ctx context.Context, tenant string, refs []artifacts.Ref) (map[artifacts.Ref]bool, error) {
	found := make(map[artifacts.Ref]bool, len(refs))
	if len(refs) == 0 {
		return found, nil
	}
	kinds := make([]string, 0, len(refs))
	ids := make([]string, 0, len(refs))
	for _, ref := range refs {
		kinds = append(kinds, ref.Kind)
		ids = append(ids, ref.ID)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.kind,r.id,EXISTS (
		    SELECT FROM ahp_journal.sessions s
		    WHERE s.tenant_id=$1 AND s.status<>'DELETED' AND s.lease_expires_at>statement_timestamp()
		      AND ((r.kind=$4 AND s.session_id=r.id) OR (r.kind=$5 AND s.lease_owner=r.id))
		)
		FROM unnest($2::text[],$3::text[]) r(kind,id)`,
		tenant, kinds, ids, artifacts.PinKindContext, artifacts.PinKindTask)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ref artifacts.Ref
		var alive bool
		if err := rows.Scan(&ref.Kind, &ref.ID, &alive); err != nil {
			return nil, err
		}
		found[ref] = alive
	}
	return found, rows.Err()
}
