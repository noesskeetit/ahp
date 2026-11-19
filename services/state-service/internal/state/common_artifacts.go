package state

// Experimental explicit references. Opaque native attributes never become
// lifecycle roots just because an artifact ID or URL occurs in their bytes.
import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
)

const CommonArtifactProfileSuffix = "/artifacts/research-1"

type CommonArtifactReference struct {
	RecordID string                        `json:"record_id"`
	Artifact references.CheckpointArtifact `json:"artifact"`
}

func WithCommonArtifactReferences() StoreOption {
	return func(s *Store) { s.commonRecords = true; s.commonArtifactReferences = true }
}

const commonArtifactDDL = `CREATE TABLE IF NOT EXISTS ahp_state.common_artifact_refs(
 thread_id text NOT NULL,namespace text NOT NULL,revision bigint NOT NULL,record_id text NOT NULL,artifact jsonb NOT NULL,
 PRIMARY KEY(thread_id,namespace,revision,record_id),
 FOREIGN KEY(thread_id,namespace,revision,record_id) REFERENCES ahp_state.common_records(thread_id,namespace,revision,id) ON DELETE CASCADE);
 CREATE INDEX IF NOT EXISTS common_artifact_id_idx ON ahp_state.common_artifact_refs((artifact->>'id'));`

func commonArtifactMarker(ref references.CheckpointArtifact) json.RawMessage {
	b, _ := json.Marshal(struct {
		Encoding string                        `json:"encoding"`
		Ref      references.CheckpointArtifact `json:"ref"`
	}{CheckpointArtifactEncoding, ref})
	return b
}
func validateCommonArtifactReferences(scope contracts.Scope, doc CommonDocument, refs []CommonArtifactReference) error {
	if len(refs) > 1000 {
		return invalid("common artifact reference count exceeds limit")
	}
	if len(refs) > 0 && !strings.HasSuffix(doc.Profile, CommonArtifactProfileSuffix) {
		return invalid("common external payload requires a distinct artifact-aware native profile")
	}
	seen := map[string]bool{}
	for _, ref := range refs {
		row, ok := doc.Records[ref.RecordID]
		if !ok || seen[ref.RecordID] {
			return invalid("common artifact record missing or duplicated")
		}
		seen[ref.RecordID] = true
		raw, e := json.Marshal(ref.Artifact)
		if e != nil {
			return invalid("common artifact descriptor")
		}
		if e = validatePayloadEncoding(CheckpointArtifactEncoding, raw); e != nil {
			return e
		}
		if ref.Artifact.Tenant != scope.Tenant || ref.Artifact.Deployment != scope.Deployment || ref.Artifact.Subject != scope.Subject {
			return contracts.NewError(contracts.ErrForbidden, "common artifact scope differs from actor")
		}
		actual, e := contracts.Digest(row.Attributes)
		if e != nil {
			return invalid("common artifact marker must be strict JSON")
		}
		expected, e := contracts.Digest(commonArtifactMarker(ref.Artifact))
		if e != nil || actual != expected {
			return invalid("common artifact marker differs from explicit reference")
		}
	}
	if strings.HasSuffix(doc.Profile, CommonArtifactProfileSuffix) {
		for id, row := range doc.Records {
			if len(row.Attributes) > MaxCheckpointPartBytes {
				return invalid("artifact-aware common record exceeds inline payload limit")
			}
			var marker map[string]json.RawMessage
			if json.Unmarshal(row.Attributes, &marker) == nil && marker != nil {
				var encoding string
				if json.Unmarshal(marker["encoding"], &encoding) == nil && encoding == CheckpointArtifactEncoding && !seen[id] {
					return invalid("common Artifact marker has no explicit lifecycle reference")
				}
			}
		}
	}
	return nil
}
func (s *Store) commonArtifactContext(ctx context.Context, scope contracts.Scope, req CommonCommit) (context.Context, error) {
	if len(req.ArtifactReferences) == 0 && !strings.HasSuffix(req.Document.Profile, CommonArtifactProfileSuffix) {
		return ctx, nil
	}
	if !s.commonArtifactReferences || s.checkpointArtifacts == nil {
		return ctx, contracts.NewError(contracts.ErrUnavailable, "common artifact reference owner is not configured")
	}
	if e := validateCommonArtifactReferences(scope, req.Document, req.ArtifactReferences); e != nil {
		return ctx, e
	}
	if len(req.ArtifactReferences) == 0 {
		return ctx, nil
	}
	return context.WithValue(ctx, checkpointAttemptKey{}, &checkpointAttempt{id: contracts.NewID(), scope: scope}), nil
}
func (s *Store) putCommonArtifactReferences(ctx context.Context, tx pgx.Tx, scope contracts.Scope, l *Lease, ns string, revision int64, refs []CommonArtifactReference) error {
	for _, ref := range refs {
		if ref.Artifact.ContextID != l.Thread.ContextID || ref.Artifact.TaskID != l.Run.TaskID {
			return contracts.NewError(contracts.ErrForbidden, "common artifact belongs to another context or task")
		}
		raw, e := json.Marshal(ref.Artifact)
		if e != nil {
			return e
		}
		if e = markCheckpointPublication(ctx, CheckpointArtifactEncoding, raw); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `INSERT INTO ahp_state.common_artifact_refs(thread_id,namespace,revision,record_id,artifact) VALUES($1,$2,$3,$4,$5)`, l.Thread.ID, ns, revision, ref.RecordID, raw); e != nil {
			return e
		}
	}
	return nil
}
func (s *Store) readCommonArtifactReferences(ctx context.Context, tx pgx.Tx, scope contracts.Scope, thread, ns string, revision int64, out *CommonRead) error {
	if !s.commonArtifactReferences {
		if strings.HasSuffix(out.Document.Profile, CommonArtifactProfileSuffix) {
			return contracts.NewError(contracts.ErrUnavailable, "artifact-aware common reader is not configured")
		}
		return nil
	}
	rows, e := tx.Query(ctx, `SELECT record_id,artifact FROM ahp_state.common_artifact_refs WHERE thread_id=$1 AND namespace=$2 AND revision=$3 ORDER BY record_id`, thread, ns, revision)
	if e != nil {
		return e
	}
	for rows.Next() {
		var ref CommonArtifactReference
		var raw []byte
		if e = rows.Scan(&ref.RecordID, &raw); e != nil {
			rows.Close()
			return e
		}
		if e = json.Unmarshal(raw, &ref.Artifact); e != nil {
			rows.Close()
			return e
		}
		out.ArtifactReferences = append(out.ArtifactReferences, ref)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	return validateCommonArtifactReferences(scope, out.Document, out.ArtifactReferences)
}

// Compose this explicit owner in the Artifact keeper when the feature is
// enabled. A missing table/owner is an error, never an empty liveness answer.
func CommonReferenceSource(pool *pgxpool.Pool, schema string) references.Owner {
	return commonReferenceSource{ReferenceSource(pool, schema), pool, schema}
}

type commonReferenceSource struct {
	legacy references.Owner
	pool   *pgxpool.Pool
	schema string
}

func (s commonReferenceSource) InspectReferences(ctx context.Context, tenant string, refs []references.Ref, arts []references.Artifact) (references.Result, error) {
	out, e := s.legacy.InspectReferences(ctx, tenant, refs, arts)
	if e != nil || len(arts) == 0 {
		return out, e
	}
	if e = references.Validate(s.pool, s.schema); e != nil {
		return out, e
	}
	ids, _ := references.ArtifactIDs(arts)
	q := `SELECT a.id FROM unnest($2::text[]) a(id) WHERE EXISTS(SELECT 1 FROM ` + s.schema + `.common_artifact_refs r JOIN ` + s.schema + `.threads t ON t.id=r.thread_id WHERE t.tenant=$1 AND r.artifact->>'id'=a.id)`
	if e = references.QueryIDs(ctx, s.pool, &out, q, tenant, ids); e != nil {
		return out, e
	}
	return out, nil
}

// CommonReferenceSourceIfPresent preserves roots after a writer feature is
// disabled; absence is permitted only for an owner that never initialized the
// common schema. Required mode always fails closed if its tables are missing.
func CommonReferenceSourceIfPresent(pool *pgxpool.Pool, schema string, required bool) references.Owner {
	return optionalCommonReferenceSource{commonReferenceSource{ReferenceSource(pool, schema), pool, schema}, required}
}

type optionalCommonReferenceSource struct {
	commonReferenceSource
	required bool
}

func (s optionalCommonReferenceSource) InspectReferences(ctx context.Context, tenant string, refs []references.Ref, arts []references.Artifact) (references.Result, error) {
	if len(arts) == 0 {
		return s.legacy.InspectReferences(ctx, tenant, refs, arts)
	}
	if s.required {
		return s.commonReferenceSource.InspectReferences(ctx, tenant, refs, arts)
	}
	if e := references.Validate(s.pool, s.schema); e != nil {
		return references.Empty(), e
	}
	var present bool
	if e := s.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL OR to_regclass($2) IS NOT NULL`, s.schema+".common_cuts", s.schema+".common_artifact_refs").Scan(&present); e != nil {
		return references.Empty(), e
	}
	if present {
		return s.commonReferenceSource.InspectReferences(ctx, tenant, refs, arts)
	}
	return s.legacy.InspectReferences(ctx, tenant, refs, arts)
}
