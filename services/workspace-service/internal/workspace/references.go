package workspace

import (
	"context"
	"fmt"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReferenceSource exposes this owner's read-only reference API. It does not
// migrate storage or turn an unavailable table into an empty answer.
func ReferenceSource(pool *pgxpool.Pool, schema string) references.Owner {
	return referenceSource{pool, schema}
}

type referenceSource struct {
	pool   *pgxpool.Pool
	schema string
}

func (s referenceSource) InspectReferences(ctx context.Context, tenant string, refs []references.Ref, arts []references.Artifact) (references.Result, error) {
	out := references.Empty()
	if err := references.Validate(s.pool, s.schema); err != nil {
		return out, err
	}

	if len(arts) > 0 {
		ids, _ := references.ArtifactIDs(arts)
		q := fmt.Sprintf(`SELECT DISTINCT f.artifact_id FROM %[1]s.files f JOIN %[1]s.workspaces w ON w.id=f.workspace_id WHERE w.tenant=$1 AND f.artifact_id=ANY($2) AND (w.head_rev=f.rev_id OR EXISTS(SELECT 1 FROM %[1]s.pins p WHERE p.workspace_id=f.workspace_id AND p.rev_id=f.rev_id) OR EXISTS(SELECT 1 FROM %[1]s.branches b WHERE b.workspace_id=f.workspace_id AND b.rev_id=f.rev_id)) UNION SELECT DISTINCT f.artifact_id FROM %[1]s.candidate_files f JOIN %[1]s.candidates c ON c.id=f.candidate_id JOIN %[1]s.workspaces w ON w.id=c.workspace_id WHERE w.tenant=$1 AND f.artifact_id=ANY($2) AND c.state IN ('pending','conflict')`, s.schema)
		if err := references.QueryIDs(ctx, s.pool, &out, q, tenant, ids); err != nil {
			return out, err
		}
	}

	return out, nil
}
