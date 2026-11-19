package memory

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
		ids, digests := references.ArtifactIDs(arts)
		q := fmt.Sprintf(`SELECT a.id FROM unnest($2::text[],$3::text[]) AS a(id,dg) WHERE EXISTS(SELECT 1 FROM %[1]s.memories m WHERE m.tenant=$1 AND m.state<>'deleted' AND (m.evidence::text LIKE '%%'||chr(34)||a.id||chr(34)||'%%' OR (COALESCE(a.dg,'')<>'' AND m.evidence::text LIKE '%%'||chr(34)||a.dg||chr(34)||'%%')))`, s.schema)
		if err := references.QueryIDs(ctx, s.pool, &out, q, tenant, ids, digests); err != nil {
			return out, err
		}
	}

	return out, nil
}

// ReferenceSourceIfPresent is for an explicitly selected non-legacy backend.
// A retained legacy rollback corpus still owns its artifact evidence and must
// protect it. A native-only installation has no such corpus. This is not a
// fallback on query failure: a present but unreadable corpus always fails closed.
// Hindsight currently accepts verified State source references, not direct
// ArtifactRefs; those references remain protected by the State owner.
func ReferenceSourceIfPresent(pool *pgxpool.Pool, schema string) references.Owner {
	return optionalReferenceSource{referenceSource{pool, schema}}
}

type optionalReferenceSource struct{ legacy referenceSource }

func (s optionalReferenceSource) InspectReferences(ctx context.Context, tenant string, refs []references.Ref, arts []references.Artifact) (references.Result, error) {
	if err := references.Validate(s.legacy.pool, s.legacy.schema); err != nil {
		return references.Empty(), err
	}
	var present bool
	if err := s.legacy.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, s.legacy.schema+".memories").Scan(&present); err != nil {
		return references.Empty(), err
	}
	if !present {
		return references.Empty(), nil
	}
	return s.legacy.InspectReferences(ctx, tenant, refs, arts)
}
