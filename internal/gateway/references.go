package gateway

import (
	"context"
	"fmt"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/jackc/pgx/v5"
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

	tasks, contexts := references.IDs(refs)
	if len(refs) > 0 {
		q := fmt.Sprintf(`SELECT task_id,context_id FROM %[1]s.tasks WHERE tenant=$1 AND terminal_at IS NULL AND state NOT IN ('TASK_STATE_COMPLETED','TASK_STATE_FAILED','TASK_STATE_CANCELED','TASK_STATE_REJECTED') AND (task_id=ANY($2) OR context_id=ANY($3))`, s.schema)
		if err := references.Each(ctx, s.pool, q, func(rows pgx.Rows) error {
			var task, context string
			if err := rows.Scan(&task, &context); err != nil {
				return err
			}
			references.Mark(&out, refs, []string{task}, []string{context})
			return nil
		}, tenant, tasks, contexts); err != nil {
			return out, err
		}
	}
	if len(arts) > 0 {
		ids, _ := references.ArtifactIDs(arts)
		q := fmt.Sprintf(`SELECT a.id FROM unnest($2::text[]) AS a(id) WHERE EXISTS(SELECT 1 FROM %[1]s.tasks t WHERE t.tenant=$1 AND t.artifacts::text LIKE '%%'||chr(34)||a.id||chr(34)||'%%')`, s.schema)
		if err := references.QueryIDs(ctx, s.pool, &out, q, tenant, ids); err != nil {
			return out, err
		}
	}

	return out, nil
}
