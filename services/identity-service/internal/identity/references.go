package identity

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
		for _, table := range []string{"grants_root", "grants_child"} {
			q := fmt.Sprintf(`SELECT task,context_id FROM %s.%s WHERE tenant=$1 AND status='active' AND expires_at>now() AND (task=ANY($2) OR context_id=ANY($3))`, s.schema, table)
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
	}

	return out, nil
}
