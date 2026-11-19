package state

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
		q := fmt.Sprintf(`SELECT r.id,r.task_id,t.id,t.context_id FROM %[1]s.runs r JOIN %[1]s.threads t ON t.id=r.thread_id WHERE t.tenant=$1 AND r.status NOT IN ('done','failed','canceled') AND (r.id=ANY($2) OR r.task_id=ANY($2) OR t.id=ANY($3) OR t.context_id=ANY($3))`, s.schema)
		if err := references.Each(ctx, s.pool, q, func(rows pgx.Rows) error {
			var run, task, thread, context string
			if err := rows.Scan(&run, &task, &thread, &context); err != nil {
				return err
			}
			references.Mark(&out, refs, []string{run, task}, []string{thread, context})
			return nil
		}, tenant, tasks, contexts); err != nil {
			return out, err
		}
	}
	if len(arts) > 0 {
		ids, _ := references.ArtifactIDs(arts)
		q := fmt.Sprintf(`SELECT a.id FROM unnest($2::text[]) AS a(id) WHERE EXISTS(SELECT 1 FROM %[1]s.kv k WHERE k.tenant=$1 AND NOT k.deleted AND k.value::text LIKE '%%'||chr(34)||a.id||chr(34)||'%%') OR EXISTS(SELECT 1 FROM %[1]s.stream_events e JOIN %[1]s.threads t ON t.id=e.thread_id WHERE t.tenant=$1 AND e.payload::text LIKE '%%'||chr(34)||a.id||chr(34)||'%%')`, s.schema)
		if err := references.QueryIDs(ctx, s.pool, &out, q, tenant, ids); err != nil {
			return out, err
		}
		// Codec is persisted outside opaque user bytes. CASE avoids decoding
		// legacy binary even when PostgreSQL reorders predicate evaluation.
		q = fmt.Sprintf(`SELECT a.id FROM unnest($2::text[]) AS a(id) WHERE
          EXISTS(SELECT 1 FROM %[1]s.checkpoints c JOIN %[1]s.threads t ON t.id=c.thread_id
            WHERE t.tenant=$1 AND CASE WHEN c.metadata_encoding=$3 THEN c.metadata->>'id' ELSE NULL END=a.id)
          OR EXISTS(SELECT 1 FROM %[1]s.checkpoint_blobs b JOIN %[1]s.threads t ON t.id=b.thread_id
            WHERE t.tenant=$1 AND CASE WHEN b.encoding=$3 THEN convert_from(b.data,'UTF8')::jsonb->>'id' ELSE NULL END=a.id)
          OR EXISTS(SELECT 1 FROM %[1]s.checkpoint_writes w JOIN %[1]s.threads t ON t.id=w.thread_id
            WHERE t.tenant=$1 AND CASE WHEN w.encoding=$3 THEN convert_from(w.data,'UTF8')::jsonb->>'id' ELSE NULL END=a.id)`, s.schema)
		if err := references.QueryIDs(ctx, s.pool, &out, q, tenant, ids, CheckpointArtifactEncoding); err != nil {
			return out, err
		}

	}

	return out, nil
}
