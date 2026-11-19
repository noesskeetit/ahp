package model

import (
	"context"
	"errors"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// StreamDelta is an actual provider text fragment with a durable per-generation
// cursor. It never contains an accumulated copy of earlier fragments.
type StreamDelta struct {
	GenerationID string `json:"generation_id"`
	Sequence     int64  `json:"sequence"`
	Text         string `json:"text"`
}

const maxGenerationStreamBytes = 2 << 20
const maxGenerationDeltas = 65536

const deltaDDL = `
ALTER TABLE ahp_model.generations ADD COLUMN IF NOT EXISTS delta_sequence bigint NOT NULL DEFAULT 0;
ALTER TABLE ahp_model.generations ADD COLUMN IF NOT EXISTS finish_reason text NOT NULL DEFAULT '';
-- Legacy rows retain their stored output; only new Submit opts into the ledger.
ALTER TABLE ahp_model.generations ADD COLUMN IF NOT EXISTS output_from_deltas boolean NOT NULL DEFAULT false;
ALTER TABLE ahp_model.generations ADD COLUMN IF NOT EXISTS streamed_bytes bigint NOT NULL DEFAULT 0 CHECK(streamed_bytes >= 0 AND streamed_bytes <= 2097152);
CREATE TABLE IF NOT EXISTS ahp_model.generation_deltas (
 generation_id text NOT NULL REFERENCES ahp_model.generations(id) ON DELETE CASCADE,
 sequence bigint NOT NULL CHECK(sequence > 0),
 text text NOT NULL CHECK(octet_length(text) > 0),
 PRIMARY KEY(generation_id,sequence)
);`

// AppendOutput journals an original provider delta and advances bounded metadata.
// New generations do not rewrite accumulated output; legacy/materialized rows
// retain their existing output-column semantics.
// The parent row serializes append/finalize/delete; the owner barriers prohibit
// new content after subject/tenant fencing but permit billing reconciliation.
func (s *GenerationStore) AppendOutput(ctx context.Context, scope contracts.Scope, id, text string, usage Usage) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if text == "" {
		return nil
	}
	if len(text) > maxGenerationStreamBytes {
		return contracts.NewError(contracts.ErrInvalid, "model: stream byte limit exceeded")
	}
	if s.boundary != contracts.CheckpointBoundaryChunk {
		// Experiment: hold the fragment in the volatile tail; the boundary flush
		// persists it in one transaction instead of one commit per fragment here.
		return s.appendOutputBuffered(ctx, scope, id, text, usage)
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		owner, err := operations.New(s.pool, "ahp_model")
		if err != nil {
			return err
		}
		if err = owner.CheckTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err = owner.CheckSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
			return err
		}
		var sequence int64
		err = tx.QueryRow(ctx, `UPDATE ahp_model.generations
   SET output=CASE WHEN output_from_deltas THEN output ELSE output||$2 END,
       streamed_bytes=CASE WHEN output_from_deltas THEN streamed_bytes+octet_length($2::text) ELSE streamed_bytes END,
       delta_sequence=delta_sequence+1,usage_prompt=$3,usage_completion=$4,usage_total=$5,updated_at=now()
   WHERE id=$1 AND tenant=$6 AND subject=$7 AND deployment=$8 AND NOT cancel_requested AND status IN ('PENDING','RUNNING','STREAMING')
   AND (CASE WHEN output_from_deltas THEN streamed_bytes ELSE octet_length(output) END)+octet_length($2::text)<=$9
   AND delta_sequence<$10 RETURNING delta_sequence`,
			id, text, usage.PromptTokens, usage.CompletionTokens, usage.TotalTokens, scope.Tenant, scope.Subject, scope.Deployment, maxGenerationStreamBytes, maxGenerationDeltas).Scan(&sequence)
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrConflict, "model: stream fenced, terminal or limit exceeded")
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO ahp_model.generation_deltas(generation_id,sequence,text) VALUES($1,$2,$3)`, id, sequence, text)
		return err
	})
}

// ReadDeltas replays only scoped durable fragments. Rows live exactly as long as
// their owning generation; subject/tenant erasure cascades without orphan content.
func (s *GenerationStore) ReadDeltas(ctx context.Context, scope contracts.Scope, id string, after int64, limit int) ([]StreamDelta, error) {
	if after < 0 || limit < 1 || limit > 256 {
		return nil, contracts.NewError(contracts.ErrInvalid, "model: invalid stream cursor or page size")
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	var exists int
	err := s.pool.QueryRow(ctx, `SELECT 1 FROM ahp_model.generations
		WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4`, id, scope.Tenant, scope.Subject, scope.Deployment).Scan(&exists)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, contracts.NewError(contracts.ErrNotFound, "generation not found")
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT d.sequence,d.text FROM ahp_model.generation_deltas d JOIN ahp_model.generations g ON g.id=d.generation_id WHERE g.id=$1 AND g.tenant=$2 AND g.subject=$3 AND g.deployment=$4 AND d.sequence>$5 ORDER BY d.sequence LIMIT $6`, id, scope.Tenant, scope.Subject, scope.Deployment, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]StreamDelta, 0)
	for rows.Next() {
		d := StreamDelta{GenerationID: id}
		if err = rows.Scan(&d.Sequence, &d.Text); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if s.boundary != contracts.CheckpointBoundaryChunk {
		out = s.mergeTailInto(id, scope, after, out)
		if len(out) > limit {
			out = out[:limit]
		}
	}
	return out, nil
}
