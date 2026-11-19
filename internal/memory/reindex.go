package memory

import (
	"context"
	"encoding/json"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// ReindexRequest explicitly rebuilds at most 20 currently visible active rows.
// The mutation key includes the target embedding space, so a changed model cannot
// replay an old batch. It requires the existing memory:review maintenance grant.
type ReindexRequest struct {
	OpKey string `json:"op_key"`
	Limit int    `json:"limit,omitempty"`
}
type ReindexResult struct {
	Indexed int  `json:"indexed"`
	Replay  bool `json:"replay,omitempty"`
}

func (s *Store) Reindex(ctx context.Context, scope contracts.Scope, req ReindexRequest) (ReindexResult, error) {
	if err := scope.Validate(); err != nil {
		return ReindexResult{}, err
	}
	if err := requireOpKey(req.OpKey); err != nil {
		return ReindexResult{}, err
	}
	if s.embedder == nil {
		return ReindexResult{}, contracts.NewError(contracts.ErrUnavailable, "memory: no embedding backend")
	}
	if err := s.authorize(ctx, scope, CapabilityReview, "memory:index"); err != nil {
		return ReindexResult{}, err
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 20 {
		limit = 20
	}
	digest, err := mutationDigest("reindex", s.embeddingSpace, limit)
	if err != nil {
		return ReindexResult{}, err
	}
	// This lock orders the whole batch with tenant erasure. Each source row is
	// locked before egress so labels cannot narrow between policy and embedding.
	tx, _, prior, replay, err := s.beginMutation(ctx, scope, req.OpKey, digest)
	if err != nil {
		return ReindexResult{}, err
	}
	defer tx.Rollback(ctx)
	if err := denyErasedTenant(ctx, tx, scope.Tenant); err != nil {
		return ReindexResult{}, err
	}
	if replay {
		return ReindexResult{Indexed: int(prior), Replay: true}, nil
	}
	ws, err := s.callerWorkspaces(ctx, scope)
	if err != nil {
		return ReindexResult{}, err
	}
	where, args := s.admissible(scope, ws, "")
	args = append(args, s.embeddingSpace)
	rows, err := tx.Query(ctx, `SELECT `+columns+` FROM ahp_memory.memories WHERE `+where+` AND (embedding IS NULL OR embedding_space<>$`+itoa(len(args))+`) ORDER BY id LIMIT `+itoa(limit)+` FOR UPDATE`, args...)
	if err != nil {
		return ReindexResult{}, storageError("reindex candidates", err)
	}
	var records []Memory
	for rows.Next() {
		m, err := scanMemory(rows)
		if err != nil {
			rows.Close()
			return ReindexResult{}, storageError("reindex scan", err)
		}
		records = append(records, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return ReindexResult{}, storageError("reindex rows", err)
	}
	count := 0
	for _, m := range records {
		ws, err := s.callerWorkspaces(ctx, scope)
		if err != nil {
			return ReindexResult{}, err
		}
		if !s.callerCanRead(ctx, m, scope, ws) {
			continue
		}
		ceiling, err := s.liveCeiling(ctx, scope)
		if err != nil {
			return ReindexResult{}, err
		}
		vec, err := s.embed(ctx, scope, m.Text, labels.Merge(ceiling, m.Labels))
		if err != nil {
			return ReindexResult{}, err
		}
		if err := s.checkEmbedding(vec); err != nil {
			return ReindexResult{}, err
		}
		set, _ := json.Marshal(m.Labels)
		tag, err := tx.Exec(ctx, `UPDATE ahp_memory.memories SET embedding=$3::vector,embedding_space=$4
   WHERE tenant=$1 AND id=$2 AND state='active' AND labels=$5::jsonb
   AND NOT EXISTS(SELECT 1 FROM ahp_memory.erased_subjects WHERE tenant=$1 AND subject=$6)`, scope.Tenant, m.ID, serializeVector(vec), s.embeddingSpace, set, m.Subject)
		if err != nil {
			return ReindexResult{}, storageError("reindex update", err)
		}
		count += int(tag.RowsAffected())
	}
	if err := finishMutation(ctx, tx, scope, req.OpKey, "", int64(count)); err != nil {
		return ReindexResult{}, err
	}
	return ReindexResult{Indexed: count}, nil
}
