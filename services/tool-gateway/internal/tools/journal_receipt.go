package tools

import (
	"context"
	"encoding/json"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func (s *Service) storeResult(ctx context.Context, scope contracts.Scope, binding artifacts.Binding, key string, payload []byte) (json.RawMessage, bool, error) {
	if binding.ContextID == "" || s.cfg.JournalArtifacts == nil {
		return s.spill(ctx, scope, payload)
	}
	if len(payload) <= s.artifactThreshold() {
		return nil, false, nil
	}
	ctx = artifacts.WithCallerContext(ctx, binding.ContextID)
	ref, err := s.cfg.JournalArtifacts.PutBoundBytes(ctx, scope, binding, key, payload)
	if err != nil {
		return nil, false, err
	}
	raw, err := json.Marshal(struct {
		Kind string `json:"kind"`
		ResultRef
	}{ArtifactRefKind, ResultRef{
		ArtifactID: ref.ID, Version: ref.Version, Digest: ref.SHA256, Size: ref.Size,
	}})
	return raw, err == nil, err
}

// ReceiptArtifactsReferenced protects the recorded result independently of
// whether the worker has already published the result in the State journal.
func (s *Service) ReceiptArtifactsReferenced(ctx context.Context, tenant string, candidates []artifacts.ArtifactQuery) (map[string]bool, error) {
	found := make(map[string]bool)
	if len(candidates) == 0 {
		return found, nil
	}
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.ID)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT result->>'artifact_id' FROM `+s.schema+`.operations
		WHERE tenant=$1 AND status='SUCCEEDED' AND result->>'kind'=$2
		  AND result->>'artifact_id'=ANY($3)`, tenant, ArtifactRefKind, ids)
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
