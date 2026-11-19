package artifacts

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// Callers hold the common object locks before checking provisional State pins
// and committed references. A tombstone removes API readability even while GC
// correctly preserves the bytes, so its admission needs the same protection.
func (s *Store) tombstoneReferences(ctx context.Context, tx pgx.Tx, tenant string, query []ArtifactQuery) (map[string]bool, error) {
	held := map[string]bool{}
	if len(query) == 0 {
		return held, nil
	}
	ids := make([]string, len(query))
	for i, q := range query {
		ids[i] = q.ID
	}
	rows, err := tx.Query(ctx, `SELECT DISTINCT artifact_id FROM ahp_artifacts.checkpoint_pins WHERE tenant=$1 AND artifact_id=ANY($2::text[])`, tenant, ids)
	if err != nil {
		return nil, storageError("read checkpoint tombstone protection", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		held[id] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if s.keep != nil {
		current, err := s.keep.ArtifactsReferenced(ctx, tenant, query)
		if err != nil {
			return nil, contracts.NewError(contracts.ErrUnavailable, "live artifact reference authority is unavailable")
		}
		for id, on := range current {
			if on {
				held[id] = true
			}
		}
	}
	return held, nil
}
