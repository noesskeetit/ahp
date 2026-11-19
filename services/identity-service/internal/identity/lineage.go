package identity

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// IsDescendant reports whether targetTask names a live-or-dead grant that is a
// PROPER descendant of ancestorTask inside one scope. It is the lineage oracle
// behind the mint guard: a task token may mint a credential for itself or for a
// grant below it, never for an ancestor, a sibling or an unrelated task. The
// walk climbs parent pointers (coalescing a root's empty parent to its root id)
// exactly as the ancestor-active check does, so the answer agrees with how the
// tree is stored. A missing target is simply not a descendant; a missing
// ancestor is a caller error.
func (s *Store) IsDescendant(ctx context.Context, scope contracts.Scope, ancestorTask, targetTask string) (bool, error) {
	if err := scope.Validate(); err != nil {
		return false, err
	}
	if err := validateTask(ancestorTask); err != nil {
		return false, err
	}
	if err := validateTask(targetTask); err != nil {
		return false, err
	}
	if ancestorTask == targetTask {
		return false, nil
	}
	var found bool
	err := s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		anc, ok, err := findGrant(ctx, tx, scope, ancestorTask)
		if err != nil {
			return err
		}
		if !ok {
			return contracts.NewError(contracts.ErrNotFound, "identity: ancestor grant not found")
		}
		tgt, ok, err := findGrant(ctx, tx, scope, targetTask)
		if err != nil {
			return err
		}
		if !ok {
			found = false
			return nil
		}
		var hit bool
		if err := tx.QueryRow(ctx, `
		    WITH RECURSIVE anc(ord, id) AS (
		        SELECT 0, $1::text
		        UNION ALL
		        SELECT a.ord + 1, coalesce(nullif(c.parent, ''), c.root)
		          FROM anc a
		          JOIN ahp_identity.grants_child c ON c.id = a.id
		         WHERE a.ord < 32
		    )
		    SELECT EXISTS (SELECT 1 FROM anc WHERE id = $2 AND ord > 0)`,
			tgt.ID, anc.ID).Scan(&hit); err != nil {
			return fmt.Errorf("identity: resolve grant lineage: %w", err)
		}
		found = hit
		return nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}
