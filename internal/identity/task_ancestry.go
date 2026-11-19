package identity

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// TaskAncestry returns the authoritative root-to-task chain under Identity's
// root lock. Budgets must not reconstruct parent ownership from caller metadata.
func (s *Store) TaskAncestry(ctx context.Context, scope contracts.Scope, task string) ([]Grant, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if err := validateTask(task); err != nil {
		return nil, err
	}
	var out []Grant
	err := s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		g, found, err := findGrant(ctx, tx, scope, task)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrNotFound, "identity: grant not found")
		}
		if err = lockRoot(ctx, tx, g.Root); err != nil {
			return err
		}
		g, found, err = findGrant(ctx, tx, scope, task)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrNotFound, "identity: grant not found")
		}
		if err = checkUsable(ctx, tx, g); err != nil {
			return err
		}
		chain, err := ancestorChain(ctx, tx, g)
		if err != nil {
			return err
		}
		if len(chain) != g.Depth+1 || len(chain) > MaxTreeDepth+1 || chain[len(chain)-1].ID != g.Root {
			return contracts.NewError(contracts.ErrConflict, "identity: incomplete task ancestry")
		}
		out = make([]Grant, len(chain))
		for i, n := range chain {
			if n.Tenant != scope.Tenant || n.Subject != scope.Subject {
				return forbidden("identity: task ancestry changes owner")
			}
			out[len(chain)-1-i] = n
		}
		return nil
	})
	return out, err
}
