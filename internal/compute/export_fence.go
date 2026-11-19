package compute

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// exportComputerState includes erasure admission, which a metadata read alone
// does not provide. Recheck this owner state before reading and publication.
func (s *Store) exportComputerState(ctx context.Context, scope contracts.Scope, id string) (Computer, error) {
	var computer Computer
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := s.checkOwnerWritable(ctx, tx, scope); err != nil {
			return err
		}
		var err error
		computer, err = s.getComputer(ctx, tx, scope, id)
		if err != nil {
			return err
		}
		if computer.Status == ComputerDestroyed {
			return contracts.NewError(contracts.ErrForbidden, "destroyed computer cannot export")
		}
		return nil
	})
	return computer, err
}
