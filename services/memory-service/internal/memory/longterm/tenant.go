package memory

import "context"

// updateIdentity registers the owner in the same transaction as its records.
// The store serializes this registration against the tenant erasure fence, so
// the coordinator's subsequent inventory cannot miss a newly admitted bank.
func (s *Service) updateIdentity(ctx context.Context, id Identity, fn func(Tx) error) error {
	return s.update(ctx, id.Owner(), func(tx Tx) error {
		if fenced, err := subjectFenced(tx); err != nil {
			return err
		} else if fenced {
			return Fail(410, "subject_deleted", "memory subject has been erased")
		}
		if err := tx.RegisterIdentity(id); err != nil {
			return err
		}
		return fn(tx)
	})
}

// A tenant fence freezes automatic retention as well as writes. The platform
// may preserve this tenant under legal hold. Only an explicit subject-erasure
// intent authorizes bank cleanup after the fence.
func preserveTenant(tx Tx) (bool, error) {
	frozen, err := tx.TenantFrozen()
	if err != nil || !frozen {
		return false, err
	}
	fenced, err := subjectFenced(tx)
	return !fenced, err
}
