package workspace

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DeleteSubjectMemberships is a trusted control-plane erasure step. Shared
// revisions/candidates keep their content and references; membership and
// personal retention pins are removed. Candidate ownership is not inferred.
func (s *Store) DeleteSubjectMemberships(ctx context.Context, scope contracts.Scope, key string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if key == "" {
		return contracts.NewError(contracts.ErrInvalid, "workspace: deletion key required")
	}
	if err := s.authorize(ctx, scope, "member:remove", "subject:"+scope.Subject); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `DELETE FROM ahp_workspace.members m USING ahp_workspace.workspaces w WHERE m.workspace_id=w.id AND w.tenant=$1 AND m.subject=$2`, scope.Tenant, scope.Subject); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM ahp_workspace.pins p USING ahp_workspace.workspaces w WHERE p.workspace_id=w.id AND w.tenant=$1 AND p.subject=$2`, scope.Tenant, scope.Subject); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) AuthorizeSubjectDeletion(ctx context.Context, scope contracts.Scope) error {
	return s.authorize(ctx, scope, "member:remove", "subject:"+scope.Subject)
}
