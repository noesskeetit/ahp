package workspace

import (
	"context"
	"errors"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// MembershipFacts is owner data, not a policy verdict. IAM receives these facts
// from this store on the current request, never from a browser-supplied role.
type MembershipFacts struct{ ProjectID, Role string }
type ResourceAuthorize func(context.Context, contracts.Scope, MembershipFacts, string) error

// SetResourceAuthority is called by composition before serving requests.
func (s *Store) SetResourceAuthority(authorize ResourceAuthorize) { s.resourceAuthorize = authorize }

func (s *Store) ReadMembershipFacts(ctx context.Context, scope contracts.Scope, project string) (MembershipFacts, error) {
	w, err := s.resolveWorkspace(ctx, scope, project)
	if err != nil {
		return MembershipFacts{}, err
	}
	f := MembershipFacts{ProjectID: w.ID}
	err = s.pool.QueryRow(ctx, `SELECT role FROM ahp_workspace.members WHERE workspace_id=$1 AND subject=$2`, w.ID, scope.Subject).Scan(&f.Role)
	if errors.Is(err, pgx.ErrNoRows) {
		return MembershipFacts{}, contracts.NewError(contracts.ErrForbidden, "workspace membership is not active")
	}
	return f, err
}
