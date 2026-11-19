package artifacts

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// SubjectScopes reports all deployments holding this subject's artifact data.
// It is a control-plane discovery API, not an unscoped object read.
func (s *Store) SubjectScopes(ctx context.Context, scope contracts.Scope) ([]contracts.Scope, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, scope, "delete", "artifacts:subject"); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT deployment FROM ahp_artifacts.artifacts WHERE tenant=$1 AND subject=$2 ORDER BY deployment`, scope.Tenant, scope.Subject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []contracts.Scope
	for rows.Next() {
		next := scope
		if err = rows.Scan(&next.Deployment); err != nil {
			return nil, err
		}
		out = append(out, next)
	}
	return out, rows.Err()
}
