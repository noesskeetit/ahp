package identityapi

import (
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func (c Config) workloadAllowed(scope contracts.Scope, exact, legacy []string) bool {
	if c.RequireExactWorkloadIDs || len(exact) > 0 {
		return contains(exact, auth.WorkloadID(scope))
	}
	return contains(legacy, scope.Deployment)
}
