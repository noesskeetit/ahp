package identityapi

import (
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
)

func TestExactWorkloadAuthorityNeverFallsBackToDeployment(t *testing.T) {
	c := Config{RequireExactWorkloadIDs: true}
	legacy := []string{"space"}
	exact := []string{"spiffe://ahp/tenant/approved/deployment/space"}
	for _, tc := range []struct {
		tenant string
		ids    []string
		want   bool
	}{{"approved", exact, true}, {"foreign", exact, false}, {"approved", nil, false}} {
		if got := c.workloadAllowed(contracts.Scope{Tenant: tc.tenant, Deployment: "space"}, tc.ids, legacy); got != tc.want {
			t.Fatalf("tenant=%s got=%v", tc.tenant, got)
		}
	}
}
