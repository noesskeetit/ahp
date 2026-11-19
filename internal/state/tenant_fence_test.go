package state

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
)

func TestTenantFenceRejectsPreviouslyUnknownSubject(t *testing.T) {
	ctx := context.Background()
	pool, _ := isolatedPool(t)
	st := New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: "tenant-erasure-" + contracts.NewID(), Deployment: "writer", Subject: "alice"}
	_, err := st.CreateThread(ctx, scope, "existing", "existing")
	if err != nil {
		t.Fatal(err)
	}
	owner, ok := any(st).(interface {
		FenceTenant(context.Context, string) error
	})
	if !ok {
		t.Fatal("State owner has no tenant barrier")
	}
	if err = owner.FenceTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	scope.Subject = "new-subject"
	scope.Deployment = "new-deployment"
	if _, err = st.CreateThread(ctx, scope, "late", "late"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("tenant resurrected: %v", err)
	}
	scope.Tenant += "-other"
	if _, err = st.CreateThread(ctx, scope, "foreign", "foreign"); err != nil {
		t.Fatal(err)
	}
}
