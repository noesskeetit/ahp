package artifacts

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"strings"
	"testing"
)

func TestTenantFenceRejectsPreviouslyUnknownOwner(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	owner, ok := any(e.store).(interface {
		FenceTenant(context.Context, contracts.Scope) error
	})
	if !ok {
		t.Fatal("Artifacts owner has no tenant barrier")
	}
	if err := owner.FenceTenant(ctx, e.scope); err != nil {
		t.Fatal(err)
	}
	scope := e.scope
	scope.Subject = "new-subject"
	scope.Deployment = "new-deployment"
	req := UploadRequest{Name: "late", MIME: "text/plain", Size: 1, SHA256: strings.Repeat("a", 64)}
	_, err := e.store.BeginUpload(ctx, scope, "late", req)
	codeIs(t, err, contracts.ErrForbidden)
	scope.Tenant += "-other"
	if _, err = e.store.BeginUpload(ctx, scope, "foreign", req); err != nil {
		t.Fatal(err)
	}
}
