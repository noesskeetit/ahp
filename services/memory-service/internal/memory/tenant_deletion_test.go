package memory

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
)

func TestTenantFenceBlocksUnknownSubjectProposal(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	scope := e.scope("alice")
	owner, ok := any(e.mem).(interface {
		FenceTenant(context.Context, contracts.Scope) error
	})
	if !ok {
		t.Fatal("Memory owner has no tenant barrier")
	}
	if err := owner.FenceTenant(ctx, scope); err != nil {
		t.Fatal(err)
	}
	_, err := e.mem.Propose(ctx, e.scope("brand-new"), ProposeRequest{Kind: KindUser, Text: "late", OpKey: "late"})
	mustErr(t, err, contracts.ErrForbidden)
}
