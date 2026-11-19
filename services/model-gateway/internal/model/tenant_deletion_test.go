package model

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func TestTenantDeletionWaitsForModelSettlement(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	owner, ok := any(NewService(e.gens, e.budget, e.ledger, nil)).(interface {
		FenceTenant(context.Context, string) error
		DeleteTenant(context.Context, string) error
	})
	if !ok {
		t.Fatal("model owner has no tenant deletion")
	}
	labelStore := NewDeploymentLabelStore(e.gens.pool)
	if err := labelStore.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := labelStore.Set(ctx, e.scope.Tenant, "dep", labels.Set{}, "test"); err != nil {
		t.Fatal(err)
	}
	b, err := e.budget.Root(ctx, e.scope, "task", "root", 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.budget.Reserve(ctx, e.scope, b.ID, "external", 10); err != nil {
		t.Fatal(err)
	}
	if err = owner.FenceTenant(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if err = labelStore.Set(ctx, e.scope.Tenant, "late", labels.Set{}, "test"); err == nil {
		t.Fatal("late deployment label resurrected tenant")
	}
	if err = owner.DeleteTenant(ctx, e.scope.Tenant); err == nil {
		t.Fatal("unsettled model reservation erased")
	}
	if _, err = e.budget.Root(ctx, e.scope, "late", "late", 100); err == nil {
		t.Fatal("late budget resurrected tenant")
	}
	if _, err = e.budget.Settle(ctx, e.scope, b.ID, "external", 7); err != nil {
		t.Fatal(err)
	}
	if err = owner.DeleteTenant(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = e.budget.Get(ctx, e.scope, b.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("budget survived: %v", err)
	}
	var remaining int
	if err = e.gens.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_model.deployment_labels WHERE tenant=$1`, e.scope.Tenant).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("deployment labels survive: %d %v", remaining, err)
	}
}

func TestTenantDeletionAdoptsCommittedReceiptBeforeAtomicErase(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	svc := e.service(nil)
	b, err := e.budget.Root(ctx, e.scope, "task", "root", 1000)
	if err != nil {
		t.Fatal(err)
	}
	g := chaosSubmit(t, e, svc, "committed-before-crash", b.ID, "reservation", 100)
	if _, err = e.ledger.Dispatch(ctx, e.scope, g.EffectKey); err != nil {
		t.Fatal(err)
	}
	receipt, _ := json.Marshal(resultPayload{Status: GenCompleted, Usage: Usage{TotalTokens: 17}})
	if _, err = e.ledger.Complete(ctx, e.scope, g.EffectKey, receipt, false); err != nil {
		t.Fatal(err)
	}
	// An independent unresolved reservation prevents erasure. The generation's
	// actual bill must be adopted, while its terminal receipt remains available.
	if _, err = e.budget.Reserve(ctx, e.scope, b.ID, "independent", 10); err != nil {
		t.Fatal(err)
	}
	if err = svc.DeleteTenant(ctx, e.scope.Tenant); err == nil {
		t.Fatal("unsettled independent expense erased")
	}
	if _, err = e.ledger.Get(ctx, e.scope, g.EffectKey); err != nil {
		t.Fatal("receipt erased before content could commit", err)
	}
	got, err := e.budget.Get(ctx, e.scope, b.ID)
	if err != nil || got.Spent != 17 || got.Reserved != 10 {
		t.Fatal("known charge not recovered", err)
	}
	if _, err = e.budget.Settle(ctx, e.scope, b.ID, "independent", 7); err != nil {
		t.Fatal(err)
	}
	if err = svc.DeleteTenant(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = e.gens.Get(ctx, e.scope, g.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("generation survives", err)
	}
	if _, err = e.ledger.Get(ctx, e.scope, g.EffectKey); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("effect survives", err)
	}
}
