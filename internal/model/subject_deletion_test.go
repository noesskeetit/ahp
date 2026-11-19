package model

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
)

func TestModelSubjectErasureKeepsUnknownAndOtherSubject(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	svc := e.service(nil)
	budget, err := e.budget.Root(ctx, e.scope, "task", "root", 1000)
	if err != nil {
		t.Fatal(err)
	}
	g := chaosSubmit(t, e, svc, "private-prompt", budget.ID, "reserve", 100)
	if _, err = e.ledger.Dispatch(ctx, e.scope, g.EffectKey); err != nil {
		t.Fatal(err)
	}
	other := e.scope
	other.Subject = "other"
	otherBudget, err := e.budget.Root(ctx, other, "task", "root", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if err = svc.FenceSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if err = svc.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err == nil {
		t.Fatal("unknown generation erased")
	}
	got, err := e.budget.Get(ctx, e.scope, budget.ID)
	if err != nil || got.Reserved != 100 || got.Spent != 0 {
		t.Fatal("unproven expense was refunded or settled")
	}
	if _, err = e.budget.Root(ctx, e.scope, "late", "late", 100); err == nil {
		t.Fatal("late budget admitted")
	}
	if _, err = e.budget.Reserve(ctx, e.scope, budget.ID, "late", 10); err == nil {
		t.Fatal("late reservation admitted")
	}
	if _, err = e.ledger.Accept(ctx, e.scope, operations.Request{Key: "late", Kind: EffectKind, Target: "late", Arguments: json.RawMessage(`{}`)}); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("late effect: %v", err)
	}
	// Trusted original executor/reconciler supplies a real known receipt; no new
	// inference call occurs. Fixture accounting is explicit, not a live LLM proof.
	receipt, _ := json.Marshal(resultPayload{Status: GenCompleted, Usage: Usage{TotalTokens: 27}})
	if _, err = e.ledger.Reconcile(ctx, e.scope, g.EffectKey, receipt, false); err != nil {
		t.Fatal(err)
	}
	if err = svc.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, err = e.gens.Get(ctx, e.scope, g.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("prompt survives %v", err)
	}
	if _, err = e.budget.Get(ctx, e.scope, budget.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("budget survives %v", err)
	}
	if _, err = e.budget.Get(ctx, other, otherBudget.ID); err != nil {
		t.Fatal("other subject lost")
	}
	if err = svc.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal("replay", err)
	}
}

func TestModelSubjectErasureCancelsProvenUnsent(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	svc := e.service(nil)
	b, err := e.budget.Root(ctx, e.scope, "task", "root", 1000)
	if err != nil {
		t.Fatal(err)
	}
	g := chaosSubmit(t, e, svc, "unsent", b.ID, "reservation", 100)
	if err = svc.FenceSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, err = e.ledger.Dispatch(ctx, e.scope, g.EffectKey); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("post-fence dispatch: %v", err)
	}
	if err = svc.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = e.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_model.generations WHERE tenant=$1 AND subject=$2`, e.scope.Tenant, e.scope.Subject).Scan(&count); err != nil || count != 0 {
		t.Fatal("generation remains", err)
	}
}

func TestModelSubjectFenceLateKnownProviderReceipt(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	inf := &fakeInference{model: "fixture"}
	svc := e.service(inf)
	inf.newStr = func(ChatRequest) (Stream, error) {
		return &fakeStream{deltas: []Delta{{Text: "known fixture output"}}, usage: Usage{TotalTokens: 19}, errAt: -1, onDelta: func(int) {
			if err := svc.FenceSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
				t.Error(err)
			}
		}}, nil
	}
	b, err := e.budget.Root(ctx, e.scope, "task", "root", 1000)
	if err != nil {
		t.Fatal(err)
	}
	g := chaosSubmit(t, e, svc, "late-ack", b.ID, "reservation", 100)
	if _, err = svc.Execute(ctx, e.scope, g.ID); err != nil {
		t.Fatal("late receipt lost", err)
	}
	op, err := e.ledger.Get(ctx, e.scope, g.EffectKey)
	if err != nil || (op.Status != operations.StatusSucceeded && op.Status != operations.StatusFailed) {
		t.Fatal("known outcome still unknown", err)
	}
	settled, err := e.budget.Get(ctx, e.scope, b.ID)
	if err != nil || settled.Spent != 19 || settled.Reserved != 0 {
		t.Fatal("actual charge lost", err)
	}
	if err = svc.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if inf.count() != 1 {
		t.Fatal("provider replayed")
	}
}

func TestModelSubjectMissingFilesOwnerCannotFinalize(t *testing.T) {
	e, files, a, source, _ := fileFixture(t)
	ctx := context.Background()
	if _, err := files.Copy(ctx, a, "known", source.ref.ID); err != nil {
		t.Fatal(err)
	}
	svc := e.service(nil)
	if err := svc.DeleteSubject(ctx, a.Scope.Tenant, a.Scope.Subject); err == nil {
		t.Fatal("missing driver completed remote erasure")
	}
	svc.WithProviderFiles(files)
	if err := svc.DeleteSubject(ctx, a.Scope.Tenant, a.Scope.Subject); err != nil {
		t.Fatal(err)
	}
}
