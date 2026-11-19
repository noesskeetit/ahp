//go:build ahp_data

package main

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
	"time"
)

func TestControlSubjectModelUnknownThenReconcile(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	budget := model.NewBudgetStore(pool)
	gens := model.NewGenerationStore(pool)
	ledger, _ := operations.New(pool, "ahp_model")
	for _, migrate := range []func(context.Context) error{st.Migrate, budget.Migrate, gens.Migrate, ledger.Migrate} {
		if err := migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	svc := model.NewService(gens, budget, ledger, nil)
	b, err := budget.Root(ctx, scope, "task", "root", 1000)
	if err != nil {
		t.Fatal(err)
	}
	g, err := svc.Submit(ctx, model.SubmitArgs{Scope: scope, Task: "task", Key: "private-prompt", Model: "protocol-fixture", Request: model.ChatRequest{Messages: []model.Message{{Role: "user", Content: "private prompt retained on unknown"}}}, BudgetID: b.ID, ReserveKey: "reservation", ReserveAmount: 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ledger.Dispatch(ctx, scope, g.EffectKey); err != nil {
		t.Fatal(err)
	}
	cfg := ControlDeletionConfig{SubjectOwners: map[string]SubjectDeletionOwner{"model": svc}, RequiredSubjectOwners: []string{"model"}}
	server := cascadeServerConfigured(t, st, art, scope, nil, cfg)
	token, err := server.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	status, body := server.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"model-erasure"}`)
	if status != 503 || body["state"] != "pending" {
		t.Fatalf("unknown deletion %d %v", status, body)
	}
	if _, err = gens.Get(ctx, scope, g.ID); err != nil {
		t.Fatal("unknown prompt evidence removed")
	}
	missing := cascadeServerConfigured(t, st, art, scope, nil, ControlDeletionConfig{RequiredSubjectOwners: []string{"model"}})
	missingToken, _ := missing.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if status, _ = missing.adminCall(t, "ControlDeleteSubject", missingToken, scope, `{"key":"model-erasure"}`); status != 503 {
		t.Fatal("missing Model owner completed erasure")
	}
	if _, err = ledger.Reconcile(ctx, scope, g.EffectKey, json.RawMessage(`{"status":"COMPLETED","usage":{"total_tokens":27}}`), false); err != nil {
		t.Fatal(err)
	}
	status, body = server.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"model-erasure"}`)
	if status != 200 || body["state"] != "completed" {
		t.Fatalf("reconciled deletion %d %v", status, body)
	}
	if _, err = gens.Get(ctx, scope, g.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("Model prompt survived control erasure", err)
	}
	if _, err = budget.Get(ctx, scope, b.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("Model budget survived", err)
	}
}

func TestControlSubjectModelHeldRemainsPending(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	budget := model.NewBudgetStore(pool)
	gens := model.NewGenerationStore(pool)
	ledger, _ := operations.New(pool, "ahp_model")
	for _, migrate := range []func(context.Context) error{st.Migrate, budget.Migrate, gens.Migrate, ledger.Migrate} {
		if err := migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.CreateThread(ctx, scope, "held-context", "held"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetLegalhold(ctx, scope, "", "hold model evidence"); err != nil {
		t.Fatal(err)
	}
	svc := model.NewService(gens, budget, ledger, nil)
	server := cascadeServerConfigured(t, st, art, scope, nil, ControlDeletionConfig{SubjectOwners: map[string]SubjectDeletionOwner{"model": svc}, RequiredSubjectOwners: []string{"model"}})
	token, _ := server.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	status, body := server.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"held-model"}`)
	if status != 503 || body["state"] != "pending" {
		t.Fatalf("hold completed %d %v", status, body)
	}
	if err := st.RemoveLegalhold(ctx, scope, ""); err != nil {
		t.Fatal(err)
	}
	status, body = server.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"held-model"}`)
	if status != 200 || body["state"] != "completed" {
		t.Fatalf("released hold did not resume %d %v", status, body)
	}

}

func TestControlSubjectLegacyReceiptDoesNotSkipNewOwner(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	legacy := cascadeServer(t, st, art, scope)
	token, _ := legacy.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if code, body := legacy.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"legacy"}`); code != 200 {
		t.Fatalf("legacy fixture %d %v", code, body)
	}
	budget := model.NewBudgetStore(pool)
	gens := model.NewGenerationStore(pool)
	ledger, _ := operations.New(pool, "ahp_model")
	for _, migrate := range []func(context.Context) error{budget.Migrate, gens.Migrate, ledger.Migrate} {
		if err := migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	svc := model.NewService(gens, budget, ledger, nil)
	current := cascadeServerConfigured(t, st, art, scope, nil, ControlDeletionConfig{SubjectOwners: map[string]SubjectDeletionOwner{"model": svc}, RequiredSubjectOwners: []string{"model"}})
	token, _ = current.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if code, body := current.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"legacy"}`); code != 409 || body["reason"] != "deletion_owner_upgrade_required" {
		t.Fatalf("old receipt falsely completed %d %v", code, body)
	}
	if code, body := current.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"upgraded"}`); code != 200 || body["state"] != "completed" {
		t.Fatalf("new owner refused new operation %d %v", code, body)
	}
}
