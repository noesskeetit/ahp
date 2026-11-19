package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
	"time"
)

type scopeCaptureModel struct {
	scope contracts.Scope
	calls int
}

func (m *scopeCaptureModel) Generate(ctx context.Context, _ *core.ModelRequest) (*core.ModelResponse, error) {
	m.scope, _, _ = core.RunScopeFromContext(ctx)
	m.calls++
	return &core.ModelResponse{}, nil
}
func TestGatewayModelCompletesBrowserScopeFromVerifiedRun(t *testing.T) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("ahp", "test", key)
	if err != nil {
		t.Fatal(err)
	}
	own := contracts.Scope{Tenant: "own", Deployment: "space", Subject: "person"}
	token, err := signer.Sign(auth.Claims{Subject: own.Subject, Tenant: own.Tenant, Deployment: own.Deployment, Audience: "ahp", Kind: auth.KindTask, Task: "task", Root: "root", Context: "ctx", Grant: "grant", Actors: []string{"space"}}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	inner := &scopeCaptureModel{}
	model := scopedGatewayModel{model: inner, authority: runScopeAttester{verifier: auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"test": signer.Public()}), taskAudience: "ahp", workload: auth.WorkloadID(own)}}
	human := own
	human.Deployment = ""
	if _, err := model.Generate(core.WithRunScope(context.Background(), human, token), &core.ModelRequest{}); err != nil {
		t.Fatal(err)
	}
	if inner.calls != 1 || inner.scope != own {
		t.Fatalf("model received wrong authority: %+v", inner.scope)
	}
	for _, bad := range []contracts.Scope{{Tenant: "foreign", Subject: "person"}, {Tenant: "own", Subject: "other"}, {Tenant: "own", Deployment: "other", Subject: "person"}} {
		if _, err := model.Generate(core.WithRunScope(context.Background(), bad, token), &core.ModelRequest{}); err == nil {
			t.Fatal("foreign scope accepted")
		}
	}
	if _, err := model.Generate(core.WithRunScope(context.Background(), human, token+"tampered"), &core.ModelRequest{}); err == nil {
		t.Fatal("unverified token accepted")
	}
	if inner.calls != 1 {
		t.Fatal("refused caller reached the model")
	}
}
