package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestSignedStateCapabilities(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("state-test", "test", private)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("state-test", map[string]ed25519.PublicKey{"test": public})
	claims := auth.Claims{Kind: auth.KindTask, Subject: "user", Tenant: "new-tenant", Deployment: "agent",
		Task: "worker", Root: "worker", Context: "ctx", Grant: "bound-grant", Audience: "ahp",
		Capabilities: []string{"state", "state.shared"}}
	token, err := signer.Sign(claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifier.Verify(token, "ahp", auth.KindTask, auth.WorkloadID(claims.Scope()))
	if err != nil {
		t.Fatal(err)
	}
	authorize, err := grantAuthorizer(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := authorize(context.Background(), verified, "state.shared"); err != nil {
		t.Fatal(err)
	}
	if err := authorize(context.Background(), verified, "state.retention"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("unsigned capability accepted: %v", err)
	}
	if _, err := verifier.Verify(token, "ahp", auth.KindTask, "spiffe://ahp/tenant/other/deployment/agent"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("workload mismatch accepted: %v", err)
	}
}

func TestGrantFileReloadDeniesRevokedScope(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "grants.json")
	write := func(id string) {
		t.Helper()
		data := []byte(`[{"tenant_id":"tenant","deployment_id":"dep","subject_id":"user","grant_id":"` +
			id + `","capabilities":["state"]}]`)
		if err := os.WriteFile(path+".tmp", data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path+".tmp", path); err != nil {
			t.Fatal(err)
		}
	}
	write("old")
	authorize, err := grantAuthorizer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	claims := auth.Claims{Tenant: "tenant", Deployment: "dep", Subject: "user", Grant: "old"}
	if err := authorize(ctx, claims, "state"); err != nil {
		t.Fatal(err)
	}
	write("new")
	newer := claims
	newer.Grant = "new"
	deadline := time.Now().Add(4 * time.Second)
	for authorize(ctx, newer, "state") != nil {
		if time.Now().After(deadline) {
			t.Fatal("new grant not loaded")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := authorize(ctx, claims, "state"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("revoked grant remains usable: %v", err)
	}
}
