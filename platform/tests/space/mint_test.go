package space_test

// mint_test.go — contract coverage for the RunCoordinator task-token mint that
// the journey contract (forum d71a7b0e, part B) requires: from an ALREADY
// authenticated turn scope the coordinator mints a short-lived KindTask token to
// present to the execution plane at /a2a/public (a human token carries no
// Deployment and is never valid there). This is pure Space capability (no PG, no
// live services); the live delegation leg (Space -> execution Public) is verified
// by IV on the deployed revision once execution (A) + delivery mTLS land.

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestCoordinatorMintTaskToken(t *testing.T) {
	const (
		issuer = "https://identity.test"
		kid    = "task-mint-1"
		aud    = space.DefaultAudience
	)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner(issuer, kid, priv)
	if err != nil {
		t.Fatal(err)
	}
	coord := space.NewCoordinator(space.CoordinatorConfig{TaskSigner: signer})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}
	tok, err := coord.MintTaskToken(scope, space.TaskTokenRequest{
		Root: "run-root-1", Context: "ctx-1", Grant: "grant-1", Depth: 1,
	})
	if err != nil {
		t.Fatalf("MintTaskToken: %v", err)
	}
	v := auth.NewVerifier(issuer, map[string]ed25519.PublicKey{kid: pub})
	claims, err := v.Verify(tok, aud, auth.KindTask, auth.WorkloadID(scope))
	if err != nil {
		t.Fatalf("Verify minted task-token (KindTask): %v", err)
	}
	if claims.Kind != auth.KindTask {
		t.Fatalf("minted token kind = %q, want %q", claims.Kind, auth.KindTask)
	}
	if claims.Tenant != scope.Tenant || claims.Deployment != scope.Deployment || claims.Subject != scope.Subject {
		t.Fatalf("minted token scope = %s/%s/%s, want %v", claims.Tenant, claims.Deployment, claims.Subject, scope)
	}
	if claims.Root != "run-root-1" || claims.Context != "ctx-1" || claims.Grant != "grant-1" || claims.Depth != 1 {
		t.Fatalf("minted token delegation claims mismatch: root=%q ctx=%q grant=%q depth=%d", claims.Root, claims.Context, claims.Grant, claims.Depth)
	}
	if claims.Task == "" {
		t.Fatalf("minted token must carry a task id")
	}
}

func TestCoordinatorMintTaskTokenFailsClosed(t *testing.T) {
	// No signer configured => minting is refused (deployment not authorized).
	noSigner := space.NewCoordinator(space.CoordinatorConfig{})
	if _, err := noSigner.MintTaskToken(contracts.Scope{Tenant: "t", Deployment: "d", Subject: "s"},
		space.TaskTokenRequest{Root: "r", Context: "c", Grant: "g"}); err == nil {
		t.Fatalf("MintTaskToken without a signer must fail closed")
	}

	// Signer present but an INCOMPLETE scope (no Deployment => not valid on the
	// execution plane) => refused, never minted.
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, err := auth.NewSigner("https://identity.test", "k", priv)
	if err != nil {
		t.Fatal(err)
	}
	coord := space.NewCoordinator(space.CoordinatorConfig{TaskSigner: signer})
	if _, err := coord.MintTaskToken(contracts.Scope{Tenant: "t", Subject: "s"}, // no Deployment
		space.TaskTokenRequest{Root: "r", Context: "c", Grant: "g"}); err == nil {
		t.Fatalf("MintTaskToken with an incomplete scope must fail closed")
	}
}
