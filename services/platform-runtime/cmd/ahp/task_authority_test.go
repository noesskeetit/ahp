package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type recordingRenewer struct {
	task, audience string
	ttl            time.Duration
	capability     string
	authzErr       error
}

func (r *recordingRenewer) MintTask(_ context.Context, scope contracts.Scope, task string,
	_ *auth.Signer, audience string, ttl time.Duration) (string, error) {
	r.task, r.audience, r.ttl = scope.Deployment+"|"+task, audience, ttl
	return "identity-renewed-token", nil
}

func (r *recordingRenewer) Authorize(_ context.Context, _ auth.Claims, capability string) error {
	r.capability = capability
	return r.authzErr
}

func testSigner(t *testing.T) *auth.Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	signer, err := auth.NewSigner("ahp", "dev-1", priv)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	return signer
}

func devIssuerReturning(token string) (stateapi.TaskIssuer, *int) {
	calls := new(int)
	return func(context.Context, contracts.Scope, string) (string, error) {
		*calls++
		return token, nil
	}, calls
}

var testScope = contracts.Scope{Tenant: "ahpstand", Deployment: "spacesvc", Subject: "demo-human"}

// Once the Agent Space human composition is mounted, State must stop minting run credentials
// itself: the token it hands out on ClaimRun is the credential Space later uses to revoke the
// run's own grant, and only Identity can issue something Identity will honour.
func TestBrowserCompositionHandsTheRunCredentialToIdentity(t *testing.T) {
	renewer := &recordingRenewer{}
	devIssue, devCalls := devIssuerReturning("locally-fabricated-token")

	authority := selectStateTaskAuthority(true, renewer, testSigner(t), devIssue)
	if authority.mode != authorityIdentityMode {
		t.Fatalf("browser composition must select the Identity-backed mode, got %q", authority.mode)
	}
	token, err := authority.issue(context.Background(), testScope, "rtask-demo-human:run-1")
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if token != "identity-renewed-token" {
		t.Fatalf("the run credential must come from Identity, got %q", token)
	}
	if *devCalls != 0 {
		t.Fatalf("the dev issuer must not be used at all, calls=%d", *devCalls)
	}
	if renewer.task != "spacesvc|rtask-demo-human:run-1" {
		t.Fatalf("renewal must name the run's own child task in the lease scope, got %q", renewer.task)
	}
	if renewer.audience != stateapiAudience {
		t.Fatalf("renewal audience must match the State API audience %q, got %q", stateapiAudience, renewer.audience)
	}
	if renewer.ttl != runTokenTTL {
		t.Fatalf("renewal ttl must be %v, got %v", runTokenTTL, renewer.ttl)
	}
	if err := authority.authorize(context.Background(), auth.Claims{Tenant: "ahpstand"}, "identity:cancel"); err != nil {
		t.Fatalf("authorize: %v", err)
	}
	if renewer.capability != "identity:cancel" {
		t.Fatalf("State must ask Identity about the route's capability, asked %q", renewer.capability)
	}
	// A refusal by Identity is a refusal by State: the live-grant check may never fail open.
	denier := &recordingRenewer{authzErr: errors.New("identity: grant is revoked")}
	denied := selectStateTaskAuthority(true, denier, testSigner(t), devIssue)
	if err := denied.authorize(context.Background(), auth.Claims{Tenant: "ahpstand"}, "identity:cancel"); err == nil {
		t.Fatal("an Identity refusal must not be swallowed by State")
	}
}

// The non-browser composition keeps the legacy shortcut - but it is labelled, so no operator can
// mistake a self-minted token for an Identity grant.
func TestNonBrowserCompositionLabelsTheDevShortcut(t *testing.T) {
	renewer := &recordingRenewer{}
	devIssue, devCalls := devIssuerReturning("locally-fabricated-token")

	authority := selectStateTaskAuthority(false, renewer, testSigner(t), devIssue)
	if authority.mode != authorityDevMode {
		t.Fatalf("non-browser composition must report the dev mode, got %q", authority.mode)
	}
	token, err := authority.issue(context.Background(), testScope, "run-1")
	if err != nil || token != "locally-fabricated-token" {
		t.Fatalf("dev mode must keep the dev issuer: token=%q err=%v", token, err)
	}
	if *devCalls != 1 {
		t.Fatalf("dev issuer should have been used once, got %d", *devCalls)
	}
	if renewer.task != "" {
		t.Fatalf("Identity must not be consulted in dev mode, saw task %q", renewer.task)
	}
}
