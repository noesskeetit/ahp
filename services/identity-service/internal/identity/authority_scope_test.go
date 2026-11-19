package identity

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
)

// TestAuthorityContextScopeIsNotInheritedByChildren proves the O2 invariant at
// the identity layer: the ctx_scope=authority claim is present ONLY on the
// per-session authority token minted by the human -> Space exchange, and a
// child (Tier-2) grant delegated from that authority and minted through
// MintTask carries NO ctx_scope and keeps its own context claim. A child token
// with no ctx_scope is therefore still bound to exactly its own conversation,
// which is what makes a foreign-conversation read of the same subject return
// 403 at the Artifacts gate (the gate exempts ONLY ctx_scope=authority).
//
// It skips without AHP_TEST_DATABASE_URL like the rest of this package; the
// live cross-conversation 403 is measured on the isolated download leg.
func TestAuthorityContextScopeIsNotInheritedByChildren(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	signer, err := auth.NewSigner("https://auth.test", "kid-1", priv)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	const audience = "ahp-platform"

	// The root must carry the explicit self-delegation grant. The child below
	// lives in the SAME deployment, and repeating a deployment actor in the chain
	// is forbidden unless the root granted it (internal/identity/delegate.go).
	// Without the grant this test failed at Delegate and never reached the
	// ctx_scope assertions it is named for — it was asserting nothing at all.
	authRoot := rootRequest("auth-task", "ctx-auth", scope.Deployment, time.Hour, "read", "write")
	authRoot.AllowSelf = true
	if _, err := s.CreateRoot(ctx, scope, authRoot); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}

	authTok, err := s.MintAuthorityTask(ctx, scope, "auth-task", signer, audience, 0)
	if err != nil {
		t.Fatalf("MintAuthorityTask: %v", err)
	}
	scopeClaim, ctxClaim := decodeCtxScopeAndCtx(t, authTok)
	if scopeClaim != auth.CtxScopeAuthority {
		t.Fatalf("authority token ctx_scope = %q, want %q", scopeClaim, auth.CtxScopeAuthority)
	}
	if ctxClaim == "" {
		t.Fatal("authority token must still carry a non-empty ctx claim")
	}

	child := "child-task-" + sanitizeID(scope.Tenant)
	if _, err := s.Delegate(ctx, scope, "auth-task", DelegateRequest{
		Task:         child,
		Context:      "ctx-child",
		Deployment:   scope.Deployment,
		Capabilities: []string{"read"},
		TTL:          time.Hour,
	}); err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	childTok, err := s.MintTask(ctx, scope, child, signer, audience, 0)
	if err != nil {
		t.Fatalf("MintTask(child): %v", err)
	}
	childScope, childCtx := decodeCtxScopeAndCtx(t, childTok)
	if childScope != "" {
		t.Fatalf("child token must NOT inherit ctx_scope, got %q", childScope)
	}
	if childCtx == "" {
		t.Fatal("child token must keep its own non-empty ctx claim (so the gate still binds it)")
	}
}

// decodeCtxScopeAndCtx returns the ctx_scope and ctx claims carried in the
// signed token payload (payload segment only; signature is not checked here).
func decodeCtxScopeAndCtx(t *testing.T, token string) (ctxScope, ctx string) {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("malformed token: %d segments", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	cs, _ := m["ctx_scope"].(string)
	cc, _ := m["ctx"].(string)
	return cs, cc
}
