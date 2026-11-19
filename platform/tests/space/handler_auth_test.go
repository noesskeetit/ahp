package space_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
)

// Real human-token authentication seam tests. Unlike two_subject_browser_test
// (which injects a principal from an X-Test-Subject header), these drive the
// Space HTTP handler through the PRODUCTION HumanAuthMiddleware, so the
// Ed25519 signature, issuer, audience and token-kind are actually verified by
// pkg/auth. The identity service mints these tokens; Space only verifies them,
// which is exactly the path a real deployment exercises.

const (
	authTestIssuer   = "ahp-identity"
	authTestAudience = "ahp" // == space.DefaultAudience
	authTestKID      = "k1"
)

// realTokenSpace builds the real Space HTTP handler behind the production
// HumanAuthMiddleware, trusting the given key map. It returns the base URL and
// a signer that mints tokens the verifier trusts (the launcher plays identity's
// SIGN role to seed the test; verification itself is the real code path).
func realTokenSpace(t *testing.T, trusted map[string]ed25519.PublicKey) (string, *auth.Signer) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	signer, err := auth.NewSigner(authTestIssuer, authTestKID, priv)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	if trusted == nil {
		trusted = map[string]ed25519.PublicKey{authTestKID: pub}
	}
	mw, err := space.HumanAuthMiddleware(space.HumanAuthConfig{
		Issuer:   authTestIssuer,
		Audience: authTestAudience,
		Keys:     trusted,
	})
	if err != nil {
		t.Fatalf("human auth middleware: %v", err)
	}
	srv := newHTTPSpace(t, &stubCore{}, mw)
	return srv.URL, signer
}

// signHumanToken mints a real human access token for a subject.
func signHumanToken(t *testing.T, signer *auth.Signer, subject string) string {
	t.Helper()
	tok, err := signer.Sign(auth.Claims{
		Audience: authTestAudience,
		Kind:     auth.KindHuman,
		Tenant:   "t1",
		Subject:  subject,
	}, 5*time.Minute)
	if err != nil {
		t.Fatalf("sign human(%s): %v", subject, err)
	}
	return tok
}

// runTurnWithToken drives one authenticated subject through the browser path
// using a real bearer token: AcceptTurn -> DispatchTurn -> poll to Done.
func runTurnWithToken(t *testing.T, base, token, content string) (string, string) {
	t.Helper()
	ctxID := "ctx-" + content // unique per content/subject
	code, body := postSpace(t, base, "AcceptTurn", map[string]any{
		"key":        "k-" + ctxID,
		"context_id": ctxID,
		"input":      map[string]any{"role": "user", "content": content},
		"max_steps":  3,
	}, token)
	if code != 200 {
		t.Fatalf("AcceptTurn = %d, want 200 (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	threadID, _ := acc["thread_id"].(string)
	if turnID == "" || threadID == "" {
		t.Fatalf("AcceptTurn missing ids: %s", body)
	}
	if c, b := postSpace(t, base, "DispatchTurn", nil, token); c != 200 {
		t.Fatalf("DispatchTurn = %d, want 200 (body=%s)", c, b)
	}
	deadline := time.Now().Add(3 * time.Second)
	var status string
	for time.Now().Before(deadline) {
		c, b := postSpace(t, base, "GetTurn", map[string]any{"turn_id": turnID}, token)
		if c != 200 {
			t.Fatalf("GetTurn poll = %d (body=%s)", c, b)
		}
		status, _ = decodeMap(t, b)["status"].(string)
		if status == string(space.TurnDone) || status == string(space.TurnFailed) || status == string(space.TurnCanceled) {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if status != string(space.TurnDone) {
		t.Fatalf("turn did not complete with a real token, status=%s", status)
	}
	return turnID, threadID
}

// TestSpaceRealHumanTokenTwoSubjectFlow proves the two-subject browser flow with
// REAL verified tokens: each subject's own human JWT is accepted, runs are
// isolated across subjects, and one subject can never read or mutate the other.
func TestSpaceRealHumanTokenTwoSubjectFlow(t *testing.T) {
	base, signer := realTokenSpace(t, nil)
	aliceTok := signHumanToken(t, signer, "alice")
	bobTok := signHumanToken(t, signer, "bob")

	aTurn, aThread := runTurnWithToken(t, base, aliceTok, "hello-from-A")
	bTurn, bThread := runTurnWithToken(t, base, bobTok, "hello-from-B")
	if aTurn == bTurn || aThread == bThread {
		t.Fatalf("subjects must not share ids: A=(%s,%s) B=(%s,%s)", aTurn, aThread, bTurn, bThread)
	}

	// Own activity is visible to the owner.
	if c, b := postSpace(t, base, "ReadStream", map[string]any{"thread_id": aThread, "after": 0, "limit": 100}, aliceTok); c != 200 {
		t.Fatalf("ReadStream(A as alice) = %d, want 200 (body=%s)", c, b)
	} else if evs, _ := decodeMap(t, b)["events"].([]any); len(evs) == 0 {
		t.Fatalf("ReadStream(A as alice) returned no events: %s", b)
	}

	// Cross-subject reads are 404 (no existence leak), never the other's data.
	if c, b := postSpace(t, base, "ReadStream", map[string]any{"thread_id": aThread, "after": 0, "limit": 100}, bobTok); c != 404 {
		t.Fatalf("ReadStream(A as bob) = %d, want 404 (leak!) body=%s", c, b)
	}
	if c, _ := postSpace(t, base, "ReadStream", map[string]any{"thread_id": bThread, "after": 0, "limit": 100}, aliceTok); c != 404 {
		t.Fatalf("ReadStream(B as alice) = %d, want 404 (leak!)", c)
	}
	if c, _ := postSpace(t, base, "GetTurn", map[string]any{"turn_id": aTurn}, bobTok); c != 404 {
		t.Fatalf("GetTurn(A as bob) = %d, want 404 (leak!)", c)
	}

	// ListTurns is subject-scoped.
	c, b := postSpace(t, base, "ListTurns", map[string]any{}, aliceTok)
	if c != 200 {
		t.Fatalf("ListTurns(alice) = %d, want 200 (body=%s)", c, b)
	}
	la := decodeMap(t, b)
	if !listContainsTurn(la, aTurn) {
		t.Fatalf("ListTurns(alice) missing own turn %s: %s", aTurn, b)
	}
	if listContainsTurn(la, bTurn) {
		t.Fatalf("ListTurns(alice) LEAKED bob's turn %s: %s", bTurn, b)
	}
	c, b = postSpace(t, base, "ListTurns", map[string]any{}, bobTok)
	if c != 200 {
		t.Fatalf("ListTurns(bob) = %d, want 200 (body=%s)", c, b)
	}
	lb := decodeMap(t, b)
	if !listContainsTurn(lb, bTurn) || listContainsTurn(lb, aTurn) {
		t.Fatalf("ListTurns(bob) wrong scope: own=%v leaked=%v (%s)", listContainsTurn(lb, bTurn), listContainsTurn(lb, aTurn), b)
	}
}

// TestSpaceHumanTokenRejections proves the production middleware refuses every
// untrusted credential with 403 and never installs a principal: missing token,
// malformed token, a token signed by an untrusted key, and a task token
// (which must never authenticate at the human-facing browser edge).
func TestSpaceHumanTokenRejections(t *testing.T) {
	base, signer := realTokenSpace(t, nil)
	valid := signHumanToken(t, signer, "alice")
	// Create real state so a rejection truly prevents access, not just a 404.
	aTurn, aThread := runTurnWithToken(t, base, valid, "secret-topic")

	// An untrusted signer: different keypair AND a kid the verifier does not trust.
	_, upriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	untrusted, err := auth.NewSigner(authTestIssuer, "k2", upriv)
	if err != nil {
		t.Fatalf("new untrusted signer: %v", err)
	}
	untrustedTok, err := untrusted.Sign(auth.Claims{
		Audience: authTestAudience, Kind: auth.KindHuman, Tenant: "t1", Subject: "alice",
	}, 5*time.Minute)
	if err != nil {
		t.Fatalf("sign untrusted: %v", err)
	}

	// A valid, correctly-signed TASK token (same trusted key) must still be
	// refused at the human edge: it is the wrong kind.
	taskTok, err := signer.Sign(auth.Claims{
		Audience: authTestAudience, Kind: auth.KindTask,
		Tenant: "t1", Deployment: "d1", Subject: "alice",
		Task: "task-1", Root: "root-1", Context: "ctx-1", Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatalf("sign task: %v", err)
	}

	for _, tc := range []struct {
		name  string
		token string
	}{
		{"missing", ""},
		{"malformed", "not-a-jwt"},
		{"untrusted-key", untrustedTok},
		{"task-token", taskTok},
	} {
		for _, q := range []struct {
			op   string
			body map[string]any
		}{
			{"ListTurns", map[string]any{}},
			{"GetTurn", map[string]any{"turn_id": aTurn}},
			{"ReadStream", map[string]any{"thread_id": aThread, "after": 0, "limit": 10}},
		} {
			c, b := postSpace(t, base, q.op, q.body, tc.token)
			if c != 403 {
				t.Fatalf("[%s] %s = %d, want 403 (body=%s)", tc.name, q.op, c, b)
			}
		}
	}
}

// TestHumanAuthConfigValidation proves HumanAuthMiddleware refuses a
// misconfigured verifier up front rather than silently failing closed.
func TestHumanAuthConfigValidation(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	goodKey := map[string]ed25519.PublicKey{authTestKID: pub}

	if _, err := space.HumanAuthMiddleware(space.HumanAuthConfig{Issuer: "", Keys: goodKey}); err == nil {
		t.Fatal("expected error for empty issuer")
	}
	if _, err := space.HumanAuthMiddleware(space.HumanAuthConfig{Issuer: authTestIssuer, Keys: nil}); err == nil {
		t.Fatal("expected error for empty key map")
	}
	if _, err := space.HumanAuthMiddleware(space.HumanAuthConfig{
		Issuer: authTestIssuer,
		Keys:   map[string]ed25519.PublicKey{"": pub},
	}); err == nil {
		t.Fatal("expected error for empty kid")
	}
	if _, err := space.HumanAuthMiddleware(space.HumanAuthConfig{
		Issuer: authTestIssuer,
		Keys:   map[string]ed25519.PublicKey{authTestKID: ed25519.PublicKey(make([]byte, 8))},
	}); err == nil {
		t.Fatal("expected error for non-32-byte key")
	}
	// A well-formed config succeeds and applies the default audience.
	if _, err := space.HumanAuthMiddleware(space.HumanAuthConfig{Issuer: authTestIssuer, Keys: goodKey}); err != nil {
		t.Fatalf("expected valid config to succeed, got %v", err)
	}
}
