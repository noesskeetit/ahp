package auth

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSignerRejectsHumanCtxScope(t *testing.T) {
	signer, _ := newPair(t)
	c := humanClaims()
	c.CtxScope = CtxScopeAuthority
	if _, err := signer.Sign(c, time.Hour); err == nil {
		t.Fatal("human token must not be signable with ctx_scope")
	}
}

func TestSignerRejectsUnknownTaskCtxScope(t *testing.T) {
	signer, _ := newPair(t)
	c := taskClaims()
	c.CtxScope = "not-authority"
	if _, err := signer.Sign(c, time.Minute); err == nil {
		t.Fatal("task token with an unknown ctx_scope value must be rejected")
	}
}

func TestAuthorityTaskRoundTripKeepsClaim(t *testing.T) {
	signer, verifier := newPair(t)
	c := taskClaims()
	c.CtxScope = CtxScopeAuthority
	token, err := signer.Sign(c, 5*time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	got, err := verifier.Verify(token, testAudience, KindTask, taskWorkload())
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.CtxScope != CtxScopeAuthority {
		t.Fatalf("ctx_scope = %q, want %q", got.CtxScope, CtxScopeAuthority)
	}
	if got.Context != "ctx-1" {
		t.Fatalf("ctx = %q, want ctx-1: ctx must stay mandatory and unchanged", got.Context)
	}
}

func TestLegacyTaskTokenOmitsCtxScope(t *testing.T) {
	signer, verifier := newPair(t)
	token := signTask(t, signer)
	if strings.Contains(claimsJSONForTest(t, token), "\"ctx_scope\"") {
		t.Fatal("empty ctx_scope must be omitted from the signed bytes")
	}
	got, err := verifier.Verify(token, testAudience, KindTask, taskWorkload())
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.CtxScope != "" {
		t.Fatalf("legacy token ctx_scope = %q, want empty", got.CtxScope)
	}
}

func TestAuthorityClaimOnlyAddsOneMember(t *testing.T) {
	signer, _ := newPair(t)
	plain := signTask(t, signer)
	c := taskClaims()
	c.CtxScope = CtxScopeAuthority
	withClaim, err := signer.Sign(c, 5*time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if claimsJSONForTest(t, plain) == claimsJSONForTest(t, withClaim) {
		t.Fatal("authority token payload must carry the ctx_scope member")
	}
}

func claimsJSONForTest(t *testing.T, token string) string {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("malformed token: %d parts", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	for _, k := range []string{"exp", "iat", "nbf"} {
		delete(m, k)
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	return string(b)
}
