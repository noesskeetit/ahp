package auth

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Fixed identities shared by the auth tests. The values match the SPIFFE
// workload the tests bind task tokens to.
const (
	testIssuer   = "https://auth.ahp.internal"
	testAudience = "ahp-platform"
	testKID      = "key-1"
)

// newKey returns a fresh Ed25519 key pair.
func newKey(t *testing.T) (ed25519.PrivateKey, ed25519.PublicKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("cannot generate test key: %v", err)
	}
	return priv, pub
}

// mustKey returns a deterministic private key for signers that only need a
// key that is not the primary test key.
func mustKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	priv, _ := newKey(t)
	return priv
}

// newPair returns a signer and a verifier that trust exactly that signer.
func newPair(t *testing.T) (*Signer, *Verifier) {
	t.Helper()
	priv, pub := newKey(t)
	signer, err := NewSigner(testIssuer, testKID, priv)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	verifier := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	return signer, verifier
}

// taskClaims is a valid task token payload before the signer stamps issuer,
// token_use and the time window.
func taskClaims() Claims {
	return Claims{
		Subject:    "user-ada",
		Audience:   testAudience,
		Kind:       KindTask,
		Tenant:     "acme",
		Deployment: "prod-eu",
		Task:       "task-123",
		Root:       "run-1",
		Context:    "ctx-1",
		Grant:      "grant-1",
		Depth:      1,
		Actors:     []string{"parent-deploy", "prod-eu"},
	}
}

// humanClaims is a valid human token payload.
func humanClaims() Claims {
	return Claims{
		Subject:  "human:ada",
		Audience: testAudience,
		Kind:     KindHuman,
		Tenant:   "acme",
		Actors:   []string{"human:ada"},
	}
}

// signTask mints a five-minute task token with the given signer.
func signTask(t *testing.T, signer *Signer) string {
	t.Helper()
	token, err := signer.Sign(taskClaims(), 5*time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return token
}

// taskWorkload is the SPIFFE identity the task test fixtures bind to.
func taskWorkload() string {
	return WorkloadID(contracts.Scope{Tenant: "acme", Deployment: "prod-eu", Subject: "task-123"})
}

// mustReject fails the test unless Verify rejects the token with a
// FORBIDDEN-classified error.
func mustReject(t *testing.T, v *Verifier, token, audience, kind, workload string) {
	t.Helper()
	_, err := v.Verify(token, audience, kind, workload)
	if err == nil {
		t.Fatal("expected Verify to reject the token")
	}
	if token != "" && strings.Contains(err.Error(), token) {
		t.Fatalf("error must not echo the token: %v", err)
	}
}

// defaultHeader is the JOSE header the fixtures re-sign payloads with.
func defaultHeader() map[string]any {
	return map[string]any{"typ": "JWT", "alg": "EdDSA", "kid": testKID}
}

// validTaskPayload is a fully signed-eligible task payload for the given now.
func validTaskPayload(now int64) map[string]any {
	return map[string]any{
		"iss":        testIssuer,
		"sub":        "user-ada",
		"aud":        testAudience,
		"exp":        now + 300,
		"iat":        now,
		"nbf":        now,
		"token_use":  TokenUse,
		"token_kind": KindTask,
		"tenant":     "acme",
		"azp":        "prod-eu",
		"task":       "task-123",
		"root":       "run-1",
		"ctx":        "ctx-1",
		"grant":      "grant-1",
		"depth":      1,
		"act":        []string{"parent-deploy", "prod-eu"},
	}
}

// validHumanPayload is a fully signed-eligible human payload for the given now.
func validHumanPayload(now int64) map[string]any {
	return map[string]any{
		"iss":        testIssuer,
		"sub":        "human:ada",
		"aud":        testAudience,
		"exp":        now + 3600,
		"iat":        now,
		"nbf":        now,
		"token_use":  TokenUse,
		"token_kind": KindHuman,
		"tenant":     "acme",
		"depth":      0,
		"act":        []string{"human:ada"},
	}
}

// jsonBytes marshals v, failing the test on error.
func jsonBytes(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return raw
}

// payloadMap decodes the payload segment of a compact token into a mutable
// map.
func payloadMap(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token must have three segments: %d", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	return m
}

// reSign builds a compact token from raw header and payload bytes, signing the
// exact ASCII bytes with priv under the given kid.
func reSign(t *testing.T, priv ed25519.PrivateKey, kid string, headerRaw, payloadRaw []byte) string {
	t.Helper()
	enc := base64.RawURLEncoding.EncodeToString
	signingInput := enc(headerRaw) + "." + enc(payloadRaw)
	sig := ed25519.Sign(priv, []byte(signingInput))
	return signingInput + "." + enc(sig)
}
