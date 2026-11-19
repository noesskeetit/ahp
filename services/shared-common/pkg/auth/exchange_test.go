package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestExchangeVerifierPurposeAudienceSignatureAndLifetime(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := NewSigner("ahp", "exchange", key)
	aud := "https://iam.example/token"
	keys := map[string]ed25519.PublicKey{"exchange": pub}
	valid, e := signer.SignExchangeAssertion(ExchangeAssertion{Audience: aud, TargetAudience: "mcp", Tenant: "tenant", Subject: "alice", Deployment: "agent", Task: "task", Grant: "grant", GrantEpoch: 1, Connection: "trusted", Resource: "urn:mcp:trusted", Scope: "read", Actor: "ahp:tool-gateway"})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = VerifyExchangeAssertion(valid, "ahp", aud, keys); e != nil {
		t.Fatal(e)
	}
	p := strings.Split(valid, ".")
	payload, _ := base64.RawURLEncoding.DecodeString(p[1])
	for _, tc := range []struct {
		name   string
		change map[string]any
	}{{"issuer", map[string]any{"iss": "foreign"}}, {"audience", map[string]any{"aud": "mcp"}}, {"public-purpose", map[string]any{"token_use": "access"}}, {"expired", map[string]any{"exp": time.Now().Add(-time.Minute).Unix()}}, {"extended-lifetime", map[string]any{"exp": time.Now().Add(time.Hour).Unix()}}, {"future", map[string]any{"iat": time.Now().Add(time.Minute).Unix(), "exp": time.Now().Add(2 * time.Minute).Unix()}}, {"actor", map[string]any{"act": "agent"}}, {"epoch", map[string]any{"grant_epoch": 0}}, {"extra-field", map[string]any{"human_token": "forbidden"}}} {
		t.Run(tc.name, func(t *testing.T) {
			var c map[string]any
			_ = json.Unmarshal(payload, &c)
			for k, v := range tc.change {
				c[k] = v
			}
			raw, _ := json.Marshal(c)
			input := p[0] + "." + base64.RawURLEncoding.EncodeToString(raw)
			token := input + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(input)))
			if _, e := VerifyExchangeAssertion(token, "ahp", aud, keys); e == nil {
				t.Fatal("invalid purpose/binding accepted")
			}
		})
	}
	bad := p[0] + "." + p[1] + "." + base64.RawURLEncoding.EncodeToString(make([]byte, 64))
	if _, e = VerifyExchangeAssertion(bad, "ahp", aud, keys); e == nil {
		t.Fatal("forged signature accepted")
	}
	duplicate := string(payload[:len(payload)-1]) + `,"aud":"` + aud + `"}`
	input := p[0] + "." + base64.RawURLEncoding.EncodeToString([]byte(duplicate))
	bad = input + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(input)))
	if _, e = VerifyExchangeAssertion(bad, "ahp", aud, keys); e == nil {
		t.Fatal("duplicate assertion fields accepted")
	}
}
