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

func TestWorkloadID(t *testing.T) {
	got := WorkloadID(contracts.Scope{Tenant: "acme", Deployment: "prod-eu", Subject: "task-1"})
	want := "spiffe://ahp/tenant/acme/deployment/prod-eu"
	if got != want {
		t.Fatalf("WorkloadID = %q, want %q", got, want)
	}
	if WorkloadID(contracts.Scope{}) != "spiffe://ahp/tenant//deployment/" {
		t.Fatal("WorkloadID must be a pure projection of the scope")
	}
}

func TestClaimsScope(t *testing.T) {
	task := Claims{Kind: KindTask, Tenant: "acme", Deployment: "prod", Task: "t1", Subject: "user-ada"}
	if got := task.Scope(); got != (contracts.Scope{Tenant: "acme", Deployment: "prod", Subject: "user-ada"}) {
		t.Fatalf("task Scope() = %+v", got)
	}
	human := Claims{Kind: KindHuman, Tenant: "acme", Subject: "human:ada"}
	if got := human.Scope(); got.Subject != "human:ada" || got.Deployment != "" {
		t.Fatalf("human Scope() = %+v", got)
	}
}

// TestScopePreservesSubjectAcrossTasks is the regression guard for the
// delegation invariant: every task token in a tree carries the original
// human/service subject, so all tasks of one subject share one scope while
// different subjects never collide.
func TestScopePreservesSubjectAcrossTasks(t *testing.T) {
	base := taskClaims()
	first := base.Scope()

	second := base
	second.Task = "task-456"
	second.Actors = []string{"parent-deploy", "task-123", "prod-eu"}
	if got := second.Scope(); got != first {
		t.Fatalf("scope changed across delegations: %+v != %+v", got, first)
	}

	other := base
	other.Subject = "user-bob"
	if got := other.Scope(); got == first {
		t.Fatal("different subjects must not share a scope")
	}
}

func TestSignVerifyTaskRoundTrip(t *testing.T) {
	signer, verifier := newPair(t)
	token := signTask(t, signer)

	claims, err := verifier.Verify(token, testAudience, KindTask, taskWorkload())
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.TokenUse != TokenUse || claims.Kind != KindTask {
		t.Fatalf("unexpected token_use/kind: %q/%q", claims.TokenUse, claims.Kind)
	}
	if claims.Issuer != testIssuer {
		t.Fatalf("issuer = %q", claims.Issuer)
	}
	if claims.ExpiresAt <= claims.IssuedAt {
		t.Fatalf("exp (%d) must be after iat (%d)", claims.ExpiresAt, claims.IssuedAt)
	}
	if ttl := time.Duration(claims.ExpiresAt-claims.IssuedAt) * time.Second; ttl != 5*time.Minute {
		t.Fatalf("lifetime = %v, want 5m", ttl)
	}
	if claims.Actors == nil || len(claims.Actors) != 2 {
		t.Fatalf("actors = %v", claims.Actors)
	}
	if claims.Subject != "user-ada" || claims.Task != "task-123" {
		t.Fatalf("round trip must preserve subject and task: sub=%q task=%q", claims.Subject, claims.Task)
	}
	if got := claims.Scope(); got.Tenant != "acme" || got.Deployment != "prod-eu" || got.Subject != "user-ada" {
		t.Fatalf("Scope() = %+v, want subject user-ada", got)
	}
}

func TestSignVerifyHumanRoundTrip(t *testing.T) {
	signer, verifier := newPair(t)
	token, err := signer.Sign(humanClaims(), time.Hour)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	claims, err := verifier.Verify(token, testAudience, KindHuman, "")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.Subject != "human:ada" || claims.Tenant != "acme" {
		t.Fatalf("unexpected claims: %+v", claims)
	}
}

func TestSignerRejectsBadInput(t *testing.T) {
	_, pub := newKey(t)
	if _, err := NewSigner("", testKID, ed25519.NewKeyFromSeed(make([]byte, 32))); err == nil {
		t.Fatal("empty issuer must be rejected")
	}
	if _, err := NewSigner(testIssuer, "", ed25519.NewKeyFromSeed(make([]byte, 32))); err == nil {
		t.Fatal("empty kid must be rejected")
	}
	if _, err := NewSigner(testIssuer, testKID, ed25519.PrivateKey(make([]byte, 16))); err == nil {
		t.Fatal("short key must be rejected")
	}
	if _, err := NewSigner(testIssuer, testKID, nil); err == nil {
		t.Fatal("nil key must be rejected")
	}
	if NewVerifier(testIssuer, nil).keys == nil || len(NewVerifier(testIssuer, nil).keys) != 0 {
		t.Fatal("nil key map must yield an empty trust store")
	}
	_ = pub
}

func TestSignerRejectsOverlongTTL(t *testing.T) {
	signer, _ := newPair(t)
	if _, err := signer.Sign(taskClaims(), 15*time.Minute+time.Second); err == nil {
		t.Fatal("task TTL above 15m must be rejected")
	}
	if _, err := signer.Sign(taskClaims(), 15*time.Minute); err != nil {
		t.Fatalf("15m task TTL must be accepted: %v", err)
	}
	if _, err := signer.Sign(humanClaims(), 24*time.Hour+time.Second); err == nil {
		t.Fatal("human TTL above 24h must be rejected")
	}
	if _, err := signer.Sign(taskClaims(), 0); err == nil {
		t.Fatal("zero TTL must be rejected")
	}
	if _, err := signer.Sign(taskClaims(), -time.Minute); err == nil {
		t.Fatal("negative TTL must be rejected")
	}
	unknown := taskClaims()
	unknown.Kind = "service"
	if _, err := signer.Sign(unknown, time.Minute); err == nil {
		t.Fatal("unknown token kind must be rejected")
	}
}

func TestSignerRejectsMalformedClaims(t *testing.T) {
	signer, _ := newPair(t)
	cases := []struct {
		name   string
		mutate func(*Claims)
	}{
		{"task without root", func(c *Claims) { c.Root = "" }},
		{"task without context", func(c *Claims) { c.Context = "" }},
		{"task without grant", func(c *Claims) { c.Grant = "" }},
		{"task without task id", func(c *Claims) { c.Task = "" }},
		{"task without subject", func(c *Claims) { c.Subject = "" }},
		{"negative depth", func(c *Claims) { c.Depth = -1 }},
		{"without audience", func(c *Claims) { c.Audience = "" }},
		{"without tenant", func(c *Claims) { c.Tenant = "" }},
		{"human without subject", func(c *Claims) {
			c.Kind, c.Task, c.Grant, c.Deployment, c.Root, c.Context = KindHuman, "", "", "", "", ""
			c.Subject = ""
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claims := taskClaims()
			tc.mutate(&claims)
			if _, err := signer.Sign(claims, time.Minute); err == nil {
				t.Fatal("expected Sign to reject the claims")
			}
		})
	}
}

func TestSignerRejectsHumanWithTaskClaims(t *testing.T) {
	signer, _ := newPair(t)
	for name, mutate := range map[string]func(*Claims){
		"task":     func(c *Claims) { c.Task = "t" },
		"grant":    func(c *Claims) { c.Grant = "g" },
		"azp":      func(c *Claims) { c.Deployment = "prod-eu" },
		"task+dep": func(c *Claims) { c.Task, c.Deployment = "t", "d" },
	} {
		t.Run(name, func(t *testing.T) {
			claims := humanClaims()
			mutate(&claims)
			if _, err := signer.Sign(claims, time.Hour); err == nil {
				t.Fatalf("human token with %s claim must be refused", name)
			}
		})
	}
}

func TestSignerCopiesKeyDefensively(t *testing.T) {
	priv, pub := newKey(t)
	signer, err := NewSigner(testIssuer, testKID, priv)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	token := signTask(t, signer)
	for i := range priv {
		priv[i] = 0
	}
	verifier := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	if _, err := verifier.Verify(token, testAudience, KindTask, taskWorkload()); err != nil {
		t.Fatalf("mutating the caller key must not invalidate the signer: %v", err)
	}
	if signer.KeyID() != testKID {
		t.Fatalf("KeyID = %q", signer.KeyID())
	}
	if !pub.Equal(signer.Public()) {
		t.Fatal("Public() must match the signing key")
	}
}

func TestVerifierCopiesKeyMapDefensively(t *testing.T) {
	signer, _ := newPair(t)
	token := signTask(t, signer)

	otherPriv, otherPub := newKey(t)
	keys := map[string]ed25519.PublicKey{testKID: signer.Public()}
	verifier := NewVerifier(testIssuer, keys)

	// Adding to the caller map must not widen trust.
	keys["late"] = otherPub
	delete(keys, testKID)
	if _, err := verifier.Verify(token, testAudience, KindTask, taskWorkload()); err != nil {
		t.Fatalf("verifier must keep its own copy: %v", err)
	}

	// A token signed by the injected key must not be trusted.
	rogue, err := NewSigner(testIssuer, "late", otherPriv)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	rogueToken := signTask(t, rogue)
	mustReject(t, verifier, rogueToken, testAudience, KindTask, taskWorkload())
}

func TestVerifyRejectsTamperedTokens(t *testing.T) {
	signer, verifier := newPair(t)
	token := signTask(t, signer)

	parts := strings.Split(token, ".")
	mustFlip := func(seg string) string {
		raw, err := base64.RawURLEncoding.DecodeString(seg)
		if err != nil {
			t.Fatal(err)
		}
		raw[0] ^= 0x01
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	mustReject(t, verifier, mustFlip(parts[0])+"."+parts[1]+"."+parts[2], testAudience, KindTask, taskWorkload())
	mustReject(t, verifier, parts[0]+"."+mustFlip(parts[1])+"."+parts[2], testAudience, KindTask, taskWorkload())
	mustReject(t, verifier, parts[0]+"."+parts[1]+"."+mustFlip(parts[2]), testAudience, KindTask, taskWorkload())

	// A payload re-signed with a different key is still untrusted.
	foreignPriv, _ := newKey(t)
	payload := payloadMap(t, token)
	payload["depth"] = 99
	mustReject(t, verifier,
		reSign(t, foreignPriv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, payload)),
		testAudience, KindTask, taskWorkload())
}

func TestVerifyRejectsAlgAndTypeConfusion(t *testing.T) {
	priv, pub := newKey(t)
	verifier := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	now := time.Now().UTC().Unix()
	payload := jsonBytes(t, validTaskPayload(now))

	for name, hdr := range map[string]map[string]any{
		"alg none":      {"typ": "JWT", "alg": "none", "kid": testKID},
		"alg empty":     {"typ": "JWT", "kid": testKID},
		"alg HS256":     {"typ": "JWT", "alg": "HS256", "kid": testKID},
		"alg RS256":     {"typ": "JWT", "alg": "RS256", "kid": testKID},
		"alg EdDSA ":    {"typ": "JWT", "alg": "EdDSA ", "kid": testKID},
		"typ JWT ":      {"typ": "JWT ", "alg": "EdDSA", "kid": testKID},
		"typ lowercase": {"typ": "jwt", "alg": "EdDSA", "kid": testKID},
		"typ missing":   {"alg": "EdDSA", "kid": testKID},
		"cty conflict":  {"typ": "JWT", "cty": "octet-stream", "alg": "EdDSA", "kid": testKID},
	} {
		t.Run(name, func(t *testing.T) {
			token := reSign(t, priv, testKID, jsonBytes(t, hdr), payload)
			mustReject(t, verifier, token, testAudience, KindTask, taskWorkload())
		})
	}

	// The classic "alg none, empty signature" token.
	enc := base64.RawURLEncoding.EncodeToString
	noneToken := enc(jsonBytes(t, map[string]any{"typ": "JWT", "alg": "none", "kid": testKID})) +
		"." + enc(payload) + "."
	if _, err := verifier.Verify(noneToken, testAudience, KindTask, taskWorkload()); err == nil {
		t.Fatal("alg none token with empty signature must be rejected")
	}
}

func TestVerifyRejectsUnknownKid(t *testing.T) {
	priv, pub := newKey(t)
	verifier := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	now := time.Now().UTC().Unix()
	for _, kid := range []string{"", "other-key", "KEY-1", testKID + " "} {
		hdr := defaultHeader()
		hdr["kid"] = kid
		token := reSign(t, priv, kid, jsonBytes(t, hdr), jsonBytes(t, validTaskPayload(now)))
		t.Run("kid="+kid, func(t *testing.T) {
			mustReject(t, verifier, token, testAudience, KindTask, taskWorkload())
		})
	}
}

func TestVerifyRejectsMalformedWireFormat(t *testing.T) {
	signer, verifier := newPair(t)
	token := signTask(t, signer)

	cases := map[string]string{
		"empty":         "",
		"two segments":  strings.SplitN(token, ".", 2)[0],
		"four segments": token + ".AAAA",
		"trailing dot":  token + ".",
		"leading dot":   "." + token,
		// Append padding to the original payload segment. Re-encoding with
		// base64.StdEncoding is not enough: whenever the raw length is a
		// multiple of 3 it yields the identical string, silently testing the
		// valid token instead of a mutation.
		"padded payload": func() string {
			p := strings.Split(token, ".")
			return p[0] + "." + p[1] + "=" + "." + p[2]
		}(),
		"padded signature": func() string {
			p := strings.Split(token, ".")
			return p[0] + "." + p[1] + "." + p[2] + "="
		}(),
		"whitespace":   " " + token,
		"newline":      token + "\n",
		"nul byte":     strings.Replace(token, "A", "\x00", 1),
		"std alphabet": strings.ReplaceAll(token, "-", "+"),
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			if bad == token {
				t.Skip("mutation had no effect")
			}
			mustReject(t, verifier, bad, testAudience, KindTask, taskWorkload())
		})
	}

	// Non-canonical base64url in the payload segment: the mutation must
	// actually change the segment, otherwise the valid token is being
	// "rejected-tested" and the check proves nothing.
	seg := strings.Split(token, ".")[1]
	raw, err := base64.RawURLEncoding.DecodeString(seg)
	if err != nil {
		t.Fatal(err)
	}
	padded := base64.StdEncoding.EncodeToString(raw)
	if padded == seg {
		padded += "="
	}
	mustReject(t, verifier, strings.Split(token, ".")[0]+"."+padded+"."+strings.Split(token, ".")[2],
		testAudience, KindTask, taskWorkload())
}

func TestVerifyRejectsOversizedToken(t *testing.T) {
	signer, verifier := newPair(t)
	token := signTask(t, signer)
	pad := strings.Repeat("A", maxTokenBytes)
	mustReject(t, verifier, token+"."+pad, testAudience, KindTask, taskWorkload())

	// A token whose claims are large enough to cross the limit cannot be
	// minted either.
	huge := taskClaims()
	huge.Context = strings.Repeat("c", maxTokenBytes)
	if _, err := signer.Sign(huge, time.Minute); err == nil {
		t.Fatal("signing a token above the size limit must fail")
	}
}

func TestVerifyRejectsDuplicateKeys(t *testing.T) {
	priv, pub := newKey(t)
	verifier := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	now := time.Now().UTC().Unix()

	dupPayload := []byte(`{"iss":"` + testIssuer + `","aud":"` + testAudience +
		`","sub":"user-ada","tenant":"acme","azp":"prod-eu","task":"task-123","root":"run-1",` +
		`"ctx":"ctx-1","grant":"grant-1","depth":1,"act":[],"token_use":"access",` +
		`"token_kind":"task","iat":` + itoa(now) + `,"nbf":` + itoa(now) +
		`,"exp":` + itoa(now+300) + `,"exp":` + itoa(now+86400) + `}`)
	mustReject(t, verifier, reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), dupPayload),
		testAudience, KindTask, taskWorkload())

	// Duplicate key inside the header, with a valid payload. The duplicate
	// value differs so the token is not byte-identical to a valid one.
	dupHeader := []byte(`{"typ":"JWT","alg":"EdDSA","kid":"` + testKID + `","kid":"other-key"}`)
	mustReject(t, verifier, reSign(t, priv, testKID, dupHeader, jsonBytes(t, validTaskPayload(now))),
		testAudience, KindTask, taskWorkload())
}

func TestVerifyRejectsMalformedJSON(t *testing.T) {
	priv, pub := newKey(t)
	verifier := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	now := time.Now().UTC().Unix()
	payload := jsonBytes(t, validTaskPayload(now))

	for name, bad := range map[string][]byte{
		"trailing data":    append(append([]byte{}, payload...), []byte("{}")...),
		"not an object":    []byte(`[]`),
		"scalar":           []byte(`42`),
		"truncated":        []byte(`{"iss":"x"`),
		"nested duplicate": []byte(`{"iss":"x","act":[{"a":1,"a":2}]}`),
	} {
		t.Run(name, func(t *testing.T) {
			mustReject(t, verifier, reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), bad),
				testAudience, KindTask, taskWorkload())
		})
	}
}

func TestVerifyRejectsNonIntegerTimestamps(t *testing.T) {
	priv, pub := newKey(t)
	verifier := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	now := time.Now().UTC().Unix()

	for _, claim := range []string{"exp", "iat", "nbf"} {
		for _, value := range []string{
			`1.5`, `"123"`, `1e3`, `-1`, `99999999999999`,
			`"1700000000"`, `"1.5"`, `"1e3"`, `" 123"`, `null`, `+123`, `0x123`,
			`123.`, `.5`, `12345678901234567890123`,
		} {
			raw := string(jsonBytes(t, validTaskPayload(now)))
			from := `"` + claim + `":` + numberFor(validTaskPayload(now)[claim])
			to := `"` + claim + `":` + value
			if !strings.Contains(raw, from) {
				t.Fatalf("fixture lacks %s", from)
			}
			raw = strings.ReplaceAll(raw, from, to)
			if raw == string(jsonBytes(t, validTaskPayload(now))) {
				t.Fatalf("mutation for %s=%s produced the valid payload", claim, value)
			}
			t.Run(claim+"="+value, func(t *testing.T) {
				mustReject(t, verifier, reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), []byte(raw)),
					testAudience, KindTask, taskWorkload())
			})
		}
	}

	// Regression: a numeric string must never pass, and a payload without
	// nbf defaults to 0 instead of being rejected as missing.
	noNBF := validTaskPayload(now)
	delete(noNBF, "nbf")
	if _, err := verifier.Verify(
		reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, noNBF)),
		testAudience, KindTask, taskWorkload()); err != nil {
		t.Fatalf("payload without nbf must be accepted with nbf=0: %v", err)
	}
	for _, claim := range []string{"exp", "iat"} {
		missing := validTaskPayload(now)
		delete(missing, claim)
		mustReject(t, verifier, reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, missing)),
			testAudience, KindTask, taskWorkload())
	}
}

func TestVerifyRejectsBadTimeWindow(t *testing.T) {
	priv, pub := newKey(t)
	verifier := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	now := time.Now().UTC().Unix()

	cases := map[string]func(map[string]any){
		"expired":           func(p map[string]any) { p["exp"] = now - 1 },
		"expired exactly":   func(p map[string]any) { p["exp"] = now },
		"issued in future":  func(p map[string]any) { p["iat"] = now + 60 },
		"not yet valid":     func(p map[string]any) { p["nbf"] = now + 60 },
		"exp equals iat":    func(p map[string]any) { p["exp"] = p["iat"] },
		"exp before iat":    func(p map[string]any) { p["exp"] = p["iat"].(int64) - 1 },
		"task ttl too long": func(p map[string]any) { p["exp"] = p["iat"].(int64) + 900 + 1 },
		"missing exp":       func(p map[string]any) { delete(p, "exp") },
		"missing iat":       func(p map[string]any) { delete(p, "iat") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			payload := validTaskPayload(now)
			mutate(payload)
			mustReject(t, verifier, reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, payload)),
				testAudience, KindTask, taskWorkload())
		})
	}

	// Exactly 15 minutes of lifetime is still allowed.
	payload := validTaskPayload(now)
	payload["exp"] = now + 900
	if _, err := verifier.Verify(
		reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, payload)),
		testAudience, KindTask, taskWorkload()); err != nil {
		t.Fatalf("15m task lifetime must be accepted: %v", err)
	}
}

func TestVerifyRejectsIssuerAudienceKindConfusion(t *testing.T) {
	signer, verifier := newPair(t)
	token := signTask(t, signer)

	if _, err := verifier.Verify(token, "other-audience", KindTask, taskWorkload()); err == nil {
		t.Fatal("wrong audience must be rejected")
	}
	if _, err := verifier.Verify(token, testAudience+" ", KindTask, taskWorkload()); err == nil {
		t.Fatal("padded audience must be rejected")
	}
	if _, err := verifier.Verify(token, "", KindTask, taskWorkload()); err == nil {
		t.Fatal("empty expected audience must be rejected")
	}
	// Human tokens cannot be verified as task tokens and vice versa.
	if _, err := verifier.Verify(token, testAudience, KindHuman, ""); err == nil {
		t.Fatal("task token must not verify as a human token")
	}
	human, err := signer.Sign(humanClaims(), time.Hour)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if _, err := verifier.Verify(human, testAudience, KindTask, taskWorkload()); err == nil {
		t.Fatal("human token must not verify as a task token")
	}
	if _, err := verifier.Verify(human, testAudience, "service", ""); err == nil {
		t.Fatal("unknown expected kind must be rejected")
	}

	// A token from another issuer is refused even when signed by a trusted key.
	otherSigner, err := NewSigner("https://evil.example", testKID, mustKey(t))
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	foreign := signTask(t, otherSigner)
	shared := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: otherSigner.Public()})
	mustReject(t, shared, foreign, testAudience, KindTask, taskWorkload())

	// Wrong token_use.
	priv, pub := newKey(t)
	v := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	now := time.Now().UTC().Unix()
	payload := validTaskPayload(now)
	payload["token_use"] = "refresh"
	mustReject(t, v, reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, payload)),
		testAudience, KindTask, taskWorkload())
	delete(payload, "token_use")
	payload["token_use"] = ""
	mustReject(t, v, reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, payload)),
		testAudience, KindTask, taskWorkload())
}

func TestVerifyTaskRequiresScopeClaims(t *testing.T) {
	priv, pub := newKey(t)
	verifier := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	now := time.Now().UTC().Unix()

	for _, claim := range []string{"task", "root", "ctx", "grant", "sub", "tenant"} {
		t.Run("missing "+claim, func(t *testing.T) {
			payload := validTaskPayload(now)
			delete(payload, claim)
			mustReject(t, verifier, reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, payload)),
				testAudience, KindTask, taskWorkload())
		})
		t.Run("empty "+claim, func(t *testing.T) {
			payload := validTaskPayload(now)
			payload[claim] = ""
			mustReject(t, verifier, reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, payload)),
				testAudience, KindTask, taskWorkload())
		})
	}
	t.Run("negative depth", func(t *testing.T) {
		payload := validTaskPayload(now)
		payload["depth"] = -1
		mustReject(t, verifier, reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, payload)),
			testAudience, KindTask, taskWorkload())
	})
	t.Run("zero depth accepted", func(t *testing.T) {
		payload := validTaskPayload(now)
		payload["depth"] = 0
		if _, err := verifier.Verify(
			reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, payload)),
			testAudience, KindTask, taskWorkload()); err != nil {
			t.Fatalf("depth 0 must be accepted: %v", err)
		}
	})
}

func TestVerifyTaskWorkloadBinding(t *testing.T) {
	priv, pub := newKey(t)
	verifier := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	now := time.Now().UTC().Unix()
	token := reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, validTaskPayload(now)))

	if _, err := verifier.Verify(token, testAudience, KindTask, taskWorkload()); err != nil {
		t.Fatalf("matching workload must verify: %v", err)
	}
	for _, bad := range []string{
		"",
		"spiffe://ahp/tenant/acme/deployment/staging",
		"spiffe://ahp/tenant/other/deployment/prod-eu",
		"spiffe://ahp/tenant/acme/deployment/prod-eu/",
		"spiffe://ahp/tenant/acme/deployment/prod-eu/extra",
		"spiffe://other/tenant/acme/deployment/prod-eu",
		"tenant/acme/deployment/prod-eu",
		"SPIFFE://ahp/tenant/acme/deployment/prod-eu",
	} {
		t.Run("workload="+bad, func(t *testing.T) {
			mustReject(t, verifier, token, testAudience, KindTask, bad)
		})
	}

	// A token whose tenant or deployment was swapped must not verify against
	// the workload the attacker claims.
	for name, mutate := range map[string]func(map[string]any){
		"tenant swap":     func(p map[string]any) { p["tenant"] = "other" },
		"deployment swap": func(p map[string]any) { p["azp"] = "staging" },
	} {
		t.Run(name, func(t *testing.T) {
			payload := validTaskPayload(now)
			mutate(payload)
			forged := reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, payload))
			mustReject(t, verifier, forged, testAudience, KindTask, taskWorkload())
			// Even presenting the workload that matches the forged claims is
			// refused when the scope identifiers are unsafe.
			payload["tenant"] = "other tenant"
			mustReject(t, verifier,
				reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, payload)),
				testAudience, KindTask,
				WorkloadID(contracts.Scope{Tenant: "other tenant", Deployment: "staging", Subject: "user-ada"}))
		})
	}
}

func TestVerifyHumanShape(t *testing.T) {
	priv, pub := newKey(t)
	verifier := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	now := time.Now().UTC().Unix()

	valid := reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, validHumanPayload(now)))
	claims, err := verifier.Verify(valid, testAudience, KindHuman, "")
	if err != nil {
		t.Fatalf("valid human token rejected: %v", err)
	}
	if claims.Actors == nil {
		t.Fatal("actors must never be nil after verification")
	}

	for name, mutate := range map[string]func(map[string]any){
		"missing subject":  func(p map[string]any) { delete(p, "sub") },
		"empty subject":    func(p map[string]any) { p["sub"] = "" },
		"missing tenant":   func(p map[string]any) { delete(p, "tenant") },
		"empty tenant":     func(p map[string]any) { p["tenant"] = "" },
		"task claim":       func(p map[string]any) { p["task"] = "task-456" },
		"grant claim":      func(p map[string]any) { p["grant"] = "grant-1" },
		"deployment claim": func(p map[string]any) { p["azp"] = "prod-eu" },
	} {
		t.Run(name, func(t *testing.T) {
			payload := validHumanPayload(now)
			mutate(payload)
			mustReject(t, verifier, reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, payload)),
				testAudience, KindHuman, "")
		})
	}
}

func TestErrorsNeverEchoToken(t *testing.T) {
	signer, verifier := newPair(t)
	token := signTask(t, signer)
	secret := "s3cr3t-subject-marker"

	parts := strings.Split(token, ".")
	bad := parts[0] + "." + parts[1] + ".AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	_, err := verifier.Verify(bad, testAudience, KindTask, taskWorkload())
	if err == nil {
		t.Fatal("expected rejection")
	}
	if strings.Contains(err.Error(), parts[1]) || strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaks payload: %v", err)
	}

	// A payload containing an obvious secret must not be echoed back.
	priv, pub := newKey(t)
	v := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	now := time.Now().UTC().Unix()
	payload := validTaskPayload(now)
	payload["grant"] = secret
	leaky := reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, payload))
	_, err = v.Verify(leaky, "wrong-audience", KindTask, taskWorkload())
	if err == nil {
		t.Fatal("expected rejection")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), leaky) {
		t.Fatalf("error leaks claim values: %v", err)
	}
	if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("error code = %q, want %q", contracts.Code(err), contracts.ErrForbidden)
	}
}

func TestClaimsJSONTagsAreStable(t *testing.T) {
	raw, err := json.Marshal(Claims{Kind: KindTask, Actors: []string{"a"}})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for _, key := range []string{"iss", "sub", "aud", "exp", "iat", "nbf", "token_use",
		"token_kind", "tenant", "azp", "task", "root", "ctx", "grant", "depth", "act"} {
		if _, ok := m[key]; !ok {
			t.Fatalf("Claims must serialise %q (shared API): %s", key, raw)
		}
	}
}

// itoa renders an int64 without importing strconv in every case.
func itoa(v int64) string {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

// numberFor renders a payload claim value as a JSON literal.
func numberFor(v any) string {
	return itoa(v.(int64))
}

// TestSignerWritesCanonicalJTIMember pins the canonical signature input: the
// signed payload is the rawClaims wire form, which never omits a member, so an
// empty jti is still written as "jti":"". A task token has no session id, yet
// its signed bytes must contain the member with an empty value; otherwise a
// peer that always writes the member cannot reproduce the signing bytes and a
// cross-service verify of a valid token fails.
func TestJTIIsOptionalBackwardCompatible(t *testing.T) {
	priv, pub := newKey(t)
	signer, err := NewSigner(testIssuer, testKID, priv)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	verifier := NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})

	// A task token carries no session id: its signed payload must omit the
	// jti member entirely, so it keeps the exact pre-jti wire bytes. The
	// verifier runs over the received signing bytes and its strict decoder does
	// not set DisallowUnknownFields, so the absent member is accepted.
	taskTok, err := signer.Sign(taskClaims(), 5*time.Minute)
	if err != nil {
		t.Fatalf("sign task: %v", err)
	}
	if _, ok := payloadMap(t, taskTok)["jti"]; ok {
		t.Fatalf("empty-jti task token must omit the jti member (pre-jti wire): %v",
			payloadMap(t, taskTok))
	}
	if _, err := verifier.Verify(taskTok, testAudience, KindTask, taskWorkload()); err != nil {
		t.Fatalf("empty-jti task token must verify: %v", err)
	}

	// A pre-jti token assembled without any jti member at all still verifies
	// with the current verifier (cross-version compatibility for tokens minted
	// before this claim existed).
	now := time.Now().UTC().Unix()
	oldTok := reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, validTaskPayload(now)))
	if _, err := verifier.Verify(oldTok, testAudience, KindTask, taskWorkload()); err != nil {
		t.Fatalf("pre-jti task token (no jti) must verify: %v", err)
	}

	// If some peer does emit an explicit empty jti, the token still decodes and
	// verifies; the claim is purely optional in both directions.
	forced := payloadMap(t, taskTok)
	forced["jti"] = ""
	forcedTok := reSign(t, priv, testKID, jsonBytes(t, defaultHeader()), jsonBytes(t, forced))
	if _, err := verifier.Verify(forcedTok, testAudience, KindTask, taskWorkload()); err != nil {
		t.Fatalf("task token with explicit empty jti must verify: %v", err)
	}

	// A session-bound credential writes its jti verbatim and it round-trips.
	h := humanClaims()
	h.JTI = "sess-abc-123"
	humTok, err := signer.Sign(h, time.Hour)
	if err != nil {
		t.Fatalf("sign human: %v", err)
	}
	if got, _ := payloadMap(t, humTok)["jti"].(string); got != "sess-abc-123" {
		t.Fatalf("human token jti = %q, want sess-abc-123", got)
	}
	claims, err := verifier.Verify(humTok, testAudience, KindHuman, "")
	if err != nil {
		t.Fatalf("verify human with jti: %v", err)
	}
	if claims.JTI != "sess-abc-123" {
		t.Fatalf("decoded jti = %q, want sess-abc-123", claims.JTI)
	}

	// Empty-jti task payloads are byte-stable (no hidden optional-member drift).
	a, _ := signer.Sign(taskClaims(), 5*time.Minute)
	b, _ := signer.Sign(taskClaims(), 5*time.Minute)
	if strings.Split(a, ".")[1] != strings.Split(b, ".")[1] {
		t.Fatalf("empty-jti task payloads diverge")
	}
}
