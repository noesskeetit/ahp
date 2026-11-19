package auth

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ExchangeAssertion is a one-minute delegation proof for a trusted token
// endpoint, not an AHP access token. Target fields come from server registration.
type ExchangeAssertion struct {
	Issuer         string `json:"iss"`
	Audience       string `json:"aud"`
	TargetAudience string `json:"target_audience"`
	Subject        string `json:"sub"`
	Tenant         string `json:"tenant"`
	Deployment     string `json:"azp"`
	Task           string `json:"task"`
	Grant          string `json:"grant"`
	GrantEpoch     int64  `json:"grant_epoch"`
	Connection     string `json:"connection"`
	Resource       string `json:"resource"`
	Scope          string `json:"scope"`
	Actor          string `json:"act"`
	TokenUse       string `json:"token_use"`
	JTI            string `json:"jti"`
	IssuedAt       int64  `json:"iat"`
	ExpiresAt      int64  `json:"exp"`
}

// VerifyExchangeAssertion verifies only the dedicated delegation format. It
// never accepts human/task JWTs, even when those use a trusted signing key.
func VerifyExchangeAssertion(token, issuer, audience string, keys map[string]ed25519.PublicKey) (ExchangeAssertion, error) {
	deny := func() (ExchangeAssertion, error) {
		return ExchangeAssertion{}, contracts.NewError(contracts.ErrForbidden, "auth: invalid exchange assertion")
	}
	if len(token) > maxTokenBytes {
		return deny()
	}
	p := strings.Split(token, ".")
	if len(p) != 3 {
		return deny()
	}
	hb, e := base64.RawURLEncoding.DecodeString(p[0])
	if e != nil {
		return deny()
	}
	pb, e := base64.RawURLEncoding.DecodeString(p[1])
	if e != nil {
		return deny()
	}
	sig, e := base64.RawURLEncoding.DecodeString(p[2])
	if e != nil {
		return deny()
	}
	if _, e = contracts.Digest(json.RawMessage(hb)); e != nil {
		return deny()
	}
	if _, e = contracts.Digest(json.RawMessage(pb)); e != nil {
		return deny()
	}
	var h header
	d := json.NewDecoder(bytes.NewReader(hb))
	d.DisallowUnknownFields()
	if d.Decode(&h) != nil || h.Algorithm != "EdDSA" || h.Type != "ahp-exchange+jwt" {
		return deny()
	}
	k := keys[h.KeyID]
	if len(k) != ed25519.PublicKeySize || !ed25519.Verify(k, []byte(p[0]+"."+p[1]), sig) {
		return deny()
	}
	var c ExchangeAssertion
	d = json.NewDecoder(bytes.NewReader(pb))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil {
		return deny()
	}
	now := time.Now().Unix()
	if c.Issuer != issuer || c.Audience != audience || c.TokenUse != "exchange" || c.Actor != "ahp:tool-gateway" || c.IssuedAt > now+5 || c.ExpiresAt <= now || c.ExpiresAt-c.IssuedAt != 60 || c.JTI == "" || c.GrantEpoch < 1 || c.Task == "" || c.Grant == "" || c.Connection == "" || c.Resource == "" || c.Scope == "" || c.TargetAudience == "" {
		return deny()
	}
	if (contracts.Scope{Tenant: c.Tenant, Subject: c.Subject, Deployment: c.Deployment}).Validate() != nil {
		return deny()
	}
	return c, nil
}

// SignExchangeAssertion is only used by the trusted Identity-to-ToolGW owner
// facade after current grant/resource authorization. There is no HTTP mint API.
func (s *Signer) SignExchangeAssertion(c ExchangeAssertion) (string, error) {
	if s == nil || c.Audience == "" || c.TargetAudience == "" || c.Resource == "" || c.Scope == "" || c.Connection == "" || c.Task == "" || c.Grant == "" || c.Actor != "ahp:tool-gateway" {
		return "", contracts.NewError(contracts.ErrInvalid, "auth: incomplete exchange delegation")
	}
	if err := (contracts.Scope{Tenant: c.Tenant, Subject: c.Subject, Deployment: c.Deployment}).Validate(); err != nil {
		return "", err
	}
	nonce := make([]byte, 24)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	c.Issuer, c.TokenUse = s.issuer, "exchange"
	c.JTI = base64.RawURLEncoding.EncodeToString(nonce)
	c.IssuedAt = time.Now().Unix()
	c.ExpiresAt = c.IssuedAt + 60
	h, err := encodeSegment(header{Type: "ahp-exchange+jwt", Algorithm: "EdDSA", KeyID: s.kid})
	if err != nil {
		return "", err
	}
	p, err := encodeSegment(c)
	if err != nil {
		return "", err
	}
	input := append(append(append([]byte{}, h...), '.'), p...)
	if len(input)+1+encodedLen(signatureLen) > maxTokenBytes {
		return "", contracts.NewError(contracts.ErrInvalid, "auth: exchange assertion too large")
	}
	return string(input) + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(s.key, input)), nil
}
