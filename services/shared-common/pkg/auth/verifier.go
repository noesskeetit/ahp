package auth

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Verifier validates tokens minted by a Signer whose public key it holds.
type Verifier struct {
	issuer string
	keys   map[string]ed25519.PublicKey
}

// NewVerifier returns a verifier that accepts tokens issued by issuer and
// signed by one of keys, indexed by key id. The map and every key are copied,
// so later mutation by the caller cannot add or weaken trust. A nil or empty
// key map yields a verifier that rejects everything: no default key is ever
// manufactured.
func NewVerifier(issuer string, keys map[string]ed25519.PublicKey) *Verifier {
	v := &Verifier{issuer: issuer, keys: make(map[string]ed25519.PublicKey, len(keys))}
	for kid, key := range keys {
		if kid == "" || len(key) != ed25519.PublicKeySize {
			continue
		}
		clone := make(ed25519.PublicKey, ed25519.PublicKeySize)
		copy(clone, key)
		v.keys[kid] = clone
	}
	return v
}

// Verify checks the signature, header, time window and kind-specific claim
// shape of token and returns its claims.
//
// audience and kind are compared byte for byte against the claims. workload is
// required for task tokens, where it must equal the SPIFFE identity derived
// from the verified scope. Errors never contain the token.
func (v *Verifier) Verify(token, audience, kind, workload string) (Claims, error) {
	if audience == "" {
		return Claims{}, invalidToken("expected audience must not be empty")
	}
	if kind != KindHuman && kind != KindTask {
		return Claims{}, invalidToken("expected token kind must be human or task")
	}

	headerRaw, payloadRaw, signature, err := splitToken(token)
	if err != nil {
		return Claims{}, err
	}

	var hdr header
	if err := decodeJSONStrict(headerRaw, &hdr); err != nil {
		return Claims{}, err
	}
	if hdr.Type != "JWT" {
		return Claims{}, invalidToken("token type must be JWT")
	}
	if hdr.Algorithm != "EdDSA" {
		return Claims{}, invalidToken("token algorithm must be EdDSA")
	}
	if hdr.Class != "" && hdr.Class != "JWT" {
		return Claims{}, invalidToken("token content type must be JWT")
	}
	key, ok := v.keys[hdr.KeyID]
	if !ok {
		return Claims{}, invalidToken("token key id is not trusted")
	}

	// The signature covers the exact ASCII bytes of header "." payload.
	encodedSig := base64.RawURLEncoding.EncodeToString(signature)
	signingInput := token[:len(token)-len(encodedSig)-1]
	if !ed25519.Verify(key, []byte(signingInput), signature) {
		return Claims{}, invalidToken("token signature is not valid")
	}

	var raw rawClaims
	if err := decodeJSONStrict(payloadRaw, &raw); err != nil {
		return Claims{}, err
	}
	claims, err := raw.toClaims()
	if err != nil {
		return Claims{}, err
	}
	if err := v.checkClaims(claims, audience, kind, workload); err != nil {
		return Claims{}, err
	}
	return claims, nil
}

// checkClaims applies the issuer, audience, time-window and kind rules.
func (v *Verifier) checkClaims(c Claims, audience, kind, workload string) error {
	if c.Issuer != v.issuer {
		return invalidToken("token issuer does not match")
	}
	if c.Audience != audience {
		return invalidToken("token audience does not match")
	}
	if c.TokenUse != TokenUse {
		return invalidToken("token_use must be " + TokenUse)
	}
	if c.Kind != kind {
		return invalidToken("token kind does not match")
	}
	if c.Tenant == "" {
		return invalidToken("token tenant must not be empty")
	}

	now := time.Now().UTC().Unix()
	exp, err := unixTime(c.ExpiresAt, "exp")
	if err != nil {
		return err
	}
	iat, err := unixTime(c.IssuedAt, "iat")
	if err != nil {
		return err
	}
	nbf, err := unixTime(c.NotBefore, "nbf")
	if err != nil {
		return err
	}
	if exp <= now {
		return invalidToken("token has expired")
	}
	if iat > now {
		return invalidToken("token was issued in the future")
	}
	if nbf > now {
		return invalidToken("token is not valid yet")
	}
	if exp <= iat {
		return invalidToken("token expiry must be after issuance")
	}
	if kind == KindTask && exp-iat > int64(maxTaskTTL/time.Second) {
		return invalidToken("task token lifetime exceeds the 15 minute maximum")
	}

	if kind == KindHuman {
		return checkHumanClaims(c)
	}
	return checkTaskClaims(c, workload)
}

// checkHumanClaims enforces the shape of a human token: a subject and a
// tenant, and nothing from the task delegation vocabulary.
func checkHumanClaims(c Claims) error {
	if c.Subject == "" {
		return invalidToken("human token requires a subject")
	}
	if c.Task != "" {
		return invalidToken("human token must not carry a task claim")
	}
	if c.Grant != "" {
		return invalidToken("human token must not carry a grant claim")
	}
	if c.Deployment != "" {
		return invalidToken("human token must not carry a deployment actor claim")
	}
	return nil
}

// checkTaskClaims enforces the shape of a task token and ties it
// cryptographically to the workload identity the caller asserted.
func checkTaskClaims(c Claims, workload string) error {
	if c.Task == "" {
		return invalidToken("task token requires a task claim")
	}
	if c.Root == "" {
		return invalidToken("task token requires a root claim")
	}
	if c.Context == "" {
		return invalidToken("task token requires a context claim")
	}
	if c.CtxScope != "" && c.CtxScope != CtxScopeAuthority {
		return invalidToken("task token has an unrecognized ctx_scope")
	}
	if c.Grant == "" {
		return invalidToken("task token requires a grant claim")
	}
	if c.Subject == "" {
		return invalidToken("task token requires a subject")
	}
	if c.Depth < 0 {
		return invalidToken("task token depth must not be negative")
	}
	if workload == "" {
		return invalidToken("task token verification requires a workload identity")
	}
	scope := c.Scope()
	if err := scope.Validate(); err != nil {
		var cerr *contracts.Error
		if errors.As(err, &cerr) {
			return invalidToken("task token scope is invalid: " + cerr.Message)
		}
		return invalidToken("task token scope is invalid")
	}
	if err := (contracts.Scope{Tenant: c.Tenant, Deployment: c.Deployment, Subject: c.Task}).Validate(); err != nil {
		return invalidToken("task token task claim is not a valid identifier")
	}
	if workload != WorkloadID(scope) {
		return invalidToken("token workload identity does not match the verified scope")
	}
	return nil
}

// unixTime bounds a claim timestamp so later arithmetic cannot overflow.
func unixTime(sec int64, name string) (int64, error) {
	const maxUnix = int64(1) << 40
	if sec < 0 || sec > maxUnix {
		return 0, invalidToken("token " + name + " is out of range")
	}
	return sec, nil
}

// invalidToken builds a FORBIDDEN error describing the violated rule without
// quoting the token or its claims.
func invalidToken(reason string) error {
	return contracts.NewError(contracts.ErrForbidden, "auth: invalid token: "+reason)
}

// toClaims converts a decoded payload into Claims, rejecting timestamps that
// are not integers or that exceed the representable range.
func (r rawClaims) toClaims() (Claims, error) {
	exp, err := claimTime(r.ExpiresAt, "exp")
	if err != nil {
		return Claims{}, err
	}
	iat, err := claimTime(r.IssuedAt, "iat")
	if err != nil {
		return Claims{}, err
	}
	// nbf is optional on the wire and defaults to 0; exp and iat are
	// required and claimTime rejects their absence.
	var nbf int64
	if len(r.NotBefore) > 0 {
		if nbf, err = claimTime(r.NotBefore, "nbf"); err != nil {
			return Claims{}, err
		}
	}
	actors := r.Actors
	if actors == nil {
		actors = []string{}
	}
	return Claims{
		Issuer:     r.Issuer,
		Subject:    r.Subject,
		Audience:   r.Audience,
		JTI:        r.JTI,
		ExpiresAt:  exp,
		IssuedAt:   iat,
		NotBefore:  nbf,
		TokenUse:   r.TokenUse,
		Kind:       r.Kind,
		Tenant:     r.Tenant,
		Deployment: r.Deployment,
		Task:       r.Task,
		Root:       r.Root,
		Context:    r.Context,
		CtxScope:   r.CtxScope,
		Grant:      r.Grant,
		Depth:      r.Depth,
		Actors:     actors,
	}, nil
}

// claimTime parses the raw JSON bytes of a timestamp claim as a bounded,
// non-negative Unix timestamp. The claim must be a bare JSON integer literal:
// a quoted numeric string, null, a fraction or an exponent is rejected on the
// wire bytes before any numeric conversion happens.
func claimTime(raw json.RawMessage, name string) (int64, error) {
	s := strings.TrimSpace(string(raw))
	if s == "" {
		return 0, invalidToken("token is missing " + name)
	}
	// The first byte must be a digit or '-': anything else (quote, 'n' of
	// null, '{', ...) is not a JSON number literal.
	if c := s[0]; !(c >= '0' && c <= '9' || c == '-') {
		return 0, invalidToken("token " + name + " must be an integer")
	}
	if strings.ContainsAny(s, ".eE") {
		return 0, invalidToken("token " + name + " must be an integer")
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, invalidToken("token " + name + " is out of range")
	}
	if v < 0 || v > math.MaxInt32 {
		return 0, invalidToken("token " + name + " is out of range")
	}
	return v, nil
}
