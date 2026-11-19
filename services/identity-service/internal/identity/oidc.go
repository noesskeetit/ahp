package identity

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"hash"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// OIDC authenticator: the real seam a corporate identity provider plugs into.
//
// It performs genuine cryptographic verification of an assertion issued by a
// real OIDC issuer — OIDC discovery, JWKS retrieval, signature verification
// (RSA/ECDSA/Ed25519) and claim validation — using only the Go standard
// library, so it adds no dependency to go.mod. Nothing about this code is a
// stub; what makes it non-production is only that no real corporate issuer is
// wired to it in this environment, so the live external trust it WOULD extend
// is unverified until a real environment exists. That must be stated, not
// implied, and this must never be presented as "the IdP requirement is done
// because a local fixture verified" — the fixture proves the adapter path is
// real, not that any real IdP was contacted.
//
// Fail-closed rule: discovery, JWKS, signature, claims and (when configured)
// the IAM active check are all mandatory. Any error denies authentication.

const (
	oidcMaxDiscoveryBody = 64 * 1024
	oidcMaxJWSKBody      = 256 * 1024
	oidcMaxAssertion     = 32 * 1024
	oidcDefaultTimeout   = 8 * time.Second
	oidcJWKSCacheTTL     = 5 * time.Minute
	oidcLeeway           = 30 * time.Second
)

// OIDCConfig configures a real OIDC authenticator.
type OIDCConfig struct {
	// Synthetic labels the operator-selected PoC issuer; cryptographic checks remain identical.
	Synthetic bool
	// Issuer must equal the verified token's iss claim.
	Issuer string
	// DiscoveryURL is the issuer's OIDC metadata document; its jwks_uri is
	// fetched to verify assertions. Required.
	DiscoveryURL string
	// ClientID is the required audience of verified assertions. Required.
	ClientID string
	// TenantClaim names the verified claim that carries the AHP tenant. It is
	// required on every assertion: an identity without a tenant cannot be
	// isolated, so a missing tenant claim fails closed. Defaults to "tenant".
	TenantClaim string
	// SubjectClaim overrides the claim read as the subject; defaults to "sub".
	SubjectClaim string
	// IAMActiveURL, when set, is queried before a session is opened. The
	// response must be a JSON object with an "active" boolean; false, or any
	// transport/decoding error, denies the login. This is the corporate IAM
	// account-active hook, not a resource Evaluate/ExtAuthz decision. Empty disables the external check (only the
	// signature and claims gate the login), which is allowed for an issuer the
	// operator has not wired IAM to yet.
	IAMActiveURL string
	// HTTPClient is used for discovery, JWKS and IAM. nil selects a client with
	// a default timeout. Tests inject a client that trusts a fixture CA, which
	// is what forces real TLS verification.
	HTTPClient *http.Client
	// Now is injectable for deterministic time tests; nil selects time.Now.
	Now func() time.Time
}

type oidcAuthenticator struct {
	cfg    OIDCConfig
	client *http.Client
	now    func() time.Time

	mu            sync.Mutex
	keys          map[string]crypto.PublicKey // by kid
	keysFetchedAt time.Time
}

// NewOIDCAuthenticator validates the configuration and returns a real OIDC
// authenticator. It fails fast on a missing issuer/JWKS source/audience so a
// misconfigured deployment cannot silently drop authentication.
func NewOIDCAuthenticator(cfg OIDCConfig) (Authenticator, error) {
	issuer, err := url.Parse(cfg.Issuer)
	discovery, discoveryErr := url.Parse(cfg.DiscoveryURL)
	if err != nil || discoveryErr != nil || !oidcHTTPSURL(issuer) || !oidcHTTPSURL(discovery) || issuer.Scheme != discovery.Scheme || issuer.Host != discovery.Host {
		return nil, invalid("identity: OIDC issuer and discovery must use the same explicit HTTPS origin")
	}

	if strings.TrimSpace(cfg.Issuer) == "" {
		return nil, invalid("identity: OIDC issuer is required")
	}
	if strings.TrimSpace(cfg.DiscoveryURL) == "" {
		return nil, invalid("identity: OIDC discovery URL is required")
	}
	if strings.TrimSpace(cfg.ClientID) == "" {
		return nil, invalid("identity: OIDC client id (audience) is required")
	}
	if cfg.TenantClaim == "" {
		cfg.TenantClaim = "tenant"
	}
	if cfg.SubjectClaim == "" {
		cfg.SubjectClaim = "sub"
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: oidcDefaultTimeout}
	}
	clientCopy := *client
	client = &clientCopy
	if client.Timeout <= 0 || client.Timeout > oidcDefaultTimeout {
		client.Timeout = oidcDefaultTimeout
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &oidcAuthenticator{cfg: cfg, client: client, now: now}, nil
}

// Profile reports the OIDC profile.
func (a *oidcAuthenticator) Profile() string {
	if a.cfg.Synthetic {
		return "synthetic-oidc"
	}
	return "oidc"
}

// Login verifies an IdP-issued assertion and returns the authoritative account
// the IdP vouches for. The assertion is the only proof accepted; there is no
// username/password path and no way to name a subject.
func (a *oidcAuthenticator) Login(ctx context.Context, in LoginInput) (BrowserUser, error) {
	if in.Username != "" || in.Password != "" {
		return BrowserUser{}, invalid("identity: the OIDC profile authenticates by assertion, not credential")
	}
	if len(in.Assertion) == 0 || len(in.Assertion) > oidcMaxAssertion {
		return BrowserUser{}, ErrAuthentication
	}
	claims, err := a.verifyAssertion(ctx, in.Assertion)
	if err != nil {
		// Signature/claim failures must be indistinguishable to the caller
		// from a valid-but-unknown identity: one error, no detail leak.
		return BrowserUser{}, ErrAuthentication
	}
	sub := strings.TrimSpace(claimString(claims, a.cfg.SubjectClaim))
	tenant := strings.TrimSpace(claimString(claims, a.cfg.TenantClaim))
	if sub == "" || tenant == "" {
		// A subject with no tenant cannot be isolated; fail closed rather than
		// minting a scope-less identity.
		return BrowserUser{}, ErrAuthentication
	}
	if a.cfg.IAMActiveURL != "" {
		active, err := a.iamActive(ctx, in.Assertion, sub)
		if err != nil || !active {
			// The IAM is the revocation authority; unreachable or "not active"
			// both deny. This is fail-closed by design.
			return BrowserUser{}, ErrAuthentication
		}
	}
	// Provision/refresh the local account row from the verified assertion. The
	// subject and tenant are the IdP's, never the caller's; this only mirrors
	// them so the session and revocation machinery (which keys off
	// tenant+subject) applies to federated humans exactly as to local ones.
	//
	// The store handle for provisioning is injected by the caller through
	// Provisioning; a bare authenticator cannot touch the database, which is
	// why Login here returns the identity and identityapi performs the mirror.
	return BrowserUser{
		Subject:     sub,
		Tenant:      tenant,
		DisplayName: strings.TrimSpace(claimString(claims, "name")),
		Email:       strings.TrimSpace(claimString(claims, "email")),
	}, nil
}

// VerifiedSubjects exposes whether an assertion verifies, for callers that
// want to mirror a federated identity without re-exposing verification.
func (a *oidcAuthenticator) VerifyAssertion(ctx context.Context, assertion string) (BrowserUser, error) {
	return a.Login(ctx, LoginInput{Assertion: assertion})
}

// --- discovery / JWKS ----------------------------------------------------

type oidcDiscovery struct {
	JWKSURI string `json:"jwks_uri"`
	Issuer  string `json:"issuer"`
}

func (a *oidcAuthenticator) jwksURI(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.cfg.DiscoveryURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("identity: oidc discovery status %d", resp.StatusCode)
	}
	var doc oidcDiscovery
	if err := oidcDecode(resp.Body, oidcMaxDiscoveryBody, &doc); err != nil {
		return "", err
	}
	if strings.TrimSpace(doc.JWKSURI) == "" {
		return "", fmt.Errorf("identity: oidc discovery missing jwks_uri")
	}
	if doc.Issuer != "" && doc.Issuer != a.cfg.Issuer {
		return "", fmt.Errorf("identity: oidc discovery issuer mismatch")
	}
	uri, err := url.Parse(doc.JWKSURI)
	discovery, _ := url.Parse(a.cfg.DiscoveryURL)
	if err != nil || !oidcHTTPSURL(uri) || uri.Host != discovery.Host || uri.Scheme != discovery.Scheme {
		return "", fmt.Errorf("identity: JWKS URI is outside configured origin")
	}
	return doc.JWKSURI, nil
}

func (a *oidcAuthenticator) keyFor(ctx context.Context, kid string) (crypto.PublicKey, error) {
	a.mu.Lock()
	if time.Since(a.keysFetchedAt) < oidcJWKSCacheTTL && len(a.keys) > 0 {
		k := a.keys[kid]
		a.mu.Unlock()
		if k == nil {
			return nil, fmt.Errorf("identity: unknown kid")
		}
		return k, nil
	}
	a.mu.Unlock()

	uri, err := a.jwksURI(ctx)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("identity: oidc jwks status %d", resp.StatusCode)
	}
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := oidcDecode(resp.Body, oidcMaxJWSKBody, &set); err != nil {
		return nil, err
	}
	keys := make(map[string]crypto.PublicKey, len(set.Keys))
	for _, k := range set.Keys {
		if k.Kid == "" || (k.Use != "" && k.Use != "sig") {
			continue
		}
		if _, duplicate := keys[k.Kid]; duplicate {
			return nil, fmt.Errorf("identity: duplicate JWKS key identifier")
		}
		pub, err := k.publicKey()
		if err != nil {
			continue // skip a malformed key rather than abort the whole set
		}
		keys[k.Kid] = pub
	}
	a.mu.Lock()
	a.keys = keys
	a.keysFetchedAt = a.now()
	a.mu.Unlock()
	if keys[kid] == nil {
		return nil, fmt.Errorf("identity: unknown kid")
	}
	return keys[kid], nil
}

// --- signature + claims --------------------------------------------------

func (a *oidcAuthenticator) verifyAssertion(ctx context.Context, assertion string) (map[string]any, error) {
	parts := strings.Split(assertion, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("identity: malformed assertion")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("identity: malformed assertion header")
	}
	if _, err := contracts.Digest(json.RawMessage(headerBytes)); err != nil {
		return nil, fmt.Errorf("identity: invalid assertion header")
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, fmt.Errorf("identity: malformed assertion header json")
	}
	if header.Typ != "" && !strings.EqualFold(header.Typ, "JWT") && !strings.EqualFold(header.Typ, "at+jwt") {
		return nil, fmt.Errorf("identity: unexpected assertion typ")
	}
	signing := parts[0] + "." + parts[1]
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("identity: malformed assertion payload")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("identity: malformed assertion signature")
	}
	pub, err := a.keyFor(ctx, header.Kid)
	if err != nil {
		return nil, err
	}
	if err := verifySignature(header.Alg, pub, signing, sig); err != nil {
		return nil, err
	}
	if _, err := contracts.Digest(json.RawMessage(payloadBytes)); err != nil {
		return nil, fmt.Errorf("identity: invalid assertion claims")
	}
	var claims map[string]any
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, fmt.Errorf("identity: malformed assertion claims")
	}
	if err := a.validateClaims(claims); err != nil {
		return nil, err
	}
	return claims, nil
}

func (a *oidcAuthenticator) validateClaims(claims map[string]any) error {
	if claimString(claims, "token_use") != "access" {
		return fmt.Errorf("identity: public assertion must be an access token")
	}
	if kind := claimString(claims, "token_kind"); kind != "" && kind != "human" {
		return fmt.Errorf("identity: public assertion must identify a human")
	}
	for _, key := range []string{"task", "grant", "root", "ctx"} {
		if value, ok := claims[key]; ok && value != nil && value != "" {
			return fmt.Errorf("identity: public assertion carries task authority")
		}
	}

	if iss := claimString(claims, "iss"); iss != a.cfg.Issuer {
		return fmt.Errorf("identity: assertion issuer mismatch")
	}
	if !audienceIncludes(claims["aud"], a.cfg.ClientID) {
		return fmt.Errorf("identity: assertion audience mismatch")
	}
	now := a.now()
	if exp, ok := claimNumber(claims, "exp"); ok && now.After(time.Unix(int64(exp), 0).Add(oidcLeeway)) {
		return fmt.Errorf("identity: assertion expired")
	} else if !ok {
		return fmt.Errorf("identity: assertion missing exp")
	}
	if nbf, ok := claimNumber(claims, "nbf"); ok && now.Add(oidcLeeway).Before(time.Unix(int64(nbf), 0)) {
		return fmt.Errorf("identity: assertion not yet valid")
	}
	return nil
}

func (a *oidcAuthenticator) iamActive(ctx context.Context, assertion, subject string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.IAMActiveURL, strings.NewReader("token="+base64.RawURLEncoding.EncodeToString([]byte(assertion))))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := a.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("identity: iam active status %d", resp.StatusCode)
	}
	var out struct {
		Active bool `json:"active"`
	}
	if err := oidcDecode(resp.Body, oidcMaxDiscoveryBody, &out); err != nil {
		return false, err
	}
	return out.Active, nil
}

// --- crypto helpers (stdlib only) ----------------------------------------

func verifySignature(alg string, pub crypto.PublicKey, signing string, sig []byte) error {
	switch alg {
	case "RS256":
		key, ok := pub.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("identity: alg/key mismatch")
		}
		sum := sha256.Sum256([]byte(signing))
		return rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig)
	case "RS384":
		key, ok := pub.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("identity: alg/key mismatch")
		}
		sum := sha512.Sum384([]byte(signing))
		return rsa.VerifyPKCS1v15(key, crypto.SHA384, sum[:], sig)
	case "ES256":
		return verifyECDSA(elliptic.P256(), crypto.SHA256, pub, signing, sig)
	case "ES384":
		return verifyECDSA(elliptic.P384(), crypto.SHA384, pub, signing, sig)
	case "EdDSA":
		key, ok := pub.(ed25519.PublicKey)
		if !ok {
			return fmt.Errorf("identity: alg/key mismatch")
		}
		if !ed25519.Verify(key, []byte(signing), sig) {
			return fmt.Errorf("identity: bad eddsa signature")
		}
		return nil
	default:
		// none, HS*, PS*, unknown: never accepted. No shared secret exists here
		// and algorithm downgrade must be impossible.
		return fmt.Errorf("identity: unsupported alg")
	}
}

func verifyECDSA(crv elliptic.Curve, h crypto.Hash, pub crypto.PublicKey, signing string, sig []byte) error {
	key, ok := pub.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("identity: alg/key mismatch")
	}
	if key.Curve != crv {
		return fmt.Errorf("identity: curve mismatch")
	}
	// JWS ECDSA signatures are fixed-width r||s, not ASN.1. Parsing the exact
	// width here blocks the ASN.1 encoding-confusion class of bugs.
	if len(sig) != 2*crv.Params().BitSize/8 {
		return fmt.Errorf("identity: bad ecdsa signature length")
	}
	half := len(sig) / 2
	r := new(big.Int).SetBytes(sig[:half])
	s := new(big.Int).SetBytes(sig[half:])
	var hv hash.Hash
	switch h {
	case crypto.SHA256:
		hv = sha256.New()
	case crypto.SHA384:
		hv = sha512.New384()
	default:
		return fmt.Errorf("identity: unsupported hash")
	}
	hv.Write([]byte(signing))
	digest := hv.Sum(nil)
	if !ecdsa.Verify(key, digest, r, s) {
		return fmt.Errorf("identity: bad ecdsa signature")
	}
	return nil
}

// jwk is a JSON Web Key limited to the asymmetric key types the platform may
// verify against.
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	// RSA
	N string `json:"n"`
	E string `json:"e"`
	// EC
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
	// OKP reuses X as the ed25519 public key
}

func (k jwk) publicKey() (crypto.PublicKey, error) {
	switch k.Kty {
	case "RSA":
		n, err := base64.RawURLEncoding.DecodeString(k.N)
		if err != nil {
			return nil, err
		}
		e, err := base64.RawURLEncoding.DecodeString(k.E)
		if err != nil {
			return nil, err
		}
		return &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}, nil
	case "EC":
		var curve elliptic.Curve
		switch k.Crv {
		case "P-256":
			curve = elliptic.P256()
		case "P-384":
			curve = elliptic.P384()
		default:
			return nil, fmt.Errorf("identity: unsupported ec curve")
		}
		x, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil {
			return nil, err
		}
		y, err := base64.RawURLEncoding.DecodeString(k.Y)
		if err != nil {
			return nil, err
		}
		return &ecdsa.PublicKey{Curve: curve, X: new(big.Int).SetBytes(x), Y: new(big.Int).SetBytes(y)}, nil
	case "OKP":
		if k.Crv != "Ed25519" {
			return nil, fmt.Errorf("identity: unsupported okp curve")
		}
		x, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil {
			return nil, err
		}
		if len(x) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("identity: bad ed25519 key size")
		}
		return ed25519.PublicKey(x), nil
	default:
		return nil, fmt.Errorf("identity: unsupported kty")
	}
}

// --- claim helpers -------------------------------------------------------

func claimString(claims map[string]any, key string) string {
	if v, ok := claims[key].(string); ok {
		return v
	}
	return ""
}

func claimNumber(claims map[string]any, key string) (float64, bool) {
	switch v := claims[key].(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func audienceIncludes(aud any, want string) bool {
	switch v := aud.(type) {
	case string:
		return subtle.ConstantTimeCompare([]byte(v), []byte(want)) == 1
	case []any:
		for _, e := range v {
			if s, ok := e.(string); ok && s == want {
				return true
			}
		}
	}
	return false
}

var _ Authenticator = (*oidcAuthenticator)(nil)

func oidcHTTPSURL(u *url.URL) bool {
	return u != nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.Fragment == "" && u.RawQuery == ""
}
func oidcDecode(r io.Reader, max int64, out any) error {
	data, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > max {
		return fmt.Errorf("identity: OIDC response too large")
	}
	if _, err := contracts.Digest(json.RawMessage(data)); err != nil {
		return fmt.Errorf("identity: invalid OIDC JSON")
	}
	return json.Unmarshal(data, out)
}
