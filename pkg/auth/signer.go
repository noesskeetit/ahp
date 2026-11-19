package auth

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// signatureLen is the byte length of an Ed25519 signature.
const signatureLen = ed25519.SignatureSize

// maxTaskTTL bounds the lifetime of a delegated task token. Human tokens are
// bounded by maxHumanTTL so no signer can mint an effectively immortal
// credential.
const (
	maxTaskTTL  = 15 * time.Minute
	maxHumanTTL = 24 * time.Hour
)

// Signer mints AHP access tokens with a single Ed25519 key.
type Signer struct {
	issuer string
	kid    string
	key    ed25519.PrivateKey
}

// NewSigner returns a signer that labels tokens with issuer and advertises kid
// as its key identifier. The key material is copied defensively, so later
// mutation of the caller slice cannot change the signing key.
func NewSigner(issuer, kid string, key ed25519.PrivateKey) (*Signer, error) {
	if strings.TrimSpace(issuer) == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "auth: issuer must not be empty")
	}
	if strings.TrimSpace(kid) == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "auth: key id must not be empty")
	}
	if len(key) != ed25519.PrivateKeySize {
		return nil, contracts.NewError(contracts.ErrInvalid, "auth: signing key must be an Ed25519 private key")
	}
	pk, ok := key.Public().(ed25519.PublicKey)
	if !ok || len(pk) != ed25519.PublicKeySize {
		return nil, contracts.NewError(contracts.ErrInvalid, "auth: signing key has no Ed25519 public half")
	}
	clone := make(ed25519.PrivateKey, ed25519.PrivateKeySize)
	copy(clone, key)
	return &Signer{issuer: issuer, kid: kid, key: clone}, nil
}

// KeyID returns the key identifier this signer advertises in token headers.
func (s *Signer) KeyID() string { return s.kid }

// Public returns a copy of the verification key matching this signer.
func (s *Signer) Public() ed25519.PublicKey {
	pk := s.key.Public().(ed25519.PublicKey)
	out := make(ed25519.PublicKey, ed25519.PublicKeySize)
	copy(out, pk)
	return out
}

// Sign serialises claims, stamps issuer, token_use and the time window, and
// returns the compact EdDSA token. The TTL is measured from now, must be
// positive and must respect the bound for the token kind.
func (s *Signer) Sign(claims Claims, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		return "", contracts.NewError(contracts.ErrInvalid, "auth: token TTL must be positive")
	}
	switch claims.Kind {
	case KindTask:
		if ttl > maxTaskTTL {
			return "", contracts.NewError(contracts.ErrInvalid,
				"auth: task token TTL exceeds the 15 minute maximum")
		}
	case KindHuman:
		if ttl > maxHumanTTL {
			return "", contracts.NewError(contracts.ErrInvalid,
				"auth: human token TTL exceeds the 24 hour maximum")
		}
	default:
		return "", contracts.NewError(contracts.ErrInvalid,
			"auth: token kind must be one of human or task")
	}
	if err := validateClaimsForKind(claims); err != nil {
		return "", err
	}

	now := time.Now().UTC().Truncate(time.Second)
	claims.Issuer = s.issuer
	claims.TokenUse = TokenUse
	claims.IssuedAt = now.Unix()
	claims.NotBefore = now.Unix()
	claims.ExpiresAt = now.Add(ttl).Unix()
	if claims.Actors == nil {
		claims.Actors = []string{}
	}

	headerSeg, err := encodeSegment(header{Type: "JWT", Algorithm: "EdDSA", KeyID: s.kid})
	if err != nil {
		return "", err
	}
	// jti is an OPTIONAL wire member: the signed payload omits it when the
	// claim is empty, so a task token or a control-plane human token that
	// carries no session id keeps the exact bytes it had before jti existed
	// (verification already runs over the received signing bytes, so there is
	// no reason to widen the wire). A credential that needs per-session
	// revocation sets jti and writes it verbatim.
	payloadSeg, err := encodeSegment(claims)
	if err != nil {
		return "", err
	}
	signingInput := append(append(append([]byte{}, headerSeg...), '.'), payloadSeg...)
	if len(signingInput)+1+encodedLen(signatureLen) > maxTokenBytes {
		return "", contracts.NewError(contracts.ErrInvalid,
			"auth: token would exceed the 16 KiB limit")
	}
	sig := ed25519.Sign(s.key, signingInput)
	return string(signingInput) + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// encodedLen returns the unpadded base64url length of n raw bytes.
func encodedLen(n int) int { return (n*8 + 5) / 6 }

// validateClaimsForKind enforces the shape each token kind must have before it
// is signed, so a signer cannot mint a token the verifier would (rightly)
// refuse.
func validateClaimsForKind(c Claims) error {
	if c.Audience == "" {
		return contracts.NewError(contracts.ErrInvalid, "auth: audience must not be empty")
	}
	if len(c.JTI) > 128 {
		return contracts.NewError(contracts.ErrInvalid, "auth: jti exceeds 128 bytes")
	}
	if c.Tenant == "" {
		return contracts.NewError(contracts.ErrInvalid, "auth: tenant must not be empty")
	}
	switch c.Kind {
	case KindHuman:
		if c.Subject == "" {
			return contracts.NewError(contracts.ErrInvalid, "auth: human token requires a subject")
		}
		if c.Task != "" || c.Grant != "" || c.Deployment != "" {
			return contracts.NewError(contracts.ErrForbidden,
				"auth: human tokens must not carry task, grant or deployment claims")
		}
		if c.CtxScope != "" {
			return contracts.NewError(contracts.ErrForbidden,
				"auth: human tokens must not carry a ctx_scope claim")
		}
		return nil
	case KindTask:
		if c.Task == "" || c.Root == "" || c.Context == "" || c.Grant == "" {
			return contracts.NewError(contracts.ErrInvalid,
				"auth: task tokens require task, root, context and grant claims")
		}
		if c.Depth < 0 {
			return contracts.NewError(contracts.ErrInvalid, "auth: task token depth must not be negative")
		}
		if c.Subject == "" {
			return contracts.NewError(contracts.ErrInvalid, "auth: task token requires a subject")
		}
		if err := c.Scope().Validate(); err != nil {
			return contracts.NewError(contracts.ErrInvalid, "auth: task token scope is invalid: "+err.Error())
		}
		if c.CtxScope != "" && c.CtxScope != CtxScopeAuthority {
			return contracts.NewError(contracts.ErrInvalid,
				"auth: task token ctx_scope must be empty or authority")
		}
		return nil
	}
	return contracts.NewError(contracts.ErrInvalid, "auth: token kind must be one of human or task")
}
