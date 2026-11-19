package space

import (
	"crypto/ed25519"
	"net/http"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// DefaultAudience is the AHP access-token audience the Space service expects
// when verifying the tokens a browser presents at the human-facing edge.
const DefaultAudience = "ahp"

// HumanAuthConfig configures verification of AHP human access tokens for the
// public Space browser API. Space is the human-facing edge: it verifies a
// compact Ed25519 (EdDSA) JWT presented as "Authorization: Bearer <jwt>" and
// projects its verified claims onto the platform isolation scope. The identity
// service issues these tokens; Space only verifies them with the published
// public keys, so no signing key is ever held here.
type HumanAuthConfig struct {
	// Issuer is the expected "iss" claim (the identity service issuer ID).
	Issuer string
	// Audience is the expected "aud" claim. Defaults to DefaultAudience.
	Audience string
	// Keys maps trusted token key identifiers ("kid") to Ed25519 public keys.
	Keys map[string]ed25519.PublicKey
}

// normalize validates the configuration and applies the default audience.
func (c HumanAuthConfig) normalize() (HumanAuthConfig, error) {
	if c.Issuer == "" {
		return c, contracts.NewError(contracts.ErrInvalid, "space: human auth requires an issuer")
	}
	if len(c.Keys) == 0 {
		return c, contracts.NewError(contracts.ErrInvalid, "space: human auth requires at least one trusted key")
	}
	for kid, k := range c.Keys {
		if kid == "" || len(k) != ed25519.PublicKeySize {
			return c, contracts.NewError(contracts.ErrInvalid, "space: human auth key map has an empty kid or a non-Ed25519 key")
		}
	}
	if c.Audience == "" {
		c.Audience = DefaultAudience
	}
	return c, nil
}

// HumanAuthMiddleware builds the Auth middleware for HandlerConfig.Auth. It
// verifies a human access token and installs a transport.Principal carrying the
// verified scope for downstream handlers. Missing, malformed, expired, or
// untrusted tokens are refused with a forbidden error and no token content is
// echoed. A task token presented here is also refused: the browser edge speaks
// human tokens only, so a caller cannot escalate into a delegated task scope
// through the public Space API.
func HumanAuthMiddleware(cfg HumanAuthConfig) (func(http.Handler) http.Handler, error) {
	cfg, err := cfg.normalize()
	if err != nil {
		return nil, err
	}
	verifier := auth.NewVerifier(cfg.Issuer, cfg.Keys)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := bearerToken(r)
			if token == "" {
				transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "space: missing bearer token"))
				return
			}
			claims, err := verifier.Verify(token, cfg.Audience, auth.KindHuman, "")
			if err != nil {
				// Verifier errors never contain the token; map to a stable,
				// log-safe forbidden outcome so nothing about the caller's
				// credential leaks back through the response.
				transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "space: unverified principal"))
				return
			}
			p := transport.Principal{Claims: claims, Scope: claims.Scope()}
			ctx := transport.WithPrincipal(r.Context(), p)
			// The authoritative pkg/auth carries the canonical browser session id (jti), and
			// exchange.go documents that the auth layer must inject it. It roots the Human->task
			// authority per (subject, session): without it every login of one subject shares ONE
			// grant root, so a later session that holds more permissions is refused by the older,
			// narrower ceiling ("delegation may only narrow the parent capability set") instead of
			// rooting a fresh authority - and revoking one session would brick unrelated sessions.
			if claims.JTI != "" {
				ctx = WithSession(ctx, claims.JTI)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}, nil
}
