package harness

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// The State/Bootstrap plane of the harness obtains every credential through the
// product boundary, never by signing one itself.
//
// This file used to carry a tokengen-style self-sign path: it read a dev ed25519
// seed and minted KindTask tokens directly with auth.Claims. Those claims left
// Actors nil, so auth.Signer.Sign serialised them with an empty "act" claim, and
// the platform's live authority check rejected every State call: CreateThread
// answered 403 "identity: token carries an empty actor chain"
// (internal/identity/tokens.go). That path is removed rather than patched -- a
// State run must never be able to fall back to a forged grant, so no seed-reading
// or token-signing code remains here. A State session that cannot reach the live
// exchange fails closed and explicitly.
//
// The live path is Human login -> /v1/identity/control/exchange -> scoped task
// token minted by Store.MintTask, which copies the live grant's non-empty actor
// chain into the token. cmd/ahp-profile/llmauth.go implements that exchange for
// both the model and the state plane.

// TokenActorChain decodes the "act" claim of a JWT without verifying it, so the
// harness can assert -- before it spends a request -- that the credential it is
// about to present really carries the delegation chain the platform demands.
// Verification stays entirely on the product side; this reads a claim, it does
// not trust one.
func TokenActorChain(token string) ([]string, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("harness: token is not a JWS (got %d segments)", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		raw, err = base64.URLEncoding.DecodeString(parts[1])
		if err != nil {
			return nil, fmt.Errorf("harness: token payload is not base64url: %w", err)
		}
	}
	var claims struct {
		Actors []string `json:"act"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		return nil, fmt.Errorf("harness: token payload is not JSON: %w", err)
	}
	return claims.Actors, nil
}

// TokenExpiry reads the "exp" (seconds since epoch) claim of a JWT without
// verifying it, so the harness can plan a refresh against the credential the
// product actually issued rather than a guessed TTL. Returns the expiry time; a
// missing/zero exp is reported as an error so a caller never rotates on a wrong
// assumption.
func TokenExpiry(token string) (time.Time, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		return time.Time{}, fmt.Errorf("harness: token is not a JWS (got %d segments)", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		raw, err = base64.URLEncoding.DecodeString(parts[1])
		if err != nil {
			return time.Time{}, fmt.Errorf("harness: token payload is not base64url: %w", err)
		}
	}
	var claims struct {
		ExpiresAt json.Number `json:"exp"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		return time.Time{}, fmt.Errorf("harness: token payload is not JSON: %w", err)
	}
	n, err := claims.ExpiresAt.Int64()
	if err != nil || n <= 0 {
		return time.Time{}, fmt.Errorf("harness: token has no usable exp claim")
	}
	return time.Unix(n, 0).UTC(), nil
}

// RequireNonEmptyActorChain is the fail-closed guard at the harness boundary: a
// credential with an empty actor chain can only be a self-signed grant, which the
// platform answers with 403 "empty actor chain". Naming the 403 here turns a
// silent mis-mint into an explicit, typed harness error before the request.
func RequireNonEmptyActorChain(token, plane string) error {
	actors, err := TokenActorChain(token)
	if err != nil {
		return fmt.Errorf("harness: %s authorization token is unreadable: %w", plane, err)
	}
	if len(actors) == 0 {
		return fmt.Errorf("harness: %s authorization token carries an empty actor chain; only the product /v1/identity/control/exchange can mint a grant with a real chain -- a self-signed dev token cannot, and the platform rejects it with 403 %q", plane, "identity: token carries an empty actor chain")
	}
	return nil
}
