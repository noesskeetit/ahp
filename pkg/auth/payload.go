// rawClaims mirrors Claims with the time-bearing members decoded as
// json.RawMessage, so the exact wire bytes of exp/iat/nbf decide acceptance:
// a JSON string that merely looks like a number ("123") must never pass,
// which json.Number would allow.
//
// rawClaims is a DECODE view only; the signer marshals Claims, so an empty jti
// is omitted from the signed bytes and old no-jti tokens keep their original
// wire. A present jti (a session-bound credential) is read back verbatim; a
// missing jti decodes to the empty string, which is exactly what every task
// token and control-plane human token carries.

package auth

import "encoding/json"

type rawClaims struct {
	Issuer   string `json:"iss"`
	Subject  string `json:"sub"`
	Audience string `json:"aud"`
	// JTI is optional: absent on task and control-plane human tokens (decodes
	// to ""), present on browser credentials that carry a session id.
	JTI        string          `json:"jti,omitempty"`
	ExpiresAt  json.RawMessage `json:"exp"`
	IssuedAt   json.RawMessage `json:"iat"`
	NotBefore  json.RawMessage `json:"nbf"`
	TokenUse   string          `json:"token_use"`
	Kind       string          `json:"token_kind"`
	Tenant     string          `json:"tenant"`
	Deployment string          `json:"azp"`
	Task       string          `json:"task"`
	Root       string          `json:"root"`
	Context    string          `json:"ctx"`
	CtxScope   string          `json:"ctx_scope,omitempty"`
	Grant      string          `json:"grant"`
	Depth      int             `json:"depth"`
	Actors     []string        `json:"act"`
}
