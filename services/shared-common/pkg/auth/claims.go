// Package auth mints and verifies AHP access tokens: compact Ed25519 (EdDSA)
// JWTs whose claims carry the full isolation scope of the holder.
//
// Tokens are signed with the standard library only. Verification is strict to
// the point of being unforgiving: a single unexpected byte, an unknown key
// identifier, a duplicated JSON member or a claim that merely looks plausible
// makes the token invalid. Error values never contain the presented token or
// any of its contents, so they are safe to log and to return to callers.
package auth

import (
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Token kinds. A token is either a human end-user token or a task token that
// was delegated down the workload tree. The strings are part of the wire
// format.
const (
	// KindHuman marks tokens issued to a human subject.
	KindHuman = "human"
	// KindTask marks tokens issued to an delegated task workload.
	KindTask = "task"
)

// CtxScope is an OPTIONAL claim (json "ctx_scope", omitted when empty) naming
// which context-scoping rule applies to the token. "authority" is the only
// value the platform defines. Every already-issued token omits it, so their
// signed bytes are byte-for-byte unchanged.
const (
	// CtxScopeAuthority marks the per-session ingress authority token minted by
	// the human -> Space -> scoped-task browser exchange. Its Artifacts read
	// scope is the subject and tenant, not one conversation, so a human
	// downloads their own artifacts across conversations. It is set ONLY by that
	// server-side mint, is never inherited by delegated children or renewals
	// (they mint via MintTask), and cannot be requested by a caller (the exchange
	// body forbids unknown fields). Every other credential keeps enforcing
	// context equality exactly as before.
	CtxScopeAuthority = "authority"
)

// TokenUse is the single value accepted for the "token_use" claim. Every AHP
// token is an access token; refresh or identity tokens are a separate design.
const TokenUse = "access"

// SPIFFE prefixes and separators used to derive a workload identity from an
// isolation scope.
const (
	// WorkloadScheme is the trust-domain prefix of every AHP workload ID.
	WorkloadScheme = "spiffe://ahp"

	tenantSegment    = "/tenant/"
	deploymentPrefix = "/deployment/"
)

// Claims is the payload of an AHP access token. Field names and JSON tags are
// a shared API consumed by the identity service and must not be renamed.
//
// Time-bearing claims are seconds since the Unix epoch. Optional members,
// including a signed capability list, are omitted when unset.
type Claims struct {
	Issuer   string `json:"iss"`
	Subject  string `json:"sub"`
	Audience string `json:"aud"`
	// JTI is the optional JWT standard identifier claim: the signer writes it only when it is non-empty. Task tokens
	// and the control-plane human tokens the platform has always issued carry
	// no session id, so their signed bytes keep the pre-jti wire they had
	// before this claim existed. A credential whose revocation must be
	// observable before its own expiry sets a jti: it makes two tokens minted
	// for the same principal in the same second distinct, which is what lets a
	// logout retire exactly the credential it issued.
	JTI          string   `json:"jti,omitempty"`
	ExpiresAt    int64    `json:"exp"`
	IssuedAt     int64    `json:"iat"`
	NotBefore    int64    `json:"nbf"`
	TokenUse     string   `json:"token_use"`
	Kind         string   `json:"token_kind"`
	Tenant       string   `json:"tenant"`
	Deployment   string   `json:"azp"`
	Task         string   `json:"task"`
	Root         string   `json:"root"`
	Context      string   `json:"ctx"`
	CtxScope     string   `json:"ctx_scope,omitempty"`
	Capabilities []string `json:"cap,omitempty"`
	Grant        string   `json:"grant"`
	Depth        int      `json:"depth"`
	Actors       []string `json:"act"`
}

// Scope projects the token claims onto the platform isolation scope. The
// subject is always the original human/service principal that originated the
// delegation chain: every task in the tree keeps the same scope, so records
// created by delegations remain attributable to the original subject.
func (c Claims) Scope() contracts.Scope {
	return contracts.Scope{
		Tenant:     c.Tenant,
		Deployment: c.Deployment,
		Subject:    c.Subject,
	}
}

// WorkloadID returns the SPIFFE identity of the workload a scope designates:
//
//	spiffe://ahp/tenant/{tenant}/deployment/{deployment}
//
// The identifiers are used verbatim; callers are expected to hand it scope
// that already passed contracts.Scope.Validate.
func WorkloadID(scope contracts.Scope) string {
	var b strings.Builder
	b.Grow(len(WorkloadScheme) + len(tenantSegment) + len(scope.Tenant) +
		len(deploymentPrefix) + len(scope.Deployment))
	b.WriteString(WorkloadScheme)
	b.WriteString(tenantSegment)
	b.WriteString(scope.Tenant)
	b.WriteString(deploymentPrefix)
	b.WriteString(scope.Deployment)
	return b.String()
}
