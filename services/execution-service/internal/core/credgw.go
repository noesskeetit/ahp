package core

// credgw.go — T3 (director-endorsed plan; PDF rev3.2 л.194): Tool & Credential Gateway seam.
//
// The architecture places EXTERNAL tool credentials behind a dedicated Tool & Credential
// Gateway. A secret must NEVER live in the agent engine, the tool registry, a tool
// definition, the model's view of a tool, or the RuntimeConfig. It is resolved ONLY at
// the moment a tool executes, for the (tool, run-scope) in flight, and handed to the
// tool through the per-call request context — never stored, never serialized, never
// logged. The model gateway is likewise environment-only (see NewOpenAICompatFromEnv);
// there is no foreign-provider fallback.
//
// This file is the isolation boundary the plan named: CredentialingExecutor sits between
// the engine's ToolRegistry and the real ToolExecutor, so the registry and the engine
// can be constructed and introspected without ever holding a credential.

import (
	"context"
	"encoding/json"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Secret is an opaque credential value that redacts itself in every human- or
// JSON-facing representation, so an accidental %v/%s/%#v/JSON of any struct that
// embeds it cannot print the plaintext.
type Secret string

// String redacts. fmt.Stringer.
func (s Secret) String() string { return "[redacted]" }

// GoString redacts. fmt.GoStringer (%#v).
func (s Secret) GoString() string { return "core.Secret(\"[redacted]\")" }

// MarshalJSON redacts. json.Marshal of any container holding a Secret.
func (s Secret) MarshalJSON() ([]byte, error) { return []byte(`"[redacted]"`), nil }

// Reveal returns the underlying plaintext. Tools call this ONLY inside execution,
// from a credential obtained via ToolCredentialFromContext. It is intentionally
// explicit (no implicit conversion) so the use site is greppable.
func (s Secret) Reveal() string { return string(s) }

// CredentialRequest identifies which credential the gateway should resolve for a
// single in-flight tool execution. Scope is the run scope already carried by the
// request context — the gateway, not the caller, decides what a (tool, scope) needs.
type CredentialRequest struct {
	Tool  string          `json:"tool"`
	Scope contracts.Scope `json:"scope"`
}

// ToolCredential is the resolved credential handed to a tool for one execution.
// The Secret self-redacts; Extra carries only non-secret handles (e.g. a key id).
type ToolCredential struct {
	Secret Secret            `json:"secret"`
	Extra  map[string]string `json:"extra,omitempty"`
}

// CredentialGateway resolves the credential for one tool execution. Implementations
// are the external secret store / Tool & Credential Gateway; they never receive the
// credential from the engine, and the engine never receives it ahead of time.
type CredentialGateway interface {
	Credential(ctx context.Context, req CredentialRequest) (ToolCredential, error)
}

type credKey struct{}

// WithToolCredential binds a credential to ctx for the duration of one tool call.
func WithToolCredential(ctx context.Context, cred ToolCredential) context.Context {
	return context.WithValue(ctx, credKey{}, cred)
}

// ToolCredentialFromContext returns the credential the CredentialingExecutor injected
// for this call, if any. Tools read their secret here and nowhere else.
func ToolCredentialFromContext(ctx context.Context) (ToolCredential, bool) {
	c, ok := ctx.Value(credKey{}).(ToolCredential)
	return c, ok
}

// CredentialingExecutor adapts a real ToolExecutor so each call first resolves the
// credential from the gateway (scoped to the run) and injects it into the per-call
// context. It fails closed: if the gateway cannot supply a credential the tool does
// not run. The gateway and the inner executor are the only parties that ever see a
// credential; the ToolRegistry that wraps this executor holds neither.
type CredentialingExecutor struct {
	inner   ToolExecutor
	gateway CredentialGateway
}

// NewCredentialingExecutor wires a real executor behind a credential gateway.
func NewCredentialingExecutor(inner ToolExecutor, gateway CredentialGateway) *CredentialingExecutor {
	return &CredentialingExecutor{inner: inner, gateway: gateway}
}

var _ ToolExecutor = (*CredentialingExecutor)(nil)

// Execute resolves the per-call credential and runs the inner tool with it in ctx.
func (e *CredentialingExecutor) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	scope, _, _ := RunScopeFromContext(ctx)
	cred, err := e.gateway.Credential(ctx, CredentialRequest{Tool: name, Scope: scope})
	if err != nil {
		// Fail closed: a tool that needs a credential must not run without one.
		return nil, err
	}
	return e.inner.Execute(WithToolCredential(ctx, cred), name, args)
}
