package core

// credgw_test.go — T3 (PDF rev3.2 л.194): the Tool & Credential Gateway isolation
// boundary. Proven WITHOUT any real secret store:
//   - a credential reaches the tool ONLY through the per-call context, scoped to the run;
//   - the registry / tool defs / marshaled credential never contain the plaintext;
//   - the executor fails closed (no credential -> the tool does not run);
//   - the model gateway is sourced ONLY from AHP_LLM_* (no foreign-provider fallback).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

const canarySecret = "s3cr3t-CANARY-do-not-leak"

type capturingExecutor struct {
	gotCred   ToolCredential
	gotCredOK bool
	gotScope  contracts.Scope
	calledFor string
}

func (c *capturingExecutor) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	c.calledFor = name
	c.gotCred, c.gotCredOK = ToolCredentialFromContext(ctx)
	c.gotScope, _, _ = RunScopeFromContext(ctx)
	return json.RawMessage(`{"ok":true}`), nil
}

type staticGateway struct {
	secret  Secret
	wantErr error
	lastReq CredentialRequest
	calls   int
}

// A real Tool & Credential Gateway is a remote secret store: it holds no plaintext
// field to dump. Model it opaque so deep fmt of anything that references the gateway
// redacts (the Secret type guarantees redaction for the credential object itself).
func (g *staticGateway) String() string   { return "staticGateway{[redacted]}" }
func (g *staticGateway) GoString() string { return "staticGateway{[redacted]}" }

func (g *staticGateway) Credential(ctx context.Context, req CredentialRequest) (ToolCredential, error) {
	g.calls++
	g.lastReq = req
	if g.wantErr != nil {
		return ToolCredential{}, g.wantErr
	}
	return ToolCredential{Secret: g.secret, Extra: map[string]string{"key_id": "k-1"}}, nil
}

func TestCredentialReachesToolViaContextScopedToRun(t *testing.T) {
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "u1"}
	inner := &capturingExecutor{}
	gw := &staticGateway{secret: Secret(canarySecret)}
	exec := NewCredentialingExecutor(inner, gw)
	reg := NewToolRegistry(exec)
	reg.Register(ToolDef{Name: "crm_lookup", Description: "lookup"})

	callCtx := WithRunScope(context.Background(), scope, "task-token")
	if _, err := reg.Execute(callCtx, "crm_lookup", json.RawMessage(`{"id":"c1"}`)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gw.calls != 1 || gw.lastReq.Tool != "crm_lookup" || gw.lastReq.Scope != scope {
		t.Fatalf("gateway must resolve per (tool, run scope): calls=%d req=%+v", gw.calls, gw.lastReq)
	}
	if !inner.gotCredOK || inner.gotCred.Secret.Reveal() != canarySecret {
		t.Fatalf("tool must receive its credential via ctx, got ok=%v", inner.gotCredOK)
	}
	if inner.gotScope != scope {
		t.Fatalf("tool run scope must be propagated: got %+v", inner.gotScope)
	}
}

func TestCredentialNeverLeaksToEngineStateOrMarshal(t *testing.T) {
	inner := &capturingExecutor{}
	gw := &staticGateway{secret: Secret(canarySecret)}
	exec := NewCredentialingExecutor(inner, gw)
	reg := NewToolRegistry(exec)
	reg.Register(ToolDef{Name: "crm_lookup", Description: "lookup"})

	// (a) tool definitions handed to the model carry no credential.
	for _, d := range reg.Defs() {
		b, _ := json.Marshal(d)
		if strings.Contains(string(b), canarySecret) {
			t.Fatalf("ToolDef must never embed the credential: %s", b)
		}
	}
	// (b) the serializable engine state (the registry and the gateway-wrapped executor
	//     are the engine-side objects) contains no credential. They hold the gateway
	//     only by interface and expose no credential field, so JSON of the engine state
	//     cannot contain it.
	for _, eng := range []any{reg, exec} {
		b := jsonMust(eng)
		if strings.Contains(string(b), canarySecret) {
			t.Fatalf("serialized engine state (%T) leaked the credential: %s", eng, b)
		}
	}
	// (c) the credential object itself redacts under every representation; only
	//     Reveal() reaches the plaintext, and only for the executing tool.
	cred := ToolCredential{Secret: Secret(canarySecret), Extra: map[string]string{"key_id": "k-1"}}
	for label, rendered := range map[string]string{
		"cred %v":     fmt.Sprintf("%v", cred),
		"cred %s":     fmt.Sprintf("%s", cred),
		"cred %#v":    fmt.Sprintf("%#v", cred),
		"cred json":   string(jsonMust(cred)),
		"secret %v":   fmt.Sprintf("%v", cred.Secret),
		"secret %#v":  fmt.Sprintf("%#v", cred.Secret),
		"secret json": string(jsonMust(cred.Secret)),
	} {
		if strings.Contains(rendered, canarySecret) {
			t.Fatalf("representation %q leaked the plaintext: %s", label, rendered)
		}
	}
	if got := string(jsonMust(cred.Secret)); got != `"[redacted]"` {
		t.Fatalf("Secret JSON must be the redaction token, got %s", got)
	}
	if cred.Secret.Reveal() != canarySecret {
		t.Fatal("Reveal must return the plaintext for the executing tool")
	}
	// (d) after execution the credential is held only by the gateway and the tool, never
	//     copied into the engine-side CredentialingExecutor fields.
	for label, eng := range map[string]any{"executor": exec, "registry": reg} {
		b := jsonMust(eng)
		if strings.Contains(string(b), canarySecret) {
			t.Fatalf("post-exec %s state leaked the credential: %s", label, b)
		}
	}
}

func TestCredentialingExecutorFailClosedNoGateway(t *testing.T) {
	inner := &capturingExecutor{}
	exec := NewCredentialingExecutor(inner, &staticGateway{wantErr: errors.New("no credential")})
	if _, err := exec.Execute(context.Background(), "crm_lookup", json.RawMessage(`{}`)); err == nil {
		t.Fatal("executor must fail closed when the gateway cannot supply a credential")
	}
	if inner.calledFor != "" {
		t.Fatalf("inner tool must NOT run without a credential, but ran for %q", inner.calledFor)
	}
}

func TestModelGatewaySourcesOnlyFromEnv(t *testing.T) {
	t.Setenv(EnvOpenAIBaseURL, "https://qwen.invalid.internal")
	t.Setenv(EnvOpenAIModel, "model-run-tqmbu-road")
	t.Setenv(EnvOpenAIAPIKey, "env-only-token")
	c, err := NewOpenAICompatFromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if c.Model() != "model-run-tqmbu-road" {
		t.Fatalf("model must come from AHP_LLM_MODEL only, got %q", c.Model())
	}
	os.Unsetenv(EnvOpenAIModel)
	if _, err := NewOpenAICompatFromEnv(); err == nil {
		t.Fatal("FromEnv must fail closed with no model (no foreign-provider fallback)")
	}
}

func jsonMust(v any) []byte { b, _ := json.Marshal(v); return b }
