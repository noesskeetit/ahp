package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"go.opentelemetry.io/otel/trace"
)

func testGatewayAuthority(t *testing.T) (runScopeAttester, contracts.Scope, string) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("tool-runner-test", "key", key)
	if err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: "tenant", Deployment: "runner", Subject: "subject"}
	token, err := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment,
		Subject: scope.Subject, Task: "grant-task", Root: "root", Context: "context", Grant: "grant"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return runScopeAttester{verifier: auth.NewVerifier("tool-runner-test", map[string]ed25519.PublicKey{"key": signer.Public()}),
		taskAudience: "ahp", workload: auth.WorkloadID(scope)}, scope, token
}

func testGatewayContext(scope contracts.Scope, token, coordinate string) context.Context {
	return core.WithRunBinding(core.WithRunScope(context.Background(), scope, token),
		core.RunBinding{TaskID: "run", ContextID: "context", OperationKey: coordinate})
}

func testToolManifest(t *testing.T, names ...string) *space.ToolManifestSnapshot {
	t.Helper()
	var defs []space.GatewayToolDescriptor
	for _, name := range names {
		defs = append(defs, space.GatewayToolDescriptor{Name: name, Description: "fixture tool", Parameters: json.RawMessage(`{"type":"object"}`)})
	}
	canonicalDefs, canonical, err := canonicalToolDescriptors(defs)
	if err != nil {
		t.Fatal(err)
	}
	return &space.ToolManifestSnapshot{Version: 1, SourceSHA256: strings.Repeat("1", 64), SHA256: toolDigest(canonical), Tools: canonicalDefs}
}

type staticToolPinner struct{ pin *space.AgentConfigRef }

func (s staticToolPinner) Pin(context.Context, string, string, string) (*space.AgentConfigRef, error) {
	return s.pin, nil
}

func TestGatewayManifestPinsAllowedSubsetAndCopiesSchemas(t *testing.T) {
	owner := &space.AgentConfigRef{Revision: 7, Dot: &space.DotRuntimeConfig{MaxSteps: 8, AllowedTools: []string{"echo"}}}
	manifest := testToolManifest(t, "hidden", "echo")
	pinner := manifestPinner{staticToolPinner{owner}, manifest}
	pin, err := pinner.Pin(context.Background(), "not-logged", "op", "context")
	if err != nil {
		t.Fatal(err)
	}
	if pin.ToolManifest == nil || len(pin.ToolManifest.Tools) != 1 || pin.ToolManifest.Tools[0].Name != "echo" || owner.ToolManifest != nil {
		t.Fatal("pinner did not enforce subset/copy isolation")
	}
	if _, err := pinnedGatewayDefinitions(pin.ToolManifest, map[string]bool{"echo": true}); err != nil {
		t.Fatal(err)
	}
	// Neither an in-flight copy nor the owner pointer can mutate another pin.
	pin.ToolManifest.Tools[0].Parameters[2] = 'X'
	second, err := pinner.Pin(context.Background(), "not-logged", "op2", "context")
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(second.ToolManifest.Tools[0].Parameters) {
		t.Fatal("shared mutable schema")
	}
	second.ToolManifest.Tools[0].Description = "tampered"
	if _, err := pinnedGatewayDefinitions(second.ToolManifest, map[string]bool{"echo": true}); err == nil {
		t.Fatal("tampered pin accepted")
	}
	if _, err := pinnedGatewayDefinitions(manifest, map[string]bool{"echo": true}); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("foreign descriptor exceeded policy")
	}
}

func TestGatewayManifestRequiresExactFilePin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tools.json")
	data := []byte(`{"version":1,"tools":[{"name":"echo","description":"safe","parameters":{"type":"object"}}]}`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readGatewayToolManifest(path, toolDigest(data)); err != nil {
		t.Fatal(err)
	}
	for _, digest := range []string{"", strings.Repeat("0", 64)} {
		if _, err := readGatewayToolManifest(path, digest); err == nil {
			t.Fatal("missing/incorrect SHA accepted")
		}
	}
	for _, data := range [][]byte{
		[]byte(`{"version":1,"tools":[],"bearer":"MUST-NOT-APPEAR"}`),
		[]byte(`{"version":1,"tools":[{"name":"echo","parameters":{"type":"string"}}]}`),
		[]byte(`{"version":1,"tools":[{"name":"echo","parameters":{"type":"object"}},{"name":"echo","parameters":{"type":"object"}}]}`),
	} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		_, err := readGatewayToolManifest(path, toolDigest(data))
		if err == nil || strings.Contains(err.Error(), "MUST-NOT-APPEAR") {
			t.Fatal("invalid manifest accepted or raw content leaked")
		}
	}
}

type gatewayRoundTrip func(*http.Request) (*http.Response, error)

func (f gatewayRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestGatewayBridgeAuthorityTraceAndOwnerResult(t *testing.T) {
	authority, scope, token := testGatewayAuthority(t)
	var calls atomic.Int32
	args := json.RawMessage(`{"value":"hello"}`)
	digest, _ := contracts.Digest(map[string]any{"kind": tools.EffectKind, "target": "echo", "guard": "", "arguments": args})
	var recordedKey string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("current run credential not forwarded")
		}
		if r.Header.Get("traceparent") == "" || r.Header.Get("baggage") != "" {
			t.Error("run trace missing or baggage leaked")
		}
		var in struct {
			Key       string          `json:"key"`
			Tool      string          `json:"tool"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if json.NewDecoder(r.Body).Decode(&in) != nil {
			t.Error("invalid owner request")
			return
		}
		if strings.HasSuffix(r.URL.Path, "/Call") {
			if in.Tool != "echo" || !bytes.Equal(in.Arguments, args) {
				t.Error("tool arguments changed")
			}
			recordedKey = in.Key
			_ = json.NewEncoder(w).Encode(map[string]any{"key": in.Key, "target": "echo", "status": "SUCCEEDED", "digest": digest})
		} else {
			if in.Key != recordedKey {
				t.Error("GetResult changed operation coordinate")
			}
			_ = json.NewEncoder(w).Encode(tools.ResultView{Key: in.Key, Target: "echo", Status: "SUCCEEDED", Digest: digest, Storage: "inline",
				Provenance: json.RawMessage(`{"schema":"tools-result-v1","grant_context":"context"}`),
				Payload:    json.RawMessage(`{"content":[{"type":"text","text":"actual-result"}]}`)})
		}
	}))
	defer server.Close()
	bridge, err := newGatewayToolBridge(server.URL, server.Client(), authority)
	if err != nil {
		t.Fatal(err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1}, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(testGatewayContext(scope, token, "0"), sc)
	result, err := bridge.Execute(ctx, "echo", args)
	if err != nil || !bytes.Contains(result, []byte("actual-result")) || calls.Load() != 2 {
		t.Fatalf("bridge failed: %v calls=%d", err, calls.Load())
	}
	if len(recordedKey) > 128 || strings.Contains(recordedKey, token) {
		t.Fatal("unsafe operation key")
	}
	for _, bad := range []context.Context{
		context.Background(),
		testGatewayContext(contracts.Scope{Tenant: "foreign", Deployment: scope.Deployment, Subject: scope.Subject}, token, "0"),
		testGatewayContext(scope, token+"tampered", "0"),
		core.WithRunBinding(core.WithRunScope(context.Background(), scope, token), core.RunBinding{TaskID: "run", ContextID: "other", OperationKey: "0"}),
		testGatewayContext(scope, token, ""),
	} {
		if _, err := bridge.Execute(bad, "echo", args); contracts.Code(err) != contracts.ErrForbidden {
			t.Fatal("invalid run authority accepted")
		}
	}
	if calls.Load() != 2 {
		t.Fatal("denied authority reached owner")
	}
}

func TestGatewayBridgeNoRetryNoRedirectNoRawCredentialErrors(t *testing.T) {
	authority, scope, token := testGatewayAuthority(t)
	canary := "CREDENTIAL-CANARY-do-not-log"
	for _, tc := range []struct {
		name   string
		status int
		body   string
		err    error
	}{
		{"transport", 0, "", fmt.Errorf("https://example.invalid/?key=%s", canary)},
		{"denied", 403, `{"code":"FORBIDDEN","message":"` + canary + `"}`, nil},
		{"ambiguous", 503, canary, nil},
		{"redirect", 307, canary, nil},
		{"malformed", 200, canary, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			client := &http.Client{Transport: gatewayRoundTrip(func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				if tc.err != nil {
					return nil, tc.err
				}
				return &http.Response{StatusCode: tc.status, Header: http.Header{"Location": []string{"https://foreign.invalid/?secret=" + canary}}, Body: io.NopCloser(strings.NewReader(tc.body)), Request: r}, nil
			})}
			bridge, err := newGatewayToolBridge("https://owner.invalid", client, authority)
			if err != nil {
				t.Fatal(err)
			}
			_, err = bridge.Execute(testGatewayContext(scope, token, "0"), "echo", json.RawMessage(`{}`))
			if err == nil || calls.Load() != 1 || strings.Contains(err.Error(), canary) || strings.Contains(err.Error(), token) {
				t.Fatalf("unsafe failure/retry: calls=%d", calls.Load())
			}
			if tc.name == "denied" && contracts.Code(err) != contracts.ErrForbidden {
				t.Fatal("owner deny class lost")
			}
			if tc.name == "transport" && contracts.ReasonOf(err) != "SIDE_EFFECT_UNKNOWN" {
				t.Fatal("ambiguous effect was not preserved")
			}
		})
	}
}

func TestGatewayBridgeDeadlineStopsAndDoesNotRetry(t *testing.T) {
	authority, scope, token := testGatewayAuthority(t)
	var calls atomic.Int32
	client := &http.Client{Transport: gatewayRoundTrip(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	bridge, err := newGatewayToolBridge("https://owner.invalid", client, authority)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(testGatewayContext(scope, token, "0"), 10*time.Millisecond)
	defer cancel()
	_, err = bridge.Execute(ctx, "echo", json.RawMessage(`{}`))
	if err == nil || calls.Load() != 1 || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("deadline did not bound one attempt")
	}
}

func TestGatewayProfileRejectsDeniedAndTamperedSchemasBeforeModel(t *testing.T) {
	model := &profileCaptureModel{}
	runner := profileRuntime{cfg: core.RuntimeConfig{Model: model, MaxTokens: 20000}}
	manifest := testToolManifest(t, "echo")
	req := &space.CoreRequest{Input: json.RawMessage(`"hi"`), AgentConfig: &space.AgentConfigRef{Dot: &space.DotRuntimeConfig{MaxSteps: 8, AllowedTools: []string{}}, ToolManifest: manifest}}
	if err := runner.Run(context.Background(), req, make(chan space.StepResult, 16)); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("descriptor outside policy accepted")
	}
	req.AgentConfig.Dot.AllowedTools = []string{"echo"}
	if err := runner.Run(context.Background(), req, make(chan space.StepResult, 16)); err == nil {
		t.Fatal("missing gateway accepted")
	}
	if len(model.requests) != 0 {
		t.Fatal("invalid snapshot reached model")
	}
}

func TestGatewayPinnerConcurrentPinsAreIsolated(t *testing.T) {
	owner := &space.AgentConfigRef{Dot: &space.DotRuntimeConfig{MaxSteps: 8, AllowedTools: []string{"echo"}}}
	pinner := manifestPinner{staticToolPinner{owner}, testToolManifest(t, "echo")}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pin, err := pinner.Pin(context.Background(), "token", "op", "context")
			if err != nil {
				t.Error(err)
				return
			}
			pin.ToolManifest.Tools[0].Parameters[0] = 'X'
		}()
	}
	wg.Wait()
	if owner.ToolManifest != nil {
		t.Fatal("owner mutated")
	}
}

func TestGatewayBridgeRejectsStaleActionAndForeignResultProvenance(t *testing.T) {
	authority, scope, token := testGatewayAuthority(t)
	args := json.RawMessage(`{"value":1}`)
	digest, _ := contracts.Digest(map[string]any{"kind": tools.EffectKind, "target": "echo", "guard": "", "arguments": args})
	for _, tamper := range []string{"action", "context", "target", "digest"} {
		t.Run(tamper, func(t *testing.T) {
			var calls atomic.Int32
			client := &http.Client{Transport: gatewayRoundTrip(func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				var in struct {
					Key string `json:"key"`
				}
				if json.NewDecoder(r.Body).Decode(&in) != nil {
					t.Error("invalid owner request")
				}
				var payload any
				if strings.HasSuffix(r.URL.Path, "/Call") {
					value := digest
					if tamper == "action" {
						value = "sha256:" + strings.Repeat("0", 64)
					}
					payload = map[string]any{"key": in.Key, "target": "echo", "status": "SUCCEEDED", "digest": value}
				} else {
					contextID, target, value := "context", "echo", digest
					if tamper == "context" {
						contextID = "foreign-context"
					}
					if tamper == "target" {
						target = "foreign-tool"
					}
					if tamper == "digest" {
						value = "foreign-digest"
					}
					provenance, _ := json.Marshal(map[string]string{"schema": tools.ProvenanceSchema, "grant_context": contextID})
					payload = tools.ResultView{Key: in.Key, Target: target, Status: "SUCCEEDED", Digest: value, Storage: "inline", Provenance: provenance, Payload: json.RawMessage(`{"content":[]}`)}
				}
				encoded, _ := json.Marshal(payload)
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(encoded)), Request: r}, nil
			})}
			bridge, err := newGatewayToolBridge("https://owner.invalid", client, authority)
			if err != nil {
				t.Fatal(err)
			}
			_, err = bridge.Execute(testGatewayContext(scope, token, "0"), "echo", args)
			want := contracts.ErrForbidden
			if tamper == "action" {
				want = contracts.ErrKeyReuse
			}
			if contracts.Code(err) != want {
				t.Fatalf("invalid receipt accepted: class=%s", contracts.Code(err))
			}
			if tamper == "action" && calls.Load() != 1 {
				t.Fatal("stale action read a result")
			}
		})
	}
}
