package toolsapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
)

// TestCallFailureIsHumanReadable is the red->green proof for the human-readable
// tool-call error (director 2026-11-02). RED: a failed call used to reach the
// caller as {"status":"FAILED"} and nothing else -- no reason, no way to tell a
// person what to do, while the raw internal cause (endpoint URLs, credential
// paths) was journalled verbatim. GREEN: a failed or tool-refused call carries a
// classified, sanitized `failure` (+ trace handle), durably -- Get shows the same
// reason -- and a successful call still echoes no tool output.
func TestCallFailureIsHumanReadable(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A tool server that ANSWERS with an error result (the call round-trips;
	// the tool itself refuses).
	srv := mcp.NewServer(&mcp.Implementation{Name: "refuser", Version: "1"}, nil)
	srv.AddTool(&mcp.Tool{Name: "refuser", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{
					Text: "quota exhausted for team ops; configure more at https://mcp.internal.example/quota",
				}},
			}, nil
		})
	ts := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv },
		&mcp.StreamableHTTPOptions{JSONResponse: true}))
	t.Cleanup(ts.Close)
	refuserConn := func(ctx context.Context, cred tools.Credential) (tools.MCPClient, error) {
		base, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			base = &http.Transport{}
		}
		cs, err := mcp.NewClient(&mcp.Implementation{Name: "ahp-tools-gw", Version: "1"}, nil).Connect(ctx,
			&mcp.StreamableClientTransport{Endpoint: ts.URL,
				HTTPClient:           &http.Client{Transport: bearerTransport{base: base, token: cred.Bearer}},
				DisableStandaloneSSE: true}, nil)
		if err != nil {
			return nil, err
		}
		return clientSession{cs: cs}, nil
	}
	// A tool server that cannot be reached at all -- the connector error carries
	// exactly the kind of detail that must never reach a person's screen.
	deadConn := func(context.Context, tools.Credential) (tools.MCPClient, error) {
		return nil, fmt.Errorf(`Post "http://svc:Hunt3rS3cret@tool-mcp.internal:9443/mcp": dial tcp: connect: connection refused`)
	}

	svc2, err := tools.New(ctx, e.pool, "ahp_tools_f"+randomHex(t, 6), tools.Config{
		Guardian:    tools.NewPolicyGuardian("test-failure"),
		Credentials: fakeCreds{bearer: "cred-" + randomHex(t, 4)},
		Connections: map[string]tools.Connector{
			"dead":    deadConn,
			"refuser": refuserConn,
			"good":    e.fixture.connector(),
		},
		Tools: map[string]tools.ToolSpec{
			"flaky":   {Name: "flaky", Connection: "dead"},
			"refuser": {Name: "refuser", Connection: "refuser"},
			"echo":    {Name: "echo", Connection: "good"},
		},
		Verifier: e.verifier,
		Audience: tAudience,
	})
	if err != nil {
		t.Fatalf("tools.New(failure): %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		_ = svc2.Drop(dctx)
	})
	// Same permissive capability callback the shared env uses; capability
	// enforcement itself is covered by the security tests.
	permit := func(context.Context, auth.Claims, string) error { return nil }
	h2, err := NewHandler(svc2, e.verifier, permit)
	if err != nil {
		t.Fatalf("NewHandler(failure): %v", err)
	}
	e.srv.Config.Handler = h2

	tok := e.signTask(t)
	const secret = "Hunt3rS3cret"

	// (1) unreachable tool server: FAILED + a class + a sentence, no leak.
	key := "k-fail-" + randomHex(t, 4)
	status, body := e.call(t, "Call", tok, `{"key":"`+key+`","tool":"flaky","arguments":{}}`)
	if status != http.StatusOK {
		t.Fatalf("failed call: status=%d body=%v, want 200 with a terminal view", status, body)
	}
	if body["status"] != "FAILED" {
		t.Fatalf("status=%v, want FAILED", body["status"])
	}
	f := requireFailure(t, body)
	if f["class"] != tools.FailureUnreachable {
		t.Fatalf("failure class=%v, want %s", f["class"], tools.FailureUnreachable)
	}
	msg, _ := f["message"].(string)
	if strings.TrimSpace(msg) == "" {
		t.Fatal("human-facing failure message is empty")
	}
	blob := strings.ToLower(mustJSONString(t, body))
	for _, leak := range []string{strings.ToLower(secret), "tool-mcp.internal", "http://"} {
		if strings.Contains(blob, leak) {
			t.Fatalf("failure surface leaked %q: %s", leak, blob)
		}
	}

	// (2) the reason is durable, not response-time decoration: Get shows it too.
	_, got := e.call(t, "Get", tok, `{"key":"`+key+`"}`)
	if g := requireFailure(t, got); g["class"] != tools.FailureUnreachable {
		t.Fatalf("Get failure=%v, want the journalled unreachable class", g)
	}
	// Correlation handle stability: the trace shown on a later read is the trace
	// of the request that failed, never of the read itself.
	if body["trace_id"] != got["trace_id"] {
		t.Fatalf("trace handle changed on read: %v vs %v", body["trace_id"], got["trace_id"])
	}

	// (3) tool refused the call: the tool's own words reach the human, URL
	// sanitized, and no tool payload is echoed on a success.
	status, body = e.call(t, "Call", tok, `{"key":"k-refuse-`+randomHex(t, 4)+`","tool":"refuser","arguments":{}}`)
	if status != http.StatusOK {
		t.Fatalf("refused call: status=%d body=%v", status, body)
	}
	f = requireFailure(t, body)
	if f["class"] != tools.FailureToolError {
		t.Fatalf("refusal class=%v, want %s", f["class"], tools.FailureToolError)
	}
	if !strings.Contains(fmt.Sprint(f["message"]), "quota exhausted") {
		t.Fatalf("the tool's own explanation must reach the human: %v", f["message"])
	}
	if strings.Contains(fmt.Sprint(f["message"]), "https://") {
		t.Fatalf("tool text was not sanitized: %v", f["message"])
	}

	_, body = e.call(t, "Call", tok, `{"key":"k-ok-`+randomHex(t, 4)+`","tool":"echo","arguments":{}}`)
	if body["status"] != "SUCCEEDED" {
		t.Fatalf("echo status=%v, want SUCCEEDED", body["status"])
	}
	if _, ok := body["failure"]; ok {
		t.Fatalf("a successful call must carry no failure: %v", body["failure"])
	}
	if strings.Contains(mustJSONString(t, body), "echoed") {
		t.Fatal("tool output must never be echoed back")
	}
	// The raw cause must not be journalled verbatim either.
	if !svc2SchemaHasNoMarker(t, e, secret) {
		t.Fatalf("the raw cause (%q) must not be persisted verbatim", secret)
	}
}

func requireFailure(t *testing.T, body map[string]any) map[string]any {
	t.Helper()
	f, ok := body["failure"].(map[string]any)
	if !ok {
		t.Fatalf("no human-readable failure in the response: %s", mustJSONString(t, body))
	}
	return f
}

func mustJSONString(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func svc2SchemaHasNoMarker(t *testing.T, e *env, marker string) bool {
	t.Helper()
	return !e.schemaContainsMarker(t, marker)
}
