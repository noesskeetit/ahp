package emil

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// capturedCard is the real Agent Card served by ahp-handbook-emil-core:5ed238b4
// (captured over the wire during smoke). Shapes must match exactly.
const capturedCard = `{"name":"core-agent","description":"Policy-enforced core agent runtime","supportedInterfaces":[{"url":"http://127.0.0.1:18000","protocolBinding":"HTTP+JSON","protocolVersion":"1.0"},{"url":"http://127.0.0.1:18000","protocolBinding":"JSONRPC","protocolVersion":"0.3"}],"version":"1.0.0","capabilities":{"streaming":true,"pushNotifications":false},"defaultInputModes":["text/plain","application/json"],"defaultOutputModes":["text/plain","application/json"],"skills":[{"id":"core_artifact_list","name":"core_artifact_list","description":"Enabled agent capability: core_artifact_list","tags":[]}]}`

// taskJSON builds a JSON-RPC response wrapping a task result.
func taskJSON(task string) string {
	return `{"jsonrpc":"2.0","id":1,"result":` + task + `}`
}

func newStub(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := NewClient(srv.URL)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c, srv
}

func TestAgentCardParsesAndDigests(t *testing.T) {
	c, _ := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != AgentCardPath {
			t.Errorf("card path = %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(capturedCard))
	})
	card, raw, err := c.AgentCard(context.Background())
	if err != nil {
		t.Fatalf("AgentCard: %v", err)
	}
	if card.Name != "core-agent" || card.Version != "1.0.0" {
		t.Fatalf("unexpected card %+v", card)
	}
	if len(card.Skills) != 1 || card.Skills[0].ID != "core_artifact_list" {
		t.Fatalf("skills not parsed: %+v", card.Skills)
	}
	if !card.Capabilities.Streaming {
		t.Errorf("streaming should be true")
	}
	sum := sha256.Sum256([]byte(capturedCard))
	if got, want := hexSHA256(raw), hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("card digest mismatch: got %s want %s", got, want)
	}
}

func TestSendCompletedWithArtifact(t *testing.T) {
	// completed task with a text artifact and a data artifact.
	task := `{"id":"task-1","contextId":"ctx-1","kind":"task","status":{"state":"completed","timestamp":"2026-10-29T09:37:40Z"},"artifacts":[{"artifactId":"a1","name":"out.txt","parts":[{"kind":"text","text":"hello"}]},{"artifactId":"a2","parts":[{"kind":"data","data":{"k":1}}]}]}`
	a, err := New(Config{BaseURL: mustURL(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case AgentCardPath:
			_, _ = w.Write([]byte(capturedCard))
		case "/":
			_, _ = w.Write([]byte(taskJSON(task)))
		}
	})})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, prov, err := a.Delegate(context.Background(), "make file")
	if err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	if res.State != StateCompleted || res.Text != "hello" {
		t.Fatalf("res=%+v", res)
	}
	if res.TaskID != "task-1" || res.ContextID != "ctx-1" {
		t.Fatalf("ids=%+v", res)
	}
	if len(prov.ArtifactDigests) != 2 {
		t.Fatalf("want 2 digests, got %d", len(prov.ArtifactDigests))
	}
	// text artifact digest must be sha256("hello")
	if want := sha256hex("hello"); prov.ArtifactDigests[0].SHA256 != want {
		t.Fatalf("text digest got %s want %s", prov.ArtifactDigests[0].SHA256, want)
	}
	if prov.ArtifactDigests[0].Bytes != 5 {
		t.Fatalf("text bytes = %d", prov.ArtifactDigests[0].Bytes)
	}
	if prov.AgentName != "core-agent" || prov.AgentCardSHA256 == "" {
		t.Fatalf("provenance card missing: %+v", prov)
	}
}

func TestSendFailedModelUnavailable(t *testing.T) {
	// The exact shape captured from the live smoke container (no model).
	task := `{"contextId":"c","history":[],"id":"t-1","kind":"task","status":{"message":{"contextId":"c","kind":"message","messageId":"m2","parts":[{"kind":"text","text":"MODEL_UNAVAILABLE"}],"role":"agent","taskId":"t-1"},"state":"failed","timestamp":"2026-10-29T09:37:40.655503Z"}}`
	a, _ := newAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case AgentCardPath:
			_, _ = w.Write([]byte(capturedCard))
		case "/":
			_, _ = w.Write([]byte(taskJSON(task)))
		}
	})
	res, prov, err := a.Delegate(context.Background(), "ping")
	if err == nil {
		t.Fatal("expected error for failed state")
	}
	if !strings.Contains(err.Error(), "MODEL_UNAVAILABLE") {
		t.Fatalf("error should carry Emil text: %v", err)
	}
	if prov == nil || prov.TaskState != StateFailed {
		t.Fatalf("provenance should report failed: %+v", prov)
	}
	if res == nil || res.State != StateFailed {
		t.Fatalf("res should report failed: %+v", res)
	}
}

func TestGetTask(t *testing.T) {
	task := `{"id":"t-9","kind":"task","status":{"state":"working"}}`
	c, _ := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		var req jsonrpcRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Method != "tasks/get" {
			t.Errorf("method=%q", req.Method)
		}
		_, _ = w.Write([]byte(taskJSON(task)))
	})
	got, _, err := c.GetTask(context.Background(), "t-9")
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.ID != "t-9" || got.Status.State != StateWorking {
		t.Fatalf("task=%+v", got)
	}
}

func TestAuthHeaderPassthroughAndRedaction(t *testing.T) {
	const secret = "secret-bearer-DO-NOT-LOG"
	var gotAuth string
	a, _ := newAdapterOpts(t, secret, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		switch r.URL.Path {
		case AgentCardPath:
			_, _ = w.Write([]byte(capturedCard))
		case "/":
			task := `{"id":"t","kind":"task","status":{"state":"completed"},"artifacts":[{"artifactId":"a","parts":[{"kind":"text","text":"ok"}]}]}`
			_, _ = w.Write([]byte(taskJSON(task)))
		}
	}, WithBearer(secret))
	if _, _, err := a.Delegate(context.Background(), "hi"); err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	if gotAuth != "Bearer "+secret {
		t.Fatalf("stub did not receive bearer header, got %q", gotAuth)
	}
	// provenance must mark auth present but never leak the secret.
	a2, _ := newAdapterOpts(t, secret, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case AgentCardPath:
			_, _ = w.Write([]byte(capturedCard))
		case "/":
			task := `{"id":"t","kind":"task","status":{"state":"completed"}}`
			_, _ = w.Write([]byte(taskJSON(task)))
		}
	}, WithBearer(secret))
	a2.mcp = []MCPServer{{ID: "mcp1", AuthType: "bearer", Secret: "mcp-secret-XYZ"}}
	res, prov, err := a2.Delegate(context.Background(), "hi")
	_ = res
	if err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	if !prov.AuthPresent || prov.AuthType != "bearer" {
		t.Fatalf("auth not recorded: %+v", prov)
	}
	b, _ := json.Marshal(prov)
	s := string(b)
	if strings.Contains(s, secret) || strings.Contains(s, "mcp-secret-XYZ") {
		t.Fatalf("provenance leaked a secret: %s", s)
	}
	if len(prov.MCPServers) != 1 || !prov.MCPServers[0].SecretPresent || prov.MCPServers[0].ID != "mcp1" {
		t.Fatalf("mcp ref wrong: %+v", prov.MCPServers)
	}
}

func TestJSONRPCErrorMapping(t *testing.T) {
	c, _ := newStub(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}`))
	})
	_, _, err := c.Send(context.Background(), SendRequest{Text: "x"})
	if err == nil || !strings.Contains(err.Error(), "Method not found") {
		t.Fatalf("expected jsonrpc error, got %v", err)
	}
}

func TestExecuteAsCoreTool(t *testing.T) {
	a, _ := newAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case AgentCardPath:
			_, _ = w.Write([]byte(capturedCard))
		case "/":
			task := `{"id":"t","contextId":"c","kind":"task","status":{"state":"completed"},"artifacts":[{"artifactId":"a","parts":[{"kind":"text","text":"result!"}]}]}`
			_, _ = w.Write([]byte(taskJSON(task)))
		}
	})
	args, _ := json.Marshal(map[string]string{"prompt": "go"})
	out, err := a.Execute(context.Background(), ToolName, args)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(out, &payload); err != nil {
		t.Fatalf("payload decode: %v", err)
	}
	if payload["text"] != "result!" || payload["task_state"] != "completed" {
		t.Fatalf("payload=%v", payload)
	}
	// unknown tool rejected
	if _, err := a.Execute(context.Background(), "other", args); err == nil {
		t.Fatal("expected unknown tool error")
	}
}

func TestPartMarshalRoundTrip(t *testing.T) {
	for _, in := range []Part{
		{Kind: "text", Text: "hi"},
		{Kind: "data", Data: json.RawMessage(`{"n":2}`)},
		{Kind: "file", File: &FilePart{Name: "f.bin", Bytes: "aGk=", MimeType: "application/octet-stream"}},
	} {
		b, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("marshal %v: %v", in, err)
		}
		var back Part
		if err := json.Unmarshal(b, &back); err != nil {
			t.Fatalf("unmarshal %s: %v", b, err)
		}
		if back.Kind != in.Kind {
			t.Fatalf("kind roundtrip %s -> %s", in.Kind, back.Kind)
		}
	}
}

func TestNewClientRejectsBadURL(t *testing.T) {
	if _, err := NewClient("ftp://x"); err == nil {
		t.Fatal("expected scheme error")
	}
	if _, err := NewClient("https://u:p@host"); err == nil {
		t.Fatal("expected credential error")
	}
}

// --- helpers ---

func sha256hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func mustURL(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

func newAdapter(t *testing.T, h http.HandlerFunc) (*Adapter, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	a, err := New(Config{BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a, srv
}

func newAdapterOpts(t *testing.T, bearer string, h http.HandlerFunc, opts ...Option) (*Adapter, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	// Build client with options directly for bearer.
	c, err := NewClient(srv.URL, opts...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	a := &Adapter{client: c, authType: "bearer", authPresent: bearer != ""}
	return a, srv
}
