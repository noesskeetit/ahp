package emil

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestDelegationKeepsAnswerWhenArtifactPartIsASealReceipt(t *testing.T) {
	var task Task
	raw := `{"status":{"state":"TASK_STATE_COMPLETED","message":{"parts":[{"text":"the actual child answer"}]}},"artifacts":[{"artifactId":"file-id","metadata":{"sha256":"` + strings.Repeat("a", 64) + `","size":23,"mime":"text/plain"},"parts":[{"text":"sealed artifact file receipt"}]}]}`
	if err := json.Unmarshal([]byte(raw), &task); err != nil {
		t.Fatal(err)
	}
	if got := extractText(&task); got != "the actual child answer" {
		t.Fatalf("answer replaced by reference receipt: %q", got)
	}
	// Legacy inline answers retain their existing precedence over status prose.
	task = Task{}
	if err := json.Unmarshal([]byte(`{"status":{"message":{"parts":[{"text":"completed"}]}},"artifacts":[{"parts":[{"kind":"text","text":"legacy inline answer"}]}]}`), &task); err != nil {
		t.Fatal(err)
	}
	if got := extractText(&task); got != "legacy inline answer" {
		t.Fatalf("legacy answer changed: %q", got)
	}
}

// canonicalCard returns a platform-style Agent Card whose JSONRPC/"1.0"
// interface advertises an RPC endpoint at <base>/a2a/public (NOT the root),
// mirroring the second deploy's card.
func canonicalCard(r *http.Request) string {
	base := "http://" + r.Host
	return `{"name":"AHP Second Deploy (child)","version":"worker-state-v1",` +
		`"supportedInterfaces":[{"url":"` + base + `/a2a/public","protocolBinding":"JSONRPC","protocolVersion":"1.0"}],` +
		`"capabilities":{"streaming":true},"defaultInputModes":["text/plain"],"defaultOutputModes":["text/plain"],"skills":[]}`
}

func TestResolveBindingPicksCanonicalAdvertisedURL(t *testing.T) {
	c, _ := NewClient("https://example.invalid")
	b := c.ResolveBinding(&AgentCard{SupportedInterfaces: []SupportedInterface{
		{URL: "https://h/a2a/public", ProtocolBinding: "JSONRPC", ProtocolVersion: "1.0"},
	}})
	if !b.Canonical || b.RPCURL != "https://h/a2a/public" {
		t.Fatalf("expected canonical @/a2a/public, got %+v", b)
	}
	// Legacy (0.3 / no JSONRPC) must stay non-canonical so the Python server path is intact.
	if l := c.ResolveBinding(&AgentCard{SupportedInterfaces: []SupportedInterface{{URL: "x", ProtocolBinding: "JSONRPC", ProtocolVersion: "0.3"}}}); l.Canonical {
		t.Fatalf("0.3 must not be canonical")
	}
	if l := c.ResolveBinding(&AgentCard{}); l.Canonical || l.RPCURL != "https://example.invalid/" {
		t.Fatalf("no interface must fall back to legacy root, got %+v", l)
	}
}

// TestResolveBindingHonoursTopLevelCardURL locks the card contract the other way
// round: a deploy that advertises its endpoint as a top-level "url" must be
// reached AT that url, not at the base root -- the root POST is the defect this
// whole lane exists to remove. The advertised interface keeps winning wherever
// both exist, so the shape proven live (interface-only) cannot regress.
func TestResolveBindingHonoursTopLevelCardURL(t *testing.T) {
	c, _ := NewClient("https://parent.invalid")

	// endpoint advertised only at the top level
	if b := c.ResolveBinding(&AgentCard{URL: "https://child.invalid/a2a/public"}); b.RPCURL != "https://child.invalid/a2a/public" || !b.Canonical {
		t.Fatalf("top-level card url must be used as the endpoint, got %+v", b)
	}
	// both present: the interface wins -- this is the live-proven shape
	if b := c.ResolveBinding(&AgentCard{URL: "https://elsewhere.invalid/root", SupportedInterfaces: []SupportedInterface{
		{URL: "https://child.invalid/a2a/public", ProtocolBinding: "JSONRPC", ProtocolVersion: "1.0"},
	}}); b.RPCURL != "https://child.invalid/a2a/public" {
		t.Fatalf("advertised interface must outrank the top-level url, got %+v", b)
	}
	// the interface decides the WIRE, the card supplies the endpoint
	if b := c.ResolveBinding(&AgentCard{URL: "https://child.invalid/a2a/public", SupportedInterfaces: []SupportedInterface{
		{ProtocolBinding: "JSONRPC", ProtocolVersion: "0.3"},
	}}); b.RPCURL != "https://child.invalid/a2a/public" || b.Canonical {
		t.Fatalf("interface must pick the wire, card url the endpoint, got %+v", b)
	}
	// a non-JSONRPC interface must not shadow the top-level url
	if b := c.ResolveBinding(&AgentCard{URL: "https://child.invalid/a2a/public", SupportedInterfaces: []SupportedInterface{
		{URL: "https://elsewhere.invalid/grpc", ProtocolBinding: "GRPC", ProtocolVersion: "1.0"},
	}}); b.RPCURL != "https://child.invalid/a2a/public" {
		t.Fatalf("only a JSONRPC interface may outrank the card url, got %+v", b)
	}
	// Emil Python 0.3 reference: advertises nothing -- legacy root + 0.3 wire intact
	if b := c.ResolveBinding(&AgentCard{Name: "emil"}); b.RPCURL != "https://parent.invalid/" || b.Canonical {
		t.Fatalf("a silent card must keep the legacy 0.3 root path, got %+v", b)
	}
}

func TestDelegateCanonicalCompletesOnAdvertisedPath(t *testing.T) {
	var mu sync.Mutex
	var gotPath string
	var methods []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == AgentCardPath {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(canonicalCard(r)))
			return
		}
		var req jsonrpcRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		gotPath = r.URL.Path
		methods = append(methods, req.Method)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "SendMessage":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"task":{"id":"t1","contextId":"c1","status":{"state":"TASK_STATE_SUBMITTED"}}}}`))
		case "GetTask":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"id":"t1","contextId":"c1","status":{"state":"TASK_STATE_COMPLETED","message":{"role":"ROLE_AGENT","parts":[{"text":"done"}]}},"artifacts":[{"name":"child-executed.txt","parts":[{"text":"CHILD-EXECUTED"}]}]}}`))
		default:
			t.Errorf("unexpected method %q", req.Method)
		}
	}))
	defer srv.Close()

	a, err := New(Config{BaseURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, prov, err := a.Delegate(ctx, "do work")
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	mu.Lock()
	path := gotPath
	mu.Unlock()
	if path != "/a2a/public" {
		t.Fatalf("RPC must go to the card-advertised /a2a/public, got %q", path)
	}
	if res.State != "completed" || prov.TaskState != "completed" {
		t.Fatalf("state = %q/%q, want completed", res.State, prov.TaskState)
	}
	if res.TaskID != "t1" {
		t.Fatalf("task id = %q", res.TaskID)
	}
	if len(prov.ArtifactDigests) != 1 || prov.ArtifactDigests[0].Bytes != len("CHILD-EXECUTED") {
		t.Fatalf("artifact digest = %+v", prov.ArtifactDigests)
	}
}

func TestDelegateCanonicalCancelsChildOnContextCancel(t *testing.T) {
	var canceled int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == AgentCardPath {
			_, _ = w.Write([]byte(canonicalCard(r)))
			return
		}
		var req jsonrpcRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "SendMessage":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"task":{"id":"t9","contextId":"c9","status":{"state":"TASK_STATE_SUBMITTED"}}}}`))
		case "GetTask": // never terminal -> forces the caller to cancel
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"id":"t9","contextId":"c9","status":{"state":"TASK_STATE_WORKING"}}}`))
		case "CancelTask":
			atomic.StoreInt32(&canceled, 1)
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"id":"t9","contextId":"c9","status":{"state":"TASK_STATE_CANCELED"}}}`))
		}
	}))
	defer srv.Close()

	a, _ := New(Config{BaseURL: srv.URL})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(1200 * time.Millisecond); cancel() }()
	_, _, err := a.Delegate(ctx, "long work")
	if err == nil {
		t.Fatal("expected ctx error after cancel")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt32(&canceled) == 0 {
		time.Sleep(50 * time.Millisecond)
	}
	if atomic.LoadInt32(&canceled) != 1 {
		t.Fatal("cancelling the live turn must CancelTask the child (subtree cancel) — none received")
	}
}

// TestDelegatePresentsRunAuthority is the guard for identity propagation: the
// credential a child deployment must see is the RUN's own task token, and the
// labels that tie the child into the parent's tree (its conversation and the
// parent-task link) must actually travel on the wire. A regression here is a
// silent identity substitution at the hop, which no other test can see.
func TestDelegatePresentsRunAuthority(t *testing.T) {
	var mu sync.Mutex
	var authHeaders []string
	var sendParams map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
		mu.Unlock()
		if r.URL.Path == AgentCardPath {
			_, _ = w.Write([]byte(canonicalCard(r)))
			return
		}
		var req struct {
			ID     int64          `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Method == "SendMessage" {
			mu.Lock()
			sendParams = req.Params
			mu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "SendMessage":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"task":{"id":"t1","contextId":"c1","status":{"state":"TASK_STATE_SUBMITTED"}}}}`))
		default:
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"id":"t1","contextId":"c1","status":{"state":"TASK_STATE_COMPLETED"}}}`))
		}
	}))
	defer srv.Close()

	a, err := New(Config{
		BaseURL: srv.URL,
		Bearer:  "deployment-wide-static",
		RunAuthorityFor: func(context.Context) *RunAuthority {
			return &RunAuthority{
				Bearer:    "run-own-task-token",
				Scope:     contracts.Scope{Tenant: "a2a", Deployment: "a2a-parent", Subject: "alice"},
				TaskID:    "run-task-1",
				ContextID: "thread-1",
				Labels:    map[string]any{MetaParentTask: "run-task-1"},
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, prov, err := a.Delegate(ctx, "do work"); err != nil {
		t.Fatalf("delegate: %v", err)
	} else if !prov.RunScoped || prov.RunSubject != "alice" {
		t.Fatalf("provenance must record the run-scoped hop, got %+v", prov)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(authHeaders) == 0 {
		t.Fatal("server saw no requests")
	}
	for i, h := range authHeaders {
		if h != "Bearer run-own-task-token" {
			t.Fatalf("request %d carried %q; the run's own credential must never be replaced by the deployment bearer", i, h)
		}
	}
	if sendParams == nil {
		t.Fatal("no SendMessage received")
	}
	msg, _ := sendParams["message"].(map[string]any)
	if msg == nil || msg["contextId"] != "thread-1" {
		t.Fatalf("child task must be placed in the parent's conversation, got %v", sendParams["message"])
	}
	reqMD, _ := sendParams["metadata"].(map[string]any)
	if reqMD == nil || reqMD[MetaParentTask] != "run-task-1" {
		t.Fatalf("tree labels must reach the child, got %v", sendParams["metadata"])
	}
	if msgMD, _ := msg["metadata"].(map[string]any); msgMD == nil || msgMD[MetaParentTask] != "run-task-1" {
		t.Fatalf("labels must also ride on the inbound message, got %v", msg["metadata"])
	}
}

// TestDelegateFallsBackToStaticBearerOutsideRun keeps the other half honest: with
// no run authority to resolve (a CLI probe, a card prefetch) the configured bearer
// still authenticates the call, so wiring the resolver cannot break a caller that
// legitimately has no per-run credential.
func TestDelegateFallsBackToStaticBearerOutsideRun(t *testing.T) {
	var mu sync.Mutex
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = r.Header.Get("Authorization")
		mu.Unlock()
		if r.URL.Path == AgentCardPath {
			_, _ = w.Write([]byte(canonicalCard(r)))
			return
		}
		var req struct {
			ID     int64  `json:"id"`
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		if req.Method == "SendMessage" {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"task":{"id":"t1","status":{"state":"TASK_STATE_SUBMITTED"}}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"id":"t1","status":{"state":"TASK_STATE_COMPLETED"}}}`))
	}))
	defer srv.Close()

	a, err := New(Config{
		BaseURL: srv.URL,
		Bearer:  "deployment-wide-static",
		RunAuthorityFor: func(context.Context) *RunAuthority {
			return nil // no run in this context
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, prov, err := a.Delegate(ctx, "do work")
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	mu.Lock()
	h := got
	mu.Unlock()
	if h != "Bearer deployment-wide-static" {
		t.Fatalf("static bearer must still authenticate an out-of-run call, got %q", h)
	}
	if prov.RunScoped {
		t.Fatalf("provenance must not claim a run-scoped hop when there was no run authority: %+v", prov)
	}
}

// sendCapture is a stand for the child deploy's canonical face: it records the
// SendMessage params exactly as they arrived and completes the task on the first
// poll, so a test can assert on the WIRE FORM of the delegation rather than on
// any internal state of the adapter.
func sendCapture(t *testing.T, captured map[string]any, guard *sync.Mutex) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == AgentCardPath {
			_, _ = w.Write([]byte(canonicalCard(r)))
			return
		}
		var req struct {
			ID     int64          `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "SendMessage":
			guard.Lock()
			for k, v := range req.Params {
				captured[k] = v
			}
			guard.Unlock()
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"task":{"id":"t1","contextId":"c1","status":{"state":"TASK_STATE_SUBMITTED"}}}}`))
		default:
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"id":"t1","contextId":"c1","status":{"state":"TASK_STATE_COMPLETED","message":{"role":"ROLE_AGENT","parts":[{"text":"done"}]}}}}`))
		}
	}))
	return srv
}

func metadataOf(t *testing.T, captured map[string]any) (reqMD, msgMD map[string]any) {
	t.Helper()
	msg, _ := captured["message"].(map[string]any)
	if msg == nil {
		t.Fatalf("SendMessage carried no message: %v", captured)
	}
	reqMD, _ = captured["metadata"].(map[string]any)
	msgMD, _ = msg["metadata"].(map[string]any)
	if reqMD == nil || msgMD == nil {
		t.Fatalf("SendMessage must carry metadata at BOTH the request and the message level (request=%v message=%v)", captured["metadata"], msg["metadata"])
	}
	return reqMD, msgMD
}

func delegateWith(t *testing.T, cfg Config, ctx context.Context) {
	t.Helper()
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, _, err := a.Delegate(ctx, "do work"); err != nil {
		t.Fatalf("delegate: %v", err)
	}
}

// TestDelegateForwardsRunStepBudget is the offensive half of the step-budget
// contract: a run that was admitted with N steps must delegate with that number,
// because the child's execution plane reads metadata.max_steps to decide
// step_budget_source=caller. Without it the child runs on its own deployment
// default, which is a DIFFERENT number nobody chose (measured live: child default
// 16, same deployment's caller ceiling 4).
func TestDelegateForwardsRunStepBudget(t *testing.T) {
	var mu sync.Mutex
	captured := map[string]any{}
	srv := sendCapture(t, captured, &mu)
	defer srv.Close()

	delegateWith(t, Config{
		BaseURL: srv.URL,
		RunAuthorityFor: func(context.Context) *RunAuthority {
			return &RunAuthority{
				Bearer:    "run-own-task-token",
				Scope:     contracts.Scope{Tenant: "a2a", Deployment: "a2a-parent", Subject: "alice"},
				TaskID:    "run-task-1",
				ContextID: "thread-1",
				Labels:    map[string]any{MetaParentTask: "run-task-1"},
				MaxSteps:  3,
			}
		},
	}, context.Background())

	mu.Lock()
	defer mu.Unlock()
	reqMD, msgMD := metadataOf(t, captured)
	// One number at both levels: the child refuses (-32602) a request that states
	// two different values, so the adapter must never split them.
	for _, md := range []map[string]any{reqMD, msgMD} {
		if got := md[MetaStepBudget]; got != float64(3) {
			t.Fatalf("max_steps must reach the child as 3, got %#v in %v", got, md)
		}
		if md[MetaParentTask] != "run-task-1" {
			t.Fatalf("the tree label must travel with the budget, got %v", md)
		}
	}
}

// TestDelegateOmitsStepBudgetWhenUnset keeps the negative honest. "Nobody stated a
// budget" must stay absent on the wire, not become 0: 0 is not a positive whole
// number of steps and the child answers it with -32602, and an invented number
// would be journalled as step_budget_source=caller -- a budget the caller never
// gave, indistinguishable from exhaustion afterwards.
func TestDelegateOmitsStepBudgetWhenUnset(t *testing.T) {
	var mu sync.Mutex
	captured := map[string]any{}
	srv := sendCapture(t, captured, &mu)
	defer srv.Close()

	delegateWith(t, Config{
		BaseURL: srv.URL,
		RunAuthorityFor: func(context.Context) *RunAuthority {
			return &RunAuthority{
				Bearer:    "run-own-task-token",
				Scope:     contracts.Scope{Tenant: "a2a", Deployment: "a2a-parent", Subject: "alice"},
				TaskID:    "run-task-1",
				ContextID: "thread-1",
				Labels:    map[string]any{MetaParentTask: "run-task-1"},
				MaxSteps:  0,
			}
		},
	}, context.Background())

	mu.Lock()
	defer mu.Unlock()
	reqMD, msgMD := metadataOf(t, captured)
	for name, md := range map[string]map[string]any{"request": reqMD, "message": msgMD} {
		if _, ok := md[MetaStepBudget]; ok {
			t.Fatalf("an unstated budget must not appear at the %s level, got %v", name, md)
		}
	}
	if reqMD[MetaParentTask] != "run-task-1" {
		t.Fatalf("labels must still be forwarded when no budget was stated, got %v", reqMD)
	}
}

// TestDelegationMetadataDoesNotMutateRunLabels protects the host's own map: one
// run can delegate several times, and a resolver that returns a cached map must
// not find the budget written into it by the first delegation.
func TestDelegationMetadataDoesNotMutateRunLabels(t *testing.T) {
	labels := map[string]any{MetaParentTask: "run-task-1"}
	ra := &RunAuthority{Labels: labels, MaxSteps: 2}
	got := delegationMetadata(ra)
	if got[MetaStepBudget] != 2 {
		t.Fatalf("budget not composed into the metadata: %v", got)
	}
	if _, ok := labels[MetaStepBudget]; ok {
		t.Fatalf("the run's own label map was mutated: %v", labels)
	}
	if len(labels) != 1 {
		t.Fatalf("the run's own label map changed size: %v", labels)
	}
}

func itoa(v int64) string       { return strings.TrimSpace(jsonNumber(v)) }
func jsonNumber(v int64) string { b, _ := json.Marshal(v); return string(b) }
