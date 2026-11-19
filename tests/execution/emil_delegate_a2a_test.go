package execution_test

// emil_delegate_a2a_test.go -- the full emil.delegate round trip over REAL
// services on isolated PostgreSQL:
//
//   browser AcceptTurn/DispatchTurn (real Space Coordinator -> real State API)
//     -> a core.Runtime turn whose MODEL is a deterministic test double (NOT a
//        real LLM proof -- the native LLM leg belongs to the stand, assigned
//        separately)
//     -> the turn calls the "emil.delegate" tool. Its adapter performs a REAL
//        mTLS HTTPS request to a delegated Emil core at its public face. The
//        adapter is the real production adapter: it fetches the Agent Card,
//        issues message/send over the A2A v0.3 wire, and computes the artifact
//        digest + provenance in production code -- nothing about the adapter is
//        faked.
//     -> the Emil core double stands in for the external Emil service (it speaks
//        the v0.3 wire exactly, the dialect the adapter targets). It returns a
//        completed task carrying a file artifact.
//     -> the delegate Result + Provenance (agent name + artifact digest) are
//        journalled into the durable step-stream
//     -> the same turn then calls save_artifact, sealing the delegate's bytes
//     -> the run reaches a durable terminal status in PostgreSQL.
//
// Scope note: the AHP execution gateway (the v1.0 A2A plane) drains tasks through
// its own worker and is covered by the gateway + composition suites; Emil is a
// SEPARATE external core agent reached over the v0.3 wire, so this test models
// that delegation honestly rather than pretending the gateway speaks Emil's wire.

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	delegateAgentName = "ahp-delegate-agent"
	delegateSubject   = "exec-svc"
)

const delegateArtifactBody = "REVENUE TABLE\nQ1=100\nQ2=137\nTOTAL=237\n"

// delegateCardJSON is the Emil core's public Agent Card in the v0.3 wire shape the
// adapter parses (kind-tagged, JSONRPC at the root path).
func delegateCardJSON() []byte {
	card := map[string]any{
		"name":                delegateAgentName,
		"description":         "delegated Emil core agent (test double)",
		"version":             "1.0.0",
		"supportedInterfaces": []any{map[string]any{"url": "https://delegate.invalid/", "protocolBinding": "JSONRPC", "protocolVersion": "0.3"}},
		"capabilities":        map[string]any{"streaming": true, "pushNotifications": false},
		"defaultInputModes":   []string{"text/plain"},
		"defaultOutputModes":  []string{"text/plain"},
		"skills":              []any{map[string]any{"id": "delegate", "name": "Delegate a durable task", "tags": []string{"a2a"}}},
	}
	raw, _ := json.Marshal(card)
	return raw
}

// delegateTaskJSON builds a v0.3 A2A task result: a COMPLETED task carrying one
// text artifact with body. It is the exact shape the adapter's Task struct parses.
func delegateTaskJSON(body string) []byte {
	task := map[string]any{
		"id":        "task-delegate-1",
		"contextId": "ctx-delegate-1",
		"kind":      "task",
		"status":    map[string]any{"state": "completed"},
		"artifacts": []any{map[string]any{
			"artifactId": "art-delegate-1",
			"name":       "delegate.txt",
			"parts":      []any{map[string]any{"kind": "text", "text": body}},
		}},
	}
	raw, _ := json.Marshal(task)
	return raw
}

// startDelegateEmil boots a minimal Emil-core double that speaks the v0.3 A2A wire
// over mTLS with a scoped task bearer on the public face, mirroring the auth seam
// the real plane enforces. It returns the base URL and an HTTP client carrying the
// workload client cert.
func startDelegateEmil(t *testing.T, e *a2aEnv, body string) (string, *http.Client) {
	t.Helper()
	inner := transport.Task(e.verifier, a2aAudience, "execution", allowAll)
	mux := http.NewServeMux()
	mux.HandleFunc(emil.AgentCardPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(delegateCardJSON())
	})
	mux.Handle("/", inner(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			ID     int64  `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "message/send", "tasks/get":
			result := delegateTaskJSON(body)
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":` + string(result) + `}`))
		default:
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"error":{"code":-32601,"message":"method not found"}}`))
		}
	})))

	dCert := e.ca.clientCert(t, workloadURI(e.tenant, e.deployment))
	serverCAs := x509.NewCertPool()
	if !serverCAs.AppendCertsFromPEM(e.ca.pem) {
		t.Fatal("delegate CA PEM rejected")
	}
	ts := httptest.NewUnstartedServer(mux)
	ts.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: serverCAs}
	ts.StartTLS()
	t.Cleanup(ts.Close)

	tr := ts.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{dCert}
	hc := &http.Client{Transport: tr, Timeout: 40 * time.Second}
	return ts.URL, hc
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// allowAll is the coarse authorize callback for the delegate face; the scoped task
// bearer and the verified workload certificate carry the real isolation, mirroring
// the execution role's own coarse authorize seam.
func allowAll(context.Context, auth.Claims, string) error { return nil }

// recordingSink is a test ArtifactSink that seals bytes in memory and records the
// handle save_artifact returns.
type recordingSink struct {
	mu       sync.Mutex
	saved    bool
	name     string
	body     string
	sha256   string
	scopeSub string
}

func (s *recordingSink) SaveArtifact(ctx context.Context, name, mime string, data []byte) (core.ArtifactHandle, error) {
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	sc, _, hasScope := core.RunScopeFromContext(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved = true
	s.name = name
	s.body = string(data)
	s.sha256 = digest
	_ = hasScope
	s.scopeSub = sc.Subject
	return core.ArtifactHandle{ID: "art-sealed", Version: "1", SHA256: digest, Size: int64(len(data))}, nil
}

// scriptedDelegateModel is the deterministic test-double model: turn 0 delegates
// via emil.delegate, turn 1 seals the delegate bytes via save_artifact, then a
// final answer. No real LLM is involved (test double, not real-model proof).
type scriptedDelegateModel struct {
	mu    sync.Mutex
	calls int
}

func (m *scriptedDelegateModel) Generate(_ context.Context, req *core.ModelRequest) (*core.ModelResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	step := m.calls
	m.calls++
	switch step {
	case 0:
		args, _ := json.Marshal(map[string]string{"prompt": "fetch the revenue table"})
		return &core.ModelResponse{Content: json.RawMessage(`"I will delegate to the execution agent"`),
			ToolCalls: []core.ToolCall{{ID: "t1", Name: emil.ToolName, Args: args}}}, nil
	case 1:
		args, _ := json.Marshal(map[string]string{"name": "report.txt", "content": delegateArtifactBody, "mime": "text/plain"})
		return &core.ModelResponse{Content: json.RawMessage(`"sealing the delegated report"`),
			ToolCalls: []core.ToolCall{{ID: "t2", Name: core.ToolSaveArtifact, Args: args}}}, nil
	default:
		return &core.ModelResponse{Content: json.RawMessage(`"the delegated report is sealed"`)}, nil
	}
}

func TestEmilDelegateRoundTrip(t *testing.T) {
	e := newA2AEnv(t)
	body := delegateArtifactBody
	bodyDigest := sha256.Sum256([]byte(body))
	wantDigest := hex.EncodeToString(bodyDigest[:])

	baseURL, gwClient := startDelegateEmil(t, e, body)

	// The emil adapter speaks the real A2A v0.3 wire to the delegate core, with
	// mTLS (its HTTP client carries the workload client cert) and a scoped bearer.
	delegateBearer := e.taskToken(t, delegateSubject, "task-delegate-"+contracts.NewID())
	adapter, err := emil.New(emil.Config{BaseURL: baseURL, HTTPClient: gwClient, Bearer: delegateBearer, CardCacheTTL: time.Minute})
	if err != nil {
		t.Fatalf("emil.New: %v", err)
	}

	sink := &recordingSink{}
	saveTool := core.NewSaveArtifactTool(sink)
	mx := core.NewMultiExecutor()
	mx.Register(emil.ToolName, adapter)
	mx.Register(core.ToolSaveArtifact, saveTool)
	reg := core.NewToolRegistry(mx)
	reg.Register(core.ToolDef{Name: emil.ToolName, Description: "delegate to the execution agent",
		Parameters: json.RawMessage(`{"type":"object","properties":{"prompt":{"type":"string"}},"required":["prompt"]}`)})
	reg.Register(saveTool.Def())

	rt := core.NewRuntime(core.RuntimeConfig{Model: &scriptedDelegateModel{}, Tools: reg, State: e.client})
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: rt, WorkerID: "w-a2a"})

	subject := "a2a-user"
	scope := e.scope(subject)
	token := e.taskToken(t, subject, "task-"+contracts.NewID())
	accept, err := coord.AcceptTurn(e.ctx, token, scope, space.AcceptTurnRequest{
		Key: "emil-" + contracts.NewID(), ContextID: "ctx-" + contracts.NewID(),
		Input: json.RawMessage(`"produce the revenue report"`), MaxSteps: 6,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(e.ctx, token, scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}

	// 1) durable terminal status of the SPACE run.
	deadline := time.Now().Add(60 * time.Second)
	var runStatus string
	for time.Now().Before(deadline) {
		run, err := e.client.GetRun(e.ctx, token, accept.TurnID)
		if err == nil {
			runStatus = run.Status
			if run.Status == "done" || run.Status == "completed" {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if runStatus != "done" && runStatus != "completed" {
		t.Fatalf("space run did not reach durable terminal status (last=%q)", runStatus)
	}

	// 2) provenance + delegate artifact digest in the DURABLE step-stream, both
	//    computed by the real emil adapter over the real mTLS hop.
	events, err := e.client.ReadStream(e.ctx, token, accept.ThreadID, 0, 200)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	var stream strings.Builder
	for _, ev := range events {
		stream.WriteString(string(ev.Payload))
		stream.WriteByte('\n')
	}
	sb := stream.String()
	if !strings.Contains(sb, wantDigest) {
		t.Fatalf("durable stream missing delegate artifact digest %s\nstream=%s", wantDigest, sb)
	}
	if !strings.Contains(sb, "provenance") || !strings.Contains(sb, delegateAgentName) {
		t.Fatalf("durable stream missing emil provenance/agent name\nstream=%s", sb)
	}

	// 3) save_artifact sealed the delegate bytes durably (the sink recorded them).
	sink.mu.Lock()
	sealed, sb2, ssha, sub := sink.saved, sink.body, sink.sha256, sink.scopeSub
	sink.mu.Unlock()
	if !sealed || sb2 != body || ssha != wantDigest {
		t.Fatalf("save_artifact did not seal the delegate bytes: sealed=%v body=%q sha=%s want sha=%s",
			sealed, sb2, ssha, wantDigest)
	}
	if sub == "" {
		t.Fatal("save_artifact ran without a task scope in context")
	}

	t.Logf("round trip OK: run durable=%q; stream carries delegate digest %s + provenance/agent %q; save_artifact sealed %d bytes",
		runStatus, wantDigest[:12], delegateAgentName, len(body))
}
