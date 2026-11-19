package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/internal/tools/toolsapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const gatewayCredentialCanary = "synthetic-owner-credential-never-for-Core"

type gatewayFixtureCredentials struct {
	mu     sync.Mutex
	scopes []contracts.Scope
}

func (c *gatewayFixtureCredentials) Resolve(_ context.Context, s contracts.Scope, connection string) (tools.Credential, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.scopes = append(c.scopes, s)
	return tools.Credential{Bearer: gatewayCredentialCanary}, nil
}

type gatewayFixtureMCP struct{ session *mcp.ClientSession }

func (c gatewayFixtureMCP) CallTool(ctx context.Context, p *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	return c.session.CallTool(ctx, p)
}
func (c gatewayFixtureMCP) Close() error { return c.session.Close() }

// This deterministic model is explicitly a protocol fixture, not inference.
// It refuses to finish until two distinct, correctly bound actual MCP receipts
// have returned through the native Core conversation.
type gatewayThreeStepModel struct {
	mu         sync.Mutex
	calls      int
	marker     string
	receipts   map[string]bool
	inputBytes int
}

func (m *gatewayThreeStepModel) Generate(_ context.Context, req *core.ModelRequest) (*core.ModelResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if len(req.Tools) != 1 || req.Tools[0].Name != "bench_echo" {
		return nil, fmt.Errorf("native Core did not advertise pinned tool")
	}
	if m.calls == 1 {
		for _, message := range req.Messages {
			if message.Role == "user" {
				var input string
				if json.Unmarshal(message.Content, &input) == nil {
					m.inputBytes = len([]byte(input))
					if !strings.Contains(input, m.marker) {
						return nil, fmt.Errorf("input marker lost")
					}
				}
			}
		}
		if m.inputBytes != 100*1024 {
			return nil, fmt.Errorf("input must be exactly 100 KiB")
		}
	} else {
		last := req.Messages[len(req.Messages)-1]
		if last.Role != "tool" || last.ToolCallID != fmt.Sprintf("call-%d", m.calls-1) {
			return nil, fmt.Errorf("MCP result is not bound to preceding model call")
		}
		var result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		}
		if json.Unmarshal(last.Content, &result) != nil || len(result.Content) != 1 {
			return nil, fmt.Errorf("missing canonical MCP result")
		}
		var receipt struct {
			Marker string `json:"marker"`
			ID     string `json:"receipt_id"`
			Step   int    `json:"step"`
		}
		if json.Unmarshal([]byte(result.Content[0].Text), &receipt) != nil || receipt.Marker != m.marker || receipt.Step != m.calls-1 || receipt.ID == "" || m.receipts[receipt.ID] {
			return nil, fmt.Errorf("invalid or duplicate actual MCP receipt")
		}
		m.receipts[receipt.ID] = true
	}
	if m.calls == 3 {
		return &core.ModelResponse{Content: json.RawMessage(`"готово"`), Stop: "stop"}, nil
	}
	if m.calls > 3 {
		return nil, fmt.Errorf("unexpected extra model call")
	}
	args, _ := json.Marshal(map[string]any{"marker": m.marker, "payload": "ok", "step": m.calls})
	return &core.ModelResponse{Content: json.RawMessage(`""`), Stop: "tool_calls", ToolCalls: []core.ToolCall{{ID: fmt.Sprintf("call-%d", m.calls), Name: "bench_echo", Args: args}}}, nil
}

// TestGatewayNativeThreeLLMTwoMCP uses real PostgreSQL Identity, Registry, State
// and ToolsService, task tokens + mTLS, and the actual MCP SDK StreamableHTTP
// client/server. Only the LLM is a deterministic fixture. It requires an OWNED
// test database; it never connects to a default/environment production DSN.
func TestGatewayNativeThreeLLMTwoMCP(t *testing.T) {
	dsn := os.Getenv("AHP_MCP_BRIDGE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires owned PostgreSQL clone via AHP_MCP_BRIDGE_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var logs bytes.Buffer
	previousLogger := slog.Default()
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	slog.SetDefault(logger)
	defer slog.SetDefault(previousLogger)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("invalid test database configuration")
	}
	defer pool.Close()
	ids, reg, st := identity.New(pool), registry.New(pool), state.New(pool)
	for _, migrate := range []func(context.Context) error{ids.Migrate, reg.Migrate, st.Migrate} {
		if err = migrate(ctx); err != nil {
			t.Fatal("owned test database migration failed")
		}
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("bridge-integration", "key", priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("bridge-integration", map[string]ed25519.PublicKey{"key": signer.Public()})
	scope := contracts.Scope{Tenant: "bridge-" + contracts.NewID(), Subject: "alice", Deployment: "space"}
	task := "root-" + contracts.NewID()
	if _, err = ids.CreateRoot(ctx, scope, identity.RootRequest{Task: task, Context: task, Deployment: scope.Deployment, Capabilities: []string{"registry", "state", "tools"}, TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	token, err := ids.MintTask(ctx, scope, task, signer, "ahp", 0)
	if err != nil {
		t.Fatal(err)
	}
	var actualMCP atomic.Int32
	receipts := map[string]bool{}
	var receiptsMu sync.Mutex
	mcpServer := mcp.NewServer(&mcp.Implementation{Name: "bridge-real-protocol-fixture", Version: "1"}, nil)
	mcpServer.AddTool(&mcp.Tool{Name: "bench_echo", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"marker": map[string]any{"type": "string"}, "payload": map[string]any{"type": "string"}, "step": map[string]any{"type": "integer"}}, "required": []string{"marker", "payload", "step"}}}, func(_ context.Context, r *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		actualMCP.Add(1)
		var args struct {
			Marker  string `json:"marker"`
			Payload string `json:"payload"`
			Step    int    `json:"step"`
		}
		if json.Unmarshal(r.Params.Arguments, &args) != nil || args.Payload != "ok" || (args.Step != 1 && args.Step != 2) {
			return nil, fmt.Errorf("invalid fixture call")
		}
		id := contracts.NewID()
		receiptsMu.Lock()
		receipts[id] = true
		receiptsMu.Unlock()
		payload, _ := json.Marshal(map[string]any{"profile_fixture": "root-density-profile-mcp", "marker": args.Marker, "tool": "bench_echo", "step": args.Step, "receipt_id": id})
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(payload)}}}, nil
	})
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return mcpServer }, &mcp.StreamableHTTPOptions{Stateless: true})
	mcpHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+gatewayCredentialCanary {
			t.Error("owner credential did not reach MCP")
			w.WriteHeader(403)
			return
		}
		mcpHandler.ServeHTTP(w, r)
	}))
	defer mcpHTTP.Close()
	credentialSource := &gatewayFixtureCredentials{}
	schema := "ahp_bridge_" + toolDigest([]byte(contracts.NewID()))[:16]
	service, err := tools.New(ctx, pool, schema, tools.Config{Guardian: tools.NewPolicyGuardian("integration-default-deny"), Credentials: credentialSource, Verifier: verifier, Audience: "ahp", Connections: map[string]tools.Connector{"fixture": func(ctx context.Context, credential tools.Credential) (tools.MCPClient, error) {
		client := &http.Client{Transport: gatewayRoundTrip(func(r *http.Request) (*http.Response, error) {
			copy := r.Clone(r.Context())
			copy.Header.Set("Authorization", "Bearer "+credential.Bearer)
			return http.DefaultTransport.RoundTrip(copy)
		})}
		session, e := mcp.NewClient(&mcp.Implementation{Name: "bridge-tools-owner", Version: "1"}, nil).Connect(ctx, &mcp.StreamableClientTransport{Endpoint: mcpHTTP.URL, HTTPClient: client}, nil)
		if e != nil {
			return nil, e
		}
		return gatewayFixtureMCP{session}, nil
	}}, Tools: map[string]tools.ToolSpec{"bench_echo": {Name: "bench_echo", Connection: "fixture"}, "denied_write": {Name: "bench_echo", Connection: "fixture", SideEffecting: true}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanup, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	}()
	toolHandler, err := toolsapi.NewHandler(service, verifier, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	registryHandler, err := registry.NewHTTPHandler(reg, verifier, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	stateHandler, err := stateapi.New(st, verifier, ids.Authorize, func(ctx context.Context, s contracts.Scope, taskID string) (string, error) {
		return ids.MintTask(ctx, s, taskID, signer, "ahp", 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	var firstKey string
	var firstKeyMu sync.Mutex
	mux := http.NewServeMux()
	mux.Handle("/v1/tools/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/tools/Call" {
			data, e := io.ReadAll(r.Body)
			if e != nil {
				w.WriteHeader(400)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))
			var in struct {
				Key string `json:"key"`
			}
			if json.Unmarshal(data, &in) == nil {
				firstKeyMu.Lock()
				if firstKey == "" {
					firstKey = in.Key
				}
				firstKeyMu.Unlock()
			}
		}
		toolHandler.ServeHTTP(w, r)
	}))
	mux.Handle("/v1/registry/", registryHandler)
	mux.Handle("/v1/state/", stateHandler)
	ca := newOwnerCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	owner := httptest.NewUnstartedServer(mux)
	owner.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	owner.StartTLS()
	defer owner.Close()
	transport := owner.Client().Transport.(*http.Transport).Clone()
	transport.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	bridge, err := newGatewayToolBridge(owner.URL, client, runScopeAttester{verifier: verifier, taskAudience: "ahp", workload: auth.WorkloadID(scope)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reg.PutConfig(ctx, scope.Tenant, registry.DotAgentID, 0, json.RawMessage(`{"instructions":"Run the native fixture tool twice, then finish.","model_route":"fixture","max_steps":8,"policy":{"allowed_model_routes":["fixture"],"allowed_skills":[],"allowed_tools":["bench_echo"],"allowed_computer_policies":[]}}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = reg.PutModelRoute(ctx, scope.Tenant, "fixture", 0, json.RawMessage(`{"model":"deterministic-contract-fixture"}`)); err != nil {
		t.Fatal(err)
	}
	pinner, err := space.NewProfileClient(owner.URL, client)
	if err != nil {
		t.Fatal(err)
	}
	manifest := testToolManifest(t, "bench_echo")
	marker := "ROOTDENSITY_0011223344556677_0"
	model := &gatewayThreeStepModel{marker: marker, receipts: map[string]bool{}}
	runtime := profileRuntime{cfg: core.RuntimeConfig{Model: model, State: space.NewStateClient(owner.URL, client), MaxTokens: 20000, Logger: logger}, selectModel: func(string) (core.ModelClient, error) { return model, nil }, gateway: bridge}
	sc := space.NewStateClient(owner.URL, client)
	coordinator := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, ConfigPinner: manifestPinner{pinner, manifest}, Core: runtime, WorkerID: "bridge-worker"})
	input := "Complete the native three-model/two-tool fixture.\n"
	input += strings.Repeat("x", 100*1024-len(input)-len(marker)-1) + "\n" + marker
	inputJSON, _ := json.Marshal(input)
	if _, err = coordinator.AcceptTurn(ctx, token, scope, space.AcceptTurnRequest{Key: "injected-config", ContextID: task, Input: inputJSON, AgentConfig: &space.AgentConfigRef{ToolManifest: manifest}}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatal("caller-owned tool descriptors accepted")
	}
	accepted, err := coordinator.AcceptTurn(ctx, token, scope, space.AcceptTurnRequest{Key: "profile-turn", ContextID: task, Input: inputJSON, MaxSteps: 8})
	if err != nil {
		t.Fatal(err)
	}
	// Mutating deployment/bootstrap configuration after admission must not
	// change this run's complete durable snapshot on a cold worker.
	manifest.Tools[0].Description = "new deployment description"
	if _, err = reg.PutConfig(ctx, scope.Tenant, registry.DotAgentID, 1, json.RawMessage(`{"instructions":"next turns only","model_route":"fixture","max_steps":8,"policy":{"allowed_model_routes":["fixture"],"allowed_skills":[],"allowed_tools":[],"allowed_computer_policies":[]}}`)); err != nil {
		t.Fatal(err)
	}
	// A new runner receives no manifest file/latest pointer: the accepted turn
	// must restore the complete selected descriptor snapshot from State.
	cold := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: runtime, WorkerID: "cold-bridge-worker"})
	if err = cold.DispatchTurn(ctx, token, scope); err != nil {
		t.Fatal(err)
	}
	for {
		run, e := sc.GetRun(ctx, token, accepted.TurnID)
		if e != nil {
			t.Fatal(e)
		}
		if run.Status == "done" {
			break
		}
		if run.Status == "failed" {
			t.Fatal("native profile run failed")
		}
		select {
		case <-ctx.Done():
			t.Fatal("native profile did not finish")
		case <-time.After(10 * time.Millisecond):
		}
	}
	model.mu.Lock()
	modelCalls, modelReceipts, inputBytes := model.calls, len(model.receipts), model.inputBytes
	model.mu.Unlock()
	if modelCalls != 3 || actualMCP.Load() != 2 || modelReceipts != 2 || inputBytes != 100*1024 {
		t.Fatalf("wrong native path counts: model=%d MCP=%d receipts=%d bytes=%d", modelCalls, actualMCP.Load(), modelReceipts, inputBytes)
	}
	frames, err := sc.ReadStream(ctx, token, accepted.ThreadID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, frame := range frames {
		counts[frame.Kind]++
	}
	if counts["model"] != 3 || counts["tool"] != 2 || counts["done"] != 1 || counts["error"] != 0 {
		t.Fatalf("native durable frames differ: model=%d tool=%d done=%d error=%d", counts["model"], counts["tool"], counts["done"], counts["error"])
	}
	// Current gateway policy still denies a side effect, even for a verified
	// task allowed to call the Tools plane; no MCP call can be substituted.
	deniedCtx := core.WithRunBinding(core.WithRunScope(ctx, scope, token), core.RunBinding{TaskID: "deny-run", ContextID: task, OperationKey: "0"})
	if _, err = bridge.Execute(deniedCtx, "denied_write", json.RawMessage(`{"marker":"denied","payload":"ok","step":1}`)); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("Guardian default deny bypassed")
	}
	// A live grant without tools capability passes signature checks and is
	// rejected by the EXISTING owner authorizer, not a new local allow rule.
	limitedTask := "limited-" + contracts.NewID()
	if _, err = ids.CreateRoot(ctx, scope, identity.RootRequest{Task: limitedTask, Context: limitedTask, Deployment: scope.Deployment, Capabilities: []string{"state"}, TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	limitedToken, e := ids.MintTask(ctx, scope, limitedTask, signer, "ahp", 0)
	if e != nil {
		t.Fatal(e)
	}
	limitedCtx := core.WithRunBinding(core.WithRunScope(ctx, scope, limitedToken), core.RunBinding{TaskID: "limited-run", ContextID: limitedTask, OperationKey: "0"})
	if _, err = bridge.Execute(limitedCtx, "bench_echo", json.RawMessage(`{}`)); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("owner capability deny bypassed")
	}
	foreignScope := scope
	foreignScope.Subject = "bob"
	foreignTask := "foreign-" + contracts.NewID()
	if _, err = ids.CreateRoot(ctx, foreignScope, identity.RootRequest{Task: foreignTask, Context: foreignTask, Deployment: foreignScope.Deployment, Capabilities: []string{"tools"}, TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	foreignToken, e := ids.MintTask(ctx, foreignScope, foreignTask, signer, "ahp", 0)
	if e != nil {
		t.Fatal(e)
	}
	firstKeyMu.Lock()
	ownerKey := firstKey
	firstKeyMu.Unlock()
	if ownerKey == "" {
		t.Fatal("missing owner operation coordinate")
	}
	if err = bridge.post(ctx, foreignToken, "GetResult", map[string]string{"key": ownerKey}, new(tools.ResultView), false); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("foreign subject read owner tool result")
	}
	if actualMCP.Load() != 2 {
		t.Fatal("denied call reached MCP")
	}
	credentialSource.mu.Lock()
	for _, seen := range credentialSource.scopes {
		if seen != scope {
			t.Error("credential resolved for foreign scope")
		}
	}
	credentialSource.mu.Unlock()
	if strings.Contains(logs.String(), gatewayCredentialCanary) || strings.Contains(logs.String(), token) || strings.Contains(logs.String(), limitedToken) {
		t.Fatal("raw credential reached Core logs")
	}
}
