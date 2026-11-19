//go:build ahp_exec

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/compute/computeapi"
	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/internal/tools/toolsapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	a2asdk "github.com/ahp-platform/ahp/sdk/go/a2a"
	computesdk "github.com/ahp-platform/ahp/sdk/go/compute"
	modelsdk "github.com/ahp-platform/ahp/sdk/go/model"
	toolssdk "github.com/ahp-platform/ahp/sdk/go/tools"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// This is real PG + Identity + HTTP/mTLS + actual public SDK traffic. The model
// and physical compute are controlled doubles, explicitly not LLM/microVM proof.
type publicInference struct{ calls atomic.Int32 }

func (*publicInference) Model() string { return "sdk-fixture" }
func (p *publicInference) Stream(ctx context.Context, _ model.ChatRequest) (model.Stream, error) {
	p.calls.Add(1)
	return &publicModelStream{}, nil
}

type publicModelStream struct{ n int }

func (s *publicModelStream) Next(ctx context.Context) (model.Delta, error) {
	select {
	case <-ctx.Done():
		return model.Delta{}, ctx.Err()
	case <-time.After(15 * time.Millisecond):
	}
	s.n++
	switch s.n {
	case 1:
		return model.Delta{Text: "first "}, nil
	case 2:
		return model.Delta{Text: "second", FinishReason: "stop"}, nil
	}
	return model.Delta{}, model.ErrStreamDone
}
func (*publicModelStream) Usage() model.Usage {
	return model.Usage{PromptTokens: 2, CompletionTokens: 2, TotalTokens: 4}
}
func (*publicModelStream) Close() error { return nil }

type publicCredentials struct{}

func (publicCredentials) Resolve(context.Context, contracts.Scope, string) (tools.Credential, error) {
	return tools.Credential{}, nil
}

type publicUnknownMCP struct{ calls *atomic.Int32 }

func (p publicUnknownMCP) CallTool(context.Context, *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	p.calls.Add(1)
	return nil, io.ErrUnexpectedEOF
}
func (publicUnknownMCP) Close() error { return nil }

func TestPublicSDKOwnerAPIsRealPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" {
		t.Fatal("isolated real PostgreSQL required")
	}
	pool, e := pgxpool.New(ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	ids := identity.New(pool)
	if e = ids.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	sc := contracts.Scope{Tenant: "sdk-public-" + randHexTest(), Deployment: "agent", Subject: "alice"}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("sdk-public", "key", priv)
	verifier := auth.NewVerifier("sdk-public", map[string]ed25519.PublicKey{"key": pub})
	issue := func(scope contracts.Scope, task string) string {
		t.Helper()
		_, e := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: task, Context: scope.Tenant + ":" + scope.Subject + ":" + scope.Deployment + ":" + task, Deployment: scope.Deployment, Capabilities: []string{"model", "tools", "compute", "execute"}, TTL: time.Hour})
		if e != nil {
			t.Fatal(e)
		}
		token, e := ids.MintTask(ctx, scope, task, signer, "ahp", 10*time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		return token
	}
	token := issue(sc, "root")
	pyToken := issue(sc, "python")
	revoked := issue(sc, "revoked")
	if e = ids.CancelSubtree(ctx, sc, "revoked"); e != nil {
		t.Fatal(e)
	}
	inf := &publicInference{}
	bridge, e := execbridge.New(ctx, pool, execbridge.Config{Inference: inf})
	if e != nil {
		t.Fatal(e)
	}
	bridge.WithBudgetLineage(taskBudgetLineage(ids))
	var mcpCalls, unknownCalls atomic.Int32
	ms := mcp.NewServer(&mcp.Implementation{Name: "sdk-protocol-fixture", Version: "1"}, nil)
	ms.AddTool(&mcp.Tool{Name: "echo", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		mcpCalls.Add(1)
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "saved tool result"}}}, nil
	})
	msrv := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return ms }, &mcp.StreamableHTTPOptions{JSONResponse: true, DisableLocalhostProtection: true}))
	defer msrv.Close()
	tschema := "ahp_sdk_tools_" + randHexTest()
	guardian := tools.NewPolicyGuardian("sdk-fixture")
	toolsvc, e := tools.New(ctx, pool, tschema, tools.Config{Guardian: guardian, Credentials: publicCredentials{}, Connections: map[string]tools.Connector{"mcp": func(ctx context.Context, _ tools.Credential) (tools.MCPClient, error) {
		return connectMCPWithClient(ctx, msrv.URL, http.DefaultClient)
	}, "unknown": func(context.Context, tools.Credential) (tools.MCPClient, error) {
		return publicUnknownMCP{&unknownCalls}, nil
	}}, Tools: map[string]tools.ToolSpec{"echo": {Name: "echo", Connection: "mcp"}, "unknown": {Name: "unknown", Connection: "unknown", SideEffecting: true}}, Verifier: verifier, Audience: "ahp"})
	if e != nil {
		t.Fatal(e)
	}
	defer toolsvc.Drop(context.Background())
	if e = toolsvc.GrantOperatorPolicy(ctx, sc, "unknown", "unknown", "test-owner"); e != nil {
		t.Fatal(e)
	}
	th, e := toolsapi.NewHandler(toolsvc, verifier, ids.Authorize)
	if e != nil {
		t.Fatal(e)
	}
	cs := compute.New(pool)
	if e = cs.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	redis := compute.NewRedis(os.Getenv("AHP_REDIS_ADDR"))
	if e = redis.Ping(ctx); e != nil {
		t.Fatal("real Redis required", e)
	}
	leases, e := compute.NewLeaseService(cs, redis)
	if e != nil {
		t.Fatal(e)
	}
	registry := compute.NewRegistry(compute.NewMockBackend())
	registry.SetClassBackend(compute.ClassS, compute.MockBackendName)
	runner := compute.NewRunner(cs, registry, t.TempDir(), "sdk-mock", nil).WithLeaseService(leases)
	ch, e := computeapi.New(cs, verifier, ids.Authorize, runner)
	if e != nil {
		t.Fatal(e)
	}
	gschema := "ahp_sdk_a2a_" + randHexTest()
	gs, e := gateway.NewStore(pool, gschema)
	if e != nil {
		t.Fatal(e)
	}
	if e = gs.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	defer pool.Exec(context.Background(), "DROP SCHEMA "+gschema+" CASCADE")
	ledger, e := operations.New(pool, gschema)
	if e != nil {
		t.Fatal(e)
	}
	if e = ledger.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	gsrv, e := gateway.NewService(gs, ledger, gateway.ExecutorFunc(func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		if in.Task.Kind == "input" {
			return gateway.ExecResult{State: protocol.TaskStateInputRequired, Message: protocol.NewMessage(protocol.MessageRoleAgent, protocol.NewTextPart("need input"))}, nil
		}
		if in.Task.Kind == "block" {
			select {
			case <-ctx.Done():
				return gateway.ExecResult{}, ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
		return gateway.ExecResult{State: protocol.TaskStateCompleted, Message: protocol.NewMessage(protocol.MessageRoleAgent, protocol.NewTextPart("standalone completed"))}, nil
	}), gateway.NullNotifier{}, gateway.Config{Poll: 10 * time.Millisecond, MaxBlock: 3 * time.Second, TaskVerifier: verifier, TaskAudience: "ahp", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if e != nil {
		t.Fatal(e)
	}
	gh, e := gateway.NewServer(gateway.NewHandler(gsrv, gateway.HandlerConfig{}), gateway.ServerConfig{Public: gateway.AuthConfig{Verifier: verifier, Audience: "ahp"}, Internal: gateway.AuthConfig{Verifier: verifier, Audience: "ahp", Capability: "execute"}, TaskAuthorize: ids.Authorize})
	if e != nil {
		t.Fatal(e)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	go gsrv.Run(workerCtx)
	mux := http.NewServeMux()
	mux.Handle("/v1/model/", bridge.ModelAPI(verifier, ids.Authorize))
	mux.Handle("/v1/tools/", th)
	mux.Handle("/v1/compute/", ch)
	mux.Handle("/a2a/", gh)
	var loseResponse atomic.Bool
	front := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if loseResponse.CompareAndSwap(true, false) {
			buffer := httptest.NewRecorder()
			mux.ServeHTTP(buffer, r)
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		mux.ServeHTTP(w, r)
	})
	ca := newMountCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(front)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	defer srv.Close()
	clientFor := func(scope contracts.Scope) *http.Client {
		hc := *srv.Client()
		tr := hc.Transport.(*http.Transport).Clone()
		tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
		hc.Transport = tr
		return &hc
	}
	hc := clientFor(sc)
	current := token
	cfg := modelsdk.Config{BaseURL: srv.URL, HTTPClient: hc, TokenSource: func(context.Context) (string, error) { return current, nil }}
	mc, e := modelsdk.New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	tc, e := toolssdk.New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	ac, e := a2asdk.New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	cc, e := computesdk.New(computesdk.Config{BaseURL: srv.URL, HTTPClient: hc, TokenSource: computesdk.TokenSource(cfg.TokenSource)})
	if e != nil {
		t.Fatal(e)
	}
	var gen modelsdk.Generation
	var computer computesdk.Computer
	var taskID protocol.TaskID
	var lifecycle computesdk.Computer
	t.Run("ModelSubmitReplayAndStreamingRecovery", func(t *testing.T) {
		request := modelsdk.Request{Key: "model-replay", Messages: []modelsdk.Message{{Role: "user", Content: "hello"}}}
		var e error
		gen, e = mc.Submit(ctx, request)
		if e != nil || gen.Status != "COMPLETED" {
			t.Fatalf("submit status=%s err=%v", gen.Status, e)
		}
		prior := inf.calls.Load()
		same, e := mc.Submit(ctx, request)
		if e != nil || same.ID != gen.ID || inf.calls.Load() != prior {
			t.Fatal("model replay dispatched twice", e)
		}
		request.Messages[0].Content = "different"
		if _, e = mc.Submit(ctx, request); contracts.ReasonOf(e) != contracts.ErrKeyReuse {
			t.Fatal("changed payload key accepted", e)
		}
		streamReq := modelsdk.Request{Key: "stream", Messages: []modelsdk.Message{{Role: "user", Content: "stream"}}}
		var cursor int64
		for f, e := range mc.Stream(ctx, streamReq) {
			if e != nil {
				t.Fatal(e)
			}
			if f.Delta != nil {
				cursor = f.Delta.Sequence
				break
			}
		}
		if cursor != 1 {
			t.Fatal("first incremental delta absent")
		}
		streamReq.After = cursor
		terminal := false
		for f, e := range mc.Stream(ctx, streamReq) {
			if e != nil {
				t.Fatal(e)
			}
			if f.Delta != nil {
				if f.Delta.Sequence <= cursor {
					t.Fatal("duplicate replay delta")
				}
				cursor = f.Delta.Sequence
			}
			if f.Generation != nil {
				terminal = true
			}
		}
		if !terminal || cursor != 2 {
			t.Fatal("resume did not finish")
		}

		cancelReq := modelsdk.Request{Key: "cancel-model", Messages: []modelsdk.Message{{Role: "user", Content: "cancel"}}}
		generationID := ""
		for f, e := range mc.Stream(ctx, cancelReq) {
			if e != nil {
				t.Fatal(e)
			}
			if f.Type == "accepted" {
				generationID = f.GenerationID
				break
			}
		}
		stopped, e := mc.Cancel(ctx, generationID)
		if e != nil || (!stopped.CancelRequested && stopped.Status != "CANCELED") {
			t.Fatal("durable model cancel", e)
		}
		loseResponse.Store(true)
		request.Key = "lost-response"
		request.Messages[0].Content = "lost"
		_, e = mc.Submit(ctx, request)
		var uncertain *modelsdk.Error
		if !errors.As(e, &uncertain) || !uncertain.OutcomeUnknown {
			t.Fatal("lost response was not UNKNOWN", e)
		}
		prior = inf.calls.Load()
		if _, e = mc.Submit(ctx, request); e != nil || inf.calls.Load() != prior {
			t.Fatal("recovery duplicated inference", e)
		}
	})
	t.Run("ToolsJournalUnknownAndHumanDenial", func(t *testing.T) {
		loseResponse.Store(true)
		_, e := tc.Call(ctx, "tool-1", "echo", json.RawMessage(`{}`))
		var uncertain *toolssdk.Error
		if !errors.As(e, &uncertain) || !uncertain.OutcomeUnknown {
			t.Fatal("lost tool response not unknown", e)
		}
		op, e := tc.Get(ctx, "tool-1")
		if e != nil || op.Status != "SUCCEEDED" {
			t.Fatal("reconciliation failed", e)
		}
		before := mcpCalls.Load()
		if _, e = tc.Call(ctx, "tool-1", "echo", json.RawMessage(`{}`)); e != nil || mcpCalls.Load() != before {
			t.Fatal("tool replay duplicated", e)
		}
		op, e = tc.Call(ctx, "uncertain", "unknown", json.RawMessage(`{}`))
		if e != nil || op.Status != "SIDE_EFFECT_UNKNOWN" {
			t.Fatal("UNKNOWN flattened", op.Status, e)
		}
		if _, e = tc.Call(ctx, "uncertain", "unknown", json.RawMessage(`{}`)); e != nil || unknownCalls.Load() != 1 {
			t.Fatal("unknown effect retried", e)
		}
		if op, e = tc.Fence(ctx, "uncertain"); e != nil || op.Status != "SIDE_EFFECT_UNKNOWN" {
			t.Fatal("fence", e)
		}
		ap, _ := toolssdk.NewApprovalClient(cfg)
		if _, e = ap.Approvals(ctx, 10); e == nil {
			t.Fatal("agent read human approvals")
		}
	})
	// GetResult is the one route allowed to release content, and only to the
	// authority that actually paid for the effect. The result read must not
	// dispatch the tool a second time, must refuse rows that never settled, and
	// must not disclose that a key exists to a foreign workload.
	t.Run("ToolsResultReadUnderOwnerAuthority", func(t *testing.T) {
		mcpBefore := mcpCalls.Load()
		res, e := tc.ToolResult(ctx, "tool-1")
		if e != nil {
			t.Fatalf("owner result read refused: %v", e)
		}
		if res.Status != "SUCCEEDED" || res.Storage != toolssdk.StorageInline || res.Ref != nil {
			t.Fatalf("result shape: %+v", res)
		}
		if !strings.Contains(string(res.Payload), "saved tool result") {
			t.Fatalf("raw MCP result lost: %s", res.Payload)
		}
		if res.Provenance.Schema != "tools-result-v1" || res.Provenance.GrantTask == "" || res.Provenance.GrantContext == "" {
			t.Fatalf("provenance is not the dispatch grant: %+v", res.Provenance)
		}
		if mcpCalls.Load() != mcpBefore {
			t.Fatalf("result read re-dispatched the effect: %d -> %d", mcpBefore, mcpCalls.Load())
		}
		// An uncertain row has no result: CONFLICT is a decision, and reading
		// must never nudge it into a second effect.
		unknownBefore := unknownCalls.Load()
		if _, e = tc.ToolResult(ctx, "uncertain"); contracts.Code(e) != contracts.ErrConflict {
			t.Fatalf("uncertain result read = %q, want CONFLICT (%v)", contracts.Code(e), e)
		}
		if unknownCalls.Load() != unknownBefore {
			t.Fatal("reading an uncertain row re-ran it")
		}
		if _, e = tc.ToolResult(ctx, "never-dispatched"); contracts.Code(e) != contracts.ErrNotFound {
			t.Fatalf("absent key = %q, want NOT_FOUND", contracts.Code(e))
		}
		// Foreign workload, same face: the answer is NOT_FOUND, not FORBIDDEN,
		// so a probe cannot learn which keys exist in another scope.
		foreign := contracts.Scope{Tenant: sc.Tenant + "-foreign", Deployment: sc.Deployment, Subject: "foreigner"}
		foreignToken := issue(foreign, "foreign-result")
		foreignClient := clientFor(foreign)
		foreignTools, e := toolssdk.New(toolssdk.Config{BaseURL: srv.URL, HTTPClient: foreignClient, TokenSource: func(context.Context) (string, error) { return foreignToken, nil }})
		if e != nil {
			t.Fatal(e)
		}
		if _, e = foreignTools.ToolResult(ctx, "tool-1"); contracts.Code(e) != contracts.ErrNotFound {
			t.Fatalf("foreign result read = %q, want NOT_FOUND", contracts.Code(e))
		}
	})
	t.Run("ComputeLedgerFenceAndAttach", func(t *testing.T) {
		var e error
		computer, e = cc.CreateComputer(ctx, "S", "personal", "", "", "computer-1", nil)
		if e != nil {
			t.Fatal(e)
		}
		same, e := cc.CreateComputer(ctx, "S", "personal", "", "", "computer-1", nil)
		if e != nil || same.ID != computer.ID {
			t.Fatal("computer replay", e)
		}
		if _, e = cc.AcquireIncarnation(ctx, computer.ID, "mock", "sdk-node", ""); e != nil {
			t.Fatal(e)
		}
		// This fixture plane is bound to the MOCK backend for class S, so the honest
		// record-time answer is "recorded, and this plane will not dispatch it" -
		// DEF-COMPUTE-5. Before that fix the plane promised dispatch and RunCommand
		// refused later, which is the sequence a caller cannot recover from. The ledger
		// flow this subtest is actually about (idempotent key, fence, attach) is unchanged:
		// the row is still accepted and a plane holding a real substrate may still claim it.
		command, dispatch, reason, e := cc.ExecWithReason(ctx, computer.ID, "command-1", "echo fixture", "")
		if e != nil {
			t.Fatal("exec", e)
		}
		if dispatch {
			t.Fatal("a plane whose backend has no execution door offered the command for dispatch")
		}
		if !strings.Contains(reason, compute.MockBackendName) || !strings.Contains(reason, "no execution door") {
			t.Fatalf("the refusal does not name the backend and the missing door: %q", reason)
		}
		if _, e = cc.MarkDispatched(ctx, computer.ID, command.ID); e != nil {
			t.Fatal(e)
		}
		f, e := cc.Fence(ctx, computer.ID, command.ID)
		if e != nil || f.Status != computesdk.CmdSideEffectUnknown {
			t.Fatal("fence", f.Status, e)
		}
		n := 0
		for v, e := range cc.AttachCommand(ctx, computer.ID, command.ID, time.Millisecond) {
			if e != nil || v.Status != computesdk.CmdSideEffectUnknown {
				t.Fatal("attach flattened unknown", e)
			}
			n++
		}
		if n != 1 {
			t.Fatal("attach did not finish on unknown")
		}
		if _, _, e = cc.Exec(ctx, computer.ID, "command-1", "different", ""); contracts.ReasonOf(e) != contracts.ErrKeyReuse {
			t.Fatal("compute key mismatch accepted", e)
		}
		if _, e = cc.EnsureEnvironment(ctx, computer.ID, "python=3"); e != nil {
			t.Fatal(e)
		}
		if _, e = cc.Export(ctx, computer.ID, "/tmp/result", "export-1"); e == nil {
			t.Fatal("unconfigured export fabricated artifact")
		} else {
			var apiErr *computesdk.Error
			if !errors.As(e, &apiErr) || apiErr.Status != 403 || apiErr.Code != contracts.ErrForbidden {
				t.Fatalf("expected missing artifacts capability, got %v", e)
			}
			// Configured positive is TestComputeExportRealOwners with both public SDKs.
		}
	})

	t.Run("ComputeLifecycleRealRedisMockPhysical", func(t *testing.T) {
		var e error
		lifecycle, e = cc.CreateComputer(ctx, "S", "personal", "", "lifecycle", "lifecycle", nil)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = cc.Wake(ctx, lifecycle.ID); e != nil {
			t.Fatal(e)
		}
		if e = os.MkdirAll(runner.HomeDir(lifecycle.ID), 0700); e != nil {
			t.Fatal(e)
		}
		patch, e := cc.TypedPatch(ctx, computesdk.PatchRequest{ComputerID: lifecycle.ID, Path: "hello.txt", ContentB64: base64.StdEncoding.EncodeToString([]byte("exact content")), Mode: 0600})
		if e != nil || patch.Tree["/hello.txt"].Size != 13 {
			t.Fatal("typed patch", e)
		}
		if _, e = cc.TypedPatch(ctx, computesdk.PatchRequest{ComputerID: lifecycle.ID, Path: "hello.txt", ExpectedHash: "stale", ContentB64: "eA=="}); contracts.Code(e) != "STALE_HASH" {
			t.Fatal("stale patch accepted", e)
		}
		if _, e = cc.StateHead(ctx, computesdk.BranchRequest{ComputerID: lifecycle.ID}); e != nil {
			t.Fatal(e)
		}
		if rows, e := cc.StateRevisions(ctx, computesdk.BranchRequest{ComputerID: lifecycle.ID}); e != nil || len(rows) == 0 {
			t.Fatal("state revisions", e)
		}
		lease := computesdk.ActivityLeaseRequest{ComputerID: lifecycle.ID, Kind: "browser", Holder: "sdk", TTLms: 10000}
		if _, e = cc.AcquireActivityLease(ctx, lease); e != nil {
			t.Fatal(e)
		}
		if rows, e := cc.ActivityLeases(ctx, lifecycle.ID); e != nil || rows.Count != 1 {
			t.Fatal("activity leases", e)
		}
		if _, e = cc.Sleep(ctx, lifecycle.ID); contracts.Code(e) != contracts.ErrConflict {
			t.Fatal("sleep ignored live lease", e)
		}
		if _, e = cc.ReleaseActivityLease(ctx, lease); e != nil {
			t.Fatal(e)
		}
		if report, e := cc.Sleep(ctx, lifecycle.ID); e != nil || report.Status != "suspended" {
			t.Fatal("sleep", e)
		}
		if _, e = cc.Wake(ctx, lifecycle.ID); e != nil {
			t.Fatal(e)
		}
		if cost, e := cc.CostReport(ctx, lifecycle.ID); e != nil || !cost.Open {
			t.Fatal("cost", e)
		}
		install, e := cc.BeginInstallDetails(ctx, lifecycle.ID, "sdk-env-v1", "installer")
		if e != nil || install.Revision.ID == "" || install.EnvDir == "" {
			t.Fatal("full install response", e)
		}
		result, e := cc.RunInstall(ctx, computesdk.InstallCommandRequest{ComputerID: lifecycle.ID, RevisionID: install.Revision.ID, Command: "printf sdk-env > installed.txt"})
		if e != nil || result.ExitCode != 0 {
			t.Fatal("install process", e)
		}
		if _, e = cc.FinishInstall(ctx, lifecycle.ID, install.Revision.ID, "installer", true, ""); e != nil {
			t.Fatal(e)
		}
		if files, e := cc.ListEnvironment(ctx, lifecycle.ID); e != nil || len(files.Files) == 0 {
			t.Fatal("environment files", e)
		}
	})
	t.Run("A2ADelegateReplayGetResubscribe", func(t *testing.T) {
		req := &protocol.SendMessageRequest{Message: protocol.NewMessage(protocol.MessageRoleUser, protocol.NewTextPart("standalone"))}
		req.Message.ID = "delegate-1"
		result, e := ac.Delegate(ctx, "root", 2, req)
		if e != nil {
			t.Fatal(e)
		}
		task, ok := result.(*protocol.Task)
		if !ok {
			t.Fatal("expected durable task")
		}
		taskID = task.ID
		same, e := ac.Delegate(ctx, "root", 2, req)
		if e != nil || same.TaskInfo().TaskID != taskID {
			t.Fatal("delegation replay", e)
		}
		got, e := ac.Get(ctx, taskID)
		if e != nil || got.ID != taskID || got.Metadata[gateway.MetaParentTask] != "root" {
			t.Fatal("get delegated task", e)
		}
		n := 0
		for event, e := range ac.Resubscribe(ctx, taskID) {
			if e != nil {
				t.Fatal(e)
			}
			if event.TaskInfo().TaskID != taskID {
				t.Fatal("foreign stream task")
			}
			n++
		}
		if n == 0 {
			t.Fatal("empty replay")
		}
	})

	t.Run("A2AInputRequiredInteraction", func(t *testing.T) {
		req := &protocol.SendMessageRequest{Message: protocol.NewMessage(protocol.MessageRoleUser, protocol.NewTextPart("ask input")), Metadata: map[string]any{"ahp_task_kind": "input"}}
		req.Message.ID = "hitl-send"
		var id protocol.TaskID
		last := protocol.TaskStateUnspecified
		for event, err := range ac.SendStream(ctx, req) {
			if err != nil {
				t.Fatal(err)
			}
			id = event.TaskInfo().TaskID
			switch ev := event.(type) {
			case *protocol.Task:
				last = ev.Status.State
			case *protocol.TaskStatusUpdateEvent:
				last = ev.Status.State
			}
		}
		if last != protocol.TaskStateInputRequired {
			t.Fatal("missing input boundary", last)
		}
		got, err := ac.Get(ctx, id)
		if err != nil || got.Status.State != protocol.TaskStateInputRequired {
			t.Fatal("durable HITL", err)
		}
		n := 0
		for _, err := range ac.Resubscribe(ctx, id) {
			if err != nil {
				t.Fatal(err)
			}
			n++
		}
		if n != 1 {
			t.Fatal("HITL snapshot boundary", n)
		}
	})
	t.Run("A2AStreamingAndCooperativeCancel", func(t *testing.T) {
		req := &protocol.SendMessageRequest{Message: protocol.NewMessage(protocol.MessageRoleUser, protocol.NewTextPart("stream"))}
		req.Message.ID = "stream-send"
		n := 0
		for _, e := range ac.SendStream(ctx, req) {
			if e != nil {
				t.Fatal(e)
			}
			n++
		}
		if n == 0 {
			t.Fatal("empty A2A SendStream")
		}
		req.Message.ID = "cancel-send"
		req.Config = &protocol.SendMessageConfig{ReturnImmediately: true}
		req.Metadata = map[string]any{"ahp_task_kind": "block"}
		result, e := ac.Send(ctx, req)
		if e != nil {
			t.Fatal(e)
		}
		id := result.TaskInfo().TaskID
		if _, e = ac.Cancel(ctx, id); e != nil {
			t.Fatal(e)
		}
		last := protocol.TaskStateUnspecified
		for ev, e := range ac.Resubscribe(ctx, id) {
			if e != nil {
				t.Fatal(e)
			}
			switch v := ev.(type) {
			case *protocol.Task:
				last = v.Status.State
			case *protocol.TaskStatusUpdateEvent:
				last = v.Status.State
			}
		}
		if last != protocol.TaskStateCanceled {
			t.Fatal("cancel did not settle", last)
		}
	})
	// Both a matching foreign certificate and a mismatched certificate are exercised.
	config := map[string]string{"base": srv.URL, "token": pyToken, "revoked": revoked, "go_model": gen.ID, "go_computer": computer.ID, "go_task": string(taskID), "go_lifecycle": lifecycle.ID}
	certs := map[string]tls.Certificate{"client": hc.Transport.(*http.Transport).TLSClientConfig.Certificates[0]}
	for _, dimension := range []string{"subject", "tenant", "deployment"} {
		other := sc
		switch dimension {
		case "subject":
			other.Subject = "bob"
		case "tenant":
			other.Tenant += "-other"
		case "deployment":
			other.Deployment = "other"
		}
		ot := issue(other, "foreign")
		config[dimension] = ot
		oh := clientFor(other)
		certs[dimension] = oh.Transport.(*http.Transport).TLSClientConfig.Certificates[0]
		t.Run("Authority_"+dimension, func(t *testing.T) {
			ocfg := modelsdk.Config{BaseURL: srv.URL, HTTPClient: oh, TokenSource: func(context.Context) (string, error) { return ot, nil }}
			om, _ := modelsdk.New(ocfg)
			otool, _ := toolssdk.New(ocfg)
			oa, _ := a2asdk.New(ocfg)
			ocompute, _ := computesdk.New(computesdk.Config{BaseURL: srv.URL, HTTPClient: oh, TokenSource: computesdk.TokenSource(ocfg.TokenSource)})
			if _, e = om.Get(ctx, gen.ID); e == nil {
				t.Fatal("foreign model read")
			}
			if _, e = otool.Get(ctx, "tool-1"); e == nil {
				t.Fatal("foreign tool read")
			}
			if _, e = ocompute.GetComputer(ctx, computer.ID); e == nil {
				t.Fatal("foreign computer read")
			}
			if _, e = oa.Get(ctx, taskID); e == nil {
				t.Fatal("foreign A2A read")
			}
		})
	}
	t.Run("PythonActualOwnerAPI", func(t *testing.T) { runPublicPython(t, ctx, srv, config, certs) })
	t.Run("CurrentGrantRevocation", func(t *testing.T) {
		if e = ids.CancelSubtree(ctx, sc, "root"); e != nil {
			t.Fatal(e)
		}
		if _, e = mc.Get(ctx, gen.ID); contracts.Code(e) != contracts.ErrForbidden {
			t.Fatal("revoked model", e)
		}
		if _, e = tc.Get(ctx, "tool-1"); contracts.Code(e) != contracts.ErrForbidden {
			t.Fatal("revoked tools", e)
		}
		if _, e = cc.GetComputer(ctx, computer.ID); contracts.Code(e) != contracts.ErrForbidden {
			t.Fatal("revoked compute", e)
		}
		if _, e = ac.Get(ctx, taskID); contracts.Code(e) != contracts.ErrForbidden {
			t.Fatal("revoked A2A", e)
		}
	})
	t.Log("real PostgreSQL/Identity/mTLS owner APIs; model inference and physical VM explicitly fixtures; Tools result read measured inline only -- artifact spill sink is a fixture in the owner package, not this composition")
}

func runPublicPython(t *testing.T, ctx context.Context, srv *httptest.Server, config map[string]string, certs map[string]tls.Certificate) {
	t.Helper()
	dir := t.TempDir()
	write := func(name string, value []byte) string {
		p := filepath.Join(dir, name)
		if e := os.WriteFile(p, value, 0600); e != nil {
			t.Fatal(e)
		}
		return p
	}
	config["ca"] = write("server.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	for name, cert := range certs {
		key, e := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
		if e != nil {
			t.Fatal(e)
		}
		config[name+"_cert"] = write(name+".pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}))
		config[name+"_key"] = write(name+".key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	}
	raw, _ := json.Marshal(config)
	path := write("config.json", raw)
	root, _ := filepath.Abs("../..")
	cmd := exec.CommandContext(ctx, "python3", "sdk/python/ahp_clients/tests/live_contract.py", path)
	cmd.Dir = root
	pythonPath := filepath.Join(root, "sdk/python")
	if extra := os.Getenv("PYTHONPATH"); extra != "" {
		pythonPath += string(os.PathListSeparator) + extra
	}
	cmd.Env = append(os.Environ(), "PYTHONPATH="+pythonPath)
	out, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("Python SDK contract: %v\n%s", e, out)
	}
	t.Log(strings.TrimSpace(string(out)))
}
