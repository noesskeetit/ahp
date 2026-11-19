package tools

import (
	"context"
	"crypto/ed25519"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := crand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// mcpWrap adapts an in-memory MCP client+server session pair to MCPClient.
type mcpWrap struct {
	cs *mcp.ClientSession
	ss *mcp.ServerSession
}

func (w *mcpWrap) CallTool(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	return w.cs.CallTool(ctx, params)
}

func (w *mcpWrap) Close() error {
	_ = w.cs.Close()
	_ = w.ss.Close()
	return nil
}

type fakeCreds struct{}

func (fakeCreds) Resolve(context.Context, contracts.Scope, string) (Credential, error) {
	return Credential{Bearer: "test"}, nil
}

type env struct {
	svc      *Service
	scope    contracts.Scope
	schema   string
	g        *PolicyGuardian
	effCount *int32
	humanTok string
	taskTok  string
}

func newTestService(t *testing.T) (*env, context.Context) {
	t.Helper()
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(pool.Close)

	sfx := randHex(6)
	schema := "ahp_tools_t" + sfx
	scope := contracts.Scope{Tenant: "t" + sfx, Deployment: "d" + sfx, Subject: "u" + sfx}
	audience := "ahp-tools-test"

	// Token issuer/verifier for the human approval gate.
	pub, priv, err := ed25519.GenerateKey(crand.Reader)
	if err != nil {
		t.Fatalf("ed25519: %v", err)
	}
	signer, err := auth.NewSigner("iss", "k1", priv)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	verifier := auth.NewVerifier("iss", map[string]ed25519.PublicKey{"k1": pub})
	humanTok, err := signer.Sign(auth.Claims{
		Kind:     auth.KindHuman,
		Subject:  scope.Subject,
		Tenant:   scope.Tenant,
		Audience: audience,
	}, time.Hour)
	if err != nil {
		t.Fatalf("sign human: %v", err)
	}
	taskTok, err := signer.Sign(auth.Claims{
		Kind:       auth.KindTask,
		Task:       "task-1",
		Root:       "task-1",
		Context:    "ctx-1",
		Grant:      "grant-1",
		Subject:    scope.Subject,
		Tenant:     scope.Tenant,
		Deployment: scope.Deployment,
		Audience:   audience,
	}, 5*time.Minute)
	if err != nil {
		t.Fatalf("sign task: %v", err)
	}

	srv := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	var effCount int32

	srv.AddTool(&mcp.Tool{Name: "echo", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil
		})
	srv.AddTool(&mcp.Tool{Name: "write", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			atomic.AddInt32(&effCount, 1)
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "written"}}}, nil
		})
	srv.AddTool(&mcp.Tool{Name: "errwrite", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			atomic.AddInt32(&effCount, 1)
			return nil, errors.New("boom")
		})
	srv.AddTool(&mcp.Tool{Name: "gated", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "gated-ok"}}}, nil
		})

	connector := func(ctx context.Context, _ Credential) (MCPClient, error) {
		ct, st := mcp.NewInMemoryTransports()
		ss, err := srv.Connect(ctx, st, nil)
		if err != nil {
			return nil, err
		}
		cs, err := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "1"}, nil).Connect(ctx, ct, nil)
		if err != nil {
			_ = ss.Close()
			return nil, err
		}
		return &mcpWrap{cs: cs, ss: ss}, nil
	}

	g := NewPolicyGuardian("test")
	svc, err := New(ctx, pool, schema, Config{
		Guardian:    g,
		Credentials: fakeCreds{},
		Connections: map[string]Connector{"c": connector},
		Tools: map[string]ToolSpec{
			"echo":     {Name: "echo", Connection: "c"},
			"write":    {Name: "write", Connection: "c", SideEffecting: true},
			"errwrite": {Name: "errwrite", Connection: "c", SideEffecting: true},
			"gated":    {Name: "gated", Connection: "c", RequiresApproval: true},
		},
		Verifier: verifier,
		Audience: audience,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		_, _ = pool.Exec(dctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})
	return &env{svc: svc, scope: scope, schema: schema, g: g, effCount: &effCount,
		humanTok: humanTok, taskTok: taskTok}, ctx
}

// (a) Human-approval gate: a task token cannot approve; a missing approval blocks
// the call before any effect; a human approval for the canonical digest unblocks
// a subsequent call.
func TestApprovalGateOnlyHuman(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"v":1}`)
	gatedDig, err := ApprovalDigest("gated", args)
	if err != nil {
		t.Fatalf("ApprovalDigest: %v", err)
	}
	if _, err := e.svc.Approve(ctx, e.scope, gatedDig, "gated", e.taskTok); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("task-token approve: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrForbidden)
	}
	if _, err := e.svc.Call(ctx, e.scope, "k-gated-1", "gated", args); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("call without approval: err=%v, want ErrApprovalRequired", err)
	}
	requestID, dg2, err := e.svc.CreateApprovalRequest(ctx, e.scope, "k-gated-2", "gated", args)
	if err != nil {
		t.Fatalf("CreateApprovalRequest: %v", err)
	}
	if dg2 != gatedDig {
		t.Fatalf("digest mismatch: %q vs %q", dg2, gatedDig)
	}
	if _, err := e.svc.ApproveRequest(ctx, e.scope, requestID, dg2, "gated", e.humanTok); err != nil {
		t.Fatalf("human approve: %v", err)
	}
	op, err := e.svc.Call(ctx, e.scope, "k-gated-2", "gated", args)
	if err != nil || op.Status != operations.StatusSucceeded {
		t.Fatalf("call after approval: op=%+v err=%v, want SUCCEEDED", op, err)
	}
}

// (b) Guardian default-deny: a side-effecting tool is denied until the Guardian
// explicitly allows its canonical digest; the same key then succeeds.
func TestGuardianDefaultDeny(t *testing.T) {
	e, ctx := newTestService(t)
	argsW := json.RawMessage(`{"v":1}`)
	if _, err := e.svc.Call(ctx, e.scope, "k-w-1", "write", argsW); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("unguarded side-effect: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrForbidden)
	}
	digW, err := ApprovalDigest("write", argsW)
	if err != nil {
		t.Fatalf("ApprovalDigest: %v", err)
	}
	e.g.AllowDigest(digW)
	op, err := e.svc.Call(ctx, e.scope, "k-w-1", "write", argsW)
	if err != nil || op.Status != operations.StatusSucceeded {
		t.Fatalf("allowed side-effect: op=%+v err=%v, want SUCCEEDED", op, err)
	}
}

// (c) Sealed replay: a second call under the same key for a succeeded,
// non-side-effecting operation returns the recorded success without re-running.
func TestSealedReplay(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"v":1}`)
	op1, err := e.svc.Call(ctx, e.scope, "k-e", "echo", args)
	if err != nil || op1.Status != operations.StatusSucceeded {
		t.Fatalf("first echo: op=%+v err=%v, want SUCCEEDED", op1, err)
	}
	op2, err := e.svc.Call(ctx, e.scope, "k-e", "echo", args)
	if err != nil || op2.Status != operations.StatusSucceeded {
		t.Fatalf("replayed echo: op=%+v err=%v, want SUCCEEDED", op2, err)
	}
}

// (c2) Key-reuse: reusing an operation key for a different side-effecting
// payload is rejected with KEY_REUSE_MISMATCH, not silently replayed.
func TestKeyReuse(t *testing.T) {
	e, ctx := newTestService(t)
	argsA := json.RawMessage(`{"v":1}`)
	argsB := json.RawMessage(`{"v":2}`)
	digA, err := ApprovalDigest("errwrite", argsA)
	if err != nil {
		t.Fatalf("digest A: %v", err)
	}
	digB, err := ApprovalDigest("errwrite", argsB)
	if err != nil {
		t.Fatalf("digest B: %v", err)
	}
	e.g.AllowDigest(digA)
	e.g.AllowDigest(digB)
	if _, err := e.svc.Call(ctx, e.scope, "k-re", "errwrite", argsA); err == nil {
		t.Fatalf("first errwrite should return an ambiguous-error")
	}
	if _, err := e.svc.Call(ctx, e.scope, "k-re", "errwrite", argsB); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("key reuse: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrKeyReuse)
	}
}

// (d) Ambiguous side-effect: an erroring side-effecting call marks the
// operation SIDE_EFFECT_UNKNOWN, runs the effect exactly once, and a retry
// never re-dispatches the effect.
func TestAmbiguousNoReplay(t *testing.T) {
	e, ctx := newTestService(t)
	atomic.StoreInt32(e.effCount, 0)
	args := json.RawMessage(`{"v":1}`)
	dig, err := ApprovalDigest("errwrite", args)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	e.g.AllowDigest(dig)
	op, err := e.svc.Call(ctx, e.scope, "k-d", "errwrite", args)
	if err == nil {
		t.Fatalf("first errwrite: expected error")
	}
	if op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("first errwrite status=%q, want %q", op.Status, operations.StatusSideEffectUnknown)
	}
	if n := atomic.LoadInt32(e.effCount); n != 1 {
		t.Fatalf("effect ran %d times, want 1", n)
	}
	op2, err2 := e.svc.Call(ctx, e.scope, "k-d", "errwrite", args)
	if err2 != nil {
		t.Fatalf("replay of unknown must not error: %v", err2)
	}
	if op2.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("replay status=%q, want %q", op2.Status, operations.StatusSideEffectUnknown)
	}
	if n := atomic.LoadInt32(e.effCount); n != 1 {
		t.Fatalf("replay ran effect again: count=%d, want 1", n)
	}
}

// (e) OAuth boundary: a provider host outside the allow-list is refused at
// construction; a mismatched audience is refused before any network call.
func TestOAuthBoundaryNoNetwork(t *testing.T) {
	ctx := context.Background()
	if _, err := NewOAuthAdapter(ProviderBoundary{
		TokenURL: "https://bad.example/token",
		Policy:   netpolicy.Policy{AllowedHosts: []string{"good.example"}, AllowedMethods: []string{"POST"}},
	}); !errors.Is(err, netpolicy.ErrHostNotAllowed) {
		t.Fatalf("bad host: err=%v, want ErrHostNotAllowed", err)
	}
	ad, err := NewOAuthAdapter(ProviderBoundary{
		TokenURL: "https://good.example/token",
		Audience: "A",
		Policy:   netpolicy.Policy{AllowedHosts: []string{"good.example"}, AllowedMethods: []string{"POST"}},
	})
	if err != nil {
		t.Fatalf("good host: %v", err)
	}
	if _, err := ad.Exchange(ctx, ExchangeRequest{SubjectToken: "x", Audience: "WRONG"}); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("audience mismatch: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrForbidden)
	}
}

// (f) Tenant isolation: an operation created under one scope is not visible
// under a different tenant, even with the same key and deployment.
func TestTenantIsolation(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"v":1}`)
	if _, err := e.svc.Call(ctx, e.scope, "k-f", "echo", args); err != nil {
		t.Fatalf("seed call: %v", err)
	}
	other := contracts.Scope{Tenant: e.scope.Tenant + "-other", Deployment: e.scope.Deployment, Subject: "u2"}
	if _, err := e.svc.Get(ctx, other, "k-f"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant get: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrNotFound)
	}
}

// (DEF-TGW-1) A side-effecting tool is default-denied on the durable path and
// becomes executable ONLY through durable evidence: a human approval for its
// exact canonical digest (the same approvals table Service.Approve binds) or an
// operator (tenant,connection,tool) grant. Both are audited; neither is process
// memory; removing either returns the tool to default-deny.
func TestSideEffectNeedsApprovalOrPolicy(t *testing.T) {
	e, ctx := newTestService(t)

	// 1) No approval, no policy -> guardian default-deny, effect not produced.
	argsA := json.RawMessage(`{"v":"ap"}`)
	digA, _ := ApprovalDigest("write", argsA)
	before := atomic.LoadInt32(e.effCount)
	if _, err := e.svc.Call(ctx, e.scope, "k-se-noauth", "write", argsA); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("side-effect without evidence: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrForbidden)
	}
	if after := atomic.LoadInt32(e.effCount); after != before {
		t.Fatalf("anti-vacuum: side effect ran despite default-deny (%d->%d)", before, after)
	}

	// 2) Human approval for the exact digest -> guardian allows via approvals.
	if _, _, err := e.svc.CreateApprovalRequest(ctx, e.scope, "k-se-appr", "write", argsA); err != nil {
		t.Fatalf("CreateApprovalRequest: %v", err)
	}
	if _, err := e.svc.Approve(ctx, e.scope, digA, "write", e.humanTok); err != nil {
		t.Fatalf("human approve: %v", err)
	}
	if op, err := e.svc.Call(ctx, e.scope, "k-se-appr", "write", argsA); err != nil || op.Status != operations.StatusSucceeded {
		t.Fatalf("side-effect after approval: op=%+v err=%v, want SUCCEEDED", op, err)
	}
	if after := atomic.LoadInt32(e.effCount); after != before+1 {
		t.Fatalf("approved side effect count=%d, want %d", after, before+1)
	}

	// 3) Operator grant at (tenant,connection,tool) -> guardian allows a different
	//    digest with no approval of its own.
	argsP := json.RawMessage(`{"v":"policy"}`)
	digP, _ := ApprovalDigest("write", argsP)
	if approved, err := newDBPolicySource(e.svc.pool, e.schema).ApprovalExists(ctx, e.scope, digP, "write"); err != nil || approved {
		t.Fatalf("precondition: digP should have no approval (approved=%v err=%v)", approved, err)
	}
	if err := e.svc.GrantOperatorPolicy(ctx, e.scope, "c", "write", "operator-ada"); err != nil {
		t.Fatalf("GrantOperatorPolicy: %v", err)
	}
	if op, err := e.svc.Call(ctx, e.scope, "k-se-pol", "write", argsP); err != nil || op.Status != operations.StatusSucceeded {
		t.Fatalf("side-effect after operator grant: op=%+v err=%v, want SUCCEEDED", op, err)
	}

	// 4) Revoke + a fresh digest -> back to default-deny (no weakening left behind).
	if err := e.svc.RevokeOperatorPolicy(ctx, e.scope, "c", "write", "operator-ada"); err != nil {
		t.Fatalf("RevokeOperatorPolicy: %v", err)
	}
	argsD := json.RawMessage(`{"v":"after-revoke"}`)
	if _, err := e.svc.Call(ctx, e.scope, "k-se-after", "write", argsD); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("after revoke: code=%q err=%v, want %q (grant must not linger)", contracts.Code(err), err, contracts.ErrForbidden)
	}
}

// A consumed one-shot approval is not permission for a second dispatch. It
// still permits reading the same durable UNKNOWN record under the same key.
func TestApprovedAmbiguousCallReturnsItsDurableUnknownOnReplay(t *testing.T) {
	e, ctx := newTestService(t)
	spec := e.svc.cfg.Tools["errwrite"]
	spec.RequiresApproval = true
	e.svc.cfg.Tools["errwrite"] = spec
	args := json.RawMessage(`{"v":7}`)
	id, digest, err := e.svc.CreateApprovalRequest(ctx, e.scope, "approved-unknown", "errwrite", args)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.svc.ApproveRequest(ctx, e.scope, id, digest, "errwrite", e.humanTok); err != nil {
		t.Fatal(err)
	}
	first, err := e.svc.Call(ctx, e.scope, "approved-unknown", "errwrite", args)
	if err == nil || first.Status != operations.StatusSideEffectUnknown {
		t.Fatal("ambiguous dispatch not journalled", err, first.Status)
	}
	again, err := e.svc.Call(ctx, e.scope, "approved-unknown", "errwrite", args)
	if err != nil || again.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("durable UNKNOWN hidden by consumed approval: %s, %v", again.Status, err)
	}
	if n := atomic.LoadInt32(e.effCount); n != 1 {
		t.Fatalf("UNKNOWN replay dispatched %d effects", n)
	}
}
