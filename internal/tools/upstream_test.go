package tools

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// (U1) The transport bound stops an oversized upstream response while it is
// still on the wire: the read fails and the gateway never buffers the whole
// body.
func TestUpstreamLimiterRefusesOversizedResponse(t *testing.T) {
	const total = 4 << 20
	var served int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		chunk := strings.Repeat("z", 64<<10)
		for served < total {
			n, err := w.Write([]byte(chunk))
			served += n
			if err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}))
	defer srv.Close()

	const max = 1 << 20
	client := LimitUpstream(&http.Client{Timeout: 20 * time.Second}, max)
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 32<<10)
	var got int64
	for {
		n, rerr := resp.Body.Read(buf)
		got += int64(n)
		if rerr != nil {
			if !errors.Is(rerr, ErrUpstreamTooLarge) {
				t.Fatalf("read error = %v, want ErrUpstreamTooLarge", rerr)
			}
			break
		}
		if got > 8*max {
			t.Fatalf("bound did not hold: read %d bytes with a %d byte cap", got, max)
		}
	}
	if got < max/2 || got > 2*max {
		t.Fatalf("bytes read before refusal = %d, want close to the %d byte cap", got, max)
	}
}

// (U2) Ordinary responses pass through untouched.
func TestUpstreamLimiterPassesSmallResponse(t *testing.T) {
	body := strings.Repeat("q", 64<<10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()
	client := LimitUpstream(&http.Client{Timeout: 20 * time.Second}, 1<<20)
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil || string(raw) != body {
		t.Fatalf("read = %d bytes err=%v, want the full body", len(raw), err)
	}
}

// (U3) Real MCP dispatch over loopback HTTP against a FAKE provider whose tool
// answers 2 MiB: with a 256 KiB ingress bound the call fails, the ledger holds
// no partial body, and nothing is truncated.
func TestCallRefusesOversizedUpstreamOverHTTP(t *testing.T) {
	mustDSN(t)
	pool := openPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var served int32
	srv := mcp.NewServer(&mcp.Implementation{Name: "big-fixture", Version: "1"}, nil)
	srv.AddTool(&mcp.Tool{Name: "big", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			atomic.AddInt32(&served, 1)
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: strings.Repeat("B", 2<<20)}}}, nil
		})
	ts := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv },
		&mcp.StreamableHTTPOptions{JSONResponse: true}))
	defer ts.Close()

	schema := "ahp_tools_u" + randHex(6)
	scope := contracts.Scope{Tenant: "tu" + randHex(5), Deployment: "du" + randHex(5), Subject: "uu" + randHex(5)}
	g := NewPolicyGuardian("test")
	connector := func(ctx context.Context, _ Credential) (MCPClient, error) {
		client := LimitUpstream(&http.Client{Timeout: 30 * time.Second}, 256<<10)
		cs, err := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "1"}, nil).Connect(ctx,
			&mcp.StreamableClientTransport{Endpoint: ts.URL, HTTPClient: client, DisableStandaloneSSE: true}, nil)
		if err != nil {
			return nil, err
		}
		return httpSession{cs}, nil
	}
	svc, err := New(ctx, pool, schema, Config{
		Guardian: g, Credentials: fakeCreds{},
		Connections: map[string]Connector{"c": connector},
		Tools:       map[string]ToolSpec{"big": {Name: "big", Connection: "c"}},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		_, _ = pool.Exec(dctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})

	_, err = svc.Call(ctx, scope, "k-u3", "big", json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("an oversized upstream answer must fail the call")
	}
	op, gerr := svc.Get(ctx, scope, "k-u3")
	if gerr != nil {
		t.Fatalf("get: %v", gerr)
	}
	if atomic.LoadInt32(&served) != 1 {
		t.Fatalf("the fake provider served %d calls: the refusal must come from streaming a REAL answer, not from a failed connect", served)
	}
	if !strings.Contains(err.Error(), "ingress bound") {
		t.Fatalf("refusal must name the ingress bound, got: %v", err)
	}
	if op.Status != operations.StatusFailed {
		t.Fatalf("status = %q, want FAILED (deterministic refusal, no truncated success)", op.Status)
	}
	stored, _ := json.Marshal(op.Result)
	if strings.Contains(string(stored), "BBBB") {
		t.Fatalf("ledger stored part of the oversized body: %s", stored)
	}
	if _, gerr := svc.GetResult(ctx, scope, ResultReader{ContextID: "ctx-1"}, "k-u3"); contracts.Code(gerr) != contracts.ErrConflict {
		t.Fatalf("GetResult on a refused result: code=%q err=%v, want %q", contracts.Code(gerr), gerr, contracts.ErrConflict)
	}
}

type httpSession struct{ cs *mcp.ClientSession }

func (h httpSession) CallTool(ctx context.Context, p *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	return h.cs.CallTool(ctx, p)
}
func (h httpSession) Close() error { return h.cs.Close() }

// (U4) The service-side ceiling cannot be bypassed by a connector that forgot
// the transport bound: a non-side-effecting answer above the ceiling becomes a
// deterministic FAILED with a sanitized summary, never a truncated SUCCEEDED.
func TestServiceCeilingRefusesOversizedNonSideEffect(t *testing.T) {
	e, ctx := newTestService(t)
	e.svc.cfg.MaxUpstreamBytes = 16
	_, err := e.svc.Call(ctx, e.scope, "k-u4", "echo", json.RawMessage(`{"v":1}`))
	if !errors.Is(err, ErrUpstreamTooLarge) {
		t.Fatalf("err = %v, want ErrUpstreamTooLarge", err)
	}
	op, gerr := e.svc.Get(ctx, e.scope, "k-u4")
	if gerr != nil || op.Status != operations.StatusFailed {
		t.Fatalf("status = %q err=%v, want FAILED", op.Status, gerr)
	}
	if _, ok := FailureFromPayload(op.Result); !ok {
		t.Fatalf("FAILED row must carry the sanitized failure summary: %s", op.Result)
	}
}

// (U5) A side-effecting answer above the ceiling stays SIDE_EFFECT_UNKNOWN: the
// effect did happen, and the guard must keep holding it rather than pretending
// the effect was absent.
func TestServiceCeilingKeepsSideEffectUnknown(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"v":1}`)
	dig, err := ApprovalDigest("write", args)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	e.g.AllowDigest(dig)
	e.svc.cfg.MaxUpstreamBytes = 16
	if _, err := e.svc.Call(ctx, e.scope, "k-u5", "write", args); !errors.Is(err, ErrUpstreamTooLarge) {
		t.Fatalf("err = %v, want ErrUpstreamTooLarge", err)
	}
	op, err := e.svc.Get(ctx, e.scope, "k-u5")
	if err != nil || op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("status = %q err=%v, want SIDE_EFFECT_UNKNOWN", op.Status, err)
	}
	if *e.effCount != 1 {
		t.Fatalf("effects = %d, want exactly 1 (no replay after a refusal)", *e.effCount)
	}
}

type unavailableArtifactSink struct{}

func (unavailableArtifactSink) PutImmutable(context.Context, contracts.Scope, []byte) (string, string, error) {
	return "", "", errors.New("fixture artifact store unavailable")
}

func TestArtifactSpillFailureSettlesOperation(t *testing.T) {
	for _, tc := range []struct {
		tool string
		want string
	}{
		{"echo", operations.StatusFailed},
		{"write", operations.StatusSideEffectUnknown},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			e, ctx := newTestService(t)
			e.svc.cfg.Artifacts = unavailableArtifactSink{}
			e.svc.cfg.ArtifactThreshold = 8
			args := json.RawMessage(`{"v":1}`)
			digest, err := ApprovalDigest(tc.tool, args)
			if err != nil {
				t.Fatal(err)
			}
			e.g.AllowDigest(digest)
			result, err := e.svc.Call(ctx, e.scope, "spill-failure", tc.tool, args)
			if err == nil || result.Status != tc.want {
				t.Errorf("Call status=%q err=%v, want %q and a storage error", result.Status, err, tc.want)
			}
			stored, err := e.svc.Get(ctx, e.scope, "spill-failure")
			if err != nil || stored.Status != tc.want {
				t.Fatalf("durable status=%q err=%v, want %q", stored.Status, err, tc.want)
			}
		})
	}
}

// mustDSN skips unless the real test database is configured.
func mustDSN(t *testing.T) string {
	t.Helper()
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	return os.Getenv("AHP_TEST_DATABASE_URL")
}
