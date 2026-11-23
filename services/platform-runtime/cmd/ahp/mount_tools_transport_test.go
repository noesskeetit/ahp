package main

import (
	"context"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type observedMCPRequest struct {
	method, address, proto, target, bearer, session string
}

type mcpTransportFixture struct {
	mu       sync.Mutex
	requests []observedMCPRequest
	live     map[net.Conn]bool
	server   *httptest.Server
}

func startMCPTransportFixture(t *testing.T, stateful, tls bool) *mcpTransportFixture {
	t.Helper()
	f := &mcpTransportFixture{live: make(map[net.Conn]bool)}
	srv := mcp.NewServer(&mcp.Implementation{Name: "transport-test", Version: "1"}, nil)
	srv.AddTool(&mcp.Tool{Name: "echo", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil
		})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv },
		&mcp.StreamableHTTPOptions{JSONResponse: true, Stateless: !stateful})
	f.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.requests = append(f.requests, observedMCPRequest{
			r.Method, r.RemoteAddr, r.Proto, r.RequestURI, r.Header.Get("Authorization"), r.Header.Get("Mcp-Session-Id"),
		})
		f.mu.Unlock()
		if r.URL.Path == "/redirect" {
			w.Header().Set("Location", "/mcp")
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	f.server.Config.ConnState = func(c net.Conn, state http.ConnState) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch state {
		case http.StateNew:
			f.live[c] = true
		case http.StateClosed, http.StateHijacked:
			delete(f.live, c)
		}
	}
	if tls {
		f.server.EnableHTTP2 = true
		f.server.StartTLS()
	} else {
		f.server.Start()
	}
	t.Cleanup(f.server.Close)
	return f
}

func (f *mcpTransportFixture) snapshot() ([]observedMCPRequest, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]observedMCPRequest(nil), f.requests...), len(f.live)
}

func testMCPConnector(t *testing.T, endpoint string, roots *x509.CertPool, idle time.Duration) tools.Connector {
	t.Helper()
	pool := newMCPTransportPool(roots, idle)
	t.Cleanup(pool.closeIdleConnections)
	return mustMCPConnector(t, pool, endpoint)
}

func mustMCPConnector(t *testing.T, pool *mcpTransportPool, endpoint string) tools.Connector {
	t.Helper()
	connector, err := pool.connector(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	return connector
}

func testMCPCall(t *testing.T, c tools.Connector, bearer string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client, err := c(ctx, tools.Credential{Bearer: bearer})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("close MCP session: %v", err)
		}
	}()
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "echo"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if len(result.Content) != 1 {
		t.Fatalf("unexpected MCP result: %+v", result)
	}
	content, ok := result.Content[0].(*mcp.TextContent)
	if !ok || content.Text != "ok" {
		t.Fatalf("unexpected MCP content: %+v", result.Content)
	}
}

func TestMCPTransportReusesSocketAcrossBearers(t *testing.T) {
	f := startMCPTransportFixture(t, false, true)
	roots := x509.NewCertPool()
	roots.AddCert(f.server.Certificate())
	c := testMCPConnector(t, f.server.URL+"/m%63p?b=2&a=1", roots, 10*time.Second)

	testMCPCall(t, c, "first")
	first, _ := f.snapshot()
	testMCPCall(t, c, "second")
	both, _ := f.snapshot()
	if len(first) == 0 || len(both) <= len(first) {
		t.Fatalf("expected requests from two calls, got %d then %d", len(first), len(both))
	}
	for i, r := range both {
		want := "Bearer first"
		if i >= len(first) {
			want = "Bearer second"
		}
		if r.bearer != want || r.proto != "HTTP/1.1" || r.target != "/m%63p?b=2&a=1" {
			t.Errorf("request %d: got %+v, want bearer %q and original HTTP/1.1 request-target", i, r, want)
		}
	}
	if both[len(first)].address != first[0].address {
		t.Errorf("second MCP session used a fresh socket: first %s, second %s", first[0].address, both[len(first)].address)
	}
}

func TestMCPTransportSeparatesEndpointsOnSameServer(t *testing.T) {
	f := startMCPTransportFixture(t, false, false)
	pool := newMCPTransportPool(nil, 10*time.Second)
	t.Cleanup(pool.closeIdleConnections)
	first := mustMCPConnector(t, pool, f.server.URL+"/mcp")
	alias := mustMCPConnector(t, pool, f.server.URL+"/mcp#client-only")
	other := mustMCPConnector(t, pool, f.server.URL+"/other")

	testMCPCall(t, first, "first")
	testMCPCall(t, alias, "alias")
	testMCPCall(t, other, "other")
	requests, _ := f.snapshot()
	sockets := make(map[string]string)
	for _, r := range requests {
		sockets[r.bearer] = r.address
	}
	a, b, c := sockets["Bearer first"], sockets["Bearer alias"], sockets["Bearer other"]
	if a == "" || b != a || c == "" || c == a {
		t.Fatalf("equivalent endpoints must reuse a socket and other paths must have a separate socket, got %v", sockets)
	}
}

func TestMCPTransportClosesIdleSockets(t *testing.T) {
	f := startMCPTransportFixture(t, false, false)
	c := testMCPConnector(t, f.server.URL+"/mcp", nil, 100*time.Millisecond)
	testMCPCall(t, c, "bearer")
	deadline := time.Now().Add(time.Second)
	for {
		_, live := f.snapshot()
		if live == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d connections still open after idle timeout", live)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMCPTransportRejectsUntrustedProfileAfterTrustedCall(t *testing.T) {
	f := startMCPTransportFixture(t, false, true)
	roots := x509.NewCertPool()
	roots.AddCert(f.server.Certificate())
	trusted := testMCPConnector(t, f.server.URL+"/mcp", roots, 10*time.Second)
	untrustedRoots := x509.NewCertPool()
	untrusted := testMCPConnector(t, f.server.URL+"/mcp", untrustedRoots, 10*time.Second)
	// Later changes to the caller's roots must not alter the registered profile.
	untrustedRoots.AddCert(f.server.Certificate())
	testMCPCall(t, trusted, "trusted")
	initial, _ := f.snapshot()

	client, err := untrusted(t.Context(), tools.Credential{Bearer: "untrusted"})
	if err == nil {
		client.Close()
		t.Fatal("untrusted TLS profile connected after a trusted call")
	}
	if !strings.Contains(err.Error(), "certificate signed by unknown authority") {
		t.Fatalf("expected TLS trust rejection, got %v", err)
	}
	after, _ := f.snapshot()
	if len(after) != len(initial) {
		t.Fatal("request from untrusted TLS profile reached MCP")
	}
	testMCPCall(t, trusted, "trusted-again")
}

func TestMCPTransportKeepsSessionsAndBearersIsolated(t *testing.T) {
	f := startMCPTransportFixture(t, true, false)
	c := testMCPConnector(t, f.server.URL+"/mcp", nil, 10*time.Second)
	t.Run("concurrent calls", func(t *testing.T) {
		for _, bearer := range []string{"first", "second"} {
			t.Run(bearer, func(t *testing.T) {
				t.Parallel()
				testMCPCall(t, c, bearer)
			})
		}
	})

	requests, _ := f.snapshot()
	owners := map[string]string{}
	deleted := map[string]int{}
	for _, r := range requests {
		if r.session == "" {
			continue
		}
		if owner, exists := owners[r.session]; exists && owner != r.bearer {
			t.Errorf("session %q owner %q also used by %q", r.session, owner, r.bearer)
		}
		owners[r.session] = r.bearer
		if r.method == http.MethodDelete {
			deleted[r.bearer]++
		}
	}
	if len(owners) != 2 || deleted["Bearer first"] != 1 || deleted["Bearer second"] != 1 {
		t.Fatalf("expected two independently closed MCP sessions, owners=%v, DELETE=%v", owners, deleted)
	}
}

func TestMCPTransportRedirectBearerStaysOnOrigin(t *testing.T) {
	t.Run("same origin", func(t *testing.T) {
		f := startMCPTransportFixture(t, false, false)
		c := testMCPConnector(t, f.server.URL+"/redirect", nil, 10*time.Second)
		testMCPCall(t, c, "redirected")
		requests, _ := f.snapshot()
		for _, r := range requests {
			if r.bearer != "Bearer redirected" {
				t.Errorf("same-origin request lost bearer: %+v", r)
			}
		}
	})
	t.Run("different origin", func(t *testing.T) {
		f := startMCPTransportFixture(t, false, false)
		source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", f.server.URL+"/mcp")
			w.WriteHeader(http.StatusTemporaryRedirect)
		}))
		defer source.Close()
		c := testMCPConnector(t, source.URL+"/mcp", nil, 10*time.Second)
		testMCPCall(t, c, "private")
		requests, _ := f.snapshot()
		for _, r := range requests {
			if r.bearer != "" {
				t.Errorf("cross-origin redirect exposed bearer: %+v", r)
			}
		}
	})
}
