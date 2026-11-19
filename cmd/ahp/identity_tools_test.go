package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/internal/tools/toolsapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type identityFixtureResolver struct{}

func (identityFixtureResolver) LookupHost(context.Context, string) ([]string, error) {
	return []string{"10.80.20.10"}, nil
}

type identityFixtureDialer string

func (d identityFixtureDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, string(d))
}

// Real task mTLS -> ToolGW -> signed owner assertion -> controlled IAM -> MCP.
// Only socket routing is redirected to loopback fixtures; netpolicy still runs.
func TestSyntheticIdentityToolExchangeMTLSRealPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" {
		t.Fatal("real PostgreSQL required")
	}
	pool, err := pgxpool.New(ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var logs bytes.Buffer
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("ahp", "exchange-test", key)
	policy := identity.NewSyntheticResourcePolicy(pool)
	owner := &server{pool: pool, iam: policy, exchangeSigner: signer, logger: slog.New(slog.NewTextHandler(&logs, nil))}
	ids := owner.identityStore()
	if err = ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	sc := contracts.Scope{Tenant: "exchange-" + randHexTest(), Subject: "alice", Deployment: "agent"}
	for _, rule := range []identity.ResourceRule{{Tenant: sc.Tenant, Subject: sc.Subject, Resource: identity.Resource{Kind: "tenant", ID: sc.Tenant}, Action: "tools", Allowed: true}, {Tenant: sc.Tenant, Subject: sc.Subject, Resource: identity.Resource{Kind: "deployment", ID: sc.Deployment}, Action: "tools", Allowed: true}, {Tenant: sc.Tenant, Subject: sc.Subject, Resource: identity.Resource{Kind: "connection", ID: "trusted"}, Action: "use", Allowed: true}} {
		if _, err = policy.PutRule(ctx, rule, 0); err != nil {
			t.Fatal(err)
		}
	}
	task := "task-" + randHexTest()
	g, err := ids.CreateRoot(ctx, sc, identity.RootRequest{Task: task, Context: task, Deployment: sc.Deployment, Capabilities: []string{"tools"}, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	token, err := ids.MintTask(ctx, sc, task, signer, "ahp", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var exchanges, calls atomic.Int32
	var badAudience, midRevoke atomic.Bool
	var assertionSeen string
	iam := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exchanges.Add(1)
		_ = r.ParseForm()
		assertionSeen = r.Form.Get("subject_token")
		parts := strings.Split(assertionSeen, ".")
		if len(parts) != 3 || assertionSeen == token || r.Header.Get("Authorization") != "" {
			http.Error(w, "invalid proof", 403)
			return
		}
		sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
		payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
		var a auth.ExchangeAssertion
		_ = json.Unmarshal(payload, &a)
		if !ed25519.Verify(pub, []byte(parts[0]+"."+parts[1]), sig) || a.TokenUse != "exchange" || a.Audience != "http://iam.fixture/token" || a.TargetAudience != "mcp-trusted" || a.Resource != "urn:mcp:trusted" || a.Scope != "read" || a.Grant != g.ID || a.Subject != sc.Subject || a.Tenant != sc.Tenant || a.ExpiresAt-a.IssuedAt != 60 || r.Form.Get("audience") != "mcp-trusted" || badAudience.Load() {
			http.Error(w, "invalid proof", 403)
			return
		}
		if midRevoke.Load() {
			_, e := policy.PutRule(ctx, identity.ResourceRule{Tenant: sc.Tenant, Subject: sc.Subject, Resource: identity.Resource{Kind: "connection", ID: "trusted"}, Action: "use", Allowed: false}, 1)
			if e != nil {
				t.Error(e)
			}
		}
		json.NewEncoder(w).Encode(tools.ExchangeResponse{AccessToken: "fixture-downstream-only", IssuedTokenType: tools.AccessTokenTokenType, TokenType: "Bearer", ExpiresIn: 60, Scope: "read"})
	}))
	defer iam.Close()
	registration := tools.ExchangeRegistration{TokenURL: "http://iam.fixture/token", Audience: "mcp-trusted", Resource: "urn:mcp:trusted", Scope: "read", Policy: netpolicy.Policy{PrivateHosts: []string{"iam.fixture"}, AllowedMethods: []string{"POST"}, AllowedPathPrefixes: []string{"/token"}}}
	creds, err := tools.NewExchangeResolver(map[string]tools.ExchangeRegistration{"trusted": registration}, owner.issueToolAssertion, owner.toolsConnectionAuthority(), nil, netpolicy.WithResolver(identityFixtureResolver{}), netpolicy.WithDialer(identityFixtureDialer(strings.TrimPrefix(iam.URL, "http://"))))
	if err != nil {
		t.Fatal(err)
	}
	mcpServer := mcp.NewServer(&mcp.Implementation{Name: "synthetic-trusted", Version: "1"}, nil)
	mcpServer.AddTool(&mcp.Tool{Name: "echo", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		calls.Add(1)
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "accepted"}}}, nil
	})
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return mcpServer }, &mcp.StreamableHTTPOptions{JSONResponse: true, DisableLocalhostProtection: true})
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "mcp.fixture" || r.Header.Get("Authorization") != "Bearer fixture-downstream-only" {
			t.Error("MCP received wrong credential")
			http.Error(w, "denied", 403)
			return
		}
		mcpHandler.ServeHTTP(w, r)
	}))
	defer downstream.Close()
	egress, err := netpolicy.NewClient(netpolicy.Policy{PrivateHosts: []string{"mcp.fixture"}, AllowedMethods: []string{"GET", "POST", "DELETE"}, AllowedPathPrefixes: []string{"/mcp"}}, netpolicy.WithResolver(identityFixtureResolver{}), netpolicy.WithDialer(identityFixtureDialer(strings.TrimPrefix(downstream.URL, "http://"))))
	if err != nil {
		t.Fatal(err)
	}
	schema := "ahp_exchange_" + randHexTest()
	svc, err := tools.New(ctx, pool, schema, tools.Config{Guardian: tools.NewPolicyGuardian("synthetic"), Credentials: creds, Connections: map[string]tools.Connector{"trusted": func(ctx context.Context, c tools.Credential) (tools.MCPClient, error) {
		client := *egress
		client.Transport = bearerInjector{base: client.Transport, bearer: c.Bearer}
		return connectMCPWithClient(ctx, "http://mcp.fixture/mcp", &client)
	}}, Tools: map[string]tools.ToolSpec{"echo": {Connection: "trusted", Name: "echo"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"exchange-test": pub})
	handler, err := toolsapi.NewHandler(svc, verifier, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	ca := newMountCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	front := httptest.NewUnstartedServer(handler)
	front.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	front.StartTLS()
	defer front.Close()
	client := front.Client()
	tr := client.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(sc))}
	client.Transport = tr
	call := func(want string) {
		body, _ := json.Marshal(map[string]any{"key": "op-" + randHexTest(), "tool": "echo", "arguments": map[string]string{"question": "safe"}})
		r, _ := http.NewRequestWithContext(ctx, "POST", front.URL+"/v1/tools/Call", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		resp, e := client.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !strings.Contains(string(raw), want) {
			t.Fatalf("status=%d expected=%s got=%s", resp.StatusCode, want, raw)
		}
		if strings.Contains(string(raw), "fixture-downstream-only") || strings.Contains(string(raw), token) || strings.Contains(string(raw), assertionSeen) {
			t.Fatal("credential leaked to agent")
		}
	}
	call("SUCCEEDED")
	if calls.Load() != 1 || exchanges.Load() != 1 {
		t.Fatal("valid tool exchange did not reach registered MCP")
	}
	badAudience.Store(true)
	call("FAILED")
	badAudience.Store(false)
	if calls.Load() != 1 {
		t.Fatal("cross-audience exchange reached MCP")
	}
	midRevoke.Store(true)
	call("FAILED")
	if calls.Load() != 1 {
		t.Fatal("revocation during exchange reached MCP")
	}
	before := exchanges.Load()
	call("FAILED")
	if exchanges.Load() != before {
		t.Fatal("old JWT contacted IAM after connection revoke")
	}
	if strings.Contains(logs.String(), token) || strings.Contains(logs.String(), "fixture-downstream-only") {
		t.Fatal("secret reached logs")
	}
}
