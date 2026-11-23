package toolsapi

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	tAudience  = "ahp"
	tIssuer    = "https://identity.test"
	tKID       = "k1"
	tSubject   = "svc-worker"
	tHumanSubj = tSubject
)

// ---------------------------------------------------------------------------
// Test-only CA + SPIFFE client certificates (no external trust material).
// ---------------------------------------------------------------------------

type testCA struct {
	cert *x509.Certificate
	pem  []byte
	key  *ecdsa.PrivateKey
}

func newCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ahp-toolsapi-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &testCA{cert: cert, key: key,
		pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca *testCA) clientCert(t *testing.T, uris ...string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	parsed := make([]*url.URL, 0, len(uris))
	for _, u := range uris {
		p, err := url.Parse(u)
		if err != nil {
			t.Fatalf("bad test uri %q: %v", u, err)
		}
		parsed = append(parsed, p)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "ahp-toolsapi-test-client"},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:        parsed,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(der)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func workloadURI(tenant, deployment string) string {
	return "spiffe://ahp/tenant/" + tenant + "/deployment/" + deployment
}

// ---------------------------------------------------------------------------
// Real MCP tool-server fixture over LOOPBACK streamable HTTP (no external
// egress): the exact same go-sdk the product uses, on 127.0.0.1.
// ---------------------------------------------------------------------------

type mcpFixture struct {
	srv               *httptest.Server
	effCount          int32
	onWrite           atomic.Pointer[func() error]
	writePadding      atomic.Int32
	dropNextCallReply atomic.Bool
	droppedCallReply  atomic.Bool
	// lastBearer records the Authorization header the fixture last saw, so a test
	// can prove the resolved credential actually flowed to the MCP server.
	lastBearer atomic.Value
}

// clientSession adapts a streamable *mcp.ClientSession to tools.MCPClient.
type clientSession struct{ cs *mcp.ClientSession }

func (c clientSession) CallTool(ctx context.Context, p *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	return c.cs.CallTool(ctx, p)
}
func (c clientSession) Close() error { return c.cs.Close() }

// bearerTransport injects a static bearer into every MCP request (the resolved
// credential) and records it for assertions.
type bearerTransport struct {
	base   http.RoundTripper
	token  string
	target *atomic.Value
}

func (b bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if b.token != "" {
		r2 := r.Clone(r.Context())
		r2.Header.Set("Authorization", "Bearer "+b.token)
		if b.target != nil {
			b.target.Store(r2.Header.Get("Authorization"))
		}
		return b.base.RoundTrip(r2)
	}
	return b.base.RoundTrip(r)
}

func newMCPFixture(t *testing.T) *mcpFixture {
	t.Helper()
	f := &mcpFixture{}
	srv := mcp.NewServer(&mcp.Implementation{Name: "toolsapi-fixture", Version: "1"}, nil)
	srv.AddTool(&mcp.Tool{Name: "echo", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "echoed"}}}, nil
		})
	srv.AddTool(&mcp.Tool{Name: "write", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			if record := f.onWrite.Load(); record != nil {
				if err := (*record)(); err != nil {
					return nil, err
				}
			}
			atomic.AddInt32(&f.effCount, 1)
			text := "written" + strings.Repeat("x", int(f.writePadding.Load()))
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}, nil
		})
	srv.AddTool(&mcp.Tool{Name: "errwrite", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			atomic.AddInt32(&f.effCount, 1)
			return nil, errors.New("boom")
		})
	srv.AddTool(&mcp.Tool{Name: "business", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			atomic.AddInt32(&f.effCount, 1)
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: "business rejection"}},
			}, nil
		})
	srv.AddTool(&mcp.Tool{Name: "gated", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "gated-ok"}}}, nil
		})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv },
		&mcp.StreamableHTTPOptions{JSONResponse: true})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !f.dropNextCallReply.Load() || r.Method != http.MethodPost {
			handler.ServeHTTP(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			http.Error(w, "fixture request read failed", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		var call struct{ Method string }
		if json.Unmarshal(body, &call) != nil || call.Method != "tools/call" || !f.dropNextCallReply.Swap(false) {
			handler.ServeHTTP(w, r)
			return
		}
		reply := httptest.NewRecorder()
		handler.ServeHTTP(reply, r)
		if reply.Code != http.StatusOK {
			t.Errorf("unexpected MCP reply before simulated loss: %d", reply.Code)
		}
		socket, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("drop MCP reply: %v", err)
			return
		}
		f.droppedCallReply.Store(true)
		_ = socket.Close()
	})) // 127.0.0.1 only; no external egress
	t.Cleanup(ts.Close)
	f.srv = ts
	return f
}

func (f *mcpFixture) connector() tools.Connector {
	return func(ctx context.Context, cred tools.Credential) (tools.MCPClient, error) {
		// The streamable transport requires a client whose URL points at 127.0.0.1
		// (loopback). The resolved bearer is injected through a custom transport so
		// the credential flow can be proven.
		base, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			base = &http.Transport{}
		}
		httpClient := &http.Client{Transport: bearerTransport{base: base, token: cred.Bearer, target: &f.lastBearer}}
		cs, err := mcp.NewClient(&mcp.Implementation{Name: "ahp-tools-gw", Version: "1"}, nil).Connect(ctx,
			&mcp.StreamableClientTransport{Endpoint: f.srv.URL, HTTPClient: httpClient, DisableStandaloneSSE: true}, nil)
		if err != nil {
			return nil, err
		}
		return clientSession{cs: cs}, nil
	}
}

type fakeCreds struct{ bearer string }

func (c fakeCreds) Resolve(context.Context, contracts.Scope, string) (tools.Credential, error) {
	return tools.Credential{Bearer: c.bearer}, nil
}

// ---------------------------------------------------------------------------
// End-to-end environment: real PG, loopback MCP fixture, real mTLS front and
// Ed25519 signer-backed task + human tokens.
// ---------------------------------------------------------------------------

type env struct {
	svc        *tools.Service
	srv        *httptest.Server
	guardian   *tools.PolicyGuardian
	fixture    *mcpFixture
	pool       *pgxpool.Pool
	schema     string
	tenant     string
	deployment string
	ca         *testCA
	signer     *auth.Signer
	verifier   *auth.Verifier
}

func newEnv(t *testing.T, options ...func(*tools.Config)) *env {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set AHP_TEST_DATABASE_URL to run toolsapi integration tests")
	}
	return newEnvForDatabase(t, dsn, options...)
}

func newEnvForDatabase(t *testing.T, dsn string, options ...func(*tools.Config)) *env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("pg unreachable: %v", err)
	}

	fixture := newMCPFixture(t)

	signerPub, signerPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner(tIssuer, tKID, signerPriv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(tIssuer, map[string]ed25519.PublicKey{tKID: signerPub})

	authorize := func(context.Context, auth.Claims, string) error { return nil }

	schema := "ahp_tools_t" + randomHex(t, 6)
	scopeTenant := "t" + randomHex(t, 6)
	deployment := "d" + randomHex(t, 6)
	g := tools.NewPolicyGuardian("test")
	config := tools.Config{
		Guardian:    g,
		Credentials: fakeCreds{bearer: "cred-" + randomHex(t, 4)},
		Connections: map[string]tools.Connector{"c": fixture.connector()},
		Tools: map[string]tools.ToolSpec{
			"echo":     {Name: "echo", Connection: "c"},
			"write":    {Name: "write", Connection: "c", SideEffecting: true},
			"errwrite": {Name: "errwrite", Connection: "c", SideEffecting: true},
			"business": {Name: "business", Connection: "c", SideEffecting: true},
			"gated":    {Name: "gated", Connection: "c", RequiresApproval: true},
		},
		Verifier: verifier,
		Audience: tAudience,
	}
	for _, option := range options {
		option(&config)
	}
	svc, err := tools.New(ctx, pool, schema, config)
	if err != nil {
		t.Fatalf("tools.New: %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		_ = svc.Drop(dctx)
		pool.Close()
	})

	handler, err := NewHandler(svc, verifier, authorize)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	ca := newCA(t)
	client := ca.clientCert(t, workloadURI(scopeTenant, deployment))
	serverCAs := x509.NewCertPool()
	if !serverCAs.AppendCertsFromPEM(ca.pem) {
		t.Fatal("test CA PEM rejected")
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: serverCAs}
	srv.StartTLS()
	_ = srv
	e := &env{svc: svc, srv: srv, guardian: g, fixture: fixture,
		pool: pool, schema: schema,
		tenant: scopeTenant, deployment: deployment, ca: ca, signer: signer, verifier: verifier}
	e.setClient(client)
	t.Cleanup(srv.Close)
	return e
}

// setClient points the env's HTTP client at a specific workload certificate.
func (e *env) setClient(cert tls.Certificate) {
	tr := e.srv.Client().Transport.(*http.Transport).Clone()
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{}
	}
	tr.TLSClientConfig.Certificates = []tls.Certificate{cert}
	e.srv.Client().Transport = tr
}

// signTask mints a task token bound to the env workload.
func (e *env) signTask(t *testing.T) string {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{
		Audience: tAudience, Kind: auth.KindTask,
		Tenant: e.tenant, Deployment: e.deployment, Subject: tSubject,
		Task: "task-1", Root: "run-1", Context: "ctx-1", Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// signHuman mints a human token for the env tenant.
func (e *env) signHuman(t *testing.T) string {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{
		Audience: tAudience, Kind: auth.KindHuman,
		Tenant: e.tenant, Subject: tHumanSubj,
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// doRaw performs a POST with an explicit bearer and an explicit workload cert
// (tenant/deployment) so cross-tenant tests can drive the front as a foreign
// workload. Returns status + decoded body.
func (e *env) doRaw(t *testing.T, cert tls.Certificate, op, token, body string) (int, map[string]any) {
	t.Helper()
	tr := e.srv.Client().Transport.(*http.Transport).Clone()
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{}
	}
	tr.TLSClientConfig.Certificates = []tls.Certificate{cert}
	client := &http.Client{Transport: tr}
	req, err := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/tools/"+op, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if json.Unmarshal(raw, &out) != nil {
		out = map[string]any{"_raw": string(raw)}
	}
	return resp.StatusCode, out
}

// call posts with the env's own workload client.
func (e *env) call(t *testing.T, op, token, body string) (int, map[string]any) {
	t.Helper()
	return e.doRaw(t, e.currentClient(), op, token, body)
}

// currentClient re-reads the client cert currently installed on the env client.
func (e *env) currentClient() tls.Certificate {
	tr := e.srv.Client().Transport.(*http.Transport)
	return tr.TLSClientConfig.Certificates[0]
}

// allowDigest pre-permits one side-effecting canonical digest on the guardian.
func (e *env) allowDigest(t *testing.T, tool string, args json.RawMessage) string {
	t.Helper()
	d, err := tools.ApprovalDigest(tool, args)
	if err != nil {
		t.Fatal(err)
	}
	e.guardian.AllowDigest(d)
	return d
}

func mustStatus(t *testing.T, got, want int, body map[string]any) {
	t.Helper()
	if got != want {
		t.Fatalf("status = %d, want %d (body %v)", got, want, body)
	}
}

// requireNoContent asserts a value carries none of the forbidden content markers
// when serialized -- used by the observability negative test.
func requireNoContent(t *testing.T, v any, markers ...string) {
	t.Helper()
	blob, err := json.Marshal(v)
	if err != nil {
		blob = []byte(strings.TrimSpace(fmtSprint(v)))
	}
	low := strings.ToLower(string(blob))
	for _, m := range markers {
		if m != "" && strings.Contains(low, strings.ToLower(m)) {
			t.Fatalf("content marker %q leaked into a content-free view: %s", m, string(blob))
		}
	}
}

func fmtSprint(v any) string { b, _ := json.Marshal(v); return string(b) }

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	const hexd = "0123456789abcdef"
	var sb strings.Builder
	for _, x := range b {
		sb.WriteByte(hexd[x%16])
	}
	return sb.String()
}

// _ keeps the operations import referenced from helper files.
var _ = operations.StatusSucceeded
