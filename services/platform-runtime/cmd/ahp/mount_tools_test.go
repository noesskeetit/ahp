package main

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
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ahp-platform/ahp/pkg/auth"
)

// TestTryMountToolsFailsClosedWithoutConfig proves the mount never fabricates a
// gateway: AHP_ENABLE_TOOLS=1 with no AHP_TOOLS_CONFIG is refused (fail closed),
// so the plane is absent rather than a face that calls nothing.
func TestTryMountToolsFailsClosedWithoutConfig(t *testing.T) {
	t.Setenv("AHP_TOOLS_CONFIG", "")
	var buf bytes.Buffer
	s := &server{logger: slog.New(slog.NewJSONHandler(&buf, nil))}
	mux := http.NewServeMux()
	verifier := auth.NewVerifier("iss", map[string]ed25519.PublicKey{"k1": mustPub(t)})
	if err := s.tryMountTools(context.Background(), mux, verifier, permissiveDevAuthority(nil), nil); err == nil {
		t.Fatal("expected fail-closed when AHP_TOOLS_CONFIG is empty")
	}
	// No handler was registered, so the mux itself answers 404.
	if resp := mountProbe(mux, "/v1/tools/Call"); !strings.Contains(resp, "404") {
		t.Fatalf("expected unmounted /v1/tools/Call to 404, got %s", resp)
	}
}

// TestTryMountToolsMountsAndRoundTrips stands up the tool face exactly as the
// composed front does: AHP_ENABLE_TOOLS=1, a loopback MCP fixture as the tool
// pool, real mTLS + a task token, and asserts the mount log line and that
// /v1/tools/Call reaches the MCP tool and returns SUCCEEDED.
func TestTryMountToolsMountsAndRoundTrips(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("pg: %v", err)
	}
	defer pool.Close()

	fixtureURL := startLoopbackMCP(t)
	schema := "ahp_tools_m" + randHexTest()
	cfg := map[string]any{
		"connections": map[string]string{"c": fixtureURL},
		"tools":       map[string]any{"echo": map[string]any{"name": "echo", "connection": "c"}},
	}
	cfgRaw, _ := json.Marshal(cfg)
	t.Setenv("AHP_ENABLE_TOOLS", "1")
	t.Setenv("AHP_TOOLS_CONFIG", string(cfgRaw))
	t.Setenv("AHP_TOOLS_SCHEMA", schema)
	t.Setenv("AHP_TOOLS_DEV_BEARER", "mount-bearer")

	var logBuf bytes.Buffer
	s := &server{pool: pool, logger: slog.New(slog.NewJSONHandler(&logBuf, nil))}
	mux := http.NewServeMux()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("iss", "k1", priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("iss", map[string]ed25519.PublicKey{"k1": pub})

	if err := s.tryMountTools(ctx, mux, verifier, permissiveDevAuthority(nil), nil); err != nil {
		t.Fatalf("tryMountTools: %v", err)
	}
	if !strings.Contains(logBuf.String(), "tools plane mounted prefix=/v1/tools/") {
		t.Fatalf("expected mount log line, got: %s", logBuf.String())
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		_, _ = pool.Exec(dctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})

	// Serve the mounted mux over real mTLS and drive one Call.
	ca := newMountCA(t)
	tenant := "mt" + randHexTest()
	dep := "md" + randHexTest()
	clientCert := ca.clientCert(t, "spiffe://ahp/tenant/"+tenant+"/deployment/"+dep)
	serverCAs := x509.NewCertPool()
	serverCAs.AppendCertsFromPEM(ca.pem)
	front := httptest.NewUnstartedServer(mux)
	front.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: serverCAs}
	front.StartTLS()
	defer front.Close()
	tr := front.Client().Transport.(*http.Transport).Clone()
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{}
	}
	tr.TLSClientConfig.Certificates = []tls.Certificate{clientCert}
	front.Client().Transport = tr

	tok, err := signer.Sign(auth.Claims{
		Audience: "ahp", Kind: auth.KindTask, Tenant: tenant, Deployment: dep, Subject: "u",
		Task: "t", Root: "r", Context: "c", Grant: "g", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, front.URL+"/v1/tools/Call",
		strings.NewReader(`{"key":"k-mount-1","tool":"echo","arguments":{"q":"hi"}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := front.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode != 200 || out["status"] != "SUCCEEDED" {
		t.Fatalf("mounted Call = %d %v, want 200 SUCCEEDED", resp.StatusCode, out)
	}
}

// --- inline test harness (self-contained; no shared package deps) -----------

func mountPermissive() func(context.Context, auth.Claims, string) error {
	return func(context.Context, auth.Claims, string) error { return nil }
}

func mustPub(t *testing.T) ed25519.PublicKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub
}

func mountProbe(mux *http.ServeMux, path string) string {
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}")))
	return rec.Result().Status
}

func randHexTest() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	const h = "0123456789abcdef"
	var sb strings.Builder
	for _, x := range b {
		sb.WriteByte(h[x%16])
	}
	return sb.String()
}

type mtCA struct {
	cert *x509.Certificate
	pem  []byte
	key  *ecdsa.PrivateKey
}

func newMountCA(t *testing.T) *mtCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "mount-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	cert, _ := x509.ParseCertificate(der)
	return &mtCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca *mtCA) clientCert(t *testing.T, uri string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(uri)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "mount-test-client"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs: []*url.URL{u},
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	leaf, _ := x509.ParseCertificate(der)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func startLoopbackMCP(t *testing.T) string {
	t.Helper()
	var n int32
	srv := mcp.NewServer(&mcp.Implementation{Name: "mount-fixture", Version: "1"}, nil)
	srv.AddTool(&mcp.Tool{Name: "echo", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			atomic.AddInt32(&n, 1)
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil
		})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv },
		&mcp.StreamableHTTPOptions{JSONResponse: true})
	ts := httptest.NewServer(handler) // loopback only
	t.Cleanup(ts.Close)
	return ts.URL
}

func TestTryMountToolsRejectsUnsafeLeaseSettingsAtStartup(t *testing.T) {
	key, err := x509.MarshalPKIXPublicKey(mustPub(t))
	if err != nil {
		t.Fatal(err)
	}
	validKey := filepath.Join(t.TempDir(), "public.pem")
	if err := os.WriteFile(validKey, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: key}), 0600); err != nil {
		t.Fatal(err)
	}
	invalidKey := filepath.Join(t.TempDir(), "invalid.pem")
	if err := os.WriteFile(invalidKey, []byte("invalid PEM"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, issuer, kid, path, required, want string
	}{
		{"partial", "state", "", "", "", "requires issuer, kid and public key"},
		{"required-without-proof-verifier", "", "", "", "1", "requires issuer, kid and public key"},
		{"unreadable", "state", "proof", filepath.Join(t.TempDir(), "missing"), "", "read the tool lease public key"},
		{"malformed", "state", "proof", invalidKey, "", "PKIX Ed25519 PEM"},
		{"dev-mode", "state", "proof", validKey, "", "live Identity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AHP_TOOLS_LEASE_ISSUER", tc.issuer)
			t.Setenv("AHP_TOOLS_LEASE_KID", tc.kid)
			t.Setenv("AHP_TOOLS_LEASE_PUBLIC_KEY", tc.path)
			t.Setenv("AHP_TOOLS_LEASE_REQUIRED", tc.required)
			t.Setenv("AHP_TOOLS_CONFIG", "")
			s := &server{}
			err := s.tryMountTools(t.Context(), http.NewServeMux(), nil, permissiveDevAuthority(nil), nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("unsafe lease settings error = %v, want %q", err, tc.want)
			}
		})
	}
}
