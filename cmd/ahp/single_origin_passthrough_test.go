package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
)

// passthroughBearer stands for the credential a browser holds. The point of these
// tests is that in the shape the director ruled at 20261101T154234Z item 2 the
// edge does not touch it: it presents its OWN workload certificate and lets the
// data plane verify the bearer, so any edit this edge makes to Authorization --
// even a helpful-looking exchange -- would silently change who the plane thinks is
// calling. That is why the assertion is byte-for-byte, not "a bearer arrived".
const passthroughBearer = "eyJhbGciOiJFZERTQSIsImtpZCI6ImRldi0xIn0.user-credential.sig"

type passthroughPKI struct {
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	caPEM  []byte
}

func newPassthroughPKI(t *testing.T) *passthroughPKI {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "asws-passthrough-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &passthroughPKI{caCert: cert, caKey: key,
		caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (p *passthroughPKI) leaf(t *testing.T, cn string, server bool, workload string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		tmpl.DNSNames = []string{"localhost"}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	if workload != "" {
		u, err := url.Parse(workload)
		if err != nil {
			t.Fatal(err)
		}
		tmpl.URIs = []*url.URL{u}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{
		Certificate: [][]byte{der, p.caCert.Raw},
		PrivateKey:  key,
	}
}

func (p *passthroughPKI) files(t *testing.T, client tls.Certificate) (certPath, keyPath, caPath string) {
	t.Helper()
	dir := t.TempDir()
	certPath = filepath.Join(dir, "edge-client.crt")
	keyPath = filepath.Join(dir, "edge-client.key")
	caPath = filepath.Join(dir, "ca.crt")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: client.Certificate[0]})
	keyDER, err := x509.MarshalECPrivateKey(client.PrivateKey.(*ecdsa.PrivateKey))
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, p.caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath, caPath
}

// passthroughMTLSOrigin is a data plane that demands a workload certificate on the
// connection, which is what the real one does: a plane that answers without one is
// not the plane the ruling describes.
func passthroughMTLSOrigin(t *testing.T, pki *passthroughPKI, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pki.caPEM) {
		t.Fatal("ca pem rejected")
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		Certificates: []tls.Certificate{pki.leaf(t, "data-plane", true, "")},
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func passthroughEnv(t *testing.T, origin, exchange, certPath, keyPath, caPath, passthrough string) {
	t.Setenv(envWorkspaceOrigin, origin)
	t.Setenv(envWorkspaceExchange, exchange)
	t.Setenv(envWorkspaceClientCert, certPath)
	t.Setenv(envWorkspaceClientKey, keyPath)
	t.Setenv(envWorkspaceCA, caPath)
	t.Setenv(envWorkspaceUserPassthrough, passthrough)
}

func TestWorkspaceEdgePassthroughForwardsTheUserCredentialUntouched(t *testing.T) {
	type call struct {
		auth, cookie, path, body string
	}
	var calls []call
	pki := newPassthroughPKI(t)
	origin := passthroughMTLSOrigin(t, pki, func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, call{
			auth: r.Header.Get("Authorization"), cookie: r.Header.Get("Cookie"),
			path: r.URL.Path, body: readAllForTest(t, r),
		})
		if len(calls) == 1 {
			_, _ = w.Write([]byte(`{"id":"ws-1","head_rev":""}`))
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"FORBIDDEN","message":"not a member of workspace ws-1"}`))
	})
	certPath, keyPath, caPath := pki.files(t, pki.leaf(t, "edge", false,
		auth.WorkloadScheme+"/tenant/t1/deployment/asws-edge-lane"))
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("the workspace boundary must not exchange in passthrough mode: that would replace the caller's credential")
	}))
	defer exchange.Close()

	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("the wired edge must not fall through to the runner")
	}))
	defer runner.Close()

	passthroughEnv(t, origin.URL, exchange.URL, certPath, keyPath, caPath, "1")
	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, runner.URL, true, quietLogger()) {
		t.Fatal("mountSingleOrigin returned false")
	}

	post := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/workspace/GetWorkspace",
			strings.NewReader(`{"workspace":"ws-1"}`))
		req.Header.Set("Authorization", "Bearer "+passthroughBearer)
		req.Header.Set("Cookie", "ahp_session=session-cookie-must-not-travel")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	rec := post()
	if len(calls) != 1 {
		t.Fatalf("origin reached %d times, want 1", len(calls))
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "ws-1") {
		t.Fatalf("caller saw %d %q, want the origin's 200 mirrored", rec.Code, rec.Body.String())
	}
	if calls[0].auth != "Bearer "+passthroughBearer {
		t.Fatalf("origin saw Authorization %q, want the caller's credential byte-for-byte", calls[0].auth)
	}
	if calls[0].cookie != "" {
		t.Fatalf("origin saw Cookie %q, which must not travel", calls[0].cookie)
	}
	if calls[0].path != "/v1/workspace/GetWorkspace" || calls[0].body != `{"workspace":"ws-1"}` {
		t.Fatalf("origin saw %s %s, want path and body forwarded unchanged", calls[0].path, calls[0].body)
	}

	rec = post()
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "not a member") {
		t.Fatalf("caller saw %d %q, want the plane's own 403 verbatim: a rights refusal the edge rewrites is a lie about who decided",
			rec.Code, rec.Body.String())
	}
}

// The artifact boundary keeps exchanging while the workspace boundary passes the
// credential through. This is not conservatism: measured at 20261101T155238Z the
// artifact face answers a user credential 403 "token audience does not match", so
// passing the bearer there would break an upload path that is green today.
func TestWorkspaceEdgePassthroughStillExchangesTheArtifactBoundary(t *testing.T) {
	const minted = "minted-child-task-token"
	var gotAuth string
	pki := newPassthroughPKI(t)
	origin := passthroughMTLSOrigin(t, pki, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"id":"upload-1"}`))
	})
	certPath, keyPath, caPath := pki.files(t, pki.leaf(t, "edge", false,
		auth.WorkloadScheme+"/tenant/t1/deployment/asws-edge-lane"))
	var exchangeBodies []string
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exchangeBodies = append(exchangeBodies, readAllForTest(t, r))
		_ = json.NewEncoder(w).Encode(map[string]string{"token": minted, "task": "spauth:child"})
	}))
	defer exchange.Close()
	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("wired edge fell through to the runner")
	}))
	defer runner.Close()

	passthroughEnv(t, origin.URL, exchange.URL, certPath, keyPath, caPath, "1")
	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, runner.URL, true, quietLogger()) {
		t.Fatal("mountSingleOrigin returned false")
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/artifacts/BeginUpload",
		strings.NewReader(`{"key":"k"}`))
	req.Header.Set("Authorization", "Bearer "+passthroughBearer)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if len(exchangeBodies) != 1 {
		t.Fatalf("exchange called %d times, want the artifact boundary to keep exchanging", len(exchangeBodies))
	}
	if gotAuth != "Bearer "+minted {
		t.Fatalf("origin saw Authorization %q, want the minted child token on the artifact boundary", gotAuth)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 mirrored", rec.Code)
	}
}

// Passthrough moves the caller's own credential across the network, so it is
// refused over a cleartext origin instead of mounting and leaking a session.
func TestWorkspaceEdgePassthroughRefusesACleartextOrigin(t *testing.T) {
	runnerHits := 0
	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runnerHits++
		http.Error(w, "404 page not found\n", http.StatusNotFound)
	}))
	defer runner.Close()
	passthroughEnv(t, "http://127.0.0.1:1", "http://127.0.0.1:1/v1/identity/control/exchange", "", "", "", "1")

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, runner.URL, true, quietLogger()) {
		t.Fatal("mountSingleOrigin returned false")
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/workspace/GetWorkspace", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+passthroughBearer)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if runnerHits != 1 {
		t.Fatalf("runner hits = %d, want the refused edge to fall back to plain forwarding", runnerHits)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want the runner's 404 mirrored, not a half-mounted upload face", rec.Code)
	}
}

// Turning the new mode on must not disturb the mode that is deployed today: with
// passthrough unset, and every certificate in place, the workspace boundary still
// exchanges and the origin still sees the minted child token.
func TestWorkspaceEdgeExchangeModeIsTheDefaultEvenWithCertificates(t *testing.T) {
	const minted = "minted-workspace-token"
	var gotAuth string
	pki := newPassthroughPKI(t)
	origin := passthroughMTLSOrigin(t, pki, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"id":"ws-1"}`))
	})
	certPath, keyPath, caPath := pki.files(t, pki.leaf(t, "edge", false,
		auth.WorkloadScheme+"/tenant/t1/deployment/asws-edge-lane"))
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"token": minted, "task": "spauth:child"})
	}))
	defer exchange.Close()
	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("wired edge fell through to the runner")
	}))
	defer runner.Close()

	passthroughEnv(t, origin.URL, exchange.URL, certPath, keyPath, caPath, "")
	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, runner.URL, true, quietLogger()) {
		t.Fatal("mountSingleOrigin returned false")
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/workspace/Create", strings.NewReader(`{"workspace":"p"}`))
	req.Header.Set("Authorization", "Bearer "+passthroughBearer)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s, want the exchanged 200", rec.Code, rec.Body.String())
	}
	if gotAuth != "Bearer "+minted {
		t.Fatalf("origin saw Authorization %q, want the minted token while passthrough is unset", gotAuth)
	}
}
