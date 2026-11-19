package execution_test

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
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// This guards director arbitration d71a7b0e (option A): the execution Public
// face accepts a SCOPED task token (KindTask bound to the mTLS certificate
// workload) so gateway.Handler.principal sees a COMPLETE scope and drives a task
// to COMPLETED, and stays fail-closed for a foreign workload, a human token
// (no deployment claim), a plaintext connection and a missing bearer token.
// It uses a deterministic stub executor so it is offline (Postgres only; no
// native key, no MinIO). The artifact/seal and live-model legs are proven in
// composition_test.go / http_handler_test.go / executor_live_test.go.

// randSuffix yields a short unique hex suffix so concurrent runs / tenants do
// not collide on the shared isolated Postgres.
func randSuffix() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type ptCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
	pem  []byte
}

func ptNewCA(t *testing.T) *ptCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ahp-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	p := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return &ptCA{cert: cert, key: key, pool: pool, pem: p}
}

// ptLeaf mints a leaf certificate signed by the CA. When uri is non-empty it is
// added as the single SPIFFE workload URI (the execution-plane workload identity).
func (ca *ptCA) ptLeaf(t *testing.T, cn string, uri string, server bool) tls.Certificate {
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
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	if server {
		tmpl.DNSNames = []string{"localhost"}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	if uri != "" {
		u, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		tmpl.URIs = []*url.URL{u}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func ptMTLSServer(t *testing.T, ca *ptCA, client tls.Certificate, handler http.Handler) *httptest.Server {
	t.Helper()
	srvCert := ca.ptLeaf(t, "ahp-exec-test", "", true)
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{srvCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca.pool}
	srv.StartTLS()
	srv.Client().Transport.(*http.Transport).TLSClientConfig = &tls.Config{Certificates: []tls.Certificate{client}, InsecureSkipVerify: true}
	t.Cleanup(srv.Close)
	return srv
}

func ptVerifierSigner(t *testing.T) (*auth.Verifier, *auth.Signer) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("ahp", "k1", priv)
	if err != nil {
		t.Fatal(err)
	}
	return auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"k1": pub}), signer
}

func ptTaskToken(t *testing.T, s *auth.Signer, tenant, dep, subject, task string) string {
	t.Helper()
	tok, err := s.Sign(auth.Claims{
		Audience: "ahp", Kind: auth.KindTask, Tenant: tenant, Deployment: dep, Subject: subject,
		Task: task, Root: task, Context: "exec", Grant: "g-exec", Depth: 0,
	}, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func ptHumanToken(t *testing.T, s *auth.Signer, tenant, subject string) string {
	t.Helper()
	tok, err := s.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindHuman, Tenant: tenant, Subject: subject}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// ptSendStatus performs one SendMessage and reports the effective status (HTTP
// status on a non-RPC body, or the JSON-RPC error code when present).
func ptSendStatus(t *testing.T, srv *httptest.Server, token string) int {
	t.Helper()
	pb, _ := json.Marshal(&a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("do it")),
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	})
	body, _ := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: "SendMessage", Params: pb})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/a2a/public", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Error *rpcErr `json:"error"`
	}
	_ = json.Unmarshal(raw, &out)
	if out.Error != nil {
		return out.Error.Code
	}
	return resp.StatusCode
}

// ptSendForID sends and returns the created task id by decoding the result body.
func ptSendForID(t *testing.T, srv *httptest.Server, token string) a2a.TaskID {
	t.Helper()
	pb, _ := json.Marshal(&a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("do it")),
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	})
	body, _ := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: "SendMessage", Params: pb})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/a2a/public", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Result sendResult `json:"result"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out.Result.Task.ID
}

func ptDecodeTask(t *testing.T, srv *httptest.Server, token string, id a2a.TaskID, into *a2a.Task) bool {
	t.Helper()
	pb, _ := json.Marshal(&a2a.GetTaskRequest{ID: id})
	body, _ := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: "GetTask", Params: pb})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/a2a/public", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := srv.Client().Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcErr         `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.Error != nil {
		return false
	}
	return json.Unmarshal(out.Result, into) == nil
}

// ptDrive sends a task over the Public face and, only if the request was
// accepted (full scope), polls to a terminal COMPLETED task. It returns nil when
// the guard rejected the request (fail-closed) or the task did not complete.
func ptDrive(t *testing.T, srv *httptest.Server, token string) *a2a.Task {
	t.Helper()
	if st := ptSendStatus(t, srv, token); st != http.StatusOK {
		return nil
	}
	id := ptSendForID(t, srv, token)
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		var cur a2a.Task
		if !ptDecodeTask(t, srv, token, id, &cur) {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if cur.Status.State == a2a.TaskStateCompleted {
			return &cur
		}
	}
	return nil
}

// ptFaceService starts a durable gateway service (stub executor -> terminal
// Completed) and wires its Public face with the scoped task-token guard the
// execution role uses (option A), returning the mTLS server.
func ptFaceService(t *testing.T, ca *ptCA, client tls.Certificate, verifier *auth.Verifier) *httptest.Server {
	t.Helper()
	_, store, ledger, sfx := gwSetup(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	execStub := gateway.ExecutorFunc(func(_ context.Context, _ gateway.ExecInput) (gateway.ExecResult, error) {
		return gateway.ExecResult{State: a2a.TaskStateCompleted}, nil
	})
	svc, err := gateway.NewService(store, ledger, execStub, gateway.NullNotifier{}, gateway.Config{
		WorkerID: "face-" + sfx[:8], Lease: 5 * time.Second, Poll: 10 * time.Millisecond,
		MaxBlock: 3 * time.Second, MaxAttempts: 2, ParkLease: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	go svc.Run(ctx)
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	deny := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "internal disabled", http.StatusForbidden) })
	}
	srv, err := gateway.NewServer(gateway.NewHandler(svc, gateway.HandlerConfig{}), gateway.ServerConfig{
		Public:       gateway.AuthConfig{Middleware: transport.Task(verifier, "ahp", "execution", authorize)},
		Internal:     gateway.AuthConfig{Middleware: deny},
		PublicPath:   "/a2a/public",
		InternalPath: "/a2a/internal",
		HealthPath:   "/a2a/healthz",
	})
	if err != nil {
		t.Fatal(err)
	}
	return ptMTLSServer(t, ca, client, srv)
}

func TestPublicFaceScopedTaskTokenFullScope(t *testing.T) {
	verifier, signer := ptVerifierSigner(t)
	ca := ptNewCA(t)
	tenant := "t-face-" + randSuffix()
	dep := "d-face"
	uri := "spiffe://ahp/tenant/" + tenant + "/deployment/" + dep

	// (1) HAPPY: valid workload cert + scoped task token -> COMPLETED (full scope).
	clientOK := ca.ptLeaf(t, "wl", uri, false)
	srvOK := ptFaceService(t, ca, clientOK, verifier)
	taskTok := ptTaskToken(t, signer, tenant, dep, "svc-run", "task-face-1")
	if got := ptDrive(t, srvOK, taskTok); got == nil || got.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("scoped task token did not drive COMPLETED via Public face (full scope expected): %+v", got)
	}

	// (2) FOREIGN workload: cert bound to a different tenant -> the token
	// (tenant/dep) mismatches the cert workload -> fail closed (rejected).
	other := ca.ptLeaf(t, "evil", "spiffe://ahp/tenant/"+tenant+"-foreign/deployment/"+dep, false)
	srvForeign := ptFaceService(t, ca, other, verifier)
	fTok := ptTaskToken(t, signer, tenant, dep, "svc-run", "task-face-2")
	if got := ptDrive(t, srvForeign, fTok); got != nil {
		t.Fatalf("foreign workload unexpectedly accepted: %+v", got)
	}

	// (3) HUMAN token (no deployment claim) against the workload face -> rejected.
	if got := ptDrive(t, srvOK, ptHumanToken(t, signer, tenant, "alice")); got != nil {
		t.Fatalf("human token unexpectedly drove the execution face to completion: %+v", got)
	}

	// (4) MISSING bearer token against a valid workload cert -> rejected.
	if got := ptDrive(t, srvOK, ""); got != nil {
		t.Fatalf("missing bearer token unexpectedly drove the execution face to completion: %+v", got)
	}
}

func TestPublicFacePlaintextFailClosed(t *testing.T) {
	_, store, ledger, sfx := gwSetup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	execStub := gateway.ExecutorFunc(func(_ context.Context, _ gateway.ExecInput) (gateway.ExecResult, error) {
		return gateway.ExecResult{State: a2a.TaskStateCompleted}, nil
	})
	svc, err := gateway.NewService(store, ledger, execStub, gateway.NullNotifier{}, gateway.Config{
		WorkerID: "face-pt-" + sfx[:8], Lease: 5 * time.Second, Poll: 10 * time.Millisecond,
		MaxBlock: 2 * time.Second, MaxAttempts: 2, ParkLease: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	go svc.Run(ctx)
	verifier, _ := ptVerifierSigner(t)
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	deny := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "internal disabled", http.StatusForbidden) })
	}
	srv, err := gateway.NewServer(gateway.NewHandler(svc, gateway.HandlerConfig{}), gateway.ServerConfig{
		Public:       gateway.AuthConfig{Middleware: transport.Task(verifier, "ahp", "execution", authorize)},
		Internal:     gateway.AuthConfig{Middleware: deny},
		PublicPath:   "/a2a/public",
		InternalPath: "/a2a/internal",
		HealthPath:   "/a2a/healthz",
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv) // plaintext: no verified client cert
	defer ts.Close()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/a2a/public", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{}}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer whatever")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("plaintext execution public face must fail closed 401/403, got %d", resp.StatusCode)
	}
}

// TestPublicFaceForgedOrIncompleteTaskTokenReject closes the refined test-gate
// (5de261fe): the execution Public face must reject a KindTask that lacks a
// trusted signature (wrong signing key), a tampered signature, or an incomplete
// scope (empty deployment). All must fail closed.
func TestPublicFaceForgedOrIncompleteTaskTokenReject(t *testing.T) {
	verifier, signer := ptVerifierSigner(t)
	ca := ptNewCA(t)
	tenant := "t-forged-" + randSuffix()
	dep := "d-forged"
	uri := "spiffe://ahp/tenant/" + tenant + "/deployment/" + dep
	clientOK := ca.ptLeaf(t, "wl", uri, false)
	srv := ptFaceService(t, ca, clientOK, verifier)

	// (5) KindTask signed by an UNTRUSTED key (same kid, wrong private key).
	_, wrongPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	evil, err := auth.NewSigner("ahp", "k1", wrongPriv)
	if err != nil {
		t.Fatal(err)
	}
	forged, err := evil.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: tenant, Deployment: dep,
		Subject: "svc", Task: "t-f", Root: "t-f", Context: "e", Grant: "g", Depth: 0}, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := ptDrive(t, srv, forged); got != nil {
		t.Fatalf("untrusted-signer KindTask unexpectedly accepted: %+v", got)
	}

	// (6) Tampered signature of an otherwise-valid scoped token.
	good := ptTaskToken(t, signer, tenant, dep, "svc", "task-tamper")
	segs := strings.Split(good, ".")
	if len(segs) != 3 {
		t.Fatalf("expected a 3-segment JWT, got %d", len(segs))
	}
	sig := []byte(segs[2])
	if sig[0] == 'A' {
		sig[0] = 'B'
	} else {
		sig[0] = 'A'
	}
	tampered := segs[0] + "." + segs[1] + "." + string(sig)
	if got := ptDrive(t, srv, tampered); got != nil {
		t.Fatalf("tampered-signature KindTask unexpectedly accepted: %+v", got)
	}

	// (7) Incomplete-scope KindTask (empty deployment): mint must refuse, and if
	// it did not, the face must still fail closed.
	inc, err := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: tenant, Deployment: "",
		Subject: "svc", Task: "t-i", Root: "t-i", Context: "e", Grant: "g", Depth: 0}, 10*time.Minute)
	if err != nil {
		t.Logf("incomplete-scope KindTask refused at mint (fail-closed): ok")
		return
	}
	if got := ptDrive(t, srv, inc); got != nil {
		t.Fatalf("incomplete-scope KindTask unexpectedly accepted: %+v", got)
	}
}
