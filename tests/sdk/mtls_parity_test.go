package sdk_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	toolssdk "github.com/ahp-platform/ahp/sdk/go/tools"
)

// The same mTLS scenario, driven from Go and from Python against one listener
// configured exactly like the product front (cmd/ahp buildTLSConfig:
// RequireAndVerifyClientCert, MinVersion TLS1.2, TLS1.3 negotiated). The workload
// certificate IS the deployment identity, so a client that presents none must be
// refused in the handshake -- and the point of this test is that a Python client
// can be made to present it without weakening anything.
//
// It also pins the trap that broke the standalone Python scenario: httpx 0.28
// create_ssl_context() returns as soon as it sees a string `verify`, so the
// deprecated `cert=` argument is never loaded. That client reaches the front with
// no certificate and the server alerts certificate_required. The fix is the SDK's
// mtls_context(); the fix is NOT a relaxed server.

type parityCA struct {
	cert  *x509.Certificate
	key   *ecdsa.PrivateKey
	pemCA []byte
}

// HTTP connection goroutines may still log a rejected handshake while the
// calling test inspects or clears the log. bytes.Buffer alone is not safe here.
type parityHandshakeLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *parityHandshakeLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *parityHandshakeLog) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *parityHandshakeLog) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

func parityCert(t *testing.T, cn string, isCA, server bool, parent *parityCA) (tls.Certificate, []byte, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		BasicConstraintsValid: true,
	}
	if isCA {
		tmpl.IsCA = true
		tmpl.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageCRLSign
	} else {
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
		if server {
			tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
			tmpl.DNSNames = []string{"localhost"}
		}
	}
	signer, signerKey := tmpl, key
	if parent != nil {
		signer, signerKey = parent.cert, parent.key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, signer, &key.PublicKey, signerKey)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: parsed}, append(certPEM, keyPEM...), key
}

func startParityFront(t *testing.T) (origin string, caFile, certFile, keyFile string, handshakeLog *parityHandshakeLog) {
	t.Helper()
	dir := t.TempDir()
	_, caMaterial, caKey := parityCert(t, "parity-ca", true, false, nil)
	if err := os.WriteFile(filepath.Join(dir, "ca.pem"), []byte(caMaterial), 0600); err != nil {
		t.Fatal(err)
	}
	// The CA PEM file holds certificate+key for convenience; the trust bundle the
	// clients load must contain only the certificate.
	caBlock, _ := pem.Decode([]byte(caMaterial))
	caFile = filepath.Join(dir, "ca-only.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(caBlock), 0600); err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	serverCert, serverMaterial, _ := parityCert(t, "parity-server", false, true, &parityCA{cert: caCert, key: caKey})
	certFile = filepath.Join(dir, "server.pem")
	if err := os.WriteFile(certFile, serverMaterial, 0600); err != nil {
		t.Fatal(err)
	}
	clientCert, clientMaterial, _ := parityCert(t, "parity-workload", false, false, &parityCA{cert: caCert, key: caKey})
	_ = clientCert
	certOnly := filepath.Join(dir, "client.crt")
	keyOnly := filepath.Join(dir, "client.key")
	clientBlock, rest := pem.Decode(clientMaterial)
	if err := os.WriteFile(certOnly, pem.EncodeToMemory(clientBlock), 0600); err != nil {
		t.Fatal(err)
	}
	keyBlock, _ := pem.Decode(rest)
	if err := os.WriteFile(keyOnly, pem.EncodeToMemory(keyBlock), 0600); err != nil {
		t.Fatal(err)
	}

	// One content-free owner operation for both languages: it echoes the identity
	// the server verified from the certificate, so a client that arrived without a
	// certificate cannot fake a pass.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tools/Get", func(w http.ResponseWriter, r *http.Request) {
		peer := ""
		if len(r.TLS.PeerCertificates) > 0 {
			peer = r.TLS.PeerCertificates[0].Subject.CommonName
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"key": "parity-1", "kind": "tool", "target": "parity", "status": "SUCCEEDED",
			"version": 1, "verified_peer": peer, "negotiated": tlsVersionName(r.TLS.Version),
		})
	})

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}
	handshakeLog = new(parityHandshakeLog)
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = tlsCfg
	srv.Config.ErrorLog = log.New(handshakeLog, "", 0)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv.URL, caFile, certOnly, keyOnly, handshakeLog
}

func pythonInterpreter(t *testing.T) string {
	t.Helper()
	candidates := []string{os.Getenv("AHP_SDK_PYTHON")}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	if found, err := exec.LookPath("python3"); err == nil {
		if err := exec.Command(found, "-c", "import httpx").Run(); err == nil {
			return found
		}
	}
	t.Skip("no Python interpreter with httpx available (set AHP_SDK_PYTHON); external prerequisite")
	return ""
}

func runPythonParity(t *testing.T, mode, origin, caFile, certFile, keyFile string) map[string]any {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"sdk/python/ahp_clients/tests/mtls_parity.py",
		"--origin", origin, "--ca", caFile, "--cert", certFile, "--key", keyFile, "--mode", mode}
	cmd := exec.Command(pythonInterpreter(t), args...)
	cmd.Dir = root
	pythonPath := filepath.Join(root, "sdk/python")
	if extra := os.Getenv("PYTHONPATH"); extra != "" {
		pythonPath += string(os.PathListSeparator) + extra
	}
	cmd.Env = append(os.Environ(), "PYTHONPATH="+pythonPath)
	out, _ := cmd.CombinedOutput()
	for _, line := range strings.Split(string(out), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "{") {
			var parsed map[string]any
			if json.Unmarshal([]byte(trimmed), &parsed) == nil {
				return parsed
			}
		}
	}
	t.Fatalf("python parity leg produced no verdict: %s", strings.TrimSpace(string(out)))
	return nil
}

func TestMTLSParityGoAndPythonOnOneFront(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	origin, caFile, certFile, keyFile, handshakeLog := startParityFront(t)

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("trust bundle unreadable")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	goClient, err := toolssdk.New(toolssdk.Config{BaseURL: origin, HTTPClient: &http.Client{Transport: transport},
		TokenSource: func(context.Context) (string, error) { return "task-token", nil }})
	if err != nil {
		t.Fatal(err)
	}
	goReceipt, err := goClient.Get(ctx, "parity-1")
	if err != nil {
		t.Fatalf("Go client could not reach the mTLS front: %v", err)
	}
	if goReceipt.Status != "SUCCEEDED" {
		t.Fatalf("Go receipt: %+v", goReceipt)
	}

	// Python, same front, same operation, the supported construction.
	py := runPythonParity(t, "helper", origin, caFile, certFile, keyFile)
	if py["status"] != "OK" {
		t.Fatalf("Python helper leg failed: %v", py)
	}
	receipt, _ := py["receipt"].(map[string]any)
	if receipt["status"] != "SUCCEEDED" {
		t.Fatalf("Python receipt: %v", py)
	}
	// Parity means the front verified the SAME workload identity from both clients.
	if fmt.Sprint(receipt["verified_peer"]) != "parity-workload" {
		t.Fatalf("Python arrived with a different identity: %v", receipt["verified_peer"])
	}
	// The verdict under test is about mutual TLS on modern TLS, so pin that the
	// helper leg really negotiated TLS 1.2/1.3 rather than landing on something a
	// weakened transport would allow. Which of the two depends on the interpreter's
	// TLS stack (CPython/OpenSSL negotiates 1.3; LibreSSL builds stop at 1.2).
	if name := fmt.Sprint(receipt["negotiated"]); name != "TLS1.3" && name != "TLS1.2" {
		t.Fatalf("Python helper leg did not negotiate a modern TLS version: %q", name)
	} else {
		t.Logf("python helper leg: %s as %v", name, receipt["verified_peer"])
	}

	// The trap, pinned: the deprecated httpx form reaches this front with no
	// certificate and is refused in the handshake. If this ever "passes", someone
	// relaxed the front instead of fixing the client.
	handshakeLog.Reset()
	broken := runPythonParity(t, "deprecated", origin, caFile, certFile, keyFile)
	if broken["status"] == "OK" {
		t.Fatal("the deprecated verify=<path> + cert= construction presented a certificate; this test's premise changed")
	}
	if !strings.Contains(handshakeLog.String(), "client didn't provide a certificate") {
		t.Fatalf("expected the front to refuse a certificate-less handshake, got: %q", handshakeLog.String())
	}
}

func tlsVersionName(version uint16) string {
	switch version {
	case tls.VersionTLS13:
		return "TLS1.3"
	case tls.VersionTLS12:
		return "TLS1.2"
	default:
		return "unknown"
	}
}
