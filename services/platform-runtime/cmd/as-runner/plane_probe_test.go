package main

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
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"
)

// testPK is one certificate authority with a leaf signed by it, held in memory: the probe
// is about WHO trusts WHOM, and that can only be shown by real handshakes.
type testPK struct {
	leaf      tls.Certificate
	leafPEM   []byte
	pool      *x509.CertPool
	serverTLS *tls.Config
}

func newTestPK(t *testing.T, common string) *testPK {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "h24-probe-" + common},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano() + 1),
		Subject:      pkix.Name{CommonName: "h24-probe-" + common + "-leaf"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, parsed, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatalf("leaf: %v", err)
	}
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	return &testPK{
		leaf:    tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey},
		leafPEM: leafPEM,
		pool:    pool,
		serverTLS: &tls.Config{
			Certificates: []tls.Certificate{{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}},
		},
	}
}

// trustPlane returns an httptest TLS server that presents pk's leaf and ACCEPTS client
// certificates signed by pk. requireClient decides whether a client certificate is demanded
// at all -- a plane that demands one is a plane that can refuse ours.
func trustPlane(t *testing.T, pk *testPK, requireClient bool) *httptest.Server {
	t.Helper()
	cfg := &tls.Config{Certificates: pk.serverTLS.Certificates}
	if requireClient {
		cfg.ClientCAs = pk.pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// A plane refuses an anonymous request; the probe reads that as "the handshake worked".
		w.WriteHeader(http.StatusForbidden)
	}))
	srv.TLS = cfg
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func clientFor(workload tls.Certificate, roots *x509.CertPool) *http.Client {
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{workload},
		RootCAs:      roots,
	}}}
}

func captureLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// TestProbePlaneNamesThePlaneThatRefusesOurWorkloadCertificate is DEF-H24-11 itself: the
// plane trusts a different CA than the one Space's certificate came from, and the journal
// must say WHICH plane and WHAT happened instead of leaving the operator with a 403 later.
func TestProbePlaneNamesThePlaneThatRefusesOurWorkloadCertificate(t *testing.T) {
	planeCA := newTestPK(t, "plane")
	ourCA := newTestPK(t, "ours")
	srv := trustPlane(t, planeCA, true)

	res := probePlane(context.Background(), planeTarget{Name: "state", URL: srv.URL}, clientFor(ourCA.leaf, planeCA.pool))
	if res.Plane != "state" {
		t.Fatalf("the result must name the plane, got %q", res.Plane)
	}
	if res.Outcome != planeProbeWorkloadRejected {
		t.Fatalf("outcome = %q (%s), want %q", res.Outcome, res.Detail, planeProbeWorkloadRejected)
	}
	if res.Detail == "" {
		t.Fatal("the operator needs the transport reason, detail is empty")
	}

	logger, buf := captureLogger()
	logPlaneProbes(logger, []planeProbeResult{res})
	lines := decodeLines(t, buf.String())
	var probe, summary map[string]any
	for _, l := range lines {
		switch {
		case strings.Contains(fmt.Sprint(l["msg"]), "workload certificate probe"):
			probe = l
		case strings.Contains(fmt.Sprint(l["msg"]), "refused by at least one configured plane"):
			summary = l
		}
	}
	if probe == nil || probe["plane"] != "state" || probe["outcome"] != planeProbeWorkloadRejected {
		t.Fatalf("probe line must name plane and outcome, got %v", probe)
	}
	if probe["level"] != "ERROR" {
		t.Fatalf("a refused certificate must be loud, got level %v", probe["level"])
	}
	if summary == nil || !strings.Contains(fmt.Sprint(summary["planes"]), "state="+planeProbeWorkloadRejected) {
		t.Fatalf("summary must list the refusing plane, got %v", summary)
	}
}

// TestProbePlaneDistinguishesAnUntrustedPlaneCertificate: the mirror case. The runner points
// at a plane from another estate (or holds a stale CA bundle); the remedy is different, so
// the outcome must be different.
func TestProbePlaneDistinguishesAnUntrustedPlaneCertificate(t *testing.T) {
	planeCA := newTestPK(t, "plane2")
	ourCA := newTestPK(t, "ours2")
	srv := trustPlane(t, planeCA, false)

	res := probePlane(context.Background(), planeTarget{Name: "artifacts", URL: srv.URL}, clientFor(ourCA.leaf, ourCA.pool))
	if res.Outcome != planeProbeCAUntrusted {
		t.Fatalf("outcome = %q (%s), want %q", res.Outcome, res.Detail, planeProbeCAUntrusted)
	}
	if res.Plane != "artifacts" {
		t.Fatalf("plane = %q", res.Plane)
	}
}

// TestProbePlaneReportsReachableWhenBothSidesTrust also pins the negative: a 403 from the
// plane is NOT a certificate problem, and the probe must not turn an authorization answer
// into a TLS alarm.
func TestProbePlaneReportsReachableWhenBothSidesTrust(t *testing.T) {
	pk := newTestPK(t, "mutual")
	srv := trustPlane(t, pk, true)

	res := probePlane(context.Background(), planeTarget{Name: "identity", URL: srv.URL}, clientFor(pk.leaf, pk.pool))
	if res.Outcome != planeProbeReachable {
		t.Fatalf("outcome = %q (%s), want %q", res.Outcome, res.Detail, planeProbeReachable)
	}
	if res.Status != http.StatusForbidden {
		t.Fatalf("status = %d, want the plane's anonymous 403 recorded verbatim", res.Status)
	}
	logger, buf := captureLogger()
	logPlaneProbes(logger, []planeProbeResult{res})
	for _, l := range decodeLines(t, buf.String()) {
		if strings.Contains(fmt.Sprint(l["msg"]), "refused by at least one configured plane") {
			t.Fatal("a healthy plane must not raise the refusal summary")
		}
	}
}

// TestProbePlaneSkipsPlainHTTPTargets: there is no certificate exchange on http, and
// pretending there were would be the exact kind of green lie this probe exists to prevent.
func TestProbePlaneSkipsPlainHTTPTargets(t *testing.T) {
	res := probePlane(context.Background(), planeTarget{Name: "state", URL: "http://127.0.0.1:1"}, http.DefaultClient)
	if res.Outcome != planeProbePlainScheme {
		t.Fatalf("outcome = %q, want %q", res.Outcome, planeProbePlainScheme)
	}
}

// TestProbePlaneReportsAnUnreachablePlane: a plane that is down must read as down, not as a
// certificate problem -- otherwise the operator fixes the wrong thing.
func TestProbePlaneReportsAnUnreachablePlane(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	res := probePlane(context.Background(), planeTarget{Name: "identity_exchange", URL: "https://" + addr}, http.DefaultClient)
	if res.Outcome != planeProbeUnreachable {
		t.Fatalf("outcome = %q (%s), want %q", res.Outcome, res.Detail, planeProbeUnreachable)
	}
}

// TestClassifyProbeErrorKeepsTheEnumsDistinct is the table behind the outcomes, including
// the wrapped shapes net/http actually returns.
func TestClassifyProbeErrorKeepsTheEnumsDistinct(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"peer alert", fmt.Errorf("Get \"https://x\": remote error: tls: bad certificate"), planeProbeWorkloadRejected},
		{"peer alert unknown authority", fmt.Errorf("Get \"https://x\": remote error: tls: unknown certificate authority"), planeProbeWorkloadRejected},
		{"unknown authority", fmt.Errorf("Get \"https://x\": tls: failed to verify: x509: certificate signed by unknown authority"), planeProbeCAUntrusted},
		{"hostname", &url.Error{Op: "Get", URL: "https://x", Err: x509.HostnameError{Certificate: &x509.Certificate{DNSNames: []string{"other"}}, Host: "x"}}, planeProbeNameMismatch},
		{"timeout", &url.Error{Op: "Get", URL: "https://x", Err: context.DeadlineExceeded}, planeProbeUnreachable},
		{"refused", &url.Error{Op: "Get", URL: "https://x", Err: syscall.ECONNREFUSED}, planeProbeUnreachable},
		{"plain failure", fmt.Errorf("something else entirely"), planeProbeTLSFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, detail := classifyProbeError(tc.err)
			if got != tc.want {
				t.Fatalf("classifyProbeError() = %q, want %q (err %v)", got, tc.want, tc.err)
			}
			if detail == "" {
				t.Fatal("detail must carry the reason")
			}
		})
	}
	if got, _ := classifyProbeError(nil); got != planeProbeReachable {
		t.Fatalf("nil error must read as reachable, got %q", got)
	}
}

// TestPlaneTargetsFromConfigNamesEveryConfiguredPlane pins the journal vocabulary and the
// fail-open-on-empty rule: an unset plane is absent, not reported as broken.
func TestPlaneTargetsFromConfigNamesEveryConfiguredPlane(t *testing.T) {
	targets := planeTargetsFromConfig(config{
		StateURL:            "https://front:29612",
		IdentityExchangeURL: "https://front:29612/v1/identity/control/exchange",
		IdentityURL:         "https://front:29612",
		ArtifactsURL:        "https://front:29612",
	})
	want := []string{"state", "identity_exchange", "identity", "artifacts"}
	if len(targets) != len(want) {
		t.Fatalf("targets = %d, want %d", len(targets), len(want))
	}
	for i, name := range want {
		if targets[i].Name != name {
			t.Fatalf("target %d = %q, want %q", i, targets[i].Name, name)
		}
	}
	if got := planeTargetsFromConfig(config{StateURL: "https://a", ArtifactsURL: "  "}); len(got) != 1 || got[0].Name != "state" {
		t.Fatalf("an unset plane must be absent, got %+v", got)
	}
}

// TestProbeDetailCarriesNoKeyMaterial: the detail line goes to the journal, so it must stay a
// one-line transport reason and never grow into a chain or a key.
func TestProbeDetailCarriesNoKeyMaterial(t *testing.T) {
	planeCA := newTestPK(t, "leakplane")
	ourCA := newTestPK(t, "leakours")
	srv := trustPlane(t, planeCA, true)
	res := probePlane(context.Background(), planeTarget{Name: "state", URL: srv.URL}, clientFor(ourCA.leaf, planeCA.pool))
	for _, bad := range []string{"BEGIN ", "PRIVATE KEY", "CERTIFICATE"} {
		if strings.Contains(res.Detail, bad) {
			t.Fatalf("detail carries certificate or key material (%q): %q", bad, res.Detail)
		}
	}
	if len(res.Detail) > 240 {
		t.Fatalf("detail must be bounded to one line, got %d bytes", len(res.Detail))
	}
	if strings.Contains(res.Detail, string(ourCA.leafPEM)) {
		t.Fatal("detail must never echo certificate bytes")
	}
}

func decodeLines(t *testing.T, out string) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("journal line is not JSON: %v (%s)", err, line)
		}
		lines = append(lines, m)
	}
	return lines
}
