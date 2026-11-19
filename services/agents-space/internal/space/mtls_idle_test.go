package space

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type idleTLSFixture struct {
	certFile, keyFile, caFile string
	server                    tls.Certificate
	roots                     *x509.CertPool
}

func newIdleTLSFixture(t *testing.T) idleTLSFixture {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal("fixture CA generation failed")
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "idle-fixture-ca"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal("fixture CA signing failed")
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	issue := func(serial int64, usage x509.ExtKeyUsage) (tls.Certificate, []byte, []byte) {
		pub, leafKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal("fixture key generation failed")
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "idle-fixture-leaf"},
			NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, pub, key)
		if err != nil {
			t.Fatal("fixture leaf signing failed")
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
		if err != nil {
			t.Fatal("fixture key encoding failed")
		}
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
		pair, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			t.Fatal("fixture keypair failed")
		}
		return pair, certPEM, keyPEM
	}
	server, _, _ := issue(2, x509.ExtKeyUsageServerAuth)
	_, certPEM, keyPEM := issue(3, x509.ExtKeyUsageClientAuth)
	dir := t.TempDir()
	f := idleTLSFixture{certFile: filepath.Join(dir, "client.crt"), keyFile: filepath.Join(dir, "client.key"),
		caFile: filepath.Join(dir, "ca.crt"), server: server, roots: x509.NewCertPool()}
	f.roots.AppendCertsFromPEM(caPEM)
	for name, data := range map[string][]byte{f.certFile: certPEM, f.keyFile: keyPEM, f.caFile: caPEM} {
		if err := os.WriteFile(name, data, 0600); err != nil {
			t.Fatal("fixture file creation failed")
		}
	}
	return f
}

func TestMTLSIdlePoolReusesConnectionsAcrossConcurrentWaves(t *testing.T) {
	f := newIdleTLSFixture(t)
	const concurrent = 8
	arrivals := []chan struct{}{make(chan struct{}, concurrent), make(chan struct{}, concurrent)}
	release := []chan struct{}{make(chan struct{}), make(chan struct{})}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wave, err := strconv.Atoi(r.URL.Query().Get("wave"))
		body, readErr := io.ReadAll(r.Body)
		if err != nil || readErr != nil || string(body) != "fixture-body-"+strconv.Itoa(wave) || r.Method != http.MethodPost || wave < 0 || wave > 1 || r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || r.ProtoMajor != 1 || r.Header.Get("Authorization") != "Bearer fixture-"+strconv.Itoa(wave) {
			http.Error(w, "fixture authority/protocol mismatch", http.StatusForbidden)
			return
		}
		arrivals[wave] <- struct{}{}
		select {
		case <-release[wave]:
			_, _ = io.WriteString(w, "ok")
		case <-r.Context().Done():
		}
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{f.server}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: f.roots, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	defer srv.Close()
	client, err := NewMTLSClient(f.certFile, f.keyFile, f.caFile)
	if err != nil {
		t.Fatal("production mTLS client construction failed")
	}
	defer client.CloseIdleConnections()
	var handshakes, reusedSecond atomic.Int32
	for wave := 0; wave < 2; wave++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		failures := make(chan bool, concurrent)
		var wg sync.WaitGroup
		for i := 0; i < concurrent; i++ {
			wg.Add(1)
			go func(wave int) {
				defer wg.Done()
				trace := &httptrace.ClientTrace{TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
					if err == nil {
						handshakes.Add(1)
					}
				}, GotConn: func(info httptrace.GotConnInfo) {
					if wave == 1 && info.Reused {
						reusedSecond.Add(1)
					}
				}}
				req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodPost, srv.URL+"?wave="+strconv.Itoa(wave), strings.NewReader("fixture-body-"+strconv.Itoa(wave)))
				if err != nil {
					failures <- true
					return
				}
				req.Header.Set("Authorization", "Bearer fixture-"+strconv.Itoa(wave))
				resp, err := client.Do(req)
				if err != nil {
					failures <- true
					return
				}
				_, readErr := io.Copy(io.Discard, resp.Body)
				closeErr := resp.Body.Close()
				if readErr != nil || closeErr != nil || resp.StatusCode != http.StatusOK {
					failures <- true
				}
			}(wave)
		}
		arrived := true
		for i := 0; i < concurrent; i++ {
			select {
			case <-arrivals[wave]:
			case <-ctx.Done():
				arrived = false
			}
		}
		close(release[wave])
		wg.Wait()
		cancel()
		if !arrived || len(failures) != 0 {
			t.Fatal("concurrent authenticated HTTP/1.1 wave failed")
		}
	}
	if handshakes.Load() != concurrent || reusedSecond.Load() != concurrent {
		t.Fatalf("second wave created new TLS handshakes: total=%d second_wave_reused=%d want=%d", handshakes.Load(), reusedSecond.Load(), concurrent)
	}
	t.Logf("real_mtls concurrent_per_wave=%d waves=2 total_handshakes=%d second_wave_reused=%d", concurrent, handshakes.Load(), reusedSecond.Load())
}

func TestMTLSIdlePolicyStillRejectsMissingClientCertificate(t *testing.T) {
	f := newIdleTLSFixture(t)
	var handled atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handled.Add(1) }))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{f.server}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: f.roots, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	defer srv.Close()
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: f.roots, MinVersion: tls.VersionTLS12}}, Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	resp, err := client.Get(srv.URL)
	if resp != nil {
		resp.Body.Close()
	}
	if err == nil || handled.Load() != 0 {
		t.Fatal("missing workload client certificate reached the handler")
	}
	if _, err := NewMTLSClient("", "", f.caFile); err == nil {
		t.Fatal("production constructor accepted missing credential files")
	}
}
