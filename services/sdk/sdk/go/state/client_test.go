package state

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

type capture struct {
	mu     sync.Mutex
	paths  []string
	heads  []http.Header
	bodies []string
}

func (c *capture) record(r *http.Request, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paths = append(c.paths, r.URL.Path)
	c.heads = append(c.heads, r.Header.Clone())
	c.bodies = append(c.bodies, string(body))
}

func (c *capture) last(t *testing.T) (string, http.Header, string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.paths) == 0 {
		t.Fatal("server saw no request")
	}
	return c.paths[len(c.paths)-1], c.heads[len(c.heads)-1], c.bodies[len(c.bodies)-1]
}

func (c *capture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.paths)
}

// testTLS builds a fresh CA, server and client certificate material.
type testTLS struct {
	server *tls.Config
	client *tls.Config
}

func newTestTLS(t *testing.T) testTLS {
	t.Helper()
	sign := func(dns, common string, parentCert *x509.Certificate, parentKey *ecdsa.PrivateKey, isCA bool) tls.Certificate {
		t.Helper()
		priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber:          big.NewInt(time.Now().UnixNano()),
			Subject:               pkix.Name{CommonName: common},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              time.Now().Add(time.Hour),
			KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			DNSNames:              []string{dns},
			IsCA:                  isCA,
			BasicConstraintsValid: true,
		}
		signer := tmpl
		signerKey := any(priv)
		if parentCert != nil {
			signer = parentCert
			signerKey = parentKey
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, signer, &priv.PublicKey, signerKey)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
	}
	caTLSCert := sign("ahp.test", "ahp-test-ca", nil, nil, true)
	caParsed, err := x509.ParseCertificate(caTLSCert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	caKey := caTLSCert.PrivateKey.(*ecdsa.PrivateKey)
	srvTLSCert := sign("ahp.test", "server", caParsed, caKey, false)
	cliTLSCert := sign("ahp.test", "client", caParsed, caKey, false)
	pool := x509.NewCertPool()
	pool.AddCert(caParsed)
	return testTLS{
		server: &tls.Config{Certificates: []tls.Certificate{srvTLSCert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12},
		client: &tls.Config{Certificates: []tls.Certificate{cliTLSCert}, RootCAs: pool, ServerName: "ahp.test", MinVersion: tls.VersionTLS12},
	}
}

// newServer starts an httptest TLS server with mTLS and returns it.
func newServer(t *testing.T, material testTLS, fn http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(fn)
	srv.TLS = material.server
	srv.StartTLS()
	srv.Certificate().DNSNames = []string{"ahp.test"}
	t.Cleanup(srv.Close)
	return srv
}

func clientFor(t *testing.T, material testTLS, rawURL string, src TokenSource) *Client {
	t.Helper()
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: material.client}}
	c, err := New(Config{BaseURL: rawURL, HTTPClient: httpClient, TokenSource: src})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func staticToken(tok string) TokenSource {
	return func(context.Context) (string, error) { return tok, nil }
}

func TestNewValidatesBaseURL(t *testing.T) {
	tr := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	for _, bad := range []string{
		"", "http://ahp.test", "https://ahp.test/api", "https://ahp.test/?x=1",
		"https://ahp.test#frag", "https://user:pw@ahp.test", "https://ahp.test/%2e",
		"ahp.test", "https://", "https://ahp test", "https://ahp.test:99999",
	} {
		if _, err := New(Config{BaseURL: bad, HTTPClient: &http.Client{Transport: tr}}); err == nil {
			t.Errorf("BaseURL %q accepted", bad)
		} else if contracts.Code(err) != contracts.ErrInvalid {
			t.Errorf("BaseURL %q: code %s", bad, contracts.Code(err))
		}
	}
	if _, err := New(Config{BaseURL: "https://ahp.test", HTTPClient: nil}); err == nil {
		t.Error("nil HTTPClient accepted")
	}
	if _, err := New(Config{BaseURL: "https://ahp.test", HTTPClient: &http.Client{}}); err == nil {
		t.Error("HTTPClient without transport accepted")
	}
	c, err := New(Config{BaseURL: "https://ahp.test/", HTTPClient: &http.Client{Transport: tr}})
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if c.base != "https://ahp.test" {
		t.Errorf("base = %q", c.base)
	}
	if c.http.CheckRedirect == nil {
		t.Error("CheckRedirect not forced")
	}
}

func TestNewClonesClient(t *testing.T) {
	tr := &http.Transport{}
	hc := &http.Client{Transport: tr}
	c, err := New(Config{BaseURL: "https://ahp.test", HTTPClient: hc})
	if err != nil {
		t.Fatal(err)
	}
	if c.http == hc {
		t.Fatal("client not cloned")
	}
	if hc.CheckRedirect != nil {
		t.Error("caller client mutated")
	}
	if err := c.http.CheckRedirect(nil, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Errorf("clone CheckRedirect = %v", err)
	}
}

func TestTaskCallPathBodyHeaders(t *testing.T) {
	m := newTestTLS(t)
	var cap capture
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cap.record(r, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"th_1","context_id":"ctx_1","scope":{"tenant":"t","deployment":"d","subject":"s"},"epoch":3,"created_at":"2024-01-01T00:00:00Z"}`))
	})
	c := clientFor(t, m, srv.URL, staticToken("task-token"))
	th, err := c.CreateThread(context.Background(), "ctx_1", "key_1")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if th.ID != "th_1" || th.Epoch != 3 || th.Scope.Tenant != "t" {
		t.Errorf("decoded thread %+v", th)
	}
	path, head, body := cap.last(t)
	if path != "/v1/state/CreateThread" {
		t.Errorf("path = %q", path)
	}
	if got := head.Get("Authorization"); got != "Bearer task-token" {
		t.Errorf("authorization = %q", got)
	}
	if ct := head.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q", ct)
	}
	if body != `{"context_id":"ctx_1","key":"key_1"}` {
		t.Errorf("body = %s", body)
	}
	if strings.Contains(body, "scope") || strings.Contains(body, "tenant") ||
		strings.Contains(body, "subject") || strings.Contains(body, "deployment") {
		t.Errorf("body carries scope: %s", body)
	}
}

func TestAllTaskOperationsPathsAndBodies(t *testing.T) {
	m := newTestTLS(t)
	var cap capture
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cap.record(r, body)
		w.Header().Set("Content-Type", "application/json")
		switch strings.TrimPrefix(r.URL.Path, "/v1/state/") {
		case "GetThread":
			_, _ = w.Write([]byte(`{"id":"th","context_id":"c","scope":{"tenant":"t","deployment":"d","subject":"s"},"epoch":1,"created_at":"2024-01-01T00:00:00Z"}`))
		case "GetThreadByContext":
			_, _ = w.Write([]byte(`{"id":"th","context_id":"c","scope":{"tenant":"t","deployment":"d","subject":"s"},"epoch":1,"created_at":"2024-01-01T00:00:00Z"}`))
		case "CreateRun":
			_, _ = w.Write([]byte(`{"id":"run","thread_id":"th","task_id":"srv-task","status":"pending","sequence":1,"payload":{"a":1}}`))
		case "GetRun":
			_, _ = w.Write([]byte(`{"id":"run","thread_id":"th","task_id":"srv-task","status":"waiting","wait_for":"evt","sequence":2,"payload":null,"deadline":"2024-06-01T00:00:00Z"}`))
		case "ListRuns":
			_, _ = w.Write([]byte(`{"runs":[{"id":"run","thread_id":"th","task_id":"t","status":"pending","sequence":1,"payload":{}}]}`))
		default:
			_, _ = w.Write([]byte(`{"expires_at":"2024-02-01T00:00:00Z","ok":true}`))
		}
	})
	c := clientFor(t, m, srv.URL, staticToken("tok"))
	ctx := context.Background()
	dl := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	steps := []struct {
		name string
		call func() error
	}{
		{"GetThread", func() error { _, e := c.GetThread(ctx, "th"); return e }},
		{"GetThreadByContext", func() error { _, e := c.GetThreadByContext(ctx, "c"); return e }},
		{"CreateRun", func() error {
			_, e := c.CreateRun(ctx, "th", "k", json.RawMessage(`{"a":1}`))
			return e
		}},
		{"GetRun", func() error { _, e := c.GetRun(ctx, "run"); return e }},
		{"ListRuns", func() error { _, e := c.ListRuns(ctx, "th"); return e }},
		{"Heartbeat", func() error { _, e := c.Heartbeat(ctx, Fence{"run", "w", 4}, 5000); return e }},
		{"Release", func() error { return c.Release(ctx, Fence{"run", "w", 4}, RunDone) }},
		{"Suspend", func() error { return c.Suspend(ctx, Fence{"run", "w", 4}, "evt", &dl) }},
	}
	for _, s := range steps {
		if err := s.call(); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		path, _, body := cap.last(t)
		if want := "/v1/state/" + s.name; path != want {
			t.Errorf("%s: path %q want %q", s.name, path, want)
		}
		for _, banned := range []string{"scope", "tenant", "subject", "deployment", "task_id"} {
			if strings.Contains(body, banned) {
				t.Errorf("%s: body carries %q: %s", s.name, banned, body)
			}
		}
	}
	_, _, hb := cap.last(t)
	if !strings.Contains(hb, `"wait_for":"evt"`) || !strings.Contains(hb, "deadline") {
		t.Errorf("suspend body = %s", hb)
	}
}

func TestClaimRunNoAuthAndTokenPassthrough(t *testing.T) {
	m := newTestTLS(t)
	var cap capture
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cap.record(r, body)
		_, _ = w.Write([]byte(`{"lease":{"thread":{"id":"th","context_id":"c","scope":{"tenant":"t","deployment":"d","subject":"deployment"},"epoch":7,"created_at":"2024-01-01T00:00:00Z"},"run":{"id":"run","thread_id":"th","task_id":"task","status":"running","sequence":1,"payload":{}},"worker":"w","epoch":7,"expires_at":"2024-03-01T00:00:00Z","scope":{"tenant":"t","deployment":"d","subject":"deployment"}},"task_token":"fresh-token"}`))
	})
	c := clientFor(t, m, srv.URL, func(context.Context) (string, error) {
		t.Error("ClaimRun consulted the token source")
		return "", nil
	})
	claimed, err := c.ClaimRun(context.Background(), "w", 30000)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if claimed == nil || claimed.TaskToken != "fresh-token" || claimed.Lease.Epoch != 7 {
		t.Fatalf("claimed = %+v", claimed)
	}
	path, head, body := cap.last(t)
	if path != "/v1/state/ClaimRun" {
		t.Errorf("path = %q", path)
	}
	if head.Get("Authorization") != "" {
		t.Error("ClaimRun sent a bearer token")
	}
	if body != `{"worker":"w","ttl_ms":30000}` {
		t.Errorf("body = %s", body)
	}
}

func TestClaimRunEmptyLease(t *testing.T) {
	m := newTestTLS(t)
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"lease":null,"task_token":""}`))
	})
	c := clientFor(t, m, srv.URL, nil)
	claimed, err := c.ClaimRun(context.Background(), "w", 1000)
	if err != nil || claimed != nil {
		t.Fatalf("claimed=%+v err=%v", claimed, err)
	}
}

func TestClaimRunPayloadFilterWireAndValidation(t *testing.T) {
	m := newTestTLS(t)
	var cap capture
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cap.record(r, body)
		_, _ = w.Write([]byte(`{"lease":null,"task_token":""}`))
	})
	c := clientFor(t, m, srv.URL, func(context.Context) (string, error) {
		t.Error("claim must use workload mTLS, not a task token")
		return "", nil
	})
	claimed, err := c.ClaimRunWithFilter(context.Background(), "worker", 30000, ClaimFilter{RunID: "run", PayloadType: "turn"})
	if err != nil || claimed != nil {
		t.Fatalf("claimed=%+v err=%v", claimed, err)
	}
	path, head, body := cap.last(t)
	var got map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if path != "/v1/state/ClaimRun" || head.Get("Authorization") != "" || got["payload_type"] != "turn" || got["run_id"] != "run" || len(got) != 4 {
		t.Fatalf("unexpected claim: path=%s auth=%q body=%s", path, head.Get("Authorization"), body)
	}
	for _, bad := range []string{strings.Repeat("x", 129), string([]byte{0xff})} {
		if _, err := c.ClaimRunWithFilter(context.Background(), "worker", 30000, ClaimFilter{PayloadType: bad}); err == nil {
			t.Error("invalid filter accepted")
		}
	}
	if cap.count() != 1 {
		t.Fatal("invalid filters reached the network")
	}
}

func TestTokenSourceErrorAndEmptyFailClosed(t *testing.T) {
	m := newTestTLS(t)
	var hits int
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) { hits++ })
	for name, src := range map[string]TokenSource{
		"error": func(context.Context) (string, error) { return "", errors.New("secret endpoint https://issuer") },
		"empty": func(context.Context) (string, error) { return "   ", nil },
		"nil":   nil,
	} {
		c := clientFor(t, m, srv.URL, src)
		_, err := c.GetThread(context.Background(), "th")
		if err == nil || contracts.Code(err) != contracts.ErrForbidden {
			t.Errorf("%s: err = %v", name, err)
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "issuer") {
			t.Errorf("%s: error leaks source detail: %v", name, err)
		}
	}
	if hits != 0 {
		t.Errorf("server received %d requests, want 0", hits)
	}
}

func TestRedirectRefusedNoCredentialForwarded(t *testing.T) {
	m := newTestTLS(t)
	var destAuth string
	var destHits int
	dest := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		destHits++
		destAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{}`))
	})
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", dest.URL)
		w.WriteHeader(http.StatusFound)
	})
	c := clientFor(t, m, srv.URL, staticToken("super-secret"))
	_, err := c.GetThread(context.Background(), "th")
	if err == nil {
		t.Fatal("redirect accepted")
	}
	if destHits != 0 {
		t.Errorf("redirect destination received %d requests", destHits)
	}
	if destAuth != "" {
		t.Error("credential forwarded to redirect destination")
	}
	if strings.Contains(err.Error(), "super-secret") {
		t.Error("error leaks bearer")
	}
}

func TestResponseBoundAndDecodeErrors(t *testing.T) {
	m := newTestTLS(t)
	t.Run("oversized", func(t *testing.T) {
		srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = w.Write([]byte(strings.Repeat("a", maxResponseBytes+1)))
		})
		c := clientFor(t, m, srv.URL, staticToken("t"))
		_, err := c.GetThread(context.Background(), "th")
		if err == nil || !strings.Contains(err.Error(), "16 MiB") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("malformed", func(t *testing.T) {
		srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = w.Write([]byte(`{"epoch":`))
		})
		c := clientFor(t, m, srv.URL, staticToken("t"))
		_, err := c.GetThread(context.Background(), "th")
		if err == nil || contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestErrorContractParsedAndSanitized(t *testing.T) {
	m := newTestTLS(t)
	cases := []struct {
		status int
		body   string
		code   string
	}{
		{http.StatusNotFound, "{\"code\":\"NOT_FOUND\",\"message\":\"no such run\"}\n", contracts.ErrNotFound},
		{http.StatusConflict, "{\"code\":\"STALE_EPOCH\",\"message\":\"lease superseded\"}", contracts.ErrStaleEpoch},
		{http.StatusConflict, "{\"code\":\"LEASE_LOST\",\"message\":\"gone\"}", contracts.ErrLeaseLost},
		{http.StatusBadRequest, "{\"code\":\"KEY_REUSE_MISMATCH\",\"message\":\"other\"}", contracts.ErrKeyReuse},
	}
	for _, tc := range cases {
		srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(tc.body))
		})
		c := clientFor(t, m, srv.URL, staticToken("t"))
		_, err := c.GetThread(context.Background(), "th")
		if contracts.Code(err) != tc.code {
			t.Errorf("status %d: code = %q want %q (%v)", tc.status, contracts.Code(err), tc.code, err)
		}
	}
	t.Run("unknown code sanitized", func(t *testing.T) {
		srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"code":"WEIRD_CODE","message":"stack trace bearer abc"}`))
		})
		c := clientFor(t, m, srv.URL, staticToken("t"))
		_, err := c.GetThread(context.Background(), "th")
		var ce *contracts.Error
		if !errors.As(err, &ce) || ce.Code != "WEIRD_CODE" {
			t.Fatalf("err = %v", err)
		}
		if strings.Contains(ce.Message, "stack trace") || strings.Contains(ce.Message, "abc") {
			t.Errorf("message not sanitized: %q", ce.Message)
		}
	})
	t.Run("non-json body not echoed", func(t *testing.T) {
		secret := "password=hunter2 " + strings.Repeat("x", 2000)
		srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(secret))
		})
		c := clientFor(t, m, srv.URL, staticToken("t"))
		_, err := c.GetThread(context.Background(), "th")
		if err == nil || strings.Contains(err.Error(), "hunter2") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestCanceledContextNoRequest(t *testing.T) {
	m := newTestTLS(t)
	var hits int
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) { hits++ })
	c := clientFor(t, m, srv.URL, staticToken("t"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.GetThread(ctx, "th"); err == nil {
		t.Fatal("canceled context accepted")
	}
	if hits != 0 {
		t.Errorf("server hit %d times", hits)
	}
	// Cancellation during the token source also stops the call.
	ctx2, cancel2 := context.WithCancel(context.Background())
	src := func(context.Context) (string, error) { cancel2(); return "", errors.New("boom") }
	c2 := clientFor(t, m, srv.URL, src)
	if _, err := c2.GetThread(ctx2, "th"); err == nil {
		t.Fatal("expected error")
	}
	if hits != 0 {
		t.Errorf("server hit %d times", hits)
	}
}

func TestInputValidation(t *testing.T) {
	m := newTestTLS(t)
	var hits int
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) { hits++ })
	c := clientFor(t, m, srv.URL, staticToken("t"))
	ctx := context.Background()
	bad := []func() error{
		func() error { _, e := c.CreateThread(ctx, "", "k"); return e },
		func() error { _, e := c.CreateThread(ctx, "c", ""); return e },
		func() error { _, e := c.GetThread(ctx, ""); return e },
		func() error { _, e := c.GetThreadByContext(ctx, ""); return e },
		func() error { _, e := c.CreateRun(ctx, "", "k", json.RawMessage(`{}`)); return e },
		func() error { _, e := c.CreateRun(ctx, "t", "", json.RawMessage(`{}`)); return e },
		func() error { _, e := c.CreateRun(ctx, "t", "k", nil); return e },
		func() error { _, e := c.CreateRun(ctx, "t", "k", json.RawMessage(`{`)); return e },
		func() error { _, e := c.GetRun(ctx, ""); return e },
		func() error { _, e := c.ListRuns(ctx, ""); return e },
		func() error { _, e := c.ClaimRun(ctx, "", 1000); return e },
		func() error { _, e := c.ClaimRun(ctx, "w", 0); return e },
		func() error { _, e := c.ClaimRun(ctx, "w", -5); return e },
		func() error { _, e := c.ClaimRun(ctx, "w", math.MaxInt64); return e },
		func() error { _, e := c.Heartbeat(ctx, Fence{"", "w", 1}, 1000); return e },
		func() error { _, e := c.Heartbeat(ctx, Fence{"r", "", 1}, 1000); return e },
		func() error { _, e := c.Heartbeat(ctx, Fence{"r", "w", 0}, 1000); return e },
		func() error { _, e := c.Heartbeat(ctx, Fence{"r", "w", 1}, math.MaxInt64); return e },
		func() error { return c.Release(ctx, Fence{"r", "w", -1}, RunDone) },
		func() error { return c.Release(ctx, Fence{"r", "w", 1}, RunPending) },
		func() error { return c.Release(ctx, Fence{"r", "w", 1}, "") },
		func() error { return c.Suspend(ctx, Fence{"r", "w", 1}, "", nil) },
	}
	for i, f := range bad {
		if err := f(); contracts.Code(err) != contracts.ErrInvalid {
			t.Errorf("case %d: err = %v", i, err)
		}
	}
	if hits != 0 {
		t.Errorf("server hit %d times, want 0", hits)
	}
}

func TestNoAutomaticRetry(t *testing.T) {
	m := newTestTLS(t)
	var hits int
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("nope"))
	})
	c := clientFor(t, m, srv.URL, staticToken("t"))
	if _, err := c.GetThread(context.Background(), "th"); err == nil {
		t.Fatal("expected error")
	}
	if hits != 1 {
		t.Errorf("server hit %d times, want exactly 1", hits)
	}
}

func TestTaskTokenNotStoredGlobally(t *testing.T) {
	m := newTestTLS(t)
	var cap capture
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cap.record(r, body)
		_, _ = w.Write([]byte(`{"id":"th","context_id":"c","scope":{"tenant":"t","deployment":"d","subject":"s"},"epoch":1,"created_at":"2024-01-01T00:00:00Z"}`))
	})
	n := 0
	c := clientFor(t, m, srv.URL, func(context.Context) (string, error) {
		n++
		return "tok-" + string(rune('0'+n)), nil
	})
	for i := 0; i < 3; i++ {
		if _, err := c.GetThread(context.Background(), "th"); err != nil {
			t.Fatal(err)
		}
	}
	cap.mu.Lock()
	defer cap.mu.Unlock()
	for i, h := range cap.heads {
		if want := "Bearer tok-" + string(rune('0'+i+1)); h.Get("Authorization") != want {
			t.Errorf("call %d auth = %q want %q", i, h.Get("Authorization"), want)
		}
	}
}

func TestDeleteThread(t *testing.T) {
	m := newTestTLS(t)
	var cap capture
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cap.record(r, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	c := clientFor(t, m, srv.URL, staticToken("tok"))
	ctx := context.Background()

	// DeleteThread sends only {thread_id} per contract.
	if err := c.DeleteThread(ctx, "thread-abc"); err != nil {
		t.Fatalf("DeleteThread: %v", err)
	}
	path, _, body := cap.last(t)
	if path != "/v1/state/DeleteThread" {
		t.Errorf("path=%q", path)
	}
	if !strings.Contains(body, "thread-abc") {
		t.Errorf("body missing thread_id: %s", body)
	}

	// Empty thread_id should fail locally.
	err := c.DeleteThread(ctx, "")
	if err == nil {
		t.Fatal("expected error for empty thread_id")
	}
}

// DEF-RELEASE-REASON-1 pinned on the public wire. The door refuses a failed or canceled
// end with nobody saying why, so this SDK must be able to carry both halves of a reason -
// and a caller with nothing to state must not move one byte, or every existing sender's
// request changes shape and an old peer starts refusing releases it used to accept.
func TestReleaseCarriesBothHalvesOfAReason(t *testing.T) {
	m := newTestTLS(t)
	var cap capture
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cap.record(r, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	c := clientFor(t, m, srv.URL, staticToken("tok"))
	ctx := context.Background()
	f := Fence{"run", "w", 4}

	if err := c.Release(ctx, f, RunFailed); err != nil {
		t.Fatal(err)
	}
	if _, _, plain := cap.last(t); plain != `{"run_id":"run","worker":"w","epoch":4,"status":"failed"}` {
		t.Errorf("an unstated release must keep the historical wire form, got %s", plain)
	}
	if err := c.ReleaseWithReason(ctx, f, RunCanceled, "canceled"); err != nil {
		t.Fatal(err)
	}
	if _, _, one := cap.last(t); one != `{"run_id":"run","worker":"w","epoch":4,"status":"canceled","close_reason":"canceled"}` {
		t.Errorf("the dictionary word changed the wire form: %s", one)
	}
	const cause = "agent preflight refused the run: it is not rooted on its own child grant"
	if err := c.ReleaseWithReasonCause(ctx, f, RunFailed, "error", cause); err != nil {
		t.Fatal(err)
	}
	_, _, both := cap.last(t)
	for _, want := range []string{`"close_reason":"error"`, `"failure_cause":"` + cause + `"`} {
		if !strings.Contains(both, want) {
			t.Errorf("release body lost %s: %s", want, both)
		}
	}
	for _, banned := range []string{"scope", "tenant", "subject", "deployment"} {
		if strings.Contains(both, banned) {
			t.Errorf("release body carries %q: %s", banned, both)
		}
	}
}
