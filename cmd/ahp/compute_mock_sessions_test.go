//go:build ahp_exec

package main

// The session facade, over the wire it will actually be used on.
//
// The unit tests in internal/compute prove what a MOCK session is. This file proves the
// two things only the mount can prove:
//
//  1. a deployment that did not opt into the mock does NOT get a plausible session. It
//     gets 503 UNAVAILABLE naming the missing facade. A route that answered an empty
//     session here would be the exact overclaim this plane has had to retract twice.
//  2. with the mock mounted, every answer - including refusals - carries the backend and
//     the disclosure header, the caller's scope comes from the client certificate and not
//     from the body, and a foreign caller cannot reach someone else's Computer.
//
// Like compute_mock_lifecycle_test.go this is about disclosure and authority, not about
// hypervisors: the facade starts no shell and no browser, so nothing measured here is
// evidence about isolation, escape resistance, latency or density.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/compute/computeapi"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestComputeSessionFacadeIsHonestAboutBeingMounted(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Fatal("isolated real PostgreSQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	store := compute.New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	redis := compute.NewRedis(os.Getenv("AHP_REDIS_ADDR"))
	if err := redis.Ping(ctx); err != nil {
		t.Fatal("real Redis required for the lease layer: " + err.Error())
	}
	leases, err := compute.NewLeaseService(store, redis)
	if err != nil {
		t.Fatal(err)
	}
	registry := compute.NewRegistry(compute.NewHostBackend(t.TempDir()), compute.NewMockBackend())
	registry.SetClassBackend(compute.ClassX, compute.MockBackendName)
	runner := compute.NewRunner(store, registry, t.TempDir(), "h22-session-node", nil).WithLeaseService(leases)

	owner := contracts.Scope{Tenant: "h22-sess-" + randHexTest(), Deployment: "agent", Subject: "alice"}
	other := contracts.Scope{Tenant: "h22-tenant-" + randHexTest(), Deployment: "agent", Subject: "mallory"}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, err := auth.NewSigner("h22-sess", "key", priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("h22-sess", map[string]ed25519.PublicKey{"key": pub})
	mint := func(sc contracts.Scope) string {
		t.Helper()
		if _, err := ids.CreateRoot(ctx, sc, identity.RootRequest{
			Task: "root", Context: sc.Tenant + ":" + sc.Subject + ":" + sc.Deployment + ":root",
			Deployment: sc.Deployment, Capabilities: []string{"model", "tools", "compute", "execute"}, TTL: time.Hour,
		}); err != nil {
			t.Fatal(err)
		}
		token, err := ids.MintTask(ctx, sc, "root", signer, "ahp", 10*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	ownerToken, otherToken := mint(owner), mint(other)
	backendName, marker := registry.Disclosure()

	ca := newTestCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)

	// serve mounts a plane the way role_compute does - behind the disclosing wrapper -
	// and returns a caller that speaks mTLS as one workload identity.
	serve := func(plane http.Handler) (*httptest.Server, func(contracts.Scope) *http.Client) {
		t.Helper()
		mux := http.NewServeMux()
		mountComputePlane(mux, disclosingPlane{backend: backendName, marker: marker, next: plane})
		srv := httptest.NewUnstartedServer(mux)
		srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
		srv.StartTLS()
		t.Cleanup(srv.Close)
		return srv, func(sc contracts.Scope) *http.Client {
			hc := *srv.Client()
			tr := srv.Client().Transport.(*http.Transport).Clone()
			tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(sc))}
			hc.Transport = tr
			return &hc
		}
	}

	// call is the only way out, and it fails on an answer that does not disclose itself -
	// including on a 4xx/5xx, which is where silence is most likely to be read as success.
	call := func(srv *httptest.Server, hc *http.Client, op, tok string, body any) map[string]any {
		t.Helper()
		raw, _ := json.Marshal(body)
		req, err := http.NewRequest(http.MethodPost, srv.URL+computeMountPrefix+op, bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+tok)
		res, err := hc.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		payload, _ := io.ReadAll(res.Body)
		if got := res.Header.Get(headerBackend); got != backendName {
			t.Fatalf("%s: %s = %q, want %q", op, headerBackend, got, backendName)
		}
		if got := res.Header.Get(headerDisclosure); got != compute.MockMarker {
			t.Fatalf("%s: %s = %q, want the mock marker verbatim", op, headerDisclosure, got)
		}
		var out map[string]any
		_ = json.Unmarshal(payload, &out)
		if out == nil {
			out = map[string]any{"raw": string(payload)}
		}
		out["__status"] = res.StatusCode
		out["__raw"] = string(payload)
		return out
	}
	str := func(body map[string]any, key string) string {
		if v, ok := body[key].(string); ok {
			return v
		}
		return ""
	}

	// --- the unmounted case: 503 with a reason, never a session-shaped lie ------------
	plain, err := computeapi.New(store, verifier, ids.Authorize, runner)
	if err != nil {
		t.Fatal(err)
	}
	plainSrv, plainClient := serve(plain)
	blocked := call(plainSrv, plainClient(owner), "OpenPTY", ownerToken, map[string]any{"computer_id": "anything"})
	if blocked["__status"] != http.StatusServiceUnavailable {
		t.Fatalf("an unmounted session facade answered %v: %v", blocked["__status"], blocked)
	}
	if code := str(blocked, "code"); code != contracts.ErrUnavailable {
		t.Fatalf("the refusal is not an UNAVAILABLE: %v", blocked)
	}
	if raw := str(blocked, "__raw"); !strings.Contains(raw, "session facade") || !strings.Contains(raw, "Nothing is simulated silently") {
		t.Fatalf("the refusal does not say what is missing: %s", raw)
	}
	if _, bad := blocked["id"]; bad {
		t.Fatalf("an unmounted facade handed out a session anyway: %v", blocked)
	}

	// --- the mounted mock -------------------------------------------------------------
	mounted, err := computeapi.NewWithOptions(store, verifier, ids.Authorize, runner,
		computeapi.Options{Sessions: compute.NewMockSessionService(store)})
	if err != nil {
		t.Fatal(err)
	}
	srv, clientFor := serve(mounted)
	ownerClient, otherClient := clientFor(owner), clientFor(other)

	created := call(srv, ownerClient, "CreateComputer", ownerToken,
		map[string]any{"class": "X", "profile": "ephemeral", "key": "h22-sess-" + randHexTest()})
	if created["__status"] != http.StatusOK {
		t.Fatalf("CreateComputer: %v", created)
	}
	computerID := str(created, "id")

	opened := call(srv, ownerClient, "OpenPTY", ownerToken, map[string]any{"computer_id": computerID})
	if opened["__status"] != http.StatusOK {
		t.Fatalf("OpenPTY: %v", opened)
	}
	if str(opened, "mock_marker") != compute.MockMarker {
		t.Fatalf("a session body did not disclose: %v", opened)
	}
	if v, ok := opened["executed_on_host"].(bool); !ok || v {
		t.Fatalf("the session body claims host execution: %v", opened["executed_on_host"])
	}
	sessionID := str(opened, "id")

	// The scope comes from the certificate and there is no way to say otherwise: a body
	// that carries someone else's tenant/subject is not honoured and not silently
	// ignored, it is refused as a malformed request.
	if got := call(srv, ownerClient, "OpenPTY", ownerToken,
		map[string]any{"computer_id": computerID, "tenant": other.Tenant, "subject": other.Subject}); got["__status"] != http.StatusBadRequest {
		t.Fatalf("a body naming another identity was not refused: %v", got)
	}

	// A foreign identity does not get a session on someone else's Computer, and gets the
	// same answer it would for an id that never existed.
	if got := call(srv, otherClient, "OpenPTY", otherToken, map[string]any{"computer_id": computerID}); got["__status"] != http.StatusNotFound {
		t.Fatalf("a foreign caller opened a session: %v", got)
	}
	if got := call(srv, otherClient, "PTYOutput", otherToken,
		map[string]any{"computer_id": computerID, "session_id": sessionID}); got["__status"] != http.StatusNotFound {
		t.Fatalf("a foreign caller read someone else's transcript: %v", got)
	}
	// A token that is not a grant is refused by the transport before any of this matters -
	// and it is refused as FORBIDDEN, which is what the plane does with a credential it
	// cannot verify, not with a missing one.
	if got := call(srv, ownerClient, "OpenPTY", "not-a-token", map[string]any{"computer_id": computerID}); got["__status"] != http.StatusForbidden {
		t.Fatalf("a bearer token that is not a grant: %v", got)
	}

	// Input that a real shell would run, answered by a line that says it ran nowhere.
	fed := call(srv, ownerClient, "PTYInput", ownerToken,
		map[string]any{"computer_id": computerID, "session_id": sessionID, "input": "id\n"})
	if fed["__status"] != http.StatusOK {
		t.Fatalf("PTYInput: %v", fed)
	}
	seen := call(srv, ownerClient, "PTYOutput", ownerToken,
		map[string]any{"computer_id": computerID, "session_id": sessionID, "after_seq": 0})
	if seen["__status"] != http.StatusOK || str(seen, "mock_marker") != compute.MockMarker {
		t.Fatalf("PTYOutput: %v", seen)
	}
	raw := str(seen, "__raw")
	if !strings.Contains(raw, "[mock]") || !strings.Contains(raw, "nothing executed") {
		t.Fatalf("the transcript does not disclose itself line by line: %s", raw)
	}

	// The browser worker is the same shape. Note what is NOT asserted here: that a live
	// session stops Sleep. The sleep gate counts the Redis lease service, and a session -
	// like a running command - holds the durable activity lease, which pins the export
	// guard. That asymmetry is DEF-COMPUTE-7, reported in the acceptance report; asserting
	// a guarantee the plane does not give would be exactly the kind of overclaim this file
	// exists to catch in someone else.
	browser := call(srv, ownerClient, "OpenBrowser", ownerToken,
		map[string]any{"computer_id": computerID, "url": "https://example.invalid/"})
	if browser["__status"] != http.StatusOK {
		t.Fatalf("OpenBrowser: %v", browser)
	}
	browserID := str(browser, "id")
	if got := call(srv, ownerClient, "BrowserAction", ownerToken, map[string]any{
		"computer_id": computerID, "session_id": browserID, "action": "navigate", "target": "https://example.invalid/"}); got["__status"] != http.StatusOK {
		t.Fatalf("BrowserAction: %v", got)
	}
	// A session id this plane never issued is unknown, and saying so is a 404 rather than
	// a 200 about nothing.
	if got := call(srv, ownerClient, "ClosePTY", ownerToken,
		map[string]any{"computer_id": computerID, "session_id": "no-such-session"}); got["__status"] != http.StatusNotFound {
		t.Fatalf("closing an unknown session: %v", got)
	}
	if got := call(srv, ownerClient, "ClosePTY", ownerToken,
		map[string]any{"computer_id": computerID, "session_id": sessionID}); got["__status"] != http.StatusOK ||
		str(got, "status") != string(compute.SessionClosed) {
		t.Fatalf("ClosePTY: %v", got)
	}
	if got := call(srv, ownerClient, "CloseBrowser", ownerToken,
		map[string]any{"computer_id": computerID, "session_id": browserID}); got["__status"] != http.StatusOK {
		t.Fatalf("CloseBrowser: %v", got)
	}
	// A second close is the same fact and must stay a 200, not a new effect or an error.
	if got := call(srv, ownerClient, "ClosePTY", ownerToken,
		map[string]any{"computer_id": computerID, "session_id": sessionID}); got["__status"] != http.StatusOK {
		t.Fatalf("closing twice was not idempotent: %v", got)
	}
	// Input to a closed session is refused out loud.
	if got := call(srv, ownerClient, "PTYInput", ownerToken,
		map[string]any{"computer_id": computerID, "session_id": sessionID, "input": "ls\n"}); got["__status"] != http.StatusConflict {
		t.Fatalf("input to a closed session: %v", got)
	}
}
