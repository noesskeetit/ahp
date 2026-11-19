//go:build ahp_exec

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

// The mock compute backend exists because a real microVM cannot be booted where
// this product is developed (measured on the Linux polygon: /dev/kvm open -> EACCES
// for uid 1000, cgroup root not writable). The customer accepted MOCK for the
// prototype, and the acceptance bar that came with the acceptance is DISCLOSURE:
// every answer of the plane, every audit line, and the UI must say mock, and the
// mock must never claim isolation, escape resistance, latency or density.
//
// Everything measured here is therefore about that bar plus the one routing
// property that makes the mock safe to demo at all: a Computer whose class resolves
// to the mock must stay on the mock for its whole lifecycle, including after Sleep,
// and must never acquire an execution door on the host.
//
// What this file deliberately does NOT claim: it is not evidence about a hypervisor.
// The simulated guest is an in-process state machine that executes nothing, and the
// plane's Sleep/Wake is durable bookkeeping over it - see the boundary assertions at
// the end, which pin that the mock is never handed an execution path.
func TestComputeMockLifecycleDisclosesOnEveryAnswer(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Fatal("isolated real PostgreSQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
	scope := contracts.Scope{Tenant: "h22-mock-life-" + randHexTest(), Deployment: "agent", Subject: "alice"}

	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, err := auth.NewSigner("h22-mock-life", "key", priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("h22-mock-life", map[string]ed25519.PublicKey{"key": pub})
	mint := func(task string, sc contracts.Scope) string {
		t.Helper()
		if _, err := ids.CreateRoot(ctx, sc, identity.RootRequest{
			Task: task, Context: sc.Tenant + ":" + sc.Subject + ":" + sc.Deployment + ":" + task,
			Deployment: sc.Deployment, Capabilities: []string{"model", "tools", "compute", "execute"}, TTL: time.Hour,
		}); err != nil {
			t.Fatal(err)
		}
		token, err := ids.MintTask(ctx, sc, task, signer, "ahp", 10*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	token := mint("root", scope)
	foreignScope := contracts.Scope{Tenant: "h22-tenant-" + randHexTest(), Deployment: "agent", Subject: "mallory"}
	foreignToken := mint("root", foreignScope)

	store := compute.New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	jail := t.TempDir()
	redis := compute.NewRedis(os.Getenv("AHP_REDIS_ADDR"))
	if err := redis.Ping(ctx); err != nil {
		t.Fatal("real Redis required for the lease layer (Sleep/Wake refuse without it by design): " + err.Error())
	}
	leases, err := compute.NewLeaseService(store, redis)
	if err != nil {
		t.Fatal(err)
	}
	// Production wiring of AHP_COMPUTE_MOCK_BACKEND=1: the host backend stays mounted
	// for the classes it serves, and ONLY class X is bound to the mock. A host that
	// cannot boot a guest must never be allowed to answer for class X instead.
	registry := compute.NewRegistry(compute.NewHostBackend(jail), compute.NewMockBackend())
	registry.SetClassBackend(compute.ClassX, compute.MockBackendName)
	runner := compute.NewRunner(store, registry, jail, "h22-mock-node", nil).WithLeaseService(leases)

	plane, err := computeapi.New(store, verifier, ids.Authorize, runner)
	if err != nil {
		t.Fatal(err)
	}
	backendName, marker := registry.Disclosure()
	if backendName != compute.MockBackendName || marker != compute.MockMarker {
		t.Fatalf("the disclosing registry answered %q/%q", backendName, marker)
	}
	mux := http.NewServeMux()
	mountComputePlane(mux, disclosingPlane{backend: backendName, marker: marker, next: plane})
	// The plane answers a verified WORKLOAD only: a bearer token without a client
	// certificate is refused, so this harness is mTLS like the stand.
	ca := newTestCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	defer srv.Close()
	clientFor := func(sc contracts.Scope) *http.Client {
		hc := *srv.Client()
		tr := srv.Client().Transport.(*http.Transport).Clone()
		tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(sc))}
		hc.Transport = tr
		return &hc
	}
	ownerClient := clientFor(scope)
	foreignClient := clientFor(foreignScope)

	// call is the ONLY way this test talks to the plane, and it fails the test on a
	// response that does not disclose who answered. Checking inside the helper is the
	// point: "every answer carries the marker" cannot be satisfied by one sampled
	// endpoint, and a refusal (401/403/503) is exactly where a caller could otherwise
	// read silence as success.
	callWith := func(hc *http.Client, op, tok string, body any) map[string]any {
		t.Helper()
		raw, _ := json.Marshal(body)
		req, err := http.NewRequest(http.MethodPost, srv.URL+computeMountPrefix+op, bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
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
		return out
	}
	call := func(op, tok string, body any) map[string]any {
		t.Helper()
		return callWith(ownerClient, op, tok, body)
	}
	field := func(body map[string]any, key string) string {
		if v, ok := body[key].(string); ok {
			return v
		}
		return ""
	}

	// An unauthenticated request still discloses. This is the answer a browser sees
	// when a grant lapses mid-lifecycle, and it must not be the one blank answer.
	if got := call("GetComputer", token, map[string]any{"computer_id": "does-not-exist"}); got["__status"] != http.StatusNotFound {
		t.Fatalf("unknown computer: %v", got)
	}

	created := call("CreateComputer", token, map[string]any{"class": "X", "profile": "ephemeral", "key": "h22-mock-life"})
	if created["__status"] != http.StatusOK {
		t.Fatalf("CreateComputer: %v", created)
	}
	computerID := field(created, "id")
	if computerID == "" {
		t.Fatalf("CreateComputer returned no id: %v", created)
	}

	awake := call("Wake", token, map[string]any{"computer_id": computerID})
	if awake["__status"] != http.StatusOK {
		t.Fatalf("Wake: %v", awake)
	}
	if got := field(awake, "backend"); got != compute.MockBackendName {
		t.Fatalf("Wake put class X on backend %q, want %q: the mock must never be substituted by the host", got, compute.MockBackendName)
	}
	firstIncarnation := field(awake, "id")

	// The execution door, measured at the two layers that exist.
	//
	// Exec is a LEDGER WRITE: it records the command and answers with a dispatch
	// instruction; the dispatch itself happens in Runner.RunCommand, which is what a
	// worker calls. So the honest assertions are (a) nothing executes on this host at
	// either layer, and (b) the component that is asked to dispatch is refused BY NAME,
	// because the mock implements neither CommandBackend nor PTYBackend.
	//
	// may_dispatch is the answer a caller ACTS on, so it has to be true. DEF-COMPUTE-5 was
	// that a row for class X came back `accepted` with may_dispatch=true even though no
	// backend bound to that class can run commands, and the refusal only surfaced later
	// inside RunCommand - so the caller was sent to do something that could only fail,
	// while the ledger looked like somebody was working. The record still stays accepted,
	// because a plane that does have a real substrate may claim it later; what is refused is
	// the promise, and it is refused with a reason naming the backend and the missing door.
	probe := filepath.Join(t.TempDir(), "executed-on-the-host")
	recorded := call("Exec", token, map[string]any{"computer_id": computerID, "key": "h22-mock-exec", "command": "touch " + probe})
	if recorded["__status"] != http.StatusOK {
		t.Fatalf("Exec (a ledger write) was refused: %v", recorded)
	}
	if _, err := os.Stat(probe); err == nil {
		t.Fatal("the mock lifecycle EXECUTED the command on the host at the record layer")
	}
	if command, ok := recorded["command"].(map[string]any); !ok || field(command, "status") != "accepted" {
		t.Fatalf("Exec did not record the command as accepted: %v", recorded)
	}
	if v, ok := recorded["may_dispatch"].(bool); !ok || v {
		t.Fatalf("a plane with no execution door offered a command for dispatch: %v", recorded)
	}
	if why := field(recorded, "dispatch_reason"); !strings.Contains(why, compute.MockBackendName) ||
		!strings.Contains(why, "no execution door") {
		t.Fatalf("the refusal to dispatch does not name the backend and the missing door: %q", why)
	}

	// The differential, without which the assertion above is worthless: may_dispatch=false
	// must mean "this plane cannot", not "this plane never says yes". A class this plane
	// genuinely serves still gets the dispatch instruction and no reason.
	served := call("CreateComputer", token, map[string]any{"class": "S", "profile": "ephemeral", "key": "h22-mock-served"})
	if served["__status"] != http.StatusOK {
		t.Fatalf("CreateComputer (class S): %v", served)
	}
	servedID := field(served, "id")
	mayRun := call("Exec", token, map[string]any{"computer_id": servedID, "key": "h22-mock-served-exec", "command": "true"})
	if mayRun["__status"] != http.StatusOK {
		t.Fatalf("Exec on a served class: %v", mayRun)
	}
	if v, ok := mayRun["may_dispatch"].(bool); !ok || !v {
		t.Fatalf("a plane that CAN run commands refused to say so: %v", mayRun)
	}
	if why := field(mayRun, "dispatch_reason"); why != "" {
		t.Fatalf("a dispatch instruction came with a refusal reason: %q", why)
	}
	if _, err := os.Stat(probe); err == nil {
		t.Fatal("recording a command for a served class executed it on the host")
	}
	refused, err := runner.RunCommand(ctx, scope, computerID, "h22-mock-exec-dispatch", "touch "+probe, "")
	if err == nil {
		t.Fatalf("the mock backend dispatched a command: %+v", refused)
	}
	if !strings.Contains(err.Error(), "cannot run commands") {
		t.Fatalf("dispatch refusal does not name the reason: %v", err)
	}
	if !strings.Contains(err.Error(), compute.MockBackendName) {
		t.Fatalf("dispatch refusal does not name the mock backend: %v", err)
	}
	if _, err := os.Stat(probe); err == nil {
		t.Fatal("the mock backend EXECUTED the command on the host: the refusal is a lie")
	}

	// Sleep then Wake: the class binding must survive the round trip. This is the
	// plane-level form of the substitution DEF-COMPUTE-3 measured (an incarnation
	// written mock, woken host).
	slept := call("Sleep", token, map[string]any{"computer_id": computerID})
	if slept["__status"] != http.StatusOK {
		t.Fatalf("Sleep: %v", slept)
	}
	woke := call("Wake", token, map[string]any{"computer_id": computerID})
	if woke["__status"] != http.StatusOK {
		t.Fatalf("Wake after Sleep: %v", woke)
	}
	if got := field(woke, "backend"); got != compute.MockBackendName {
		t.Fatalf("Wake after Sleep substituted backend %q: sleep/resume on the mock does not hold", got)
	}
	if field(woke, "id") == firstIncarnation {
		t.Fatalf("Wake returned the incarnation the Sleep ended (%q): the report does not describe a new incarnation", firstIncarnation)
	}

	snap := call("Snapshot", token, map[string]any{"computer_id": computerID, "kind": "directory",
		"path": "h22/mock-snapshot", "digest": "sha256:" + strings.Repeat("a", 64), "size_bytes": 1})
	if snap["__status"] != http.StatusOK {
		t.Fatalf("Snapshot: %v", snap)
	}
	if stopped := call("StopIncarnation", token, map[string]any{"computer_id": computerID, "incarnation_id": field(woke, "id")}); stopped["__status"] != http.StatusOK {
		t.Fatalf("StopIncarnation: %v", stopped)
	}
	if destroyed := call("Destroy", token, map[string]any{"computer_id": computerID}); destroyed["__status"] != http.StatusOK {
		t.Fatalf("Destroy: %v", destroyed)
	}

	// Isolation still holds under the mock, and the refusal still discloses it.
	if other := callWith(foreignClient, "GetComputer", foreignToken, map[string]any{"computer_id": computerID}); other["__status"] == http.StatusOK {
		t.Fatalf("another tenant read the mock computer: %v", other)
	}

	// Nothing above put a byte on the host for this computer: no jail directory, no
	// working tree, no probe file. The jail base is where a real backend would build a
	// sandbox, so an empty one is the out-of-band witness that the mock path executed
	// nothing.
	entries, err := os.ReadDir(jail)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the mock lifecycle created host state under the jail base: %v", entries)
	}
}

// TestMockBackendAuditLinesAllCarryTheMarker covers the audit half of the disclosure
// bar. The plane's own lifecycle calls do not drive the simulated guest - the mock
// implements no execution door, so RunCommand/OpenPTY refuse before Acquire - which
// means the audit trail of the simulated guest is written where the guest is actually
// driven: the backend contract itself. Every one of those lines must carry the marker
// as a FIELD, because an audit row that reads like a hypervisor row is how a
// simulation ends up as evidence three months later.
func TestMockBackendAuditLinesAllCarryTheMarker(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	backends := compute.NewRegistry(compute.NewMockBackend())
	backends.SetClassBackend(compute.ClassX, compute.MockBackendName)
	backend, err := backends.Require(context.Background(), compute.ClassX)
	if err != nil {
		t.Fatal(err)
	}
	freezer, ok := backend.(compute.SleepResumer)
	if !ok {
		t.Fatalf("the mock must satisfy the sleep/resume contract, got %T", backend)
	}
	parker, ok := backend.(compute.Snapshotter)
	if !ok {
		t.Fatalf("the mock must satisfy the snapshot/restore contract, got %T", backend)
	}
	guest, err := backend.Acquire(context.Background(), compute.AcquireSpec{ComputerID: "h22-audit", Class: compute.ClassX})
	if err != nil {
		t.Fatal(err)
	}
	if err := freezer.Pause(context.Background(), guest); err != nil {
		t.Fatal(err)
	}
	if err := freezer.Resume(context.Background(), guest); err != nil {
		t.Fatal(err)
	}
	ref, err := parker.Snapshot(context.Background(), guest)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := parker.RestoreSnapshot(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if restored.ID() != guest.ID() {
		t.Fatalf("restore produced %q, not the parked incarnation %q", restored.ID(), guest.ID())
	}
	if err := backend.Release(context.Background(), guest.ID()); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	seen := map[string]bool{}
	for _, line := range lines {
		if line == "" {
			continue
		}
		// slog quotes a value containing spaces, so the marker is checked as a field
		// name plus its text rather than as one literal.
		if !strings.Contains(line, "mock_marker=") || !strings.Contains(line, compute.MockMarker) {
			t.Fatalf("an audit line about the simulated guest carries no marker: %s", line)
		}
		for _, op := range []string{"acquire", "pause", "resume", "snapshot", "restore", "release"} {
			if strings.Contains(line, "op="+op) {
				seen[op] = true
			}
		}
	}
	for _, op := range []string{"acquire", "pause", "resume", "snapshot", "restore", "release"} {
		if !seen[op] {
			t.Fatalf("the lifecycle produced no audit line for %q; lines: %s", op, logs.String())
		}
	}
	// And the disclosure never drifts into a claim: the marker text itself is the
	// contract, so pinning it here fails the moment somebody edits it into marketing.
	if !strings.Contains(compute.MockMarker, "no isolation") || !strings.Contains(compute.MockMarker, "Not evidence about isolation, escape, latency or density") {
		t.Fatalf("the mock marker no longer states its own limits: %s", compute.MockMarker)
	}
}
