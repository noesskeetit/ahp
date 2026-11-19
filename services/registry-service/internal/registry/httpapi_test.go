package registry

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// These tests exercise the item-6 HTTP face (PDF rev 3.2, c. 42) added in
// httpapi.go: RegisterAgent/PutVersion/Install/Promote/Rollback/ListDeployments
// are reached over the SAME handlers the composed front serves, with the caller
// identity injected the way transport.Task would inject it -- from a VERIFIED
// principal (mTLS client certificate), never from a header or the body. Before
// this wiring the six routes were absent from HTTPHandler.routes and every call
// was a 404 unknown route; httpCall fails the test in that case, so these tests
// are the red->green marker for the wiring.
//
// They run against the real PostgreSQL instance (AHP_TEST_DATABASE_URL), exactly
// as the accepted registry/store tests do; the publication path is gated on the
// real internal/evals plane, so the eval precondition ("оценки перед
// публикацией") is proven on the HTTP layer, not stubbed.

func httpSuffix(t *testing.T) string {
	t.Helper()
	id := contracts.NewID()
	if len(id) > 20 {
		id = id[len(id)-20:]
	}
	return strings.ToLower(strings.ReplaceAll(id, "_", ""))
}

// newHTTPFixture builds a real, eval-gated registry store plus the HTTP handler
// under test, mirroring the product mount (New + WithEvalGate + Migrate).
func newHTTPFixture(t *testing.T) (*HTTPHandler, *evals.Store) {
	t.Helper()
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AHP_TEST_DATABASE_URL is not set: registry HTTP tests run against the real PostgreSQL instance")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("build pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping real PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)

	evalStore := evals.NewStore(pool)
	if err := evalStore.Migrate(ctx); err != nil {
		t.Fatalf("evals Migrate: %v", err)
	}
	store := New(pool, WithEvalGate(evalStore))
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("registry Migrate: %v", err)
	}

	// The verifier/authorize are never consulted: httpCall invokes the route
	// handler directly and installs the principal transport.Task would install.
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{})
	allow := func(context.Context, auth.Claims, string) error { return nil }
	h, err := NewHTTPHandler(store, verifier, allow)
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	return h.(*HTTPHandler), evalStore
}

// The evals policy the HTTP-face fixtures publish under: same id and content
// as the registry_test harness declares, so one stored policy serves both test
// packages of the same database.
const (
	httpHarnessPolicyID  = "h26-harness-policy"
	httpHarnessTaskSetID = "reference-set-h26"
)

func ensureHTTPHarnessPolicy(t *testing.T, s *evals.Store) {
	t.Helper()
	if _, err := s.SetPolicy(context.Background(), evals.Policy{
		ID: httpHarnessPolicyID, TaskSetID: httpHarnessTaskSetID,
		RequiredTaskIDs: []string{"reference-task"}, MaxAgeSeconds: 3600,
	}); err != nil {
		t.Fatalf("declare harness eval policy: %v", err)
	}
}

// httpPassVerdict records a genuine passing evaluation verdict for exactly this
// digest through the real evals plane -- the only way to satisfy the publication
// precondition. Content is fixture data; the check itself is the product's.
func httpPassVerdict(t *testing.T, evalStore *evals.Store, digest string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ensureHTTPHarnessPolicy(t, evalStore)
	if _, err := evalStore.Record(ctx, evals.RecordInput{
		AgentDigest: digest,
		TaskSetID:   httpHarnessTaskSetID,
		Setup:       map[string]any{"harness": "registry-http"},
		Outcome:     evals.OutcomePass,
		Tasks:       []evals.TaskResult{{TaskID: "reference-task", Passed: true, TerminalState: evals.TerminalCompleted, Cost: 0}},
		PolicyID:    httpHarnessPolicyID, TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("record passing verdict for %s: %v", digest, err)
	}
}

// httpCall invokes one route handler with a verified principal in context and
// returns the HTTP status and the decoded JSON body. A route that is not wired
// fails the test -- that is the red marker for a missing HTTP adapter.
func httpCall(t *testing.T, hh *HTTPHandler, route string, body any, scope contracts.Scope) (int, map[string]any) {
	t.Helper()
	fn, ok := hh.routes[route]
	if !ok {
		t.Fatalf("registry route %q is NOT wired over HTTP (would be 404 unknown route)", route)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/registry/"+route, bytes.NewReader(raw))
	req = req.WithContext(transport.WithPrincipal(req.Context(), transport.Principal{Scope: scope}))
	w := httptest.NewRecorder()
	fn(w, req)
	out := map[string]any{}
	if b := w.Body.Bytes(); len(b) > 0 {
		_ = json.Unmarshal(b, &out)
	}
	return w.Code, out
}

func nestedID(t *testing.T, out map[string]any, key string) string {
	t.Helper()
	obj, ok := out[key].(map[string]any)
	if !ok {
		t.Fatalf("response has no object %q: %v", key, out)
	}
	id, _ := obj["id"].(string)
	if id == "" {
		t.Fatalf("response %q has no id: %v", key, out)
	}
	return id
}

// TestHTTPRegistryFullPath drives the whole item-6 user path over HTTP against
// the real store: register -> eval-gated publish -> install -> promote (update)
// -> rollback -> list deployments, asserting the eval gate blocks publication
// without a passing verdict and that every identity comes from the certificate.
func TestHTTPRegistryFullPath(t *testing.T) {
	hh, evalStore := newHTTPFixture(t)
	suf := httpSuffix(t)
	publisher := "pub-h26-" + suf
	tenant := "t-h26-" + suf
	agent := "agent-h26-" + suf
	scope := contracts.Scope{Tenant: tenant, Deployment: "d-h26-" + suf, Subject: publisher}

	// 1) Register the agent; publisher must be the cert subject, not a body field.
	code, out := httpCall(t, hh, "RegisterAgent", map[string]any{"agent_id": agent}, scope)
	if code != http.StatusOK {
		t.Fatalf("RegisterAgent: got %d body=%v", code, out)
	}
	if a, _ := out["agent"].(map[string]any); a["publisher_id"] != publisher {
		t.Fatalf("RegisterAgent publisher must come from the certificate: got %v", out["agent"])
	}

	// 2) PutVersion WITHOUT a passing eval verdict is refused on the HTTP layer.
	// The eval verdicts live in a persistent, shared ahp_evals schema, so the
	// manifest must carry a run-unique field: a constant digest would inherit a
	// verdict recorded by an earlier run and the "no verdict yet" assertion
	// would be meaningless. Compat is validated and free-form enough to carry it.
	m1 := validManifest()
	m1.State.Compat = "c" + suf
	digest1, err := contracts.Digest(m1)
	if err != nil {
		t.Fatalf("digest v1: %v", err)
	}
	code, out = httpCall(t, hh, "PutVersion",
		map[string]any{"agent_id": agent, "digest": digest1, "manifest": m1}, scope)
	if code != http.StatusConflict {
		t.Fatalf("PutVersion without eval verdict must be refused (409), got %d body=%v", code, out)
	}

	// 3) After a real passing verdict for this exact digest, publication succeeds.
	httpPassVerdict(t, evalStore, digest1)
	code, out = httpCall(t, hh, "PutVersion",
		map[string]any{"agent_id": agent, "digest": digest1, "manifest": m1}, scope)
	if code != http.StatusOK {
		t.Fatalf("PutVersion with passing verdict: got %d body=%v", code, out)
	}
	version1 := nestedID(t, out, "version")

	// 4) Install the version in the tenant's dev environment.
	code, out = httpCall(t, hh, "Install", map[string]any{
		"agent_id": agent, "version_id": version1, "environment": "dev",
		"key": "install-" + suf, "granted": m1.Capabilities,
	}, scope)
	if code != http.StatusOK {
		t.Fatalf("Install: got %d body=%v", code, out)
	}
	installation := nestedID(t, out, "installation")

	// 5) Update (promote) to a second version -- itself eval-gated.
	m2 := validManifest()
	m2.Limits.ActiveTasks = m2.Limits.ActiveTasks + 1 // a distinct, valid manifest -> new digest
	digest2, err := contracts.Digest(m2)
	if err != nil {
		t.Fatalf("digest v2: %v", err)
	}
	if digest2 == digest1 {
		t.Fatalf("v2 digest must differ from v1")
	}
	httpPassVerdict(t, evalStore, digest2)
	code, out = httpCall(t, hh, "PutVersion",
		map[string]any{"agent_id": agent, "digest": digest2, "manifest": m2}, scope)
	if code != http.StatusOK {
		t.Fatalf("PutVersion v2 with verdict: got %d body=%v", code, out)
	}
	version2 := nestedID(t, out, "version")

	code, out = httpCall(t, hh, "Promote", map[string]any{
		"installation_id": installation, "version_id": version2,
	}, scope)
	if code != http.StatusOK {
		t.Fatalf("Promote: got %d body=%v", code, out)
	}
	if got, _ := out["installation"].(map[string]any); got["version_id"] != version2 {
		t.Fatalf("Promote must move the installation to v2: got %v", out["installation"])
	}

	// 6) Rollback returns the installation to the previous version it ran.
	code, out = httpCall(t, hh, "Rollback", map[string]any{
		"installation_id": installation,
	}, scope)
	if code != http.StatusOK {
		t.Fatalf("Rollback: got %d body=%v", code, out)
	}
	if got, _ := out["installation"].(map[string]any); got["version_id"] != version1 {
		t.Fatalf("Rollback must move back to v1: got %v", out["installation"])
	}

	// 7) Deployment history is observable, newest first.
	code, out = httpCall(t, hh, "ListDeployments", map[string]any{
		"installation_id": installation, "limit": 50,
	}, scope)
	if code != http.StatusOK {
		t.Fatalf("ListDeployments: got %d body=%v", code, out)
	}
	rows, _ := out["deployments"].([]any)
	if len(rows) < 3 {
		t.Fatalf("expected install+promote+rollback in the history, got %d rows: %v", len(rows), out)
	}
	if first, _ := rows[0].(map[string]any); first["action"] != "rollback" {
		t.Fatalf("newest deployment must be the rollback, got %v", rows[0])
	}
}

// TestHTTPRegistryForeignIsolation proves the tenant/publisher boundary: a
// foreign publisher cannot publish versions of another publisher's agent, and a
// foreign tenant cannot roll back or read another tenant's installation.
func TestHTTPRegistryForeignIsolation(t *testing.T) {
	hh, evalStore := newHTTPFixture(t)
	suf := httpSuffix(t)
	publisher := "pub-h26-" + suf
	tenant := "t-h26-" + suf
	foreignTenant := "x-h26-" + suf
	foreignPub := "xpub-h26-" + suf
	agent := "agent-h26-" + suf
	owner := contracts.Scope{Tenant: tenant, Deployment: "d-h26-" + suf, Subject: publisher}
	fTenant := contracts.Scope{Tenant: foreignTenant, Deployment: "d-h26-" + suf, Subject: publisher}
	fPub := contracts.Scope{Tenant: tenant, Deployment: "d-h26-" + suf, Subject: foreignPub}

	if code, out := httpCall(t, hh, "RegisterAgent", map[string]any{"agent_id": agent}, owner); code != http.StatusOK {
		t.Fatalf("RegisterAgent: got %d body=%v", code, out)
	}

	// A foreign publisher is refused BEFORE the eval gate runs (403, not 409).
	m := validManifest()
	digest, _ := contracts.Digest(m)
	if code, out := httpCall(t, hh, "PutVersion",
		map[string]any{"agent_id": agent, "digest": digest, "manifest": m}, fPub); code != http.StatusForbidden {
		t.Fatalf("foreign publisher must be refused (403), got %d body=%v", code, out)
	}

	// Owner installs a version, then a foreign tenant cannot roll it back (404).
	httpPassVerdict(t, evalStore, digest)
	code, out := httpCall(t, hh, "PutVersion",
		map[string]any{"agent_id": agent, "digest": digest, "manifest": m}, owner)
	if code != http.StatusOK {
		t.Fatalf("owner PutVersion: got %d body=%v", code, out)
	}
	version := nestedID(t, out, "version")
	code, out = httpCall(t, hh, "Install", map[string]any{
		"agent_id": agent, "version_id": version, "environment": "dev",
		"key": "install-" + suf, "granted": m.Capabilities,
	}, owner)
	if code != http.StatusOK {
		t.Fatalf("owner Install: got %d body=%v", code, out)
	}
	installation := nestedID(t, out, "installation")

	if code, out := httpCall(t, hh, "Rollback", map[string]any{"installation_id": installation}, fTenant); code != http.StatusNotFound {
		t.Fatalf("foreign tenant rollback must be scoped out (404), got %d body=%v", code, out)
	}

	// A foreign tenant sees an EMPTY history for the owner's installation.
	code, out = httpCall(t, hh, "ListDeployments", map[string]any{"installation_id": installation, "limit": 50}, fTenant)
	if code != http.StatusOK {
		t.Fatalf("foreign ListDeployments: got %d body=%v", code, out)
	}
	if rows, _ := out["deployments"].([]any); len(rows) != 0 {
		t.Fatalf("foreign tenant must see no deployments of the owner installation, got %v", out)
	}
}

// TestHTTPRegistryBodyIdentityRejected proves identity is not injectable: the
// request bodies for these routes have no tenant/publisher field, and an attempt
// to smuggle one is rejected by the strict JSON reader (400), not obeyed.
func TestHTTPRegistryBodyIdentityRejected(t *testing.T) {
	hh, _ := newHTTPFixture(t)
	suf := httpSuffix(t)
	scope := contracts.Scope{Tenant: "t-h26-" + suf, Deployment: "d-h26-" + suf, Subject: "pub-h26-" + suf}

	if code, out := httpCall(t, hh, "RegisterAgent",
		map[string]any{"agent_id": "agent-h26-" + suf, "publisher_id": "attacker"}, scope); code != http.StatusBadRequest {
		t.Fatalf("body-injected publisher_id must be rejected (400), got %d body=%v", code, out)
	}
	if code, out := httpCall(t, hh, "ListDeployments",
		map[string]any{"installation_id": "inst-x", "tenant": "victim"}, scope); code != http.StatusBadRequest {
		t.Fatalf("body-injected tenant must be rejected (400), got %d body=%v", code, out)
	}
}

// TestHTTPRegistryUnknownRoute404 is the regression guard: the dispatcher still
// answers 404 for a route that does not exist, and 404 for the six routes only
// when they are genuinely unwired (they are not, once the adapter is present).
func TestHTTPRegistryUnknownRoute404(t *testing.T) {
	hh, _ := newHTTPFixture(t)
	for _, route := range []string{"RegisterAgent", "PutVersion", "Install", "Promote", "Rollback", "ListDeployments"} {
		if _, ok := hh.routes[route]; !ok {
			t.Fatalf("route %q must be wired", route)
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/registry/BogusRoute", bytes.NewReader([]byte(`{}`)))
	w := httptest.NewRecorder()
	hh.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown registry route must be 404, got %d", w.Code)
	}
}

func TestHTTPConfigCASAndTenantIsolation(t *testing.T) {
	h, _ := newHTTPFixture(t)
	scope := contracts.Scope{Tenant: "cfg-" + httpSuffix(t), Deployment: "registry", Subject: "owner"}
	agent := "agent-" + httpSuffix(t)
	if status, _ := httpCall(t, h, "PutConfig", map[string]any{"agent_id": agent, "expected_revision": 0, "data": map[string]string{"prompt": "first"}}, scope); status != 200 {
		t.Fatalf("create=%d", status)
	}
	if status, _ := httpCall(t, h, "PutConfig", map[string]any{"agent_id": agent, "expected_revision": 0, "data": map[string]string{"prompt": "stale"}}, scope); status != 409 {
		t.Fatalf("CAS stale=%d", status)
	}
	status, out := httpCall(t, h, "GetConfig", map[string]string{"agent_id": agent}, scope)
	if status != 200 || out["config"].(map[string]any)["revision"] != float64(1) {
		t.Fatalf("read=%d %v", status, out)
	}
	scope.Tenant += "-foreign"
	if status, _ := httpCall(t, h, "GetConfig", map[string]string{"agent_id": agent}, scope); status != 404 {
		t.Fatalf("foreign=%d", status)
	}
}
