// Package identity_test is the real end-to-end acceptance suite for the
// Identity & Policy block. It exercises the live HTTP surface (real TLS/mTLS
// + real Ed25519 JWTs) backed by a real PostgreSQL store, and asserts the
// behaviour required by the acceptance contract. It deliberately does not
// trust the green unit tests: every assertion is behavioural.
package identity_test

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
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/identityapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

var fullCaps = []string{
	"identity:read", "identity:delegate", "identity:mint",
	"identity:evaluate", "identity:labels", "identity:cancel",
}

// controlPlaneDeployment is the one deployment the test server trusts as an
// operator control plane (control/cancel-subtree and control/revoke-subject).
// Ordinary task/agent scopes use random deployments and are therefore refused by
// those admin routes, exactly as in production where Space is not an admin.
const controlPlaneDeployment = "cp-harness-test"

const testAudience = "ahp-identity.test"

// ---------------------------------------------------------------------------
// CA / TLS test fixtures.
// ---------------------------------------------------------------------------

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ahp-acceptance-ca"},
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
	return &testCA{cert: cert, key: key}
}

func (ca *testCA) leaf(t *testing.T, serial int64, uris []string, ips []net.IP, dns []string, server bool) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	parsed := make([]*url.URL, 0, len(uris))
	for _, u := range uris {
		p, err := url.Parse(u)
		if err != nil {
			t.Fatalf("bad test uri %q: %v", u, err)
		}
		parsed = append(parsed, p)
	}
	usage := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	if server {
		usage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "ahp-acceptance-leaf"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usage,
		URIs:         parsed,
		IPAddresses:  ips,
		DNSNames:     dns,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	parsedLeaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: parsedLeaf}
}

func (ca *testCA) clientCert(t *testing.T, uris ...string) tls.Certificate {
	t.Helper()
	return ca.leaf(t, 2, uris, nil, nil, false)
}

func (ca *testCA) serverCert(t *testing.T) tls.Certificate {
	t.Helper()
	return ca.leaf(t, 3, nil, []net.IP{net.IPv4(127, 0, 0, 1)}, []string{"localhost"}, true)
}

func newSignerVerifier(t *testing.T) (*auth.Signer, *auth.Verifier) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("https://identity.test", "k1", priv)
	if err != nil {
		t.Fatal(err)
	}
	return signer, auth.NewVerifier("https://identity.test",
		map[string]ed25519.PublicKey{"k1": pub})
}

// ---------------------------------------------------------------------------
// Database harness.
// ---------------------------------------------------------------------------

func newStore(t *testing.T) *identity.Store {
	t.Helper()
	pool, isolateNote := isolatedPool(t)
	t.Logf("identity test db isolation: %s", isolateNote)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := identity.New(pool)
	// The PostgreSQL-backed tests are opt-in by reachability, not just by the
	// presence of a DSN string: a DSN that points at a host that is not serving
	// PostgreSQL (for example a developer laptop) skips them, while a reachable
	// database that fails to migrate is a hard failure. Integration validation
	// runs these against the isolated AHP test PostgreSQL.
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("identity acceptance: PostgreSQL not reachable (%v)", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func sanitizeID(s string) string {
	var b []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			b = append(b, c)
		}
	}
	return string(b)
}

func uniqueScope(t *testing.T) contracts.Scope {
	s := sanitizeID(contracts.NewID())
	return contracts.Scope{Tenant: "t-" + s, Deployment: "dep-" + s, Subject: "subj-" + s}
}

// ctxID builds a globally unique context journal id for scope. The contexts
// table has a global primary key on id, and a context owned by another scope
// is invisible (NOT_FOUND) rather than shared, so every scope must allocate
// its own context ids; reusing the same name inside one scope is the intended
// journal accumulation path and stays correct.
func ctxID(scope contracts.Scope, name string) string {
	return sanitizeID(scope.Tenant) + "-c-" + sanitizeID(name)
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// ---------------------------------------------------------------------------
// HTTP harness.
// ---------------------------------------------------------------------------

type harness struct {
	t        *testing.T
	store    *identity.Store
	signer   *auth.Signer
	verifier *auth.Verifier
	audience string
	ca       *testCA
	baseURL  string
	// human never carries a client certificate.
	human *http.Client
}

func newHarness(t *testing.T) *harness { return newHarnessAudience(t, testAudience) }

func newHarnessAudience(t *testing.T, audience string) *harness {
	return newHarnessConfig(t, audience, nil)
}

func newHarnessConfig(t *testing.T, audience string, configure func(*identityapi.Config)) *harness {
	t.Helper()
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	store := newStore(t)

	caPool := x509.NewCertPool()
	caPool.AddCert(ca.cert)

	cfg := identityapi.Config{
		Store: store, Verifier: verifier, Signer: signer, Audience: audience,
		// Only this deployment is a control plane; every uniqueScope deployment
		// is an ordinary workload, so the admin routes must refuse them.
		ControlPlaneDeployments: []string{controlPlaneDeployment},
	}
	if configure != nil {
		configure(&cfg)
	}
	srv := httptest.NewUnstartedServer(identityapi.Handler(cfg))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{ca.serverCert(t)},
		// Verify any client certificate that is offered (populating
		// VerifiedChains for the workload guard) while still admitting human
		// callers that present no certificate at all.
		ClientAuth: tls.VerifyClientCertIfGiven,
		ClientCAs:  caPool,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	human := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: caPool},
	}}
	t.Cleanup(func() { human.CloseIdleConnections() })

	return &harness{t: t, store: store, signer: signer, verifier: verifier,
		audience: audience, ca: ca, baseURL: srv.URL, human: human}
}

// taskClient returns an HTTP client presenting an mTLS workload certificate
// for scope; the same client presents task tokens on the Authorization header.
func (h *harness) taskClient(t *testing.T, scope contracts.Scope) *http.Client {
	t.Helper()
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs:      h.caPool(),
		Certificates: []tls.Certificate{h.ca.clientCert(t, auth.WorkloadID(scope))},
	}}}
	t.Cleanup(func() { c.CloseIdleConnections() })
	return c
}

// controlClient presents an operator control-plane workload certificate for a
// tenant. Its deployment is the one on the ControlPlaneDeployments allowlist, so
// it may reach control/cancel-subtree and control/revoke-subject; an ordinary
// taskClient (random deployment) may not.
func (h *harness) controlClient(t *testing.T, tenant string) *http.Client {
	t.Helper()
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs:      h.caPool(),
		Certificates: []tls.Certificate{h.ca.clientCert(t, auth.WorkloadID(contracts.Scope{Tenant: tenant, Deployment: controlPlaneDeployment}))},
	}}}
	t.Cleanup(func() { c.CloseIdleConnections() })
	return c
}

func (h *harness) caPool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(h.ca.cert)
	return p
}

func (h *harness) signHuman(scope contracts.Scope) string {
	tok, err := h.signer.Sign(auth.Claims{
		Audience: h.audience, Kind: auth.KindHuman,
		Tenant: scope.Tenant, Subject: scope.Subject,
	}, time.Hour)
	if err != nil {
		h.t.Fatalf("sign human: %v", err)
	}
	return tok
}

// mint bootstraps a task token directly from the store (the only way past the
// mint-route chicken-and-egg), bounded by the grant's remaining life.
func (h *harness) mint(t *testing.T, scope contracts.Scope, task string) string {
	t.Helper()
	tok, err := h.store.MintTask(testContext(t), scope, task, h.signer, h.audience, 0)
	if err != nil {
		t.Fatalf("mint %s: %v", task, err)
	}
	return tok
}

type response struct {
	status int
	body   []byte
}

func (h *harness) do(t *testing.T, c *http.Client, method, path, bearer string, body any) response {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(testContext(t), method, h.baseURL+path, rdr)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	res, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return response{status: res.StatusCode, body: b}
}

func (h *harness) createRootStore(t *testing.T, scope contracts.Scope, task string, allowSelf bool, req func(*identity.RootRequest)) identity.Grant {
	t.Helper()
	r := identity.RootRequest{
		Task: task, Context: ctxID(scope, task), Deployment: scope.Deployment,
		Capabilities: append([]string{}, fullCaps...), TTL: time.Hour, AllowSelf: allowSelf,
	}
	if req != nil {
		req(&r)
	}
	g, err := h.store.CreateRoot(testContext(t), scope, r)
	if err != nil {
		t.Fatalf("CreateRoot %s: %v", task, err)
	}
	return g
}

func grantFrom(t *testing.T, b []byte) identity.Grant {
	t.Helper()
	var g identity.Grant
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatalf("decode grant: %v (body=%s)", err, b)
	}
	return g
}

func contains(hay []string, needle string) bool {
	for _, s := range hay {
		if s == needle {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Criterion 1 — real JWT/mTLS lifecycle, subject invariant, actor chain,
// rights intersection, current-IAM live authorize.
// ---------------------------------------------------------------------------

func TestHTTPLifecycleAndLiveAuthorize(t *testing.T) {
	h := newHarness(t)
	scope := uniqueScope(t)

	// Human opens a root over HTTP. The body attempts to inflate ceilings and
	// enable self-delegation; the handler must ignore them.
	body := map[string]any{
		"task": "root-a", "context": ctxID(scope, "a"), "capabilities": fullCaps,
		"ttl": "1h", "deployment": scope.Deployment,
		"max_depth": 16, "max_fanout": 1024, "max_active": 100000, "allow_self": true,
	}
	res := h.do(t, h.human, http.MethodPost, "/v1/identity/roots", h.signHuman(scope), body)
	if res.status != http.StatusCreated {
		t.Fatalf("createRoot status=%d body=%s", res.status, res.body)
	}
	created := grantFrom(t, res.body)
	if created.MaxDepth != identity.DefaultMaxDepth {
		t.Errorf("public body must not raise depth: got %d want %d", created.MaxDepth, identity.DefaultMaxDepth)
	}
	if created.AllowSelf {
		t.Error("public body must not enable allow_self")
	}
	if created.Subject != scope.Subject || created.Tenant != scope.Tenant {
		t.Errorf("subject/tenant must come from the signed token, got %+v", created)
	}
	if created.Depth != 0 || created.Status != identity.StatusActive {
		t.Errorf("root depth/status wrong: %+v", created)
	}
	if len(created.Actors) != 1 || created.Actors[0] != scope.Deployment {
		t.Errorf("root actors = %v, want [%s]", created.Actors, scope.Deployment)
	}

	// A self-delegation-capable tree (HTTP cannot set allow_self, so build the
	// second root through the store) and drive a real HTTP delegation chain.
	scope2 := uniqueScope(t)
	root := h.createRootStore(t, scope2, "root-b", true, nil)
	tokRoot := h.mint(t, scope2, "root-b")
	taskC := h.taskClient(t, scope2)

	// GET the root over mTLS + task token.
	got := h.do(t, taskC, http.MethodGet, "/v1/identity/roots/root-b", tokRoot, nil)
	if got.status != http.StatusOK {
		t.Fatalf("getRoot status=%d body=%s", got.status, got.body)
	}
	if g := grantFrom(t, got.body); g.ID != root.ID {
		t.Errorf("getRoot id mismatch: %s vs %s", g.ID, root.ID)
	}

	// Delegate a child over HTTP (subject must be preserved, actor chain grows).
	// HTTP delegation always stays inside the caller's deployment, so it is a
	// same-deployment re-entry: it is permitted only because the trusted
	// root-b manifest enabled allow_self, and the child asks to keep that
	// self-capability so the chain can extend one more level.
	childRes := h.do(t, taskC, http.MethodPost, "/v1/identity/roots/root-b/delegate", tokRoot,
		map[string]any{"task": "child-b", "context": ctxID(scope2, "child"), "capabilities": fullCaps, "ttl": "30m", "allow_self": true})
	if childRes.status != http.StatusCreated {
		t.Fatalf("delegate child status=%d body=%s", childRes.status, childRes.body)
	}
	child := grantFrom(t, childRes.body)
	if child.Subject != root.Subject {
		t.Errorf("subject mutated across delegation: %s -> %s", root.Subject, child.Subject)
	}
	if child.Depth != 1 || child.Parent != root.ID {
		t.Errorf("child depth/parent wrong: %+v", child)
	}
	if len(child.Actors) != 2 {
		t.Errorf("actor chain not extended: %v", child.Actors)
	}

	// Mint a child token and delegate a grandchild with narrowed rights.
	tokChild := h.mint(t, scope2, "child-b")
	grandRes := h.do(t, taskC, http.MethodPost, "/v1/identity/roots/child-b/delegate", tokChild,
		map[string]any{"task": "grand-b", "context": ctxID(scope2, "grand"), "capabilities": []string{"identity:read"}, "ttl": "30m"})
	if grandRes.status != http.StatusCreated {
		t.Fatalf("delegate grandchild status=%d body=%s", grandRes.status, grandRes.body)
	}
	grand := grantFrom(t, grandRes.body)
	if grand.Subject != root.Subject {
		t.Errorf("subject must be invariant to the leaf")
	}
	if len(grand.Actors) != 3 {
		t.Errorf("actor chain length %d want 3", len(grand.Actors))
	}
	if contains(grand.Capabilities, "identity:delegate") {
		t.Error("grandchild must not inherit delegate")
	}

	// Live authorize: the grandchild token lacks identity:delegate, so the
	// delegate route must deny even though the token is valid and unexpired.
	tokGrand := h.mint(t, scope2, "grand-b")
	denied := h.do(t, taskC, http.MethodPost, "/v1/identity/roots/grand-b/delegate", tokGrand,
		map[string]any{"task": "should-fail", "context": "x", "capabilities": fullCaps, "ttl": "5m"})
	if denied.status != http.StatusForbidden {
		t.Fatalf("rights intersection: status=%d body=%s", denied.status, denied.body)
	}
}

// ---------------------------------------------------------------------------
// Criterion 2 — PDF default depth 4, trusted manifest limits, cycle deny /
// self exception, atomic fanout & subtree-active quotas, budget delegation.
// ---------------------------------------------------------------------------

func TestLimitsQuotasAndBudget(t *testing.T) {
	h := newHarness(t)
	ctx := testContext(t)

	t.Run("depth ceiling is 4 by default", func(t *testing.T) {
		scope := uniqueScope(t)
		g := h.createRootStore(t, scope, "d-root", false, nil)
		if g.MaxDepth != 4 {
			t.Fatalf("default max depth = %d want 4", g.MaxDepth)
		}
	})

	t.Run("manifest depth honored", func(t *testing.T) {
		scope := uniqueScope(t)
		g := h.createRootStore(t, scope, "m-root", true, func(r *identity.RootRequest) { r.MaxDepth = 2 })
		if g.MaxDepth != 2 {
			t.Fatalf("manifest max depth = %d want 2", g.MaxDepth)
		}
	})

	t.Run("child may only narrow ceilings", func(t *testing.T) {
		scope := uniqueScope(t)
		h.createRootStore(t, scope, "n-root", true, func(r *identity.RootRequest) { r.MaxDepth = 3 })
		_, err := h.store.Delegate(ctx, scope, "n-root", identity.DelegateRequest{
			Task: "n-child", Context: ctxID(scope, "c"), Deployment: scope.Deployment,
			Capabilities: fullCaps, TTL: time.Hour, MaxDepth: 9,
		})
		if err == nil {
			t.Fatal("widening max_depth beyond parent must be forbidden")
		}
		if contracts.Code(err) != contracts.ErrForbidden {
			t.Fatalf("code = %s want FORBIDDEN", contracts.Code(err))
		}
	})

	t.Run("cycle deny and self exception", func(t *testing.T) {
		// Same-deployment re-entry is refused unless allow_self is set.
		scope := uniqueScope(t)
		h.createRootStore(t, scope, "c-root", false, nil)
		_, err := h.store.Delegate(ctx, scope, "c-root", identity.DelegateRequest{
			Task: "c-child", Context: ctxID(scope, "c"), Deployment: scope.Deployment,
			Capabilities: fullCaps, TTL: time.Hour,
		})
		if contracts.Code(err) != contracts.ErrForbidden {
			t.Fatalf("self-delegation without allow_self: code=%s err=%v", contracts.Code(err), err)
		}
		// With the self exception the same delegation is allowed.
		scope2 := uniqueScope(t)
		h.createRootStore(t, scope2, "c-root", true, nil)
		g, err := h.store.Delegate(ctx, scope2, "c-root", identity.DelegateRequest{
			Task: "c-child", Context: ctxID(scope2, "c"), Deployment: scope2.Deployment,
			Capabilities: fullCaps, TTL: time.Hour,
		})
		if err != nil {
			t.Fatalf("self exception must allow same-deployment child: %v", err)
		}
		if len(g.Actors) != 2 || g.Actors[0] != g.Actors[1] {
			t.Fatalf("self child actor chain unexpected: %v", g.Actors)
		}
	})

	t.Run("atomic fanout quota", func(t *testing.T) {
		scope := uniqueScope(t)
		h.createRootStore(t, scope, "f-root", true, func(r *identity.RootRequest) { r.MaxFanout = 2 })
		ok := 0
		var denied int32
		var wg sync.WaitGroup
		var mu sync.Mutex
		var granted int
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, err := h.store.Delegate(ctx, scope, "f-root", identity.DelegateRequest{
					Task: "f-child-" + sanitizeID(contracts.NewID()), Context: ctxID(scope, "c"),
					Deployment: scope.Deployment, Capabilities: fullCaps, TTL: time.Hour,
				})
				if err == nil {
					mu.Lock()
					granted++
					mu.Unlock()
				} else {
					atomic.AddInt32(&denied, 1)
					if contracts.Code(err) != contracts.ErrForbidden {
						t.Errorf("fanout overflow code=%s", contracts.Code(err))
					}
				}
			}(i)
		}
		wg.Wait()
		ok = granted
		if ok > 2 {
			t.Fatalf("fanout exceeded: %d children granted, ceiling 2", ok)
		}
		if ok != 2 {
			t.Fatalf("expected exactly 2 children to fit, got %d", ok)
		}
	})

	t.Run("subtree active quota", func(t *testing.T) {
		scope := uniqueScope(t)
		// Active subtree bound of 2: the root itself plus one child.
		h.createRootStore(t, scope, "a-root", true, func(r *identity.RootRequest) { r.MaxActive = 2 })
		_, err1 := h.store.Delegate(ctx, scope, "a-root", identity.DelegateRequest{
			Task: "a-child-1", Context: ctxID(scope, "c"), Deployment: scope.Deployment,
			Capabilities: fullCaps, TTL: time.Hour,
		})
		if err1 != nil {
			t.Fatalf("first child should fit: %v", err1)
		}
		_, err2 := h.store.Delegate(ctx, scope, "a-child-1", identity.DelegateRequest{
			Task: "a-child-2", Context: ctxID(scope, "c"), Deployment: scope.Deployment,
			Capabilities: fullCaps, TTL: time.Hour,
		})
		if contracts.Code(err2) != contracts.ErrForbidden {
			t.Fatalf("subtree-active overflow must be forbidden, got %v", err2)
		}
	})

	t.Run("budget delegation clamps child expiry to parent", func(t *testing.T) {
		scope := uniqueScope(t)
		root, err := h.store.CreateRoot(ctx, scope, identity.RootRequest{
			Task: "b-root", Context: ctxID(scope, "c"), Deployment: scope.Deployment,
			Capabilities: fullCaps, TTL: 30 * time.Minute, AllowSelf: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		child, err := h.store.Delegate(ctx, scope, "b-root", identity.DelegateRequest{
			Task: "b-child", Context: ctxID(scope, "c"), Deployment: scope.Deployment,
			Capabilities: fullCaps, TTL: 10 * time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		if child.ExpiresAt.After(root.ExpiresAt) {
			t.Fatalf("child expiry %s must not exceed parent %s", child.ExpiresAt, root.ExpiresAt)
		}
	})
}

// ---------------------------------------------------------------------------
// Criterion 3 — cancel-subtree / subject revoke deny immediately without agent
// cooperation; expiry/token renew never extends a grant.
// ---------------------------------------------------------------------------

func TestCancelRevokeAreImmediateAndNonExtending(t *testing.T) {
	h := newHarness(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	h.createRootStore(t, scope, "r-root", true, nil)
	// child under root
	if _, err := h.store.Delegate(ctx, scope, "r-root", identity.DelegateRequest{
		Task: "r-child", Context: ctxID(scope, "c"), Deployment: scope.Deployment,
		Capabilities: fullCaps, TTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	tokChild := h.mint(t, scope, "r-child")
	taskC := h.taskClient(t, scope)

	// Pre-cancellation: the (already issued) child token works.
	ok := h.do(t, taskC, http.MethodGet, "/v1/identity/roots/r-child", tokChild, nil)
	if ok.status != http.StatusOK {
		t.Fatalf("child get before cancel: %d %s", ok.status, ok.body)
	}

	// Non-extension: minting a token must not move the grant's expiry.
	before, err := h.store.Get(ctx, scope, "r-child")
	if err != nil {
		t.Fatal(err)
	}
	_ = h.mint(t, scope, "r-child")
	after, err := h.store.Get(ctx, scope, "r-child")
	if err != nil {
		t.Fatal(err)
	}
	if !before.ExpiresAt.Equal(after.ExpiresAt) {
		t.Fatalf("token renewal extended grant expiry: %s -> %s", before.ExpiresAt, after.ExpiresAt)
	}

	// A generic workload (an ordinary deployment cert) is NOT an operator: the
	// admin control/cancel-subtree route must refuse it before touching the store,
	// so no ordinary service can cancel a tree it was not granted.
	generic := h.do(t, h.taskClient(t, scope), http.MethodPost, "/v1/identity/control/cancel-subtree", "",
		map[string]string{"tenant": scope.Tenant, "deployment": scope.Deployment,
			"subject": scope.Subject, "task": "r-root"})
	if generic.status != http.StatusForbidden {
		t.Fatalf("ordinary workload must not reach operator cancel: status=%d body=%s", generic.status, generic.body)
	}

	// Cancel the subtree through the lineage route: a task token bound to r-root
	// (carrying identity:cancel) cancels exactly its own tree. Scope and task come
	// from the token, never from a body.
	tokRoot := h.mint(t, scope, "r-root")
	cancelRes := h.do(t, taskC, http.MethodPost, "/v1/identity/roots/r-root/cancel", tokRoot, nil)
	if cancelRes.status != http.StatusNoContent {
		t.Fatalf("lineage cancel-subtree status=%d body=%s", cancelRes.status, cancelRes.body)
	}

	// The same, still-valid, unexpired child token is now denied immediately.
	denied := h.do(t, taskC, http.MethodGet, "/v1/identity/roots/r-child", tokChild, nil)
	if denied.status != http.StatusForbidden {
		t.Fatalf("cancelled child must be denied: status=%d body=%s", denied.status, denied.body)
	}
	// Minting a new token for a cancelled grant fails at the store too.
	if _, err := h.store.MintTask(ctx, scope, "r-child", h.signer, h.audience, 0); err == nil {
		t.Fatal("mint after cancel must fail")
	}
	// Cancelling without an mTLS certificate must be refused.
	noCert := h.do(t, h.human, http.MethodPost, "/v1/identity/roots/r-root/cancel", tokRoot, nil)
	if noCert.status != http.StatusForbidden {
		t.Fatalf("lineage cancel without cert must be refused: %d", noCert.status)
	}

	// Subject revocation: every grant dies and no new root may open.
	scope2 := uniqueScope(t)
	h.createRootStore(t, scope2, "s-root", true, nil)
	tokS := h.mint(t, scope2, "s-root")
	revokeRes := h.do(t, h.controlClient(t, scope2.Tenant), http.MethodPost, "/v1/identity/control/revoke-subject", "",
		map[string]string{"tenant": scope2.Tenant, "subject": scope2.Subject})
	if revokeRes.status != http.StatusNoContent {
		t.Fatalf("revoke-subject status=%d body=%s", revokeRes.status, revokeRes.body)
	}
	if s := h.do(t, h.taskClient(t, scope2), http.MethodGet, "/v1/identity/roots/s-root", tokS, nil).status; s != http.StatusForbidden {
		t.Fatalf("revoked subject token must be denied: %d", s)
	}
	if _, err := h.store.CreateRoot(ctx, scope2, identity.RootRequest{
		Task: "s-root2", Context: ctxID(scope2, "c"), Deployment: scope2.Deployment,
		Capabilities: fullCaps, TTL: time.Hour,
	}); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("new root for revoked subject must be forbidden, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Criterion 4 — labels monotonic child->ancestor, siblings clean, sink policy.
// ---------------------------------------------------------------------------

func TestLabelsMonotonicAndSinks(t *testing.T) {
	h := newHarness(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	h.createRootStore(t, scope, "l-root", true, nil)
	for _, c := range []string{"l-child", "l-sibling"} {
		if _, err := h.store.Delegate(ctx, scope, "l-root", identity.DelegateRequest{
			Task: c, Context: ctxID(scope, "c"), Deployment: scope.Deployment,
			Capabilities: fullCaps, TTL: time.Hour,
		}); err != nil {
			t.Fatal(err)
		}
	}
	tokChild := h.mint(t, scope, "l-child")
	taskC := h.taskClient(t, scope)

	add := h.do(t, taskC, http.MethodPost, "/v1/identity/roots/l-child/labels", tokChild,
		map[string]any{"workspaces": []string{"ws-alpha"}})
	if add.status != http.StatusOK {
		t.Fatalf("addLabels status=%d body=%s", add.status, add.body)
	}

	// Ancestor (root) inherits the workspace label.
	rootRes := h.do(t, taskC, http.MethodGet, "/v1/identity/roots/l-root", tokChild, nil)
	if rootRes.status != http.StatusOK {
		t.Fatalf("get root after label: %d", rootRes.status)
	}
	if root := grantFrom(t, rootRes.body); !contains(root.Labels.Workspaces, "ws-alpha") {
		t.Errorf("ancestor must inherit label, got %v", root.Labels.Workspaces)
	}
	// Sibling stays clean.
	sib := grantFrom(t, h.do(t, taskC, http.MethodGet, "/v1/identity/roots/l-sibling", tokChild, nil).body)
	if len(sib.Labels.Workspaces) != 0 {
		t.Errorf("sibling must stay clean, got %v", sib.Labels.Workspaces)
	}

	// Monotonic accumulation on the next turn.
	add2 := h.do(t, taskC, http.MethodPost, "/v1/identity/roots/l-child/labels", tokChild,
		map[string]any{"workspaces": []string{"ws-beta"}})
	if add2.status != http.StatusOK {
		t.Fatalf("second addLabels: %d", add2.status)
	}
	rootNow := grantFrom(t, h.do(t, taskC, http.MethodGet, "/v1/identity/roots/l-root", tokChild, nil).body)
	if !contains(rootNow.Labels.Workspaces, "ws-alpha") || !contains(rootNow.Labels.Workspaces, "ws-beta") {
		t.Errorf("labels must accumulate monotonically, got %v", rootNow.Labels.Workspaces)
	}

	// Sink policy (labels.CheckWrite is the write-gate for taints/audiences).
	t.Run("sinks", func(t *testing.T) {
		secret := labels.Set{Taints: []string{labels.TaintSecret}}
		if err := labels.CheckWrite(secret, labels.Audience{Kind: labels.KindExternal, AllowExternal: true}); err == nil {
			t.Error("secret taint must never be writable")
		}
		external := labels.Set{Taints: []string{labels.TaintExternal}}
		if err := labels.CheckWrite(external, labels.Audience{Kind: labels.KindWorkspace, Workspace: "ws-1"}); err == nil {
			t.Error("external taint must require external audience or AllowExternal")
		}
		if err := labels.CheckWrite(external, labels.Audience{Kind: labels.KindExternal}); err != nil {
			t.Errorf("external audience may write external taint: %v", err)
		}
		if err := labels.CheckWrite(external, labels.Audience{Kind: labels.KindWorkspace, Workspace: "ws-1", AllowExternal: true}); err != nil {
			t.Errorf("AllowExternal workspace may write external taint: %v", err)
		}
		priv := labels.Set{Subjects: []string{"p-1"}}
		if err := labels.CheckWrite(priv, labels.Audience{Kind: labels.KindPrivate, Subject: "p-1"}); err != nil {
			t.Errorf("matching private subject may write: %v", err)
		}
		if err := labels.CheckWrite(priv, labels.Audience{Kind: labels.KindPrivate, Subject: "other"}); err == nil {
			t.Error("non-matching private subject must be denied")
		}
		if err := labels.CheckWrite(labels.Set{Subjects: []string{"p-1", "p-2"}}, labels.Audience{Kind: labels.KindPrivate, Subject: "p-1"}); err == nil {
			t.Error("multi-subject write must be denied")
		}
	})
}

// ---------------------------------------------------------------------------
// Criterion 5 — human vs task tokens are not interchangeable; foreign scope is
// invisible (404, not 403 leakage).
// ---------------------------------------------------------------------------

func TestTokenNonInterchangeabilityAndInvisibility(t *testing.T) {
	h := newHarness(t)
	ctx := testContext(t)
	scopeA := uniqueScope(t)
	scopeB := uniqueScope(t)

	tokHuman := h.signHuman(scopeA)

	t.Run("task token cannot open a human root", func(t *testing.T) {
		h.createRootStore(t, scopeA, "x-root", true, nil)
		tokTask := h.mint(t, scopeA, "x-root")
		res := h.do(t, h.human, http.MethodPost, "/v1/identity/roots", tokTask,
			map[string]any{"task": "nope", "context": "c", "capabilities": fullCaps, "ttl": "1h", "deployment": scopeA.Deployment})
		if res.status != http.StatusForbidden {
			t.Fatalf("task token on human route: %d %s", res.status, res.body)
		}
	})

	t.Run("human token cannot drive an agent route", func(t *testing.T) {
		res := h.do(t, h.taskClient(t, scopeA), http.MethodGet, "/v1/identity/roots/x-root", tokHuman, nil)
		if res.status != http.StatusForbidden {
			t.Fatalf("human token on task route: %d %s", res.status, res.body)
		}
	})

	t.Run("valid task token without mTLS is refused", func(t *testing.T) {
		tokTask := h.mint(t, scopeA, "x-root")
		res := h.do(t, h.human, http.MethodGet, "/v1/identity/roots/x-root", tokTask, nil)
		if res.status != http.StatusForbidden {
			t.Fatalf("task token without cert: %d", res.status)
		}
	})

	t.Run("cross-tenant grant is invisible", func(t *testing.T) {
		// A root exists in tenant A with a distinct task name.
		h.createRootStore(t, scopeA, "secret-task", false, nil)
		// Tenant B has its own valid root + read token.
		h.createRootStore(t, scopeB, "b-root", false, nil)
		tokB := h.mint(t, scopeB, "b-root")
		res := h.do(t, h.taskClient(t, scopeB), http.MethodGet, "/v1/identity/roots/secret-task", tokB, nil)
		if res.status != http.StatusNotFound {
			t.Fatalf("foreign grant must be invisible (404), got %d %s", res.status, res.body)
		}
	})

	t.Run("token workload must match the certificate", func(t *testing.T) {
		tokA := h.mint(t, scopeA, "x-root")
		// Present a scope-A token over a scope-B certificate.
		res := h.do(t, h.taskClient(t, scopeB), http.MethodGet, "/v1/identity/roots/x-root", tokA, nil)
		if res.status != http.StatusForbidden {
			t.Fatalf("token/cert workload mismatch: %d %s", res.status, res.body)
		}
	})

	t.Run("duplicate authorization header is refused", func(t *testing.T) {
		tokTask := h.mint(t, scopeA, "x-root")
		req, _ := http.NewRequestWithContext(testContext(t), http.MethodGet,
			h.baseURL+"/v1/identity/roots/x-root", nil)
		req.Header.Add("Authorization", "Bearer "+tokTask)
		req.Header.Add("Authorization", "Bearer "+tokTask)
		c := h.taskClient(t, scopeA)
		res, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusForbidden {
			t.Fatalf("duplicate Authorization header: %d", res.StatusCode)
		}
	})
	_ = ctx
}

// ---------------------------------------------------------------------------
// Criterion 6 — race scenarios against real PostgreSQL.
// ---------------------------------------------------------------------------

func TestConcurrentDelegateVersusCancelSubtree(t *testing.T) {
	h := newHarness(t)
	scope := uniqueScope(t)
	h.createRootStore(t, scope, "rc-root", true, nil)

	const n = 12
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen []string
	)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			task := "rc-child-" + sanitizeID(contracts.NewID())
			_, err := h.store.Delegate(testContext(t), scope, "rc-root", identity.DelegateRequest{
				Task: task, Context: ctxID(scope, "c"), Deployment: scope.Deployment,
				Capabilities: fullCaps, TTL: time.Hour,
			})
			if err == nil {
				mu.Lock()
				seen = append(seen, task)
				mu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()

	// Cancel the whole subtree, then every child that was created must be gone.
	if err := h.store.CancelSubtree(testContext(t), scope, "rc-root"); err != nil {
		t.Fatal(err)
	}
	for _, task := range seen {
		g, err := h.store.Get(testContext(t), scope, task)
		if err != nil {
			t.Fatalf("get %s after cancel: %v", task, err)
		}
		if g.Status != identity.StatusRevoked {
			t.Fatalf("child %s survived cancel-subtree (status=%s)", task, g.Status)
		}
	}
}

func TestConcurrentDelegateRespectsFanout(t *testing.T) {
	h := newHarness(t)
	scope := uniqueScope(t)
	h.createRootStore(t, scope, "rf-root", true, func(r *identity.RootRequest) { r.MaxFanout = 3 })

	const n = 12
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen []string
	)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			task := "rf-child-" + sanitizeID(contracts.NewID())
			_, err := h.store.Delegate(testContext(t), scope, "rf-root", identity.DelegateRequest{
				Task: task, Context: ctxID(scope, "c"), Deployment: scope.Deployment,
				Capabilities: fullCaps, TTL: time.Hour,
			})
			if err == nil {
				mu.Lock()
				seen = append(seen, task)
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	active := 0
	for _, task := range seen {
		g, err := h.store.Get(testContext(t), scope, task)
		if err != nil {
			t.Fatalf("get %s: %v", task, err)
		}
		if g.Status == identity.StatusActive {
			active++
		}
	}
	if active > 3 {
		t.Fatalf("fanout race exceeded ceiling: %d active, want <=3", active)
	}
	if active != 3 {
		t.Fatalf("expected the fanout ceiling to be saturated at 3, got %d", active)
	}
}

// keep strings imported even if assertions change.
var _ = strings.TrimSpace
