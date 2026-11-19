package identity_test

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/identityapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The Human -> authorized Space -> scoped task boundary (PDF 3.5). The browser
// holds only a human token for the browser audience; it cannot open a root or a
// task. A trusted service (its deployment is allowlisted) presents that human
// token over its own workload mTLS and receives a scoped task token bound to
// the SERVICE's workload, which it presents to State with its own certificate.

const exchangerDeployment = "as-space"

type exchangeHarness struct {
	t       *testing.T
	store   *identity.Store
	verif   *auth.Verifier
	ca      *testCA
	baseURL string
}

func startIDServer(t *testing.T, cfg identityapi.Config) (string, *testCA, *auth.Verifier, *identity.Store) {
	t.Helper()
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	store := newStore(t)
	cfg.Store = store
	cfg.Signer = signer
	cfg.Verifier = verifier
	cfg.Audience = testAudience
	caPool := x509.NewCertPool()
	caPool.AddCert(ca.cert)
	srv := httptest.NewUnstartedServer(identityapi.Handler(cfg))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{ca.serverCert(t)},
		ClientAuth:   tls.VerifyClientCertIfGiven,
		ClientCAs:    caPool,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv.URL, ca, verifier, store
}

func newExchangeHarness(t *testing.T, exchangers []string, required string) *exchangeHarness {
	t.Helper()
	cfg := identityapi.Config{
		Browser:              &identityapi.BrowserConfig{},
		ExchangerDeployments: exchangers,
		ExchangeRequired:     required,
	}
	base, ca, verifier, store := startIDServer(t, cfg)
	return &exchangeHarness{t: t, store: store, verif: verifier, ca: ca, baseURL: base}
}

func (h *exchangeHarness) caPool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(h.ca.cert)
	return p
}

// browserClient keeps a session cookie and presents no client certificate, like
// the real UI.
func (h *exchangeHarness) browserClient(t *testing.T) *http.Client {
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("jar: %v", err)
	}
	c := &http.Client{Jar: jar, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: h.caPool()}}}
	t.Cleanup(func() { c.CloseIdleConnections() })
	return c
}

// exchangerClient presents a workload certificate for scope, like a trusted
// service calling the control plane.
func (h *exchangeHarness) exchangerClient(t *testing.T, scope contracts.Scope) *http.Client {
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs:      h.caPool(),
		Certificates: []tls.Certificate{h.ca.clientCert(t, auth.WorkloadID(scope))},
	}}}
	t.Cleanup(func() { c.CloseIdleConnections() })
	return c
}

func (h *exchangeHarness) post(t *testing.T, c *http.Client, path string, body any, csrf bool) response {
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(testContext(t), http.MethodPost, h.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if csrf {
		req.Header.Set("X-AHP-Session", "1")
	}
	res, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return response{status: res.StatusCode, body: b}
}

// seedHuman provisions a human with the given permissions and returns its
// tenant, subject and a signed-in browser access token (aud browser, Kind human).
func (h *exchangeHarness) seedHuman(t *testing.T, perms []string) (tenant, subject, browserToken string) {
	t.Helper()
	tag := sanitizeID(contracts.NewID())
	tenant = "acme-" + tag
	subject = "human-" + tag
	username := subject + "@acme.test"
	password := "correct-horse-battery-staple"
	u := identity.BrowserUser{UserID: "u-" + tag, Tenant: tenant, Subject: subject, DisplayName: "H", Email: username}
	if err := h.store.SeedBrowserUser(testContext(t), u, username, password, []string{"user"}, perms); err != nil {
		t.Fatalf("seed: %v", err)
	}
	bc := h.browserClient(t)
	if res := h.post(t, bc, browserPrefix+"/auth/login", map[string]string{"username": username, "password": password}, true); res.status != http.StatusOK {
		t.Fatalf("login: %d %s", res.status, res.body)
	}
	res := h.post(t, bc, browserPrefix+"/auth/token", map[string]string{}, true)
	if res.status != http.StatusOK {
		t.Fatalf("token: %d %s", res.status, res.body)
	}
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(res.body, &tok); err != nil || tok.AccessToken == "" {
		t.Fatalf("decode token: %v %s", err, res.body)
	}
	// Return the browser client too by stashing it via a closure is awkward; the
	// caller that needs logout uses seedHumanWithClient.
	return tenant, subject, tok.AccessToken
}

func (h *exchangeHarness) seedHumanWithClient(t *testing.T, perms []string) (tenant, subject, browserToken string, bc *http.Client) {
	t.Helper()
	tag := sanitizeID(contracts.NewID())
	tenant = "acme-" + tag
	subject = "human-" + tag
	username := subject + "@acme.test"
	password := "correct-horse-battery-staple"
	u := identity.BrowserUser{UserID: "u-" + tag, Tenant: tenant, Subject: subject, DisplayName: "H", Email: username}
	if err := h.store.SeedBrowserUser(testContext(t), u, username, password, []string{"user"}, perms); err != nil {
		t.Fatalf("seed: %v", err)
	}
	bc = h.browserClient(t)
	if res := h.post(t, bc, browserPrefix+"/auth/login", map[string]string{"username": username, "password": password}, true); res.status != http.StatusOK {
		t.Fatalf("login: %d %s", res.status, res.body)
	}
	res := h.post(t, bc, browserPrefix+"/auth/token", map[string]string{}, true)
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.Unmarshal(res.body, &tok)
	return tenant, subject, tok.AccessToken, bc
}

func TestExchangeHumanToScopedTask(t *testing.T) {
	h := newExchangeHarness(t, []string{exchangerDeployment}, "")
	tenant, subject, browserTok := h.seedHuman(t, []string{"space:access", "space:write"})

	// A browser token cannot open a control-plane root (different audience):
	// the first credential is never gated by an authority the browser lacks.
	rootRes := h.post(t, h.browserClient(t), "/v1/identity/roots",
		map[string]any{"task": "t1", "deployment": exchangerDeployment, "ttl": "5m"}, false)
	if rootRes.status == http.StatusCreated || rootRes.status == http.StatusOK {
		t.Fatalf("browser token unexpectedly opened a root: %d %s", rootRes.status, rootRes.body)
	}

	// The trusted service exchanges the live browser session for a scoped task.
	body := map[string]any{"human_token": browserTok, "task": "thread-1"}
	res := h.post(t, h.exchangerClient(t, contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment}),
		"/v1/identity/control/exchange", body, false)
	if res.status != http.StatusOK {
		t.Fatalf("exchange: %d %s", res.status, res.body)
	}
	var out struct {
		Token string `json:"token"`
		Task  string `json:"task"`
	}
	if err := json.Unmarshal(res.body, &out); err != nil || out.Token == "" || out.Task != "thread-1" {
		t.Fatalf("decode exchange: %v %s", err, res.body)
	}

	// The returned credential is a TASK token bound to the SERVICE workload, not
	// the browser. State guards its routes exactly like transport.Task below, so
	// this verification is the integration proof on the identity side.
	claims, err := h.verif.Verify(out.Token, testAudience, auth.KindTask,
		auth.WorkloadID(contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment}))
	if err != nil {
		t.Fatalf("minted token is not a valid task token for the caller workload: %v", err)
	}
	if claims.Subject != subject || claims.Tenant != tenant || claims.Deployment != exchangerDeployment {
		t.Fatalf("task scope wrong: subject=%q tenant=%q deployment=%q", claims.Subject, claims.Tenant, claims.Deployment)
	}
	// The same token is NOT valid bound to another workload identity.
	if _, err := h.verif.Verify(out.Token, testAudience, auth.KindTask,
		auth.WorkloadID(contracts.Scope{Tenant: tenant, Deployment: "other"})); err == nil {
		t.Fatal("task token accepted for a workload other than the caller")
	}
	// The minted task token is not a human/browser credential.
	if _, err := h.verif.Verify(out.Token, browserPrefixAud(), auth.KindHuman, ""); err == nil {
		t.Fatal("task token verified as a human token")
	}
}

func browserPrefixAud() string { return "ahp-web" }

func TestExchangeFailClosed(t *testing.T) {
	h := newExchangeHarness(t, []string{exchangerDeployment}, "")
	tenant, _, browserTok := h.seedHuman(t, []string{"space:access", "space:write"})
	ok := contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment}

	t.Run("cross_tenant_exchanger_denied", func(t *testing.T) {
		res := h.post(t, h.exchangerClient(t, contracts.Scope{Tenant: "globex-x", Deployment: exchangerDeployment}),
			"/v1/identity/control/exchange", map[string]any{"human_token": browserTok, "task": "th"}, false)
		if res.status != http.StatusForbidden {
			t.Fatalf("cross-tenant exchange: %d %s", res.status, res.body)
		}
	})
	t.Run("unauthorized_deployment_denied", func(t *testing.T) {
		res := h.post(t, h.exchangerClient(t, contracts.Scope{Tenant: tenant, Deployment: "notspace"}),
			"/v1/identity/control/exchange", map[string]any{"human_token": browserTok, "task": "th"}, false)
		if res.status != http.StatusForbidden {
			t.Fatalf("unauthorized deployment exchange: %d %s", res.status, res.body)
		}
	})
	t.Run("browser_cannot_call_control", func(t *testing.T) {
		// No client certificate -> transport.Workload denies before any logic.
		res := h.post(t, h.browserClient(t), "/v1/identity/control/exchange",
			map[string]any{"human_token": browserTok, "task": "th"}, false)
		if res.status != http.StatusForbidden {
			t.Fatalf("certless exchange: %d %s", res.status, res.body)
		}
	})
	t.Run("injected_subject_rejected", func(t *testing.T) {
		res := h.post(t, h.exchangerClient(t, ok),
			"/v1/identity/control/exchange",
			map[string]any{"human_token": browserTok, "task": "th", "subject": "evil"}, false)
		if res.status != http.StatusBadRequest {
			t.Fatalf("injected subject: %d %s", res.status, res.body)
		}
	})
	t.Run("human_token_instead_of_task_denied", func(t *testing.T) {
		// A control human token (not a browser token) is not accepted as input.
		res := h.post(t, h.exchangerClient(t, ok),
			"/v1/identity/control/exchange", map[string]any{"human_token": "not.a.jwt", "task": "th"}, false)
		if res.status != http.StatusForbidden {
			t.Fatalf("bogus human token: %d %s", res.status, res.body)
		}
	})
	t.Run("revocation_reaches_exchange", func(t *testing.T) {
		tenant, subject, tok, bc := h.seedHumanWithClient(t, []string{"space:access"})
		c := h.exchangerClient(t, contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment})
		if res := h.post(t, c, "/v1/identity/control/exchange",
			map[string]any{"human_token": tok, "task": "th"}, false); res.status != http.StatusOK {
			t.Fatalf("baseline exchange: %d %s", res.status, res.body)
		}
		// Logout retires the session; the next exchange must fail immediately.
		if res := h.post(t, bc, browserPrefix+"/auth/logout", map[string]string{}, true); res.status != http.StatusOK {
			t.Fatalf("logout: %d %s", res.status, res.body)
		}
		if res := h.post(t, c, "/v1/identity/control/exchange",
			map[string]any{"human_token": tok, "task": "th"}, false); res.status != http.StatusForbidden {
			t.Fatalf("exchange after logout: %d %s", res.status, res.body)
		}
		// Subject revocation is also immediate for a fresh login.
		if err := h.store.RevokeSubject(testContext(t), tenant, subject); err != nil {
			t.Fatalf("revoke subject: %v", err)
		}
	})
}

func TestExchangeRightsAndAllowlist(t *testing.T) {
	t.Run("human_without_entitlement_denied", func(t *testing.T) {
		h := newExchangeHarness(t, []string{exchangerDeployment}, "")
		tenant, _, tok := h.seedHuman(t, []string{"space:read"}) // no space:access
		res := h.post(t, h.exchangerClient(t, contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment}),
			"/v1/identity/control/exchange", map[string]any{"human_token": tok, "task": "th"}, false)
		if res.status != http.StatusForbidden {
			t.Fatalf("no-entitlement exchange: %d %s", res.status, res.body)
		}
	})
	t.Run("capability_ceiling", func(t *testing.T) {
		h := newExchangeHarness(t, []string{exchangerDeployment}, "")
		tenant, _, tok := h.seedHuman(t, []string{"space:access", "space:write"})
		c := h.exchangerClient(t, contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment})
		// Asking only for a capability the human lacks is denied: the task can
		// never exceed its subject.
		res := h.post(t, c, "/v1/identity/control/exchange",
			map[string]any{"human_token": tok, "task": "th", "capabilities": []string{"space:admin"}}, false)
		if res.status != http.StatusForbidden {
			t.Fatalf("escalating capability: %d %s", res.status, res.body)
		}
		// Asking for a held capability succeeds.
		if res := h.post(t, c, "/v1/identity/control/exchange",
			map[string]any{"human_token": tok, "task": "th2", "capabilities": []string{"space:write"}}, false); res.status != http.StatusOK {
			t.Fatalf("held capability exchange: %d %s", res.status, res.body)
		}
	})
	t.Run("empty_allowlist_denies_everyone", func(t *testing.T) {
		h := newExchangeHarness(t, nil, "") // fail-closed
		tenant, _, tok := h.seedHuman(t, []string{"space:access"})
		res := h.post(t, h.exchangerClient(t, contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment}),
			"/v1/identity/control/exchange", map[string]any{"human_token": tok, "task": "th"}, false)
		if res.status != http.StatusForbidden {
			t.Fatalf("empty allowlist must deny: %d %s", res.status, res.body)
		}
	})
}
