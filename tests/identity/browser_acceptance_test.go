// This file is the browser-facing half of the Identity acceptance suite: the
// Agent Space UI login, session lifecycle and current-user surface, driven over
// real HTTPS against a real PostgreSQL store with real Ed25519 tokens.
//
// The invariants it exists to protect:
//   - a login proves a credential and the identity comes from the account row,
//     never from the request;
//   - logout, refresh rotation and a subject tombstone stop an already-issued
//     access token immediately, without the browser cooperating;
//   - refreshing proves liveness, it never extends the session's authority;
//   - a human token cannot act as a task token and vice versa;
//   - one tenant cannot see another's accounts.
package identity_test

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/identityapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const browserPrefix = "/u-ahp/v1"

// browserHarness is a running identity service with the UI surface mounted,
// plus clients that behave like a browser (cookie jar, no client certificate)
// and like a workload (mTLS certificate, no JWT of its own).
type browserHarness struct {
	t        *testing.T
	store    *identity.Store
	pool     *pgxpool.Pool
	signer   *auth.Signer
	audience string
	baseURL  string
	ca       *testCA
}

func newBrowserHarness(t *testing.T) *browserHarness {
	t.Helper()
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	store := newStore(t)
	pool := browserPool(t)

	caPool := x509.NewCertPool()
	caPool.AddCert(ca.cert)

	cfg := identityapi.Config{
		Store: store, Verifier: verifier, Signer: signer, Audience: testAudience,
		Browser: &identityapi.BrowserConfig{FeatureFlags: map[string]bool{"as-admin": false}},
	}
	srv := httptest.NewUnstartedServer(identityapi.Handler(cfg))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{ca.serverCert(t)},
		ClientAuth:   tls.VerifyClientCertIfGiven,
		ClientCAs:    caPool,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return &browserHarness{t: t, store: store, pool: pool, signer: signer,
		audience: testAudience, baseURL: srv.URL, ca: ca}
}

// browserClient behaves like the UI: it keeps the session cookie and never
// presents a client certificate.
func (h *browserHarness) browserClient(t *testing.T) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookie jar: %v", err)
	}
	c := &http.Client{Jar: jar, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: h.caPool()},
	}}
	t.Cleanup(func() { c.CloseIdleConnections() })
	return c
}

func (h *browserHarness) caPool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(h.ca.cert)
	return p
}

// workloadClient presents a verified client certificate for scope: the only
// way to reach the control-plane introspection route.
func (h *browserHarness) workloadClient(t *testing.T, scope contracts.Scope) *http.Client {
	t.Helper()
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs:      h.caPool(),
		Certificates: []tls.Certificate{h.ca.clientCert(t, auth.WorkloadID(scope))},
	}}}
	t.Cleanup(func() { c.CloseIdleConnections() })
	return c
}

type browserCall struct {
	method, path string
	body         any
	bearer       string
	csrf         bool
	noCookie     bool
}

func (h *browserHarness) call(t *testing.T, c *http.Client, in browserCall) response {
	t.Helper()
	var rdr io.Reader
	if in.body != nil {
		raw, err := json.Marshal(in.body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(testContext(t), in.method, h.baseURL+in.path, rdr)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if in.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if in.csrf {
		req.Header.Set("X-AHP-Session", "1")
	}
	if in.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+in.bearer)
	}
	if in.noCookie {
		// Clone the transport-level request without touching the jar: an
		// unauthenticated caller must not inherit a session by accident.
		req.AddCookie(&http.Cookie{Name: "ahp_session", Value: ""})
		req.Header.Set("Cookie", "")
	}
	res, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", in.method, in.path, err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return response{status: res.StatusCode, body: b}
}

// seedAccount provisions a human account through the same store seam a
// deployment would use. Passwords stay in the test; only the argon2id digest
// reaches the database.
func (h *browserHarness) seedAccount(t *testing.T, username, password, tenant, subject, displayName string,
	roles, permissions []string) identity.BrowserUser {
	t.Helper()
	user := identity.BrowserUser{
		UserID:      sanitizeID(username),
		Tenant:      tenant,
		Subject:     subject,
		DisplayName: displayName,
		Email:       username,
	}
	if err := h.store.SeedBrowserUser(testContext(t), user, username, password, roles, permissions); err != nil {
		t.Fatalf("seed %s: %v", username, err)
	}
	return user
}

func (h *browserHarness) login(t *testing.T, c *http.Client, username, password string) response {
	t.Helper()
	return h.call(t, c, browserCall{
		method: http.MethodPost, path: browserPrefix + "/auth/login",
		body: map[string]string{"username": username, "password": password}, csrf: true,
	})
}

func (h *browserHarness) token(t *testing.T, c *http.Client) response {
	t.Helper()
	return h.call(t, c, browserCall{method: http.MethodPost, path: browserPrefix + "/auth/token", csrf: true})
}

func (h *browserHarness) refresh(t *testing.T, c *http.Client) response {
	t.Helper()
	return h.call(t, c, browserCall{method: http.MethodPost, path: browserPrefix + "/auth/refresh", csrf: true})
}

func (h *browserHarness) logout(t *testing.T, c *http.Client) response {
	t.Helper()
	return h.call(t, c, browserCall{method: http.MethodPost, path: browserPrefix + "/auth/logout", csrf: true})
}

func (h *browserHarness) meUser(t *testing.T, c *http.Client, bearer string) response {
	t.Helper()
	return h.call(t, c, browserCall{method: http.MethodGet, path: browserPrefix + "/me/user", bearer: bearer})
}

func (h *browserHarness) meFlags(t *testing.T, c *http.Client, bearer string) response {
	t.Helper()
	return h.call(t, c, browserCall{method: http.MethodGet, path: browserPrefix + "/me/flags", bearer: bearer})
}

// signedIn performs the whole browser handshake and returns the access token.
func (h *browserHarness) signedIn(t *testing.T, c *http.Client, username, password string) string {
	t.Helper()
	if res := h.login(t, c, username, password); res.status != http.StatusOK {
		t.Fatalf("login %s: status %d body %s", username, res.status, res.body)
	}
	res := h.token(t, c)
	if res.status != http.StatusOK {
		t.Fatalf("token %s: status %d body %s", username, res.status, res.body)
	}
	var view struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(res.body, &view); err != nil {
		t.Fatalf("decode token: %v (%s)", err, res.body)
	}
	if view.AccessToken == "" || view.TokenType != "Bearer" || view.ExpiresIn <= 0 {
		t.Fatalf("unexpected token view: %s", res.body)
	}
	return view.AccessToken
}

// twoAccounts creates two humans in two different tenants, the minimum that can
// prove an identity cannot be chosen and a tenant cannot be crossed.
func (h *browserHarness) twoAccounts(t *testing.T) (a, b struct{ username, password, tenant, subject string }) {
	t.Helper()
	tag := sanitizeID(fmt.Sprintf("%d", time.Now().UnixNano()))
	a = struct{ username, password, tenant, subject string }{
		username: "alice-" + tag + "@acme.test",
		password: "correct-horse-battery-staple",
		tenant:   "acme-" + tag, subject: "alice-" + tag,
	}
	b = struct{ username, password, tenant, subject string }{
		username: "boris-" + tag + "@globex.test",
		password: "another-correct-horse-staple",
		tenant:   "globex-" + tag, subject: "boris-" + tag,
	}
	h.seedAccount(t, a.username, a.password, a.tenant, a.subject, "Alice A",
		[]string{"user", "developer"}, []string{"space:read", "space:write"})
	h.seedAccount(t, b.username, b.password, b.tenant, b.subject, "Boris B",
		[]string{"user"}, []string{"space:read"})
	return a, b
}

func TestBrowserLoginWhoamiAndLogout(t *testing.T) {
	h := newBrowserHarness(t)
	a, b := h.twoAccounts(t)

	ca := h.browserClient(t)
	tokA := h.signedIn(t, ca, a.username, a.password)

	res := h.meUser(t, ca, tokA)
	if res.status != http.StatusOK {
		t.Fatalf("me/user: status %d body %s", res.status, res.body)
	}
	var view struct {
		ID, Subject, Tenant, DisplayName string
	}
	if err := json.Unmarshal(res.body, &view); err != nil {
		t.Fatalf("decode me/user: %v", err)
	}
	// The identity the UI is told is exactly the account the credential belongs
	// to: nothing on the request selected it.
	if view.Subject != a.subject || view.Tenant != a.tenant || view.DisplayName != "Alice A" {
		t.Fatalf("whoami leaked a different identity: %+v", view)
	}

	res = h.meFlags(t, ca, tokA)
	var flags struct {
		Roles, Permissions []string
		FeatureFlags       map[string]bool
	}
	if err := json.Unmarshal(res.body, &flags); err != nil {
		t.Fatalf("decode flags: %v", err)
	}
	if len(flags.Roles) != 2 || len(flags.Permissions) != 2 {
		t.Fatalf("unexpected role/permission set: %s", res.body)
	}
	if _, ok := flags.FeatureFlags["as-admin"]; !ok {
		t.Fatalf("feature flags not surfaced: %s", res.body)
	}

	// The second human is a genuinely different principal.
	cb := h.browserClient(t)
	tokB := h.signedIn(t, cb, b.username, b.password)
	res = h.meUser(t, cb, tokB)
	if !bytes.Contains(res.body, []byte(b.subject)) || bytes.Contains(res.body, []byte(a.subject)) {
		t.Fatalf("second session resolved the wrong subject: %s", res.body)
	}

	// Logout ends the session; the previously issued token is dead at once and
	// no new token can be minted without logging in again.
	if res := h.logout(t, ca); res.status != http.StatusOK {
		t.Fatalf("logout: status %d body %s", res.status, res.body)
	}
	if res := h.meUser(t, ca, tokA); res.status != http.StatusUnauthorized {
		t.Fatalf("token survived logout: status %d body %s", res.status, res.body)
	}
	if res := h.token(t, ca); res.status != http.StatusUnauthorized {
		t.Fatalf("token mint survived logout: status %d body %s", res.status, res.body)
	}
	// The other human's session is untouched by alice's logout.
	if res := h.meUser(t, cb, tokB); res.status != http.StatusOK {
		t.Fatalf("logout of one subject broke another: status %d", res.status)
	}
}

func TestBrowserLoginRefusesChosenIdentityAndBadInput(t *testing.T) {
	h := newBrowserHarness(t)
	a, _ := h.twoAccounts(t)
	c := h.browserClient(t)

	// A credential is required and it must be right. Unknown account and wrong
	// password answer identically, so login cannot enumerate accounts.
	bad := h.login(t, c, a.username, "definitely-not-the-password")
	if bad.status != http.StatusUnauthorized {
		t.Fatalf("wrong password: status %d body %s", bad.status, bad.body)
	}
	unknown := h.login(t, c, "nobody-here-"+sanitizeID(fmt.Sprint(time.Now().UnixNano()))+"@nowhere.test",
		"also-not-the-password")
	if unknown.status != http.StatusUnauthorized {
		t.Fatalf("unknown account: status %d", unknown.status)
	}
	if !bytes.Equal(bad.body, unknown.body) {
		t.Fatalf("login distinguishes unknown account from wrong password: %q vs %q", bad.body, unknown.body)
	}

	// The request may not name an identity or a tenant.
	for _, extra := range []map[string]string{
		{"username": a.username, "password": a.password, "tenant": "someone-elses-tenant"},
		{"username": a.username, "password": a.password, "subject": "someone-else"},
		{"username": a.username, "password": a.password, "deployment": "as-turn-runner"},
	} {
		res := h.call(t, c, browserCall{
			method: http.MethodPost, path: browserPrefix + "/auth/login", body: extra, csrf: true,
		})
		if res.status != http.StatusBadRequest {
			t.Fatalf("login with %v: status %d body %s, want 400", extra, res.status, res.body)
		}
	}

	// No CSRF header means a cross-site form post: refused before any work.
	res := h.call(t, c, browserCall{
		method: http.MethodPost, path: browserPrefix + "/auth/login",
		body: map[string]string{"username": a.username, "password": a.password},
	})
	if res.status != http.StatusForbidden {
		t.Fatalf("login without session header: status %d body %s", res.status, res.body)
	}

	// No session means no token, whatever the client claims.
	fresh := h.browserClient(t)
	if res := h.token(t, fresh); res.status != http.StatusUnauthorized {
		t.Fatalf("token without session: status %d body %s", res.status, res.body)
	}

	// The current-user endpoints take no parameters: identity comes from the
	// token, so a caller cannot ask to be looked up as somebody else.
	tok := h.signedIn(t, c, a.username, a.password)
	for _, q := range []string{"?subject=" + a.subject, "?tenant=" + a.tenant, "?as=admin"} {
		res := h.call(t, c, browserCall{
			method: http.MethodGet, path: browserPrefix + "/me/user" + q, bearer: tok,
		})
		if res.status != http.StatusBadRequest {
			t.Fatalf("me/user%s: status %d body %s, want 400", q, res.status, res.body)
		}
	}
}

func TestBrowserRevocationIsImmediateWithoutTheBrowser(t *testing.T) {
	h := newBrowserHarness(t)
	a, _ := h.twoAccounts(t)
	c := h.browserClient(t)
	tok := h.signedIn(t, c, a.username, a.password)
	if res := h.meUser(t, c, tok); res.status != http.StatusOK {
		t.Fatalf("baseline me/user: status %d", res.status)
	}

	// Rotation retires every token minted under the previous epoch.
	res := h.refresh(t, c)
	if res.status != http.StatusOK {
		t.Fatalf("refresh: status %d body %s", res.status, res.body)
	}
	if res := h.meUser(t, c, tok); res.status != http.StatusUnauthorized {
		t.Fatalf("token from a rotated epoch still works: status %d", res.status)
	}

	// A subject tombstone written by the control plane kills the live session
	// and its current token with no cooperation from the browser at all.
	scope := contracts.Scope{Tenant: a.tenant, Deployment: "as-web", Subject: a.subject}
	if err := h.store.RevokeSubject(testContext(t), scope.Tenant, scope.Subject); err != nil {
		t.Fatalf("revoke subject: %v", err)
	}
	res = h.refresh(t, c) // the newest credential the browser holds
	if res.status != http.StatusUnauthorized {
		t.Fatalf("refresh survived a subject revocation: status %d body %s", res.status, res.body)
	}
	if res := h.token(t, c); res.status != http.StatusUnauthorized {
		t.Fatalf("token mint survived a subject revocation: status %d", res.status)
	}
	// And the revoked subject cannot simply log back in.
	if res := h.login(t, h.browserClient(t), a.username, a.password); res.status != http.StatusForbidden {
		t.Fatalf("revoked subject logged in: status %d body %s", res.status, res.body)
	}
}

func TestBrowserRefreshNeverExtendsSessionAuthority(t *testing.T) {
	h := newBrowserHarness(t)
	a, _ := h.twoAccounts(t)
	c := h.browserClient(t)
	if res := h.login(t, c, a.username, a.password); res.status != http.StatusOK {
		t.Fatalf("login: status %d body %s", res.status, res.body)
	}
	// Read the session row rather than trusting the response shape.
	var absolute, expires time.Time
	var epoch int64
	if err := h.pool.QueryRow(testContext(t), `
		SELECT absolute_at, expires_at, epoch FROM ahp_identity.browser_sessions
		 WHERE tenant = $1 AND subject = $2`, a.tenant, a.subject).
		Scan(&absolute, &expires, &epoch); err != nil {
		t.Fatalf("read session: %v", err)
	}
	for i := 0; i < 5; i++ {
		if res := h.refresh(t, c); res.status != http.StatusOK {
			t.Fatalf("refresh %d: status %d body %s", i, res.status, res.body)
		}
	}
	var absolute2, expires2 time.Time
	var epoch2 int64
	if err := h.pool.QueryRow(testContext(t), `
		SELECT absolute_at, expires_at, epoch FROM ahp_identity.browser_sessions
		 WHERE tenant = $1 AND subject = $2`, a.tenant, a.subject).
		Scan(&absolute2, &expires2, &epoch2); err != nil {
		t.Fatalf("reread session: %v", err)
	}
	if !absolute.Equal(absolute2) {
		t.Fatalf("refresh moved the absolute deadline: %v -> %v", absolute, absolute2)
	}
	if epoch2 != epoch+5 {
		t.Fatalf("expected 5 rotations from epoch %d, got %d", epoch, epoch2)
	}
	if expires2.After(absolute2) {
		t.Fatalf("idle deadline outran the absolute deadline: %v > %v", expires2, absolute2)
	}

	// The database itself refuses an attempt to extend, so no writer can do it
	// by other means.
	if _, err := h.pool.Exec(testContext(t), `
		UPDATE ahp_identity.browser_sessions
		   SET absolute_at = absolute_at + interval '1 hour'
		 WHERE tenant = $1 AND subject = $2`, a.tenant, a.subject); err == nil {
		t.Fatal("extending a session absolute deadline succeeded")
	} else if !strings.Contains(err.Error(), "may not be extended") {
		t.Fatalf("unexpected error extending deadline: %v", err)
	}

	// A session past its absolute deadline cannot be refreshed even though it
	// was used moments ago. Backdating created_at keeps the row's own ordering
	// constraints true while pulling the deadline into the past.
	if _, err := h.pool.Exec(testContext(t), `
		UPDATE ahp_identity.browser_sessions
		   SET created_at   = clock_timestamp() - interval '2 days',
		       last_seen_at = clock_timestamp() - interval '2 days',
		       absolute_at  = clock_timestamp() - interval '1 second',
		       expires_at   = clock_timestamp() - interval '1 second'
		 WHERE tenant = $1 AND subject = $2`, a.tenant, a.subject); err != nil {
		t.Fatalf("shorten deadline: %v", err)
	}
	if res := h.refresh(t, c); res.status != http.StatusUnauthorized {
		t.Fatalf("refresh past the absolute deadline succeeded: status %d body %s", res.status, res.body)
	}
}

func TestBrowserHumanAndTaskAndBrowserCredentialsAreNotInterchangeable(t *testing.T) {
	h := newBrowserHarness(t)
	a, _ := h.twoAccounts(t)
	scope := contracts.Scope{Tenant: a.tenant, Deployment: "as-web", Subject: a.subject}
	c := h.browserClient(t)
	browserTok := h.signedIn(t, c, a.username, a.password)

	rootTask := "web-root-" + sanitizeID(a.subject)
	body := map[string]any{"task": rootTask, "context": ctxID(scope, rootTask),
		"deployment": scope.Deployment, "ttl": "10m", "capabilities": []string{"identity:read"}}

	// Signing in to the UI is not the general human authority: a browser
	// credential is refused by the control-plane human route, which is a
	// different audience.
	res := h.call(t, c, browserCall{
		method: http.MethodPost, path: "/v1/identity/roots", bearer: browserTok, body: body,
	})
	if res.status != http.StatusForbidden {
		t.Fatalf("browser credential reached the control plane: status %d body %s", res.status, res.body)
	}
	// The real human control token for the same subject is accepted there, so
	// the refusal above is the audience boundary, not an unknown subject.
	control := h.controlHumanToken(t, scope)
	if res := h.call(t, c, browserCall{
		method: http.MethodPost, path: "/v1/identity/roots", bearer: control, body: body,
	}); res.status != http.StatusCreated {
		t.Fatalf("human control token rejected: status %d body %s", res.status, res.body)
	}
	// In the other direction: a control-plane human token cannot open the
	// browser surface.
	if res := h.meUser(t, c, control); res.status != http.StatusUnauthorized {
		t.Fatalf("control token opened the browser surface: status %d body %s", res.status, res.body)
	}

	// A browser credential is not a task credential either: with a workload
	// certificate in hand it still cannot exercise a task-guarded route.
	grant := h.mustCreateRoot(t, scope, "t-"+sanitizeID(a.subject))
	_ = grant
	admin := h.workloadClient(t, scope)
	res = h.call(t, admin, browserCall{
		method: http.MethodPost,
		path:   "/v1/identity/roots/" + url.PathEscape("t-"+sanitizeID(a.subject)) + "/delegate",
		bearer: browserTok,
		body: map[string]any{"task": "child-" + sanitizeID(a.subject),
			"context":      ctxID(scope, "child-"+sanitizeID(a.subject)),
			"capabilities": []string{"identity:read"}, "ttl": "5m"},
	})
	if res.status != http.StatusForbidden {
		t.Fatalf("browser credential exercised a task route: status %d body %s", res.status, res.body)
	}
	// And a task token cannot open the browser surface.
	taskTok := h.mustMintTask(t, scope, "t-"+sanitizeID(a.subject))
	if res := h.meUser(t, c, taskTok); res.status != http.StatusUnauthorized {
		t.Fatalf("task token opened the browser surface: status %d body %s", res.status, res.body)
	}
}

func TestBrowserForeignTenantAccountsAreInvisible(t *testing.T) {
	h := newBrowserHarness(t)
	a, b := h.twoAccounts(t)

	// Reading an account by (tenant, subject) is scope-filtered: the row of the
	// other tenant is not found, which is also what stops existence leaking.
	if _, err := h.store.BrowserUserBySubject(testContext(t), a.tenant, b.subject); err == nil {
		t.Fatal("a foreign tenant subject resolved")
	} else if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign subject error code %s, want NOT_FOUND", contracts.Code(err))
	}
	found, err := h.store.BrowserUserBySubject(testContext(t), b.tenant, b.subject)
	if err != nil || found.UserID == "" {
		t.Fatalf("own subject not found: %v", err)
	}

	// Two sessions for the same account are independent; killing one leaves the
	// other's authority intact.
	c1, c2 := h.browserClient(t), h.browserClient(t)
	tok1 := h.signedIn(t, c1, a.username, a.password)
	tok2 := h.signedIn(t, c2, a.username, a.password)
	if res := h.logout(t, c1); res.status != http.StatusOK {
		t.Fatalf("logout c1: status %d", res.status)
	}
	if res := h.meUser(t, c1, tok1); res.status != http.StatusUnauthorized {
		t.Fatalf("revoked session token still works: status %d", res.status)
	}
	if tok1 == tok2 {
		t.Fatal("two sessions of one principal minted an identical credential: a logout would not be per-session")
	}
	if res := h.meUser(t, c2, tok2); res.status != http.StatusOK {
		t.Fatalf("one session's logout revoked another: status %d", res.status)
	}

	// The credential store itself cannot be driven across tenants: signing in
	// yields exactly the account's own tenant.
	cb := h.browserClient(t)
	tokB := h.signedIn(t, cb, b.username, b.password)
	if res := h.meUser(t, cb, tokB); !bytes.Contains(res.body, []byte(b.tenant)) ||
		bytes.Contains(res.body, []byte(a.tenant)) {
		t.Fatalf("boris resolved into alice's tenant: %s", res.body)
	}
	// The root a human may open lands in the token's tenant only, never in a
	// tenant named by the caller.
	scope := contracts.Scope{Tenant: b.tenant, Deployment: "as-web", Subject: b.subject}
	grant := h.mustCreateRoot(t, scope, "root-"+sanitizeID(b.subject))
	if grant.Tenant != b.tenant || grant.Subject != b.subject {
		t.Fatalf("root landed in a foreign scope: %+v", grant)
	}
}

func TestBrowserConcurrentRefreshAndRevocation(t *testing.T) {
	h := newBrowserHarness(t)
	a, _ := h.twoAccounts(t)
	c := h.browserClient(t)
	first := h.signedIn(t, c, a.username, a.password)

	const racers = 8
	var wg sync.WaitGroup
	results := make([]int, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Every racer shares the one session (one cookie jar), so this is a
			// race over a single session row.
			results[i] = h.refresh(t, c).status
		}(i)
	}
	wg.Wait()
	for i, status := range results {
		if status != http.StatusOK {
			t.Fatalf("concurrent refresh %d: status %d (all must serialise cleanly)", i, status)
		}
	}
	// Whatever the interleaving, the token minted before the storm is dead: the
	// epoch moved at least once.
	if res := h.meUser(t, c, first); res.status != http.StatusUnauthorized {
		t.Fatalf("pre-rotation token survived %d rotations: status %d", racers, res.status)
	}

	res := h.token(t, c)
	var view struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(res.body, &view); err != nil {
		t.Fatalf("decode token: %v", err)
	}
	var sessionID string
	if err := h.pool.QueryRow(testContext(t), `
		SELECT session_id FROM ahp_identity.browser_sessions
		 WHERE tenant = $1 AND subject = $2`, a.tenant, a.subject).Scan(&sessionID); err != nil {
		t.Fatalf("read session: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := h.store.RevokeBrowserSession(testContext(t), sessionID); err != nil {
			t.Errorf("revoke session: %v", err)
		}
	}()
	// Hammer the endpoint while the revocation lands; only the end state is
	// interesting: a revoked session must not serve the token any more.
	for i := 0; i < 20; i++ {
		h.meUser(t, c, view.AccessToken)
	}
	<-done
	if res := h.meUser(t, c, view.AccessToken); res.status != http.StatusUnauthorized {
		t.Fatalf("token outlived its revoked session: status %d", res.status)
	}
}

func TestBrowserSeedingRejectsWeakInputAndUnknownAccounts(t *testing.T) {
	h := newBrowserHarness(t)
	tag := sanitizeID(fmt.Sprint(time.Now().UnixNano()))

	if err := h.store.SeedBrowserUser(testContext(t), identity.BrowserUser{
		UserID: "u-" + tag, Tenant: "t-" + tag, Subject: "s-" + tag,
	}, "user-"+tag+"@x.test", "short", nil, nil); err == nil {
		t.Fatal("a too-short password was accepted")
	}
	if err := h.store.SeedBrowserUser(testContext(t), identity.BrowserUser{
		UserID: "u-" + tag, Tenant: "", Subject: "s-" + tag,
	}, "user2-"+tag+"@x.test", "long-enough-password", nil, nil); err == nil {
		t.Fatal("an account without a tenant was accepted")
	}
	if _, ok, err := h.store.AuthenticateBrowser(testContext(t), "definitely-absent", "whatever"); ok || err != nil {
		t.Fatalf("absent account authenticated: ok=%v err=%v", ok, err)
	}
}

// --- helpers used only by the browser suite --------------------------------

// browserPool opens the same isolated database the store uses, so assertions
// can read the rows the service wrote instead of trusting the response shape.
func browserPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, _ := isolatedPool(t)
	return pool
}

// controlHumanToken mints the general human credential of the control plane,
// the credential a UI must never hold.
func (h *browserHarness) controlHumanToken(t *testing.T, scope contracts.Scope) string {
	t.Helper()
	tok, err := h.signer.Sign(auth.Claims{
		Audience: h.audience, Kind: auth.KindHuman,
		Tenant: scope.Tenant, Subject: scope.Subject,
	}, time.Hour)
	if err != nil {
		t.Fatalf("sign control token: %v", err)
	}
	return tok
}

func (h *browserHarness) mustCreateRoot(t *testing.T, scope contracts.Scope, task string) identity.Grant {
	t.Helper()
	grant, err := h.store.CreateRoot(testContext(t), scope, identity.RootRequest{
		Task: task, Context: ctxID(scope, task), Deployment: scope.Deployment,
		Capabilities: append([]string{}, fullCaps...), TTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("create root %s: %v", task, err)
	}
	return grant
}

func (h *browserHarness) mustMintTask(t *testing.T, scope contracts.Scope, task string) string {
	t.Helper()
	tok, err := h.store.MintTask(testContext(t), scope, task, h.signer, h.audience, 0)
	if err != nil {
		t.Fatalf("mint task %s: %v", task, err)
	}
	return tok
}
