package identity_test

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/internal/identityapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Every refusal on /v1/identity/control/exchange must carry a stable, machine-
// readable reason code in the response body's "code" field, while the deny
// decision itself is unchanged (fail-closed preserved). These tests prove the
// codes are emitted on a REAL Identity (no mocks), that the cross-tenant
// boundary stays indistinguishable (no tenant oracle), that refusals never
// leak the human token or a DSN, and that previous successes still succeed.

// captureLogger collects slog records so a test can assert the WARN journal
// carries the reason code and request identifiers without a secret.
// It backs a slog.TextHandler so the assertion reads the exact journal bytes.
type captureLogger struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *captureLogger) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}
func (c *captureLogger) handler() slog.Handler {
	return slog.NewTextHandler(c, &slog.HandlerOptions{Level: slog.LevelWarn})
}
func (c *captureLogger) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Split(strings.TrimRight(c.buf.String(), "\n"), "\n")
}
func (c *captureLogger) contains(sub string) bool {
	for _, l := range c.snapshot() {
		if strings.Contains(l, sub) {
			return true
		}
	}
	return false
}

// exExchangeCode posts an exchange and returns the status plus the response
// body's "code" and raw body, so a test can branch on the reason code.
func exExchangeCode(t *testing.T, h *exchangeHarness, c *http.Client, humanToken, task string, caps []string) (int, string, []byte) {
	t.Helper()
	body := map[string]any{"human_token": humanToken, "task": task}
	if caps != nil {
		body["capabilities"] = caps
	}
	res := h.post(t, c, "/v1/identity/control/exchange", body, false)
	var v struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(res.body, &v)
	return res.status, v.Code, res.body
}

// newExchangeHarnessLogger wires an exchange server whose WARN journal is
// captured by clog (a real logger, not a no-op).
func newExchangeHarnessLogger(t *testing.T, exchangers []string, clog *captureLogger) *exchangeHarness {
	t.Helper()
	cfg := identityapi.Config{
		Browser:              &identityapi.BrowserConfig{},
		ExchangerDeployments: exchangers,
		Logger:               slog.New(clog.handler()),
	}
	base, ca, verifier, store := startIDServer(t, cfg)
	return &exchangeHarness{t: t, store: store, verif: verifier, ca: ca, baseURL: base}
}

// --- DB-free wire codes (reached before any Store call) -------------------

func TestExchangeRefusalCodesWire(t *testing.T) {
	base, ca := newWireServer(t, []string{exchangerDeployment})
	notAllowed := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs:      ca.pool(),
		Certificates: []tls.Certificate{ca.clientCert(t, auth.WorkloadID(contracts.Scope{Tenant: "acme", Deployment: "mallory"}))},
	}}}
	allow := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs:      ca.pool(),
		Certificates: []tls.Certificate{ca.clientCert(t, auth.WorkloadID(contracts.Scope{Tenant: "acme", Deployment: exchangerDeployment}))},
	}}}

	t.Run("non_allowlisted_deployment_azp_mismatch", func(t *testing.T) {
		code := postForCode(t, base, notAllowed, map[string]any{"human_token": "x", "task": "t"})
		if code != identityapi.ReasonAzpMismatch {
			t.Fatalf("want code %q, got %q", identityapi.ReasonAzpMismatch, code)
		}
	})
	t.Run("unverifiable_human_token_token_use_invalid", func(t *testing.T) {
		code := postForCode(t, base, allow, map[string]any{"human_token": "not.a.jwt", "task": "t"})
		if code != identityapi.ReasonTokenUseInvalid {
			t.Fatalf("want code %q, got %q", identityapi.ReasonTokenUseInvalid, code)
		}
	})
}

func postForCode(t *testing.T, base string, c *http.Client, body map[string]any) string {
	t.Helper()
	res := exchangePostResponse(t, base, c, body)
	var v struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(res.body, &v)
	if res.status != http.StatusForbidden {
		t.Fatalf("status: want 403 got %d body=%s", res.status, res.body)
	}
	return v.Code
}

// --- real-Identity codes (PostgreSQL) -------------------------------------

func TestExchangeRefusalCodesRealIdentity(t *testing.T) {
	clog := &captureLogger{}
	h := newExchangeHarnessLogger(t, []string{exchangerDeployment}, clog)

	t.Run("exchange_permission_missing", func(t *testing.T) {
		tenant, _, tok := h.seedHuman(t, []string{"some:other"})
		c := h.exchangerClient(t, contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment})
		status, code, body := exExchangeCode(t, h, c, tok, uniqTask("perm"), nil)
		if status != http.StatusForbidden {
			t.Fatalf("status: want 403 got %d body=%s", status, body)
		}
		if code != identityapi.ReasonExchangePermissionMissing {
			t.Fatalf("want code %q got %q", identityapi.ReasonExchangePermissionMissing, code)
		}
		if !clog.contains(identityapi.ReasonExchangePermissionMissing) {
			t.Fatalf("WARN journal missing reason code: %v", clog.snapshot())
		}
	})

	t.Run("capability_widening", func(t *testing.T) {
		tenant, _, tok := h.seedHuman(t, []string{"space:access"})
		c := h.exchangerClient(t, contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment})
		status, code, body := exExchangeCode(t, h, c, tok, uniqTask("widen"), []string{"not:held"})
		if status != http.StatusForbidden {
			t.Fatalf("status: want 403 got %d body=%s", status, body)
		}
		if code != identityapi.ReasonCapabilityWidening {
			t.Fatalf("want code %q got %q", identityapi.ReasonCapabilityWidening, code)
		}
	})

	t.Run("grant_expired_after_logout", func(t *testing.T) {
		tenant, _, tok, bc := h.seedHumanWithClient(t, []string{"space:access"})
		if res := h.post(t, bc, browserPrefix+"/auth/logout", map[string]string{}, true); res.status != http.StatusOK {
			t.Fatalf("logout: %d %s", res.status, res.body)
		}
		c := h.exchangerClient(t, contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment})
		status, code, body := exExchangeCode(t, h, c, tok, uniqTask("logout"), nil)
		if status != http.StatusForbidden {
			t.Fatalf("status: want 403 got %d body=%s", status, body)
		}
		if code != identityapi.ReasonGrantExpired {
			t.Fatalf("want code %q got %q", identityapi.ReasonGrantExpired, code)
		}
	})

	// Cross-tenant exchange stays INDISTINGUISHABLE: the wire code is the
	// generic FORBIDDEN, never a tenant- or subject-revealing code. The journal
	// still records the internal cause.
	t.Run("cross_tenant_boundary_is_opaque", func(t *testing.T) {
		humanTenant, _, tok := h.seedHuman(t, []string{"space:access"})
		_ = humanTenant
		// The exchanger certificate is scoped to a DIFFERENT tenant but an
		// allowlisted deployment (the allowlist is by deployment, not tenant).
		c := h.exchangerClient(t, contracts.Scope{Tenant: "tenant-B-" + sanitizeID(contracts.NewID()), Deployment: exchangerDeployment})
		status, code, body := exExchangeCode(t, h, c, tok, uniqTask("crosstenant"), nil)
		if status != http.StatusForbidden {
			t.Fatalf("status: want 403 got %d body=%s", status, body)
		}
		if code != contracts.ErrForbidden {
			t.Fatalf("cross-tenant must be opaque FORBIDDEN, got %q", code)
		}
		if code == "tenant_mismatch" || strings.Contains(strings.ToLower(string(body)), "tenant") {
			t.Fatalf("cross-tenant refusal must not reveal tenant cause on the wire: %s", body)
		}
		if !clog.contains("tenant_mismatch") {
			t.Fatalf("WARN journal must still record the internal cross-tenant cause: %v", clog.snapshot())
		}
	})

	// A denial body must never carry the presented human token, a DSN scheme,
	// or any signing material.
	t.Run("refusal_body_carries_no_secrets", func(t *testing.T) {
		tenant, _, tok := h.seedHuman(t, []string{"some:other"})
		c := h.exchangerClient(t, contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment})
		status, _, body := exExchangeCode(t, h, c, tok, uniqTask("nosecret"), nil)
		if status != http.StatusForbidden {
			t.Fatalf("status: want 403 got %d body=%s", status, body)
		}
		for _, forbidden := range []string{tok, "postgres://", "postgresql://", "://", "-----BEGIN", "password"} {
			if strings.Contains(string(body), forbidden) {
				t.Fatalf("refusal body leaks %q: %s", forbidden, body)
			}
		}
		// The WARN journal must not carry the raw human token either.
		for _, l := range clog.snapshot() {
			if strings.Contains(l, tok) {
				t.Fatalf("WARN journal leaked the human token: %s", l)
			}
		}
	})

	// Regression: a previously successful exchange still returns 200 and a token.
	t.Run("successful_exchange_still_200", func(t *testing.T) {
		tenant, _, tok := h.seedHuman(t, []string{"space:access", "space:write"})
		c := h.exchangerClient(t, contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment})
		status, code, body := exExchangeCode(t, h, c, tok, uniqTask("ok"), nil)
		if status != http.StatusOK {
			t.Fatalf("regression: success must stay 200, got %d code=%q body=%s", status, code, body)
		}
		var v struct {
			Token string `json:"token"`
			Task  string `json:"task"`
		}
		if err := json.Unmarshal(body, &v); err != nil || v.Token == "" {
			t.Fatalf("regression: missing token: %v %s", err, body)
		}
	})
}

// exchangePostResponse is a body-returning variant of exchangePost for the
// DB-free wire tests.
func exchangePostResponse(t *testing.T, base string, c *http.Client, body map[string]any) response {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(testContext(t), http.MethodPost, base+"/v1/identity/control/exchange", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer res.Body.Close()
	b := make([]byte, 0, 256)
	buf := make([]byte, 256)
	for {
		n, err := res.Body.Read(buf)
		b = append(b, buf[:n]...)
		if err != nil {
			break
		}
	}
	return response{status: res.StatusCode, body: b}
}

// uniqTask returns a per-run unique task (and therefore context, since the
// handler derives context from the task when the body omits it). The identity
// contexts table has a global primary key on the context id, so a retained
// test database would otherwise make a fixed task id collide with a prior run's
// scope and surface as NOT_FOUND — exactly the non-repeatable flakiness this
// block must remove. Unique ids keep every run hermetic and repeatable.
func uniqTask(prefix string) string {
	return "chat-" + prefix + "-" + sanitizeID(contracts.NewID())
}

// TestExchangeChildrenQuotaRealIdentity proves, on a REAL Identity (not the
// classifier unit test), that when the per-authority fan-out ceiling is
// exhausted the exchange refusal carries the stable children_quota code. The
// exchange mints one distinct attenuated child per distinct requested
// capability set under the same authority; the authority root's default fan-out
// is DefaultMaxFanout (32), so the 33rd distinct narrowed exchange is refused by
// the Store's fan-out guard and surfaced by the handler as children_quota. This
// is the real-Identity counterpart of the classifier test for a tree/quota code.
func TestExchangeChildrenQuotaRealIdentity(t *testing.T) {
	clog := &captureLogger{}
	h := newExchangeHarnessLogger(t, []string{exchangerDeployment}, clog)

	// The human holds the exchange entitlement plus enough distinct extras to
	// form more than DefaultMaxFanout distinct narrowed effective sets.
	perms := []string{"space:access"}
	const extras = 40
	for i := 0; i < extras; i++ {
		perms = append(perms, "cap:"+sanitizeID(contracts.NewID()))
	}
	tenant, _, tok := h.seedHuman(t, perms)
	c := h.exchangerClient(t, contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment})
	authority := uniqTask("fanout")

	const fanout = 32 // DefaultMaxFanout of the authority root.
	var lastCode string
	for i := 0; i <= fanout; i++ {
		// Each request yields a distinct two-element effective set (always
		// narrower than the root ceiling), so each mints a distinct child.
		caps := []string{"space:access", perms[1+i]}
		status, code, body := exExchangeCode(t, h, c, tok, authority, caps)
		if i < fanout {
			if status != http.StatusOK {
				t.Fatalf("child %d must mint (200), got %d code=%q body=%s", i, status, code, body)
			}
			continue
		}
		// The child that would exceed the fan-out ceiling is refused.
		if status != http.StatusForbidden {
			t.Fatalf("fan-out must be denied at child %d, got %d body=%s", i, status, body)
		}
		lastCode = code
	}
	if lastCode != identityapi.ReasonChildrenQuota {
		t.Fatalf("want code %q when fan-out exhausted, got %q", identityapi.ReasonChildrenQuota, lastCode)
	}
	if !clog.contains(identityapi.ReasonChildrenQuota) {
		t.Fatalf("WARN journal missing children_quota: %v", clog.snapshot())
	}
}
