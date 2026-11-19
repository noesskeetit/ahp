package identity_test

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/identityapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DB-free wire-contract test for POST /v1/identity/control/exchange.
//
// It uses a nil Store on purpose: every assertion here is reached BEFORE any
// database call, so it proves the wire contract without PostgreSQL. The
// success path (resp {token,task}) and the full authz chain are covered by the
// PG+mTLS joint test TestExchangeHumanToScopedTask, which integration
// validation runs on the composed checkout.
//
// Request contract: { human_token, task, context?, capabilities? }; subject,
// tenant, deployment and every other field are rejected (strict DTO). Guarded
// by transport.Workload() (mTLS) and a fail-closed exchanger allowlist.
func newWireServer(t *testing.T, exchangers []string) (string, *testCA) {
	t.Helper()
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	cfg := identityapi.Config{
		Store:                nil, // pre-DB paths only
		Verifier:             verifier,
		Signer:               signer,
		Audience:             testAudience,
		Browser:              &identityapi.BrowserConfig{},
		ExchangerDeployments: exchangers,
	}
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
	return srv.URL, ca
}

func (ca *testCA) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(ca.cert)
	return p
}

func exchangePost(t *testing.T, base string, c *http.Client, body map[string]any) int {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(testContext(t), http.MethodPost,
		base+"/v1/identity/control/exchange", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer res.Body.Close()
	_, _ = io.ReadAll(res.Body)
	return res.StatusCode
}

func TestExchangeWireContractNoDB(t *testing.T) {
	base, ca := newWireServer(t, []string{"as-space"})
	plain := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca.pool()}}}
	allow := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs:      ca.pool(),
		Certificates: []tls.Certificate{ca.clientCert(t, auth.WorkloadID(contracts.Scope{Tenant: "acme", Deployment: "as-space"}))},
	}}}
	notAllowed := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs:      ca.pool(),
		Certificates: []tls.Certificate{ca.clientCert(t, auth.WorkloadID(contracts.Scope{Tenant: "acme", Deployment: "mallory"}))},
	}}}

	t.Run("no_client_cert_denied", func(t *testing.T) {
		if got := exchangePost(t, base, plain, map[string]any{"human_token": "x", "task": "t"}); got != http.StatusForbidden {
			t.Fatalf("certless: want 403 got %d", got)
		}
	})
	t.Run("allowlisted_missing_task_400", func(t *testing.T) {
		if got := exchangePost(t, base, allow, map[string]any{"human_token": "x"}); got != http.StatusBadRequest {
			t.Fatalf("missing task: want 400 got %d", got)
		}
	})
	t.Run("allowlisted_empty_body_400", func(t *testing.T) {
		if got := exchangePost(t, base, allow, map[string]any{}); got != http.StatusBadRequest {
			t.Fatalf("empty body: want 400 got %d", got)
		}
	})
	t.Run("injected_subject_rejected_400", func(t *testing.T) {
		// subject/tenant/deployment are never accepted in the body.
		if got := exchangePost(t, base, allow, map[string]any{"human_token": "x", "task": "t", "subject": "evil"}); got != http.StatusBadRequest {
			t.Fatalf("injected subject: want 400 got %d", got)
		}
	})
	t.Run("unverifiable_human_token_403", func(t *testing.T) {
		// Reached after DTO parse, before any DB use: proves req field parsing.
		if got := exchangePost(t, base, allow, map[string]any{"human_token": "not.a.jwt", "task": "t"}); got != http.StatusForbidden {
			t.Fatalf("bad token: want 403 got %d", got)
		}
	})
	t.Run("non_allowlisted_deployment_403", func(t *testing.T) {
		if got := exchangePost(t, base, notAllowed, map[string]any{"human_token": "x", "task": "t"}); got != http.StatusForbidden {
			t.Fatalf("non-allowlisted: want 403 got %d", got)
		}
	})
}

// controlPost posts to a control-plane route and returns the status.
func controlPost(t *testing.T, base string, c *http.Client, path string, body map[string]any) int {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(testContext(t), http.MethodPost, base+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.Do(req)
	if err != nil {
		t.Fatalf("post: %v", path)
	}
	defer res.Body.Close()
	_, _ = io.ReadAll(res.Body)
	return res.StatusCode
}

// DB-free guards for the cancel/revoke authority boundary. Every assertion is
// reached BEFORE any Store call (the control allowlist and the certificate
// check precede any grant read), so it proves the boundary without PostgreSQL.
// The positive lineage cancel, foreign-subject denial and sibling isolation are
// covered by the real-PG test TestCancelSubtreeAuthorizationBoundary.
func TestCancelRevokeWireGuardsNoDB(t *testing.T) {
	base, ca := newWireServer(t, []string{"as-space"})
	plain := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca.pool()}}}
	// An ordinary workload: a valid AHP cert for a deployment that is NOT on the
	// (empty) ControlPlaneDeployments allowlist.
	generic := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs:      ca.pool(),
		Certificates: []tls.Certificate{ca.clientCert(t, auth.WorkloadID(contracts.Scope{Tenant: "acme", Deployment: "as-space"}))},
	}}}

	const cancelPath = "/v1/identity/control/cancel-subtree"
	const revokePath = "/v1/identity/control/revoke-subject"
	body := map[string]any{"tenant": "acme", "deployment": "as-space", "subject": "s", "task": "t"}

	t.Run("cancel_operator_denies_generic_workload", func(t *testing.T) {
		if got := controlPost(t, base, generic, cancelPath, body); got != http.StatusForbidden {
			t.Fatalf("generic workload on operator cancel: want 403 got %d", got)
		}
	})
	t.Run("cancel_operator_denies_without_cert", func(t *testing.T) {
		if got := controlPost(t, base, plain, cancelPath, body); got != http.StatusForbidden {
			t.Fatalf("certless operator cancel: want 403 got %d", got)
		}
	})
	t.Run("revoke_denies_generic_workload", func(t *testing.T) {
		if got := controlPost(t, base, generic, revokePath, map[string]any{"tenant": "acme", "subject": "s"}); got != http.StatusForbidden {
			t.Fatalf("generic workload on revoke: want 403 got %d", got)
		}
	})
	t.Run("lineage_cancel_requires_task_token", func(t *testing.T) {
		// transport.Task demands a verified certificate AND a bearer task token;
		// with a certificate but no token it is refused before any grant read.
		if got := controlPost(t, base, generic, "/v1/identity/roots/t/cancel", nil); got != http.StatusForbidden {
			t.Fatalf("lineage cancel without a task token: want 403 got %d", got)
		}
	})
	t.Run("lineage_cancel_denies_without_cert", func(t *testing.T) {
		if got := controlPost(t, base, plain, "/v1/identity/roots/t/cancel", nil); got != http.StatusForbidden {
			t.Fatalf("lineage cancel without cert: want 403 got %d", got)
		}
	})
}
