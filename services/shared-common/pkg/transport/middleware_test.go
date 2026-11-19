package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	testAudience = "ahp-worker"
	testURI      = "spiffe://ahp/tenant/t1/deployment/d1"
)

func allowAll(context.Context, auth.Claims, string) error { return nil }

// newMTLSServer runs handler behind a real mTLS server that requires and
// verifies client certificates against the fresh test CA.
func newMTLSServer(t *testing.T, ca *testCA, client tls.Certificate, handler http.Handler) *httptest.Server {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca.pem) {
		t.Fatal("test CA PEM rejected")
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	srv.StartTLS()
	srv.Client().Transport.(*http.Transport).TLSClientConfig = &tls.Config{
		Certificates:       []tls.Certificate{client},
		InsecureSkipVerify: true, // local transport evidence only
	}
	t.Cleanup(srv.Close)
	return srv
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := FromContext(r.Context())
		if !ok {
			WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "no principal"))
			return
		}
		WriteJSON(w, http.StatusOK, p.Scope)
	})
}

func do(t *testing.T, srv *httptest.Server, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/run", nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestTaskSuccessOverRealMTLS(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		Task(verifier, testAudience, "cap:run", allowAll)(okHandler()))
	resp := do(t, srv, signTask(t, signer, testAudience, "t1", "d1", "task-1"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestTaskRejectsUnsignedTLS(t *testing.T) {
	_, verifier := newSignerVerifier(t)
	// Plain HTTP: TLS state has no VerifiedChains at all.
	srv := httptest.NewServer(Task(verifier, testAudience, "cap:run", allowAll)(okHandler()))
	t.Cleanup(srv.Close)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/run", nil)
	req.Header.Set("Authorization", "Bearer whatever")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestTaskRejectsSpoofedWorkloadHeaders(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		Task(verifier, testAudience, "cap:run", allowAll)(okHandler()))
	// Token bound to the attacker tenant; headers claim it is legitimate.
	tok := signTask(t, signer, testAudience, "evil", "d1", "task-1")
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/run", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("X-Workload-Identity", testURI)
	req.Header.Set("X-Tenant-Id", "evil")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestTaskRejectsWrongScopeCertificate(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, "spiffe://ahp/tenant/t2/deployment/d2"),
		Task(verifier, testAudience, "cap:run", allowAll)(okHandler()))
	resp := do(t, srv, signTask(t, signer, testAudience, "t1", "d1", "task-1"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestTaskRejectsHumanToken(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		Task(verifier, testAudience, "cap:run", allowAll)(okHandler()))
	resp := do(t, srv, signHuman(t, signer, testAudience, "t1", "alice"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestTaskRequiresBearer(t *testing.T) {
	ca := newCA(t)
	_, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		Task(verifier, testAudience, "cap:run", allowAll)(okHandler()))
	for _, hdr := range []string{"", "Basic dXNlcjpwdw==", "Bearer ", "Token abc"} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/run", nil)
		if hdr != "" {
			req.Header.Set("Authorization", hdr)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("header %q: status = %d, want 403", hdr, resp.StatusCode)
		}
	}
}

func TestTaskRevokedAuthorize(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	deny := func(context.Context, auth.Claims, string) error {
		return contracts.NewError(contracts.ErrForbidden, "grant revoked")
	}
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		Task(verifier, testAudience, "cap:run", deny)(okHandler()))
	resp := do(t, srv, signTask(t, signer, testAudience, "t1", "d1", "task-1"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestTaskNilAuthorizeFailsClosed(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		Task(verifier, testAudience, "cap:run", nil)(okHandler()))
	resp := do(t, srv, signTask(t, signer, testAudience, "t1", "d1", "task-1"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestTaskRejectsAmbiguousIdentities(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca,
		ca.clientCert(t, testURI, "spiffe://ahp/tenant/t2/deployment/d2"),
		Task(verifier, testAudience, "cap:run", allowAll)(okHandler()))
	resp := do(t, srv, signTask(t, signer, testAudience, "t1", "d1", "task-1"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestMalformedURIVariants(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	bad := []string{
		"https://ahp/tenant/t1/deployment/d1",
		"spiffe://other/tenant/t1/deployment/d1",
		"spiffe://ahp/tenant/t1",
		"spiffe://ahp/tenant//deployment/d1",
		"spiffe://ahp/tenant/t1/deployment/",
		"spiffe://ahp/tenant/t1/deployment/d1/extra",
		"spiffe://ahp/tenant/t1/deployment/d%31",
		"spiffe://ahp/tenant/t1/deployment/d1?x=1",
		"spiffe://ahp/tenant/t1/deployment/d1#frag",
		"spiffe://ahp/tenant/t1/deployment/d1@evil",
		"spiffe://ahp/tenant/t1/deployment/d1:8443",
		"spiffe://ahp/tenant/t1/deployment/d1/",
		"spiffe://ahp/tenant/../deployment/d1",
		"spiffe://ahp/tenant/t1/deployment/..",
		"spiffe://ahp/tenant/t1/deployment/dé1",
		"spiffe://ahp/tenant/t@t1/deployment/d1",
		"spiffe://ahp:8080/tenant/t1/deployment/d1",
		"spiffe://user@ahp/tenant/t1/deployment/d1",
		"spiffe://ahp/tenant/t1/deploymentx/d1",
		"spiffe://ahp/tenants/t1/deployment/d1",
	}
	good := map[string][2]string{
		"spiffe://ahp/tenant/t-1.a_b/deployment/d-1.2_3": {"t-1.a_b", "d-1.2_3"},
		"spiffe://ahp/tenant/a/deployment/b":             {"a", "b"},
	}
	for uri, want := range good {
		tenant, deployment, err := parseWorkloadURI(uri)
		if err != nil {
			t.Errorf("uri %q rejected: %v", uri, err)
			continue
		}
		if tenant != want[0] || deployment != want[1] {
			t.Errorf("uri %q parsed as %q/%q, want %q/%q", uri, tenant, deployment, want[0], want[1])
		}
	}
	for _, uri := range bad {
		if tenant, deployment, err := parseWorkloadURI(uri); err == nil {
			t.Errorf("uri %q accepted as %q/%q", uri, tenant, deployment)
		}
	}
	srv := newMTLSServer(t, ca, ca.clientCert(t, "spiffe://ahp/tenant/t1/deployment/bad%2Fx"),
		Task(verifier, testAudience, "cap:run", allowAll)(okHandler()))
	resp := do(t, srv, signTask(t, signer, testAudience, "t1", "d1", "task-1"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("encoded-slash uri: status = %d, want 403", resp.StatusCode)
	}
}

func TestHumanAudienceSeparation(t *testing.T) {
	signer, verifier := newSignerVerifier(t)
	srv := httptest.NewServer(Human(verifier, "ahp-console")(okHandler()))
	t.Cleanup(srv.Close)

	post := func(tok string) int {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api", nil)
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if got := post(signHuman(t, signer, "ahp-console", "t1", "alice")); got != http.StatusOK {
		t.Fatalf("human token: status = %d, want 200", got)
	}
	if got := post(signHuman(t, signer, "other-audience", "t1", "alice")); got != http.StatusForbidden {
		t.Fatalf("wrong audience: status = %d, want 403", got)
	}
	if got := post(signTask(t, signer, "ahp-console", "t1", "d1", "task-1")); got != http.StatusForbidden {
		t.Fatalf("task token on human route: status = %d, want 403", got)
	}
	if got := post(""); got != http.StatusForbidden {
		t.Fatalf("missing token: status = %d, want 403", got)
	}
}

// TestTaskPrincipalKeepsOriginalSubject asserts the middleware principal scope
// carries the original human subject of the token, never the task id, so all
// tasks delegated from one subject share one isolation scope.
func TestTaskPrincipalKeepsOriginalSubject(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	const subject = "user-ada"

	var got Principal
	handler := Task(verifier, testAudience, "cap:run", allowAll)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got, _ = FromContext(r.Context())
			WriteJSON(w, http.StatusOK, got.Scope)
		}))
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI), handler)

	resp := do(t, srv, signTaskWithSubject(t, signer, testAudience, "t1", "d1", "task-1", subject))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got.Scope.Subject != subject {
		t.Fatalf("principal scope subject = %q, want original subject %q", got.Scope.Subject, subject)
	}
	if got.Claims.Task != "task-1" {
		t.Fatalf("claims task = %q, want task-1", got.Claims.Task)
	}
	if got.Claims.Subject != subject {
		t.Fatalf("claims subject = %q, want %q", got.Claims.Subject, subject)
	}
	if err := got.Scope.Validate(); err != nil {
		t.Fatalf("principal scope invalid: %v", err)
	}

	// A sibling task under the same subject yields the identical scope.
	resp = do(t, srv, signTaskWithSubject(t, signer, testAudience, "t1", "d1", "task-2", subject))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sibling task: status = %d, want 200", resp.StatusCode)
	}
	if got.Scope.Subject != subject {
		t.Fatalf("sibling task scope subject = %q, want %q", got.Scope.Subject, subject)
	}
}

func TestWorkloadDeploymentOnly(t *testing.T) {
	ca := newCA(t)
	var got Principal
	handler := Workload()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = FromContext(r.Context())
		WriteJSON(w, http.StatusOK, got.Scope)
	}))
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI), handler)

	// No JWT: certificate alone is the identity.
	resp, err := srv.Client().Post(srv.URL+"/claim", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got.Scope.Tenant != "t1" || got.Scope.Deployment != "d1" ||
		got.Scope.Subject != deploymentSubject || got.Claims.Subject != "" {
		t.Fatalf("principal = %+v", got)
	}
	if err := got.Scope.Validate(); err != nil {
		t.Fatalf("deployment scope invalid: %v", err)
	}

	// A bearer token is not consulted: still succeeds, still no claims.
	resp = do(t, srv, "not-a-real-token")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("garbage token: status = %d, want 200", resp.StatusCode)
	}
}
