package registry_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/auth"
)

// These prove the two things that make the supply chain real in the product
// rather than only in the library: the composed Store (WithEvalGate +
// WithSupplyChain -- exactly what cmd/ahp tryMountRegistry builds) refuses an
// unsigned runtime, and the RecordSecurityVerdict HTTP face is reachable only by
// a human security authority, never by a workload/task token.

// (g) The mounted composition -- the same Store options the front uses -- refuses
// to register a runtime whose "attestation" is a nonempty but unsigned document:
// the nonempty-string posture this work replaces is rejected at registration.
// The control (a correctly signed binding for the same image) is accepted, so
// the refusal is the signature, not an unrelated precondition.
func TestMountedCompositionRejectsUnsignedRuntime(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t) // WithEvalGate(evals) + WithSupplyChain(operator roots)

	im := e.publishImage(t, "unsigned")
	im.secID = e.recordSecurity(t, im, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))

	unsigned := registry.RuntimeBinding{
		Tenant: e.tenant, Deployment: "rt-" + im.version.ID, Environment: "dev", AgentID: im.agent,
		Digest: im.version.Digest, Image: im.imageRef, SBOM: im.sbom,
		// A well-formed-looking DSSE envelope carrying no verifiable signature:
		// the payload decodes but the single signature has no signing certificate,
		// exactly the empty-attestation defect dressed as a document.
		Attestation: `{"payloadType":"application/vnd.in-toto+json","payload":"bm90LWEtc2lnbmF0dXJl","signatures":[{"keyid":"0000000000000000000000000000000000000000000000000000000000000000","sig":"AAAA"}]}`,
		Endpoint:    "https://runtime.invalid/a2a", ReadyURL: "https://runtime.invalid/readyz",
	}
	if err := e.store.RegisterRuntime(ctx, unsigned); err == nil {
		t.Fatal("mounted composition accepted an unsigned runtime binding")
	} else {
		t.Logf("unsigned runtime rejected as required: %v", err)
	}

	// Control: the very same image with a signature chaining to the operator
	// root is accepted by the same composition.
	if err := e.store.RegisterRuntime(ctx, e.bindingFor(t, im, e.auth)); err != nil {
		t.Fatalf("signed runtime should be accepted by the composition: %v", err)
	}
}

// (h) RecordSecurityVerdict is a human-only administrative write. It fails
// closed when no security authority is attached; a workload/task token is
// refused structurally even when authorize would allow it, so the artifact
// publisher's own runtime can never self-issue the clearance its image is gated
// on; a human security authority records the verdict (and it persists).
func TestRecordSecurityVerdictRouteIsHumanOnly(t *testing.T) {
	e := newSupplyEnv(t)

	_, digestHex, _ := makeImage(t, "secroute")
	oci := "sha256:" + digestHex
	expires := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	body := `{"oci_digest":"` + oci + `","agent_id":"sec-agent","outcome":"pass","source":"live-authz","expires_at":"` + expires + `"}`

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519: %v", err)
	}
	signer, err := auth.NewSigner("ahp", "test", priv)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"test": pub})
	// Deliberately permissive: the human-only guarantee must hold structurally,
	// independent of an authorize callback that a mis-wiring could loosen.
	authorize := func(context.Context, auth.Claims, string) error { return nil }

	// Fail closed: a handler that was never given a security authority.
	unwired, err := registry.NewHTTPHandler(e.store, verifier, authorize)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if code, _ := postVerdict(unwired, e.tenant, body, ""); code != http.StatusForbidden {
		t.Fatalf("unwired security authority: status=%d want 403 (fail closed)", code)
	}

	handler, err := registry.NewHTTPHandler(e.store, verifier, authorize)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if err := registry.AttachSecurityAuthority(handler, verifier, "ahp-browser", authorize); err != nil {
		t.Fatalf("attach security authority: %v", err)
	}

	// A task token -- a valid workload credential for a permitted scope, even
	// against a permissive authorize -- is refused: it is not a human.
	taskTok, err := signer.Sign(auth.Claims{
		Audience: "ahp", Kind: auth.KindTask, Tenant: e.tenant, Deployment: "secadmin",
		Subject: "operator-1", Task: "task-sec", Root: "root-sec", Context: "ctx-sec", Grant: "grant-sec", Depth: 0,
	}, time.Minute)
	if err != nil {
		t.Fatalf("sign task token: %v", err)
	}
	if code, _ := postVerdict(handler, e.tenant, body, taskTok); code != http.StatusForbidden {
		t.Fatalf("task token on security-verdict route: status=%d want 403", code)
	}
	if n := countVerdicts(t, e, oci); n != 0 {
		t.Fatalf("task token persisted %d security verdicts; the write must not happen", n)
	}

	// A human security authority records the clearance.
	humanTok, err := signer.Sign(auth.Claims{
		Audience: "ahp-browser", Kind: auth.KindHuman, Tenant: e.tenant, Subject: "sec-admin-1",
	}, time.Minute)
	if err != nil {
		t.Fatalf("sign human token: %v", err)
	}
	if code, out := postVerdict(handler, e.tenant, body, humanTok); code != http.StatusOK {
		t.Fatalf("human token on security-verdict route: status=%d want 200 body=%v", code, out)
	}
	if n := countVerdicts(t, e, oci); n != 1 {
		t.Fatalf("security verdicts for %s = %d, want 1 after a human write", oci, n)
	}
}

// postVerdict POSTs a RecordSecurityVerdict body over a verified mTLS workload
// certificate, presenting the given bearer token, and returns status + body.
func postVerdict(h http.Handler, tenant, body, token string) (int, string) {
	uri, _ := url.Parse("spiffe://ahp/tenant/" + tenant + "/deployment/secadmin")
	r := httptest.NewRequest(http.MethodPost, "/v1/registry/RecordSecurityVerdict", strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{uri}}}}}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Code, w.Body.String()
}

func countVerdicts(t *testing.T, e *supplyEnv, oci string) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_registry.security_verdicts WHERE oci_digest = $1`, oci).Scan(&n); err != nil {
		t.Fatalf("count verdicts: %v", err)
	}
	return n
}
