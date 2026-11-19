package registry_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Positive publish end-to-end on the isolated stand: a real RunEval (operator
// AHP_EVAL_SUITES_FILE loaded the same strict way cmd/ahp does) runs against a
// controlled A2A target and records a fresh passing verdict; the SAME runtime
// image then publishes through the registry's eval gate (409 without it, 200 with
// it). A/B/A is then pinned on the publish gate, and the still-reject cases are
// re-proved: a verdict that is absent, for a foreign/other digest, or superseded
// by a later failure never lets a version publish.
//
// Proof level (marked honestly): the A2A target is a loopback FIXTURE double that
// speaks the real A2A SendMessage/readyz wire -- not a production container -- and
// the eval target credential is a fixture issuer. Everything else (evals runner,
// verdict plane, registry store, both HTTP faces, the publish gate) is the
// accepted production code on real PostgreSQL.

// fixtureEvalIssuer stands in for cmd/ahp's live runtimeAuthority. The double
// requires a bearer, so the issuance path is exercised end to end; only the token
// VALUE is fixture, not a platform task grant.
type fixtureEvalIssuer struct{}

func (fixtureEvalIssuer) IssueEvalToken(context.Context, evals.RunRequest, evals.Target) (string, error) {
	return "fixture-eval-target-token", nil
}

// a2aDouble is the controlled target: /readyz proves the immutable identity the
// suite pinned; SendMessage completes only with a bearer and returns text that
// contains the required marker, so a pass is earned, not assumed.
func a2aDouble(t *testing.T, agent, digest, image, answer string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "agent_id": agent, "version_digest": digest, "image": image})
			return
		}
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": "1", "result": map[string]any{
			"id":        "task-1",
			"status":    map[string]any{"state": "completed", "message": map[string]any{"parts": []any{map[string]any{"kind": "text", "text": answer}}}},
			"artifacts": []any{map[string]any{"parts": []any{map[string]any{"kind": "text", "text": answer}}}},
		}})
	}))
}

// e2eHarness composes the accepted planes over the isolated stand: the registry
// store (WithEvalGate + WithSupplyChain, the mounted options), the shared evals
// plane, and both HTTP faces behind the SAME verifier/authorize contract.
type e2eHarness struct {
	store      *registry.Store
	evalStore  *evals.Store
	registryH  http.Handler
	evalsH     http.Handler
	signer     *auth.Signer
	verifier   *auth.Verifier
	tenant     string
	subject    string
	deployment string
}

func newE2EHarness(t *testing.T) *e2eHarness {
	t.Helper()
	e := newSupplyEnv(t) // registry Store: WithEvalGate(evalsHarness) + WithSupplyChain(roots)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519: %v", err)
	}
	signer, err := auth.NewSigner("ahp", "e2e", priv)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"e2e": pub})
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	registryH, err := registry.NewHTTPHandler(e.store, verifier, authorize)
	if err != nil {
		t.Fatalf("registry handler: %v", err)
	}
	return &e2eHarness{store: e.store, evalStore: evalsHarness(t), registryH: registryH,
		signer: signer, verifier: verifier, tenant: e.tenant,
		subject: "e2e-pub-" + randSuffix(t), deployment: "e2e-rt"}
}

// attachRunner mounts the evals HTTP face with an operator suite file, mirroring
// cmd/ahp: readOperatorJSON(AHP_EVAL_SUITES_FILE) -> NewRunner -> AttachRunner.
func (h *e2eHarness) attachRunner(t *testing.T, suites ...evals.Suite) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "suites.json")
	raw, err := json.Marshal(suites)
	if err != nil {
		t.Fatalf("marshal suites: %v", err)
	}
	if err := os.WriteFile(file, raw, 0o600); err != nil {
		t.Fatalf("write suites: %v", err)
	}
	var loaded []evals.Suite
	f, err := os.Open(file)
	if err != nil {
		t.Fatalf("open suites: %v", err)
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields() // exactly the operator loader's strictness
	if err := d.Decode(&loaded); err != nil {
		t.Fatalf("decode operator suites: %v", err)
	}
	// Operator mount order (cmd/ahp): policies are declared before a runner can
	// record verdicts under them. One policy per suite, covering the suite's
	// cases, fixture targets allowed (the controlled double is local by design).
	for _, su := range loaded {
		req := make([]string, 0, len(su.Cases))
		for _, c := range su.Cases {
			req = append(req, c.ID)
		}
		if _, err := h.evalStore.SetPolicy(context.Background(), evals.Policy{
			ID: su.PolicyID, TaskSetID: su.ID, RequiredTaskIDs: req, MaxAgeSeconds: 3600,
		}); err != nil {
			t.Fatalf("declare suite policy %s: %v", su.PolicyID, err)
		}
	}
	runner, err := evals.NewRunner(h.evalStore, loaded, nil)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	runner.SetTokenIssuer(fixtureEvalIssuer{})
	eh, err := evals.NewHTTPHandler(h.evalStore, h.verifier, func(context.Context, auth.Claims, string) error { return nil })
	if err != nil {
		t.Fatalf("evals handler: %v", err)
	}
	if err := evals.AttachRunner(eh, runner); err != nil {
		t.Fatalf("attach runner: %v", err)
	}
	h.evalsH = eh
}

// taskPost drives one route through the real transport.Task guard: a verified mTLS
// workload certificate plus a KindTask bearer bound to that exact workload.
func taskPost(t *testing.T, h http.Handler, path string, body any, h2 *e2eHarness) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	tok, err := h2.signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: h2.tenant, Deployment: h2.deployment,
		Subject: h2.subject, Task: "task-" + randSuffix(t), Root: "run-" + randSuffix(t), Context: "ctx-" + randSuffix(t), Grant: "grant-registry", Depth: 0}, time.Minute)
	if err != nil {
		t.Fatalf("sign task token: %v", err)
	}
	uri, _ := url.Parse("spiffe://ahp/tenant/" + h2.tenant + "/deployment/" + h2.deployment)
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+tok)
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{uri}}}}}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	out := map[string]any{}
	if b := w.Body.Bytes(); len(b) > 0 {
		_ = json.Unmarshal(b, &out)
	}
	return w.Code, out
}

// manifestForTarget builds a registry manifest whose single A2A component is
// pinned to the image, so its canonical digest is the exact agent_digest the
// evals suite -- and therefore the publish gate -- binds.
func manifestForTarget(t *testing.T, imageRef, system string) (registry.Manifest, string, []byte) {
	t.Helper()
	m := manifest(t, system)
	m.Components = []registry.Component{{Name: "router", A2A: true, MinReplicas: 1, MaxReplicas: 3, Image: imageRef}}
	digest, err := contracts.Digest(m)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	// The suite binds the digest computed over the raw manifest bytes; it must
	// equal the digest the registry gate will compute over the struct.
	rawDigest, err := contracts.Digest(json.RawMessage(raw))
	if err != nil || rawDigest != digest {
		t.Fatalf("manifest digest drift: struct=%s raw=%s err=%v", digest, rawDigest, err)
	}
	return m, digest, raw
}

func TestPositivePublishE2E(t *testing.T) {
	ctx := context.Background()
	h := newE2EHarness(t)

	agent := "e2e-" + randSuffix(t)
	imageRef, _ := signImageForTest(t, "pub")
	m, digest, rawManifest := manifestForTarget(t, imageRef, "supply.e2e")
	answer := "AHPEVAL-OK::" + randSuffix(t)
	srv := a2aDouble(t, agent, digest, imageRef, answer)
	defer srv.Close()

	suite := evals.Suite{
		ID: "e2e-set-" + randSuffix(t), Tenant: h.tenant,
		Target: evals.Target{Manifest: rawManifest, Deployment: h.deployment, AgentID: agent, AgentDigest: digest, Image: imageRef,
			Endpoint: srv.URL + "/a2a", ReadyURL: srv.URL + "/readyz"},
		Cases:          []evals.Case{{ID: "c1", Input: json.RawMessage(`"ping"`), RequiredText: "AHPEVAL-OK"}},
		TimeoutSeconds: 30, PolicyID: "e2e-policy-" + randSuffix(t),
	}
	h.attachRunner(t, suite)

	// Register the agent (publisher owns it).
	if code, out := taskPost(t, h.registryH, "/v1/registry/RegisterAgent", map[string]any{"agent_id": agent}, h); code != http.StatusOK {
		t.Fatalf("RegisterAgent: code=%d body=%v", code, out)
	}

	// (A) Before any evaluation, publish is refused by the gate.
	if code, _ := taskPost(t, h.registryH, "/v1/registry/PutVersion",
		map[string]any{"agent_id": agent, "digest": digest, "manifest": m}, h); code != http.StatusConflict {
		t.Fatalf("publish with no verdict: code=%d want 409", code)
	}

	// RunEval produces the fresh passing verdict against the controlled target.
	if code, out := taskPost(t, h.evalsH, "/v1/evals/RunEval", map[string]any{"suite_id": suite.ID, "agent_digest": digest}, h); code != http.StatusOK {
		t.Fatalf("RunEval: code=%d body=%v", code, out)
	} else if v, _ := out["verdict"].(map[string]any); v["outcome"] != "pass" {
		t.Fatalf("RunEval verdict not pass: %v", out)
	}

	// (B) The same image now publishes through the eval gate.
	if code, out := taskPost(t, h.registryH, "/v1/registry/PutVersion",
		map[string]any{"agent_id": agent, "digest": digest, "manifest": m}, h); code != http.StatusOK {
		t.Fatalf("publish after RunEval: code=%d want 200 body=%v", code, out)
	}

	// The Verdict face reports it as the passing gate fact.
	if code, out := taskPost(t, h.evalsH, "/v1/evals/Verdict", map[string]any{"agent_digest": digest}, h); code != http.StatusOK {
		t.Fatalf("Verdict: code=%d body=%v", code, out)
	} else if v, _ := out["verdict"].(map[string]any); v["passing"] != true {
		t.Fatalf("Verdict.passing false: %v", out)
	}

	// (A') Revocation: a later FAIL supersedes the pass -> publish is refused again.
	if _, err := h.evalStore.Record(ctx, evals.RecordInput{
		AgentDigest: digest, TaskSetID: "e2e-revoke", Setup: map[string]any{"why": "revoked"},
		Outcome: evals.OutcomeFail, Tasks: []evals.TaskResult{{TaskID: "c1", Passed: false, Cost: 0}},
		PolicyID: suite.PolicyID, TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("record revocation verdict: %v", err)
	}
	if code, _ := taskPost(t, h.registryH, "/v1/registry/PutVersion",
		map[string]any{"agent_id": agent, "digest": digest, "manifest": m}, h); code != http.StatusConflict {
		t.Fatalf("publish after revocation: code=%d want 409", code)
	}

	// (Neg) Foreign/other digest: a pass for one digest never clears another. A
	// never-evaluated digest fails though another digest passed for the same agent.
	otherImg, _ := signImageForTest(t, "other")
	_, otherDigest, _ := manifestForTarget(t, otherImg, "supply.e2e.other")
	if _, err := h.evalStore.Record(ctx, evals.RecordInput{
		AgentDigest: otherDigest, TaskSetID: suite.ID, Setup: map[string]any{"v": "1"},
		Outcome: evals.OutcomePass, Tasks: []evals.TaskResult{{TaskID: "c1", Passed: true, TerminalState: evals.TerminalCompleted, Cost: 0}},
		PolicyID: suite.PolicyID, TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("record other pass: %v", err)
	}
	thirdImg, _ := signImageForTest(t, "third")
	mThird, thirdDigest, _ := manifestForTarget(t, thirdImg, "supply.e2e.third")
	if code, _ := taskPost(t, h.registryH, "/v1/registry/PutVersion",
		map[string]any{"agent_id": agent, "digest": thirdDigest, "manifest": mThird}, h); code != http.StatusConflict {
		t.Fatalf("publish for never-evaluated digest: code=%d want 409", code)
	}
}

// signImageForTest returns an immutable image reference and its content digest.
func signImageForTest(t *testing.T, tag string) (string, string) {
	t.Helper()
	ref, digestHex, _ := makeImage(t, tag+"-e2e")
	return ref, digestHex
}

// TestPositiveStageDeployWithFreshVerdicts closes the ИБ half of the positive
// path on the isolated stand: with a fresh passing evaluation verdict and a fresh
// (SYNTHETIC, marked) security clearance the same immutable runtime image admits
// and installs into stage; once that clearance is superseded by a failing one the
// stage deploy is refused again. Synthetic here: the security clearance is a
// test-signed verdict (RecordSecurityVerdict), standing in for the ИБ service;
// the admission/digest binding and the deploy gate are production code on PG.
func TestPositiveStageDeployWithFreshVerdicts(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t) // WithEvalGate + WithSupplyChain

	im := e.admittedImage(t, "stagedeploy") // eval pass + synthetic pass clearance + signed admission
	if inst := e.install(t, im, "stage"); inst.Environment != "stage" {
		t.Fatalf("stage install of a freshly cleared image failed: %+v", inst)
	}

	// A superseded (failing) clearance blocks a NEW stage install for the image.
	e.recordSecurity(t, im, "fail", time.Now().UTC().Add(time.Second), time.Now().UTC().Add(24*time.Hour))
	if _, err := e.store.Install(ctx, e.tenant, im.agent, im.version.ID, "stage", "inst-"+randSuffix(t), im.version.Manifest.Capabilities); err == nil {
		t.Fatal("stage install succeeded after the security clearance was revoked")
	}
}
