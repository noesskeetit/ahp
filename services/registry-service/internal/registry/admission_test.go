package registry_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests run the CI supply chain and deploy admission end to end against the
// real PostgreSQL instance, exactly as the composed product wires it: the registry
// store carries both the evaluation gate (WithEvalGate) and the operator trust
// roots (WithSupplyChain). Every acceptance fact -- publisher identity, image
// digest, SBOM, evaluation and security verdicts -- is verified by the Registry;
// none is taken from a caller assertion.

type supplyEnv struct {
	store  *registry.Store
	tenant string
	auth   *testAuthority
	pool   *pgxpool.Pool
}

func newSupplyEnv(t *testing.T) *supplyEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	auth := newAuthority(t, "operator-root-"+randSuffix(t))
	roots, err := registry.NewTrustRootsFromPEM(auth.caPEM)
	if err != nil {
		t.Fatalf("trust roots: %v", err)
	}
	store := registry.New(pool, registry.WithEvalGate(evalsHarness(t)), registry.WithSupplyChain(roots))
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("supply migrate: %v", err)
	}
	return &supplyEnv{store: store, tenant: "t-" + randSuffix(t), auth: auth, pool: pool}
}

// imageEnv is one published, security-cleared, fully admitted image.
type imageEnv struct {
	agent      string
	version    registry.Version
	imageRef   string
	digestHex  string
	sbom       []byte
	sbomDigest string
	secID      string
}

func (e *supplyEnv) publishImage(t *testing.T, tag string) imageEnv {
	t.Helper()
	imageRef, digestHex, _ := makeImage(t, tag)
	m := manifest(t, "supply."+randSuffix(t))
	m.Components[0].Image = imageRef
	agent := "supply-" + randSuffix(t)
	v := publish(t, e.store, agent, m)
	sbom, sbomDigest := makeSBOM(t, imageRef)
	return imageEnv{agent: agent, version: v, imageRef: imageRef, digestHex: digestHex, sbom: sbom, sbomDigest: sbomDigest}
}

// recordSecurity records a security clearance for the image and returns its id.
func (e *supplyEnv) recordSecurity(t *testing.T, im imageEnv, outcome string, checked, expires time.Time) string {
	t.Helper()
	sv, err := e.store.RecordSecurityVerdict(context.Background(), registry.SecurityVerdict{
		OCIDigest: "sha256:" + im.digestHex, AgentID: im.agent, Outcome: outcome,
		Source: "sec-authority", CheckedAt: checked, ExpiresAt: expires,
	})
	if err != nil {
		t.Fatalf("record security verdict: %v", err)
	}
	return sv.ID
}

// signFor signs a well-formed attestation binding im, unless a caller wants to
// tamper by passing overrides through the spec.
func (e *supplyEnv) signFor(t *testing.T, im imageEnv, publisher string, expires time.Time) registry.Attestation {
	t.Helper()
	leaf, key, _ := e.auth.mintPublisher(t, publisher, time.Hour)
	return sign(t, leaf, key, attestationSpec{
		OCIDigestHex: im.digestHex, ImageRef: im.imageRef, VersionDigest: im.version.Digest,
		SBOMDigest: im.sbomDigest, EvalVerdictDgst: im.version.Digest,
		SecurityVerdict: im.secID, ExpiresAt: expires,
	})
}

// admit runs the happy-path admission and fails the test on refusal.
func (e *supplyEnv) admit(t *testing.T, im imageEnv) registry.Admission {
	t.Helper()
	att := e.signFor(t, im, testPublisher, time.Now().Add(time.Hour))
	adm, err := e.store.AdmitImage(context.Background(), registry.AdmitImageInput{
		AgentID: im.agent, VersionID: im.version.ID, Image: im.imageRef, Attestation: att, SBOM: im.sbom,
	})
	if err != nil {
		t.Fatalf("AdmitImage for a trusted publisher: %v", err)
	}
	return adm
}

// admittedImage publishes an image, records the stage clearances a real CI
// pipeline leaves behind (passing security + passing restart-chaos) and admits
// it under a signature that binds them. A caller that wants to vary one plane
// builds its fixture inline instead of mutating this one.
func (e *supplyEnv) admittedImage(t *testing.T, tag string) imageEnv {
	t.Helper()
	im := e.publishImage(t, tag)
	im.secID = e.recordSecurity(t, im, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	e.recordChaos(t, im, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	e.admit(t, im)
	return im
}

func (e *supplyEnv) install(t *testing.T, im imageEnv, env string) registry.Installation {
	t.Helper()
	inst, err := e.store.Install(context.Background(), e.tenant, im.agent, im.version.ID, env, "inst-"+randSuffix(t), im.version.Manifest.Capabilities)
	if err != nil {
		t.Fatalf("Install %s of an admitted image: %v", env, err)
	}
	return inst
}

// (a) A signature from a trusted publisher, bound to the correct digest and SBOM,
// is admitted; the image then installs into dev and stage (with a passing security
// clearance), and a runtime binding whose attestation verifies is registered.
func TestAdmitAndDeployVerifiedImage(t *testing.T) {
	e := newSupplyEnv(t)
	im := e.admittedImage(t, "devstage")

	// dev install (admission + evaluation gate, no security verdict required).
	e.install(t, im, "dev")

	// stage install additionally requires the passing, unexpired security verdict.
	e.install(t, im, "stage")

	// A runtime binding carries the same verified attestation and is accepted.
	b := e.bindingFor(t, im, e.auth)
	if err := e.store.RegisterRuntime(context.Background(), b); err != nil {
		t.Fatalf("RegisterRuntime with a verified attestation: %v", err)
	}
	// A binding with a forged (non-attested) proof is refused -- the defect.
	bogus := b
	bogus.Deployment = "bogus-" + randSuffix(t)
	bogus.Attestation = `{"payloadType":"application/vnd.in-toto+json","payload":"bm90LWEtc2lnbmF0dXJl","signatures":[]}`
	if err := e.store.RegisterRuntime(context.Background(), bogus); err == nil {
		t.Fatal("RegisterRuntime accepted a non-cryptographic attestation")
	} else if contracts.Code(err) != contracts.ErrConflict && contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("bogus attestation error = %v, want CONFLICT/INVALID", err)
	}
}

// (b) A trusted root alone is not enough: the signing identity must own the
// artifact, and the signature must bind the image actually being deployed.
func TestAdmissionRejectsWrongPublisherDigestAndRoot(t *testing.T) {
	e := newSupplyEnv(t)

	// wrong publisher: identity is attested but does not own the agent.
	im := e.publishImage(t, "pub")
	im.secID = e.recordSecurity(t, im, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	att := e.signFor(t, im, "pub-intruder", time.Now().Add(time.Hour))
	if _, err := e.store.AdmitImage(context.Background(), registry.AdmitImageInput{
		AgentID: im.agent, VersionID: im.version.ID, Image: im.imageRef, Attestation: att, SBOM: im.sbom,
	}); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("wrong publisher admission = %v, want FORBIDDEN", err)
	}

	// wrong digest: an attestation made for one image must not admit another.
	target := e.publishImage(t, "target")
	target.secID = e.recordSecurity(t, target, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	foreign := e.publishImage(t, "foreign") // different digest the attestation was made for
	foreign.secID = e.recordSecurity(t, foreign, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	mis := foreign
	mis.secID = target.secID // reference the target clearance but keep foreign's signed digest
	att2 := e.signFor(t, mis, testPublisher, time.Now().Add(time.Hour))
	if _, err := e.store.AdmitImage(context.Background(), registry.AdmitImageInput{
		AgentID: target.agent, VersionID: target.version.ID, Image: target.imageRef, Attestation: att2, SBOM: foreign.sbom,
	}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("wrong-digest admission = %v, want CONFLICT", err)
	}

	// untrusted root: a signature under a CA the operator never installed.
	rogue := newAuthority(t, "rogue-root")
	rogueLeaf, rogueKey, _ := rogue.mintPublisher(t, testPublisher, time.Hour)
	rogueAtt := sign(t, rogueLeaf, rogueKey, attestationSpec{
		OCIDigestHex: target.digestHex, ImageRef: target.imageRef, VersionDigest: target.version.Digest,
		SBOMDigest: target.sbomDigest, EvalVerdictDgst: target.version.Digest, SecurityVerdict: target.secID,
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if _, err := e.store.AdmitImage(context.Background(), registry.AdmitImageInput{
		AgentID: target.agent, VersionID: target.version.ID, Image: target.imageRef, Attestation: rogueAtt, SBOM: target.sbom,
	}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("untrusted-root admission = %v, want CONFLICT", err)
	}
}

// (c) A missing or mismatched SBOM is refused: the SBOM must hash to what the
// signature bound.
func TestAdmissionRejectsMissingOrMismatchedSBOM(t *testing.T) {
	e := newSupplyEnv(t)
	im := e.publishImage(t, "sbom")
	im.secID = e.recordSecurity(t, im, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	att := e.signFor(t, im, testPublisher, time.Now().Add(time.Hour))

	// missing SBOM
	if _, err := e.store.AdmitImage(context.Background(), registry.AdmitImageInput{
		AgentID: im.agent, VersionID: im.version.ID, Image: im.imageRef, Attestation: att, SBOM: nil,
	}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("missing SBOM admission = %v, want CONFLICT", err)
	}
	// mismatched SBOM
	tampered := append(append([]byte{}, im.sbom...), '\n')
	if _, err := e.store.AdmitImage(context.Background(), registry.AdmitImageInput{
		AgentID: im.agent, VersionID: im.version.ID, Image: im.imageRef, Attestation: att, SBOM: tampered,
	}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("mismatched SBOM admission = %v, want CONFLICT", err)
	}
	// matching SBOM admits
	if _, err := e.store.AdmitImage(context.Background(), registry.AdmitImageInput{
		AgentID: im.agent, VersionID: im.version.ID, Image: im.imageRef, Attestation: att, SBOM: im.sbom,
	}); err != nil {
		t.Fatalf("matching SBOM should admit: %v", err)
	}
}

// (d) A promotion onto an image whose evaluation verdict later fails, or whose
// security clearance fails or expires, is blocked. Admission itself already
// required a passing verdict; a verdict that goes stale afterwards must still stop
// the deploy ("устаревший eval/ИБ блокирует promotion").
func TestPromotionBlockedByFailedOrStaleVerdicts(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t)

	// Two admitted images of the SAME agent so one installation can move A -> B.
	a := e.publishImage(t, "sameA")
	a.secID = e.recordSecurity(t, a, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	e.admit(t, a)
	b := e.publishImage(t, "sameB")
	b.agent = a.agent
	b.secID = e.recordSecurity(t, b, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	// re-publish B under A's agent so promotion targets the same agent.
	bm := manifest(t, "supply.promote")
	bm.Components[0].Image = b.imageRef
	bv := publish(t, e.store, a.agent, bm)
	b.version = bv
	att := e.signFor(t, b, testPublisher, time.Now().Add(time.Hour))
	if _, err := e.store.AdmitImage(ctx, registry.AdmitImageInput{
		AgentID: a.agent, VersionID: bv.ID, Image: b.imageRef, Attestation: att, SBOM: b.sbom,
	}); err != nil {
		t.Fatalf("admit B: %v", err)
	}
	_ = att

	// install A into dev, then a failing evaluation for B lands.
	inst := e.install(t, a, "dev")
	if _, err := evalsHarness(t).Record(ctx, evalsRecordFail(b.version.Digest)); err != nil {
		t.Fatalf("record failing eval for B: %v", err)
	}
	if _, err := e.store.Promote(ctx, e.tenant, inst.ID, b.version.ID); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("promotion onto B with a failing eval = %v, want CONFLICT", err)
	}

	// Security verdict for stage: fresh passing admits; a later failing/expired one blocks.
	c := e.admittedImage(t, "secstage")
	if _, err := e.store.Install(ctx, e.tenant, c.agent, c.version.ID, "stage", "inst-"+randSuffix(t), c.version.Manifest.Capabilities); err != nil {
		t.Fatalf("stage install with passing security verdict: %v", err)
	}
	e.recordSecurity(t, c, "fail", time.Now().UTC(), time.Now().Add(24*time.Hour))
	if _, err := e.store.Install(ctx, e.tenant, c.agent, c.version.ID, "prod", "inst-"+randSuffix(t), c.version.Manifest.Capabilities); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("prod install with failing security verdict = %v, want CONFLICT", err)
	}
}

// (e) Promotion and rollback move the installation's version pointer, but a task's
// historical pin never moves: across A -> B -> A the task first pinned on A stays
// on A, and one pinned on B stays on B.
func TestHistoricalPinPreservedAcrossABA(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t)

	agent := "supply-" + randSuffix(t)
	ai, bi := e.imageRefPair(t)
	am := manifest(t, "supply.pina")
	am.Components[0].Image = ai.imageRef
	bm := manifest(t, "supply.pinb")
	bm.Components[0].Image = bi.imageRef
	va := publish(t, e.store, agent, am)
	vb := publish(t, e.store, agent, bm)
	for _, pair := range []struct {
		v   registry.Version
		img imageEnv
	}{{va, ai}, {vb, bi}} {
		im := pair.img
		im.agent = agent
		im.version = pair.v
		im.secID = e.recordSecurity(t, im, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
		att := e.signFor(t, im, testPublisher, time.Now().Add(time.Hour))
		if _, err := e.store.AdmitImage(ctx, registry.AdmitImageInput{
			AgentID: agent, VersionID: pair.v.ID, Image: im.imageRef, Attestation: att, SBOM: im.sbom,
		}); err != nil {
			t.Fatalf("admit %s: %v", im.imageRef, err)
		}
	}

	inst, err := e.store.Install(ctx, e.tenant, agent, va.ID, "dev", "inst-"+randSuffix(t), am.Capabilities)
	if err != nil {
		t.Fatalf("install A: %v", err)
	}
	taskA := "task-" + randSuffix(t)
	if _, err := e.store.Pin(ctx, e.tenant, inst.ID, taskA); err != nil {
		t.Fatalf("pin task on A: %v", err)
	}

	// Promote A -> B and pin a second task; then promote back to A.
	if _, err := e.store.Promote(ctx, e.tenant, inst.ID, vb.ID); err != nil {
		t.Fatalf("promote A->B: %v", err)
	}
	taskB := "task-" + randSuffix(t)
	if _, err := e.store.Pin(ctx, e.tenant, inst.ID, taskB); err != nil {
		t.Fatalf("pin task on B: %v", err)
	}
	if _, err := e.store.Promote(ctx, e.tenant, inst.ID, va.ID); err != nil {
		t.Fatalf("promote B->A: %v", err)
	}

	// The second task pinned while on B remains pinned to B.
	pinned, err := e.store.PinnedVersion(ctx, e.tenant, taskB)
	if err != nil {
		t.Fatalf("read pin for taskB: %v", err)
	}
	if pinned.ID != vb.ID {
		t.Fatalf("task pinned on B drifted to %s after A->B->A", pinned.ID)
	}
	pinnedA, err := e.store.PinnedVersion(ctx, e.tenant, taskA)
	if err != nil {
		t.Fatalf("read pin for taskA: %v", err)
	}
	if pinnedA.ID != va.ID {
		t.Fatalf("task pinned on A drifted to %s after A->B->A", pinnedA.ID)
	}
	history, err := e.store.ListDeployments(ctx, e.tenant, inst.ID, 10)
	if err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	if len(history) < 3 {
		t.Fatalf("expected an A->B->A rollout history, got %d entries", len(history))
	}
}

// (f) The publisher owns the artifact and its versions but reads none of a
// consuming tenant's deployment state: installations, rollout history and task
// pins are scoped to the consuming tenant and are NOT_FOUND to everyone else.
func TestPublisherCannotReadConsumerState(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t)
	im := e.admittedImage(t, "boundary")

	inst := e.install(t, im, "prod")
	task := "task-" + randSuffix(t)
	if _, err := e.store.Pin(ctx, e.tenant, inst.ID, task); err != nil {
		t.Fatalf("pin: %v", err)
	}

	publisherTenant := "pubtenant-" + randSuffix(t)
	if _, err := e.store.GetInstallation(ctx, publisherTenant, inst.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("publisher read consumer installation = %v, want NOT_FOUND", err)
	}
	if _, err := e.store.PinnedVersion(ctx, publisherTenant, task); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("publisher read consumer pin = %v, want NOT_FOUND", err)
	}
	deploys, err := e.store.ListDeployments(ctx, publisherTenant, inst.ID, 10)
	if err != nil {
		t.Fatalf("list deployments as publisher: %v", err)
	}
	if len(deploys) != 0 {
		t.Fatalf("publisher saw %d consumer deployments", len(deploys))
	}
	// The publisher still sees its own artifact versions (ownership is preserved).
	versions, err := e.store.ListVersions(ctx, testPublisher, im.agent, 10)
	if err != nil {
		t.Fatalf("publisher list own versions: %v", err)
	}
	if len(versions) == 0 {
		t.Fatal("publisher could not see its own published version")
	}
}

// ARCH-03: invoke routes to the runtime bound to the selected immutable digest,
// never to a shared endpoint. Two runtimes of the same agent, each with its own
// image, attestation and endpoint, are registered; an invoke for a version's
// digest reaches only that version's endpoint.
func TestInvokeRoutesBySelectedRuntimeDigest(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t)

	agent := "supply-" + randSuffix(t)
	ai, bi := e.imageRefPair(t)
	am := manifest(t, "supply.routea")
	am.Components[0].Image = ai.imageRef
	bm := manifest(t, "supply.routeb")
	bm.Components[0].Image = bi.imageRef
	va := publish(t, e.store, agent, am)
	vb := publish(t, e.store, agent, bm)

	var hitA, hitB atomic.Int64
	srvA := e.runtimeServer(t, agent, va.Digest, ai.imageRef, &hitA)
	defer srvA.Close()
	srvB := e.runtimeServer(t, agent, vb.Digest, bi.imageRef, &hitB)
	defer srvB.Close()

	for _, r := range []struct {
		v   registry.Version
		img imageEnv
		srv *httptest.Server
	}{
		{va, ai, srvA},
		{vb, bi, srvB},
	} {
		im := r.img
		im.agent = agent
		im.version = r.v
		im.secID = e.recordSecurity(t, im, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
		att := e.signFor(t, im, testPublisher, time.Now().Add(time.Hour))
		if _, err := e.store.AdmitImage(ctx, registry.AdmitImageInput{
			AgentID: agent, VersionID: r.v.ID, Image: im.imageRef, Attestation: att, SBOM: im.sbom,
		}); err != nil {
			t.Fatalf("admit route image: %v", err)
		}
		b := registry.RuntimeBinding{
			Tenant: e.tenant, Deployment: "route-" + r.v.ID, Environment: "dev", AgentID: agent,
			Digest: r.v.Digest, Image: im.imageRef, Attestation: string(mustJSON(t, att)), SBOM: im.sbom,
			Endpoint: srvURL(r.srv) + "/a2a", ReadyURL: srvURL(r.srv) + "/readyz",
		}
		if err := e.store.RegisterRuntime(ctx, b); err != nil {
			t.Fatalf("register runtime for %s: %v", im.imageRef, err)
		}
	}
	inst, err := e.store.Install(ctx, e.tenant, agent, va.ID, "dev", "inst-"+randSuffix(t), am.Capabilities)
	if err != nil {
		t.Fatalf("install: %v", err)
	}

	disp, err := registry.NewRoutedA2AInvokeDispatcher(e.store, nil)
	if err != nil {
		t.Fatalf("dispatcher: %v", err)
	}
	disp.SetRuntimeTokenIssuer(protocolTestIssuer{})
	if _, err := disp.Invoke(ctx, registry.InvokeRequest{Tenant: e.tenant, TaskID: "route-grant-a", AgentID: agent, InstallationID: inst.ID, Environment: "dev", VersionID: va.ID, VersionDigest: va.Digest, Input: json.RawMessage(`"go"`)}); err != nil {
		t.Fatalf("invoke A: %v", err)
	}
	if hitA.Load() != 1 || hitB.Load() != 0 {
		t.Fatalf("invoke for A routed to A=%d B=%d, want A=1 B=0", hitA.Load(), hitB.Load())
	}
	if _, err := disp.Invoke(ctx, registry.InvokeRequest{Tenant: e.tenant, TaskID: "route-grant-b", AgentID: agent, Environment: "dev", VersionID: vb.ID, VersionDigest: vb.Digest, Input: json.RawMessage(`"go"`)}); err != nil {
		t.Fatalf("invoke B: %v", err)
	}
	if hitB.Load() != 1 {
		t.Fatalf("invoke for B did not reach B endpoint (B=%d)", hitB.Load())
	}
}
