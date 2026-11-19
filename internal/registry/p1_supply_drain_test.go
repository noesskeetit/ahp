package registry_test

// P1 (root-audit 20261109, Registry/Deploy) acceptance face. Every rule here is
// taken from the PDF rev 3.2 text, not from an implementation convenience:
//
//   - c. 41 pipeline: "ahp-sdk check, сборка image@sha256, SBOM и подпись. Затем
//     версия в Registry, dev, stage с оценками ... chaos: рестарт пода, проверка
//     ИБ и prod" -- a stage/prod rollout therefore needs the evaluation, the
//     restart-chaos and the security clearance of the SAME image digest, while a
//     dev/draft rollout must not depend on the stage verdicts that only exist
//     after it (no circular publication dependency).
//   - c. 41/42 versions: "старая версия работает, пока у неё есть незавершённые
//     задачи, но не дольше окна drain из манифеста. После окна задачи, ждущие
//     ответа, переносятся на новую версию, если манифест объявляет совместимость
//     состояния (state.compat), иначе завершаются с понятной причиной" -- so the
//     manifest's `state.drain` is a WINDOW (a duration such as "7d"), and the
//     window must actually be enforced somewhere, not merely planned.
//   - c. 41: promotion is impossible when any mandatory stage failed, and a
//     caller that mutates the stored records behind the Registry's back must
//     still be refused where the traffic is actually routed.

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// 1. state.drain is the drain WINDOW from the manifest, not a hook path.
func TestManifestDrainWindowIsADurationFromManifest(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{name: "pdf example", raw: "7d", want: 7 * 24 * time.Hour},
		{name: "hours", raw: "12h", want: 12 * time.Hour},
		{name: "compound", raw: "1d6h", want: 30 * time.Hour},
		{name: "seconds", raw: "45s", want: 45 * time.Second},
		{name: "declared zero", raw: "0s", want: 0},
		{name: "undeclared", raw: "", want: 0},
		{name: "hook path is not a window", raw: "hooks/drain", wantErr: true},
		{name: "negative", raw: "-5m", wantErr: true},
		{name: "garbage", raw: "7x", wantErr: true},
		{name: "absurdly long", raw: "100d", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := registry.ParseDrainWindow(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseDrainWindow(%q) = %v, want refusal", tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDrainWindow(%q): %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("ParseDrainWindow(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}

	m := manifest(t, "drain.window")
	m.State.Drain = "7d"
	if err := registry.ValidateManifest(m); err != nil {
		t.Fatalf("manifest with the PDF drain window rejected: %v", err)
	}
	m.State.Drain = "hooks/drain"
	if err := registry.ValidateManifest(m); err == nil {
		t.Fatal("manifest accepted a hook path where the PDF fixes a duration window")
	}
}

// 2. The promote records the window it has to honour, and inside that window a
// task already pinned to the previous version keeps running there.
func TestPromoteRecordsManifestDrainWindowAndServesPinnedTasksInsideIt(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t)

	agent := "supply-" + randSuffix(t)
	a, b := e.imageRefPair(t)
	am := manifest(t, "supply.win.a")
	am.State.Drain = "7d"
	am.State.Compat = "compat-" + randSuffix(t)
	am.Components[0].Image = a.imageRef
	bm := manifest(t, "supply.win.b")
	bm.State.Drain = "7d"
	bm.State.Compat = "compat-" + randSuffix(t)
	bm.Components[0].Image = b.imageRef
	va, vb := publish(t, e.store, agent, am), publish(t, e.store, agent, bm)
	for _, pair := range []struct {
		v   registry.Version
		img imageEnv
	}{{va, a}, {vb, b}} {
		e.admitPair(t, agent, pair.v, pair.img)
	}

	inst := e.stageInstall(t, agent, va, a, "stage")
	waiting := "task-" + randSuffix(t)
	if _, err := e.store.Pin(ctx, e.tenant, inst.ID, waiting); err != nil {
		t.Fatalf("pin waiting task on A: %v", err)
	}
	if _, err := e.store.Promote(ctx, e.tenant, inst.ID, vb.ID); err != nil {
		t.Fatalf("promote A->B: %v", err)
	}

	drain, err := e.store.DrainState(ctx, e.tenant, inst.ID)
	if err != nil {
		t.Fatalf("drain state: %v", err)
	}
	if !drain.InDrain {
		t.Fatalf("promote left no drain in progress: %+v", drain)
	}
	if drain.PreviousVersion != va.ID {
		t.Fatalf("draining version = %s, want %s", drain.PreviousVersion, va.ID)
	}
	if drain.PinnedOnPrevious != 1 {
		t.Fatalf("pinned tasks on the draining version = %d, want 1", drain.PinnedOnPrevious)
	}
	// The window comes from the manifest, not from an invented default.
	if since := time.Until(drain.DrainUntil); since < 6*24*time.Hour || since > 7*24*time.Hour {
		t.Fatalf("drain deadline %v does not carry the manifest's 7d window (in %v)", drain.DrainUntil, since)
	}

	resolved, err := e.store.ResolveConfig(ctx, e.tenant, inst.ID, waiting)
	if err != nil {
		t.Fatalf("resolve a task inside the drain window: %v", err)
	}
	if resolved.VersionID != va.ID {
		t.Fatalf("task inside the drain window resolved to %s, want the pinned %s", resolved.VersionID, va.ID)
	}
	// A NEW task starts on the promoted version: the drain only protects tasks
	// that already began.
	fresh, err := e.store.ResolveConfig(ctx, e.tenant, inst.ID, "task-"+randSuffix(t))
	if err != nil {
		t.Fatalf("resolve a new task during drain: %v", err)
	}
	if fresh.VersionID != vb.ID {
		t.Fatalf("new task resolved to %s, want the promoted %s", fresh.VersionID, vb.ID)
	}
}

// 3. Once the window elapses, a waiting task moves to the new version when the
// new manifest declares state compatibility -- and the pin records where it came
// from, so ResolveConfig reports what actually executes.
func TestDrainWindowElapsedTransfersWaitingTaskOnStateCompat(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t)

	agent := "supply-" + randSuffix(t)
	a, b := e.imageRefPair(t)
	shared := "compat-" + randSuffix(t)
	am := manifest(t, "supply.tx.a")
	am.State.Drain = "1s"
	am.State.Compat = shared
	am.Components[0].Image = a.imageRef
	bm := manifest(t, "supply.tx.b")
	bm.State.Drain = "1s"
	bm.State.Compat = shared
	bm.Components[0].Image = b.imageRef
	va, vb := publish(t, e.store, agent, am), publish(t, e.store, agent, bm)
	e.admitPair(t, agent, va, a)
	e.admitPair(t, agent, vb, b)

	inst := e.stageInstall(t, agent, va, a, "stage")
	waiting := "task-" + randSuffix(t)
	if _, err := e.store.Pin(ctx, e.tenant, inst.ID, waiting); err != nil {
		t.Fatalf("pin: %v", err)
	}
	if _, err := e.store.Promote(ctx, e.tenant, inst.ID, vb.ID); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if got, err := e.store.PinnedVersion(ctx, e.tenant, waiting); err != nil || got.ID != va.ID {
		t.Fatalf("pin immediately after promote = %s (%v), want %s", got.ID, err, va.ID)
	}

	e.awaitDrainElapsed(t, inst.ID)

	resolved, err := e.store.ResolveConfig(ctx, e.tenant, inst.ID, waiting)
	if err != nil {
		t.Fatalf("resolve after the drain window with compatible state: %v", err)
	}
	if resolved.VersionID != vb.ID {
		t.Fatalf("task after the drain window resolved to %s, want the compatible %s", resolved.VersionID, vb.ID)
	}
	if resolved.MigratedFrom != va.ID {
		t.Fatalf("transfer did not report its origin: %+v", resolved)
	}
	if got, err := e.store.PinnedVersion(ctx, e.tenant, waiting); err != nil || got.ID != vb.ID {
		t.Fatalf("pin was not carried over with the task: %s (%v), want %s", got.ID, err, vb.ID)
	}
}

// 4. The same situation without a declared compatibility must FAIL with a
// reason, not silently re-point the task at state it cannot read.
func TestDrainWindowElapsedWithoutCompatFailsWaitingTaskWithReason(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t)

	agent := "supply-" + randSuffix(t)
	a, b := e.imageRefPair(t)
	am := manifest(t, "supply.incompat.a")
	am.State.Drain = "1s"
	am.State.Compat = "compat-" + randSuffix(t)
	am.Components[0].Image = a.imageRef
	bm := manifest(t, "supply.incompat.b")
	bm.State.Drain = "1s"
	bm.State.Compat = ""
	bm.Components[0].Image = b.imageRef
	va, vb := publish(t, e.store, agent, am), publish(t, e.store, agent, bm)
	e.admitPair(t, agent, va, a)
	e.admitPair(t, agent, vb, b)

	inst := e.stageInstall(t, agent, va, a, "stage")
	waiting := "task-" + randSuffix(t)
	if _, err := e.store.Pin(ctx, e.tenant, inst.ID, waiting); err != nil {
		t.Fatalf("pin: %v", err)
	}
	if _, err := e.store.Promote(ctx, e.tenant, inst.ID, vb.ID); err != nil {
		t.Fatalf("promote: %v", err)
	}
	e.awaitDrainElapsed(t, inst.ID)

	_, err := e.store.ResolveConfig(ctx, e.tenant, inst.ID, waiting)
	if err == nil {
		t.Fatal("incompatible task outlived the drain window and was silently carried over")
	}
	if !strings.Contains(err.Error(), "drain") {
		t.Fatalf("refusal must explain the drain window, got: %v", err)
	}
	if got, perr := e.store.PinnedVersion(ctx, e.tenant, waiting); perr != nil || got.ID != va.ID {
		t.Fatalf("a failed transfer must not destroy the original pin: %s (%v)", got.ID, perr)
	}
}

// 5. stage/prod require the restart-chaos ("chaos: рестарт пода") clearance of
// the same image: missing, failing or expired all block the rollout, while dev
// stays reachable without the stage planes.
func TestStageAndProdRefusedWhenRestartChaosVerdictIsMissingFailingOrExpired(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t)
	// Admitted and security-cleared, but with NO restart-chaos verdict yet: that
	// is the one stage plane this leg varies, so the fixture is built inline
	// rather than through admittedImage (which records a passing chaos verdict).
	im := e.publishImage(t, "chaos")
	im.secID = e.recordSecurity(t, im, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	e.signAndAdmit(t, im)

	if _, err := e.store.Install(ctx, e.tenant, im.agent, im.version.ID, "dev", "inst-"+randSuffix(t), im.version.Manifest.Capabilities); err != nil {
		t.Fatalf("dev install of an image cleared for dev: %v", err)
	}
	if _, err := e.store.Install(ctx, e.tenant, im.agent, im.version.ID, "stage", "inst-"+randSuffix(t), im.version.Manifest.Capabilities); err == nil {
		t.Fatal("stage install admitted an image with no restart-chaos verdict")
	}

	e.recordChaos(t, im, "fail", time.Now().UTC().Add(-time.Minute), time.Now().UTC().Add(time.Hour))
	if _, err := e.store.Install(ctx, e.tenant, im.agent, im.version.ID, "stage", "inst-"+randSuffix(t), im.version.Manifest.Capabilities); err == nil {
		t.Fatal("stage install admitted an image that failed the restart-chaos test")
	}

	e.recordChaos(t, im, "pass", time.Now().UTC().Add(-2*time.Hour), time.Now().UTC().Add(-time.Hour))
	if _, err := e.store.Install(ctx, e.tenant, im.agent, im.version.ID, "stage", "inst-"+randSuffix(t), im.version.Manifest.Capabilities); err == nil {
		t.Fatal("stage install admitted an image whose restart-chaos verdict expired")
	}

	e.recordChaos(t, im, "pass", time.Now().UTC(), time.Now().UTC().Add(24*time.Hour))
	if _, err := e.store.Install(ctx, e.tenant, im.agent, im.version.ID, "stage", "inst-"+randSuffix(t), im.version.Manifest.Capabilities); err != nil {
		t.Fatalf("stage install of an image with every stage plane passed: %v", err)
	}
}

// 6. dev/draft must not need the stage security clearance, and stage must use
// the clearance the signature actually binds. This is what removes the
// publication cycle without loosening the stage/prod gate.
func TestDevAdmissionDoesNotRequireStageSecurityClearance(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t)

	im := e.publishImage(t, "nocycle")
	att := e.signFor(t, im, testPublisher, time.Now().Add(time.Hour))
	if _, err := e.store.AdmitImage(ctx, registry.AdmitImageInput{
		AgentID: im.agent, VersionID: im.version.ID, Image: im.imageRef, Attestation: att, SBOM: im.sbom,
	}); err != nil {
		t.Fatalf("dev admission before the ИБ check: %v", err)
	}
	if _, err := e.store.Install(ctx, e.tenant, im.agent, im.version.ID, "dev", "inst-"+randSuffix(t), im.version.Manifest.Capabilities); err != nil {
		t.Fatalf("dev install before the ИБ check: %v", err)
	}
	if _, err := e.store.Install(ctx, e.tenant, im.agent, im.version.ID, "stage", "inst-"+randSuffix(t), im.version.Manifest.Capabilities); err == nil {
		t.Fatal("stage install admitted an image with no security clearance at all")
	}

	// The ИБ authority clears the image; the publisher re-admits so the signature
	// binds that clearance; stage then accepts it.
	im.secID = e.recordSecurity(t, im, "pass", time.Now().UTC(), time.Now().UTC().Add(24*time.Hour))
	e.recordChaos(t, im, "pass", time.Now().UTC(), time.Now().UTC().Add(24*time.Hour))
	if _, err := e.store.Install(ctx, e.tenant, im.agent, im.version.ID, "stage", "inst-"+randSuffix(t), im.version.Manifest.Capabilities); err == nil {
		t.Fatal("stage accepted a signature that does not bind the issued clearance")
	}
	e.admit(t, im)
	if _, err := e.store.Install(ctx, e.tenant, im.agent, im.version.ID, "stage", "inst-"+randSuffix(t), im.version.Manifest.Capabilities); err != nil {
		t.Fatalf("stage install after the clearance was bound: %v", err)
	}
}

// 7. An operator who has installed no trust roots cannot register a runtime on
// the strength of a non-empty attestation string. The only way out is an
// explicitly elected SYNTHETIC stand, and the mode has to stay observable so a
// synthetic result can never be reported as real PKI verification.
func TestRegisterRuntimeFailsClosedWithoutTrustRoots(t *testing.T) {
	ctx := context.Background()
	store, tenant := newStore(t)

	srv := httptest.NewServer(nil)
	t.Cleanup(srv.Close)
	binding := registry.RuntimeBinding{
		Tenant: tenant, Deployment: "rt-" + randSuffix(t), Environment: "dev",
		AgentID: "agent-" + randSuffix(t),
		Digest:  "sha256:" + strings.Repeat("a", 64),
		Image:   "registry.local/ahp/agent:p1@sha256:" + strings.Repeat("a", 64),
		// A non-empty, well-shaped document: exactly what the old check accepted.
		Attestation: `{"payloadType":"application/vnd.in-toto+json","payload":"e30","signatures":[]}`,
		SBOM:        []byte(`{"bomFormat":"CycloneDX","components":[]}`),
		Endpoint:    srv.URL + "/a2a", ReadyURL: srv.URL + "/readyz",
	}
	err := store.RegisterRuntime(ctx, binding)
	if err == nil {
		t.Fatal("runtime accepted on a non-empty attestation string with no trust roots installed")
	}
	if !strings.Contains(err.Error(), "trust roots") {
		t.Fatalf("refusal must name the missing trust roots, got: %v", err)
	}
	var apiErr *contracts.Error
	if !errors.As(err, &apiErr) || apiErr.Code != contracts.ErrUnavailable {
		t.Fatalf("fail-closed registration = %v, want UNAVAILABLE", err)
	}

	if mode := store.SupplyChainMode(); mode != registry.SupplyChainModeUnset {
		t.Fatalf("an unconfigured store must report %q, got %q", registry.SupplyChainModeUnset, mode)
	}
	if reason := store.SyntheticElection(); reason != "" {
		t.Fatalf("a store without a synthetic election must report no reason, got %q", reason)
	}

	dev := registry.New(poolFor(t), registry.WithSyntheticSupplyChain("lane fixture, no operator PKI"))
	if err := dev.RegisterRuntime(ctx, binding); err != nil {
		t.Fatalf("explicitly elected unverified runtime registration: %v", err)
	}
	if mode := dev.SupplyChainMode(); mode != registry.SupplyChainModeSynthetic {
		t.Fatalf("SupplyChainMode() = %q, want the explicit synthetic election to be visible", mode)
	}
	if reason := dev.SyntheticElection(); reason == "" {
		t.Fatal("a synthetic stand must report its stated reason so evidence can mark it synthetic")
	}
}

// 9. dev may not serve a real tenant's users: publication into the Agents Space
// catalogue -- the plane employees actually reach -- is refused for a dev
// installation of a real tenant, and only the prod installation becomes visible.
// This is what makes "the security/eval/chaos gate starts at stage" safe: dev
// cannot receive user traffic in the first place.
func TestDevInstallationCannotServeRealTenantTraffic(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t)

	im := e.fullReadyImage(t, "devtraffic")
	m := im.version.Manifest
	m.Visibility = registry.VisibilityCatalog
	agent := "supply-" + randSuffix(t)
	v := publish(t, e.store, agent, m)
	im.version = v
	e.admitPair(t, agent, v, im)

	dev, err := e.store.Install(ctx, e.tenant, agent, v.ID, "dev", "inst-"+randSuffix(t), m.Capabilities)
	if err != nil {
		t.Fatalf("dev install: %v", err)
	}
	if err := e.store.Publish(ctx, e.tenant, dev.ID, true); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("publishing a dev installation = %v, want CONFLICT", err)
	}
	if err := e.store.PublishWithRoles(ctx, e.tenant, dev.ID, []string{"role:employee"}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("PublishWithRoles on a dev installation = %v, want CONFLICT", err)
	}
	if got, err := e.store.GetInstallation(ctx, e.tenant, dev.ID); err != nil || got.Published {
		t.Fatalf("dev installation became user-visible anyway: %+v (%v)", got, err)
	}
	if cards, err := e.store.CatalogCards(ctx, e.tenant, nil); err != nil || len(cards) != 0 {
		t.Fatalf("dev installation leaked into the catalogue: %+v (%v)", cards, err)
	}

	prod, err := e.store.Install(ctx, e.tenant, agent, v.ID, "prod", "inst-"+randSuffix(t), m.Capabilities)
	if err != nil {
		t.Fatalf("prod install: %v", err)
	}
	if err := e.store.Publish(ctx, e.tenant, prod.ID, true); err != nil {
		t.Fatalf("publishing the prod installation: %v", err)
	}
	cards, err := e.store.CatalogCards(ctx, e.tenant, nil)
	if err != nil || len(cards) != 1 || cards[0].Environment != "prod" {
		t.Fatalf("catalogue after publishing prod = %+v (%v)", cards, err)
	}
}

// 8. Skipping the gate by writing to the stored records behind the Registry's
// back must still be refused where the task is actually pinned and resolved.
func TestDirectMutationOfInstallationCannotBypassAdmission(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t)

	agent := "supply-" + randSuffix(t)
	good, bad := e.imageRefPair(t)
	gm := manifest(t, "supply.bypass.good")
	gm.Components[0].Image = good.imageRef
	bm := manifest(t, "supply.bypass.bad")
	bm.Components[0].Image = bad.imageRef
	vg, vbad := publish(t, e.store, agent, gm), publish(t, e.store, agent, bm)
	e.admitPair(t, agent, vg, good)
	// vbad is published but never admitted: no signature, no SBOM, no verdicts.

	inst := e.stageInstall(t, agent, vg, good, "stage")
	if _, err := e.store.Promote(ctx, e.tenant, inst.ID, vbad.ID); err == nil {
		t.Fatal("promote accepted an image with no verified supply chain")
	}
	if _, err := e.pool.Exec(ctx, `UPDATE ahp_registry.installations SET version_id = $1 WHERE tenant = $2 AND id = $3`,
		vbad.ID, e.tenant, inst.ID); err != nil {
		t.Fatalf("simulate a direct record mutation: %v", err)
	}

	if _, err := e.store.Pin(ctx, e.tenant, inst.ID, "task-"+randSuffix(t)); err == nil {
		t.Fatal("a task was pinned onto a version reached by mutating the installation row")
	}
	if _, err := e.store.ResolveConfig(ctx, e.tenant, inst.ID, "task-"+randSuffix(t)); err == nil {
		t.Fatal("ResolveConfig routed a new task onto a version reached by mutating the installation row")
	}
	// Reading the installation as it stands must say why it cannot serve.
	if _, err := e.store.ResolveConfig(ctx, e.tenant, inst.ID, ""); err == nil {
		t.Fatal("ResolveConfig reported the mutated installation as executable")
	}
}
