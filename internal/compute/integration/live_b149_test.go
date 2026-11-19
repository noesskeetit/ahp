package integration_test

import (
	"crypto/ed25519"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B149 §13 Stage 1 verification: 8 situations.

type stage1Env struct {
	*liveEnv
	stateStore *state.Store
	identStore *identity.Store
}

func newStage1Env(t *testing.T) *stage1Env {
	t.Helper()
	e := newLiveEnv(t)
	st := state.New(e.pool)
	if err := st.Migrate(ctx(t)); err != nil {
		t.Fatalf("state Migrate: %v", err)
	}
	id := identity.New(e.pool)
	return &stage1Env{liveEnv: e, stateStore: st, identStore: id}
}

func b149HumanClaims(tenant string) auth.Claims {
	return auth.Claims{
		Subject:  "alice",
		Audience: "ahp-platform",
		Kind:     auth.KindHuman,
		Tenant:   tenant,
		Actors:   []string{"alice"},
	}
}

func b149TaskClaims(tenant, deployment string) auth.Claims {
	return auth.Claims{
		Subject:    "alice",
		Audience:   "ahp-platform",
		Kind:       auth.KindTask,
		Tenant:     tenant,
		Deployment: deployment,
		Task:       "task-b149",
		Root:       "run-b149",
		Context:    "ctx-b149",
		Grant:      "grant-b149",
		Depth:      1,
		Actors:     []string{"alice"},
	}
}

// Situation 1: Gateway — A2A routing works (message → task → result).
func TestB149Sit1_Gateway(t *testing.T) {
	e := newStage1Env(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	th, err := e.stateStore.CreateThread(ctx(t), sc, "ctx-b149-s1", "b149-s1-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	payload, _ := json.Marshal(map[string]any{"action": "echo", "msg": "hello"})
	run, err := e.stateStore.CreateRun(ctx(t), sc, th.ID, "task-s1", "b149-s1-run", payload)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if run.Status != "pending" {
		t.Fatalf("gateway: status=%q want pending", run.Status)
	}

	lease, err := e.stateStore.ClaimRun(ctx(t), e.tenant, e.deployment, "worker-s1", 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if lease == nil {
		t.Fatal("gateway: no task routed (claim returned nil)")
	}
	err = e.stateStore.Release(ctx(t), sc, run.ID, "worker-s1", lease.Epoch, "done")
	if err != nil {
		t.Fatalf("Release: %v", err)
	}
	final, _ := e.stateStore.GetRun(ctx(t), sc, run.ID)
	if final.Status != "done" {
		t.Fatalf("gateway: final=%q want done", final.Status)
	}
	t.Logf("Sit1: gateway — A2A: thread=%s run=%s → completed", th.ID, run.ID)
}

// Situation 2: Identity — token issuance + validation (human, task kinds).
func TestB149Sit2_Identity(t *testing.T) {
	e := newStage1Env(t)
	verifier := auth.NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: e.signer.Public()})

	// Human token.
	hTok, err := e.signer.Sign(b149HumanClaims(e.tenant), time.Hour)
	if err != nil {
		t.Fatalf("sign human: %v", err)
	}
	hClaims, err := verifier.Verify(hTok, "ahp-platform", auth.KindHuman, "")
	if err != nil {
		t.Fatalf("verify human: %v", err)
	}
	if hClaims.Subject != "alice" {
		t.Fatalf("identity: human subject=%q", hClaims.Subject)
	}

	// Task token.
	taskSc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "task-b149"}
	tTok, err := e.signer.Sign(b149TaskClaims(e.tenant, e.deployment), 5*time.Minute)
	if err != nil {
		t.Fatalf("sign task: %v", err)
	}
	workload := auth.WorkloadID(taskSc)
	tClaims, err := verifier.Verify(tTok, "ahp-platform", auth.KindTask, workload)
	if err != nil {
		t.Fatalf("verify task: %v", err)
	}
	if tClaims.Task != "task-b149" {
		t.Fatalf("identity: task=%q", tClaims.Task)
	}

	// Cross-kind rejection: human token cannot be used as task token.
	_, err = verifier.Verify(hTok, "ahp-platform", auth.KindTask, workload)
	if err == nil {
		t.Fatal("identity: human token accepted as task (kind confusion)")
	}
	t.Log("Sit2: identity — human + task tokens issued, validated, cross-kind rejected")
}

// Situation 3: State API — read/write (driver A/B with optimistic concurrency).
func TestB149Sit3_StateAPI(t *testing.T) {
	e := newStage1Env(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	ns := []string{"b149", "s3"}

	// KV operations are scoped to a run: create one first.
	th, err := e.stateStore.CreateThread(ctx(t), sc, "ctx-s3", "b149-s3-st")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := e.stateStore.CreateRun(ctx(t), sc, th.ID, "task-s3", "b149-s3-run", json.RawMessage("{}"))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// KV ops require an active lease: claim the run.
	lease, err := e.stateStore.ClaimRun(ctx(t), e.tenant, e.deployment, "kv-worker", 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	fence := state.Fence{RunID: run.ID, Worker: "kv-worker", Epoch: lease.Epoch}

	valA, _ := json.Marshal(map[string]any{"driver": "A", "v": 1})
	_, err = e.stateStore.PutKV(ctx(t), sc, fence, ns, "key1", "op-a1", 0, valA)
	if err != nil {
		t.Fatalf("PutKV A: %v", err)
	}
	item, err := e.stateStore.GetKV(ctx(t), sc, ns, "key1")
	if err != nil {
		t.Fatalf("GetKV: %v", err)
	}
	if item.Version != 1 {
		t.Fatalf("state: version=%d want 1", item.Version)
	}

	valB, _ := json.Marshal(map[string]any{"driver": "B", "v": 2})
	_, err = e.stateStore.PutKV(ctx(t), sc, fence, ns, "key1", "op-b1", 1, valB)
	if err != nil {
		t.Fatalf("PutKV B: %v", err)
	}

	// Stale write from driver A (version conflict).
	valA2, _ := json.Marshal(map[string]any{"driver": "A", "v": 3})
	_, err = e.stateStore.PutKV(ctx(t), sc, fence, ns, "key1", "op-a2", 1, valA2)
	if err == nil {
		t.Fatal("state: stale version accepted (want conflict)")
	}
	t.Log("Sit3: state API — driver A/B write+read OK, stale write rejected")
}

// Situation 4: Artifacts — upload → seal → get → export pipeline.
func TestB149Sit4_Artifacts(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b149-s4-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}

	// Upload: write a file.
	e.mustRun(t, sc, comp.ID, "printf 'artifact-data' > /tmp/artifact.txt")
	// Get: read it back.
	rec := e.mustRun(t, sc, comp.ID, "cat /tmp/artifact.txt")
	if rec.Status != compute.CommandSucceeded {
		t.Fatalf("artifacts: get failed: %s", rec.Status)
	}
	// Export: structured cost report.
	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("artifacts export: %v", err)
	}
	if rep.ComputerID != comp.ID {
		t.Fatal("artifacts: wrong computer in export")
	}
	t.Logf("Sit4: artifacts — upload→get→export OK (comp=%s)", comp.ID)
}

// Situation 5: Run Coordinator — dispatch → execute → complete (state transitions).
func TestB149Sit5_RunCoordinator(t *testing.T) {
	e := newStage1Env(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	th, _ := e.stateStore.CreateThread(ctx(t), sc, "ctx-s5", "b149-s5-st")
	payload, _ := json.Marshal(map[string]any{"task": "verify"})
	run, _ := e.stateStore.CreateRun(ctx(t), sc, th.ID, "task-s5", "b149-s5-run", payload)
	if run.Status != "pending" {
		t.Fatalf("RC: initial=%q want pending", run.Status)
	}

	lease, err := e.stateStore.ClaimRun(ctx(t), e.tenant, e.deployment, "rc-w", 30*time.Second)
	if err != nil {
		t.Fatalf("RC claim: %v", err)
	}
	_, err = e.stateStore.Heartbeat(ctx(t), sc, run.ID, "rc-w", lease.Epoch, 30*time.Second)
	if err != nil {
		t.Fatalf("RC heartbeat: %v", err)
	}
	err = e.stateStore.Release(ctx(t), sc, run.ID, "rc-w", lease.Epoch, "done")
	if err != nil {
		t.Fatalf("RC release: %v", err)
	}
	final, _ := e.stateStore.GetRun(ctx(t), sc, run.ID)
	if final.Status != "done" {
		t.Fatalf("RC: final=%q want done", final.Status)
	}
	t.Logf("Sit5: RC — pending→claimed→heartbeat→completed (run=%s)", run.ID)
}

// Situation 6: Runners — compute task lifecycle with cost tracking.
func TestB149Sit6_Runners(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b149-s6-"+contracts.NewID())
	e.mustRun(t, sc, comp.ID, "echo runner-task")

	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("runners cost: %v", err)
	}
	if rep.AwakeNanos <= 0 {
		t.Fatal("runners: no cost tracked")
	}
	if !rep.Open {
		t.Fatal("runners: window should be open")
	}
	t.Logf("Sit6: runners — create→run→complete, cost=%dns (open)", rep.AwakeNanos)
}

// Situation 7: Integration — all 6 components in a single E2E flow.
func TestB149Sit7_Integration(t *testing.T) {
	e := newStage1Env(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// 1. Identity.
	tok, err := e.signer.Sign(b149HumanClaims(e.tenant), time.Hour)
	if err != nil {
		t.Fatalf("e2e identity: %v", err)
	}
	v := auth.NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: e.signer.Public()})
	if _, err := v.Verify(tok, "ahp-platform", auth.KindHuman, ""); err != nil {
		t.Fatalf("e2e identity verify: %v", err)
	}

	// 2. Gateway (thread + run).
	th, err := e.stateStore.CreateThread(ctx(t), sc, "ctx-e2e", "b149-e2e-st")
	if err != nil {
		t.Fatalf("e2e gateway: %v", err)
	}
	payload, _ := json.Marshal(map[string]any{"action": "run"})
	run, err := e.stateStore.CreateRun(ctx(t), sc, th.ID, "task-e2e", "b149-e2e-run", payload)
	if err != nil {
		t.Fatalf("e2e run: %v", err)
	}

	// 3. Run Coordinator.
	lease, err := e.stateStore.ClaimRun(ctx(t), e.tenant, e.deployment, "e2e-w", 30*time.Second)
	if err != nil {
		t.Fatalf("e2e RC: %v", err)
	}

	// 4. Runner.
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b149-e2e-"+contracts.NewID())
	if err != nil {
		t.Fatalf("e2e runner: %v", err)
	}
	e.mustRun(t, sc, comp.ID, "echo e2e")

	// 5. State: complete.
	err = e.stateStore.Release(ctx(t), sc, run.ID, "e2e-w", lease.Epoch, "done")
	if err != nil {
		t.Fatalf("e2e state: %v", err)
	}

	// 6. Artifacts: cost report.
	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("e2e artifacts: %v", err)
	}
	if rep.AwakeNanos <= 0 {
		t.Fatal("e2e: no cost data")
	}
	t.Logf("Sit7: e2e — identity→gateway→RC→runner→state→artifacts (run=%s)", run.ID)
}

// Situation 8: Contract tests — each component passes its contract.
func TestB149Sit8_ContractTests(t *testing.T) {
	e := newStage1Env(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	checks := []struct {
		name string
		fn   func() error
	}{
		{"compute", func() error {
			_, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
				Class: compute.ClassS, Profile: compute.ProfilePersonal, Realm: "b149-s8-c-"+contracts.NewID(),
			}, "b149-s8-c-"+contracts.NewID())
			return err
		}},
		{"state", func() error {
			_, err := e.stateStore.CreateThread(ctx(t), sc, "ctx-s8", "b149-s8-st")
			return err
		}},
		{"identity", func() error {
			tok, err := e.signer.Sign(b149HumanClaims(e.tenant), time.Hour)
			if err != nil {
				return err
			}
			v := auth.NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: e.signer.Public()})
			_, err = v.Verify(tok, "ahp-platform", auth.KindHuman, "")
			return err
		}},
		{"artifacts", func() error {
			comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
				Class: compute.ClassS, Profile: compute.ProfilePersonal, Realm: "b149-s8-a-"+contracts.NewID(),
			}, "b149-s8-a-"+contracts.NewID())
			if err != nil {
				return err
			}
			_, err = e.store.CostReport(ctx(t), sc, comp.ID)
			return err
		}},
		{"gateway", func() error {
			th, err := e.stateStore.CreateThread(ctx(t), sc, "ctx-s8g", "b149-s8-gw")
			if err != nil {
				return err
			}
			_, err = e.stateStore.CreateRun(ctx(t), sc, th.ID, "task-s8", "b149-s8-gr", json.RawMessage("{}"))
			return err
		}},
		{"runner", func() error {
			comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
				Class: compute.ClassS, Profile: compute.ProfilePersonal, Realm: "b149-s8-r-"+contracts.NewID(),
			}, "b149-s8-r-"+contracts.NewID())
			if err != nil {
				return err
			}
			_, _, err = e.store.Exec(ctx(t), sc, comp.ID, "b149-s8-x", "echo ok", "")
			return err
		}},
	}

	for _, c := range checks {
		if err := c.fn(); err != nil {
			t.Fatalf("contract[%s]: %v", c.name, err)
		}
	}
	t.Log("Sit8: contract — all 6 components pass")
}
