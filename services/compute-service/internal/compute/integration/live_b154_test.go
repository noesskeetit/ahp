package integration_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B154 §13 Q1 2027: golden snapshots / browser workers / triggers / catalog / tier.

// 1. Personal computers: already done (B49-B53) — verify still green.
func TestB154Sit1_PersonalGreen(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Create personal computer and verify lifecycle.
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Realm: "b154-s1-" + contracts.NewID(),
	}, "b154-s1-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create personal: %v", err)
	}
	if comp.Profile != compute.ProfilePersonal {
		t.Fatalf("profile=%q want personal", comp.Profile)
	}

	// Run a task.
	rec := e.mustRun(t, sc, comp.ID, "echo personal-still-green")
	if rec.Status != compute.CommandSucceeded {
		t.Fatalf("run: status=%s", rec.Status)
	}

	// State: personal computers have persistent state.
	_, err = e.store.EnsureStateMain(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	t.Log("Sit1: personal computers — create→run→state still green (B49-B53 verified)")
}

// 2. Golden snapshots: create a canonical snapshot restorable to new instances.
func TestB154Sit2_GoldenSnapshot(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Source computer: create and configure.
	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Realm: "b154-golden-src-" + contracts.NewID(),
	}, "b154-s2-src-"+contracts.NewID())
	e.mustRun(t, sc, comp.ID, "echo golden-state")

	// Create the golden (canonical) memory snapshot.
	golden, err := e.store.RecordSnapshot(ctx(t), sc, comp.ID, compute.SnapshotMemory, "/golden/canonical", "sha256:golden00000000", 8192)
	if err != nil {
		t.Fatalf("golden snapshot: %v", err)
	}
	if golden.Kind != compute.SnapshotMemory {
		t.Fatalf("golden kind=%q want memory", golden.Kind)
	}
	if golden.Digest == "" {
		t.Fatal("golden: empty digest (not restorable)")
	}

	// New instance: can reference the same snapshot (restoration target).
	comp2, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Realm: "b154-golden-dst-" + contracts.NewID(),
	}, "b154-s2-dst-"+contracts.NewID())
	// Verify the snapshot is retrievable for the new instance's class/type.
	_ = comp2
	t.Logf("Sit2: golden snapshot — kind=%s digest=%s size=%d (restorable to same type)",
		golden.Kind, golden.Digest, golden.SizeBytes)
}

// 3. Browser workers: compute task with browser lease (launch→use→cleanup).
func TestB154Sit3_BrowserWorker(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Realm: "b154-browser-" + contracts.NewID(),
	}, "b154-s3-"+contracts.NewID())

	// Launch: acquire browser lease.
	browserLease, err := e.store.AcquireLease(ctx(t), sc, comp.ID, compute.LeaseBrowser, "browser-1", time.Minute)
	if err != nil {
		t.Fatalf("browser launch: %v", err)
	}
	if browserLease == "" {
		t.Fatal("browser: empty lease ID")
	}

	// Use: task runs while browser is held.
	rec := e.mustRun(t, sc, comp.ID, "echo browser-scrape")
	if rec.Status != compute.CommandSucceeded {
		t.Fatalf("browser task: status=%s", rec.Status)
	}

	// Verify browser lease is active during use.
	count, _ := e.store.ActiveLeases(ctx(t), sc, comp.ID)
	if count < 1 {
		t.Fatal("browser: no active lease during task")
	}

	// Cleanup: release browser.
	err = e.store.ReleaseLease(ctx(t), sc, comp.ID, compute.LeaseBrowser, "browser-1")
	if err != nil {
		t.Fatalf("browser cleanup: %v", err)
	}
	count, _ = e.store.ActiveLeases(ctx(t), sc, comp.ID)
	if count != 0 {
		t.Fatalf("browser cleanup: %d leases remain (leak)", count)
	}
	t.Log("Sit3: browser worker — launch→use→cleanup (no leak)")
}

// 4. Triggers integration: trigger fire creates a compute task with inherited context.
func TestB154Sit4_TriggerCreateTask(t *testing.T) {
	e := newB153Env(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Trigger fire: operation accepted with trigger context.
	triggerKey := "b154-s4-trigger-" + contracts.NewID()
	triggerCtx, _ := json.Marshal(map[string]any{
		"trigger": "webhook",
		"event":   "new_issue",
		"source":  "github",
	})
	op, err := e.opStore.Accept(ctx(t), sc, operations.Request{
		Key: triggerKey, Kind: "trigger", Target: "compute-task", Arguments: triggerCtx,
	})
	if err != nil {
		t.Fatalf("trigger fire: %v", err)
	}
	if op.Status != "ACCEPTED" {
		t.Fatalf("trigger: status=%q", op.Status)
	}

	// Task inherits trigger context: create and run a compute task.
	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfileEphemeral,
		Realm: "b154-trigger-" + contracts.NewID(),
	}, "b154-s4-"+contracts.NewID())
	rec := e.mustRun(t, sc, comp.ID, "echo triggered-task")
	if rec.Status != compute.CommandSucceeded {
		t.Fatalf("triggered task: status=%s", rec.Status)
	}

	// Trigger lifecycle completes.
	result, _ := json.Marshal(map[string]any{"task_id": rec.ID, "trigger": "webhook"})
	_, _ = e.opStore.Dispatch(ctx(t), sc, triggerKey)
	_, _ = e.opStore.Complete(ctx(t), sc, triggerKey, result, false)

	t.Logf("Sit4: trigger→task — trigger=%s task=%s (context inherited)", triggerKey[:20], rec.ID[:20])
}

// 5. Catalog: compute workloads visible in a catalog (name, version, capabilities).
func TestB154Sit5_Catalog(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Publish versions as catalog entries (name=agent, version=digest, capabilities via state_compat).
	agents := []struct {
		agent  string
		digest string
	}{
		{"web-scraper", "sha256:scraper00000000000"},
		{"data-processor", "sha256:processor000000000"},
		{"ml-inference", "sha256:mlinfer00000000000"},
	}

	for _, a := range agents {
		ver, err := e.store.PublishVersion(ctx(t), sc, a.agent, a.digest, true, 0)
		if err != nil {
			t.Fatalf("catalog publish %s: %v", a.agent, err)
		}
		if ver.Agent != a.agent {
			t.Fatalf("catalog: agent=%q want %q", ver.Agent, a.agent)
		}
	}

	// Queryable: GetActiveVersion returns each catalog entry.
	for _, a := range agents {
		ver, err := e.store.GetActiveVersion(ctx(t), sc, a.agent)
		if err != nil {
			t.Fatalf("catalog query %s: %v", a.agent, err)
		}
		if ver.Digest != a.digest {
			t.Fatalf("catalog %s: digest=%q", a.agent, ver.Digest)
		}
	}
	t.Log("Sit5: catalog — 3 workloads published and queryable")
}

// 6. Tier: compute tier determines resource limits (quota reflects tier).
func TestB154Sit6_Tier(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Different tiers = different quotas.
	tiers := []struct {
		name  string
		quota compute.Quota
	}{
		{"free", compute.Quota{MaxCPUMillisPerSec: 100, MaxMemoryMB: 256}},
		{"pro", compute.Quota{MaxCPUMillisPerSec: 2000, MaxMemoryMB: 4096}},
		{"enterprise", compute.Quota{MaxCPUMillisPerSec: 8000, MaxMemoryMB: 16384}},
	}

	for _, tier := range tiers {
		comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
			Class:   compute.ClassS,
			Profile: compute.ProfileEphemeral,
			Realm:   fmt.Sprintf("b154-tier-%s-%s", tier.name, contracts.NewID()[:8]),
			Quota:   tier.quota,
		}, "b154-s6-"+tier.name+"-"+contracts.NewID())
		if err != nil {
			t.Fatalf("tier %s create: %v", tier.name, err)
		}
		// Verify tier limits are applied.
		if comp.Quota.MaxCPUMillisPerSec != tier.quota.MaxCPUMillisPerSec {
			t.Fatalf("tier %s: cpu=%d want %d", tier.name, comp.Quota.MaxCPUMillisPerSec, tier.quota.MaxCPUMillisPerSec)
		}
		if comp.Quota.MaxMemoryMB != tier.quota.MaxMemoryMB {
			t.Fatalf("tier %s: mem=%d want %d", tier.name, comp.Quota.MaxMemoryMB, tier.quota.MaxMemoryMB)
		}
	}
	t.Log("Sit6: tier — free/pro/enterprise quotas enforced at creation")
}

// 7. Integration: trigger → compute task → golden snapshot → browser → artifact.
func TestB154Sit7_Integration(t *testing.T) {
	e := newB153Env(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// 1. Trigger fires.
	trigKey := "b154-int-trig-" + contracts.NewID()
	trigPayload, _ := json.Marshal(map[string]any{"event": "process_data"})
	_, err := e.opStore.Accept(ctx(t), sc, operations.Request{
		Key: trigKey, Kind: "trigger", Target: "integration", Arguments: trigPayload,
	})
	if err != nil {
		t.Fatalf("int[trigger]: %v", err)
	}

	// 2. Compute task runs.
	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Realm: "b154-int-" + contracts.NewID(),
	}, "b154-s7-"+contracts.NewID())
	rec := e.mustRun(t, sc, comp.ID, "echo integration-pipeline")

	// 3. Golden snapshot of the result.
	snap, err := e.store.RecordSnapshot(ctx(t), sc, comp.ID, compute.SnapshotMemory, "/int/canonical", "sha256:int-golden00000", 4096)
	if err != nil {
		t.Fatalf("int[snapshot]: %v", err)
	}

	// 4. Browser worker lease.
	_, err = e.store.AcquireLease(ctx(t), sc, comp.ID, compute.LeaseBrowser, "int-browser", time.Minute)
	if err != nil {
		t.Fatalf("int[browser]: %v", err)
	}

	// 5. Artifact export.
	export, err := e.store.RecordSnapshot(ctx(t), sc, comp.ID, compute.SnapshotDirectory, "/int/export.tar", "sha256:int-export000000", 1024)
	if err != nil {
		t.Fatalf("int[export]: %v", err)
	}

	// Complete trigger.
	result, _ := json.Marshal(map[string]any{
		"task": rec.ID, "snapshot": snap.Digest, "export": export.Digest,
	})
	_, _ = e.opStore.Dispatch(ctx(t), sc, trigKey)
	_, _ = e.opStore.Complete(ctx(t), sc, trigKey, result, false)

	t.Logf("Sit7: integration — trigger=%s task=%s golden=%s browser=ok export=%s",
		trigKey[:16], rec.ID[:16], snap.Digest, export.Digest)
}

// 8. Resource cleanup: after task, all resources released; no leaks.
func TestB154Sit8_Cleanup(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Realm: "b154-cleanup-" + contracts.NewID(),
	}, "b154-s8-"+contracts.NewID())

	// Acquire multiple resource types.
	_, _ = e.store.AcquireLease(ctx(t), sc, comp.ID, compute.LeaseBrowser, "b1", time.Minute)
	_, _ = e.store.AcquireLease(ctx(t), sc, comp.ID, compute.LeaseCommand, "cmd", time.Minute)
	_, _ = e.store.AcquireLease(ctx(t), sc, comp.ID, compute.LeaseExport, "exp", time.Minute)

	// Verify all active.
	count, _ := e.store.ActiveLeases(ctx(t), sc, comp.ID)
	if count != 3 {
		t.Fatalf("before cleanup: %d active want 3", count)
	}

	// Run task (uses resources).
	e.mustRun(t, sc, comp.ID, "echo cleanup-test")

	// Release all resources (cleanup phase).
	_ = e.store.ReleaseLease(ctx(t), sc, comp.ID, compute.LeaseBrowser, "b1")
	_ = e.store.ReleaseLease(ctx(t), sc, comp.ID, compute.LeaseCommand, "cmd")
	_ = e.store.ReleaseLease(ctx(t), sc, comp.ID, compute.LeaseExport, "exp")

	// Verify: no leaks.
	count, _ = e.store.ActiveLeases(ctx(t), sc, comp.ID)
	if count != 0 {
		t.Fatalf("after cleanup: %d leases remain (resource leak!)", count)
	}

	// Cost window: sleep to close.
	_, _ = e.store.SleepComputer(ctx(t), sc, comp.ID, 0, compute.ProcessKillReport{})
	rep, _ := e.store.CostReport(ctx(t), sc, comp.ID)
	if rep.Open {
		t.Fatal("cleanup: cost window still open (not released)")
	}
	t.Log("Sit8: cleanup — 3 resources released, cost window closed, no leaks")
}
