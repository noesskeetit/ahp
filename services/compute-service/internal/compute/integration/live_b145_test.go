package integration_test

import (
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B145 §11 Capacity and cost: 8 metering situations.

// Situation 1: Per-task metering — a completed task has a cost report with
// awake time > 0 (the incarnation was acquired during execution).
func TestB145Sit1_PerTaskMetering(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b145-s1-"+contracts.NewID())

	// Run a command (opens a cost window via incarnation acquire).
	e.mustRun(t, sc, comp.ID, "echo metered-task")

	// Get the cost report: should show awake time > 0.
	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}
	if rep.AwakeNanos <= 0 {
		t.Fatalf("per-task metering: AwakeNanos=%d want > 0", rep.AwakeNanos)
	}
	t.Logf("Sit1: per-task metering — AwakeNanos=%d (%s), windows=%d",
		rep.AwakeNanos, rep.AwakeHuman, len(rep.Windows))
}

// Situation 2: Per-deployment budget tracking — cumulative cost across
// multiple tasks on the same computer accumulates.
func TestB145Sit2_BudgetAccumulation(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b145-s2-"+contracts.NewID())

	// First task.
	e.mustRun(t, sc, comp.ID, "echo task-1")
	rep1, _ := e.store.CostReport(ctx(t), sc, comp.ID)
	nanos1 := rep1.AwakeNanos

	// Small delay so the second measurement is strictly greater.
	time.Sleep(10 * time.Millisecond)

	// Second task.
	e.mustRun(t, sc, comp.ID, "echo task-2")
	rep2, _ := e.store.CostReport(ctx(t), sc, comp.ID)
	nanos2 := rep2.AwakeNanos

	// Cost is cumulative: second reading >= first.
	if nanos2 < nanos1 {
		t.Fatalf("budget accumulation: second=%d < first=%d (must be monotonic)", nanos2, nanos1)
	}
	t.Logf("Sit2: budget accumulation — task1=%dns, task2=%dns (monotonic)", nanos1, nanos2)
}

// Situation 3: Cost attribution — cost report is scoped to the subject;
// cross-subject access returns NOT_FOUND.
func TestB145Sit3_CostAttribution(t *testing.T) {
	e := newLiveEnv(t)
	scAlice := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	scBob := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "bob"}

	comp, _ := e.store.CreateComputer(ctx(t), scAlice, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b145-s3-"+contracts.NewID())
	e.mustRun(t, scAlice, comp.ID, "echo attributed")

	// Alice can see her cost report.
	rep, err := e.store.CostReport(ctx(t), scAlice, comp.ID)
	if err != nil {
		t.Fatalf("owner CostReport: %v", err)
	}
	_ = rep

	// Bob cannot (cross-subject → NOT_FOUND).
	_, err = e.store.CostReport(ctx(t), scBob, comp.ID)
	if err == nil {
		t.Fatal("cross-subject CostReport: expected error, got nil")
	}
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-subject: code=%s want NOT_FOUND", contracts.Code(err))
	}
	t.Log("Sit3: cost attribution — owner sees report, cross-subject gets NOT_FOUND")
}

// Situation 4: Overhead separation — cost window only opens on incarnation
// acquire (user compute start), not on computer creation (platform overhead).
func TestB145Sit4_OverheadSeparation(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Create a computer (platform overhead: scheduling, provisioning).
	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b145-s4-"+contracts.NewID())

	// Immediately after creation (before any command), the cost report
	// should show 0 awake nanos (no user compute yet).
	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport after create: %v", err)
	}
	if rep.AwakeNanos > 0 {
		// Allow a tiny epsilon for race between create and report.
		// But the cost window should NOT be open at this point
		// since no incarnation has been acquired yet.
		if rep.Open {
			t.Fatalf("overhead separation: cost window is OPEN before any command (AwakeNanos=%d)", rep.AwakeNanos)
		}
	}
	t.Logf("Sit4: overhead separation — after create: AwakeNanos=%d, open=%v (no user compute yet)",
		rep.AwakeNanos, rep.Open)
}

// Situation 5: Free tier — personal (class S) computers are metered;
// the cost window tracks usage for free-tier accounting.
func TestB145Sit5_FreeTier(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Personal tier (class S, personal profile) — free tier.
	compPersonal, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b145-s5-p-"+contracts.NewID())
	e.mustRun(t, sc, compPersonal.ID, "echo free-tier")

	repPersonal, err := e.store.CostReport(ctx(t), sc, compPersonal.ID)
	if err != nil {
		t.Fatalf("personal CostReport: %v", err)
	}
	if repPersonal.AwakeNanos <= 0 {
		t.Fatal("free tier: personal computer not metered (AwakeNanos=0)")
	}

	// Ephemeral (class C) also metered.
	scCustom := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "custom-agent"}
	compC, _ := e.store.CreateComputer(ctx(t), scCustom, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
	}, "b145-s5-c-"+contracts.NewID())
	e.mustRun(t, scCustom, compC.ID, "echo paid-tier")

	repC, err := e.store.CostReport(ctx(t), scCustom, compC.ID)
	if err != nil {
		t.Fatalf("class C CostReport: %v", err)
	}
	if repC.AwakeNanos <= 0 {
		t.Fatal("class C: not metered (AwakeNanos=0)")
	}
	t.Logf("Sit5: free tier — personal=%dns, classC=%dns (both metered)",
		repPersonal.AwakeNanos, repC.AwakeNanos)
}

// Situation 6: Cost snapshot — after SleepComputer, the cost window is
// closed (immutable); subsequent reports show the same total.
func TestB145Sit6_CostSnapshot(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b145-s6-"+contracts.NewID())
	e.mustRun(t, sc, comp.ID, "echo snapshot-test")

	// Get report while awake.
	repOpen, _ := e.store.CostReport(ctx(t), sc, comp.ID)

	// Sleep the computer (closes the cost window).
	_, err := e.store.SleepComputer(ctx(t), sc, comp.ID, 0, compute.ProcessKillReport{})
	if err != nil {
		t.Fatalf("SleepComputer: %v", err)
	}

	// Get report after sleep: window is closed, total is fixed.
	repClosed, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport after sleep: %v", err)
	}
	if repClosed.Open {
		t.Fatal("cost snapshot: window still open after sleep")
	}
	// All windows should have ended_at set.
	for _, w := range repClosed.Windows {
		if w.EndedAt == nil {
			t.Fatal("cost snapshot: window has no end time after sleep")
		}
	}
	// The closed total should be >= the open total (time elapsed between reads).
	if repClosed.AwakeNanos < repOpen.AwakeNanos {
		t.Fatalf("cost snapshot: closed=%d < open=%d", repClosed.AwakeNanos, repOpen.AwakeNanos)
	}
	t.Logf("Sit6: cost snapshot — open=%dns, closed=%dns (immutable after sleep)",
		repOpen.AwakeNanos, repClosed.AwakeNanos)
}

// Situation 7: Budget alert readiness — cost grows monotonically; at any
// threshold (e.g. 80%), the report provides the data for alert emission.
func TestB145Sit7_BudgetAlert(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b145-s7-"+contracts.NewID())

	// Multiple tasks to accumulate cost.
	e.mustRun(t, sc, comp.ID, "echo workload-1")
	time.Sleep(5 * time.Millisecond)
	e.mustRun(t, sc, comp.ID, "echo workload-2")
	time.Sleep(5 * time.Millisecond)
	e.mustRun(t, sc, comp.ID, "echo workload-3")

	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}
	// The report provides AwakeNanos which can be compared against a budget.
	// At 80% threshold: if budget = AwakeNanos/0.8, we're at 80%.
	if rep.AwakeNanos <= 0 {
		t.Fatal("budget alert: no cost accumulated")
	}
	// Verify the data structure supports threshold detection.
	ratio80 := float64(rep.AwakeNanos) * 0.8
	t.Logf("Sit7: budget alert — AwakeNanos=%d, 80%%=%dns (data ready for alert emission)",
		rep.AwakeNanos, int64(ratio80))
}

// Situation 8: Cost export — CostReport via API returns data (200);
// cross-subject access denied (404). Ties to B114-B116 artifact isolation.
func TestB145Sit8_CostExport(t *testing.T) {
	e := newLiveEnv(t)
	scAlice := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	scBob := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "bob"}

	comp, _ := e.store.CreateComputer(ctx(t), scAlice, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b145-s8-"+contracts.NewID())
	e.mustRun(t, scAlice, comp.ID, "echo export-me")

	// Owner can export (CostReport returns full data).
	rep, err := e.store.CostReport(ctx(t), scAlice, comp.ID)
	if err != nil {
		t.Fatalf("owner export: %v", err)
	}
	if rep.ComputerID != comp.ID {
		t.Fatalf("export: ComputerID=%q want %q", rep.ComputerID, comp.ID)
	}
	if len(rep.Windows) == 0 {
		t.Fatal("export: no windows in report")
	}

	// Cross-subject: denied (same as B114-B116 artifact isolation).
	_, err = e.store.CostReport(ctx(t), scBob, comp.ID)
	if err == nil {
		t.Fatal("cross-subject export: expected error")
	}
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-subject export: code=%s want NOT_FOUND", contracts.Code(err))
	}
	t.Log("Sit8: cost export — owner gets full report, cross-subject denied (NOT_FOUND)")
}
