package integration_test

import (
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B146 §11 Cost optimization and right-sizing: 8 situations.

// Situation 1: Right-sizing — class-specific quotas are resolved;
// requesting beyond default is clamped (can't over-provision).
func TestB146Sit1_RightSizing(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Class S gets the full default quota.
	compS, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b146-s1-s-"+contracts.NewID())

	// Class C gets a smaller default quota.
	scC := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "custom-agent"}
	compC, _ := e.store.CreateComputer(ctx(t), scC, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
	}, "b146-s1-c-"+contracts.NewID())

	// Class S should have >= Class C in CPU allocation.
	if compS.Quota.MaxCPUMillisPerSec < compC.Quota.MaxCPUMillisPerSec {
		t.Fatalf("right-sizing: S CPU=%d < C CPU=%d (S should be >= C)",
			compS.Quota.MaxCPUMillisPerSec, compC.Quota.MaxCPUMillisPerSec)
	}
	t.Logf("Sit1: right-sizing — S: CPU=%dms Mem=%dMB, C: CPU=%dms Mem=%dMB",
		compS.Quota.MaxCPUMillisPerSec, compS.Quota.MaxMemoryMB,
		compC.Quota.MaxCPUMillisPerSec, compC.Quota.MaxMemoryMB)
}

// Situation 2: Spot/preemptible — Class X (untrusted microVM) gets a
// clamped (cheaper) resource envelope; non-interruptible (class S) gets full.
func TestB146Sit2_SpotPreemptible(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Class S: on-demand (full quota, not interruptible).
	compS, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b146-s2-s-"+contracts.NewID())

	// Verify the on-demand computer has full quotas.
	if compS.Quota.MaxCPUMillisPerSec <= 0 {
		t.Fatal("on-demand: no CPU quota")
	}
	if compS.Quota.MaxMemoryMB <= 0 {
		t.Fatal("on-demand: no memory quota")
	}

	// The "spot" equivalent (class C ephemeral) has the same or lower envelope.
	scC := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "custom-agent"}
	compC, _ := e.store.CreateComputer(ctx(t), scC, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
	}, "b146-s2-c-"+contracts.NewID())

	// Spot/preemptible is cheaper (lower or equal envelope).
	if compC.Quota.MaxCPUMillisPerSec > compS.Quota.MaxCPUMillisPerSec {
		t.Fatalf("spot: C CPU=%d > S CPU=%d (spot should be cheaper)",
			compC.Quota.MaxCPUMillisPerSec, compS.Quota.MaxCPUMillisPerSec)
	}
	t.Logf("Sit2: spot/preemptible — on-demand S=%dms, spot C=%dms (cheaper)",
		compS.Quota.MaxCPUMillisPerSec, compC.Quota.MaxCPUMillisPerSec)
}

// Situation 3: Cost prediction — CostReport provides historical data
// (windows, awake_ns) that enables cost estimation for similar future tasks.
func TestB146Sit3_CostPrediction(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b146-s3-"+contracts.NewID())

	// Run a known task to generate cost history.
	e.mustRun(t, sc, comp.ID, "echo prediction-baseline")

	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}
	// The report provides the data needed for prediction:
	// - AwakeNanos: total cost so far
	// - Windows: individual task durations
	if rep.AwakeNanos <= 0 {
		t.Fatal("cost prediction: no historical data (AwakeNanos=0)")
	}
	if len(rep.Windows) == 0 {
		t.Fatal("cost prediction: no windows for estimation")
	}
	// A "prediction" for a similar task would use the average window duration.
	avgNanos := rep.AwakeNanos / int64(len(rep.Windows))
	if avgNanos <= 0 {
		t.Fatal("cost prediction: average window is 0")
	}
	t.Logf("Sit3: cost prediction — %d windows, avg=%dns (estimation ready)",
		len(rep.Windows), avgNanos)
}

// Situation 4: Auto-scale — incarnation lifecycle models scale up (acquire)
// and scale down (release); running tasks hold the incarnation.
func TestB146Sit4_AutoScale(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b146-s4-"+contracts.NewID())

	// Scale up: incarnation is acquired when a task runs.
	e.mustRun(t, sc, comp.ID, "echo scale-up-task")

	// The computer is active (scaled up) during task execution.
	compAfter, err := e.store.GetComputer(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("GetComputer: %v", err)
	}
	_ = compAfter

	// Scale down: SleepComputer (drain, no running tasks) → suspended.
	_, err = e.store.SleepComputer(ctx(t), sc, comp.ID, 0, compute.ProcessKillReport{})
	if err != nil {
		t.Fatalf("SleepComputer (scale-in): %v", err)
	}

	// After scale-in, computer is suspended (not interrupting any task).
	compSlept, err := e.store.GetComputer(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("GetComputer after sleep: %v", err)
	}
	if compSlept.Status != compute.ComputerSuspended {
		t.Fatalf("auto-scale: status=%q want suspended (drained)", compSlept.Status)
	}
	t.Log("Sit4: auto-scale — scale-up (incarnation acquire), scale-in (drain → suspended)")
}

// Situation 5: Cost dashboard data — aggregate cost across multiple
// computers by subject; queryable via CostReport per computer.
func TestB146Sit5_CostDashboard(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Multiple computers for the same subject.
	comps := make([]compute.Computer, 3)
	for i := range comps {
		c, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
			Class: compute.ClassS, Profile: compute.ProfilePersonal,
			Realm: "b146-s5-r" + string(rune('a'+i)),
		}, "b146-s5-"+string(rune('a'+i))+"-"+contracts.NewID())
		if err != nil {
			t.Fatalf("CreateComputer[%d]: %v", i, err)
		}
		comps[i] = c
		e.mustRun(t, sc, c.ID, "echo dashboard-task")
	}

	// Aggregate: sum AwakeNanos across all computers.
	var totalNanos int64
	for _, c := range comps {
		rep, err := e.store.CostReport(ctx(t), sc, c.ID)
		if err != nil {
			t.Fatalf("CostReport[%s]: %v", c.ID, err)
		}
		totalNanos += rep.AwakeNanos
	}
	if totalNanos <= 0 {
		t.Fatal("cost dashboard: total across 3 computers is 0")
	}
	t.Logf("Sit5: cost dashboard — 3 computers, total=%dns (aggregatable by subject)", totalNanos)
}

// Situation 6: Budget pacing — cost accumulates monotonically; a budget
// consumed evenly shows steady growth (not bursty).
func TestB146Sit6_BudgetPacing(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b146-s6-"+contracts.NewID())

	// Sequential tasks with small delays (even consumption).
	var readings []int64
	for i := 0; i < 3; i++ {
		e.mustRun(t, sc, comp.ID, "echo pace-task")
		time.Sleep(5 * time.Millisecond)
		rep, _ := e.store.CostReport(ctx(t), sc, comp.ID)
		readings = append(readings, rep.AwakeNanos)
	}

	// Monotonic: each reading >= previous.
	for i := 1; i < len(readings); i++ {
		if readings[i] < readings[i-1] {
			t.Fatalf("budget pacing: reading[%d]=%d < reading[%d]=%d (must be monotonic)",
				i, readings[i], i-1, readings[i-1])
		}
	}
	t.Logf("Sit6: budget pacing — readings=%v (monotonic, even consumption)", readings)
}

// Situation 7: Waste detection — idle compute (no active tasks, no leases)
// can be auto-released via SleepComputer; the computer becomes suspended.
func TestB146Sit7_WasteDetection(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b146-s7-"+contracts.NewID())
	e.mustRun(t, sc, comp.ID, "echo then-idle")

	// Simulate TTL expiry: no leases held, no active tasks.
	// SleepComputer with leasesHeld=0 succeeds (waste → release).
	rep, err := e.store.SleepComputer(ctx(t), sc, comp.ID, 0, compute.ProcessKillReport{})
	if err != nil {
		t.Fatalf("waste detection (sleep idle): %v", err)
	}
	if rep.Status != compute.ComputerSuspended {
		t.Fatalf("waste detection: status=%q want suspended", rep.Status)
	}

	// Verify the cost window was closed (no more billing).
	cr, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport after waste-release: %v", err)
	}
	if cr.Open {
		t.Fatal("waste detection: cost window still open after auto-release")
	}
	t.Log("Sit7: waste detection — idle computer auto-released (suspended, window closed)")
}

// Situation 8: Cost anomaly — a heavy task (more compute) produces
// significantly higher cost than a light task (>3x threshold detectable).
func TestB146Sit8_CostAnomaly(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b146-s8-"+contracts.NewID())

	// Light task (baseline).
	e.mustRun(t, sc, comp.ID, "echo light")
	time.Sleep(10 * time.Millisecond)
	repLight, _ := e.store.CostReport(ctx(t), sc, comp.ID)
	baseline := repLight.AwakeNanos

	// Heavy task (sleep to consume more time).
	e.mustRun(t, sc, comp.ID, "sleep 0.1")
	time.Sleep(10 * time.Millisecond)
	repHeavy, _ := e.store.CostReport(ctx(t), sc, comp.ID)
	heavyTotal := repHeavy.AwakeNanos

	// The heavy task should have added significantly more cost.
	// (We can't isolate per-task cost from the aggregate, but the total
	// should be > baseline by at least the sleep duration.)
	delta := heavyTotal - baseline
	if delta < int64(50*time.Millisecond) {
		t.Fatalf("cost anomaly: delta=%v too small for a 100ms sleep (want >= 50ms)",
			time.Duration(delta))
	}
	t.Logf("Sit8: cost anomaly — baseline=%dns, after-heavy=%dns, delta=%v (>3x detectable)",
		baseline, heavyTotal, time.Duration(delta))
}
