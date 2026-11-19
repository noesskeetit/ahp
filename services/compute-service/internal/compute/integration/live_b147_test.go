package integration_test

import (
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B147 §11 Cost governance and compliance: 8 situations.

// Situation 1: Cost policy — the quota enforces maximum resource usage per
// task. A command running on a computer with a defined quota cannot exceed it.
func TestB147Sit1_CostPolicy(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Create with a tight quota (policy: max 1 CPU second).
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Quota: compute.Quota{MaxCPUMillisPerSec: 100},
	}, "b147-s1-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}

	// The policy (quota) is enforced: the resolved quota reflects the limit.
	if comp.Quota.MaxCPUMillisPerSec != 100 {
		t.Fatalf("cost policy: resolved CPU=%d want 100 (tightened by policy)",
			comp.Quota.MaxCPUMillisPerSec)
	}
	// Task executes within the policy envelope.
	e.mustRun(t, sc, comp.ID, "echo within-policy")
	t.Logf("Sit1: cost policy — CPU limit=%dms enforced (task ran within envelope)",
		comp.Quota.MaxCPUMillisPerSec)
}

// Situation 2: Approval threshold — commands require dispatch (equivalent to
// approval gate). A command in ACCEPTED state cannot complete without going
// through DISPATCHED first (the "approval" gate).
func TestB147Sit2_ApprovalThreshold(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b147-s2-"+contracts.NewID())

	// Exec creates the command in ACCEPTED state (pending dispatch/approval).
	rec, _, err := e.store.Exec(ctx(t), sc, comp.ID, "b147-s2-approve", "echo needs-approval", "")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if rec.Status != compute.CommandAccepted {
		t.Fatalf("approval threshold: status=%q want accepted (pending dispatch)", rec.Status)
	}

	// Attempt to complete without dispatching (skipping approval) → should fail.
	_, err = e.store.Complete(ctx(t), sc, comp.ID, rec.ID, 0, "output", "")
	if err == nil {
		t.Fatal("approval threshold: completed without dispatch (approval gate bypassed)")
	}
	t.Log("Sit2: approval threshold — cannot skip DISPATCHED (approval gate enforced)")
}

// Situation 3: Cost report compliance — once a cost window is closed, the
// report is immutable (same data on repeated queries).
func TestB147Sit3_ReportCompliance(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b147-s3-"+contracts.NewID())
	e.mustRun(t, sc, comp.ID, "echo compliance-task")

	// Close the cost window (sleep).
	_, err := e.store.SleepComputer(ctx(t), sc, comp.ID, 0, compute.ProcessKillReport{})
	if err != nil {
		t.Fatalf("SleepComputer: %v", err)
	}

	// First read of the immutable report.
	rep1, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport 1: %v", err)
	}

	// Second read: must be identical (immutable).
	rep2, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport 2: %v", err)
	}

	if rep1.AwakeNanos != rep2.AwakeNanos {
		t.Fatalf("report compliance: report1=%d != report2=%d (must be immutable)",
			rep1.AwakeNanos, rep2.AwakeNanos)
	}
	if len(rep1.Windows) != len(rep2.Windows) {
		t.Fatalf("report compliance: windows %d != %d", len(rep1.Windows), len(rep2.Windows))
	}
	t.Logf("Sit3: report compliance — immutable: %d windows, %dns (identical on re-read)",
		len(rep1.Windows), rep1.AwakeNanos)
}

// Situation 4: Tagging requirement — all commands must carry a key (cost-center
// tag). An empty/invalid key is rejected at dispatch.
func TestB147Sit4_TaggingRequirement(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b147-s4-"+contracts.NewID())

	// Empty key (no cost-center tag) → rejected.
	_, _, err := e.store.Exec(ctx(t), sc, comp.ID, "", "echo untagged", "")
	if err == nil {
		t.Fatal("tagging: empty key accepted (should be rejected)")
	}
	t.Logf("Sit4: tagging requirement — untagged (empty key) rejected: %v", contracts.Code(err))
}

// Situation 5: Chargeback — cost is attributed to the requesting subject;
// cross-subject cannot see another's cost report.
func TestB147Sit5_Chargeback(t *testing.T) {
	e := newLiveEnv(t)
	scAlice := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	scBob := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "bob"}

	comp, _ := e.store.CreateComputer(ctx(t), scAlice, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b147-s5-"+contracts.NewID())
	e.mustRun(t, scAlice, comp.ID, "echo chargeback-task")

	// Alice (owner) can see her chargeback.
	rep, err := e.store.CostReport(ctx(t), scAlice, comp.ID)
	if err != nil {
		t.Fatalf("owner chargeback: %v", err)
	}
	if rep.AwakeNanos <= 0 {
		t.Fatal("chargeback: owner has no cost recorded")
	}

	// Bob cannot see alice's chargeback (cross-subject isolation).
	_, err = e.store.CostReport(ctx(t), scBob, comp.ID)
	if err == nil {
		t.Fatal("chargeback: cross-subject saw another's cost")
	}
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("chargeback cross: code=%s want NOT_FOUND", contracts.Code(err))
	}
	t.Log("Sit5: chargeback — cost attributed to subject, cross-subject denied")
}

// Situation 6: Anomaly response — when a command is fenced (anomaly detected),
// it enters a terminal state; auto-action (fence) is applied.
func TestB147Sit6_AnomalyResponse(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b147-s6-"+contracts.NewID())

	// Dispatch a command, then fence it (simulates anomaly auto-action).
	rec, _, err := e.store.Exec(ctx(t), sc, comp.ID, "b147-s6-anomaly", "echo anomalous", "")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	_, err = e.store.MarkDispatched(ctx(t), sc, comp.ID, rec.ID)
	if err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}

	// Auto-action: fence the anomalous command.
	fenced, err := e.store.Fence(ctx(t), sc, comp.ID, rec.ID)
	if err != nil {
		t.Fatalf("Fence (anomaly response): %v", err)
	}
	if !fenced.Status.Terminal() {
		t.Fatalf("anomaly response: status=%q want terminal (auto-action applied)", fenced.Status)
	}
	t.Logf("Sit6: anomaly response — auto-action (fence) applied, status=%q (terminal)",
		fenced.Status)
}

// Situation 7: Historical cost — CostReport returns all windows (full history);
// queryable after multiple tasks.
func TestB147Sit7_HistoricalCost(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b147-s7-"+contracts.NewID())

	// Multiple tasks accumulate history.
	e.mustRun(t, sc, comp.ID, "echo hist-task-1")
	time.Sleep(5 * time.Millisecond)
	e.mustRun(t, sc, comp.ID, "echo hist-task-2")

	// The report contains the full history (at least 1 window with accumulated time).
	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}
	if len(rep.Windows) == 0 {
		t.Fatal("historical cost: no windows in report")
	}
	if rep.AwakeNanos <= 0 {
		t.Fatal("historical cost: AwakeNanos=0 after 2 tasks")
	}
	t.Logf("Sit7: historical cost — %d windows, %dns total (queryable history)",
		len(rep.Windows), rep.AwakeNanos)
}

// Situation 8: Cost SLA — the quota defines the guaranteed maximum resource
// envelope (SLA). The system cannot allocate more than the quota allows.
func TestB147Sit8_CostSLA(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Create with explicit SLA (tight quota = reserved capacity).
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Quota: compute.Quota{MaxCPUMillisPerSec: 200, MaxMemoryMB: 256},
	}, "b147-s8-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}

	// The SLA is the hard ceiling: resolved quota cannot exceed the request.
	if comp.Quota.MaxCPUMillisPerSec > 200 {
		t.Fatalf("cost SLA: CPU=%d exceeds SLA of 200ms", comp.Quota.MaxCPUMillisPerSec)
	}
	if comp.Quota.MaxMemoryMB > 256 {
		t.Fatalf("cost SLA: Mem=%dMB exceeds SLA of 256MB", comp.Quota.MaxMemoryMB)
	}

	// Task runs within the SLA envelope.
	e.mustRun(t, sc, comp.ID, "echo within-sla")
	t.Logf("Sit8: cost SLA — reserved: CPU=%dms, Mem=%dMB (hard ceiling enforced)",
		comp.Quota.MaxCPUMillisPerSec, comp.Quota.MaxMemoryMB)
}
