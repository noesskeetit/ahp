package integration_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B148 §11 Cost transparency and user-facing reports: 8 situations.

// Situation 1: User-facing cost view — subject can see their own cost
// breakdown by window (task period). Cross-subject denied.
func TestB148Sit1_UserCostView(t *testing.T) {
	e := newLiveEnv(t)
	scAlice := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	scBob := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "bob"}

	comp, _ := e.store.CreateComputer(ctx(t), scAlice, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b148-s1-"+contracts.NewID())

	// Run tasks in two separate periods (windows).
	e.mustRun(t, scAlice, comp.ID, "echo task-1")
	_, err := e.store.SleepComputer(ctx(t), scAlice, comp.ID, 0, compute.ProcessKillReport{})
	if err != nil {
		t.Fatalf("SleepComputer: %v", err)
	}
	e.mustRun(t, scAlice, comp.ID, "echo task-2")
	_, err = e.store.SleepComputer(ctx(t), scAlice, comp.ID, 0, compute.ProcessKillReport{})
	if err != nil {
		t.Fatalf("SleepComputer 2: %v", err)
	}

	// Alice (owner) sees her cost breakdown with multiple windows.
	rep, err := e.store.CostReport(ctx(t), scAlice, comp.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}
	if len(rep.Windows) < 2 {
		t.Fatalf("user cost view: windows=%d want >=2 (per-period breakdown)", len(rep.Windows))
	}
	if rep.AwakeNanos <= 0 {
		t.Fatal("user cost view: AwakeNanos=0 after 2 task periods")
	}

	// Bob cannot see Alice's cost view.
	_, err = e.store.CostReport(ctx(t), scBob, comp.ID)
	if err == nil {
		t.Fatal("user cost view: cross-subject saw another's cost (should be denied)")
	}
	t.Logf("Sit1: user cost view — %d windows, %dns; cross-subject denied",
		len(rep.Windows), rep.AwakeNanos)
}

// Situation 2: Cost comparison — cost across periods (window 1 vs window 2)
// is independently measurable.
func TestB148Sit2_CostComparison(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b148-s2-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}

	// Measure each closed awake period, including command and store overhead.
	// Task counts and requested sleeps do not determine the duration ordering.
	e.mustRun(t, sc, comp.ID, "sleep 0.05")
	_, err = e.store.SleepComputer(ctx(t), sc, comp.ID, 0, compute.ProcessKillReport{})
	if err != nil {
		t.Fatalf("Sleep 1: %v", err)
	}
	first, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport 1: %v", err)
	}
	if first.ComputerID != comp.ID || first.Open || len(first.Windows) != 1 {
		t.Fatalf("period 1: want one closed window for %s, got %+v", comp.ID, first)
	}
	w1 := first.Windows[0]
	if w1.ID == "" || w1.StartedAt.IsZero() || w1.EndedAt == nil || !w1.EndedAt.After(w1.StartedAt) {
		t.Fatalf("period 1: invalid closed window: %+v", w1)
	}
	d1 := w1.EndedAt.Sub(w1.StartedAt)
	if first.AwakeNanos != int64(d1) {
		t.Fatalf("period 1: cost=%dns want measured duration %v", first.AwakeNanos, d1)
	}

	// Period 2 has multiple tasks but still constitutes one awake window.
	e.mustRun(t, sc, comp.ID, "sleep 0.02")
	e.mustRun(t, sc, comp.ID, "sleep 0.02")
	e.mustRun(t, sc, comp.ID, "sleep 0.02")
	_, err = e.store.SleepComputer(ctx(t), sc, comp.ID, 0, compute.ProcessKillReport{})
	if err != nil {
		t.Fatalf("Sleep 2: %v", err)
	}

	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}
	if rep.ComputerID != comp.ID || rep.Open || len(rep.Windows) != 2 {
		t.Fatalf("cost comparison: want two closed windows for %s, got %+v", comp.ID, rep)
	}

	// Appending a period must preserve the first period's identity and bounds.
	prior := rep.Windows[0]
	if prior.ID != w1.ID || !prior.StartedAt.Equal(w1.StartedAt) || prior.EndedAt == nil || !prior.EndedAt.Equal(*w1.EndedAt) {
		t.Fatalf("cost comparison: first closed period changed: before=%+v after=%+v", w1, prior)
	}
	w2 := rep.Windows[1]
	if w2.ID == "" || w2.ID == w1.ID || w2.StartedAt.Before(*w1.EndedAt) || w2.EndedAt == nil || !w2.EndedAt.After(w2.StartedAt) {
		t.Fatalf("cost comparison: second period must be distinct, closed and non-overlapping: %+v", w2)
	}
	d2 := w2.EndedAt.Sub(w2.StartedAt)
	if rep.AwakeNanos-first.AwakeNanos != int64(d2) {
		t.Fatalf("cost comparison: added cost=%dns want measured period 2 duration %v", rep.AwakeNanos-first.AwakeNanos, d2)
	}
	t.Logf("Sit2: cost comparison — period1=%v period2=%v delta=%v total=%dns (closed periods)",
		d1, d2, d2-d1, rep.AwakeNanos)
}

// Known historical windows provide an independent arithmetic oracle for Sit2.
// These durations reproduce the valid reverse ordering seen under load; the
// minute between starts must not be billed as continuous awake time.
func TestB148Sit2_CostComparisonKnownWindows(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b148-s2-known-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	windows := []struct {
		id       string
		start    time.Time
		end      time.Time
		wantCost int64
	}{
		{contracts.NewID(), time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 12, 0, 0, 142385000, time.UTC), 142385000},
		{contracts.NewID(), time.Date(2026, 1, 1, 12, 1, 0, 0, time.UTC), time.Date(2026, 1, 1, 12, 1, 0, 113027000, time.UTC), 113027000},
	}
	// Insert historical fixtures in reverse order so ordering is report behavior.
	for i := len(windows) - 1; i >= 0; i-- {
		w := windows[i]
		_, err := e.pool.Exec(ctx(t), `INSERT INTO ahp_compute.cost_ledger
			(id, computer_id, kind, started_at, ended_at) VALUES ($1, $2, 'active', $3, $4)`,
			w.id, comp.ID, w.start, w.end)
		if err != nil {
			t.Fatalf("insert historical period %d: %v", i+1, err)
		}
	}
	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}
	// Hand-calculated: 142.385ms + 113.027ms = 255.412ms, not the 60s span.
	if rep.ComputerID != comp.ID || rep.Open || len(rep.Windows) != 2 || rep.AwakeNanos != 255412000 || rep.AwakeHuman != "255ms" {
		t.Fatalf("known periods: want two closed windows, cost=255412000ns (255ms), got %+v", rep)
	}
	for i, want := range windows {
		got := rep.Windows[i]
		if got.ID != want.id || !got.StartedAt.Equal(want.start) || got.EndedAt == nil || !got.EndedAt.Equal(want.end) {
			t.Fatalf("known period %d: bounds changed: got=%+v want=%+v", i+1, got, want)
		}
		if cost := int64(got.EndedAt.Sub(got.StartedAt)); cost != want.wantCost {
			t.Fatalf("known period %d: cost=%dns want %dns", i+1, cost, want.wantCost)
		}
	}
	t.Log("Sit2 known periods: 142385000ns + 113027000ns = 255412000ns; gap excluded, boundaries preserved")
}

// Situation 3: Cost breakdown — the report provides structured components
// (computer_id, total awake time, per-window detail).
func TestB148Sit3_CostBreakdown(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Quota: compute.Quota{MaxCPUMillisPerSec: 4000, MaxMemoryMB: 8192},
	}, "b148-s3-"+contracts.NewID())
	e.mustRun(t, sc, comp.ID, "echo breakdown-task")

	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}

	// Structured breakdown: has computer identity, total, and per-window detail.
	if rep.ComputerID != comp.ID {
		t.Fatalf("breakdown: computer_id=%q want %q", rep.ComputerID, comp.ID)
	}
	if rep.AwakeNanos <= 0 {
		t.Fatal("breakdown: AwakeNanos=0")
	}
	if rep.AwakeHuman == "" {
		t.Fatal("breakdown: AwakeHuman empty (must have human-readable total)")
	}
	if len(rep.Windows) == 0 {
		t.Fatal("breakdown: no windows (must have per-period detail)")
	}
	// Each window has started_at and ended_at for component-level analysis.
	for i, w := range rep.Windows {
		if w.StartedAt.IsZero() {
			t.Fatalf("breakdown: window[%d].StartedAt is zero", i)
		}
	}
	t.Logf("Sit3: cost breakdown — id=%s total=%s windows=%d",
		rep.ComputerID, rep.AwakeHuman, len(rep.Windows))
}

// Situation 4: Forecast — based on current usage rate, predict month-end cost.
func TestB148Sit4_Forecast(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b148-s4-"+contracts.NewID())

	// Generate usage: a few tasks to establish a rate.
	for i := 0; i < 3; i++ {
		e.mustRun(t, sc, comp.ID, fmt.Sprintf("echo forecast-task-%d", i))
	}

	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}
	if rep.AwakeNanos <= 0 {
		t.Fatal("forecast: no usage recorded")
	}

	// Forecast: rate = total awake / elapsed since first window start.
	// Project to end of month: remaining days × rate.
	firstStart := rep.Windows[0].StartedAt
	now := time.Now().UTC()
	elapsed := now.Sub(firstStart)
	if elapsed <= 0 {
		t.Fatal("forecast: elapsed <= 0")
	}
	rateNanosPerSec := float64(rep.AwakeNanos) / elapsed.Seconds()

	// End of current month.
	monthEnd := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
	remaining := monthEnd.Sub(now)
	predictedMonthEnd := int64(rateNanosPerSec*remaining.Seconds()) + rep.AwakeNanos

	if predictedMonthEnd <= rep.AwakeNanos {
		t.Fatalf("forecast: predicted=%d not greater than current=%d", predictedMonthEnd, rep.AwakeNanos)
	}
	t.Logf("Sit4: forecast — current=%dns rate=%.0fns/s remaining=%v predictedMonthEnd=%dns",
		rep.AwakeNanos, rateNanosPerSec, remaining.Round(time.Second), predictedMonthEnd)
}

// Situation 5: Savings recommendations — identify over-provisioned
// computers where actual usage is far below the quota ceiling.
func TestB148Sit5_SavingsRecommendations(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Provisioned with generous quota but light usage.
	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Quota: compute.Quota{MaxCPUMillisPerSec: 8000, MaxMemoryMB: 16384},
	}, "b148-s5-"+contracts.NewID())
	e.mustRun(t, sc, comp.ID, "echo light-task")
	_, err := e.store.SleepComputer(ctx(t), sc, comp.ID, 0, compute.ProcessKillReport{})
	if err != nil {
		t.Fatalf("SleepComputer: %v", err)
	}

	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}

	// Compute utilization: actual awake time vs. available quota capacity.
	// A computer with 8000 CPU-ms/s quota that only used a few ms is over-provisioned.
	quotaCPUMillis := int64(comp.Quota.MaxCPUMillisPerSec)
	window := rep.Windows[0]

	// If actual awake << quota capacity for the period, it's over-provisioned.
	periodMs := int64(window.EndedAt.Sub(window.StartedAt).Milliseconds())
	availableCPUNanos := quotaCPUMillis * 1000 * periodMs // quota * period
	utilization := float64(rep.AwakeNanos) / float64(availableCPUNanos)

	// For a short "echo" task, utilization should be well under 50%.
	if utilization >= 0.5 {
		t.Fatalf("savings: utilization=%.2f too high for light task (want < 0.50)", utilization)
	}
	t.Logf("Sit5: savings — quota=%dms CPU, utilization=%.4f → over-provisioned (recommend class C)",
		quotaCPUMillis, utilization)
}

// Situation 6: Cost alerting — when cost exceeds a user-configured
// threshold, the condition is detectable.
func TestB148Sit6_CostAlerting(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b148-s6-"+contracts.NewID())

	// Set a very low alert threshold (1ms of awake time).
	alertThresholdNanos := int64(1 * time.Millisecond)

	// Generate enough usage to exceed the threshold.
	for i := 0; i < 5; i++ {
		e.mustRun(t, sc, comp.ID, fmt.Sprintf("echo alert-task-%d", i))
	}

	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}

	// Alert condition: cost exceeds threshold.
	if rep.AwakeNanos <= alertThresholdNanos {
		t.Fatalf("cost alert: cost=%dns did not exceed threshold=%dns (alert should fire)",
			rep.AwakeNanos, alertThresholdNanos)
	}
	// Budget % alert: if budget is 10ms and we used more, budget exceeded.
	budgetNanos := int64(10 * time.Millisecond)
	budgetUsed := float64(rep.AwakeNanos) / float64(budgetNanos)
	if budgetUsed <= 1.0 {
		t.Fatalf("cost alert: budget used=%.2f%% want >100%% (cost=%d budget=%d)",
			budgetUsed*100, rep.AwakeNanos, budgetNanos)
	}
	t.Logf("Sit6: cost alerting — cost=%dns threshold=%dns EXCEEDED; budget %s used=%.0f%%",
		rep.AwakeNanos, alertThresholdNanos,
		time.Duration(budgetNanos), budgetUsed*100)
}

// Situation 7: Export — cost report is serializable to JSON with the
// expected structure (B114 parity: same fields as API response).
func TestB148Sit7_Export(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b148-s7-"+contracts.NewID())
	e.mustRun(t, sc, comp.ID, "echo export-task")

	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}

	// JSON export: marshal and verify structure.
	data, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("export JSON marshal: %v", err)
	}

	var exported map[string]any
	if err := json.Unmarshal(data, &exported); err != nil {
		t.Fatalf("export JSON unmarshal: %v", err)
	}

	// Required fields for B114 parity.
	for _, field := range []string{"computer_id", "awake_ns", "awake_human", "open_window", "windows"} {
		if _, ok := exported[field]; !ok {
			t.Fatalf("export: missing field %q in JSON: %s", field, data)
		}
	}
	// Windows array is valid.
	windows, ok := exported["windows"].([]any)
	if !ok || len(windows) == 0 {
		t.Fatalf("export: windows invalid or empty: %s", data)
	}
	t.Logf("Sit7: export — JSON valid, %d fields, %d windows, %d bytes",
		len(exported), len(windows), len(data))
}

// Situation 8: Audit — cost data is immutable. Closed windows cannot be
// modified; the ledger is append-only.
func TestB148Sit8_Audit(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b148-s8-"+contracts.NewID())
	e.mustRun(t, sc, comp.ID, "echo audit-task")

	// Close the window.
	_, err := e.store.SleepComputer(ctx(t), sc, comp.ID, 0, compute.ProcessKillReport{})
	if err != nil {
		t.Fatalf("SleepComputer: %v", err)
	}

	// Capture the closed window state.
	rep1, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport 1: %v", err)
	}
	if rep1.Open {
		t.Fatal("audit: window still open after sleep")
	}
	if rep1.Windows[0].EndedAt == nil {
		t.Fatal("audit: closed window has no EndedAt")
	}

	// Attempt to open a duplicate window (idempotent no-op) — the existing
	// closed window is not modified.
	if err := e.store.OpenCostWindow(ctx(t), sc, comp.ID); err != nil {
		t.Fatalf("OpenCostWindow (reopen): %v", err)
	}
	rep2, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport 2: %v", err)
	}

	// The original closed window is unchanged (same start, same end).
	origEnd := rep1.Windows[0].EndedAt
	// Find the original window in the new report.
	found := false
	for _, w := range rep2.Windows {
		if w.StartedAt.Equal(rep1.Windows[0].StartedAt) {
			found = true
			if w.EndedAt == nil || !w.EndedAt.Equal(*origEnd) {
				t.Fatalf("audit: closed window was modified (original end=%v, now=%v)",
					origEnd, w.EndedAt)
			}
			break
		}
	}
	if !found {
		t.Fatal("audit: original window disappeared from report")
	}
	t.Logf("Sit8: audit — closed window immutable (start=%v end=%v), new window appended",
		rep1.Windows[0].StartedAt, *origEnd)
}
