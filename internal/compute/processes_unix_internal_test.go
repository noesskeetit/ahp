//go:build unix

package compute

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestKillComputerProcessesProvesTheWholeTreeDead pins the sleep primitive on
// real processes on any POSIX host, which is the substrate's whole promise: the
// command's own process tree is gone and PROVEN gone, including a background job
// the command orphaned and then exited from. The registry entry for that case
// points at a dead leader, so a check on the spawned pid alone would call it
// clean while the orphan keeps running - the failure sleep exists to prevent.
//
// Linux is covered by the /proc-based reaper tests; this file is the same
// assertion on the portable path, so a host that can no longer prove death fails
// here instead of in a live lane.
func TestKillComputerProcessesProvesTheWholeTreeDead(t *testing.T) {
	base := t.TempDir()
	const computer = "comp-sleep-proof"

	// A command that leaves a background job behind and exits: exactly the shape
	// the host backend registers and leaves until sleep.
	out := new(bytes.Buffer)
	leader := exec.Command("/bin/sh", "-c", "sleep 60 >/dev/null 2>&1 & echo $!")
	leader.Stdout = out
	leader.SysProcAttr = &syscall.SysProcAttr{}
	setProcessGroup(leader.SysProcAttr)
	if err := leader.Start(); err != nil {
		t.Fatalf("start the command: %v", err)
	}
	leaderPID := leader.Process.Pid
	if err := leader.Wait(); err != nil {
		t.Fatalf("the launching command should exit cleanly: %v", err)
	}
	orphanPID, err := strconv.Atoi(strings.TrimSpace(out.String()))
	if err != nil || orphanPID <= 0 {
		t.Fatalf("probe did not report the background pid: %q (err=%v)", out.String(), err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-orphanPID, syscall.SIGKILL) })
	if err := assertProcessAlive(orphanPID); err != nil {
		t.Fatalf("the probe left no live background job: %v", err)
	}

	if err := registerProcess(base, computer, leaderPID, leaderPID, "row-sleep-proof"); err != nil {
		t.Fatalf("register the command: %v", err)
	}

	// A stale entry: its process is already gone, and must be reported dead
	// rather than kept or treated as a failure.
	stale := exec.Command("/bin/sh", "-c", "exit 0")
	if err := stale.Start(); err != nil {
		t.Fatalf("start the stale probe: %v", err)
	}
	stalePID := stale.Process.Pid
	_, _ = stale.Process.Wait()
	if err := registerProcess(base, computer, stalePID, stalePID, "row-stale"); err != nil {
		t.Fatalf("register the stale probe: %v", err)
	}

	// An unregistered bystander must survive: the reaper acts on this Computer's
	// registry, not on the machine.
	bystander := exec.Command("/bin/sleep", "60")
	if err := bystander.Start(); err != nil {
		t.Fatalf("start the bystander: %v", err)
	}
	bystanderPID := bystander.Process.Pid
	t.Cleanup(func() {
		_ = syscall.Kill(bystanderPID, syscall.SIGKILL)
		_, _ = bystander.Process.Wait()
	})

	report, err := KillComputerProcesses(base, computer)
	if err != nil {
		t.Fatalf("KillComputerProcesses: %v", err)
	}
	if len(report.Processes) == 0 {
		t.Fatal("sleep report carries no process evidence")
	}
	for _, p := range report.Processes {
		if !p.Dead {
			t.Fatalf("process %d (group %d) not proven dead: %+v", p.PID, p.PGID, p)
		}
	}
	var namedRow bool
	for _, p := range report.Processes {
		if p.CommandID == "row-sleep-proof" {
			namedRow = true
		}
	}
	if !namedRow {
		t.Fatalf("report does not name the ledger row the tree was running: %+v", report.Processes)
	}

	// Independent proof: the orphan the command left behind is gone from the
	// kernel even though its own pid was never registered.
	if err := assertProcessGone(orphanPID); err != nil {
		t.Fatalf("background job %d survived the sleep: %v", orphanPID, err)
	}
	if err := assertProcessAlive(bystanderPID); err != nil {
		t.Fatalf("unregistered bystander was killed: %v", err)
	}
	left, err := os.ReadDir(filepath.Join(base, "procs", computer))
	if err != nil {
		t.Fatalf("read the registry after the kill: %v", err)
	}
	if len(left) != 0 {
		t.Fatalf("registry still holds %d entries after the kill: %v", len(left), left)
	}

	// A second sleep over an empty registry is a no-op, not an error.
	again, err := KillComputerProcesses(base, computer)
	if err != nil || len(again.Processes) != 0 {
		t.Fatalf("second kill over an empty registry: report=%+v err=%v", again, err)
	}
}

// assertProcessAlive checks the kernel still knows pid as a runnable process.
func assertProcessAlive(pid int) error {
	if err := syscall.Kill(pid, 0); err != nil {
		if err == syscall.ESRCH {
			return syscall.ESRCH
		}
		return err
	}
	return nil
}

// assertProcessGone waits for the kernel to stop knowing pid. A reparented
// orphan needs its new parent to reap it first, so the check is a poll, exactly
// as the substrate's own death checks are.
func assertProcessGone(pid int) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); err == syscall.ESRCH {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return syscall.EAGAIN
}
