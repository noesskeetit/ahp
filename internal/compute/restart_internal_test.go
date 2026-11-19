//go:build linux

package compute

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestStartupReaperKillsRegisteredGroupsAndProvesThemDead pins the recovery
// primitive on real processes: a group registered under a Computer is killed,
// verified gone (ESRCH) and unregistered; a stale registry entry whose process
// is already gone is reported dead too; Computers are reaped in a stable order
// and a process that is NOT registered is left alone.
func TestStartupReaperKillsRegisteredGroupsAndProvesThemDead(t *testing.T) {
	base := t.TempDir()

	// A live command tree: a group leader with its own process group, exactly
	// as the host backend spawns it.
	live := exec.Command("/bin/sleep", "60")
	live.SysProcAttr = &syscall.SysProcAttr{}
	setProcessGroup(live.SysProcAttr)
	if err := live.Start(); err != nil {
		t.Fatalf("start the probe process: %v", err)
	}
	livePID := live.Process.Pid
	t.Cleanup(func() {
		_ = syscall.Kill(-livePID, syscall.SIGKILL)
		_, _ = live.Process.Wait()
	})
	if err := registerProcess(base, "comp-live", livePID, livePID, "step-live-cmd"); err != nil {
		t.Fatalf("register the live probe: %v", err)
	}

	// A process that is registered but already exited: the entry is stale and
	// must be reported dead, not kept.
	gone := exec.Command("/bin/true")
	if err := gone.Run(); err != nil {
		t.Fatalf("run the short-lived probe: %v", err)
	}
	gonePID := gone.Process.Pid
	if err := registerProcess(base, "comp-stale", gonePID, gonePID, "step-stale-cmd"); err != nil {
		t.Fatalf("register the exited probe: %v", err)
	}

	// An unregistered sibling of the live process must survive the reaping: the
	// reaper acts on the registry, not on the whole machine.
	bystander := exec.Command("/bin/sleep", "60")
	if err := bystander.Start(); err != nil {
		t.Fatalf("start the bystander: %v", err)
	}
	bystanderPID := bystander.Process.Pid
	t.Cleanup(func() {
		_ = syscall.Kill(bystanderPID, syscall.SIGKILL)
		_, _ = bystander.Process.Wait()
	})

	reap, err := ReapProcessesAtStartup(base)
	if err != nil {
		t.Fatalf("ReapProcessesAtStartup: %v", err)
	}
	if len(reap.Computers) != 2 {
		t.Fatalf("reaped %d computer(s), want the 2 that had registry entries: %+v", len(reap.Computers), reap.Computers)
	}
	if reap.Computers[0].ComputerID != "comp-live" || reap.Computers[1].ComputerID != "comp-stale" {
		t.Fatalf("reaping order is not stable: %s, %s", reap.Computers[0].ComputerID, reap.Computers[1].ComputerID)
	}
	if reap.Survivors() != nil {
		t.Fatalf("process(es) survived the reaper: %+v", reap.Survivors())
	}
	if got := reap.ReapedProcs(); got < 2 {
		t.Fatalf("ReapedProcs()=%d, want at least the two registered probes", got)
	}
	// The live probe is this test's own child, so it cannot leave the pid table
	// until this test waits for it: being a zombie is the honest proof that it
	// stopped executing, and the report must say so rather than claim a clean
	// disappearance or - worse - a survivor.
	if gone, zombie := procNotRunning(livePID); !gone || !zombie {
		t.Fatalf("the registered process is still executing after reaping: gone=%v zombie=%v", gone, zombie)
	}
	if p := findKilled(reap, livePID); p == nil || !p.Dead || !p.Zombie {
		t.Fatalf("the report for the killed group leader is not honest about it being a zombie: %+v", p)
	}
	if p := findKilled(reap, gonePID); p == nil || !p.Dead || p.Zombie {
		t.Fatalf("the report for the already-exited probe: %+v (want dead, not zombie)", p)
	}
	// The reaper names the ledger row each killed process belonged to, so an
	// operator reconciling after the crash knows exactly which dispatched command
	// has an unknown outcome, not merely which pid died.
	if p := findKilled(reap, livePID); p.CommandID != "step-live-cmd" {
		t.Fatalf("the reaped live process does not name its ledger row: got %q, want step-live-cmd", p.CommandID)
	}
	if p := findKilled(reap, gonePID); p.CommandID != "step-stale-cmd" {
		t.Fatalf("the reaped stale entry does not name its ledger row: got %q, want step-stale-cmd", p.CommandID)
	}
	if got := reap.UnknownOutcomeCommands(); len(got) != 2 || got[0] != "step-live-cmd" || got[1] != "step-stale-cmd" {
		t.Fatalf("UnknownOutcomeCommands()=%v, want [step-live-cmd step-stale-cmd]", got)
	}
	if err := syscall.Kill(bystanderPID, 0); err != nil {
		t.Fatalf("the reaper killed a process that was not in the registry: kill(%d,0)=%v", bystanderPID, err)
	}
	for _, id := range []string{"comp-live", "comp-stale"} {
		entries, err := os.ReadDir(filepath.Join(base, "procs", id))
		if err != nil {
			t.Fatalf("read the registry of %s after reaping: %v", id, err)
		}
		if len(entries) != 0 {
			t.Fatalf("%s still has %d registry entr(ies) after reaping", id, len(entries))
		}
	}
}

// TestStartupReaperWithoutRegistryIsANoOp is the normal start of a healthy
// node: no registry directory at all must not be an error, and an empty jail
// base must not make the executor scan the filesystem root.
func TestStartupReaperWithoutRegistryIsANoOp(t *testing.T) {
	reap, err := ReapProcessesAtStartup(t.TempDir())
	if err != nil {
		t.Fatalf("a node with no registry must start clean: %v", err)
	}
	if len(reap.Computers) != 0 || reap.ReapedProcs() != 0 || reap.Survivors() != nil {
		t.Fatalf("an empty registry produced evidence: %+v", reap)
	}
	if _, err := ReapProcessesAtStartup(""); err != nil {
		t.Fatalf("an unset jail base must be a no-op, got %v", err)
	}
}

// TestStartupReaperReportsTheRegistryItReads proves the reaper only looks at
// registry entries (pid -> pgid) and trusts the recorded pgid, so a group that
// grew a member after registration is still killed as a group.
func TestStartupReaperReportsTheRegistryItReads(t *testing.T) {
	base := t.TempDir()
	cmd := exec.Command("/bin/sh", "-c", "sleep 60 & sleep 60; wait")
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	setProcessGroup(cmd.SysProcAttr)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the group probe: %v", err)
	}
	leader := cmd.Process.Pid
	t.Cleanup(func() {
		_ = syscall.Kill(-leader, syscall.SIGKILL)
		_, _ = cmd.Process.Wait()
	})
	// Record the pgid explicitly (the same value here, but through the file the
	// reaper actually reads).
	dir := filepath.Join(base, "procs", "comp-group")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, strconv.Itoa(leader)), []byte(strconv.Itoa(leader)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Wait for the group to actually grow: reaping a group that has not forked
	// yet would only ever enumerate its leader.
	deadline := time.Now().Add(10 * time.Second)
	for len(livePIDsInGroup(leader)) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the probe group never grew past its leader: this test cannot show group-wide reaping")
		}
		time.Sleep(20 * time.Millisecond)
	}

	reap, err := ReapProcessesAtStartup(base)
	if err != nil {
		t.Fatalf("ReapProcessesAtStartup: %v", err)
	}
	if len(reap.Computers) != 1 || len(reap.Computers[0].Report.Processes) < 2 {
		t.Fatalf("the reaper did not enumerate the live group members: %+v", reap.Computers)
	}
	if reap.Survivors() != nil {
		t.Fatalf("group members survived: %+v", reap.Survivors())
	}
}

// procNotRunning reports whether pid can no longer execute anything: the kernel
// has forgotten the pid, or the process is a zombie that only waits to be
// reaped by its parent.
func procNotRunning(pid int) (gone bool, zombie bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return true, false
	}
	s := string(b)
	i := strings.LastIndex(s, ")")
	if i < 0 {
		return true, false
	}
	fields := strings.Fields(s[i+2:])
	if len(fields) == 0 {
		return true, false
	}
	// Anything but Z means the process is still executing.
	if fields[0] != "Z" {
		return false, false
	}
	return true, true
}

func findKilled(reap StartupReap, pid int) *ProcessKill {
	for i := range reap.Computers {
		for j := range reap.Computers[i].Report.Processes {
			if reap.Computers[i].Report.Processes[j].PID == pid {
				return &reap.Computers[i].Report.Processes[j]
			}
		}
	}
	return nil
}

// TestCommandKeyForStepIsThePositionNotTheProcess pins that the command key is a
// function of the run and the step in it: the same position always derives the same
// key, another position never does, and takeover coordinates (which node, which
// incarnation) are not part of it - a key that changed on takeover would be a new key
// in exactly the case that must not repeat an external effect.
func TestCommandKeyForStepIsThePositionNotTheProcess(t *testing.T) {
	first := CommandKeyForStep("run-1", 3)
	if again := CommandKeyForStep("run-1", 3); again != first {
		t.Fatalf("the same position derived two keys: %q then %q", first, again)
	}
	for _, other := range []struct {
		run  string
		step int
	}{{"run-2", 3}, {"run-1", 4}, {"run-1", 0}} {
		if got := CommandKeyForStep(other.run, other.step); got == first {
			t.Fatalf("position %+v collides with the key of (run-1,3)", other)
		}
	}
	if err := validIdentifier(first, "idempotency key"); err != nil {
		t.Fatalf("the derived key is not a legal command key: %v", err)
	}
}

// TestReaperOmitsCommandlessProcessesFromUnknownOutcomeList pins the boundary of
// the naming feature: a process registered for a ledger command is named by that
// row when the reaper proves it dead, while a process registered for no command
// (an interactive shell, an environment install) is still proven dead but is
// never invented into a command row - an unknown command id stays unknown.
func TestReaperOmitsCommandlessProcessesFromUnknownOutcomeList(t *testing.T) {
	base := t.TempDir()

	spawn := func(computerID, commandID string) int {
		p := exec.Command("/bin/sleep", "60")
		p.SysProcAttr = &syscall.SysProcAttr{}
		setProcessGroup(p.SysProcAttr)
		if err := p.Start(); err != nil {
			t.Fatalf("start probe for %s: %v", computerID, err)
		}
		pid := p.Process.Pid
		t.Cleanup(func() {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			_, _ = p.Process.Wait()
		})
		if err := registerProcess(base, computerID, pid, pid, commandID); err != nil {
			t.Fatalf("register probe for %s: %v", computerID, err)
		}
		return pid
	}

	named := spawn("comp-named", "step-named-cmd")
	commandless := spawn("comp-shell", "")

	reap, err := ReapProcessesAtStartup(base)
	if err != nil {
		t.Fatalf("ReapProcessesAtStartup: %v", err)
	}
	if reap.Survivors() != nil {
		t.Fatalf("process(es) survived the reaper: %+v", reap.Survivors())
	}
	if p := findKilled(reap, named); p == nil || p.CommandID != "step-named-cmd" {
		t.Fatalf("the named process is not reported with its ledger row: %+v", p)
	}
	if p := findKilled(reap, commandless); p == nil || p.CommandID != "" || !p.Dead {
		t.Fatalf("a commandless process must be proven dead with no invented row: %+v", p)
	}
	if got := reap.UnknownOutcomeCommands(); len(got) != 1 || got[0] != "step-named-cmd" {
		t.Fatalf("UnknownOutcomeCommands()=%v, want exactly [step-named-cmd]", got)
	}
}

// TestStartupReaperNamesEveryMemberOfAGrownGroup reaches the naming path the
// single-process reaping tests cannot: a command whose process group grew an
// orphaned member (a background job outliving the command). The extra members are
// enumerated from /proc, not read from a registry file of their own, so nothing
// but the shared entry's recorded command id can give them a row. The reaper must
// name the SAME dispatched row on every member it kills, and UnknownOutcomeCommands
// must report that row exactly once however many pids of the one command it reaped.
func TestStartupReaperNamesEveryMemberOfAGrownGroup(t *testing.T) {
	base := t.TempDir()
	const rowID = "step-grown-group-cmd"

	cmd := exec.Command("/bin/sh", "-c", "sleep 60 & sleep 60; wait")
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	setProcessGroup(cmd.SysProcAttr)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the group probe: %v", err)
	}
	leader := cmd.Process.Pid
	t.Cleanup(func() {
		_ = syscall.Kill(-leader, syscall.SIGKILL)
		_, _ = cmd.Process.Wait()
	})
	if err := registerProcess(base, "comp-grown", leader, leader, rowID); err != nil {
		t.Fatalf("register the group leader with its row: %v", err)
	}

	// The group must actually have grown before reaping, else only the leader is
	// enumerated and the naming of /proc-sourced members is never exercised.
	deadline := time.Now().Add(10 * time.Second)
	for len(livePIDsInGroup(leader)) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the probe group never grew past its leader: cannot demonstrate group-wide naming")
		}
		time.Sleep(20 * time.Millisecond)
	}

	reap, err := ReapProcessesAtStartup(base)
	if err != nil {
		t.Fatalf("ReapProcessesAtStartup: %v", err)
	}
	if reap.Survivors() != nil {
		t.Fatalf("group members survived the reaper: %+v", reap.Survivors())
	}
	if len(reap.Computers) != 1 {
		t.Fatalf("expected exactly one reaped computer, got %d: %+v", len(reap.Computers), reap.Computers)
	}
	procs := reap.Computers[0].Report.Processes
	if len(procs) < 2 {
		t.Fatalf("the reaper did not enumerate the whole grown group: %+v", procs)
	}
	for _, p := range procs {
		if p.CommandID != rowID {
			t.Fatalf("reaped member pid %d (pgid %d) does not name the row: got %q, want %q", p.PID, p.PGID, p.CommandID, rowID)
		}
	}
	if got := reap.UnknownOutcomeCommands(); len(got) != 1 || got[0] != rowID {
		t.Fatalf("UnknownOutcomeCommands()=%v, want exactly [%s] despite %d killed pids", got, rowID, len(procs))
	}
}
