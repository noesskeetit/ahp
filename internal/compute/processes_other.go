//go:build unix && !linux

package compute

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// setProcessGroup gives the spawned process its own process group. setpgid is
// POSIX, so the isolation the Linux target runtime relies on is available here
// too; without it a Computer's children would share the service's own group and
// a sleep could not signal them without signalling the service itself.
func setProcessGroup(cmdSysProcAttr *syscall.SysProcAttr) {
	cmdSysProcAttr.Setpgid = true
}

// KillComputerProcesses signals SIGKILL to every process group registered for
// the Computer and proves each one actually gone.
//
// The proof is the group, not the spawned pid: kill(-pgid, 0) returning ESRCH
// means the kernel has no member of that process group left. That matters
// because a command's entry stays in the registry after the command itself
// exits - the background jobs it orphaned are still in the group and are exactly
// what sleep must not leave alive. Probing the spawned pid would report a dead
// leader and a surviving orphan as a clean sleep. Linux enumerates /proc to name
// every live member; this platform has no /proc, so the evidence names the pid
// that was registered and carries the group-level proof, and the zombie flag
// stays unset (killed children are reaped by the executor that waited for them,
// so a zombie leaves the pid table inside the polling window).
//
// A group is only signalled when the entry says the registered pid IS that
// group's leader (pgid == pid), which is how the host backend writes every
// entry. An entry naming some other group is never signalled as a group - that
// would kill processes this Computer did not start - it is signalled as a single
// process, and its fate is reported as such.
func KillComputerProcesses(base, computerID string) (ProcessKillReport, error) {
	dir := filepath.Join(base, "procs", computerID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return ProcessKillReport{}, nil
		}
		return ProcessKillReport{}, err
	}
	var report ProcessKillReport
	for _, e := range entries {
		spawned, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		pgid, commandID := readProcessEntry(filepath.Join(dir, e.Name()), spawned)
		ownGroup := pgid == spawned
		if ownGroup {
			if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
				return ProcessKillReport{}, fmt.Errorf("compute: signal process group %d: %w", pgid, err)
			}
		} else if err := syscall.Kill(spawned, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			return ProcessKillReport{}, fmt.Errorf("compute: signal process %d: %w", spawned, err)
		}
		report.Processes = append(report.Processes, ProcessKill{
			PID:       spawned,
			PGID:      pgid,
			CommandID: commandID,
			Dead:      killedProcessIsGone(pgid, spawned, ownGroup),
		})
		unregisterProcess(base, computerID, spawned)
	}
	return report, nil
}

// killedProcessIsGone waits for the evidence that what was signalled can no
// longer execute anything: for a group this Computer owns, the group must have
// no members left (kill(-pgid, 0) -> ESRCH); otherwise the single signalled pid
// must be unknown to the kernel. Anything still there after the window is a
// survivor the caller has to surface, never swallow.
func killedProcessIsGone(pgid, pid int, ownGroup bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ownGroup {
			if err := syscall.Kill(-pgid, 0); err == syscall.ESRCH {
				return true
			}
		} else if err := syscall.Kill(pid, 0); err == syscall.ESRCH {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
