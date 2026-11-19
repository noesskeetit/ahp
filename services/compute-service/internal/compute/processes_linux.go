//go:build linux

package compute

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// setpgid gives the spawned process its own process group so a Computer's
// whole process tree (including backgrounded children) can be signalled as
// one unit at sleep time.
func setProcessGroup(cmdSysProcAttr *syscall.SysProcAttr) {
	cmdSysProcAttr.Setpgid = true
}

// KillComputerProcesses signals SIGKILL to every process group registered for
// the Computer and verifies each process is actually gone (kill(pid, 0)
// returning ESRCH). Before signalling it enumerates the group's live members
// via /proc, so the evidence lists the processes that were actually running
// (including background jobs orphaned by finished commands), not only the
// originally spawned pid.
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
		members := livePIDsInGroup(pgid)
		if len(members) == 0 {
			// Group already empty (the command exited and orphaned nothing):
			// report the spawned pid, which is trivially gone.
			dead, zombie := processFate(spawned)
			report.Processes = append(report.Processes,
				ProcessKill{PID: spawned, PGID: pgid, CommandID: commandID, Dead: dead, Zombie: zombie})
			unregisterProcess(base, computerID, spawned)
			continue
		}
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		for _, m := range members {
			dead, zombie := processFate(m)
			report.Processes = append(report.Processes,
				ProcessKill{PID: m, PGID: pgid, CommandID: commandID, Dead: dead, Zombie: zombie})
		}
		unregisterProcess(base, computerID, spawned)
	}
	return report, nil
}

// livePIDsInGroup lists the pids of the processes currently in the process
// group pgid, parsed from /proc/<pid>/stat.
func livePIDsInGroup(pgid int) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var out []int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		if pgrpOf(pid) == pgid {
			out = append(out, pid)
		}
	}
	return out
}

// pgrpOf reads field 5 (pgrp) of /proc/<pid>/stat. The comm field can contain
// spaces and parentheses, so parsing starts after the last ')'.
func pgrpOf(pid int) int {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0
	}
	s := string(b)
	i := strings.LastIndex(s, ")")
	if i < 0 || i+2 >= len(s) {
		return 0
	}
	fields := strings.Fields(s[i+2:])
	// fields: state(3) ppid(4) pgrp(5) ...
	if len(fields) < 3 {
		return 0
	}
	v, err := strconv.Atoi(fields[2])
	if err != nil {
		return 0
	}
	return v
}

// processFate waits until pid can no longer execute anything and reports how
// that was proven: either the kernel stopped knowing the pid at all (ESRCH), or
// the process is a zombie - killed, still in the pid table only because its
// parent has not waited for it yet.
//
// Treating a zombie as alive would make a reaper that did its job report a
// failure: the process holds a task struct, not a running command, and can no
// longer touch the Computer's files. Treating it as merely gone would hide
// which of the two happened, so the report keeps the flag.
func processFate(pid int) (dead bool, zombie bool) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); err == syscall.ESRCH {
			return true, false
		}
		if procIsZombie(pid) {
			return true, true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false, false
}

// procIsZombie reads the state field of /proc/<pid>/stat ('Z' = zombie).
func procIsZombie(pid int) bool {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	s := string(b)
	i := strings.LastIndex(s, ")")
	if i < 0 || i+2 >= len(s) {
		return false
	}
	fields := strings.Fields(s[i+2:])
	// fields: state(3) ppid(4) pgrp(5) ...
	return len(fields) > 0 && fields[0] == "Z"
}
