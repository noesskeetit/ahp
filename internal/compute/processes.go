package compute

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ProcessKill is the verified fate of one registered process.
type ProcessKill struct {
	PID  int  `json:"pid"`
	PGID int  `json:"pgid"`
	Dead bool `json:"dead"`
	// Zombie reports the one way a killed process can be proven dead without
	// its pid being freed: it stopped executing and only waits to be reaped by
	// its parent. It cannot land an external effect, but the distinction is
	// kept in the evidence instead of being reported as a clean disappearance.
	// Platforms without a portable zombie probe leave it unset and prove death
	// at the process-group level instead.
	Zombie bool `json:"zombie,omitempty"`

	// CommandID is the ledger row this process was running, when it was spawned
	// for one. The restart reaper uses it to name the dispatched row whose
	// outcome is still unknown after its orphan is killed. Omitted for processes
	// that never belonged to a command row (PTY shells, environment installs).
	CommandID string `json:"command_id,omitempty"`
}

// ProcessKillReport is the evidence returned by a sleep: every process the
// Computer was running, signalled, and verified dead.
type ProcessKillReport struct {
	Processes []ProcessKill `json:"processes"`
}

// registerProcess records a live child pid under the per-Computer registry so
// SleepComputer (and the restart reaper) can kill and verify its whole process
// tree. The registry is a plain directory of files inside the jail base; each
// file holds the process-group id on its first line and, when the process was
// spawned for a ledger command, that command's row id on its second line. It is
// the host substrate's stand-in for the guest process table, extended so the
// reaper can name the ledger row whose outcome is unknown when it kills an
// orphan a crashed executor left behind.
func registerProcess(base, computerID string, pid, pgid int, commandID string) error {
	dir := filepath.Join(base, "procs", computerID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, strconv.Itoa(pid)), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if commandID == "" {
		_, err = fmt.Fprintf(f, "%d\n", pgid)
	} else {
		_, err = fmt.Fprintf(f, "%d\n%s\n", pgid, commandID)
	}
	return err
}

func unregisterProcess(base, computerID string, pid int) {
	_ = os.Remove(filepath.Join(base, "procs", computerID, strconv.Itoa(pid)))
}

// readProcessEntry parses one registry file: the process-group id on the first
// line and, when present, the ledger command row id on the second line. Entries
// written before a command row was recorded (and hand-written test entries) hold
// only the pgid, so a missing second line yields an empty command id rather than
// an error - an unrecorded command id is unknown, never a fabricated one.
func readProcessEntry(path string, fallbackPID int) (pgid int, commandID string) {
	pgid = fallbackPID
	b, err := os.ReadFile(path)
	if err != nil {
		return pgid, ""
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) > 0 {
		if v, err := strconv.Atoi(strings.TrimSpace(lines[0])); err == nil {
			pgid = v
		}
	}
	if len(lines) > 1 {
		commandID = strings.TrimSpace(lines[1])
	}
	return pgid, commandID
}
