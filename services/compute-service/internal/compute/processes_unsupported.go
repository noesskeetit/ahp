//go:build !unix && !linux

package compute

import (
	"syscall"
)

// setProcessGroup does nothing here: this platform is not a supported host
// substrate, and no process is ever started on it.
func setProcessGroup(*syscall.SysProcAttr) {}

// KillComputerProcesses fails closed: this platform cannot signal a process
// group, so it can never PROVE that a Computer's processes stopped executing.
// It reports Unavailable - a fact about the host, and a defined contract code,
// never an internal error that would let a sleep claim a suspension it did not
// perform.
func KillComputerProcesses(base, computerID string) (ProcessKillReport, error) {
	return ProcessKillReport{}, Unavailable("host",
		"this platform cannot signal a Computer's process tree; sleep cannot prove its processes dead")
}
