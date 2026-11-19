package compute

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// CommandKeyForStep derives the idempotency key of a command issued by a turn from
// the LOGICAL POSITION that issued it: the run and the step within it.
//
// This is what makes a restart safe rather than lucky. A tool that invents a new key
// per call re-dispatches the same external effect after a crash, because the ledger's
// deduplication cannot recognise it. Two executor processes - one that died and one
// that restarted, possibly on another node - derive the same key from the same
// position with no communication, so the ledger, not the memory of the dead process,
// decides whether the world gets touched again.
//
// Deliberately absent from the key: node, incarnation and epoch. Those change exactly
// when a run is taken over, so a key that carried them would be a new key precisely in
// the case that must not repeat an effect.
func CommandKeyForStep(runID string, step int) string {
	sum := sha256.Sum256([]byte(runID + "\x00" + strconv.Itoa(step)))
	return "step-" + hex.EncodeToString(sum[:])[:40]
}

// ReapOrphanProcesses kills every process group the Computer still has
// registered in this node's process registry and proves each one dead.
//
// It exists for the recovery path, not the sleep path. When an executor dies
// mid-command, the ledger row stays DISPATCHED (its outcome is never blindly
// recomputed) and the sandboxed command keeps running as an orphan: its
// external effect can still land after the "crash", and a restarted executor
// that dispatches new work into the same home would race with it. Reaping is
// the operation that closes that window; the ledger keeps the command
// non-terminal, the reaper makes sure nothing is still executing it.
//
// The scope is checked first, so a caller cannot reach into another tenant's
// Computer through a shared jail base.
func (r *Runner) ReapOrphanProcesses(ctx context.Context, scope contracts.Scope, computerID string) (ProcessKillReport, error) {
	if r.jailBase == "" {
		return ProcessKillReport{}, Unavailable("host", "runner has no jail base to read the process registry from")
	}
	if _, err := r.store.GetComputer(ctx, scope, computerID); err != nil {
		return ProcessKillReport{}, err
	}
	return KillComputerProcesses(r.jailBase, computerID)
}

// ComputerReap is the reaping evidence for one Computer.
type ComputerReap struct {
	ComputerID string            `json:"computer_id"`
	Report     ProcessKillReport `json:"report"`
}

// StartupReap is what a starting executor found and killed in the registry.
type StartupReap struct {
	Computers []ComputerReap `json:"computers"`
}

// Survivors returns the processes that were signalled but are still alive.
// A non-empty result is a real leak and must be surfaced, never swallowed.
func (s StartupReap) Survivors() []ProcessKill {
	var out []ProcessKill
	for _, c := range s.Computers {
		for _, p := range c.Report.Processes {
			if !p.Dead {
				out = append(out, p)
			}
		}
	}
	return out
}

// ReapedProcs counts the processes this reaping proved dead.
func (s StartupReap) ReapedProcs() int {
	n := 0
	for _, c := range s.Computers {
		for _, p := range c.Report.Processes {
			if p.Dead {
				n++
			}
		}
	}
	return n
}

// UnknownOutcomeCommands returns the distinct ledger command row ids whose
// processes the reaper found and killed. A row appears here when an executor
// died while it was DISPATCHED and left its sandboxed command running: the
// reaper proved the process dead, but the command's external effect may or may
// not have landed before the crash, so the ledger keeps the row non-terminal and
// this list is what names it for reconciliation - the report now points at the
// exact row rather than an anonymous pid. Rows with no recorded command id (PTY
// shells, environment installs) are excluded: they were never ledger rows.
func (s StartupReap) UnknownOutcomeCommands() []string {
	seen := map[string]bool{}
	var out []string
	for _, c := range s.Computers {
		for _, p := range c.Report.Processes {
			if p.CommandID != "" && !seen[p.CommandID] {
				seen[p.CommandID] = true
				out = append(out, p.CommandID)
			}
		}
	}
	sort.Strings(out)
	return out
}

// ReapProcessesAtStartup reaps every registered process group under a jail
// base, for every Computer. It is called by an executor before it serves any
// work: a process this executor did not start cannot belong to a command it is
// running, so every entry in the registry predates it (the previous process of
// this node died and left its sandbox behind).
//
// A missing registry directory is the normal case and yields an empty report.
func ReapProcessesAtStartup(base string) (StartupReap, error) {
	var out StartupReap
	if base == "" {
		return out, nil
	}
	dir := filepath.Join(base, "procs")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return out, err
	}
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			ids = append(ids, e.Name())
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		report, err := KillComputerProcesses(base, id)
		if err != nil {
			return out, err
		}
		out.Computers = append(out.Computers, ComputerReap{ComputerID: id, Report: report})
	}
	return out, nil
}
