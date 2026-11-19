// Package registry: drain planning for version roll-forward.
package registry

import (
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Drain decisions returned by DrainPlan.
const (
	// DrainContinue means no version change is pending: keep running.
	DrainContinue = "continue"
	// DrainWait means the candidate is not state compatible and the tasks still
	// pinned to the previous version must finish before it is replaced.
	DrainWait = "wait"
	// DrainFail means the new version cannot replace the running one.
	DrainFail = "fail"
	// DrainTransfer means the drain window has elapsed and a task still waiting
	// on the previous version moves to the running one, because that version's
	// manifest declares the state compatibility the task needs.
	DrainTransfer = "transfer"
)

// TaskStatus is the registry-side view of a pinned task's lifecycle. It is a
// plain string so callers can map their own status vocabulary onto it.
type TaskStatus string

// Recognised task statuses.
const (
	TaskQueued    TaskStatus = "queued"
	TaskRunning   TaskStatus = "running"
	TaskPaused    TaskStatus = "paused"
	TaskSucceeded TaskStatus = "succeeded"
	TaskFailed    TaskStatus = "failed"
	TaskCancelled TaskStatus = "cancelled"
	TaskUnknown   TaskStatus = "unknown"
)

// IsTerminal reports whether the status can still produce work.
func (s TaskStatus) IsTerminal() bool {
	switch s {
	case TaskSucceeded, TaskFailed, TaskCancelled:
		return true
	default:
		return false
	}
}

// DrainInput describes one pending version change together with the tasks that
// are still pinned to the previous version.
type DrainInput struct {
	// CurrentCompat is the state compatibility identifier of the running
	// version. An empty value means the running version declared no
	// compatibility contract.
	CurrentCompat string
	// Candidate is the version being rolled forward.
	Candidate Manifest
	// WithinWindow reports whether the drain deadline has not elapsed yet.
	WithinWindow bool
	// PinnedTasks counts tasks still pinned to the previous version.
	PinnedTasks int
	// Statuses carries the status of each still-pinned task. Entries that are
	// empty are treated as TaskUnknown, which blocks the drain.
	Statuses []TaskStatus
}

// DrainDecision is the outcome of a drain evaluation.
type DrainDecision struct {
	// Action is one of DrainContinue, DrainWait or DrainFail.
	Action string
	// Reason is a short, content-free explanation.
	Reason string
	// Recheck reports whether the caller should evaluate the plan again later.
	Recheck bool
}

// DrainPlan decides whether a candidate version may replace the running one.
//
// The rules are, in order of precedence:
//
//   - no candidate, or a candidate whose state compatibility matches the
//     running version: continue, no drain is required;
//   - an incompatible candidate while any pinned task is still active, or its
//     status is unknown: wait inside the window, fail once it elapses;
//   - an incompatible candidate once every pinned task reached a terminal
//     state: continue, the old version can be replaced.
//
// The decision is pure: it reads nothing but its input and never touches the
// database, so it can be evaluated repeatedly while a drain is in progress.
func DrainPlan(in DrainInput) DrainDecision {
	if in.Candidate.State.Compat == "" || in.CurrentCompat == "" ||
		strings.EqualFold(in.Candidate.State.Compat, in.CurrentCompat) {
		return DrainDecision{Action: DrainContinue, Reason: "state compatible"}
	}
	if !hasActivePinnedTask(in) {
		return DrainDecision{Action: DrainContinue, Reason: "no active task pinned to the previous version"}
	}
	if in.WithinWindow {
		return DrainDecision{Action: DrainWait, Reason: "incompatible state change, pinned tasks still active", Recheck: true}
	}
	return DrainDecision{Action: DrainFail, Reason: "incompatible state change, pinned tasks outlived the drain window"}
}

// hasActivePinnedTask reports whether any pinned task may still write state.
// A positive pinned count with no reported statuses is treated as active: the
// drain must not proceed on missing information.
func hasActivePinnedTask(in DrainInput) bool {
	if len(in.Statuses) == 0 {
		return in.PinnedTasks > 0
	}
	for _, s := range in.Statuses {
		if s == "" || !s.IsTerminal() {
			return true
		}
	}
	return false
}

// ValidateDrainInput rejects inputs a caller clearly built wrong, so a deploy
// controller cannot silently drain on a malformed request.
func ValidateDrainInput(in DrainInput) error {
	if in.PinnedTasks < 0 {
		return contracts.NewError(contracts.ErrInvalid, "drain pinned_tasks must not be negative")
	}
	if in.Candidate.State.Compat != "" {
		if err := ValidateManifest(in.Candidate); err != nil {
			return err
		}
	}
	return nil
}

// DrainTransferPlan decides what happens to a task that is still waiting on the
// previous version once the drain question is asked:
//
//   - inside the window it waits: the previous version still serves it, whatever
//     the compatibility declaration says, because a rollout must not change the
//     behaviour of a task that already began;
//   - after the window, the task moves to the running version only when that
//     manifest declares a state compatibility equal to the one the task's state
//     was written under. An absent or different declaration is a fail, not a
//     silent guess: the task would otherwise be pointed at state it cannot read.
//
// This is the rule of rev 3.2, c. 52 ("Окно drain истекло: Перенос на новую
// версию при state.compat, иначе — failed с причиной") and it deliberately does
// NOT reuse DrainPlan's notion of compatibility, which answers a different
// question -- whether a replacement may happen at all.
func DrainTransferPlan(in DrainInput) DrainDecision {
	if err := ValidateDrainInput(in); err != nil {
		return DrainDecision{Action: DrainFail, Reason: "malformed drain input: " + err.Error()}
	}
	if in.WithinWindow {
		return DrainDecision{Action: DrainWait, Reason: "drain window still open", Recheck: true}
	}
	if in.Candidate.State.Compat == "" {
		return DrainDecision{Action: DrainFail,
			Reason: "the new manifest declares no state compatibility"}
	}
	if in.CurrentCompat == "" {
		return DrainDecision{Action: DrainFail,
			Reason: "the running version wrote state without a compatibility declaration"}
	}
	if !strings.EqualFold(in.Candidate.State.Compat, in.CurrentCompat) {
		return DrainDecision{Action: DrainFail,
			Reason: "the new manifest declares a different state compatibility"}
	}
	return DrainDecision{Action: DrainTransfer,
		Reason: "state compatible, the waiting task moves to the running version"}
}
