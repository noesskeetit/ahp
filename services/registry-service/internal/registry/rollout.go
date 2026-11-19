// Package registry: the drain a version replacement owes the tasks already
// running on the version it replaces.
//
// PDF rev 3.2, c. 41/42 and the c. 52 table fix the behaviour literally:
//
//   - "Версия деплоя и AgentConfig закрепляются за задачей, и выкладка не меняет
//     поведения начатых задач" -- a pinned task keeps its version;
//   - "старая версия работает, пока у неё есть незавершённые задачи, но не дольше
//     окна drain из манифеста" -- the previous version keeps serving those tasks,
//     bounded by the window the manifest declares as state.drain;
//   - "После окна задачи, ждущие ответа, переносятся на новую версию, если
//     манифест объявляет совместимость состояния (state.compat), иначе завершаются
//     с понятной причиной" -- once the window elapses, a waiting task either moves
//     to the version that is actually running now, or is refused with a reason.
//
// A rollout therefore records the window it must honour, and ResolveConfig --
// the call that decides what a task actually executes -- is where the window is
// enforced. Nothing here invents a deadline the manifest did not declare.
package registry

import (
	"context"
	"errors"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// reasonDrainWindowElapsed is the stable reason a waiting task is refused when
// its version's drain window elapsed and the running manifest declares no state
// compatibility that could carry it over.
const reasonDrainWindowElapsed = "drain_window_elapsed"

// rollout is one recorded version replacement.
type rollout struct {
	ID             string
	FromVersion    string
	ToVersion      string
	Action         string
	Window         time.Duration
	DeclaredWindow bool
	Until          time.Time
	CompatSource   string
	CompatTarget   string
}

// within reports whether the drain obligation of this rollout is still running.
// A rollout whose window the manifest never declared has no deadline: the
// Registry will not invent one, so the previous version keeps serving the tasks
// pinned to it until they finish.
func (r rollout) within(now time.Time) bool {
	return !r.DeclaredWindow || now.Before(r.Until)
}

// recordRollout stores one version replacement of an installation together with
// the drain window the REPLACED version's manifest declared. It runs inside the
// caller's transaction, so a version pointer, its rollout history and the drain
// obligation can never disagree.
func recordRollout(ctx context.Context, tx pgx.Tx, tenant, installationID, agentID, environment string, from, to Version, action string) error {
	if from.ID == to.ID {
		return nil
	}
	window, err := from.Manifest.State.DrainWindow()
	if err != nil {
		return err
	}
	var windowArg, untilArg any
	if from.Manifest.State.Drain != "" {
		windowArg = window
		untilArg = time.Now().UTC().Add(window)
	}
	const q = `
INSERT INTO ahp_registry.rollouts
    (id, tenant, installation_id, agent_id, environment, from_version_id, to_version_id,
     action, drain_window, drain_until, compat_source, compat_target)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`
	if _, err := tx.Exec(ctx, q, contracts.NewID(), tenant, installationID, agentID, environment,
		from.ID, to.ID, action, windowArg, untilArg, from.Manifest.State.Compat, to.Manifest.State.Compat); err != nil {
		return storageError("record rollout", err)
	}
	return nil
}

const rolloutColumns = `id, from_version_id, to_version_id, action, drain_window, drain_until, compat_source, compat_target`

// scanRollout reads one rollout row; the window and deadline are nullable,
// because an undeclared window stays absent rather than defaulted.
func scanRollout(row pgx.Row) (rollout, error) {
	var (
		r       rollout
		window  *time.Duration
		until   *time.Time
		src, to *string
	)
	err := row.Scan(&r.ID, &r.FromVersion, &r.ToVersion, &r.Action, &window, &until, &src, &to)
	if window != nil {
		r.Window = *window
		r.DeclaredWindow = true
	}
	if until != nil {
		r.Until = until.UTC()
	}
	if src != nil {
		r.CompatSource = *src
	}
	if to != nil {
		r.CompatTarget = *to
	}
	return r, err
}

// latestRolloutFrom returns the most recent replacement that took `fromVersion`
// out of service for this installation, if one was recorded.
func latestRolloutFrom(ctx context.Context, tx pgx.Tx, tenant, installationID, fromVersion string) (rollout, bool, error) {
	r, err := scanRollout(tx.QueryRow(ctx, `
SELECT `+rolloutColumns+`
FROM ahp_registry.rollouts
WHERE tenant = $1 AND installation_id = $2 AND from_version_id = $3
ORDER BY created_at DESC, id DESC LIMIT 1`, tenant, installationID, fromVersion))
	if errors.Is(err, pgx.ErrNoRows) {
		return rollout{}, false, nil
	}
	if err != nil {
		return rollout{}, false, storageError("read rollout", err)
	}
	return r, true, nil
}

// currentDrain returns the replacement whose version still carries pinned tasks,
// i.e. the drain an installation is in right now, if any.
func currentDrain(ctx context.Context, tx pgx.Tx, tenant, installationID string) (rollout, bool, error) {
	r, err := scanRollout(tx.QueryRow(ctx, `
SELECT `+rolloutColumns+`
FROM ahp_registry.rollouts r
WHERE r.tenant = $1 AND r.installation_id = $2
  AND EXISTS (SELECT 1 FROM ahp_registry.task_pins p
              WHERE p.tenant = r.tenant AND p.installation_id = r.installation_id
                AND p.version_id = r.from_version_id)
ORDER BY r.created_at DESC, r.id DESC LIMIT 1`, tenant, installationID))
	if errors.Is(err, pgx.ErrNoRows) {
		return rollout{}, false, nil
	}
	if err != nil {
		return rollout{}, false, storageError("read drain position", err)
	}
	return r, true, nil
}

// countPinnedInTx counts the tasks still pinned to one version of one
// installation inside the caller's transaction.
func countPinnedInTx(ctx context.Context, tx pgx.Tx, tenant, installationID, versionID string) (int, error) {
	var n int
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM ahp_registry.task_pins
WHERE tenant = $1 AND installation_id = $2 AND version_id = $3`, tenant, installationID, versionID).Scan(&n); err != nil {
		return 0, storageError("count pinned tasks", err)
	}
	return n, nil
}

// DrainState is what an operator or a deploy controller sees about one
// installation: which version runs now, which one is draining, how long it may
// still run, and what the drain plan says about the tasks pinned to it.
type DrainState struct {
	Installation     string    `json:"installation_id"`
	Environment      string    `json:"environment"`
	CurrentVersion   string    `json:"current_version"`
	PreviousVersion  string    `json:"previous_version,omitempty"`
	Window           string    `json:"drain_window,omitempty"`
	DrainUntil       time.Time `json:"drain_until,omitempty"`
	Unbounded        bool      `json:"drain_window_undeclared"`
	PinnedOnPrevious int       `json:"pinned_tasks_on_previous"`
	InDrain          bool      `json:"draining"`
	Plan             string    `json:"drain_plan"`
	PlanReason       string    `json:"drain_reason,omitempty"`
}

// DrainState reports the drain position of one installation of the caller tenant.
func (s *Store) DrainState(ctx context.Context, tenant, installationID string) (DrainState, error) {
	if err := requireID("tenant", tenant); err != nil {
		return DrainState{}, err
	}
	if err := requireID("installation id", installationID); err != nil {
		return DrainState{}, err
	}
	var out DrainState
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		inst, err := lockInstallation(ctx, tx, tenant, installationID)
		if err != nil {
			return err
		}
		out = DrainState{Installation: inst.ID, Environment: inst.Environment, CurrentVersion: inst.VersionID}
		current, err := getVersion(ctx, tx, inst.VersionID)
		if err != nil {
			return err
		}
		r, ok, err := currentDrain(ctx, tx, tenant, inst.ID)
		if err != nil {
			return err
		}
		if !ok {
			out.Plan = DrainContinue
			out.PlanReason = "no pinned task is left on a replaced version"
			return nil
		}
		previous, err := getVersion(ctx, tx, r.FromVersion)
		if err != nil {
			return err
		}
		count, err := countPinnedInTx(ctx, tx, tenant, inst.ID, previous.ID)
		if err != nil {
			return err
		}
		plan := DrainPlan(DrainInput{
			CurrentCompat: previous.Manifest.State.Compat,
			Candidate:     current.Manifest,
			WithinWindow:  r.within(s.now()),
			PinnedTasks:   count,
		})
		out.PreviousVersion = previous.ID
		out.PinnedOnPrevious = count
		out.Unbounded = !r.DeclaredWindow
		out.Window = previous.Manifest.State.Drain
		out.DrainUntil = r.Until
		out.InDrain = plan.Action == DrainWait
		out.Plan = plan.Action
		out.PlanReason = plan.Reason
		return nil
	})
	if err != nil {
		return DrainState{}, err
	}
	return out, nil
}

// drainOutcome is the result of applying the drain rules to one pinned task.
type drainOutcome struct {
	// VersionID is the version the task must execute on now: the pinned one
	// while the drain runs, or the running version after a transfer.
	VersionID string
	// Action is "" for an ordinary pin, "waiting" while the drain is in
	// progress, and "transferred" when the task moved after the window elapsed.
	Action string
	// MigratedFrom names the version the task was transferred from.
	MigratedFrom string
}

// applyPinnedDrain decides, for a task whose pin names a version that is no
// longer the running one, whether it keeps running there, moves to the running
// version, or is refused with a reason. It runs inside ResolveConfig, in the
// transaction that decides what the task executes, so the rule cannot be missed
// by a poller that is late or absent.
func (s *Store) applyPinnedDrain(ctx context.Context, tx pgx.Tx, tenant string, inst Installation, pinned, running Version, taskID string) (drainOutcome, error) {
	keep := drainOutcome{VersionID: pinned.ID}
	if pinned.ID == running.ID {
		return keep, nil
	}
	r, ok, err := latestRolloutFrom(ctx, tx, tenant, inst.ID, pinned.ID)
	if err != nil {
		return drainOutcome{}, err
	}
	if !ok {
		// No recorded replacement took this pin out of service: it predates the
		// rollout record. The pin rule stands -- a task keeps its version.
		return keep, nil
	}
	count, err := countPinnedInTx(ctx, tx, tenant, inst.ID, pinned.ID)
	if err != nil {
		return drainOutcome{}, err
	}
	// While the window runs the previous version still serves its tasks: a
	// replacement may not change the behaviour of a task that already began.
	if r.within(s.now()) {
		keep.Action = "waiting"
		return keep, nil
	}
	// The window elapsed. A waiting task moves to the running version only if
	// that manifest declares the state compatibility needed to read its state.
	decision := DrainTransferPlan(DrainInput{
		CurrentCompat: pinned.Manifest.State.Compat,
		Candidate:     running.Manifest,
		WithinWindow:  false,
		PinnedTasks:   count,
	})
	if decision.Action != DrainTransfer {
		return drainOutcome{}, contracts.NewReasoned(contracts.ErrConflict, reasonDrainWindowElapsed,
			"drain window of version "+shortDigest(pinned.Digest)+" elapsed and the running manifest declares no state compatibility: "+decision.Reason)
	}
	const q = `
UPDATE ahp_registry.task_pins
   SET version_id = $1, migrated_from = $2, migrated_at = now()
 WHERE tenant = $3 AND task_id = $4 AND installation_id = $5 AND version_id = $2`
	tag, err := tx.Exec(ctx, q, running.ID, pinned.ID, tenant, taskID, inst.ID)
	if err != nil {
		return drainOutcome{}, storageError("transfer pinned task", err)
	}
	if tag.RowsAffected() != 1 {
		return drainOutcome{}, contracts.NewError(contracts.ErrConflict,
			"drain: task pin changed while its drain window elapsed")
	}
	return drainOutcome{VersionID: running.ID, Action: "transferred", MigratedFrom: pinned.ID}, nil
}
