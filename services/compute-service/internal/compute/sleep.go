package compute

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// SleepReport is the durable outcome of a Computer sleep: the Computer's new
// status, the lease gate result, and the verified process-death evidence.
type SleepReport struct {
	ComputerID string            `json:"computer_id"`
	Status     ComputerStatus    `json:"status"`
	LeasesHeld int               `json:"leases_held"`
	Processes  ProcessKillReport `json:"processes"`
	SleptAt    time.Time         `json:"slept_at"`
	// IncarnationsStopped is how many control incarnations this sleep settled.
	// It is reported, not implied: an operator reading a sleep answer can see the
	// incarnation ledger moved, which is the half the sleep used to leave undone.
	IncarnationsStopped int64 `json:"incarnations_stopped"`
}

// SleepComputer puts an active Computer to sleep. The gate is the lease
// count (activity leases plus the writer lease, all in Redis): a Computer
// with any holding lease is awake by definition and the sleep is refused
// with CONFLICT — that is how a background command "holds the VM". With no
// holding lease the process tree is killed and verified dead, the status
// moves to suspended, and the open cost window is closed.
func (s *Store) SleepComputer(ctx context.Context, scope contracts.Scope, computerID string, leasesHeld int, processes ProcessKillReport) (SleepReport, error) {
	if err := validIdentifier(computerID, "computer id"); err != nil {
		return SleepReport{}, err
	}
	if leasesHeld < 0 {
		return SleepReport{}, invalid("lease count must not be negative")
	}
	var out SleepReport
	var incarnationsStopped int64
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		c, err := s.getComputer(ctx, tx, scope, computerID)
		if err != nil {
			return err
		}
		switch c.Status {
		case ComputerActive:
		case ComputerProvisioning:
			// Never activated: nothing to sleep, but allow settling to
			// suspended so a created-never-woken Computer costs nothing.
		default:
			return contracts.NewError(contracts.ErrConflict,
				fmt.Sprintf("computer is %s, not awake", c.Status))
		}
		if leasesHeld > 0 {
			return contracts.NewError(contracts.ErrConflict,
				fmt.Sprintf("computer is awake: %d lease(s) held", leasesHeld))
		}
		// Serialize with the quiescent-export publication guard for the whole
		// settlement, exactly as StopIncarnation does: an export deciding between
		// "source is live" and "source is parked" must not interleave with a sleep
		// that is about to change that answer.
		if _, err := tx.Exec(ctx, `SELECT id FROM ahp_compute.computers WHERE id=$1 FOR UPDATE`, computerID); err != nil {
			return err
		}
		// Close the open cost window, if any.
		if _, err := tx.Exec(ctx, `
			UPDATE ahp_compute.cost_ledger
			SET ended_at = clock_timestamp()
			WHERE computer_id = $1 AND ended_at IS NULL`, computerID); err != nil {
			return err
		}
		// Settle the control incarnation with the Computer.
		//
		// A sleeping Computer with its incarnation still open is not a cosmetic
		// ledger gap. The export guard reads "any incarnation with status <>
		// 'stopped'" as "this source is live" (export_guard.go), so a Computer put to
		// sleep by the documented path could NEVER export: the only way out was a
		// manual StopIncarnation against a row the user was never told about - the
		// defect named in the root audit (10-compute.md, item 1). Sleep is one
		// transition over computer + cost window + incarnation; the guard itself is
		// not weakened by this, it is what makes the guard reachable.
		stopped, err := tx.Exec(ctx, `
			UPDATE ahp_compute.incarnations
			SET status = 'stopped', stopped_at = clock_timestamp()
			WHERE computer_id = $1 AND status <> 'stopped'`, computerID)
		if err != nil {
			return err
		}
		incarnationsStopped = stopped.RowsAffected()
		c, err = scanComputer(tx.QueryRow(ctx, `
			UPDATE ahp_compute.computers
			SET status = 'suspended', updated_at = clock_timestamp()
			WHERE id = $1
			RETURNING `+computerSelect, computerID))
		if err != nil {
			return err
		}
		out = SleepReport{
			ComputerID:          c.ID,
			Status:              c.Status,
			LeasesHeld:          leasesHeld,
			Processes:           processes,
			SleptAt:             time.Now().UTC(),
			IncarnationsStopped: incarnationsStopped,
		}
		return nil
	})
	return out, err
}

// OpenCostWindow opens (idempotently) the awake-window of a Computer: the
// first incarnation acquire on a freshly created or sleeping Computer starts
// billing; a second acquire while awake is a no-op.
func (s *Store) OpenCostWindow(ctx context.Context, scope contracts.Scope, computerID string) error {
	if err := validIdentifier(computerID, "computer id"); err != nil {
		return err
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO ahp_compute.cost_ledger (id, computer_id, kind, started_at)
			VALUES ($1, $2, 'active', clock_timestamp())
			ON CONFLICT (computer_id) WHERE ended_at IS NULL DO NOTHING`,
			contracts.NewID(), computerID)
		return err
	})
}

// CostWindow is one awake window of a Computer.
type CostWindow struct {
	ID        string     `json:"id"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at"`
}

// CostReport is the Computer's accumulated cost so far: the sum of closed
// awake windows plus the open one measured up to now.
type CostReport struct {
	ComputerID string       `json:"computer_id"`
	AwakeNanos int64        `json:"awake_ns"`
	AwakeHuman string       `json:"awake_human"`
	Open       bool         `json:"open_window"`
	Windows    []CostWindow `json:"windows"`
}

// CostReport returns the Computer's cost accounting. The PDF rule is encoded
// here: a background command holds an activity lease, which keeps the
// Computer awake, and every awake second is accounted.
func (s *Store) CostReport(ctx context.Context, scope contracts.Scope, computerID string) (CostReport, error) {
	var out CostReport
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT id, started_at, ended_at FROM ahp_compute.cost_ledger
			WHERE computer_id = $1 ORDER BY started_at ASC`, computerID)
		if err != nil {
			return err
		}
		defer rows.Close()
		now := time.Now().UTC()
		for rows.Next() {
			var w CostWindow
			if err := rows.Scan(&w.ID, &w.StartedAt, &w.EndedAt); err != nil {
				return err
			}
			w.StartedAt = w.StartedAt.UTC()
			if w.EndedAt != nil {
				ut := w.EndedAt.UTC()
				w.EndedAt = &ut
				out.AwakeNanos += int64(w.EndedAt.Sub(w.StartedAt))
			} else {
				out.AwakeNanos += int64(now.Sub(w.StartedAt))
				out.Open = true
			}
			out.Windows = append(out.Windows, w)
		}
		return rows.Err()
	})
	if err != nil {
		return CostReport{}, err
	}
	out.ComputerID = computerID
	out.AwakeHuman = time.Duration(out.AwakeNanos).Round(time.Millisecond).String()
	return out, nil
}
