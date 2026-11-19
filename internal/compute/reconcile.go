package compute

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// ReconcileCommands closes dispatched commands whose dispatch budget ran out without an
// answer. It is the reconciliation the audit called unconfirmed: today a command whose
// execution host vanished keeps the ledger saying "somebody is executing this" forever,
// and every consumer that trusts that row - the export guard, tenant/subject deletion,
// an operator reading the ledger - inherits the lie indefinitely.
//
// See Store.ReconcileStaleDispatches for the verdicts and the evidence behind them.
// Nothing here executes anything: the sweep only ever narrows a status to a terminal
// verdict the ledger can prove, never re-dispatches work and never reports success.
func (r *Runner) ReconcileCommands(ctx context.Context, limit int) (ReconcileReport, error) {
	return r.store.ReconcileStaleDispatches(ctx, limit)
}

// sweepIndexDDL is the index the sweep wants on the dispatched tail of the ledger.
// CONCURRENTLY and off the startup path, on purpose: a plain CREATE INDEX takes a SHARE
// lock that blocks writers on the hot ledger and is blocked by them, and startup DDL runs
// on every process start - measured here as a 30s migrate timeout when another connection
// held the table. SHARE UPDATE EXCLUSIVE permits writes, so a build can be slow but cannot
// stop a plane from starting or a command from being recorded.
const sweepIndexDDL = `CREATE INDEX CONCURRENTLY IF NOT EXISTS commands_stale_dispatched_idx ` +
	`ON ahp_compute.commands (dispatch_deadline) WHERE status = 'dispatched'`

// EnsureSweepIndex builds the sweep's index if it is missing. Best effort by design: the
// sweep is correct without it, so a failure here is logged and the loop still runs. An
// interrupted CONCURRENTLY build leaves an INVALID index that the planner ignores, which
// is also just a scan; retiring one is a maintenance task, not something a loop should do
// on its own.
func (s *Store) EnsureSweepIndex(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return invalid("compute: store is not initialised")
	}
	_, err := s.pool.Exec(ctx, sweepIndexDDL)
	if err != nil {
		return fmt.Errorf("compute: sweep index unavailable, the reconciliation sweep will scan: %w", err)
	}
	return nil
}

// StartCommandReconcileLoop runs that sweep on a ticker until the returned stop is
// called or ctx ends. interval <= 0 disables the loop, which is a deployment choice and
// is therefore said out loud rather than left as a silent no-op.
//
// Why the loop lives with the runner: the sweep is only meaningful next to an execution
// plane, because the thing being reconciled is a dispatch that plane's own commands made
// and whose heartbeat only that plane would have renewed. Concurrent sweeps are safe by
// construction - each row is claimed FOR UPDATE SKIP LOCKED and re-checked inside its
// transaction - so running one per replica is not a hazard.
func (r *Runner) StartCommandReconcileLoop(ctx context.Context, interval time.Duration) (stop func()) {
	if interval <= 0 {
		slog.Warn("compute: command reconciliation loop DISABLED by configuration; a dispatch whose host dies "+
			"will stay DISPATCHED until somebody sweeps it by hand", "interval", interval.String())
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		// Off the startup path, once, before the first sweep: the sweep needs no index to
		// be correct, so this is recorded as a degradation and never as a reason to stop.
		indexCtx, cancelIndex := context.WithTimeout(ctx, 5*time.Minute)
		if err := r.store.EnsureSweepIndex(indexCtx); err != nil {
			slog.Warn("compute: the reconciliation sweep has no index and will scan the ledger",
				"err", err.Error())
		}
		cancelIndex()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
			}
			report, err := r.store.ReconcileStaleDispatches(ctx, 64)
			if err != nil {
				// A failed sweep is not a verdict: the rows stay exactly as they are and
				// the next tick tries again. Reporting it is the difference between a
				// reconciliation that is running and one that quietly stopped.
				slog.Warn("compute: command reconciliation sweep failed; nothing was changed",
					"err", err.Error())
				continue
			}
			for _, closed := range report.Closed {
				slog.Warn("compute: a dispatch that never answered was closed with a terminal verdict",
					"computer_id", closed.ComputerID, "command_id", closed.CommandID,
					"status", string(closed.Status), "terminal_reason", closed.Reason,
					"backend_invoked", closed.BackendInvoked)
			}
		}
	}()
	return func() { close(done) }
}
