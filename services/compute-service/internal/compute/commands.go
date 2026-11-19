package compute

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// The command ledger is the durable record of every command Compute has been
// asked to run. It exists to make side effects safe across retries, restarts
// and reconnects (SBX-05): a command is written once, dispatched at most once,
// and any ambiguity about whether it ran is recorded as SIDE_EFFECT_UNKNOWN
// rather than silently re-run.

const commandSelect = `id, computer_id, incarnation, key, execution_id, status, command, cwd,
	stdout, stderr, exit_code, has_exit, output_truncated, output_seq,
	dispatch_deadline, heartbeat_at, backend_started_at, terminal_reason, created_at, updated_at`

// Dispatch/heartbeat budget. A dispatch without a bound is the compute-plane twin of
// the run that stays WORKING forever: the row says "somebody is executing this", the
// somebody is gone, and every consumer that trusts the row (export guard, tenant
// deletion, the operator reading the ledger) inherits that lie for good.
//
// The numbers are deliberately not the command's own timeout: the budget is the
// HEARTBEAT contract, and the holder renews it while it lives, so a healthy long
// command is never reconciled out from under its runner. It is the dead holder whose
// silence becomes observable inside this window instead of never.
const (
	// CommandHeartbeatInterval is how often a running command renews its proof of life.
	CommandHeartbeatInterval = 10 * time.Second
	// CommandDispatchBudget is how long a dispatch may go without a heartbeat before
	// reconciliation is entitled to close it. Three intervals: one lost tick must not
	// condemn a live command, and four would only slow the sweep down.
	CommandDispatchBudget = 3 * CommandHeartbeatInterval
)

// Exec records a command in the ledger idempotently and reports whether the
// caller may dispatch it to a backend. Replaying the same (computer, key, body)
// returns the original record with its current status; a mismatched body under
// the same key is KEY_REUSE_MISMATCH. mayDispatch is true only while the record
// is still ACCEPTED and not cancelled — after DISPATCHED the outcome is never
// blindly recomputed.
func (s *Store) Exec(ctx context.Context, scope contracts.Scope, computerID, key, command, cwd string) (CommandRecord, bool, error) {
	if err := validIdentifier(key, "idempotency key"); err != nil {
		return CommandRecord{}, false, err
	}
	cmd, dir, digest, err := canonicalCommand(command, cwd)
	if err != nil {
		return CommandRecord{}, false, err
	}
	id := contracts.NewID()
	var out CommandRecord
	var mayDispatch bool
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		c, err := s.getComputer(ctx, tx, scope, computerID)
		if err != nil {
			return err
		}
		if c.Status == ComputerDestroyed {
			return contracts.NewError(contracts.ErrConflict, "cannot exec on a destroyed computer")
		}
		row := tx.QueryRow(ctx, `
			INSERT INTO ahp_compute.commands
				(id, tenant, deployment, subject, computer_id, incarnation, key, command, cwd, status, body_digest)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'accepted',$10)
			ON CONFLICT (tenant, deployment, subject, computer_id, key) DO NOTHING
			RETURNING `+commandSelect,
			id, scope.Tenant, scope.Deployment, scope.Subject, computerID, c.Incarnation, key, cmd, dir, digest)
		rec, err := scanCommand(row)
		if err == nil {
			out = rec
			mayDispatch = true
			return nil
		}
		if contracts.Code(err) != string(contracts.ErrNotFound) {
			return err
		}
		existing, getErr := s.getCommandByKey(ctx, tx, scope, computerID, key)
		if getErr != nil {
			return getErr
		}
		if existing.digest != digest {
			return contracts.NewError(contracts.ErrKeyReuse,
				"command idempotency key was reused with a different command body")
		}
		out = existing.CommandRecord
		mayDispatch = existing.CommandRecord.Status == CommandAccepted && !existing.cancelRequested
		return nil
	})
	if isUniqueViolation(err) {
		return CommandRecord{}, false, contracts.NewError(contracts.ErrConflict, "command key collision")
	}
	if err != nil {
		return CommandRecord{}, false, err
	}
	return out, mayDispatch, nil
}

// MarkDispatched transitions an ACCEPTED command to DISPATCHED and assigns the
// SBX-05 execution id used for output buffering and reconnect. It is idempotent:
// replaying after the first dispatch returns the same record unchanged.
func (s *Store) MarkDispatched(ctx context.Context, scope contracts.Scope, computerID, commandID string) (CommandRecord, error) {
	var out CommandRecord
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := s.checkOwnerWritable(ctx, tx, scope); err != nil {
			return err
		}
		cur, err := s.getCommand(ctx, tx, scope, computerID, commandID)
		if err != nil {
			return err
		}
		if cur.Status != CommandAccepted {
			out = cur
			return nil
		}
		rec, err := scanCommand(tx.QueryRow(ctx, `
			UPDATE ahp_compute.commands
			SET status = 'dispatched',
			    execution_id = CASE WHEN execution_id = '' THEN $3 ELSE execution_id END,
			    dispatch_deadline = clock_timestamp() + make_interval(secs => $4::double precision),
			    heartbeat_at = clock_timestamp(),
			    updated_at = clock_timestamp()
			WHERE computer_id = $1 AND id = $2 AND status = 'accepted'
			RETURNING `+commandSelect,
			computerID, commandID, contracts.NewID(), CommandDispatchBudget.Seconds()))
		if err != nil {
			return err
		}
		out = rec
		return nil
	})
	return out, err
}

// derefTimes flattens the three nullable dispatch timestamps onto the record. NULL
// means "this never happened" (never dispatched, never heartbeated, no backend reached)
// and is carried as the zero time; a caller that must distinguish them reads the status
// alongside, which is the fact the timestamps are evidence for.
func derefTimes(first, second, third *time.Time) (time.Time, time.Time, time.Time) {
	deref := func(v *time.Time) time.Time {
		if v == nil {
			return time.Time{}
		}
		return v.UTC()
	}
	return deref(first), deref(second), deref(third)
}

// MarkBackendStarted records the instant the reconciliation verdicts turn on: the moment
// the command was handed to a backend, which is the earliest instant an effect became
// possible. Calling it is the runner's job, immediately before the backend call, and the
// first write wins so a retry cannot push the evidence boundary later than the fact.
//
// A command that is no longer DISPATCHED is reported without error: the row may have
// been reconciled while this runner was still alive, and what the runner needs to hear
// is "stop, this command is closed", not a transport failure.
func (s *Store) MarkBackendStarted(ctx context.Context, scope contracts.Scope, computerID, commandID string) (CommandRecord, bool, error) {
	var out CommandRecord
	var marked bool
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		cur, err := s.getCommand(ctx, tx, scope, computerID, commandID)
		if err != nil {
			return err
		}
		if cur.Status != CommandDispatched {
			out = cur
			return nil
		}
		rec, err := scanCommand(tx.QueryRow(ctx, `
			UPDATE ahp_compute.commands
			SET backend_started_at = COALESCE(backend_started_at, clock_timestamp()),
			    heartbeat_at = clock_timestamp(),
			    dispatch_deadline = clock_timestamp() + make_interval(secs => $3::double precision),
			    updated_at = clock_timestamp()
			WHERE computer_id = $1 AND id = $2 AND status = 'dispatched'
			RETURNING `+commandSelect,
			computerID, commandID, CommandDispatchBudget.Seconds()))
		if err != nil {
			return err
		}
		out, marked = rec, true
		return nil
	})
	return out, marked, err
}

// HeartbeatCommand renews the dispatch budget of a command that is still running.
//
// The second return value is the one the caller acts on: false means this command is
// NOT dispatched any more - reconciliation (or an answer) closed it while this process
// was still executing it. That is a fence, not information: the row is closed, so this
// holder must stop and must not write an outcome over a verdict reached without it. It
// is reported as an ordinary answer rather than an error, because "you arrived after the
// verdict" is a fact about the ledger, not a failure to reach it.
func (s *Store) HeartbeatCommand(ctx context.Context, scope contracts.Scope, computerID, commandID string,
	extend time.Duration) (CommandRecord, bool, error) {
	if extend <= 0 {
		extend = CommandDispatchBudget
	}
	var out CommandRecord
	var live bool
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		rec, err := scanCommand(tx.QueryRow(ctx, `
			UPDATE ahp_compute.commands
			SET heartbeat_at = clock_timestamp(),
			    dispatch_deadline = clock_timestamp() + make_interval(secs => $3::double precision),
			    updated_at = clock_timestamp()
			WHERE computer_id = $1 AND id = $2 AND status = 'dispatched'
			RETURNING `+commandSelect,
			computerID, commandID, extend.Seconds()))
		if err == nil {
			out, live = rec, true
			return nil
		}
		if contracts.Code(err) != string(contracts.ErrNotFound) {
			return err
		}
		// Nothing was updated: read the row back so the caller can see the verdict it
		// arrived after. A command that does not exist at all stays an error.
		cur, getErr := s.getCommand(ctx, tx, scope, computerID, commandID)
		if getErr != nil {
			return getErr
		}
		out = cur
		return nil
	})
	return out, live, err
}

// ReconcileStaleDispatches closes dispatched commands whose dispatch budget expired.
//
// This is the answer to "reconcile не подтверждён". A command whose execution host
// vanished used to stay DISPATCHED forever, which is worse than a failure: every
// consumer of the ledger must keep treating it as live work - the export guard refuses
// the Computer, deletion counts it as outstanding, and an operator cannot tell "still
// running" from "died an hour ago". Nothing here re-runs a command and nothing here
// calls one a success; the ledger records only the strongest statement the evidence
// supports, and says which evidence that was:
//
//   - a backend had been invoked for the command, or output had already arrived, so an
//     effect may have landed: SIDE_EFFECT_UNKNOWN, and the export guard keeps holding
//     it - that refusal is the accepted contract for an unproven effect;
//   - otherwise no backend was ever reached on this dispatch, so nothing on this path
//     could have touched the world: LOST - terminal, and no longer blocking its owner's
//     export. "The runner died before invoking a backend" is a statement the ledger can
//     actually prove, because the runner marks the invocation before making it.
//
// Rows whose deadline has not passed are not touched, and each row is claimed FOR UPDATE
// SKIP LOCKED and re-checked inside its own transaction, so a concurrent sweep - or an
// answer that arrived in the meantime - wins over a verdict inferred from silence.
func (s *Store) ReconcileStaleDispatches(ctx context.Context, limit int) (ReconcileReport, error) {
	if limit <= 0 {
		limit = 64
	}
	var out ReconcileReport
	stale, err := s.pool.Query(ctx, `
		SELECT id, computer_id FROM ahp_compute.commands
		WHERE status = 'dispatched'
		  AND dispatch_deadline IS NOT NULL
		  AND dispatch_deadline < clock_timestamp()
		ORDER BY dispatch_deadline
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return out, fmt.Errorf("compute: read stale dispatches: %w", err)
	}
	type staleRow struct{ id, computerID string }
	var rows []staleRow
	for stale.Next() {
		var r staleRow
		if err := stale.Scan(&r.id, &r.computerID); err != nil {
			stale.Close()
			return out, fmt.Errorf("compute: scan stale dispatch: %w", err)
		}
		rows = append(rows, r)
	}
	stale.Close()
	if err := stale.Err(); err != nil {
		return out, err
	}
	for _, r := range rows {
		var closed ReconciledCommand
		err := s.inTx(ctx, func(tx pgx.Tx) error {
			var invoked bool
			if err := tx.QueryRow(ctx, `
				SELECT (backend_started_at IS NOT NULL OR output_seq > 0)
				FROM ahp_compute.commands
				WHERE computer_id = $1 AND id = $2 AND status = 'dispatched'
				FOR UPDATE`, r.computerID, r.id).Scan(&invoked); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return nil // answered, or reconciled by somebody else, in the meantime
				}
				return err
			}
			verdict, reason := CommandLost, TerminalReasonDispatchDeadline
			if invoked {
				verdict, reason = CommandSideEffectUnknown, TerminalReasonBackendUnanswered
			}
			rec, err := scanCommand(tx.QueryRow(ctx, `
				UPDATE ahp_compute.commands
				SET status = $3, terminal_reason = $4, updated_at = clock_timestamp()
				WHERE computer_id = $1 AND id = $2 AND status = 'dispatched'
				RETURNING `+commandSelect, r.computerID, r.id, string(verdict), reason))
			if err != nil {
				return err
			}
			if !rec.Status.Terminal() {
				return contracts.NewError(contracts.ErrConflict,
					"compute: reconciliation produced a non-terminal verdict")
			}
			closed = ReconciledCommand{ComputerID: r.computerID, CommandID: r.id,
				Status: rec.Status, Reason: reason, BackendInvoked: invoked}
			return nil
		})
		if err != nil {
			return out, err
		}
		out.Examined++
		if closed.CommandID != "" {
			out.Closed = append(out.Closed, closed)
		}
	}
	return out, nil
}

// AppendOutput appends buffered stdout/stderr produced by a dispatched command,
// ordered by seq. Late or duplicate chunks (seq <= current) are ignored so a
// reconnecting backend can replay safely.
func (s *Store) AppendOutput(ctx context.Context, scope contracts.Scope, computerID, commandID, stdout, stderr string, seq int64) error {
	if seq < 0 {
		return invalid("output sequence must not be negative")
	}
	if len(stdout) > maxOutputBytes || len(stderr) > maxOutputBytes {
		return invalid("output chunk exceeds 4 MiB")
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getCommand(ctx, tx, scope, computerID, commandID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			UPDATE ahp_compute.commands
			SET stdout = stdout || $3, stderr = stderr || $4,
			    output_seq = $5, updated_at = clock_timestamp()
			WHERE computer_id = $1 AND id = $2
			  AND status = 'dispatched' AND output_seq < $5`,
			computerID, commandID, stdout, stderr, seq)
		return err
	})
}

// Complete records the terminal outcome of a dispatched command. A command whose
// cancellation was requested settles as CANCELLED regardless of exit code. It is
// idempotent: replaying after a terminal status returns the recorded outcome.
func (s *Store) Complete(ctx context.Context, scope contracts.Scope, computerID, commandID string, exitCode int, stdout, stderr string) (CommandRecord, error) {
	return s.CompleteWithOutput(ctx, scope, computerID, commandID, exitCode, stdout, stderr, false)
}

// CompleteWithOutput is Complete plus the truth about the captured output:
// outputTruncated says the backend had to stop reading at the per-stream cap, so the
// log that reaches an observer is a prefix, not the whole. It is recorded in the same
// transaction as the outcome, because a mark written afterwards is a mark that can be
// lost - and a lost mark is exactly the silent downgrade it exists to prevent. The flag
// is sticky: once a stream has been cut, no later write may claim the log is complete.
func (s *Store) CompleteWithOutput(ctx context.Context, scope contracts.Scope, computerID, commandID string, exitCode int, stdout, stderr string, outputTruncated bool) (CommandRecord, error) {
	var out CommandRecord
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		cur, err := s.getCommand(ctx, tx, scope, computerID, commandID)
		if err != nil {
			return err
		}
		if cur.Status.Terminal() {
			out = cur
			return nil
		}
		if cur.Status != CommandDispatched {
			return contracts.NewError(contracts.ErrConflict,
				"a command must be dispatched before it can complete")
		}
		baseStatus := CommandSucceeded
		if exitCode != 0 {
			baseStatus = CommandFailed
		}
		// A command whose cancellation was requested settles as CANCELLED
		// regardless of exit code; the flag is authoritative in the row.
		rec, err := scanCommand(tx.QueryRow(ctx, `
			UPDATE ahp_compute.commands
			SET status = CASE WHEN cancel_requested THEN 'cancelled' ELSE $3 END,
			    exit_code = $4, has_exit = true,
			    stdout = stdout || $5, stderr = stderr || $6,
			    output_truncated = output_truncated OR $7,
			    updated_at = clock_timestamp()
			WHERE computer_id = $1 AND id = $2 AND status = 'dispatched'
			RETURNING `+commandSelect,
			computerID, commandID, string(baseStatus), exitCode, stdout, stderr, outputTruncated))
		if err != nil {
			return err
		}
		out = rec
		return nil
	})
	return out, err
}

// Fence is the side-effect guard. An ACCEPTED command that is fenced is closed
// forever (FENCED) and its key may never be dispatched. A DISPATCHED command
// whose outcome is unknown becomes SIDE_EFFECT_UNKNOWN: the ledger refuses to
// claim it either ran or did not, so the caller must reconcile instead of
// blindly retrying. Idempotent for already-terminal records.
func (s *Store) Fence(ctx context.Context, scope contracts.Scope, computerID, commandID string) (CommandRecord, error) {
	var out CommandRecord
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		cur, err := s.getCommand(ctx, tx, scope, computerID, commandID)
		if err != nil {
			return err
		}
		if cur.Status.Terminal() {
			out = cur
			return nil
		}
		status := CommandFenced
		if cur.Status == CommandDispatched {
			status = CommandSideEffectUnknown
		}
		rec, err := scanCommand(tx.QueryRow(ctx, `
			UPDATE ahp_compute.commands
			SET status = $3, updated_at = clock_timestamp()
			WHERE computer_id = $1 AND id = $2
			  AND status IN ('accepted','dispatched')
			RETURNING `+commandSelect,
			computerID, commandID, string(status)))
		if err != nil {
			return err
		}
		out = rec
		return nil
	})
	return out, err
}

// Cancel requests cancellation of a not-yet-settled command. A command still
// ACCEPTED closes immediately as CANCELLED (CancelCancelled). A DISPATCHED
// command cannot be proven not to have run, so it is held for reconciliation:
// cancel_requested is set and the phase is CancelUnderReview ("N operations
// under review"). Cancelling an already-terminal command is a conflict.
func (s *Store) Cancel(ctx context.Context, scope contracts.Scope, computerID, commandID string) (CommandRecord, CancelPhase, error) {
	var out CommandRecord
	var phase CancelPhase
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		cur, err := s.getCommand(ctx, tx, scope, computerID, commandID)
		if err != nil {
			return err
		}
		if cur.Status.Terminal() {
			return contracts.NewError(contracts.ErrConflict,
				"a settled command cannot be cancelled")
		}
		status := cur.Status
		phase = CancelUnderReview
		if cur.Status == CommandAccepted {
			status = CommandCancelled
			phase = CancelCancelled
		}
		rec, err := scanCommand(tx.QueryRow(ctx, `
			UPDATE ahp_compute.commands
			SET status = $3, cancel_requested = true, updated_at = clock_timestamp()
			WHERE computer_id = $1 AND id = $2 AND status = $4
			RETURNING `+commandSelect,
			computerID, commandID, string(status), string(cur.Status)))
		if err != nil {
			return err
		}
		out = rec
		return nil
	})
	if err != nil {
		return CommandRecord{}, "", err
	}
	return out, phase, nil
}

// Restart re-arms an ACCEPTED command for dispatch after a dropped attempt. It
// is only legal before DISPATCHED; once a command has been dispatched its
// outcome is governed by Complete/Fence/Cancel, never by retry.
func (s *Store) Restart(ctx context.Context, scope contracts.Scope, computerID, commandID string) (CommandRecord, error) {
	var out CommandRecord
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		cur, err := s.getCommand(ctx, tx, scope, computerID, commandID)
		if err != nil {
			return err
		}
		if cur.Status != CommandAccepted {
			return contracts.NewError(contracts.ErrConflict,
				"only an accepted command can be restarted; dispatched outcomes are not retried")
		}
		out = cur
		return nil
	})
	return out, err
}

// GetCommand returns one ledger entry in scope.
func (s *Store) GetCommand(ctx context.Context, scope contracts.Scope, computerID, commandID string) (CommandRecord, error) {
	var out CommandRecord
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		c, err := s.getCommand(ctx, tx, scope, computerID, commandID)
		if err != nil {
			return err
		}
		out = c
		return nil
	})
	return out, err
}

func (s *Store) getCommand(ctx context.Context, tx pgx.Tx, scope contracts.Scope, computerID, commandID string) (CommandRecord, error) {
	if err := validIdentifier(commandID, "command id"); err != nil {
		return CommandRecord{}, err
	}
	row := tx.QueryRow(ctx, `SELECT `+commandSelect+`
		FROM ahp_compute.commands
		WHERE id = $1 AND computer_id = $2
		  AND tenant = $3 AND deployment = $4 AND subject = $5`,
		commandID, computerID, scope.Tenant, scope.Deployment, scope.Subject)
	rec, err := scanCommand(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return CommandRecord{}, notFound("command")
	}
	return rec, err
}

type commandWithDigest struct {
	CommandRecord
	digest          string
	cancelRequested bool
}

func (s *Store) getCommandByKey(ctx context.Context, tx pgx.Tx, scope contracts.Scope, computerID, key string) (commandWithDigest, error) {
	row := tx.QueryRow(ctx, `SELECT `+commandSelect+`, body_digest, cancel_requested
		FROM ahp_compute.commands
		WHERE computer_id = $1 AND tenant = $2 AND deployment = $3 AND subject = $4 AND key = $5`,
		computerID, scope.Tenant, scope.Deployment, scope.Subject, key)
	var rec CommandRecord
	var digest string
	var cancel bool
	var deadline, heartbeat, backend *time.Time
	if err := row.Scan(&rec.ID, &rec.ComputerID, &rec.Incarnation, &rec.Key, &rec.ExecutionID,
		&rec.Status, &rec.Command, &rec.Cwd, &rec.Stdout, &rec.Stderr, &rec.ExitCode,
		&rec.HasExit, &rec.OutputTruncated, &rec.OutputSeq,
		&deadline, &heartbeat, &backend, &rec.TerminalReason, &rec.CreatedAt, &rec.UpdatedAt,
		&digest, &cancel); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return commandWithDigest{}, notFound("command")
		}
		return commandWithDigest{}, fmt.Errorf("compute: scan command: %w", err)
	}
	rec.DispatchDeadline, rec.HeartbeatAt, rec.BackendStartedAt = derefTimes(deadline, heartbeat, backend)
	rec.CreatedAt = rec.CreatedAt.UTC()
	rec.UpdatedAt = rec.UpdatedAt.UTC()
	return commandWithDigest{CommandRecord: rec, digest: digest, cancelRequested: cancel}, nil
}

func scanCommand(row scanner) (CommandRecord, error) {
	var r CommandRecord
	var deadline, heartbeat, backend *time.Time
	err := row.Scan(&r.ID, &r.ComputerID, &r.Incarnation, &r.Key, &r.ExecutionID,
		&r.Status, &r.Command, &r.Cwd, &r.Stdout, &r.Stderr, &r.ExitCode,
		&r.HasExit, &r.OutputTruncated, &r.OutputSeq,
		&deadline, &heartbeat, &backend, &r.TerminalReason, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return CommandRecord{}, notFound("command")
	}
	if err != nil {
		return CommandRecord{}, fmt.Errorf("compute: scan command: %w", err)
	}
	r.DispatchDeadline, r.HeartbeatAt, r.BackendStartedAt = derefTimes(deadline, heartbeat, backend)
	r.CreatedAt = r.CreatedAt.UTC()
	r.UpdatedAt = r.UpdatedAt.UTC()
	return r, nil
}
