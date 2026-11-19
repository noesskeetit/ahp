package state

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// BatchRequest is an atomic batch: checkpoint + stream + run status in one
// transaction. Crash/restart leaves either everything or nothing applied.
type BatchRequest struct {
	Checkpoint *Checkpoint    `json:"checkpoint,omitempty"`
	Key        string         `json:"key"`
	Stream     *StreamOp      `json:"stream,omitempty"`
	RunStatus  *RunStatusOp   `json:"run_status,omitempty"`
	Writes     []PendingWrite `json:"writes,omitempty"`
	WritesNs   string         `json:"writes_namespace,omitempty"`
}

// StreamOp describes one stream append within a batch.
type StreamOp struct {
	Key     string          `json:"key"`
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

// RunStatusOp describes a run status transition within a batch.
//
// DEF-BATCH-1: a terminal Status owes the same explanation Release owes. CloseReason
// is the execution plane's dictionary word and FailureCause the ending plane's own
// sentence, both optional on the wire (older senders stay byte-identical for every
// NON-terminal transition) and both journaled under the same payload keys Release
// uses, so a run ended by a batch is indistinguishable - to a reader and to the
// A->B audit - from a run ended by Release. A terminal status with neither is
// refused: the batch is an atomic PACKAGE of the same operations, not a side door
// around the plane's explanation contract.
type RunStatusOp struct {
	Status       string     `json:"status"`
	CloseReason  string     `json:"close_reason,omitempty"`
	FailureCause string     `json:"failure_cause,omitempty"`
	WaitFor      string     `json:"wait_for,omitempty"`
	Deadline     *time.Time `json:"deadline,omitempty"`
}

// validateAtomicRunStatus applies the same transition policy to both atomic
// storage layouts. Waiting carries Suspend's target/deadline, never a terminal
// reason. Other transitions must not silently discard wait fields.
func validateAtomicRunStatus(op *RunStatusOp, door string) error {
	if op == nil {
		return nil
	}
	st := op.Status
	if st != RunPending && st != RunRunning && st != RunWaiting && !IsTerminalStatus(st) {
		return invalid(door + " must be pending/running/waiting/done/failed/canceled")
	}
	if st == RunWaiting {
		if err := validIdentifier(op.WaitFor, "wait target"); err != nil {
			return err
		}
	} else if op.WaitFor != "" || op.Deadline != nil {
		return invalid(door + " may only include wait fields on a waiting transition")
	}
	if err := runEndReasonPolicy(door, st, op.CloseReason, op.FailureCause, RunDone, RunFailed, RunCanceled); err != nil {
		return err
	}
	if !IsTerminalStatus(st) && (op.CloseReason != "" || op.FailureCause != "") {
		return invalid(door + " may only record a reason on a terminal transition")
	}
	return nil
}

// applyAtomicRunStatus runs while the caller holds currentLease's thread lock.
// In particular readiness is checked after any inbox ACKs in the same commit.
func applyAtomicRunStatus(ctx context.Context, tx pgx.Tx, threadID, runID string, op *RunStatusOp) error {
	if op.Status == RunWaiting {
		return applyRunWaiting(ctx, tx, threadID, runID, op.WaitFor, op.Deadline)
	}
	_, err := applyRunStatusJournal(ctx, tx, runID, op.Status, op.CloseReason, op.FailureCause)
	return err
}

// BatchResult is returned when the batch was committed.
type BatchResult struct {
	Checkpoint  *Checkpoint  `json:"checkpoint,omitempty"`
	StreamEvent *StreamEvent `json:"stream_event,omitempty"`
	OK          bool         `json:"ok"`
}

// PutBatch atomically applies checkpoint + writes + stream + run status in one
// PostgreSQL transaction. The fence is validated once; all mutations share the
// same lease guarantee. Terminal, pending and waiting statuses release the lease.
func (s *Store) PutBatch(ctx context.Context, scope contracts.Scope, f Fence, req BatchRequest) (BatchResult, error) {
	var result BatchResult
	if req.Key == "" {
		return result, contracts.NewError(contracts.ErrInvalid, "batch key required")
	}
	if err := validateAtomicRunStatus(req.RunStatus, "batch run_status"); err != nil {
		return result, err
	}
	var checkpoints []Checkpoint
	if req.Checkpoint != nil {
		checkpoints = []Checkpoint{*req.Checkpoint}
	}
	ctx, err := checkpointReferenceContext(ctx, scope, checkpoints, req.Writes)
	if err != nil {
		return result, err
	}
	err = s.withLease(ctx, scope, f.RunID, f.Worker, f.Epoch, func(tx pgx.Tx, l *Lease) error {
		threadID := l.Thread.ID

		if req.Checkpoint != nil {
			cp := *req.Checkpoint
			cpParts, digest, err := validateCheckpointRequest(&cp)
			if err != nil {
				return err
			}
			metadata := cp.Metadata
			if len(metadata) == 0 {
				metadata = json.RawMessage("null")
			}
			scopedID, err := internalCheckpointID(threadID, cp.Namespace, cp.ID)
			if err != nil {
				return err
			}
			cpKey := req.Key + ":cp"

			var eID, eDigest string
			qerr := tx.QueryRow(ctx, `
SELECT id, body_digest FROM ahp_state.checkpoints
 WHERE thread_id = $1 AND namespace = $2 AND key = $3`,
				threadID, cp.Namespace, cpKey).Scan(&eID, &eDigest)
			if qerr == nil {
				matches, err := checkpointReplayMatches(ctx, tx, threadID, cp, cpParts, eDigest, digest)
				if err != nil {
					return err
				}
				if !matches {
					return contracts.NewError(contracts.ErrKeyReuse, "batch checkpoint key reuse mismatch")
				}
				loaded, err := loadCheckpoint(ctx, tx, threadID, cp.Namespace, eID)
				if err != nil {
					return err
				}
				result.Checkpoint = &loaded
			} else if !errors.Is(qerr, pgx.ErrNoRows) {
				return qerr
			} else {
				parentInternal := ""
				if cp.Parent != "" {
					resolved, err := resolveCheckpoint(ctx, tx, threadID, cp.Namespace, cp.Parent)
					if err != nil {
						return err
					}
					parentInternal = resolved
				}
				if err := markCheckpointPublication(ctx, cp.MetadataEncoding, metadata); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, `
INSERT INTO ahp_state.checkpoints (id, external_id, thread_id, namespace, parent_id, version, metadata, key, body_digest, created_at, metadata_encoding)
VALUES ($1,$2,$3,$4,NULLIF($5,''),$6,$7,$8,$9,clock_timestamp(),$10)`,
					scopedID, cp.ID, threadID, cp.Namespace, parentInternal, cp.Version, metadata, cpKey, digest, cp.MetadataEncoding)
				if err != nil {
					return err
				}
				if err := insertCheckpointParts(ctx, tx, threadID, cp.Namespace, scopedID, cpParts); err != nil {
					return err
				}
				loaded, err := loadCheckpoint(ctx, tx, threadID, cp.Namespace, scopedID)
				if err != nil {
					return err
				}
				result.Checkpoint = &loaded
			}

			if len(req.Writes) > 0 {
				for _, w := range req.Writes {
					if err := validatePayloadEncoding(w.Encoding, w.Data); err != nil {
						return err
					}
					data := w.Data
					if data == nil {
						data = []byte{}
					}
					tag, err := tx.Exec(ctx, `
INSERT INTO ahp_state.checkpoint_writes (checkpoint_id, thread_id, task, idx, channel, data, encoding)
VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (checkpoint_id, task, idx) DO NOTHING`,
						scopedID, threadID, w.Task, w.Index, w.Channel, data, w.Encoding)
					if err != nil {
						return err
					}
					if tag.RowsAffected() > 0 {
						if err := markCheckpointPublication(ctx, w.Encoding, data); err != nil {
							return err
						}
					}
					var existing PendingWrite
					if err := tx.QueryRow(ctx, `SELECT channel,data,encoding FROM ahp_state.checkpoint_writes WHERE checkpoint_id=$1 AND task=$2 AND idx=$3`, scopedID, w.Task, w.Index).Scan(&existing.Channel, &existing.Data, &existing.Encoding); err != nil {
						return err
					}
					if existing.Channel != w.Channel || string(existing.Data) != string(data) || existing.Encoding != w.Encoding {
						return contracts.NewError(contracts.ErrKeyReuse, "batch pending write key reused with a different body")
					}
				}
			}
		}

		if req.Stream != nil {
			// Honor the caller-provided stream key when present (consistent with
			// the standalone AppendStream API); otherwise derive a key from the
			// batch key so a replayed batch stays idempotent.
			sKey := req.Stream.Key
			if sKey == "" {
				sKey = req.Key + ":s"
			}
			digest, err := contracts.Digest(struct {
				Kind    string          `json:"kind"`
				Payload json.RawMessage `json:"payload"`
			}{req.Stream.Kind, req.Stream.Payload})
			if err != nil {
				return err
			}
			var (
				existing     StreamEvent
				storedDigest []byte
			)
			serr := tx.QueryRow(ctx,
				`SELECT id, thread_id, key, kind, sequence, payload, digest
				   FROM ahp_state.stream_events WHERE thread_id = $1 AND key = $2`,
				threadID, sKey).Scan(&existing.ID, &existing.ThreadID, &existing.Key,
				&existing.Kind, &existing.Sequence, &existing.Payload, &storedDigest)
			if serr == nil {
				if !digestMatches(storedDigest, digest) {
					return contracts.NewError(contracts.ErrKeyReuse, "batch stream key reuse mismatch")
				}
				result.StreamEvent = &existing
			} else if !errors.Is(serr, pgx.ErrNoRows) {
				return serr
			} else {
				var seq int64
				if err := tx.QueryRow(ctx,
					`SELECT GREATEST(COALESCE(MAX(sequence),0), COALESCE((SELECT stream_high_water FROM ahp_state.threads WHERE id=$1),0)) + 1 FROM ahp_state.stream_events WHERE thread_id = $1`,
					threadID).Scan(&seq); err != nil {
					return err
				}
				ev := StreamEvent{ID: contracts.NewID(), ThreadID: threadID, Key: sKey, Kind: req.Stream.Kind, Sequence: seq, Payload: req.Stream.Payload}
				if _, err := tx.Exec(ctx,
					`INSERT INTO ahp_state.stream_events (id, thread_id, run_id, key, kind, sequence, payload, digest)
					 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
					ev.ID, ev.ThreadID, f.RunID, ev.Key, ev.Kind, ev.Sequence, ev.Payload, digest); err != nil {
					return err
				}
				result.StreamEvent = &ev
			}
		}

		if req.RunStatus != nil {
			st := req.RunStatus.Status
			if err := applyAtomicRunStatus(ctx, tx, threadID, f.RunID, req.RunStatus); err != nil {
				return err
			}
			// DEF-BATCH-2 (PDF rev3.2 §2.5): a batch that ends a run must publish the
			// completion event in the SAME transaction, exactly as release does, or the
			// outbox diverges from state - the one thing the atomic package exists to
			// prevent. Same door, same predicate, same idempotent insert as release; it
			// runs BEFORE the lease clears so a publish refusal (no verified task
			// authority) rolls the whole terminal back, never leaving a silent dead run.
			if err := s.publishRunCompletionIfTerminal(ctx, tx, scope, f.RunID, st); err != nil {
				return err
			}
			if IsTerminalStatus(st) || st == RunPending || st == RunWaiting {
				if err := s.clearLease(ctx, tx, threadID, f.RunID, f.Worker, f.Epoch); err != nil {
					return err
				}
			}
		}
		result.OK = true
		return nil
	})
	if err != nil {
		return BatchResult{}, err
	}
	return result, nil
}

// applyRunStatusJournal writes a batch's run-status transition, journaling the stated
// reason(s) into the run's EXISTING payload document with exactly the keys and
// semantics release() uses (RunPayloadCloseReason, RunPayloadFailureCause). Nothing
// stated writes nothing: a non-terminal transition, or a terminal one whose reason was
// omitted by an older sender, touches only the status columns - byte-identical to the
// historical batch behavior.
func applyRunStatusJournal(ctx context.Context, tx pgx.Tx, runID, status, closeReason, cause string) (pgconn.CommandTag, error) {
	switch {
	case closeReason == "" && cause == "":
		return tx.Exec(ctx, `
UPDATE ahp_state.runs SET status = $2, wait_for = '', deadline = NULL, updated_at = clock_timestamp()
 WHERE id = $1`, runID, status)
	case cause == "":
		return tx.Exec(ctx, `
UPDATE ahp_state.runs SET status = $2, wait_for = '', deadline = NULL, updated_at = clock_timestamp(),
    payload = jsonb_set(payload, '{`+RunPayloadCloseReason+`}', to_jsonb($3::text), true)
 WHERE id = $1`, runID, status, closeReason)
	case closeReason == "":
		return tx.Exec(ctx, `
UPDATE ahp_state.runs SET status = $2, wait_for = '', deadline = NULL, updated_at = clock_timestamp(),
    payload = jsonb_set(payload, '{`+RunPayloadFailureCause+`}', to_jsonb($3::text), true)
 WHERE id = $1`, runID, status, cause)
	default:
		return tx.Exec(ctx, `
UPDATE ahp_state.runs SET status = $2, wait_for = '', deadline = NULL, updated_at = clock_timestamp(),
    payload = jsonb_set(jsonb_set(payload, '{`+RunPayloadCloseReason+`}', to_jsonb($3::text), true),
                  '{`+RunPayloadFailureCause+`}', to_jsonb($4::text), true)
 WHERE id = $1`, runID, status, closeReason, cause)
	}
}
