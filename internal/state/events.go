package state

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// MaxPayloadBytes is the strict upper bound for inbox/stream payloads.
const MaxPayloadBytes = 1 << 20

const (
	maxLimit     = 1000
	defaultLimit = 100
)

var (
	// ErrPayloadTooLarge reports a payload above MaxPayloadBytes.
	ErrPayloadTooLarge = contracts.NewError(contracts.ErrInvalid, "payload exceeds 1MiB limit")
	// ErrPayloadInvalid reports a payload that is not valid JSON.
	ErrPayloadInvalid = contracts.NewError(contracts.ErrInvalid, "payload is not valid JSON")
	// ErrKeyReuse reports a replayed key carrying a different body.
	ErrKeyReuse = contracts.NewError(contracts.ErrKeyReuse, "key reused with a different body")
	// ErrForeignEvent reports an event outside the caller scope or lease thread.
	ErrForeignEvent = contracts.NewError(contracts.ErrNotFound, "event not found in scope")
)

// InboxEvent is one externally delivered signal into a thread. Key dedupes
// per thread; Sequence is dense and monotone per thread under the thread lock.
type InboxEvent struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
	Acked    bool            `json:"acked"`
}

// StreamEvent is one ordered event appended by a lease holder for a thread.
type StreamEvent struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
}

func checkPayload(payload json.RawMessage) error {
	if len(payload) == 0 || !json.Valid(payload) {
		return ErrPayloadInvalid
	}
	if len(payload) > MaxPayloadBytes {
		return ErrPayloadTooLarge
	}
	return nil
}

// digestMatches compares the persisted digest against the freshly computed
// one; any difference in the stored body is a key reuse violation.
func digestMatches(stored []byte, computed string) bool {
	return subtle.ConstantTimeCompare(stored, []byte(computed)) == 1
}

func capLimit(limit int) int {
	if limit <= 0 {
		return defaultLimit
	}
	if limit > maxLimit {
		return maxLimit
	}
	return limit
}

// Enqueue stores an inbox event for the thread, idempotent per (thread,key).
// Replaying the identical body returns the original event; a different body
// under the same key fails with KEY_REUSE_MISMATCH. When the earliest
// unfinished run of the thread is waiting on this kind (or on "*"), it is
// requeued as pending.
func (s *Store) Enqueue(ctx context.Context, scope contracts.Scope, threadID, key, kind string, payload json.RawMessage) (InboxEvent, error) {
	if err := scope.Validate(); err != nil {
		return InboxEvent{}, err
	}
	if threadID == "" || key == "" || kind == "" {
		return InboxEvent{}, contracts.NewError(contracts.ErrInvalid, "thread_id, key and kind are required")
	}
	if err := checkPayload(payload); err != nil {
		return InboxEvent{}, err
	}
	digest, err := contracts.Digest(struct {
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Kind: kind, Payload: payload})
	if err != nil {
		return InboxEvent{}, err
	}
	var out InboxEvent
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkSubjectWritable(ctx, tx, scope); err != nil {
			return err
		}
		var tid string
		err := tx.QueryRow(ctx,
			`SELECT id FROM ahp_state.threads
			  WHERE id = $1 AND tenant = $2 AND deployment = $3 AND subject = $4
			  FOR UPDATE`,
			threadID, scope.Tenant, scope.Deployment, scope.Subject).Scan(&tid)
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrNotFound, "thread not found")
		} else if err != nil {
			return err
		}

		var (
			existing     InboxEvent
			storedDigest []byte
		)
		err = tx.QueryRow(ctx,
			`SELECT id, thread_id, key, kind, sequence, payload, acked, digest
			   FROM ahp_state.inbox_events
			  WHERE thread_id = $1 AND key = $2`, threadID, key).
			Scan(&existing.ID, &existing.ThreadID, &existing.Key, &existing.Kind,
				&existing.Sequence, &existing.Payload, &existing.Acked, &storedDigest)
		if err == nil {
			if !digestMatches(storedDigest, digest) {
				return ErrKeyReuse
			}
			out = existing
			return nil
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		var seq int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(sequence), 0) + 1 FROM ahp_state.inbox_events
			  WHERE thread_id = $1`, threadID).Scan(&seq); err != nil {
			return err
		}

		ev := InboxEvent{ID: contracts.NewID(), ThreadID: threadID, Key: key, Kind: kind, Sequence: seq}
		if _, err := tx.Exec(ctx,
			`INSERT INTO ahp_state.inbox_events
				(id, thread_id, key, kind, sequence, payload, digest)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			ev.ID, ev.ThreadID, ev.Key, ev.Kind, ev.Sequence, payload, digest); err != nil {
			return err
		}

		var (
			runID, status, waitFor string
			runSeq                 int64
		)
		err = tx.QueryRow(ctx,
			`SELECT id, status, wait_for, sequence FROM ahp_state.runs
			  WHERE thread_id = $1 AND status IN ('pending','running','waiting')
			  ORDER BY sequence ASC LIMIT 1`, threadID).
			Scan(&runID, &status, &waitFor, &runSeq)
		if errors.Is(err, pgx.ErrNoRows) {
			status = ""
		} else if err != nil {
			return err
		}
		if status == RunWaiting && (waitFor == kind || waitFor == "*") {
			if _, err := tx.Exec(ctx,
				`UPDATE ahp_state.runs
				    SET status = 'pending', wait_for = '', deadline = NULL
				  WHERE id = $1 AND status = 'waiting'`, runID); err != nil {
				return err
			}
		}
		ev.Payload = payload
		out = ev
		return nil
	})
	if err != nil {
		return InboxEvent{}, err
	}
	return out, nil
}

// ReadInbox returns unacked inbox events with sequence greater than after,
// oldest first, capped at 1000 per call.
func (s *Store) ReadInbox(ctx context.Context, scope contracts.Scope, threadID string, after int64, limit int) ([]InboxEvent, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if _, err := s.GetThread(ctx, scope, threadID); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT e.id, e.thread_id, e.key, e.kind, e.sequence, e.payload, e.acked
		   FROM ahp_state.inbox_events e
		   JOIN ahp_state.threads t ON t.id = e.thread_id
		  WHERE e.thread_id = $1 AND t.tenant = $2 AND t.deployment = $3
		    AND t.subject = $4 AND e.acked = false AND e.sequence > $5
		  ORDER BY e.sequence ASC LIMIT $6`,
		threadID, scope.Tenant, scope.Deployment, scope.Subject, after, capLimit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]InboxEvent, 0)
	for rows.Next() {
		var ev InboxEvent
		if err := rows.Scan(&ev.ID, &ev.ThreadID, &ev.Key, &ev.Kind,
			&ev.Sequence, &ev.Payload, &ev.Acked); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// AckInbox marks inbox events as acknowledged under a valid lease. Every id
// must belong to the leased thread; acknowledging an already acked event is a
// no-op.
func (s *Store) AckInbox(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64, ids []string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	return s.withLease(ctx, scope, runID, worker, epoch, func(tx pgx.Tx, l *Lease) error {
		for _, id := range ids {
			var (
				threadID string
				acked    bool
			)
			err := tx.QueryRow(ctx,
				`SELECT thread_id, acked FROM ahp_state.inbox_events WHERE id = $1`, id).
				Scan(&threadID, &acked)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrForeignEvent
			} else if err != nil {
				return err
			}
			if threadID != l.Thread.ID {
				return ErrForeignEvent
			}
			if acked {
				continue
			}
			tag, err := tx.Exec(ctx,
				`UPDATE ahp_state.inbox_events SET acked = true
				  WHERE id = $1 AND thread_id = $2 AND acked = false`, id, l.Thread.ID)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				return ErrForeignEvent
			}
		}
		return nil
	})
}

// AppendStream appends one stream event to the leased thread, idempotent per
// (thread,key) and strictly ordered by a dense per-thread sequence.
func (s *Store) AppendStream(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64, key, kind string, payload json.RawMessage) (StreamEvent, error) {
	if err := scope.Validate(); err != nil {
		return StreamEvent{}, err
	}
	if key == "" || kind == "" {
		return StreamEvent{}, contracts.NewError(contracts.ErrInvalid, "key and kind are required")
	}
	if err := checkPayload(payload); err != nil {
		return StreamEvent{}, err
	}
	digest, err := contracts.Digest(struct {
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Kind: kind, Payload: payload})
	if err != nil {
		return StreamEvent{}, err
	}
	if s.boundary != contracts.CheckpointBoundaryChunk && kind != terminalStreamKind {
		// Experiment: hold the event in a volatile per-run tail. No PostgreSQL write
		// happens here -- that is what removes the per-fragment commit. The boundary
		// flush persists it in one transaction with the same idempotency digest. The
		// terminal "status" kind is NEVER buffered: it stays on the durable path so a
		// live read can never see a run as closed before that frame is committed.
		return s.appendStreamBuffered(ctx, scope, runID, worker, epoch, key, kind, payload, digest)
	}
	var out StreamEvent
	err = s.withAppendStreamLease(ctx, scope, runID, worker, epoch, func(tx pgx.Tx, l *Lease) error {
		threadID := l.Thread.ID
		var (
			existing     StreamEvent
			storedDigest []byte
		)
		reads := &pgx.Batch{}
		reads.Queue(
			`SELECT id, thread_id, key, kind, sequence, payload, digest
			   FROM ahp_state.stream_events
			  WHERE thread_id = $1 AND key = $2`, threadID, key)
		// GREATEST with the coarse per-thread high-water: a no-op in journal modes
		// (high-water 0 => MAX+1), but in results-only it keeps a durable record
		// strictly above sequences already handed to the volatile tail, so a withheld
		// model_delta can never make this durable row collide with a live cursor.
		reads.Queue(`SELECT GREATEST(COALESCE(MAX(sequence),0), COALESCE((SELECT stream_high_water FROM ahp_state.threads WHERE id=$1),0)) + 1 FROM ahp_state.stream_events WHERE thread_id=$1`, threadID)
		results := tx.SendBatch(ctx, reads)
		err := results.QueryRow().Scan(&existing.ID, &existing.ThreadID, &existing.Key, &existing.Kind,
			&existing.Sequence, &existing.Payload, &storedDigest)
		var seq int64
		sequenceErr := results.QueryRow().Scan(&seq)
		closeErr := results.Close()
		if sequenceErr != nil {
			return sequenceErr
		}
		if closeErr != nil {
			return closeErr
		}
		if err == nil {
			if !digestMatches(storedDigest, digest) {
				return ErrKeyReuse
			}
			out = existing
			return nil
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		ev := StreamEvent{ID: contracts.NewID(), ThreadID: threadID, Key: key, Kind: kind, Sequence: seq}
		if _, err := tx.Exec(ctx,
			`INSERT INTO ahp_state.stream_events
				(id, thread_id, run_id, key, kind, sequence, payload, digest)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			ev.ID, ev.ThreadID, runID, ev.Key, ev.Kind, ev.Sequence, payload, digest); err != nil {
			return err
		}
		ev.Payload = payload
		out = ev
		return nil
	})
	if err != nil {
		return StreamEvent{}, err
	}
	return out, nil
}

// ReadStream returns stream events of a scoped thread after the given
// sequence, oldest first, capped at 1000 per call.
func (s *Store) ReadStream(ctx context.Context, scope contracts.Scope, threadID string, after int64, limit int) ([]StreamEvent, error) {
	page, err := s.ReadStreamPage(ctx, scope, threadID, after, limit, MaxStreamResponseBytes)
	if err != nil {
		return nil, err
	}
	if page.ByteLimited {
		return nil, contracts.NewReasoned(contracts.ErrInvalid, "STREAM_PAGE_EXCEEDS_RESPONSE_BUDGET", "state: stream count page exceeds response byte budget; request a bounded page")
	}
	return page.Events, nil
}
