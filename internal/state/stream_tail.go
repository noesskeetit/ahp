package state

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// The volatile stream tail is bounded so a run cannot grow memory without limit.
// A single payload is already capped at MaxPayloadBytes by checkPayload; these
// bound the whole tail per run. Exceeding them refuses the append (a durability
// signal the writer treats as a stream hole) rather than growing memory forever.
const (
	maxStreamTailBytes = 32 << 20
	maxStreamTailRows  = 262144
	// terminalStreamKind is the frame that signals a run's terminal status. It is
	// never held in the volatile tail: it is written durably at the boundary, so a
	// live read can never observe a run as finished before that frame is committed.
	terminalStreamKind = "status"
)

// bufferedEvent is one not-yet-durable stream event plus the idempotency digest
// it will carry when the boundary flush writes it.
type bufferedEvent struct {
	ev     StreamEvent
	digest string
}

// runTail is a VOLATILE, bounded in-memory tail of stream events for one run,
// used only when AHP_EXPERIMENT_CHECKPOINT_BOUNDARY is step or turn. It continues
// the dense per-thread sequence from a base captured when the tail opened, so
// cursors are stable across the flush. A crash loses it. The global map
// (Store.streamTails) is guarded only by streamTailsMu for short get/set/delete;
// each tail's own fields are guarded by its per-run mu, so a boundary flush that
// does database work serialises only its OWN run, never every run.
type runTail struct {
	mu       sync.Mutex
	scope    contracts.Scope
	runID    string
	threadID string
	// worker and epoch are bound immutably at open (blocker #4): a later append or
	// flush from a different worker/epoch can never mutate or persist this tail.
	worker string
	epoch  int64
	base   int64 // durable MAX(sequence) captured when the tail opened
	// nextSeq is the next monotonic per-thread cursor to hand out. It is derived
	// from the high-water at open and incremented on every append, and is NEVER
	// recomputed from len(rows): flushes prune already-durable rows, so a
	// base+len(rows) formula would reuse a cursor after a flush. This keeps live
	// cursors strictly increasing across append -> flush -> append -> flush.
	nextSeq int64
	bytes   int
	keys    map[string]string // key -> digest, for idempotency
	rows    []bufferedEvent
	// results-only only. durableThrough is the highest sequence already made durable
	// (or deliberately withheld) by a prior flush; the next flush processes only rows
	// above it, so repeated step-boundary flushes stay O(new rows), not O(rows) each.
	durableThrough int64
	// sealed/retainedAfter bound how long a terminal results-only tail is kept in
	// memory for a live reader to drain the withheld deltas after the run released,
	// because no durable fragment row exists to re-read. The reaper bounds lifetime
	// and global capacity.
	sealed        bool
	retainedAfter time.Time
}

// appendStreamBuffered journals one stream event with a stable cursor and NO
// durable write. Idempotency per (run,key) is preserved in memory against the
// digest. A takeover (different worker/epoch) isolates the old fenced tail and
// rebinds to the current grant; the old volatile events are a declared loss.
func (s *Store) appendStreamBuffered(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64, key, kind string, payload json.RawMessage, digest string) (StreamEvent, error) {
	s.streamTailsMu.Lock()
	t := s.streamTails[runID]
	s.streamTailsMu.Unlock()
	if t == nil || t.scope != scope || t.worker != worker || t.epoch != epoch {
		// Validate the current lease fence (first open OR a takeover), which also
		// reads the durable high-water this tail continues from.
		threadID, base, err := s.openRunTail(ctx, scope, runID, worker, epoch)
		if err != nil {
			return StreamEvent{}, err
		}
		nt := &runTail{scope: scope, runID: runID, threadID: threadID, worker: worker, epoch: epoch, base: base, nextSeq: base + 1, keys: make(map[string]string)}
		s.streamTailsMu.Lock()
		s.streamTails[runID] = nt // replaces (isolates) any fenced tail for the same run
		s.streamTailsMu.Unlock()
		t = nt
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if prev, ok := t.keys[key]; ok {
		if prev != digest {
			return StreamEvent{}, ErrKeyReuse
		}
		for _, be := range t.rows {
			if be.ev.Key == key {
				return be.ev, nil
			}
		}
		return StreamEvent{}, ErrKeyReuse
	}
	// A results-only run tail is SEALED at its terminal boundary; a straggler append
	// from the SAME (still-fenced) worker must never mutate the volatile output, hand
	// out a new cursor, or emit an event after terminal. The identical-key/body replay
	// above already returned before this point; only a NEW key reaches here. Control
	// (journal) tails are never sealed, so this branch is results-only only.
	if s.resultsOnly && t.sealed {
		return StreamEvent{}, contracts.NewError(contracts.ErrConflict, "state: run is terminal; late append refused")
	}
	if t.bytes+len(payload) > maxStreamTailBytes || len(t.rows) >= maxStreamTailRows {
		return StreamEvent{}, contracts.NewError(contracts.ErrConflict, "state: volatile stream tail exceeded its bound; flush required")
	}
	// Defensive: a tail must never hand out a cursor at or below its durable base.
	// Real tails already carry nextSeq > base from openRunTail; this self-heals any
	// tail whose cursor was not initialised and is a no-op otherwise.
	if t.nextSeq <= t.base {
		t.nextSeq = t.base + 1
	}
	ev := StreamEvent{ID: contracts.NewID(), ThreadID: t.threadID, Key: key, Kind: kind, Sequence: t.nextSeq, Payload: payload}
	t.nextSeq++
	t.rows = append(t.rows, bufferedEvent{ev: ev, digest: digest})
	t.keys[key] = digest
	t.bytes += len(payload)
	return ev, nil
}

// openRunTail validates the lease fence (stale epoch, scope, subject erasure) and
// reads the durable MAX(sequence) the volatile tail continues from. It runs on the
// first append (or a takeover rebind), so the fence is enforced but not per fragment.
func (s *Store) openRunTail(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64) (string, int64, error) {
	var threadID string
	var base int64
	err := s.withAppendStreamLease(ctx, scope, runID, worker, epoch, func(tx pgx.Tx, l *Lease) error {
		threadID = l.Thread.ID
		// Continue from GREATEST(durable MAX, coarse high-water). In journal modes the
		// high-water is 0 and this equals MAX (unchanged). In results-only withheld
		// deltas raised the high-water above MAX, so a reopened tail starts strictly
		// above every cursor a live reader already saw -- never a reset or collision.
		return tx.QueryRow(ctx, `SELECT GREATEST(COALESCE(MAX(sequence),0), COALESCE((SELECT stream_high_water FROM ahp_state.threads WHERE id=$1),0)) FROM ahp_state.stream_events WHERE thread_id=$1`, threadID).Scan(&base)
	})
	if err != nil {
		return "", 0, err
	}
	return threadID, base, nil
}

// validateStreamFence re-runs the exact lease fence an AppendStream would (scope,
// run, worker/epoch grant, lease not expired, subject not erased) with no write, so
// an empty or absent experimental tail is still refused with the same STALE_EPOCH /
// NOT_FOUND the durable path would return. It never mutates anything.
func (s *Store) validateStreamFence(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64) error {
	return s.withAppendStreamLease(ctx, scope, runID, worker, epoch, func(pgx.Tx, *Lease) error { return nil })
}

// FlushStream persists a run's volatile tail in one transaction at a checkpoint
// boundary. It re-acquires the full lease fence AND verifies the tail is bound to
// this exact worker/epoch: a stale epoch or a new owner can never persist another
// owner's tail. Within the transaction each buffered key is checked against the
// durable row for key+digest equivalence (mirroring AppendStream's idempotency),
// and inserts keep the buffered sequence, so a cursor/body mismatch is refused,
// never silently accepted by ON CONFLICT. A failure writes nothing and returns;
// the caller must treat it as a durability failure. In chunk mode it is a no-op.
//
// The per-run mu is held across the database work, so an append racing this flush
// on the SAME run blocks until the committed prefix is removed -- no event is
// lost and cursors stay dense. The global map lock is never held across I/O.
func (s *Store) FlushStream(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64) ([]StreamEvent, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if s.boundary == contracts.CheckpointBoundaryChunk {
		return nil, nil
	}
	if s.resultsOnly {
		// Results-only persists no model_delta rows; the boundary flush instead
		// makes completed non-fragment results durable and advances the coarse
		// thread high-water, keeping withheld deltas in the volatile tail for live
		// drain. Isolated path so the batched step/turn journal control is unchanged.
		return s.flushStreamResultsOnly(ctx, scope, runID, worker, epoch)
	}
	s.streamTailsMu.Lock()
	t := s.streamTails[runID]
	s.streamTailsMu.Unlock()
	if t != nil {
		if t.scope != scope {
			// A run outside the caller's scope is absent to it, exactly as the durable
			// path would answer, and its tail is left untouched.
			return nil, notFound("run")
		}
		if t.worker != worker || t.epoch != epoch {
			// Same run, but the caller is not the owner/epoch this tail is bound to
			// (e.g. a new owner after takeover): it must never persist the previous
			// owner's volatile tail.
			return nil, staleEpoch()
		}
	}
	// A boundary flush is acknowledged (returns success) only while this caller still
	// owns the LIVE grant. When the tail is absent -- discarded by a terminal release,
	// or never opened -- or holds nothing yet to persist, there is still no silent ack:
	// validate the fence first, so a writer that lost the lease (release, takeover,
	// expiry) is refused with STALE_EPOCH exactly as it would be at the durable write,
	// and can never mistake a lost-lease flush for a committed terminal. The durable
	// path re-validates this fence inside its own transaction.
	if t == nil {
		if err := s.validateStreamFence(ctx, scope, runID, worker, epoch); err != nil {
			return nil, err
		}
		return nil, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.rows) == 0 {
		if err := s.validateStreamFence(ctx, scope, runID, worker, epoch); err != nil {
			return nil, err
		}
		return nil, nil
	}
	rows := make([]bufferedEvent, len(t.rows))
	copy(rows, t.rows)
	keys := make([]string, 0, len(rows))
	for _, be := range rows {
		keys = append(keys, be.ev.Key)
	}
	var flushedThrough int64
	err := s.withAppendStreamLease(ctx, scope, runID, worker, epoch, func(tx pgx.Tx, l *Lease) error {
		threadID := l.Thread.ID
		existing := map[string]struct {
			sequence int64
			digest   []byte
		}{}
		qrows, err := tx.Query(ctx, `SELECT key, sequence, digest FROM ahp_state.stream_events WHERE thread_id=$1 AND key = ANY($2)`, threadID, keys)
		if err != nil {
			return err
		}
		for qrows.Next() {
			var k string
			var seq int64
			var dg []byte
			if err := qrows.Scan(&k, &seq, &dg); err != nil {
				qrows.Close()
				return err
			}
			existing[k] = struct {
				sequence int64
				digest   []byte
			}{seq, dg}
		}
		if err := qrows.Err(); err != nil {
			qrows.Close()
			return err
		}
		qrows.Close()
		batch := &pgx.Batch{}
		toInsert := 0
		flushedThrough = t.base
		for _, be := range rows {
			// A committed batch makes every snapshotted row durable (newly inserted
			// or already durable and skipped), so the retained suffix is everything
			// appended after the highest snapshotted sequence -- not merely the
			// inserted ones. Advancing over all rows keeps the base dense even when a
			// row is an idempotent replay, so the tail drains and never stalls.
			if be.ev.Sequence > flushedThrough {
				flushedThrough = be.ev.Sequence
			}
			if ex, ok := existing[be.ev.Key]; ok {
				if !digestMatches(ex.digest, be.digest) {
					return ErrKeyReuse // same key, different body: refuse, never overwrite
				}
				continue // idempotent replay already durable
			}
			batch.Queue(`INSERT INTO ahp_state.stream_events
				(id, thread_id, run_id, key, kind, sequence, payload, digest)
				 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				be.ev.ID, threadID, runID, be.ev.Key, be.ev.Kind, be.ev.Sequence, be.ev.Payload, be.digest)
			toInsert++
		}
		if toInsert == 0 {
			return nil
		}
		br := tx.SendBatch(ctx, batch)
		for i := 0; i < toInsert; i++ {
			if _, err := br.Exec(); err != nil {
				_ = br.Close()
				return err // e.g. UNIQUE(thread_id,sequence): a stale/overlapping batch aborts
			}
		}
		return br.Close()
	})
	if err != nil {
		return nil, err // durable write did not commit; tail retained for a bounded retry
	}
	// Commit succeeded. Drop the flushed prefix; keep any suffix appended after the
	// snapshot (appends on this run blocked on t.mu, so normally none). Rebind base.
	kept := make([]bufferedEvent, 0)
	keptKeys := make(map[string]string)
	for _, be := range t.rows {
		if be.ev.Sequence > flushedThrough {
			kept = append(kept, be)
			keptKeys[be.ev.Key] = be.digest
		}
	}
	t.rows = kept
	t.keys = keptKeys
	t.base = flushedThrough
	t.bytes = 0
	for _, be := range kept {
		t.bytes += len(be.ev.Payload)
	}
	out := make([]StreamEvent, 0, len(rows))
	for _, be := range rows {
		out = append(out, be.ev)
	}
	return out, nil
}

// DiscardRunTail frees a run's volatile tail once it is terminal. No-op in chunk
// mode; safe on an unknown run.
//
// In results-only there is no durable model_delta row to re-read, so the tail is
// not deleted here: it is sealed and kept readable until a live reader drains it or
// the bounded reaper reclaims it (TTL / global capacity). This is what prevents
// losing the withheld suffix between the terminal commit and a lagging reader.
func (s *Store) DiscardRunTail(runID string) {
	s.streamTailsMu.Lock()
	if s.resultsOnly {
		s.sealStreamTailLocked(runID)
		s.reapSealedStreamLocked()
		s.streamTailsMu.Unlock()
		return
	}
	delete(s.streamTails, runID)
	s.streamTailsMu.Unlock()
}

// dropTailsForScope / dropTailsForTenant isolate a subject's or tenant's volatile
// tails when it is fenced/erased, so no not-yet-durable content survives the
// fence in memory. Durable rows are removed by the erasure's own cascade; the live
// read path additionally masks erased threads through GetThread (unchanged).
func (s *Store) dropTailsForScope(tenant, subject string) {
	s.streamTailsMu.Lock()
	for runID, t := range s.streamTails {
		if t.scope.Tenant == tenant && t.scope.Subject == subject {
			delete(s.streamTails, runID)
		}
	}
	s.streamTailsMu.Unlock()
}

func (s *Store) dropTailsForTenant(tenant string) {
	s.streamTailsMu.Lock()
	for runID, t := range s.streamTails {
		if t.scope.Tenant == tenant {
			delete(s.streamTails, runID)
		}
	}
	s.streamTailsMu.Unlock()
}

// tailCandidateEvents returns the thread's buffered (volatile) events with
// sequence STRICTLY GREATER THAN the caller's original `after` cursor. It does NOT
// advance from a durable high-water: in results-only a durable model_result / status
// can carry a sequence ABOVE withheld fragments, so the durable high-water would
// silently skip the unread fragment prefix and could expose DONE before the last
// deltas. ReadStreamPage combines these candidates with the durable rows, dedupes by
// sequence, and globally sorts before applying any limit. A terminal "status" frame
// is never buffered, so the volatile set can never itself claim the run is closed.
// Sealed tails are still in the map until the bounded reaper reclaims them, so a
// lagging live reader keeps draining its withheld text until explicit bounded expiry.
func (s *Store) tailCandidateEvents(scope contracts.Scope, threadID string, after int64) []StreamEvent {
	s.streamTailsMu.Lock()
	var tails []*runTail
	for _, t := range s.streamTails {
		if t.scope == scope && t.threadID == threadID {
			tails = append(tails, t)
		}
	}
	s.streamTailsMu.Unlock()
	var cand []StreamEvent
	for _, t := range tails {
		t.mu.Lock()
		for _, be := range t.rows {
			if be.ev.Sequence > after && be.ev.Kind != terminalStreamKind {
				cand = append(cand, be.ev)
			}
		}
		t.mu.Unlock()
	}
	return cand
}

// Results-only sealed-tail bounds: a terminal results-only tail is kept (for live
// drain of withheld deltas, since no durable fragment row exists to re-read) for at
// most sealedStreamRetention, and at most maxSealedStreamTails are retained
// process-wide. This gives streamTails a bounded lifetime and a global capacity --
// no unbounded per-run retention, no per-fragment goroutine or map entry.
//
// sealedStreamRetention MUST cover the frozen outer completion horizon (180s) with
// margin, so a healthy-but-lagging reader that is still draining within its allowed
// deadline never loses a sealed fragment. Chosen bound: 5 minutes (>= 4x the 180s
// window is not required; 5m is 100s of margin over 180s) -- strictly greater than
// the 180s horizon. Controls (chunk/step/turn) are unaffected: they never seal.
const (
	sealedStreamRetention = 5 * time.Minute
	maxSealedStreamTails  = 4096
)

// kindModelDelta is the run's provider text-fragment stream event kind
// (space.StepModelDelta). The state package cannot import space, so the wire
// constant is spelled here directly. It is NEVER persisted in results-only.
const kindModelDelta = "model_delta"

// resultsOnlyPersist decides whether a buffered event becomes durable at a
// results-only boundary flush.
//   - model_delta fragments: NEVER, in any mode (that is the whole point).
//   - STEP: every completed step result is durable (model / tool / compaction /
//     done / error / model_intent).
//   - TURN: ONLY the terminal result/control frames are durable (done / error).
//     Intermediate step results stay volatile -- TURN must not batch-persist every
//     intermediate State row at the end. The final turn snapshot is written by the
//     coordinator's checkpoint primitive, not by this stream flush; service-owned
//     Model/Tool aggregate operation receipts live in their own service ledgers,
//     not here, and are not counted as agent checkpoints.
func resultsOnlyPersist(boundary contracts.CheckpointBoundary, kind string) bool {
	if kind == kindModelDelta {
		return false
	}
	if boundary == contracts.CheckpointBoundaryTurn {
		return kind == "done" || kind == "error"
	}
	return true // STEP
}

// flushStreamResultsOnly persists the run's COMPLETED non-fragment results (model /
// tool / done / model_intent / status, i.e. every kind except model_delta) that were
// appended since the last flush, at the sequences the volatile tail already handed
// them, and advances the coarse per-thread high-water to the highest sequence the
// tail ever handed out (withheld deltas included). NO model_delta row is ever
// inserted, so PostgreSQL holds zero text-fragment rows for this run. The tail rows
// are retained (not pruned) so a live reader keeps draining withheld deltas;
// durableThrough makes repeated step-boundary flushes O(new rows), not O(rows).
//
// It re-runs the full lease fence (scope / worker / epoch / erasure) and is
// idempotent per (key, digest): a re-run inserts nothing. A failure commits nothing
// and leaves durableThrough unchanged so a bounded retry reapplies the same rows.
func (s *Store) flushStreamResultsOnly(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64) ([]StreamEvent, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	s.streamTailsMu.Lock()
	t := s.streamTails[runID]
	if t != nil {
		if t.scope != scope {
			s.streamTailsMu.Unlock()
			return nil, notFound("run")
		}
		if t.worker != worker || t.epoch != epoch {
			s.streamTailsMu.Unlock()
			return nil, staleEpoch()
		}
	}
	s.streamTailsMu.Unlock()
	if t == nil {
		// No live tail for this run: still refuse a lost lease exactly as the durable
		// write would, so a fenced writer never mistakes an empty flush for success.
		if err := s.validateStreamFence(ctx, scope, runID, worker, epoch); err != nil {
			return nil, err
		}
		return nil, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// processedThrough covers EVERY row (fragments and TURN-withheld intermediates
	// alike) so the coarse high-water still advances past cursors already handed to
	// live readers even when those rows are deliberately never persisted.
	type pending struct {
		be     bufferedEvent
		insert bool
	}
	var todo []pending
	processedThrough := t.durableThrough
	keys := make([]string, 0)
	for _, be := range t.rows {
		if be.ev.Sequence <= t.durableThrough {
			continue
		}
		if be.ev.Sequence > processedThrough {
			processedThrough = be.ev.Sequence
		}
		insert := resultsOnlyPersist(s.boundary, be.ev.Kind)
		todo = append(todo, pending{be: be, insert: insert})
		if insert {
			keys = append(keys, be.ev.Key)
		}
	}
	if len(todo) == 0 {
		if err := s.validateStreamFence(ctx, scope, runID, worker, epoch); err != nil {
			return nil, err
		}
		return nil, nil
	}
	var flushed []StreamEvent
	err := s.withAppendStreamLease(ctx, scope, runID, worker, epoch, func(tx pgx.Tx, l *Lease) error {
		threadID := l.Thread.ID
		existing := map[string][]byte{}
		if len(keys) > 0 {
			qrows, err := tx.Query(ctx, `SELECT key, digest FROM ahp_state.stream_events WHERE thread_id=$1 AND key = ANY($2)`, threadID, keys)
			if err != nil {
				return err
			}
			for qrows.Next() {
				var k string
				var dg []byte
				if err := qrows.Scan(&k, &dg); err != nil {
					qrows.Close()
					return err
				}
				existing[k] = dg
			}
			if err := qrows.Err(); err != nil {
				qrows.Close()
				return err
			}
			qrows.Close()
		}
		batch := &pgx.Batch{}
		toInsert := 0
		out := make([]StreamEvent, 0, len(todo))
		for _, p := range todo {
			if !p.insert {
				out = append(out, p.be.ev) // withheld: reported, never durable
				continue
			}
			if dg, ok := existing[p.be.ev.Key]; ok {
				if !digestMatches(dg, p.be.digest) {
					return ErrKeyReuse
				}
				out = append(out, p.be.ev)
				continue // idempotent replay: already durable
			}
			batch.Queue(`INSERT INTO ahp_state.stream_events
				(id, thread_id, run_id, key, kind, sequence, payload, digest)
				 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				p.be.ev.ID, threadID, runID, p.be.ev.Key, p.be.ev.Kind, p.be.ev.Sequence, p.be.ev.Payload, p.be.digest)
			toInsert++
			out = append(out, p.be.ev)
		}
		if toInsert > 0 {
			br := tx.SendBatch(ctx, batch)
			for i := 0; i < toInsert; i++ {
				if _, err := br.Exec(); err != nil {
					_ = br.Close()
					return err // e.g. UNIQUE(thread_id,sequence): stale/overlapping batch aborts
				}
			}
			if err := br.Close(); err != nil {
				return err
			}
		}
		// Coarse high-water: advance past every sequence this tail handed out,
		// including withheld deltas, so no durable record or reopened tail regresses
		// below a live cursor. One column update per flush -- never one marker per
		// fragment.
		if _, err := tx.Exec(ctx,
			`UPDATE ahp_state.threads SET stream_high_water = GREATEST(stream_high_water, $2) WHERE id=$1`,
			threadID, processedThrough); err != nil {
			return err
		}
		flushed = out
		return nil
	})
	if err != nil {
		return nil, err // durable write did not commit; durableThrough unchanged for a bounded retry
	}
	t.durableThrough = processedThrough
	// Rows and idempotency keys are RETAINED (not pruned) until the tail is reclaimed
	// at bounded terminal expiry: this keeps an identical (key,digest) replay resolving
	// to its ORIGINAL event -- with its original cursor -- across the boundary, instead
	// of falling through to a NEW sequence or a spurious KEY_REUSE. Memory is still
	// bounded per-run by maxStreamTailBytes/maxStreamTailRows (enforced in
	// appendStreamBuffered) and per-process by the sealed-tail reaper; durable results
	// additionally live in stream_events. The read path de-duplicates by sequence, so
	// keeping the durable copy in memory never shows a duplicate to a reader.
	return flushed, nil
}

// sealStreamTailLocked marks a terminal results-only tail retained and enqueues it
// for bounded reclamation. Caller must hold streamTailsMu; per-run fields are then
// guarded by the tail's own mu. A second seal is a no-op so the FIFO is duplicate-free.
func (s *Store) sealStreamTailLocked(runID string) {
	t := s.streamTails[runID]
	if t == nil {
		return
	}
	// sealed/retainedAfter are per-tail fields guarded by t.mu (the same lock that
	// guards rows/bytes/keys), NOT by the global streamTailsMu. Lock order is always
	// global streamTailsMu (held by the caller) -> per-tail t.mu, never the reverse.
	t.mu.Lock()
	if t.sealed {
		t.mu.Unlock()
		return
	}
	t.sealed = true
	t.retainedAfter = time.Now()
	t.mu.Unlock()
	s.streamSealedOrder = append(s.streamSealedOrder, runID)
}

// reapSealedStreamLocked evicts retained results-only tails past the global capacity
// or the retention window, oldest-retained first. Caller must hold streamTailsMu.
// Amortised O(1): each sealed id is enqueued and dequeued at most once, and the scan
// stops at the first still-valid retained tail.
func (s *Store) reapSealedStreamLocked() {
	now := time.Now()
	for len(s.streamSealedOrder) > 0 {
		head := s.streamSealedOrder[0]
		t := s.streamTails[head]
		if t == nil {
			s.streamSealedOrder = s.streamSealedOrder[1:]
			continue
		}
		t.mu.Lock()
		sealed := t.sealed
		expired := sealed && now.Sub(t.retainedAfter) >= sealedStreamRetention
		t.mu.Unlock()
		if !sealed {
			s.streamSealedOrder = s.streamSealedOrder[1:]
			continue
		}
		over := len(s.streamSealedOrder) > maxSealedStreamTails
		if !over && !expired {
			return
		}
		s.streamSealedOrder = s.streamSealedOrder[1:]
		delete(s.streamTails, head)
	}
}
