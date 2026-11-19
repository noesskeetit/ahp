package model

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// genTail is a VOLATILE, bounded in-memory tail of provider fragments for one
// generation, used only when AHP_EXPERIMENT_CHECKPOINT_BOUNDARY is step or turn.
// Its rows exist in this process only: a crash loses them. They are still served
// by ReadDeltas so streaming stays incremental until the boundary flush persists
// them in a single transaction. It is guarded by GenerationStore.tailsMu.
type genTail struct {
	scope contracts.Scope
	base  int64 // durable delta_sequence captured when the tail opened
	bytes int64 // buffered bytes, bounded by maxGenerationStreamBytes
	usage Usage // latest provider usage observed while buffered
	rows  []StreamDelta
	// sealed marks a results-only tail whose generation reached its boundary
	// flush: no further fragments are appended, but the buffered rows stay
	// readable (for live drain) until drained or reaped. sealedAt bounds how long
	// a sealed tail may live so an abandoned reader cannot pin it forever.
	sealed   bool
	sealedAt time.Time
}

// appendOutputBuffered journals one fragment into the volatile tail with a stable
// global cursor. It performs NO durable write; per-fragment PostgreSQL commits are
// exactly what this experiment removes. Ownership/erasure checks happen at flush.
func (s *GenerationStore) appendOutputBuffered(ctx context.Context, scope contracts.Scope, id, text string, usage Usage) error {
	s.tailsMu.Lock()
	t := s.tails[id]
	s.tailsMu.Unlock()
	if t == nil {
		// The base cursor is read WITHOUT the mutex: tailsMu is never held across
		// a database call. A single executor owns a generation, so re-opening here
		// races only itself; the map is reconciled on the way back in.
		base, err := s.openTailBase(ctx, scope, id)
		if err != nil {
			return err
		}
		t = &genTail{scope: scope, base: base}
		s.tailsMu.Lock()
		if existing := s.tails[id]; existing != nil {
			t = existing
		} else {
			s.tails[id] = t
		}
		s.tailsMu.Unlock()
	}
	s.tailsMu.Lock()
	defer s.tailsMu.Unlock()
	if t.scope != scope {
		return contracts.NewError(contracts.ErrConflict, "model: generation tail reused across scope")
	}
	// A results-only generation tail is SEALED once its aggregate has been flushed at
	// the terminal boundary; a straggler fragment append must never extend the volatile
	// output or advance the cursor after terminal. Control (journal) tails are never
	// sealed, so this branch is results-only only.
	if s.resultsOnly && t.sealed {
		return contracts.NewError(contracts.ErrConflict, "model: stream fenced, terminal or limit exceeded")
	}
	newBytes := t.bytes + int64(len(text))
	if newBytes > maxGenerationStreamBytes || int64(len(t.rows)) >= maxGenerationDeltas {
		return contracts.NewError(contracts.ErrConflict, "model: stream fenced, terminal or limit exceeded")
	}
	t.rows = append(t.rows, StreamDelta{GenerationID: id, Sequence: t.base + int64(len(t.rows)) + 1, Text: text})
	t.bytes = newBytes
	t.usage = usage
	return nil
}

// openTailBase reads the durable delta_sequence a volatile tail continues from.
// It refuses a foreign or already-terminal generation so buffering never hides a
// fenced write behind an in-memory success.
func (s *GenerationStore) openTailBase(ctx context.Context, scope contracts.Scope, id string) (int64, error) {
	var seq int64
	err := s.pool.QueryRow(ctx, `SELECT delta_sequence FROM ahp_model.generations
		WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4
		  AND status IN ('PENDING','RUNNING','STREAMING')`,
		id, scope.Tenant, scope.Subject, scope.Deployment).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, contracts.NewError(contracts.ErrConflict, "model: stream fenced, terminal or not in scope")
	}
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// FlushGeneration persists the volatile tail of one generation in a single
// transaction at a checkpoint boundary. It preserves every fragment's cursor,
// re-applies the owner-writable (tenant/subject erasure) and byte/delta/status
// fences, and is idempotent. A failure commits nothing (no partial durable tail)
// and the caller must treat it as a durability failure: never ack a successful
// terminal past a failed flush. In the default chunk mode it is a no-op because
// fragments are already durable per fragment.
func (s *GenerationStore) FlushGeneration(ctx context.Context, scope contracts.Scope, id string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if s.boundary == contracts.CheckpointBoundaryChunk {
		return nil
	}
	if s.resultsOnly {
		// Results-only never writes fragment rows; the boundary flush instead
		// materialises one aggregate generation result and seals the tail for
		// bounded live drain. Kept on its own path so the batched step/turn
		// journal (the control) is byte-for-byte unchanged.
		return s.flushResultsOnly(ctx, scope, id)
	}
	s.tailsMu.Lock()
	t := s.tails[id]
	if t != nil && t.scope != scope {
		s.tailsMu.Unlock()
		return contracts.NewError(contracts.ErrNotFound, "generation not found")
	}
	if t == nil || len(t.rows) == 0 {
		s.tailsMu.Unlock()
		var exists int
		err := s.pool.QueryRow(ctx, `SELECT 1 FROM ahp_model.generations
			WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4`,
			id, scope.Tenant, scope.Subject, scope.Deployment).Scan(&exists)
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrNotFound, "generation not found")
		}
		return err
	}
	rows := make([]StreamDelta, len(t.rows))
	copy(rows, t.rows)
	base, sumBytes, usage := t.base, t.bytes, t.usage
	s.tailsMu.Unlock()

	newSeq := base + int64(len(rows))
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		owner, err := operations.New(s.pool, "ahp_model")
		if err != nil {
			return err
		}
		if err = owner.CheckTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err = owner.CheckSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
			return err
		}
		var got int64
		uerr := tx.QueryRow(ctx, `UPDATE ahp_model.generations
   SET streamed_bytes=streamed_bytes+$2::bigint,
       delta_sequence=$3::bigint,usage_prompt=$4::bigint,usage_completion=$5::bigint,usage_total=$6::bigint,updated_at=now()
   WHERE id=$1 AND tenant=$7 AND subject=$8 AND deployment=$9
     AND status IN ('PENDING','RUNNING','STREAMING') AND NOT cancel_requested
     AND delta_sequence=$12::bigint AND streamed_bytes+$2::bigint<=$10::bigint AND $3::bigint<=$11::bigint
   RETURNING delta_sequence`,
			id, sumBytes, newSeq, usage.PromptTokens, usage.CompletionTokens, usage.TotalTokens,
			scope.Tenant, scope.Subject, scope.Deployment, maxGenerationStreamBytes, maxGenerationDeltas,
			// exactly the base this batch continues from: an overlapping or stale batch
			// (delta_sequence already advanced) applies to zero rows, never a partial write.
			base).Scan(&got)
		if errors.Is(uerr, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrConflict, "model: stream fenced, terminal or limit exceeded")
		}
		if uerr != nil {
			return uerr
		}
		batch := &pgx.Batch{}
		for _, r := range rows {
			batch.Queue(`INSERT INTO ahp_model.generation_deltas(generation_id,sequence,text) VALUES($1,$2,$3)`, id, r.Sequence, r.Text)
		}
		br := tx.SendBatch(ctx, batch)
		for i := 0; i < len(rows); i++ {
			if _, err := br.Exec(); err != nil {
				_ = br.Close()
				return err
			}
		}
		return br.Close()
	})
	if err != nil {
		return err // durable write did not commit; volatile tail is retained for a bounded retry
	}
	s.tailsMu.Lock()
	if cur := s.tails[id]; cur != nil {
		kept := make([]StreamDelta, 0, len(cur.rows))
		var nb int64
		for _, r := range cur.rows {
			if r.Sequence > newSeq {
				kept = append(kept, r)
				nb += int64(len(r.Text))
			}
		}
		cur.rows = kept
		cur.base = newSeq
		cur.bytes = nb
	}
	s.tailsMu.Unlock()
	return nil
}

// DiscardGeneration frees a generation's volatile tail once it is terminal. It is
// a no-op in default chunk mode. It is safe to call on an unknown id.
//
// In results-only retention there is NO durable fragment row to fall back on, so
// the tail is not deleted here: it is SEALED and kept readable until a live reader
// drains it or it is reclaimed by the bounded reaper (TTL / global capacity). This
// is what prevents losing the unread suffix between the terminal commit and a
// reader that is still draining. An unflushed (abandoned) tail is sealed too, so
// every tail has a bounded lifetime regardless of how the generation ended.
func (s *GenerationStore) DiscardGeneration(id string) {
	if s.boundary == contracts.CheckpointBoundaryChunk {
		return
	}
	s.tailsMu.Lock()
	if s.resultsOnly {
		s.sealResultsOnlyTailLocked(id)
		s.reapSealedLocked()
		s.tailsMu.Unlock()
		return
	}
	delete(s.tails, id)
	s.tailsMu.Unlock()
}

// mergeTailInto folds this generation's volatile tail into a durable read, keyed
// by sequence so a flush racing a read can never produce duplicates or holes.
// Durable rows already satisfy the after filter; buffered rows are added only for
// sequences the durable snapshot does not already carry.
func (s *GenerationStore) mergeTailInto(id string, scope contracts.Scope, after int64, durable []StreamDelta) []StreamDelta {
	s.tailsMu.Lock()
	var buffered []StreamDelta
	if t := s.tails[id]; t != nil && t.scope == scope {
		for _, r := range t.rows {
			if r.Sequence > after {
				buffered = append(buffered, r)
			}
		}
	}
	s.tailsMu.Unlock()
	if len(buffered) == 0 {
		return durable
	}
	seen := make(map[int64]int, len(durable)+len(buffered))
	merged := append([]StreamDelta(nil), durable...)
	for i, d := range merged {
		seen[d.Sequence] = i
	}
	for _, r := range buffered {
		if _, ok := seen[r.Sequence]; ok {
			continue
		}
		seen[r.Sequence] = len(merged)
		merged = append(merged, r)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Sequence < merged[j].Sequence })
	return merged
}

// Results-only retention bounds: a sealed tail is kept (for live drain, since no
// fragment row exists to re-read) for at most sealedTailRetention, and at most
// maxSealedGenerations sealed tails are retained process-wide. These give the
// tails map a bounded lifetime and a global capacity -- no unbounded per-generation
// retention, and no goroutine or map entry per fragment.
const (
	sealedTailRetention  = 5 * time.Minute
	maxSealedGenerations = 4096
)

// flushResultsOnly materialises one aggregate generation result at the boundary
// and persists NO fragment rows. It re-applies the same owner-writable
// (tenant/subject erasure) and byte/delta/status fences and the exact base-cursor
// guard the batched journal uses, so a stale or overlapping flush applies to zero
// rows rather than a partial write. On success the tail is sealed and retained for
// bounded live drain; on failure nothing commits and the tail is kept for a retry.
// Idempotent: a second flush of an already-sealed tail is a no-op.
func (s *GenerationStore) flushResultsOnly(ctx context.Context, scope contracts.Scope, id string) error {
	s.tailsMu.Lock()
	t := s.tails[id]
	if t != nil && t.scope != scope {
		s.tailsMu.Unlock()
		return contracts.NewError(contracts.ErrNotFound, "generation not found")
	}
	if t == nil || len(t.rows) == 0 {
		s.tailsMu.Unlock()
		var exists int
		err := s.pool.QueryRow(ctx, `SELECT 1 FROM ahp_model.generations
			WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4`,
			id, scope.Tenant, scope.Subject, scope.Deployment).Scan(&exists)
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrNotFound, "generation not found")
		}
		return err
	}
	if t.sealed {
		s.tailsMu.Unlock()
		return nil // aggregate already durable from a prior successful flush
	}
	rows := make([]StreamDelta, len(t.rows))
	copy(rows, t.rows)
	base, sumBytes, usage := t.base, t.bytes, t.usage
	s.tailsMu.Unlock()

	newSeq := base + int64(len(rows))
	var b strings.Builder
	b.Grow(int(sumBytes))
	for _, r := range rows {
		b.WriteString(r.Text)
	}
	aggregate := b.String()

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		owner, err := operations.New(s.pool, "ahp_model")
		if err != nil {
			return err
		}
		if err = owner.CheckTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err = owner.CheckSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
			return err
		}
		var got int64
		uerr := tx.QueryRow(ctx, `UPDATE ahp_model.generations
   SET output=$2, output_from_deltas=false,
       streamed_bytes=streamed_bytes+$3::bigint,
       delta_sequence=$4::bigint,
       usage_prompt=$5,usage_completion=$6,usage_total=$7,updated_at=now()
   WHERE id=$1 AND tenant=$8 AND subject=$9 AND deployment=$10
     AND status IN ('PENDING','RUNNING','STREAMING') AND NOT cancel_requested
     AND delta_sequence=$11::bigint AND streamed_bytes+$3::bigint<=$12::bigint AND $4::bigint<=$13::bigint
   RETURNING delta_sequence`,
			id, aggregate, sumBytes, newSeq, usage.PromptTokens, usage.CompletionTokens, usage.TotalTokens,
			scope.Tenant, scope.Subject, scope.Deployment, base, maxGenerationStreamBytes, maxGenerationDeltas).Scan(&got)
		if errors.Is(uerr, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrConflict, "model: stream fenced, terminal or limit exceeded")
		}
		if uerr != nil {
			return uerr
		}
		// Deliberately ZERO generation_deltas INSERT statements: results-only keeps
		// no fragment row in PostgreSQL. This single UPDATE is the whole durable write.
		return nil
	})
	if err != nil {
		return err // aggregate did not commit; tail retained for a bounded retry
	}
	s.tailsMu.Lock()
	s.sealResultsOnlyTailLocked(id)
	s.reapSealedLocked()
	s.tailsMu.Unlock()
	return nil
}

// sealResultsOnlyTailLocked marks a tail sealed and enqueues it for bounded
// reclamation. Caller must hold tailsMu. A second seal of the same tail is a
// no-op so the FIFO carries each sealed tail at most once.
func (s *GenerationStore) sealResultsOnlyTailLocked(id string) {
	t := s.tails[id]
	if t == nil || t.sealed {
		return
	}
	t.sealed = true
	t.sealedAt = time.Now()
	s.sealedOrder = append(s.sealedOrder, id)
}

// reapSealedLocked evicts sealed tails that exceeded the global capacity or the
// retention window, oldest-sealed first. Caller must hold tailsMu. Work is
// amortised O(1): every sealed id is enqueued and dequeued at most once, and the
// scan stops at the first still-valid sealed tail.
func (s *GenerationStore) reapSealedLocked() {
	now := time.Now()
	for len(s.sealedOrder) > 0 {
		head := s.sealedOrder[0]
		t := s.tails[head]
		if t == nil || !t.sealed {
			s.sealedOrder = s.sealedOrder[1:]
			continue
		}
		over := len(s.sealedOrder) > maxSealedGenerations
		expired := now.Sub(t.sealedAt) >= sealedTailRetention
		if !over && !expired {
			return
		}
		s.sealedOrder = s.sealedOrder[1:]
		delete(s.tails, head)
	}
}
