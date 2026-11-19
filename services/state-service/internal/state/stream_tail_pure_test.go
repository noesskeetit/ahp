package state

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// These tests exercise the VOLATILE tail logic that does NOT touch PostgreSQL: the
// dense per-run cursor assignment, in-memory idempotency, byte bound, immutable
// worker/epoch/scope binding on flush, and the read-side merge that folds buffered
// events into a durable page. They seed a runTail directly so the code path skips
// openRunTail (the only durable read), and therefore run with no database at all.
// The durable flush/race/takeover behaviour is covered by stream_checkpoint_boundary_test.go
// (real PostgreSQL).

// mustJSON builds a valid json.RawMessage from a Go string without raw-string
// literals, keeping the composite literals below readable.
func mustJSON(s string) json.RawMessage { return json.RawMessage(s) }

func pureStore(boundary contracts.CheckpointBoundary) *Store {
	return &Store{boundary: boundary, streamTails: make(map[string]*runTail)}
}

func pureScope() contracts.Scope {
	return contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}
}

func seedTail(s *Store, runID string, t *runTail) {
	s.streamTails[runID] = t
}

// TestStreamTailPureAppendDenseAndIdempotent: buffered events continue a dense
// sequence from base with no durable write, and a replay of the same key+digest
// returns the stored event (same cursor) while a different digest is refused.
func TestStreamBoundaryTailPureAppendDenseAndIdempotent(t *testing.T) {
	ctx := context.Background()
	scope := pureScope()
	s := pureStore(contracts.CheckpointBoundaryStep)
	seedTail(s, "run1", &runTail{scope: scope, runID: "run1", threadID: "th1", worker: "w1", epoch: 1, base: 10, keys: map[string]string{}})

	ev1, err := s.appendStreamBuffered(ctx, scope, "run1", "w1", 1, "k1", "model_delta", json.RawMessage(`"a"`), "dig-a")
	if err != nil || ev1.Sequence != 11 || ev1.Key != "k1" || ev1.ThreadID != "th1" {
		t.Fatalf("first append wrong: %+v err=%v", ev1, err)
	}
	ev2, err := s.appendStreamBuffered(ctx, scope, "run1", "w1", 1, "k2", "model_delta", json.RawMessage(`"b"`), "dig-b")
	if err != nil || ev2.Sequence != 12 {
		t.Fatalf("second append must continue densely: %+v err=%v", ev2, err)
	}
	// Idempotent replay: same key, same digest -> the original event, no new cursor.
	again, err := s.appendStreamBuffered(ctx, scope, "run1", "w1", 1, "k1", "model_delta", json.RawMessage(`"a"`), "dig-a")
	if err != nil || again.Sequence != 11 || again.ID != ev1.ID {
		t.Fatalf("idempotent replay must return the original: %+v err=%v", again, err)
	}
	// Same key, different body -> KEY_REUSE_MISMATCH (mirrors AppendStream).
	if _, err := s.appendStreamBuffered(ctx, scope, "run1", "w1", 1, "k1", "model_delta", json.RawMessage(`"tampered"`), "dig-x"); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("digest mismatch must be refused as key reuse, got %v", err)
	}
}

// TestStreamTailPureByteBound: a volatile tail refuses rather than growing memory
// without limit once it crosses its bound.
func TestStreamBoundaryTailPureByteBound(t *testing.T) {
	ctx := context.Background()
	scope := pureScope()
	s := pureStore(contracts.CheckpointBoundaryStep)
	seedTail(s, "run1", &runTail{scope: scope, runID: "run1", threadID: "th1", worker: "w1", epoch: 1, keys: map[string]string{}, bytes: maxStreamTailBytes - 1})
	if _, err := s.appendStreamBuffered(ctx, scope, "run1", "w1", 1, "k1", "model_delta", json.RawMessage(`"too-big"`), "dig"); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("oversized buffered tail must be refused, got %v", err)
	}
}

// TestStreamTailPureFlushImmutableBinding: a flush by a different worker/epoch is
// STALE_EPOCH and a flush from another scope is not-found, both checked BEFORE any
// database work -- so a new owner can never persist the previous owner's tail and a
// foreign caller can never see it (Root blocker #4).
func TestStreamBoundaryTailPureFlushImmutableBinding(t *testing.T) {
	ctx := context.Background()
	scope := pureScope()
	s := pureStore(contracts.CheckpointBoundaryStep)
	seedTail(s, "run1", &runTail{scope: scope, runID: "run1", threadID: "th1", worker: "w1", epoch: 1, keys: map[string]string{}})

	if _, err := s.FlushStream(ctx, scope, "run1", "w2", 1); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("wrong worker flush must be STALE_EPOCH, got %v", err)
	}
	if _, err := s.FlushStream(ctx, scope, "run1", "w1", 2); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("wrong epoch flush must be STALE_EPOCH, got %v", err)
	}
	other := pureScope()
	other.Subject = "intruder"
	if _, err := s.FlushStream(ctx, other, "run1", "w1", 1); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign-scope flush must be not-found, got %v", err)
	}
	// An absent or empty tail is NOT a pure no-op in experimental mode: FlushStream
	// re-validates the live lease fence before it acks, which is a database read, so it
	// is exercised by the real-PostgreSQL FailedFlush/Isolation tests rather than here.
	// The cheap owner/scope refusals above short-circuit before any database touch and
	// are exactly what this no-pool test can prove.
}

// TestStreamTailPureChunkNoOp: the default chunk mode never consults the tail, so
// FlushStream is a pure no-op and the historical path is untouched.
func TestStreamBoundaryTailPureChunkNoOp(t *testing.T) {
	s := pureStore(contracts.CheckpointBoundaryChunk)
	seedTail(s, "run1", &runTail{scope: pureScope(), runID: "run1", threadID: "th1", worker: "w1", epoch: 1, keys: map[string]string{}})
	evs, err := s.FlushStream(context.Background(), pureScope(), "run1", "w1", 1)
	if err != nil || evs != nil {
		t.Fatalf("chunk flush must be a no-op, got %v %v", evs, err)
	}
}

// TestStreamTailPureMergeOrderAndTerminalExclusion: the read-side merge appends
// buffered events after the durable prefix in strict sequence order, dedups by
// sequence, never emits a terminal "status" frame from the volatile tail, and
// advances NextAfter only through the events it returns.
func TestStreamBoundaryTailPureMergeOrderAndTerminalExclusion(t *testing.T) {
	scope := pureScope()
	s := pureStore(contracts.CheckpointBoundaryStep)
	tail := &runTail{scope: scope, runID: "run1", threadID: "th1", worker: "w1", epoch: 1, keys: map[string]string{}}
	// Buffered events, including a terminal frame (must be excluded) and one already
	// in the durable page (must be deduped by sequence).
	tail.rows = []bufferedEvent{
		{ev: StreamEvent{ID: "b13", ThreadID: "th1", Key: "k13", Kind: "model_delta", Sequence: 13, Payload: json.RawMessage(`"c"`)}},
		{ev: StreamEvent{ID: "b11", ThreadID: "th1", Key: "k11", Kind: "model_delta", Sequence: 11, Payload: json.RawMessage(`"a"`)}},
		{ev: StreamEvent{ID: "b12", ThreadID: "th1", Key: "k12", Kind: "model_delta", Sequence: 12, Payload: json.RawMessage(`"b"`)}},
		{ev: StreamEvent{ID: "bterm", ThreadID: "th1", Key: "kterm", Kind: terminalStreamKind, Sequence: 14, Payload: json.RawMessage(`"done"`)}},
		{ev: StreamEvent{ID: "bdup", ThreadID: "th1", Key: "kdup", Kind: "model_delta", Sequence: 10, Payload: json.RawMessage(`"a"`)}},
	}
	seedTail(s, "run1", tail)

	after := int64(9)
	durable := []StreamEvent{{ID: "d10", ThreadID: "th1", Key: "k10", Kind: "model_delta", Sequence: 10, Payload: json.RawMessage(`"a"`)}}
	cand := s.tailCandidateEvents(scope, "th1", after)
	merged, err := collectStreamPageFromEvents(combineStreamEvents(durable, cand, after), after, 256, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	if len(merged.Events) != 4 {
		t.Fatalf("expected 4 events (durable 10 + buffered 11,12,13; terminal excluded, dup dropped): %d", len(merged.Events))
	}
	var last int64
	for i, e := range merged.Events {
		if e.Sequence <= last {
			t.Fatalf("events not in strictly increasing sequence order at %d: %+v", i, merged.Events)
		}
		last = e.Sequence
		if e.Kind == terminalStreamKind {
			t.Fatalf("terminal frame leaked from the volatile tail: %+v", e)
		}
		if e.Sequence == 14 {
			t.Fatalf("terminal sequence 14 leaked despite kind filter: %+v", e)
		}
	}
	if merged.NextAfter != 13 {
		t.Fatalf("NextAfter must advance through returned events only, got %d", merged.NextAfter)
	}
}

// TestStreamTailPureMergeForeignScopeIgnored: a tail belonging to another scope is
// never folded into this scope's page (isolation across the read path too).
func TestStreamBoundaryTailPureMergeForeignScopeIgnored(t *testing.T) {
	s := pureStore(contracts.CheckpointBoundaryStep)
	foreign := pureScope()
	foreign.Tenant = "other"
	tail := &runTail{scope: foreign, runID: "run1", threadID: "th1", worker: "w1", epoch: 1, keys: map[string]string{}}
	tail.rows = []bufferedEvent{{ev: StreamEvent{ThreadID: "th1", Sequence: 21, Kind: "model_delta", Payload: mustJSON("\"x\"")}}}
	seedTail(s, "run1", tail)
	cand := s.tailCandidateEvents(pureScope(), "th1", 0)
	merged, err := collectStreamPageFromEvents(combineStreamEvents(nil, cand, 0), 0, 256, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(merged.Events) != 0 {
		t.Fatalf("foreign-scope tail must not leak into this scope's page: %+v", merged.Events)
	}
}
