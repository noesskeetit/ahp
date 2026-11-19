package model

import (
	"context"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// These tests cover the parts of the volatile generation tail that never touch
// PostgreSQL: the dense per-generation cursor, the byte bound, the immutable scope
// binding, and the read-side merge that folds buffered fragments into a durable
// ReadDeltas page. They seed a genTail directly so appendOutputBuffered skips
// openTailBase (its only durable read) and runs with no database at all. The durable
// flush/race/erasure behaviour is covered by checkpoint_boundary_test.go (real PG).

func pureGenStore(boundary contracts.CheckpointBoundary) *GenerationStore {
	return &GenerationStore{boundary: boundary, tails: make(map[string]*genTail)}
}

func pureGenScope() contracts.Scope {
	return contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}
}

// TestModelTailPureAppendDense: buffered fragments continue a dense sequence from
// the captured base with no durable write.
func TestCheckpointBoundaryModelPureAppendDense(t *testing.T) {
	ctx := context.Background()
	scope := pureGenScope()
	s := pureGenStore(contracts.CheckpointBoundaryStep)
	s.tails["g1"] = &genTail{scope: scope, base: 5}
	if err := s.appendOutputBuffered(ctx, scope, "g1", "a", Usage{TotalTokens: 1}); err != nil {
		t.Fatal(err)
	}
	s.tailsMu.Lock()
	tail := s.tails["g1"]
	firstSeq := tail.rows[0].Sequence
	s.tailsMu.Unlock()
	if firstSeq != 6 {
		t.Fatalf("first buffered fragment must be base+1=%d, got %d", 6, firstSeq)
	}
	if err := s.appendOutputBuffered(ctx, scope, "g1", "b", Usage{}); err != nil {
		t.Fatal(err)
	}
	s.tailsMu.Lock()
	secondSeq := s.tails["g1"].rows[1].Sequence
	s.tailsMu.Unlock()
	if secondSeq != 7 {
		t.Fatalf("second buffered fragment must be dense=%d, got %d", 7, secondSeq)
	}
}

// TestModelTailPureByteBound: the byte bound still refuses in experiment mode rather
// than growing memory without limit.
func TestCheckpointBoundaryModelPureByteBound(t *testing.T) {
	ctx := context.Background()
	scope := pureGenScope()
	s := pureGenStore(contracts.CheckpointBoundaryStep)
	s.tails["g1"] = &genTail{scope: scope, bytes: maxGenerationStreamBytes}
	if err := s.appendOutputBuffered(ctx, scope, "g1", "x", Usage{}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("oversized buffered tail must be refused, got %v", err)
	}
}

// TestModelTailPureScopeImmutable: a fragment appended under a different scope is
// refused so one tenant/subject's volatile tail can never be written by another.
func TestCheckpointBoundaryModelPureScopeImmutable(t *testing.T) {
	ctx := context.Background()
	scope := pureGenScope()
	s := pureGenStore(contracts.CheckpointBoundaryTurn)
	s.tails["g1"] = &genTail{scope: scope, base: 0}
	other := pureGenScope()
	other.Subject = "intruder"
	if err := s.appendOutputBuffered(ctx, other, "g1", "x", Usage{}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("foreign-scope append must be refused, got %v", err)
	}
}

// TestModelTailPureMergeReadBeforeFlush: ReadDeltas-style merge shows buffered
// fragments (read-before-flush incremental streaming) after the durable prefix, in
// order, deduped by sequence and respecting the after cursor; a foreign tail is
// ignored.
func TestCheckpointBoundaryModelPureMergeReadBeforeFlush(t *testing.T) {
	scope := pureGenScope()
	s := pureGenStore(contracts.CheckpointBoundaryStep)
	tail := &genTail{scope: scope, base: 5}
	tail.rows = []StreamDelta{
		{GenerationID: "g1", Sequence: 8, Text: "c"},
		{GenerationID: "g1", Sequence: 6, Text: "a"},
		{GenerationID: "g1", Sequence: 7, Text: "b"},
	}
	s.tails["g1"] = tail

	durable := []StreamDelta{{GenerationID: "g1", Sequence: 5, Text: "z"}, {GenerationID: "g1", Sequence: 6, Text: "a"}}
	merged := s.mergeTailInto("g1", scope, 4, durable)
	if len(merged) != 4 {
		t.Fatalf("expected durable 5,6 + buffered 7,8 (6 deduped): %+v", merged)
	}
	for i := 1; i < len(merged); i++ {
		if merged[i].Sequence <= merged[i-1].Sequence {
			t.Fatalf("merge not ordered: %+v", merged)
		}
	}
	if merged[len(merged)-1].Text != "c" || merged[len(merged)-1].Sequence != 8 {
		t.Fatalf("highest buffered fragment missing: %+v", merged)
	}
	// after cursor: only fragments strictly after 6 surface from the buffer.
	tailOnly := s.mergeTailInto("g1", scope, 6, nil)
	if len(tailOnly) != 2 || tailOnly[0].Sequence != 7 || tailOnly[1].Sequence != 8 {
		t.Fatalf("after-cursor merge wrong: %+v", tailOnly)
	}
	// Foreign scope: the durable slice is returned unchanged, buffer ignored.
	foreign := pureGenScope()
	foreign.Subject = "intruder"
	got := s.mergeTailInto("g1", foreign, 0, durable)
	if len(got) != len(durable) {
		t.Fatalf("foreign scope must not see the buffered tail: %+v", got)
	}
}
