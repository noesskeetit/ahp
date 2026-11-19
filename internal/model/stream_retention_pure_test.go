package model

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func pureScope() contracts.Scope {
	return contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}
}

// appendOutputBuffered must journal into the volatile tail with NO durable write.
// Seeding the tail first skips openRunTail/openTailBase (the only DB touch), so
// this proves per-fragment buffering performs no PostgreSQL work -- exactly what
// results-only and the batched journal rely on.
func TestResultsOnlyBufferingHasNoDurableWriteAndDenseCursor(t *testing.T) {
	ctx := context.Background()
	scope := pureScope()
	s := &GenerationStore{boundary: contracts.CheckpointBoundaryStep, resultsOnly: true, tails: map[string]*genTail{}}
	s.tails["g1"] = &genTail{scope: scope, base: 0}
	for i, txt := range []string{"a", "bb", "ccc"} {
		if err := s.appendOutputBuffered(ctx, scope, "g1", txt, Usage{TotalTokens: i + 1}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	tail := s.tails["g1"]
	if len(tail.rows) != 3 || tail.bytes != 6 || tail.base != 0 {
		t.Fatalf("volatile tail wrong: rows=%d bytes=%d base=%d", len(tail.rows), tail.bytes, tail.base)
	}
	for i, r := range tail.rows {
		if r.Sequence != int64(i+1) {
			t.Fatalf("cursor must be dense from base+1: row %d seq=%d", i, r.Sequence)
		}
	}
}

// DiscardGeneration in results-only must SEAL (retain), not delete, so a lagging
// reader can still drain the withheld suffix; and the reaper must bound both the
// retention window and the global count.
func TestResultsOnlyDiscardSealsAndReaperBounds(t *testing.T) {
	scope := pureScope()
	s := &GenerationStore{boundary: contracts.CheckpointBoundaryTurn, resultsOnly: true, tails: map[string]*genTail{}}
	s.tails["g1"] = &genTail{scope: scope, base: 0, rows: []StreamDelta{{GenerationID: "g1", Sequence: 1, Text: "x"}}}
	s.DiscardGeneration("g1")
	if _, ok := s.tails["g1"]; !ok {
		t.Fatal("results-only discard must retain the sealed tail, not delete it")
	}
	if !s.tails["g1"].sealed {
		t.Fatal("retained tail must be sealed")
	}
	// Expired sealed tail is reaped.
	s.tails["g1"].sealedAt = time.Now().Add(-2 * sealedTailRetention)
	s.reapSealedLocked()
	if _, ok := s.tails["g1"]; ok {
		t.Fatal("expired sealed tail must be reclaimed")
	}
	if len(s.sealedOrder) != 0 {
		t.Fatalf("reaped id must leave the FIFO: %+v", s.sealedOrder)
	}
	// Global capacity: fill beyond the bound, reap evicts oldest-sealed first.
	for i := 0; i <= maxSealedGenerations; i++ {
		s.DiscardGeneration("g" + time.Now().Format("20060102150405.000000"))
	}
	for i := 0; i < maxSealedGenerations+1; i++ {
		id := idName(i)
		s.tails[id] = &genTail{scope: scope, sealed: true, sealedAt: time.Now().Add(-time.Duration(i) * time.Second)}
		s.sealedOrder = append(s.sealedOrder, id)
	}
	s.reapSealedLocked()
	if len(s.sealedOrder) > maxSealedGenerations {
		t.Fatalf("reaper must enforce global capacity: %d > %d", len(s.sealedOrder), maxSealedGenerations)
	}
}

// idName is a small deterministic id generator for the capacity test.
func idName(i int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	out := "g"
	for i >= 0 {
		out += string(letters[i%26])
		i = i/26 - 1
	}
	return out
}

// Fix #1 (model plane, pure): once a results-only generation tail is SEALED at its
// terminal flush, a straggler fragment append must be refused outright -- it must
// not extend the volatile output or advance the cursor after terminal. A control
// (journal) tail is never sealed, so the same append still buffers there. Seeding the
// tail keeps this DB-free.
func TestResultsOnlySealedTailRefusesLateAppend(t *testing.T) {
	ctx := context.Background()
	scope := pureScope()
	s := &GenerationStore{boundary: contracts.CheckpointBoundaryTurn, resultsOnly: true, tails: map[string]*genTail{}}
	s.tails["g1"] = &genTail{scope: scope, base: 0, sealed: true,
		rows:  []StreamDelta{{GenerationID: "g1", Sequence: 1, Text: "a"}},
		bytes: 1}
	if err := s.appendOutputBuffered(ctx, scope, "g1", "late", Usage{TotalTokens: 1}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("append to a sealed results-only tail must be refused as CONFLICT: %v", err)
	}
	tail := s.tails["g1"]
	if len(tail.rows) != 1 || tail.bytes != 1 {
		t.Fatalf("refused late append must not mutate the sealed tail: rows=%d bytes=%d", len(tail.rows), tail.bytes)
	}
	// Control (journal) tail: sealed is never set, so the buffer still accepts it.
	ctrl := &GenerationStore{boundary: contracts.CheckpointBoundaryStep, resultsOnly: false, tails: map[string]*genTail{}}
	ctrl.tails["g2"] = &genTail{scope: scope, base: 0, rows: []StreamDelta{{GenerationID: "g2", Sequence: 1, Text: "a"}}, bytes: 1}
	if err := ctrl.appendOutputBuffered(ctx, scope, "g2", "next", Usage{}); err != nil {
		t.Fatalf("journal (control) append must still buffer: %v", err)
	}
	if len(ctrl.tails["g2"].rows) != 2 {
		t.Fatalf("control append must extend the journal tail: rows=%d", len(ctrl.tails["g2"].rows))
	}
}
