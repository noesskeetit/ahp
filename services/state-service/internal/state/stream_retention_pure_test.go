package state

import (
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func retentionStore(resultsOnly bool) *Store {
	return &Store{boundary: contracts.CheckpointBoundaryStep, resultsOnly: resultsOnly, streamTails: make(map[string]*runTail)}
}

// In journal mode (the control) DiscardRunTail deletes the terminal tail. In
// results-only it must SEAL and retain it, because no durable model_delta row
// exists to re-read: a lagging SSE reader drains the withheld suffix from memory.
func TestDiscardRunTailJournalDeletesResultsOnlySeals(t *testing.T) {
	scope := pureScope()
	journal := retentionStore(false)
	journal.streamTails["r"] = &runTail{scope: scope, runID: "r", threadID: "th", worker: "w", epoch: 1, keys: map[string]string{}}
	journal.DiscardRunTail("r")
	if _, ok := journal.streamTails["r"]; ok {
		t.Fatal("journal discard must delete the tail (control unchanged)")
	}

	ro := retentionStore(true)
	ro.streamTails["r"] = &runTail{scope: scope, runID: "r", threadID: "th", worker: "w", epoch: 1, keys: map[string]string{}}
	ro.DiscardRunTail("r")
	tail, ok := ro.streamTails["r"]
	if !ok {
		t.Fatal("results-only discard must retain the sealed tail, not delete it")
	}
	if !tail.sealed {
		t.Fatal("retained tail must be sealed")
	}
	// A second discard must not double-enqueue the sealed FIFO.
	ro.DiscardRunTail("r")
	n := 0
	for _, id := range ro.streamSealedOrder {
		if id == "r" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("sealed FIFO must hold each run once, got %d", n)
	}
}

// The reaper must reclaim expired retained tails and enforce the global capacity,
// oldest-retained first, and leave still-valid tails in place.
func TestStreamRetentionReaperBounds(t *testing.T) {
	scope := pureScope()
	s := retentionStore(true)
	// One expired retained tail -> reclaimed.
	s.streamTails["old"] = &runTail{scope: scope, runID: "old", threadID: "th", sealed: true, retainedAfter: time.Now().Add(-2 * sealedStreamRetention)}
	s.streamSealedOrder = append(s.streamSealedOrder, "old")
	s.reapSealedStreamLocked()
	if _, ok := s.streamTails["old"]; ok {
		t.Fatal("expired retained tail must be reclaimed")
	}
	if len(s.streamSealedOrder) != 0 {
		t.Fatalf("reaped id must leave the FIFO: %+v", s.streamSealedOrder)
	}
	// Over capacity -> oldest evicted down to the bound.
	s = retentionStore(true)
	for i := 0; i < maxSealedStreamTails+5; i++ {
		id := idName(i)
		s.streamTails[id] = &runTail{scope: scope, runID: id, threadID: "th", sealed: true, retainedAfter: time.Now()}
		s.streamSealedOrder = append(s.streamSealedOrder, id)
	}
	s.reapSealedStreamLocked()
	if len(s.streamSealedOrder) > maxSealedStreamTails {
		t.Fatalf("reaper must enforce capacity: %d > %d", len(s.streamSealedOrder), maxSealedStreamTails)
	}
}

// idName builds a deterministic, unique id for the capacity test.
func idName(i int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	out := "r"
	for i >= 0 {
		out += string(letters[i%26])
		i = i/26 - 1
	}
	return out
}
