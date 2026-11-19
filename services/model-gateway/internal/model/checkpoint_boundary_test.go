package model

import (
	"context"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// durableDeltaState reads the durable delta ledger for a generation: number of
// committed fragment rows, the parent's delta_sequence and streamed_bytes. These
// are the only facts that prove whether a write happened per fragment.
func durableDeltaState(t *testing.T, e *modelEnv, id string) (rows int, seq int64, bytes int64) {
	t.Helper()
	ctx := context.Background()
	var sumBytes *int64
	if err := e.pool.QueryRow(ctx,
		`SELECT count(*), COALESCE(max(sequence),0), sum(octet_length(text))
		   FROM ahp_model.generation_deltas WHERE generation_id=$1`, id).Scan(&rows, &seq, &sumBytes); err != nil {
		t.Fatalf("read delta ledger: %v", err)
	}
	var parentSeq int64
	if err := e.pool.QueryRow(ctx,
		`SELECT delta_sequence, streamed_bytes FROM ahp_model.generations WHERE id=$1`, id).Scan(&parentSeq, &bytes); err != nil {
		t.Fatalf("read parent: %v", err)
	}
	if parentSeq != seq {
		t.Fatalf("delta_sequence=%d but max(fragment sequence)=%d: ledger/parent mismatch", parentSeq, seq)
	}
	return rows, parentSeq, bytes
}

// TestCheckpointBoundaryDefaultWritesPerFragment pins the historical behaviour:
// with the experiment unset, every AppendOutput is its own durable fragment.
func TestCheckpointBoundaryDefaultWritesPerFragment(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "")
	e := newModelEnv(t)
	ctx := context.Background()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "cb-default", "qwen", chatReq("hi"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "tok", Usage{}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		rows, seq, _ := durableDeltaState(t, e, g.ID)
		if rows != i || seq != int64(i) {
			t.Fatalf("chunk mode must write per fragment: after %d got rows=%d seq=%d", i, rows, seq)
		}
	}
}

// TestCheckpointBoundaryNoPerFragmentWrites is the core experiment assertion, run
// identically for step and turn: fragments accumulate in a volatile tail with NO
// durable write, remain readable (incremental streaming), and a single flush
// persists them with stable cursors and no duplicates.
func TestCheckpointBoundaryNoPerFragmentWrites(t *testing.T) {
	for _, mode := range []string{"step", "turn"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Setenv(contracts.EnvCheckpointBoundary, mode)
			e := newModelEnv(t)
			ctx := context.Background()
			g, _, err := e.gens.Submit(ctx, e.scope, "task", "cb-"+mode, "qwen", chatReq("hi"), "", "", 0)
			if err != nil {
				t.Fatal(err)
			}
			texts := []string{"alpha", "beta", "gamma"}
			for _, tx := range texts {
				if err = e.gens.AppendOutput(ctx, e.scope, g.ID, tx, Usage{TotalTokens: 1}); err != nil {
					t.Fatalf("append %q: %v", tx, err)
				}
			}
			// No durable fragment may exist before the boundary flush.
			if rows, seq, _ := durableDeltaState(t, e, g.ID); rows != 0 || seq != 0 {
				t.Fatalf("%s mode buffered no durable write expected, got rows=%d seq=%d", mode, rows, seq)
			}
			// Read-before-flush: the volatile tail is served so SSE stays incremental.
			before, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, 0, 256)
			if err != nil {
				t.Fatal(err)
			}
			if len(before) != 3 || before[0].Sequence != 1 || before[2].Sequence != 3 || before[2].Text != "gamma" {
				t.Fatalf("tail must be readable before flush: %+v", before)
			}
			// Incremental read: after cursor 1 yields only the newer fragments.
			tail, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, 1, 256)
			if err != nil {
				t.Fatal(err)
			}
			if len(tail) != 2 || tail[0].Sequence != 2 {
				t.Fatalf("incremental tail read wrong: %+v", tail)
			}
			// Single boundary flush persists everything in one transaction.
			if err = e.gens.FlushGeneration(ctx, e.scope, g.ID); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if rows, seq, bytes := durableDeltaState(t, e, g.ID); rows != 3 || seq != 3 || bytes != int64(len("alphabetagamma")) {
				t.Fatalf("flush must persist all fragments: rows=%d seq=%d bytes=%d", rows, seq, bytes)
			}
			// Stable cursor across flush, no duplicates, no holes.
			after, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, 0, 256)
			if err != nil {
				t.Fatal(err)
			}
			if len(after) != 3 || after[0].Sequence != 1 || after[2].Sequence != 3 {
				t.Fatalf("cursor must be stable across flush: %+v", after)
			}
			for i := range after {
				if after[i].Text != texts[i] {
					t.Fatalf("fragment %d changed across flush: %q != %q", i, after[i].Text, texts[i])
				}
			}
			// Flushing again is an idempotent no-op (no duplicate rows).
			if err = e.gens.FlushGeneration(ctx, e.scope, g.ID); err != nil {
				t.Fatalf("re-flush: %v", err)
			}
			if rows, _, _ := durableDeltaState(t, e, g.ID); rows != 3 {
				t.Fatalf("re-flush duplicated rows: %d", rows)
			}
		})
	}
}

// TestCheckpointBoundaryFlushFailureKeepsNoPartialRows: a fenced subject makes the
// boundary flush refuse, and NOTHING is written (no partial durable tail).
func TestCheckpointBoundaryFlushFailureKeepsNoPartialRows(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	e := newModelEnv(t)
	ctx := context.Background()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "cb-fail", "qwen", chatReq("hi"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"one", "two"} {
		if err = e.gens.AppendOutput(ctx, e.scope, g.ID, s, Usage{}); err != nil {
			t.Fatal(err)
		}
	}
	if err = e.ledger.FenceSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if err = e.gens.FlushGeneration(ctx, e.scope, g.ID); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("fenced flush must refuse, got %v", err)
	}
	if rows, seq, _ := durableDeltaState(t, e, g.ID); rows != 0 || seq != 0 {
		t.Fatalf("failed flush wrote partial rows: rows=%d seq=%d", rows, seq)
	}
}

// TestCheckpointBoundaryIsolation: a foreign subject cannot flush or read the tail.
func TestCheckpointBoundaryIsolation(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "turn")
	e := newModelEnv(t)
	ctx := context.Background()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "cb-iso", "qwen", chatReq("hi"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "secret", Usage{}); err != nil {
		t.Fatal(err)
	}
	other := e.scope
	other.Subject = "intruder-" + e.u
	if _, err = e.gens.ReadDeltas(ctx, other, g.ID, 0, 256); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign read leaked: %v", err)
	}
	if err = e.gens.FlushGeneration(ctx, other, g.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign flush must not touch the tail: %v", err)
	}
	// The owning subject's tail is intact (still volatile, not silently dropped).
	own, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, 0, 256)
	if err != nil || len(own) != 1 || own[0].Text != "secret" {
		t.Fatalf("owner tail lost after foreign flush: %+v %v", own, err)
	}
}

// TestCheckpointBoundaryModelIncrementalBeforeFlush: a live SSE reader sees provider
// fragments grow incrementally through ReadDeltas BEFORE the generation completes or
// the boundary flush runs, with nothing durable yet (Root blocker #8, store-level proof;
// the composed StreamModel SSE reads this same ReadDeltas).
func TestCheckpointBoundaryModelIncrementalBeforeFlush(t *testing.T) {
	// A live SSE reader must see provider fragments incrementally, BEFORE the
	// generation completes/flushes (Root blocker #8). store-level proof; composed
	// StreamModel SSE reads the same ReadDeltas.
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	e := newModelEnv(t)
	ctx := context.Background()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "cb-incr", "qwen", chatReq("hi"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"a", "b"} {
		if err = e.gens.AppendOutput(ctx, e.scope, g.ID, s, Usage{}); err != nil {
			t.Fatal(err)
		}
	}
	if d, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, 0, 256); err != nil || len(d) != 2 || d[1].Sequence != 2 {
		t.Fatalf("expected 2 volatile deltas, got %d err=%v", len(d), err)
	}
	if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "c", Usage{}); err != nil {
		t.Fatal(err)
	}
	d, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, 0, 256)
	if err != nil || len(d) != 3 || d[2].Sequence != 3 || d[2].Text != "c" {
		t.Fatalf("deltas must grow incrementally before flush: %d err=%v", len(d), err)
	}
	if rows, seq, _ := durableDeltaState(t, e, g.ID); rows != 0 || seq != 0 {
		t.Fatalf("nothing durable before flush: rows=%d seq=%d", rows, seq)
	}
}

// TestCheckpointBoundarySingleFragmentOverLimitRefused: the byte bound still applies
// to a single buffered fragment in experiment mode.
func TestCheckpointBoundarySingleFragmentOverLimitRefused(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	e := newModelEnv(t)
	ctx := context.Background()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "cb-big", "qwen", chatReq("hi"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	huge := strings.Repeat("x", maxGenerationStreamBytes+1)
	if err = e.gens.AppendOutput(ctx, e.scope, g.ID, huge, Usage{}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("oversized fragment must be refused, got %v", err)
	}
	if rows, _, _ := durableDeltaState(t, e, g.ID); rows != 0 {
		t.Fatalf("oversized fragment must not be durable: %d", rows)
	}
}
