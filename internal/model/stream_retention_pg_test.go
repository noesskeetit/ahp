package model

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// resultsOnlyFragState reads the two facts that prove results-only holds: the
// number of durable generation_deltas fragment rows (must always be 0) and the
// parent's durable output column length + delta_sequence.
func resultsOnlyFragState(t *testing.T, e *modelEnv, id string) (rows int, outBytes int, seq int64) {
	t.Helper()
	ctx := context.Background()
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_model.generation_deltas WHERE generation_id=$1`, id).Scan(&rows); err != nil {
		t.Fatalf("count deltas: %v", err)
	}
	if err := e.pool.QueryRow(ctx, `SELECT octet_length(output), delta_sequence FROM ahp_model.generations WHERE id=$1`, id).Scan(&outBytes, &seq); err != nil {
		t.Fatalf("read parent: %v", err)
	}
	return rows, outBytes, seq
}

// TestResultsOnlyZeroFragmentRowsFullAggregateDrain is the results-only meter: 4000
// original fragments streamed live from the volatile tail, ZERO durable fragment
// rows at every stage (before and after the boundary flush and the terminal), one
// aggregate generation result whose raw output is 44000 bytes with the frozen SHA,
// and full native aggregate + DONE. It pairs the existing step/turn journal controls
// with the results-only retention selector and leaves them untouched.
func TestResultsOnlyZeroFragmentRowsFullAggregateDrain(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	t.Setenv(contracts.EnvStreamRetention, "results-only")
	e := newModelEnv(t)
	ctx := context.Background()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "results-only-4000", "fixture", chatReq(strings.Repeat("x", 1024)), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	var expected strings.Builder
	for i := 0; i < 4000; i++ {
		fragment := fmt.Sprintf("result%04d ", i)
		expected.WriteString(fragment)
		if err := e.gens.AppendOutput(ctx, e.scope, g.ID, fragment, Usage{CompletionTokens: i + 1, TotalTokens: i + 11}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if expected.Len() != 44000 || outputDigest(expected.String()) != "7769e72b2888273b62509ce0855b9f70ed16924c2035d6becfa367c7b0416d61" {
		t.Fatal("results-only output fixture drifted from the frozen meter")
	}
	// BEFORE the boundary flush: zero fragment rows, zero durable output, and the
	// live reader drains all 4000 fragments from the volatile tail with no dupes.
	if rows, out, seq := resultsOnlyFragState(t, e, g.ID); rows != 0 || out != 0 || seq != 0 {
		t.Fatalf("results-only must not persist fragments/output before flush: rows=%d out=%d seq=%d", rows, out, seq)
	}
	drainAll := func(tag string) {
		after := int64(0)
		for seen := 0; after < 4000; {
			page, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, after, 256)
			if err != nil || len(page) == 0 {
				t.Fatalf("%s: live drain stalled at %d: %v", tag, after, err)
			}
			for _, d := range page {
				after++
				seen++
				if d.Sequence != after || d.Text != fmt.Sprintf("result%04d ", after-1) {
					t.Fatalf("%s: fragment identity/loss: seq=%d want=%d", tag, d.Sequence, after)
				}
			}
		}
	}
	drainAll("before-flush")

	// Boundary flush materialises ONE aggregate and STILL writes no fragment rows.
	if err := e.gens.FlushGeneration(ctx, e.scope, g.ID); err != nil {
		t.Fatalf("boundary flush: %v", err)
	}
	if rows, _, _ := resultsOnlyFragState(t, e, g.ID); rows != 0 {
		t.Fatalf("results-only flush persisted %d fragment rows (expected zero)", rows)
	}
	got, err := e.gens.Get(ctx, e.scope, g.ID)
	if err != nil || got.Output != expected.String() || got.DeltaSequence != 4000 {
		t.Fatalf("aggregate not materialised: bytes=%d seq=%d err=%v", len(got.Output), got.DeltaSequence, err)
	}
	if err := e.gens.prepareCompletion(ctx, e.scope, g.ID, expected.String(), expected.String(), "stop", Usage{TotalTokens: 4011}); err != nil {
		t.Fatalf("prepareCompletion: %v", err)
	}
	final, err := e.gens.FinalizeWithOutput(ctx, e.scope, g.ID, GenCompleted, "", expected.String(), Usage{TotalTokens: 4011}, "stop")
	if err != nil || final.Output != expected.String() || final.DeltaSequence != 4000 || final.Status != GenCompleted {
		t.Fatalf("terminal aggregate changed: %v err=%v", final, err)
	}
	// AFTER terminal: still zero fragment rows, and a live reader that is still
	// draining sees the whole withheld suffix (sealed tail retained, not deleted).
	if rows, _, _ := resultsOnlyFragState(t, e, g.ID); rows != 0 {
		t.Fatalf("results-only terminal persisted %d fragment rows", rows)
	}
	drainAll("after-terminal")
}

// TestResultsOnlyFlushFailsFencedGeneration proves the boundary aggregate honours
// the fence: a fenced (subject-erased) generation refuses the flush and commits
// neither a fragment row nor the aggregate, so no successful terminal is possible.
func TestResultsOnlyFlushFailsFencedGeneration(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "turn")
	t.Setenv(contracts.EnvStreamRetention, "results-only")
	e := newModelEnv(t)
	ctx := context.Background()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "results-only-fenced", "fixture", chatReq("hi"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.gens.AppendOutput(ctx, e.scope, g.ID, "text", Usage{TotalTokens: 3}); err != nil {
		t.Fatal(err)
	}
	if err := e.ledger.FenceSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if err := e.gens.FlushGeneration(ctx, e.scope, g.ID); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("fenced flush must refuse, got %v", err)
	}
	if rows, out, seq := resultsOnlyFragState(t, e, g.ID); rows != 0 || out != 0 || seq != 0 {
		t.Fatalf("failed flush committed durable state: rows=%d out=%d seq=%d", rows, out, seq)
	}
}
