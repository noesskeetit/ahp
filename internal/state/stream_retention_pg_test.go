package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func threadHighWater(t *testing.T, s *Store, threadID string) int64 {
	t.Helper()
	var hw int64
	if err := s.Pool().QueryRow(context.Background(), `SELECT stream_high_water FROM ahp_state.threads WHERE id=$1`, threadID).Scan(&hw); err != nil {
		t.Fatalf("read high-water: %v", err)
	}
	return hw
}

// TestStreamResultsOnlyZeroDeltaRowsBoundaryCadence verifies the state-plane half of
// the experiment: NO model_delta fragment is ever persisted, only completed results,
// in both boundary cadences; the live reader sees the fragments before the boundary
// (from the volatile tail); the coarse high-water advances past withheld deltas so
// the durable terminal frame lands strictly above every live cursor (no reset or
// collision); and a foreign epoch is refused. The chunk/step/turn journal controls
// are preserved (this only runs under AHP_EXPERIMENT_STREAM_RETENTION=results-only).
func TestStreamResultsOnlyZeroDeltaRowsBoundaryCadence(t *testing.T) {
	// Skip honestly at the top level, never let a parent report PASS while its
	// subtests silently skipped for want of a database (Root requires no skipped
	// required case counted as PASS). stateDSN is the fixtures' own DSN resolution.
	if stateDSN() == "" {
		t.Skip("set AHP_TEST_DATABASE_URL or DATABASE_URL to run the results-only state tests")
	}
	for _, mode := range []string{"step", "turn"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Setenv(contracts.EnvCheckpointBoundary, mode)
			t.Setenv(contracts.EnvStreamRetention, "results-only")
			s, scope, lease, _ := appendLeaseFixture(t)
			ctx := context.Background()
			th, w, ep := lease.Thread.ID, lease.Worker, lease.Epoch
			app := func(key, kind, body string) {
				if _, err := s.AppendStream(ctx, scope, lease.Run.ID, w, ep, key, kind, json.RawMessage(body)); err != nil {
					t.Fatalf("append %s/%s: %v", kind, key, err)
				}
			}
			// step 1: two text fragments + one completed model result.
			app("d1", "model_delta", `"he"`)
			app("d2", "model_delta", `"llo"`)
			app("m1", "model", `{"text":"hello"}`)
			// Live reader before any boundary: the volatile tail carries all three, dense.
			page, err := s.ReadStream(ctx, scope, th, 0, 256)
			if err != nil || len(page) != 3 || page[0].Kind != "model_delta" || page[2].Kind != "model" {
				t.Fatalf("live read before boundary: %d %+v err=%v", len(page), page, err)
			}
			// STEP persists the completed result now; TURN holds everything until terminal.
			if mode == "step" {
				if _, err := s.FlushStream(ctx, scope, lease.Run.ID, w, ep); err != nil {
					t.Fatalf("step flush: %v", err)
				}
				if rows, _ := durableStreamCount(t, s, th, "model"); rows != 1 {
					t.Fatalf("step mode must persist the completed result at the boundary: rows=%d", rows)
				}
			}
			// Fragments are NEVER durable, before or after a boundary.
			if rows, _ := durableStreamCount(t, s, th, "model_delta"); rows != 0 {
				t.Fatalf("results-only persisted %d model_delta rows at a boundary", rows)
			}
			// step 2: fragments + tool result + done.
			app("d3", "model_delta", `"!"`)
			app("t1", "tool", `{"ok":true}`)
			app("dn", "done", `{"stop":"end_turn"}`)
			if _, err := s.FlushStream(ctx, scope, lease.Run.ID, w, ep); err != nil {
				t.Fatalf("terminal flush: %v", err)
			}
			// Still zero fragment rows total.
			if rows, _ := durableStreamCount(t, s, th, "model_delta"); rows != 0 {
				t.Fatalf("results-only persisted %d model_delta rows after terminal flush", rows)
			}
			// STEP persists every completed step result; TURN persists ONLY the final
			// result/control frames and must leave the intermediate State step-result
			// rows (model/tool) NON-durable -- distinguishing TURN from STEP, not just
			// a checkpoint count.
			if mode == "step" {
				for _, kind := range []string{"model", "tool", "done"} {
					if rows, _ := durableStreamCount(t, s, th, kind); rows != 1 {
						t.Fatalf("STEP: completed %q result must be durable once: rows=%d", kind, rows)
					}
				}
			} else {
				for _, kind := range []string{"model", "tool"} {
					if rows, _ := durableStreamCount(t, s, th, kind); rows != 0 {
						t.Fatalf("TURN: intermediate %q step row must NOT be persisted: rows=%d", kind, rows)
					}
				}
				if rows, _ := durableStreamCount(t, s, th, "done"); rows != 1 {
					t.Fatalf("TURN: the final done result must be durable once: rows=%d", rows)
				}
			}
			// High-water advanced past the withheld delta (sequence 4) to the max handed (6).
			if hw := threadHighWater(t, s, th); hw != 6 {
				t.Fatalf("coarse high-water must cover withheld deltas: got %d want 6", hw)
			}
			// The durable terminal status frame lands strictly above every live cursor.
			app("st", "status", `{"status":"done"}`)
			statusSeq := int64(0)
			if err := s.Pool().QueryRow(ctx, `SELECT max(sequence) FROM ahp_state.stream_events WHERE thread_id=$1 AND kind='status'`, th).Scan(&statusSeq); err != nil {
				t.Fatal(err)
			}
			if statusSeq <= 6 {
				t.Fatalf("terminal frame (seq %d) must be above withheld-delta cursors (<=6): collision/DONE-drop", statusSeq)
			}
			// Foreign epoch can never persist/ack this run's tail.
			if _, err := s.FlushStream(ctx, scope, lease.Run.ID, w, ep+1); contracts.Code(err) != contracts.ErrStaleEpoch {
				t.Fatalf("stale epoch flush must be refused: %v", err)
			}
		})
	}
}

// TestStreamResultsOnlyMonotonicCursorsAcrossFlushes proves defect #2: with the
// volatile tail pruned of already-durable rows at each step flush, newly appended
// events still receive STRICTLY INCREASING cursors (never a reused earlier
// sequence), and the persisted high-water tracks the true high-water. A single
// terminal flush after one append would not catch this.
func TestStreamResultsOnlyMonotonicCursorsAcrossFlushes(t *testing.T) {
	if stateDSN() == "" {
		t.Skip("set AHP_TEST_DATABASE_URL or DATABASE_URL to run the results-only state tests")
	}
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	t.Setenv(contracts.EnvStreamRetention, "results-only")
	s, scope, lease, _ := appendLeaseFixture(t)
	ctx := context.Background()
	th, w, ep := lease.Thread.ID, lease.Worker, lease.Epoch
	var seqs []int64
	app := func(key, kind, body string) int64 {
		ev, err := s.AppendStream(ctx, scope, lease.Run.ID, w, ep, key, kind, json.RawMessage(body))
		if err != nil {
			t.Fatalf("append %s: %v", key, err)
		}
		seqs = append(seqs, ev.Sequence)
		return ev.Sequence
	}
	flush := func(tag string) {
		if _, err := s.FlushStream(ctx, scope, lease.Run.ID, w, ep); err != nil {
			t.Fatalf("flush %s: %v", tag, err)
		}
	}
	// step 1: deltas + result, then flush (prunes the result from the volatile tail).
	d1 := app("d1", "model_delta", `"a"`)
	d2 := app("d2", "model_delta", `"b"`)
	m1 := app("m1", "model", `{"t":1}`)
	flush("s1")
	// step 2: must NOT reuse 1..3.
	d3 := app("d3", "model_delta", `"c"`)
	t1 := app("t1", "tool", `{"ok":1}`)
	flush("s2")
	// terminal.
	dn := app("dn", "done", `{"stop":"end"}`)
	flush("term")

	for _, pair := range []struct {
		name string
		a, b int64
	}{{"d1<d2", d1, d2}, {"d2<m1", d2, m1}, {"m1<d3", m1, d3}, {"d3<t1", d3, t1}, {"t1<dn", t1, dn}} {
		if pair.b <= pair.a {
			t.Fatalf("cursors must strictly increase across flushes; %s: %d<=%d", pair.name, pair.a, pair.b)
		}
	}
	// Every live cursor handed out is <= the persisted high-water, which must equal
	// the highest sequence handed (6).
	if hw := threadHighWater(t, s, th); hw != dn {
		t.Fatalf("persisted high-water must equal the last cursor handed: hw=%d last=%d", hw, dn)
	}
	// No model_delta fragment is durable anywhere, in either step.
	if rows, _ := durableStreamCount(t, s, th, "model_delta"); rows != 0 {
		t.Fatalf("results-only persisted %d model_delta rows across multiple flushes", rows)
	}
}

// TestStreamResultsOnlyDelayedReaderSeesWithheldText proves defect #1: a reader that
// starts BEFORE the boundary, and a reader that starts only AFTER the producer
// terminal flush, both observe EVERY original fragment in cursor order, correctly
// interleaved with the persisted results (a durable model/done row carries a
// sequence ABOVE withheld fragments), with no skip of the unread prefix and no DONE
// exposed before the last deltas. Covers pagination across holes and a partial cursor,
// and that a lagging reader keeps its withheld text after the tail is sealed.
func TestStreamResultsOnlyDelayedReaderSeesWithheldText(t *testing.T) {
	if stateDSN() == "" {
		t.Skip("set AHP_TEST_DATABASE_URL or DATABASE_URL to run the results-only state tests")
	}
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	t.Setenv(contracts.EnvStreamRetention, "results-only")
	s, scope, lease, _ := appendLeaseFixture(t)
	ctx := context.Background()
	th, w, ep := lease.Thread.ID, lease.Worker, lease.Epoch
	app := func(key, kind, body string) {
		if _, err := s.AppendStream(ctx, scope, lease.Run.ID, w, ep, key, kind, json.RawMessage(body)); err != nil {
			t.Fatalf("append %s: %v", key, err)
		}
	}
	// Produce a full turn: fragments bracketed and interleaved by results.
	app("d1", "model_delta", `"a"`)
	app("d2", "model_delta", `"b"`)
	app("m1", "model", `{"t":1}`) // durable, sequence ABOVE d1,d2
	app("d3", "model_delta", `"c"`)
	app("dn", "done", `{"stop":"end"}`) // durable, highest so far
	if _, err := s.FlushStream(ctx, scope, lease.Run.ID, w, ep); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Durable rows are ABOVE some withheld fragments (m1 at 3, dn at 5); a naive
	// durable-high-water merge would skip the withheld fragments. The combined read
	// from cursor 0 must return all five in order.
	page, err := s.ReadStreamPage(ctx, scope, th, 0, 256, MaxStreamResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	wantKinds := []string{"model_delta", "model_delta", "model", "model_delta", "done"}
	if len(page.Events) != 5 {
		t.Fatalf("delayed reader must see all 5 originals (fragments + results), got %d: %+v", len(page.Events), page.Events)
	}
	var last int64
	for i, e := range page.Events {
		if e.Sequence <= last {
			t.Fatalf("events not ascending at %d: %+v", i, page.Events)
		}
		last = e.Sequence
		if e.Kind != wantKinds[i] {
			t.Fatalf("event %d kind = %s want %s (fragments must interleave with results)", i, e.Kind, wantKinds[i])
		}
	}
	if page.HasMore {
		t.Fatal("no more expected")
	}
	// Partial cursor starting inside the withheld prefix: pagination across holes.
	page2, err := s.ReadStreamPage(ctx, scope, th, 2, 256, MaxStreamResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Events) != 3 || page2.Events[0].Kind != "model" || page2.Events[0].Sequence != 3 {
		t.Fatalf("partial cursor from 2 must resume at seq 3 and cross the hole: %+v", page2.Events)
	}
	// Small page size forces pagination across a hole; every original must still be
	// seen exactly once, in order, and DONE (status) never before the last fragment.
	seen := map[int64]string{}
	cursor := int64(0)
	for {
		p, err := s.ReadStreamPage(ctx, scope, th, cursor, 2, MaxStreamResponseBytes)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range p.Events {
			if _, dup := seen[e.Sequence]; dup {
				t.Fatalf("duplicate cursor %d across pages", e.Sequence)
			}
			seen[e.Sequence] = e.Kind
		}
		if !p.HasMore {
			if p.NextAfter <= cursor && len(p.Events) > 0 {
				t.Fatalf("NextAfter must advance: %d -> %d", cursor, p.NextAfter)
			}
			cursor = p.NextAfter
			break
		}
		if p.NextAfter <= cursor {
			t.Fatalf("page must advance cursor: %d", cursor)
		}
		cursor = p.NextAfter
	}
	if len(seen) != 5 {
		t.Fatalf("paginated drain must observe all 5 originals exactly once, saw %d", len(seen))
	}
	// Now the terminal status frame (durable, above the high-water). A reader from 0
	// must still get every withheld fragment BEFORE the status frame — never DONE
	// replacing the unread suffix.
	app("st", "status", `{"status":"done"}`)
	final, err := s.ReadStreamPage(ctx, scope, th, 0, 256, MaxStreamResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(final.Events) != 6 || final.Events[5].Kind != "status" {
		t.Fatalf("status must be the last frame after all fragments: %+v", final.Events)
	}
	// Seal the run tail (terminal release path); a lagging reader still gets its
	// withheld text until bounded expiry, not just coarse results.
	s.DiscardRunTail(lease.Run.ID)
	afterSeal, err := s.ReadStreamPage(ctx, scope, th, 0, 256, MaxStreamResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	fragAfterSeal := 0
	for _, e := range afterSeal.Events {
		if e.Kind == "model_delta" {
			fragAfterSeal++
		}
	}
	if fragAfterSeal != 3 {
		t.Fatalf("sealed tail must still serve withheld fragments to a lagging reader until expiry: got %d", fragAfterSeal)
	}
}

// TestStreamResultsOnlyConcurrentAppendFlushReadSeal drives concurrent append,
// boundary flush, page read and terminal seal against the SAME run tail so the race
// detector validates the lock discipline: per-run t.mu guards rows/keys/nextSeq/
// bytes/durableThrough/sealed/retainedAfter, while the global streamTailsMu guards the
// map and the sealed FIFO, always taken outer->inner and never inverted. Cursors must
// stay strictly monotonic and every read must be self-consistent (ascending, no dup).
func TestStreamResultsOnlyConcurrentAppendFlushReadSeal(t *testing.T) {
	if stateDSN() == "" {
		t.Skip("set AHP_TEST_DATABASE_URL or DATABASE_URL to run the results-only state tests")
	}
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	t.Setenv(contracts.EnvStreamRetention, "results-only")
	s, scope, lease, _ := appendLeaseFixture(t)
	ctx := context.Background()
	th, w, ep, run := lease.Thread.ID, lease.Worker, lease.Epoch, lease.Run.ID

	var wg sync.WaitGroup
	// producer: append fragments + periodic completed results, monotonic keys.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 400; i++ {
			kind := "model_delta"
			if i%25 == 24 {
				kind = "model"
			}
			key := fmt.Sprintf("ev-%d", i)
			if _, err := s.AppendStream(ctx, scope, run, w, ep, key, kind, json.RawMessage(`{"i":`+fmt.Sprint(i)+`}`)); err != nil {
				t.Errorf("append %d: %v", i, err)
				return
			}
		}
	}()
	// concurrent boundary flusher.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			if _, err := s.FlushStream(ctx, scope, run, w, ep); err != nil {
				t.Errorf("flush %d: %v", i, err)
				return
			}
		}
	}()
	// concurrent reader: every returned page must be ascending with no duplicate cursor.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			p, err := s.ReadStreamPage(ctx, scope, th, 0, 32, MaxStreamResponseBytes)
			if err != nil {
				t.Errorf("read %d: %v", i, err)
				return
			}
			var last int64
			for _, e := range p.Events {
				if e.Sequence <= last {
					t.Errorf("non-ascending page: last=%d cur=%d", last, e.Sequence)
					return
				}
				last = e.Sequence
			}
		}
	}()
	wg.Wait()

	// The terminal seal now races ONLY reads, never new appends: a straggler append
	// from the same fenced worker is legitimately refused once the run is terminal
	// (covered by TestStreamResultsOnlyLateAppendAfterSeal), so mixing it into the
	// append phase would reject correct producers. Here we prove a SEALED results-only
	// tail stays readable under concurrent seal + drain -- never silently dropped
	// mid-drain -- until bounded expiry (defect #1 live-lag guarantee).
	var seal sync.WaitGroup
	seal.Add(2)
	go func() {
		defer seal.Done()
		for i := 0; i < 8; i++ {
			s.DiscardRunTail(run) // idempotent seal; enqueues bounded reclamation
			time.Sleep(time.Millisecond)
		}
	}()
	go func() {
		defer seal.Done()
		for i := 0; i < 100; i++ {
			p, err := s.ReadStreamPage(ctx, scope, th, 0, 64, MaxStreamResponseBytes)
			if err != nil {
				t.Errorf("sealed read %d: %v", i, err)
				return
			}
			var last int64
			for _, e := range p.Events {
				if e.Sequence <= last {
					t.Errorf("sealed read non-ascending: last=%d cur=%d", last, e.Sequence)
					return
				}
				last = e.Sequence
			}
		}
	}()
	seal.Wait()

	// Final drain: after quiescing, a reader from cursor 0 sees every original
	// fragment in strict order (fragments were never persisted; the volatile tail is
	// the only source until bounded expiry), with the durable results interleaved.
	p, err := s.ReadStreamPage(ctx, scope, th, 0, 1000, MaxStreamResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	if rows, _ := durableStreamCount(t, s, th, "model_delta"); rows != 0 {
		t.Fatalf("results-only persisted %d model_delta rows under concurrency", rows)
	}
	fragSeen := 0
	var last int64
	for _, e := range p.Events {
		if e.Sequence <= last {
			t.Fatalf("final drain not ascending: last=%d cur=%d", last, e.Sequence)
		}
		last = e.Sequence
		if e.Kind == "model_delta" {
			fragSeen++
		}
	}
	if fragSeen == 0 {
		t.Fatal("final drain must still surface withheld fragments (volatile tail), got none")
	}
}

// TestStreamResultsOnlyLateAppendAfterSeal proves fix #1 on the state plane: once a
// results-only run is SEALED (its tail released at terminal), a straggler append
// from the SAME (still-fenced) worker must be refused outright -- it must not mutate
// the volatile output, hand out a new cursor, advance the coarse high-water, persist
// a durable row, or expose a new event to readers after terminal. An identical
// (key,body) replay is still honoured with its ORIGINAL event identity (fix #2); a
// changed body under a seen key is refused as KEY_REUSE. Controls (journal) never
// seal, so they are unaffected.
func TestStreamResultsOnlyLateAppendAfterSeal(t *testing.T) {
	if stateDSN() == "" {
		t.Skip("set AHP_TEST_DATABASE_URL or DATABASE_URL to run the results-only state tests")
	}
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	t.Setenv(contracts.EnvStreamRetention, "results-only")
	s, scope, lease, _ := appendLeaseFixture(t)
	ctx := context.Background()
	th, w, ep, run := lease.Thread.ID, lease.Worker, lease.Epoch, lease.Run.ID
	app := func(key, kind, body string) StreamEvent {
		t.Helper()
		ev, err := s.AppendStream(ctx, scope, run, w, ep, key, kind, json.RawMessage(body))
		if err != nil {
			t.Fatalf("append %s/%s: %v", kind, key, err)
		}
		return ev
	}
	d1 := app("d1", "model_delta", `"he"`)
	app("d2", "model_delta", `"llo"`)
	app("m1", "model", `{"text":"hello"}`)
	if _, err := s.FlushStream(ctx, scope, run, w, ep); err != nil {
		t.Fatalf("step flush: %v", err)
	}
	// Terminal: release the tail, which in results-only SEALS (never deletes) it.
	s.DiscardRunTail(run)
	tail := s.streamTails[run]
	if tail == nil || !tail.sealed {
		t.Fatalf("results-only terminal must seal the retained tail: %+v", tail)
	}
	hwBefore := threadHighWater(t, s, th)
	rowsBefore, _ := durableStreamCount(t, s, th, "model")

	// (a) Identical (key,body) replay after seal returns the ORIGINAL event with its
	// original cursor -- not a fresh sequence, and not a KEY_REUSE error.
	replay, err := s.AppendStream(ctx, scope, run, w, ep, "d1", "model_delta", json.RawMessage(`"he"`))
	if err != nil {
		t.Fatalf("identical replay after seal must succeed: %v", err)
	}
	if replay.ID != d1.ID || replay.Sequence != d1.Sequence {
		t.Fatalf("identical replay must return original event identity: got %s/%d want %s/%d", replay.ID, replay.Sequence, d1.ID, d1.Sequence)
	}

	// (b) A changed body under a seen key is still refused (never accepted, never a
	// new event), even on a sealed tail.
	if _, err := s.AppendStream(ctx, scope, run, w, ep, "d1", "model_delta", json.RawMessage(`"CHANGED"`)); !errors.Is(err, ErrKeyReuse) {
		t.Fatalf("changed-body replay after seal must be refused: %v", err)
	}

	// (c) A brand-new key on a sealed tail is refused outright.
	if _, err := s.AppendStream(ctx, scope, run, w, ep, "d3", "model_delta", json.RawMessage(`"!"`)); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("late append after terminal seal must be refused as CONFLICT: %v", err)
	}
	// No durable fragment was ever written, the durable result count is unchanged, and
	// the coarse high-water did not advance past the sealed run's last real cursor.
	if rows, _ := durableStreamCount(t, s, th, "model_delta"); rows != 0 {
		t.Fatalf("results-only persisted %d model_delta rows after a refused late append", rows)
	}
	if rows, _ := durableStreamCount(t, s, th, "model"); rows != rowsBefore {
		t.Fatalf("refused late append must not persist new rows: before=%d after=%d", rowsBefore, rows)
	}
	if hw := threadHighWater(t, s, th); hw != hwBefore {
		t.Fatalf("refused late append must not advance high-water: before=%d after=%d", hwBefore, hw)
	}
	// (d) No NEW event surfaced to a reader: the visible sequence set is unchanged
	// after the refused late append (still exactly the three original events).
	page, err := s.ReadStream(ctx, scope, th, 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for _, e := range page {
		if e.Sequence > hwBefore {
			t.Fatalf("reader saw a post-seal event cursor %d > high-water %d", e.Sequence, hwBefore)
		}
		seen[e.Sequence] = true
	}
	if len(page) != 3 || !seen[d1.Sequence] {
		t.Fatalf("post-seal reader must see exactly the original events, got %d: %+v", len(page), page)
	}
}

// TestStreamResultsOnlyIdempotentReplayAcrossBoundary proves fix #2: idempotency keys
// and their buffered rows SURVIVE a step-boundary flush, so a replay after the
// boundary resolves to the ORIGINAL event/cursor (not a new sequence and not a
// spurious KEY_REUSE). A different body under the same key still errors, and a new
// key still advances the monotonic cursor -- so retaining keys never wedges the run.
func TestStreamResultsOnlyIdempotentReplayAcrossBoundary(t *testing.T) {
	if stateDSN() == "" {
		t.Skip("set AHP_TEST_DATABASE_URL or DATABASE_URL to run the results-only state tests")
	}
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	t.Setenv(contracts.EnvStreamRetention, "results-only")
	s, scope, lease, _ := appendLeaseFixture(t)
	ctx := context.Background()
	th, w, ep, run := lease.Thread.ID, lease.Worker, lease.Epoch, lease.Run.ID

	first, err := s.AppendStream(ctx, scope, run, w, ep, "d1", "model_delta", json.RawMessage(`"he"`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendStream(ctx, scope, run, w, ep, "m1", "model", json.RawMessage(`{"text":"x"}`)); err != nil {
		t.Fatal(err)
	}
	// Boundary flush makes the completed result durable; the delta stays volatile.
	if _, err := s.FlushStream(ctx, scope, run, w, ep); err != nil {
		t.Fatal(err)
	}
	// Replay AFTER the boundary: identical body -> the original event identity.
	replay, err := s.AppendStream(ctx, scope, run, w, ep, "d1", "model_delta", json.RawMessage(`"he"`))
	if err != nil {
		t.Fatalf("identical replay across boundary must succeed: %v", err)
	}
	if replay.ID != first.ID || replay.Sequence != first.Sequence {
		t.Fatalf("replay across boundary must resolve to original event: got %s/%d want %s/%d", replay.ID, replay.Sequence, first.ID, first.Sequence)
	}
	// Changed body under the seen key is refused (correct idempotency).
	if _, err := s.AppendStream(ctx, scope, run, w, ep, "d1", "model_delta", json.RawMessage(`"DIFFERENT"`)); !errors.Is(err, ErrKeyReuse) {
		t.Fatalf("changed-body replay across boundary must be refused: %v", err)
	}
	// A genuinely new key still advances strictly past the replayed/flushed cursors.
	next, err := s.AppendStream(ctx, scope, run, w, ep, "d9", "model_delta", json.RawMessage(`"z"`))
	if err != nil {
		t.Fatalf("new key after a replay must still be accepted: %v", err)
	}
	if next.Sequence <= first.Sequence {
		t.Fatalf("new cursor must exceed the replayed original: got %d want > %d", next.Sequence, first.Sequence)
	}
	// A reader still sees every original fragment exactly once in dense order; the
	// replay did not duplicate d1 and the refused body left no event.
	page, err := s.ReadStream(ctx, scope, th, 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	var last int64
	deltas := 0
	for _, e := range page {
		if e.Sequence <= last {
			t.Fatalf("cursor not dense/ascending after replay: last=%d cur=%d", last, e.Sequence)
		}
		last = e.Sequence
		if e.Kind == "model_delta" {
			deltas++
		}
	}
	if deltas != 2 { // d1 + d9 only; the refused change left nothing, the replay collapsed
		t.Fatalf("expected exactly 2 volatile deltas after replay, got %d: %+v", deltas, page)
	}
}
