package state

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// durableStreamCount counts durable rows of one kind for a thread and the max
// sequence among them. Scoped by kind so a Release journal row (kind "status")
// never counts as one of our buffered "model_delta" fragments.
func durableStreamCount(t *testing.T, s *Store, threadID, kind string) (int, int64) {
	t.Helper()
	var rows int
	var maxSeq int64
	if err := s.Pool().QueryRow(context.Background(),
		`SELECT count(*), COALESCE(max(sequence),0) FROM ahp_state.stream_events WHERE thread_id=$1 AND kind=$2`, threadID, kind).Scan(&rows, &maxSeq); err != nil {
		t.Fatalf("count stream rows: %v", err)
	}
	return rows, maxSeq
}

// TestStreamBoundaryDefaultWritesPerFragment pins the historical behaviour: unset
// experiment, every AppendStream is its own durable write.
func TestStreamBoundaryDefaultWritesPerFragment(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "")
	s, scope, lease, _ := appendLeaseFixture(t)
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, "k"+string(rune('a'+i)), "model_delta", json.RawMessage(`"frag"`)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if rows, seq := durableStreamCount(t, s, lease.Thread.ID, "model_delta"); rows != i || seq != int64(i) {
			t.Fatalf("chunk mode must write per fragment: after %d rows=%d seq=%d", i, rows, seq)
		}
	}
}

// TestStreamBoundaryNoPerFragmentWrites: step and turn buffer with NO durable
// write, remain readable through ReadStream, and persist in one flush with stable
// cursors and no duplicates.
func TestStreamBoundaryNoPerFragmentWrites(t *testing.T) {
	for _, mode := range []string{"step", "turn"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Setenv(contracts.EnvCheckpointBoundary, mode)
			s, scope, lease, _ := appendLeaseFixture(t)
			ctx := context.Background()
			kind := "model_delta"
			// Interleave a step-boundary frame to prove both stream kinds buffer.
			payloads := []string{"one", "two", "three"}
			for i, p := range payloads {
				key := keyFor(mode, i)
				if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, key, kind, json.RawMessage(`"`+p+`"`)); err != nil {
					t.Fatalf("append %q: %v", p, err)
				}
			}
			if rows, seq := durableStreamCount(t, s, lease.Thread.ID, "model_delta"); rows != 0 || seq != 0 {
				t.Fatalf("%s mode must not write per fragment: rows=%d seq=%d", mode, rows, seq)
			}
			// Read-before-flush keeps streaming incremental.
			page, err := s.ReadStream(ctx, scope, lease.Thread.ID, 0, 256)
			if err != nil {
				t.Fatal(err)
			}
			if len(page) != 3 || page[0].Sequence != 1 || page[2].Sequence != 3 {
				t.Fatalf("volatile tail must be readable before flush: %+v", page)
			}
			// Incremental read from a cursor.
			tail, err := s.ReadStream(ctx, scope, lease.Thread.ID, 1, 256)
			if err != nil {
				t.Fatal(err)
			}
			if len(tail) != 2 || tail[0].Sequence != 2 {
				t.Fatalf("incremental tail read wrong: %+v", tail)
			}
			if _, err := s.FlushStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if rows, seq := durableStreamCount(t, s, lease.Thread.ID, "model_delta"); rows != 3 || seq != 3 {
				t.Fatalf("flush must persist all: rows=%d seq=%d", rows, seq)
			}
			after, err := s.ReadStream(ctx, scope, lease.Thread.ID, 0, 256)
			if err != nil {
				t.Fatal(err)
			}
			if len(after) != 3 || after[0].Sequence != 1 || after[2].Sequence != 3 {
				t.Fatalf("cursor unstable across flush: %+v", after)
			}
			// Idempotent replay of a buffered key (still in scope after flush) and a
			// re-flush must not duplicate rows.
			if _, err := s.FlushStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch); err != nil {
				t.Fatalf("re-flush: %v", err)
			}
			if rows, _ := durableStreamCount(t, s, lease.Thread.ID, "model_delta"); rows != 3 {
				t.Fatalf("re-flush duplicated: %d", rows)
			}
		})
	}
}

// TestStreamBoundaryFailedFlushKeepsNoPartialRows: a stale lease (released before
// the boundary flush) refuses the flush and writes nothing.
func TestStreamBoundaryFailedFlushKeepsNoPartialRows(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	s, scope, lease, _ := appendLeaseFixture(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, keyFor("fail", i), "model_delta", json.RawMessage(`"x"`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Release(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, "done"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := s.FlushStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("flush after lease loss must refuse with STALE_EPOCH, got %v", err)
	}
	if rows, _ := durableStreamCount(t, s, lease.Thread.ID, "model_delta"); rows != 0 {
		t.Fatalf("failed flush wrote partial rows: %d", rows)
	}
}

// TestStreamBoundaryIsolation: a foreign scope cannot read or flush the tail.
func TestStreamBoundaryIsolation(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "turn")
	s, scope, lease, _ := appendLeaseFixture(t)
	ctx := context.Background()
	if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, "secret", "model_delta", json.RawMessage(`"s"`)); err != nil {
		t.Fatal(err)
	}
	other := eventScope("intruder-" + lease.Thread.ID)
	if _, err := s.ReadStream(ctx, other, lease.Thread.ID, 0, 256); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign read leaked: %v", err)
	}
	if _, err := s.FlushStream(ctx, other, lease.Run.ID, lease.Worker, lease.Epoch); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign flush must not touch the tail: %v", err)
	}
}

// TestStreamBoundaryIncrementalBeforeFlush proves the live tail streams incremental
// fragments BEFORE any commit: a reader sees a growing prefix, not the whole reply at
// the end (Root blocker #8, store-level). Composed SSE uses the same ReadStream path.
func TestStreamBoundaryIncrementalBeforeFlush(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	s, scope, lease, _ := appendLeaseFixture(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, keyFor("inc", i), "model_delta", json.RawMessage(`"a"`)); err != nil {
			t.Fatal(err)
		}
	}
	if page, err := s.ReadStream(ctx, scope, lease.Thread.ID, 0, 256); err != nil || len(page) != 2 {
		t.Fatalf("expected 2 volatile frames, got %d err=%v", len(page), err)
	}
	if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, keyFor("inc", 2), "model_delta", json.RawMessage(`"b"`)); err != nil {
		t.Fatal(err)
	}
	page, err := s.ReadStream(ctx, scope, lease.Thread.ID, 0, 256)
	if err != nil || len(page) != 3 || page[2].Sequence != 3 {
		t.Fatalf("tail must grow incrementally before flush: %d err=%v", len(page), err)
	}
	if rows, _ := durableStreamCount(t, s, lease.Thread.ID, "model_delta"); rows != 0 {
		t.Fatalf("nothing durable before flush, got %d", rows)
	}
}

// TestStreamBoundaryTerminalFrameNotInVolatileTail proves the terminal "status" frame
// is NEVER buffered: it commits immediately, so a live read cannot observe a run as
// closed before that frame is durable (Root blocker #2/#7, store-level).
func TestStreamBoundaryTerminalFrameNotInVolatileTail(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "turn")
	s, scope, lease, _ := appendLeaseFixture(t)
	ctx := context.Background()
	if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, "frag", "model_delta", json.RawMessage(`"x"`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, "status:1", "status", json.RawMessage(`"done"`)); err != nil {
		t.Fatal(err)
	}
	if rows, _ := durableStreamCount(t, s, lease.Thread.ID, "status"); rows != 1 {
		t.Fatalf("terminal status must be durable immediately, got %d", rows)
	}
	if rows, _ := durableStreamCount(t, s, lease.Thread.ID, "model_delta"); rows != 0 {
		t.Fatalf("model_delta must stay volatile until flush, got %d", rows)
	}
}

// TestStreamBoundaryFlushBodyMismatchRejected injects a durable row carrying the SAME
// key as a buffered fragment but a DIFFERENT body, so the flush must refuse rather than
// ON CONFLICT silently accept it (Root blocker #5).
func TestStreamBoundaryFlushBodyMismatchRejected(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	s, scope, lease, _ := appendLeaseFixture(t)
	ctx := context.Background()
	k := keyFor("mism", 0)
	if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, k, "model_delta", json.RawMessage(`"original"`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, keyFor("mism", 1), "model_delta", json.RawMessage(`"other"`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `INSERT INTO ahp_state.stream_events(id,thread_id,run_id,key,kind,sequence,payload,digest)
		VALUES('inj',$1,$2,$3,'injected',999,'"tampered"'::jsonb,'wrong-digest')`, lease.Thread.ID, lease.Run.ID, k); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FlushStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("flush must reject a body mismatch, got %v", err)
	}
	if rows, _ := durableStreamCount(t, s, lease.Thread.ID, "model_delta"); rows != 0 {
		t.Fatalf("mismatch flush must write no buffered rows, got %d", rows)
	}
}

// TestStreamBoundaryFlushOwnerEpochMismatch proves a tail bound to one worker/epoch
// can never be flushed by a different worker/epoch (Root blocker #4).
func TestStreamBoundaryFlushOwnerEpochMismatch(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	s, scope, lease, _ := appendLeaseFixture(t)
	ctx := context.Background()
	if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, keyFor("own", 0), "model_delta", json.RawMessage(`"a"`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FlushStream(ctx, scope, lease.Run.ID, lease.Worker+"-other", lease.Epoch); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("wrong worker flush must be STALE_EPOCH, got %v", err)
	}
	if _, err := s.FlushStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch+1); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("wrong epoch flush must be STALE_EPOCH, got %v", err)
	}
	if rows, _ := durableStreamCount(t, s, lease.Thread.ID, "model_delta"); rows != 0 {
		t.Fatalf("refused flush wrote rows: %d", rows)
	}
}

// TestStreamBoundaryFlushRacingAppendNoLoss is the controlled append-vs-flush race
// (Root blocker #3): concurrent appends and boundary flushes on the SAME run must lose
// no event and keep the per-thread sequence dense.
func TestStreamBoundaryFlushRacingAppendNoLoss(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "step")
	s, scope, lease, _ := appendLeaseFixture(t)
	ctx := context.Background()
	const total = 400
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, keyFor("race", i), "model_delta", json.RawMessage(`"d"`)); err != nil {
				t.Errorf("append %d: %v", i, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < total/10; i++ {
			if _, err := s.FlushStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch); err != nil {
				t.Errorf("flush %d: %v", i, err)
				return
			}
		}
	}()
	wg.Wait()
	if _, err := s.FlushStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch); err != nil {
		t.Fatal(err)
	}
	rows, maxSeq := durableStreamCount(t, s, lease.Thread.ID, "model_delta")
	if rows != total || maxSeq != int64(total) {
		t.Fatalf("lost events or cursor holes under append/flush race: rows=%d maxSeq=%d want %d", rows, maxSeq, total)
	}
}

func keyFor(prefix string, i int) string {
	return prefix + "-key-" + string(rune('a'+i))
}
