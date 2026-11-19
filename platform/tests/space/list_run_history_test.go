package space_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestListRunHistoryEdge drives the durable run-history read through the real
// Space HTTP edge (POST /v1/space/ListRunHistory) over the accepted State wire
// (the State peer is the test double here; the real-server proof is the
// state_integration build below). It asserts:
//   - two turns accepted on the SAME context_id share one thread and each adds a
//     run, so ListRunHistory returns BOTH runs oldest-first (FIFO by sequence);
//   - the response is the OBJECT envelope {"runs":[...]}, never a bare array
//     (UI/SDK wire contract);
//   - a foreign subject reading another subject's thread gets 404, never a leak.
func TestListRunHistoryEdge(t *testing.T) {
	srv := newHTTPSpace(t, &stubCore{}, authPerSubject)
	ctx := "ctx-runhist-" + contracts.NewID()

	accept := func(subject, key string) (threadID string) {
		code, body := postSpaceAs(t, srv.URL, "AcceptTurn", map[string]any{
			"key":        key,
			"context_id": ctx,
			"input":      map[string]any{"role": "user", "content": "hi"},
			"max_steps":  2,
		}, subject)
		if code != 200 {
			t.Fatalf("AcceptTurn(%s) = %d, want 200 (body=%s)", subject, code, body)
		}
		acc := decodeMap(t, body)
		tid, _ := acc["thread_id"].(string)
		if tid == "" {
			t.Fatalf("AcceptTurn(%s) missing thread_id: %s", subject, body)
		}
		return tid
	}

	th1 := accept("alice", "rh-a1")
	th2 := accept("alice", "rh-a2")
	if th1 != th2 {
		t.Fatalf("two accepts on same context_id got different threads: %q vs %q", th1, th2)
	}

	// Owner reads run history: object envelope, both runs, FIFO by sequence.
	code, body := postSpaceAs(t, srv.URL, "ListRunHistory", map[string]any{"thread_id": th1}, "alice")
	if code != 200 {
		t.Fatalf("ListRunHistory(alice) = %d, want 200 (body=%s)", code, body)
	}
	// Wire-shape guard: must be an object with a "runs" array, not a bare array.
	env := decodeMap(t, body)
	rawRuns, ok := env["runs"].([]any)
	if !ok {
		t.Fatalf("ListRunHistory body is not object envelope {runs:[...]}: %s", body)
	}
	if len(rawRuns) != 2 {
		t.Fatalf("ListRunHistory returned %d runs, want 2 (body=%s)", len(rawRuns), body)
	}
	var lastSeq float64 = -1
	for _, raw := range rawRuns {
		r, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("run is not an object: %#v", raw)
		}
		if id, _ := r["id"].(string); id == "" {
			t.Fatalf("run missing non-empty id: %#v", r)
		}
		if tid, _ := r["thread_id"].(string); tid != th1 {
			t.Fatalf("run thread_id=%q, want %q: %#v", tid, th1, r)
		}
		seq, ok := r["sequence"].(float64)
		if !ok {
			t.Fatalf("run missing numeric sequence: %#v", r)
		}
		if seq < lastSeq {
			t.Fatalf("runs not FIFO by sequence: %v after %v", seq, lastSeq)
		}
		lastSeq = seq
	}

	// Foreign subject reading the same thread: 404, no leak.
	if code, body = postSpaceAs(t, srv.URL, "ListRunHistory", map[string]any{"thread_id": th1}, "bob"); code != 404 {
		t.Fatalf("ListRunHistory(bob,foreign) = %d, want 404 (body=%s)", code, body)
	}

	// Unknown thread for a valid subject: also 404 (fail-closed).
	if code, body = postSpaceAs(t, srv.URL, "ListRunHistory", map[string]any{"thread_id": "no-such-thread"}, "alice"); code != 404 {
		t.Fatalf("ListRunHistory(alice,unknown-thread) = %d, want 404 (body=%s)", code, body)
	}
}

// TestListRunHistoryDurableToFreshClient proves the run LIST itself is durable and
// decoupled from the coordinator's in-memory turn table: turns are written through
// one StateClient, then read back through a brand-new StateClient (no in-memory
// turns at all), which is the offline analogue of reading history after a restart.
// The State peer here is still the test double; the durable-after-restart proof on
// real PostgreSQL is the state_integration test.
func TestListRunHistoryDurableToFreshClient(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	ctx := context.Background()
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	contextID := "ctx-durable-" + contracts.NewID()

	w := space.NewStateClient(srv.URL, nil)
	th, err := w.CreateThread(ctx, "tok", scope, contextID, "k-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := w.CreateRun(ctx, "tok", th.ID, "k-run", json.RawMessage(`{"type":"turn"}`), "task-"+contracts.NewID()); err != nil {
			t.Fatalf("CreateRun: %v", err)
		}
	}

	// Fresh client: same State peer, zero in-memory coordinator state.
	r := space.NewStateClient(srv.URL, nil)
	runs, err := r.ListRuns(ctx, "tok", th.ID)
	if err != nil {
		t.Fatalf("ListRuns (fresh client): %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("fresh client ListRuns = %d runs, want 2", len(runs))
	}
	for i := 1; i < len(runs); i++ {
		if runs[i].Sequence < runs[i-1].Sequence {
			t.Fatalf("runs not FIFO: %v after %v", runs[i].Sequence, runs[i-1].Sequence)
		}
	}

	// Unknown thread yields zero runs (server returns an empty list, never an error).
	empty, err := r.ListRuns(ctx, "tok", "absent-thread")
	if err != nil {
		t.Fatalf("ListRuns(absent-thread): %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("ListRuns(absent-thread) = %d runs, want 0", len(empty))
	}
	_ = time.Now
}
