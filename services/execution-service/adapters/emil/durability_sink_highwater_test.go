package emil

// durability_sink_highwater_test.go — CLIENT-CONFORMANCE for the sink's optional
// core.DurableHighWater capability (fake State, same harness as the sink contract test).
// It exists because the Core recorder and the Space Coordinator append the SAME run's
// steps under the SAME canonical StepKey space: the recorder must learn the next key from
// the authoritative stream instead of restarting at 0 (stand 99f3396 closed a run whose
// work had completed as failed, because the recorder landed a foreign `done` on the key
// the coordinator needed next).

import (
	"context"
	"fmt"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

func streamCtx() context.Context {
	return space.WithRunContext(context.Background(), space.RunContext{
		Fence:     space.Fence{RunID: "run1", Worker: "w1", Epoch: 3},
		ThreadID:  "th1",
		TaskToken: "TT",
	})
}

func ev(key string, seq int64) map[string]any {
	return map[string]any{"id": fmt.Sprintf("e%d", seq), "thread_id": "th1", "key": key,
		"kind": "model", "sequence": seq, "payload": map[string]any{"x": 1}}
}

func TestRunStoreDurabilitySink_StepHighWater(t *testing.T) {
	var reads []map[string]any
	sc, _ := newFakeState(t, func(op string, body map[string]any) (int, any) {
		if op != "ReadStream" {
			return 400, map[string]any{"code": "UNEXPECTED_OP", "message": op}
		}
		reads = append(reads, body)
		return 200, map[string]any{"events": []map[string]any{
			ev("other-run:0", 1),       // another run on the same thread: ignored
			ev("run1:0", 2),            // this loop, low-water
			ev("run1:1", 3),            // this loop
			ev("run1:7", 4),            // this loop, highest seen
			ev("run1:not-a-number", 5), // malformed, ignored
			ev("run11:99", 6),          // prefix look-alike, ignored
		}}
	})
	sink := NewRunStoreDurabilitySink(NewStateRunStore(sc))

	next, err := sink.StepHighWater(streamCtx(), "run1")
	if err != nil {
		t.Fatalf("high-water read: %v", err)
	}
	if next != 8 {
		t.Fatalf("next key = %d, want 8 (max seen 7 + 1, foreign/foreign-prefix keys ignored)", next)
	}
	if len(reads) != 1 {
		t.Fatalf("expected a single drained page, got %d", len(reads))
	}
	if reads[0]["thread_id"] != "th1" {
		t.Fatalf("read must be scoped to the run's thread: %v", reads[0])
	}
	for _, banned := range []string{"scope", "tenant", "subject", "deployment"} {
		if _, ok := reads[0][banned]; ok {
			t.Fatalf("ReadStream body leaked %q: %v", banned, reads[0])
		}
	}
}

// A full page means "keep reading": the scan must page with the cursor it was given
// instead of reporting a low-water from the first page alone.
func TestRunStoreDurabilitySink_StepHighWaterPages(t *testing.T) {
	var cursors []any
	sc, _ := newFakeState(t, func(op string, body map[string]any) (int, any) {
		cursors = append(cursors, body["after"])
		if len(cursors) == 1 {
			page := make([]map[string]any, 0, 500)
			for i := 0; i < 500; i++ {
				page = append(page, ev(fmt.Sprintf("run1:%d", i), int64(i+1)))
			}
			return 200, map[string]any{"events": page}
		}
		return 200, map[string]any{"events": []map[string]any{ev("run1:500", 501)}}
	})
	sink := NewRunStoreDurabilitySink(NewStateRunStore(sc))
	next, err := sink.StepHighWater(streamCtx(), "run1")
	if err != nil {
		t.Fatalf("paged high-water: %v", err)
	}
	if next != 501 {
		t.Fatalf("next key = %d, want 501 (scan must drain, not stop at the first page)", next)
	}
	if len(cursors) < 2 || fmt.Sprint(cursors[1]) != "500" {
		t.Fatalf("second page must continue from the last sequence: %v", cursors)
	}
}

// Fail-closed: an unreadable stream has NO high-water. Returning 0 here is what lets a
// foreign body land on a live writer's key, so it must be an error.
func TestRunStoreDurabilitySink_StepHighWaterFailsClosedOnReadError(t *testing.T) {
	sc, _ := newFakeState(t, func(op string, body map[string]any) (int, any) {
		return 503, map[string]any{"code": "UNAVAILABLE", "message": "state is reading"}
	})
	sink := NewRunStoreDurabilitySink(NewStateRunStore(sc))
	if _, err := sink.StepHighWater(streamCtx(), "run1"); err == nil {
		t.Fatal("a failed read must be an error, never a guessed zero")
	}
}

func TestRunStoreDurabilitySink_StepHighWaterFailsClosedWithoutRunContext(t *testing.T) {
	sc, _ := newFakeState(t, func(op string, body map[string]any) (int, any) { return 200, map[string]any{} })
	sink := NewRunStoreDurabilitySink(NewStateRunStore(sc))
	if _, err := sink.StepHighWater(context.Background(), "run1"); err == nil {
		t.Fatal("missing run context must fail closed")
	}
	if _, err := sink.StepHighWater(streamCtx(), "  "); err == nil {
		t.Fatal("an empty loop id must fail closed")
	}
}
