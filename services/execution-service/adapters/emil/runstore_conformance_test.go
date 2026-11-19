package emil

// runstore_conformance_test.go — CLIENT-CONFORMANCE only (director-endorsed): a fake
// State HTTP SERVER drives OUR StateClient through the StateRunStore bridge to assert
// the accepted wire contract (fence in body; namespace body-field; OBJECT envelopes for
// reads; scope/tenant/subject/deployment NEVER in the body; STALE_EPOCH mapped to
// ErrStaleEpoch). This is NOT a live-proof of State: it tests the client/bridge we ship.
// Live State proof lives in the real-PG suites (tests/space, tags state_integration/live_real).

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
)

func newFakeState(t *testing.T, handler func(op string, body map[string]any) (int, any)) (*space.StateClient, func() map[string]any) {
	t.Helper()
	var last map[string]any
	var lastOp string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		op := r.URL.Path[len("/v1/state/"):]
		lastOp = op
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		last = b
		code, resp := handler(op, b)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return space.NewStateClient(srv.URL, nil), func() map[string]any { _ = lastOp; return last }
}

func TestStateRunStoreWireConformance(t *testing.T) {
	ctx := context.Background()
	sc, lastBody := newFakeState(t, func(op string, body map[string]any) (int, any) {
		switch op {
		case "ClaimRun":
			return 200, map[string]any{"lease": map[string]any{
				"thread": map[string]any{"id": "th1"},
				"run":    map[string]any{"id": "run1", "task_id": "task1"},
				"worker": "w1", "epoch": 3,
			}, "task_token": "TT"}
		case "AppendStream":
			return 200, map[string]any{"id": "e1", "thread_id": "th1", "key": body["key"], "kind": body["kind"], "sequence": 7, "payload": body["payload"]}
		case "ReadStream":
			return 200, map[string]any{"events": []any{map[string]any{"id": "e1", "key": "k", "kind": "tool", "sequence": 1}}}
		default:
			return 200, map[string]any{}
		}
	})
	rs := NewStateRunStore(sc)

	l, err := rs.ClaimLease(ctx, "tok", "w1", 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimLease: %v", err)
	}
	if l.RunID != "run1" || l.ThreadID != "th1" || l.Worker != "w1" || l.Epoch != 3 || l.TaskToken != "TT" {
		t.Fatalf("ClaimLease parsed wrong lease: %+v", l)
	}
	if _, ok := lastBody()["worker"]; !ok {
		t.Fatalf("ClaimRun body missing worker: %v", lastBody())
	}

	ev, err := rs.AppendEvent(ctx, *l, "step-0", "tool", json.RawMessage(`{"x":1}`))
	if err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if ev.ID != "e1" || ev.Sequence != 7 {
		t.Fatalf("AppendEvent parsed wrong event: %+v", ev)
	}
	b := lastBody()
	fence, ok := b["fence"].(map[string]any)
	if !ok {
		t.Fatalf("AppendStream body must carry a nested fence object, got %v", b)
	}
	if fence["run_id"] != "run1" || fence["worker"] != "w1" || fence["epoch"].(float64) != 3 {
		t.Fatalf("fence mismatch: %v", fence)
	}
	if b["key"] == nil || b["kind"] == nil || b["payload"] == nil {
		t.Fatalf("AppendStream body must carry key/kind/payload: %v", b)
	}
	for _, forbidden := range []string{"scope", "tenant", "subject", "deployment"} {
		if _, present := b[forbidden]; present {
			t.Fatalf("AppendStream body must NOT contain scope field %q (scope comes from the task-token principal): %v", forbidden, b)
		}
	}

	evs, err := rs.ReadEvents(ctx, "tok", "th1", 0, 50)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(evs) != 1 || evs[0].ID != "e1" {
		t.Fatalf("ReadEvents must decode the {events:[...]} object envelope: %+v", evs)
	}
	if lastBody()["thread_id"] != "th1" || lastBody()["limit"].(float64) != 50 {
		t.Fatalf("ReadStream body must carry thread_id/after/limit: %v", lastBody())
	}
}

func TestStateRunStoreMapsStaleEpoch(t *testing.T) {
	sc, _ := newFakeState(t, func(op string, body map[string]any) (int, any) {
		return http.StatusConflict, map[string]any{"code": "STALE_EPOCH", "message": "epoch mismatch"}
	})
	rs := NewStateRunStore(sc)
	l := Lease{ThreadID: "th1", RunID: "run1", TaskToken: "TT", Worker: "w1", Epoch: 1}
	err := rs.Heartbeat(context.Background(), l, 30*time.Second)
	if !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("Heartbeat on STALE_EPOCH must map to ErrStaleEpoch, got %v", err)
	}
}

func TestStateRunStoreIntentsConformance(t *testing.T) {
	ctx := context.Background()
	var lastOp string
	sc, lastBody := newFakeState(t, func(op string, body map[string]any) (int, any) {
		lastOp = op
		switch op {
		case "RecordIntent", "UpdateIntent", "GetIntent", "ReconcileIntent":
			return 200, map[string]any{"id": "int1", "run_id": body["run_id"], "node": body["node"], "call": body["call"], "status": "recorded"}
		default:
			return 200, map[string]any{}
		}
	})
	rs := NewStateRunStore(sc)
	l := Lease{ThreadID: "th1", RunID: "run1", TaskToken: "TT", Worker: "w1", Epoch: 4}

	// RecordIntent: fenced write, node/call coordinates, no scope leak.
	it, err := rs.RecordIntent(ctx, l, "agent", "save_artifact", "tool", "artifact", json.RawMessage(`{"name":"r.txt"}`))
	if err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	if it.ID != "int1" || it.Call != "save_artifact" {
		t.Fatalf("RecordIntent parsed wrong intent: %+v", it)
	}
	b := lastBody()
	fence, ok := b["fence"].(map[string]any)
	if !ok || fence["run_id"] != "run1" || fence["worker"] != "w1" || fence["epoch"].(float64) != 4 {
		t.Fatalf("RecordIntent body must carry nested fence: %v", b)
	}
	if b["node"] != "agent" || b["call"] != "save_artifact" || b["kind"] != "tool" || b["target"] != "artifact" {
		t.Fatalf("RecordIntent body must carry node/call/kind/target: %v", b)
	}
	assertNoScopeLeak(t, b, "RecordIntent")

	// GetIntent: a READ — the published contract carries NO fence on reads.
	if _, err := rs.GetIntent(ctx, "tok", "run1", "agent", "save_artifact"); err != nil {
		t.Fatalf("GetIntent: %v", err)
	}
	if _, hasFence := lastBody()["fence"]; hasFence {
		t.Fatalf("GetIntent (read) must NOT send a fence: %v", lastBody())
	}
	if lastBody()["run_id"] != "run1" || lastBody()["node"] != "agent" || lastBody()["call"] != "save_artifact" {
		t.Fatalf("GetIntent body must address by (run_id,node,call): %v", lastBody())
	}

	// ReconcileIntent: privileged UNKNOWN exit — fence + exact digest required.
	if _, err := rs.ReconcileIntent(ctx, l, "agent", "save_artifact", "succeeded", "sha256:deadbeef", json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatalf("ReconcileIntent: %v", err)
	}
	if lastOp != "ReconcileIntent" {
		t.Fatalf("expected ReconcileIntent call, got %q", lastOp)
	}
	b = lastBody()
	if _, hasFence := b["fence"]; !hasFence {
		t.Fatalf("ReconcileIntent must carry a fence: %v", b)
	}
	if b["digest"] != "sha256:deadbeef" || b["status"] != "succeeded" {
		t.Fatalf("ReconcileIntent must carry digest + status: %v", b)
	}
	assertNoScopeLeak(t, b, "ReconcileIntent")
}

func TestStateRunStoreIntentStaleEpoch(t *testing.T) {
	sc, _ := newFakeState(t, func(op string, body map[string]any) (int, any) {
		return http.StatusConflict, map[string]any{"code": "LEASE_LOST", "message": "epoch superseded"}
	})
	rs := NewStateRunStore(sc)
	l := Lease{ThreadID: "th1", RunID: "run1", TaskToken: "TT", Worker: "w1", Epoch: 1}
	if _, err := rs.RecordIntent(context.Background(), l, "agent", "c", "tool", "t", nil); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("RecordIntent on LEASE_LOST must map to ErrStaleEpoch, got %v", err)
	}
}
