package space_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// spFaultState is a minimal, self-contained fake State HTTP server used to inject durable-read
// failures on the cold-recovery path. It is intentionally independent of the shared mockState so
// that fault toggles never disturb other tests' assumptions about a healthy State.
//
// It exercises the two SP defects the root/director reviews confirmed against source:
//   - SP-2 (ListCheckpoints): a transient durable-read error during cold rehydrate must NOT be
//     mistaken for "no checkpoint" -> the run must fail closed BEFORE Core, with zero Core calls
//     and zero durable stream appends.
//   - SP-1 (OG-2 step high-water): a ReadStream failure during the high-water scan must fail
//     closed BEFORE Core, with zero Core calls and zero appends.
//
// A positive-control case (both reads succeed) proves the harness actually reaches Core, so the
// zero-call assertions in the fault cases are non-vacuous.
type spFaultState struct {
	failList bool
	failRead bool
	// failAppendsAfter (0 = never) makes AppendStream fail for every attempt AFTER the given
	// number of successful attempts: it injects a durable stream hole mid-run.
	failAppendsAfter int32

	appends    int32
	attempts   int32
	claims     int32
	coreTokReq int32

	runID    string
	threadID string
	taskID   string
	worker   string
	epoch    int64

	releaseCh chan string
	srv       *httptest.Server
}

func newSPFaultState(t *testing.T) *spFaultState {
	t.Helper()
	m := &spFaultState{
		runID:     "run-sp-1",
		threadID:  "thr-sp-1",
		taskID:    "rtask-sp-1:k1", // a CHILD run task (not space-authority:*)
		worker:    "w1",
		epoch:     7,
		releaseCh: make(chan string, 4),
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *spFaultState) handle(w http.ResponseWriter, r *http.Request) {
	op := r.URL.Path
	if len(op) > len("/v1/state/") {
		op = op[len("/v1/state/"):]
	}
	// Drain the request body so the client never blocks, but we do not enforce body shape here.
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	write := func(code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	switch op {
	case "ClaimRun":
		atomic.AddInt32(&m.claims, 1)
		write(200, map[string]any{
			"task_token": "tok-sp-run",
			"lease": map[string]any{
				"thread":     map[string]any{"id": m.threadID},
				"run":        map[string]any{"id": m.runID, "thread_id": m.threadID, "task_id": m.taskID, "status": "pending"},
				"scope":      map[string]any{"tenant": "t-sp", "subject": "s-sp", "deployment": "d-sp"},
				"worker":     m.worker,
				"epoch":      m.epoch,
				"expires_at": time.Now().Add(time.Minute).UTC(),
			},
		})
	case "ListCheckpoints":
		if m.failList {
			http.Error(w, `{"code":"internal","message":"injected checkpoint read failure"}`, http.StatusInternalServerError)
			return
		}
		write(200, map[string]any{"checkpoints": []any{}})
	case "ReadStream":
		if m.failRead {
			http.Error(w, `{"code":"internal","message":"injected stream read failure"}`, http.StatusInternalServerError)
			return
		}
		page := map[string]any{"events": []any{}}
		if _, bounded := body["max_response_bytes"]; bounded {
			page["has_more"] = false
			page["next_after"] = body["after"]
		}
		write(200, page)
	case "AppendStream":
		attempt := atomic.AddInt32(&m.attempts, 1)
		if m.failAppendsAfter > 0 && attempt > m.failAppendsAfter {
			http.Error(w, `{"code":"internal","message":"injected stream append failure"}`, http.StatusInternalServerError)
			return
		}
		n := atomic.AddInt32(&m.appends, 1)
		write(200, map[string]any{"sequence": n})
	case "Heartbeat":
		write(200, map[string]any{})
	case "Release":
		status := "unknown"
		if body != nil {
			if s, ok := body["status"].(string); ok {
				status = s
			}
		}
		select {
		case m.releaseCh <- status:
		default:
		}
		write(200, map[string]any{})
	default:
		write(200, map[string]any{})
	}
}

// spCountingCore records every Core.Run invocation. It emits no steps and returns nil so the
// happy path is fully deterministic (zero step appends; exactly one terminal status append).
type spCountingCore struct {
	calls int32
}

func (c *spCountingCore) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	atomic.AddInt32(&c.calls, 1)
	// Honor cancellation promptly so the SP-1 stream-hole cancel (if it ever fired mid-loop) drains.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return nil
}

// waitRelease waits for the executeTurn goroutine to Release the lease (its last durable action
// on the preflight-fail path and on completion), then returns the reported status.
func (m *spFaultState) waitRelease(t *testing.T, d time.Duration) (string, bool) {
	t.Helper()
	select {
	case s := <-m.releaseCh:
		return s, true
	case <-time.After(d):
		return "", false
	}
}

// TestColdRecoveryListCheckpointsFailsClosed (SP-2): the cold rehydrate lists checkpoints; a
// transient durable error must NOT be treated as "no checkpoint". The run fails closed with zero
// Core calls and zero durable appends.
func TestColdRecoveryListCheckpointsFailsClosed(t *testing.T) {
	m := newSPFaultState(t)
	m.failList = true
	core := &spCountingCore{}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(m.srv.URL, nil),
		Core:        core,
		WorkerID:    "w1",
	})
	if err := coord.DispatchTurn(context.Background(), "tok-sp-ingress", contracts.Scope{}); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	status, ok := m.waitRelease(t, 5*time.Second)
	if !ok {
		t.Fatal("no Release observed: cold preflight did not fail closed (possible Core start or hang)")
	}
	if status != "failed" {
		t.Fatalf("expected failed Release on checkpoint-read fault, got %q", status)
	}
	if got := atomic.LoadInt32(&core.calls); got != 0 {
		t.Fatalf("SP-2: Core must not be called when checkpoint recovery read failed; calls=%d", got)
	}
	if got := atomic.LoadInt32(&m.appends); got != 0 {
		t.Fatalf("SP-2: no durable stream append is allowed on a failed recovery read; appends=%d", got)
	}
}

// TestColdRecoveryHighWaterFailsClosed (SP-1): checkpoint list succeeds, but the OG-2 step-stream
// high-water ReadStream fails. The run must fail closed BEFORE Core, zero calls and zero appends.
func TestColdRecoveryHighWaterFailsClosed(t *testing.T) {
	m := newSPFaultState(t)
	m.failRead = true
	core := &spCountingCore{}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(m.srv.URL, nil),
		Core:        core,
		WorkerID:    "w1",
	})
	if err := coord.DispatchTurn(context.Background(), "tok-sp-ingress", contracts.Scope{}); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	status, ok := m.waitRelease(t, 5*time.Second)
	if !ok {
		t.Fatal("no Release observed: cold preflight did not fail closed (possible Core start or hang)")
	}
	if status != "failed" {
		t.Fatalf("expected failed Release on high-water read fault, got %q", status)
	}
	if got := atomic.LoadInt32(&core.calls); got != 0 {
		t.Fatalf("SP-1: Core must not be called when high-water scan read failed; calls=%d", got)
	}
	if got := atomic.LoadInt32(&m.appends); got != 0 {
		t.Fatalf("SP-1: no durable stream append is allowed on a failed recovery read; appends=%d", got)
	}
}

// TestColdRecoveryHappyPathReachesCore is the positive control: when both durable recovery reads
// succeed, preflight passes and Core is actually invoked. Without this, the two fault tests could
// pass vacuously (e.g. if Core were never reachable at all).
func TestColdRecoveryHappyPathReachesCore(t *testing.T) {
	m := newSPFaultState(t) // no faults
	core := &spCountingCore{}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(m.srv.URL, nil),
		Core:        core,
		WorkerID:    "w1",
	})
	if err := coord.DispatchTurn(context.Background(), "tok-sp-ingress", contracts.Scope{}); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	status, ok := m.waitRelease(t, 5*time.Second)
	if !ok {
		t.Fatal("no Release observed on happy cold dispatch")
	}
	if status != "done" {
		t.Fatalf("expected done Release on happy path, got %q", status)
	}
	if got := atomic.LoadInt32(&core.calls); got != 1 {
		t.Fatalf("positive control: Core must be invoked exactly once when durable reads succeed; calls=%d", got)
	}
	if got := atomic.LoadInt32(&m.appends); got < 1 {
		t.Fatalf("positive control: a successful run must emit at least the terminal status append; appends=%d", got)
	}
}
