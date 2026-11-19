package space_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The snapshot primitive ("снимки" in the PDF) has exactly one honest shape, and these
// tests pin both halves of it, measured against a State double that answers with real
// contracts codes:
//
//  1. A refusal from State keeps its code. Space used to turn every State 409/403/404
//     into a bare 500 INTERNAL, because its wire error never converted into
//     *contracts.Error, so contracts.Code saw "" and the transport default answered 500.
//  2. A snapshot is written by the process that HOLDS the run's lease, with that lease's
//     actual worker/epoch. Any other process - above all the ingress leg that accepted
//     the turn while a different worker executes it - is refused with STALE_EPOCH, never
//     with a 500 and never with a false 200.

// fenceCall is one observed PutCheckpoint request: the fence the client actually sent.
type fenceCall struct {
	fence space.Fence
	key   string
}

// spyState is the shared mockState plus two things the shared double cannot offer: every
// PutCheckpoint fence is captured, and PutCheckpoint can be made to answer with an
// arbitrary contracts code. That is what lets one test assert BOTH who wrote and what the
// caller saw when State refused.
type spyState struct {
	*mockState
	mu       sync.Mutex
	fences   []fenceCall
	failWith string // when set: PutCheckpoint answers this code with HTTP 409
	srv      *httptest.Server
}

func newSpyState(t *testing.T) *spyState {
	t.Helper()
	s := &spyState{mockState: newMockState()}
	inner := s.mockState.Handler()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/state/PutCheckpoint", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var in map[string]any
		_ = json.Unmarshal(body, &in)
		runID, worker, epoch := fenceOf(in)
		key := ""
		if cp, ok := in["checkpoint"].(map[string]any); ok {
			key, _ = cp["namespace"].(string)
		}
		s.mu.Lock()
		s.fences = append(s.fences, fenceCall{fence: space.Fence{RunID: runID, Worker: worker, Epoch: epoch}, key: key})
		fail := s.failWith
		s.mu.Unlock()
		if fail != "" {
			writeErr(w, http.StatusConflict, fail, "injected State refusal")
			return
		}
		s.mockState.putCheckpoint(w, in)
	})
	mux.Handle("/v1/state/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inner.ServeHTTP(w, r)
	}))
	s.srv = httptest.NewServer(mux)
	t.Cleanup(s.srv.Close)
	return s
}

func (s *spyState) setFailure(code string) {
	s.mu.Lock()
	s.failWith = code
	s.mu.Unlock()
}

func (s *spyState) calls() []fenceCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]fenceCall(nil), s.fences...)
}

// TestStateErrorKeepsItsContractCode is the unit leg of finding 20261031T014517Z: every
// code State answers with must survive StateClient.call, so the Space edge can report the
// honest 409/403/404 instead of 500 INTERNAL.
func TestStateErrorKeepsItsContractCode(t *testing.T) {
	codes := []struct {
		httpStatus int
		code       string
	}{
		{http.StatusConflict, contracts.ErrStaleEpoch},
		{http.StatusConflict, contracts.ErrConflict},
		{http.StatusConflict, contracts.ErrLeaseLost},
		{http.StatusForbidden, contracts.ErrForbidden},
		{http.StatusNotFound, contracts.ErrNotFound},
		{http.StatusConflict, contracts.ErrKeyReuse},
	}
	for _, c := range codes {
		t.Run(c.code, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeErr(w, c.httpStatus, c.code, "state refused for its own reason")
			}))
			defer srv.Close()
			sc := space.NewStateClient(srv.URL, nil)

			_, err := sc.PutCheckpoint(context.Background(), "tok",
				space.Fence{RunID: "run-1", Worker: "w1", Epoch: 3},
				space.CheckpointNamespace, "cp-1", "", "ckpt", 1, json.RawMessage(`{}`), nil)
			if err == nil {
				t.Fatal("expected the State refusal to surface")
			}
			var ce *contracts.Error
			if !errors.As(err, &ce) {
				t.Fatalf("errors.As(*contracts.Error) failed for code %s: %v", c.code, err)
			}
			if ce.Code != c.code {
				t.Fatalf("code = %q, want %q (the State code must survive verbatim)", ce.Code, c.code)
			}
			if contracts.Code(err) != c.code {
				t.Fatalf("contracts.Code = %q, want %q", contracts.Code(err), c.code)
			}
			// The reason must survive too: a bare code with no cause is not debuggable.
			if !bytes.Contains([]byte(err.Error()), []byte("state refused")) {
				t.Fatalf("error lost the State reason: %v", err)
			}
		})
	}
}

// TestStateRefusalWithoutEnvelopeStillClassifies proves the transport never answers a
// State REJECTION as an internal error just because the body was not a contracts
// envelope: the status is the only evidence left and it is honoured.
func TestStateRefusalWithoutEnvelopeStillClassifies(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream refused", http.StatusConflict)
	}))
	defer srv.Close()
	sc := space.NewStateClient(srv.URL, nil)
	_, err := sc.PutCheckpoint(context.Background(), "tok",
		space.Fence{RunID: "run-1", Worker: "w1", Epoch: 3},
		space.CheckpointNamespace, "cp-1", "", "ckpt", 1, nil, nil)
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("code = %q, want %q (err=%v)", contracts.Code(err), contracts.ErrConflict, err)
	}
}

// waitLeaseHeld blocks until the given process records the lease identity of the run it
// executes, so a test never races the claim against the snapshot write.
func waitLeaseHeld(t *testing.T, coord *space.Coordinator, turnID, worker string) space.Turn {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap, ok := coord.TurnSnapshot(turnID)
		if ok && snap.LeaseWorker == worker && snap.LeaseEpoch > 0 {
			return snap
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("worker %s never recorded a lease for turn %s", worker, turnID)
	return space.Turn{}
}

// TestSnapshotWriterIsTheLeaseOwner is the finding's split topology in one process pair:
// the ingress process accepted the turn, a DIFFERENT process claims and executes it. The
// owner writes with the lease's real worker/epoch; the ingress leg is refused honestly.
func TestSnapshotWriterIsTheLeaseOwner(t *testing.T) {
	spy := newSpyState(t)
	sc := space.NewStateClient(spy.srv.URL, nil)
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}

	// Ingress process: accepts the turn and never executes it.
	ingress := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{}, WorkerID: "w-ingress",
	})
	accepted, err := ingress.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key: "snap-owner-" + contracts.NewID(), ContextID: "ctx-snap-" + contracts.NewID(),
		Input: json.RawMessage(`"snapshot me"`), MaxSteps: 4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}

	// The ingress leg holds no lease, so it may not write a snapshot - and it must say so
	// with the fence code State itself would use, not with a 500.
	_, err = ingress.SaveCheckpoint(context.Background(), "token", space.CheckpointRequest{
		TurnID: accepted.TurnID, Sequence: 1, Messages: json.RawMessage(`[{"role":"user","content":"hi"}]`),
	})
	if contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("ingress SaveCheckpoint code = %q, want %q (err=%v)", contracts.Code(err), contracts.ErrStaleEpoch, err)
	}
	if len(spy.calls()) != 0 {
		t.Fatalf("a non-owner must never reach State with a snapshot write: %+v", spy.calls())
	}

	// Execution process: claims the run and is still executing it, so it owns the lease.
	owner := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{blockForever: true}, WorkerID: "w-exec",
	})
	if err := owner.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	held := waitLeaseHeld(t, owner, accepted.TurnID, "w-exec")

	if _, err := owner.SaveCheckpoint(context.Background(), "token", space.CheckpointRequest{
		TurnID: accepted.TurnID, Sequence: 1, Messages: json.RawMessage(`[{"role":"user","content":"hi"}]`),
	}); err != nil {
		t.Fatalf("lease owner SaveCheckpoint: %v", err)
	}

	// The fence that reached State is the GRANTED lease, not the coordinator's own
	// zero-profile {run, c.worker, epoch 1}.
	calls := spy.calls()
	if len(calls) != 1 {
		t.Fatalf("PutCheckpoint calls = %d, want 1: %+v", len(calls), calls)
	}
	got := calls[0].fence
	if got.Worker != "w-exec" || got.Epoch != held.LeaseEpoch || got.RunID != accepted.TurnID {
		t.Fatalf("fence = %+v, want run %s worker w-exec epoch %d (the lease the owner holds)",
			got, accepted.TurnID, held.LeaseEpoch)
	}
	if got.Worker == "w-ingress" || got.Epoch == 1 {
		t.Fatalf("fence fell back to the ingress zero-profile: %+v", got)
	}

	// The snapshot is durable and readable in Space's own namespace.
	cps, err := sc.ListCheckpoints(context.Background(), "token", accepted.ThreadID, space.CheckpointNamespace, 10)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	if len(cps) != 1 || cps[0].Namespace != space.CheckpointNamespace {
		t.Fatalf("checkpoints after owner write = %+v", cps)
	}

	// A cancel closes the run's lease: the snapshot window is the execution window.
	if err := owner.CancelTurn(context.Background(), accepted.TurnID, "token"); err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := owner.SaveCheckpoint(context.Background(), "token", space.CheckpointRequest{
			TurnID: accepted.TurnID, Sequence: 2,
		}); contracts.Code(err) == contracts.ErrStaleEpoch {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := owner.SaveCheckpoint(context.Background(), "token", space.CheckpointRequest{
		TurnID: accepted.TurnID, Sequence: 3,
	}); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("after the lease is gone code = %q, want %q", contracts.Code(err), contracts.ErrStaleEpoch)
	}
}

// TestSaveCheckpointInFlightWhileExecutorHoldsLeaseIsStaleEpoch is the finding's
// measurement B reproduced exactly as tools/checkpoint-probe.sh records it with
// REPRODUCE_500=1: the turn is IN EXECUTION and a DIFFERENT process (the executor) holds
// the run lease, while the ingress leg - which accepted the turn but never claimed it -
// tries to write a snapshot. It must get a distinguishable STALE_EPOCH, never a 500
// INTERNAL and never a false 200, and it must not touch durable state at all.
//
// The ordering is the whole point: ingress accepts first (its turn is stuck at the
// accepted epoch), THEN the executor claims the run, which bumps the fencing epoch under
// the real State contract. The ingress's next snapshot therefore carries a stale epoch
// that State refuses - the refusal is what used to collapse into a 500. A test that only
// saves "after the turn completes" cannot see this: the ingress zero-profile still matches
// the epoch and the write falsely succeeds.
func TestSaveCheckpointInFlightWhileExecutorHoldsLeaseIsStaleEpoch(t *testing.T) {
	spy := newSpyState(t)
	sc := space.NewStateClient(spy.srv.URL, nil)
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}

	// Ingress leg: accepts the turn, never executes it, never holds the lease.
	ingress := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{}, WorkerID: "w-ingress",
	})
	srvIngress := httptest.NewServer(space.NewHandler(space.HandlerConfig{Coordinator: ingress, Auth: authWithPrincipal}))
	t.Cleanup(srvIngress.Close)

	code, body := postSpace(t, srvIngress.URL, "AcceptTurn", map[string]any{
		"key": "snap-inflight-" + contracts.NewID(), "context_id": "ctx-snap-inflight-" + contracts.NewID(),
		"input": "snapshot me", "max_steps": 4,
	}, "tok")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn = %d (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	threadID, _ := acc["thread_id"].(string)

	// Executor holds the lease and stays inside its Core loop, so State keeps the run's
	// fencing epoch bumped to the executor for the rest of the test.
	executor := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{blockForever: true}, WorkerID: "w-exec",
	})
	if err := executor.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn(executor): %v", err)
	}
	held := waitLeaseHeld(t, executor, turnID, "w-exec")
	if held.LeaseEpoch <= 1 {
		t.Fatalf("executor lease epoch = %d, want > 1 (claiming must bump the fencing epoch above the accepted epoch)", held.LeaseEpoch)
	}

	// The ingress leg writes WHILE the executor holds the lease. The honest answer is
	// State's own fence code, distinguishable, never a 500 and never a false 200.
	code, body = postSpace(t, srvIngress.URL, "SaveCheckpoint", map[string]any{
		"turn_id": turnID, "sequence": 1, "state": map[string]any{"probe": "in-flight"},
		"messages": []any{map[string]any{"role": "user", "content": "snapshot me"}},
	}, "tok")
	if code == http.StatusInternalServerError {
		t.Fatalf("the finding's regression: a fence refusal collapsed into 500 (body=%s)", body)
	}
	if code == http.StatusOK {
		t.Fatalf("a non-owner wrote a false 200 snapshot while the executor held the lease (body=%s)", body)
	}
	if code != http.StatusConflict {
		t.Fatalf("in-flight ingress SaveCheckpoint = %d, want 409 (body=%s)", code, body)
	}
	if got, _ := decodeMap(t, body)["code"].(string); got != contracts.ErrStaleEpoch {
		t.Fatalf("body code = %q, want %q (body=%s)", got, contracts.ErrStaleEpoch, body)
	}

	// The executor (real owner) still writes successfully - measurement B must not have
	// broken the legitimate holder - so it is the ONLY writer this test produces.
	if _, err := executor.SaveCheckpoint(context.Background(), "token", space.CheckpointRequest{
		TurnID: turnID, Sequence: 1, Messages: json.RawMessage(`[{"role":"user","content":"snapshot me"}]`),
	}); err != nil {
		t.Fatalf("executor SaveCheckpoint (legitimate lease holder): %v", err)
	}
	calls := spy.calls()
	if len(calls) != 1 {
		t.Fatalf("PutCheckpoint calls = %d, want exactly 1 (only the lease owner may write): %+v", len(calls), calls)
	}
	if calls[0].fence.Worker != "w-exec" || calls[0].fence.Epoch != held.LeaseEpoch {
		t.Fatalf("the single write was not by the lease owner: fence=%+v want w-exec epoch %d", calls[0].fence, held.LeaseEpoch)
	}

	// Nothing from the ingress attempt reached durable state: reading via the State
	// operation (Space has no read op) shows exactly the owner's one checkpoint.
	cps, err := sc.ListCheckpoints(context.Background(), "token", threadID, space.CheckpointNamespace, 10)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	if len(cps) != 1 {
		t.Fatalf("durable checkpoints = %d, want 1 (the refused ingress write must not persist): %+v", len(cps), cps)
	}

	if err := executor.CancelTurn(context.Background(), turnID, "token"); err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}
}

// TestSaveCheckpointFromIngressIsHonestConflict is the finding measured on the public
// HTTP surface: the process that only accepted the turn (the ingress leg of the stand)
// answers 409 STALE_EPOCH, never the 500 INTERNAL the probe recorded.
func TestSaveCheckpointFromIngressIsHonestConflict(t *testing.T) {
	spy := newSpyState(t)
	sc := space.NewStateClient(spy.srv.URL, nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{}, WorkerID: "w-ingress",
	})
	srv := httptest.NewServer(space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: authWithPrincipal}))
	t.Cleanup(srv.Close)

	code, body := postSpace(t, srv.URL, "AcceptTurn", map[string]any{
		"key": "snap-ingress-" + contracts.NewID(), "context_id": "ctx-snap-ingress-" + contracts.NewID(),
		"input": "snapshot me", "max_steps": 4,
	}, "tok")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn = %d (body=%s)", code, body)
	}
	turnID, _ := decodeMap(t, body)["turn_id"].(string)

	code, body = postSpace(t, srv.URL, "SaveCheckpoint", map[string]any{
		"turn_id": turnID, "sequence": 1, "state": map[string]any{"step": 1},
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}, "tok")
	if code != http.StatusConflict {
		t.Fatalf("SaveCheckpoint from a non-owner = %d, want 409 (body=%s)", code, body)
	}
	if got, _ := decodeMap(t, body)["code"].(string); got != contracts.ErrStaleEpoch {
		t.Fatalf("body code = %q, want %q (body=%s)", got, contracts.ErrStaleEpoch, body)
	}
	if n := len(spy.calls()); n != 0 {
		t.Fatalf("a non-owner must not write to State: %d PutCheckpoint calls", n)
	}
}

// TestSaveCheckpointStateRefusalIsNotInternalError drives the same surface for the leg
// that DOES own the lease: when State refuses the write (a takeover raced the snapshot),
// the caller gets State's own code back as a 409 instead of a masked 500.
func TestSaveCheckpointStateRefusalIsNotInternalError(t *testing.T) {
	spy := newSpyState(t)
	sc := space.NewStateClient(spy.srv.URL, nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{blockForever: true}, WorkerID: "w-exec",
	})
	srv := httptest.NewServer(space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: authWithPrincipal}))
	t.Cleanup(srv.Close)

	code, body := postSpace(t, srv.URL, "AcceptTurn", map[string]any{
		"key": "snap-owner-http-" + contracts.NewID(), "context_id": "ctx-snap-owner-" + contracts.NewID(),
		"input": "snapshot me", "max_steps": 4,
	}, "tok")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn = %d (body=%s)", code, body)
	}
	turnID, _ := decodeMap(t, body)["turn_id"].(string)

	if code, body = postSpace(t, srv.URL, "DispatchTurn", nil, "tok"); code != http.StatusOK {
		t.Fatalf("DispatchTurn = %d (body=%s)", code, body)
	}
	waitLeaseHeld(t, coord, turnID, "w-exec")

	// State refuses with a code of its own (a takeover between claim and snapshot).
	spy.setFailure(contracts.ErrStaleEpoch)
	code, body = postSpace(t, srv.URL, "SaveCheckpoint", map[string]any{
		"turn_id": turnID, "sequence": 1, "state": map[string]any{"step": 1},
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}, "tok")
	if code != http.StatusConflict {
		t.Fatalf("SaveCheckpoint = %d, want 409 (body=%s) - a State refusal must never surface as 500", code, body)
	}
	if got, _ := decodeMap(t, body)["code"].(string); got != contracts.ErrStaleEpoch {
		t.Fatalf("body code = %q, want %q (body=%s)", got, contracts.ErrStaleEpoch, body)
	}
	if !bytes.Contains(body, []byte("injected State refusal")) {
		t.Fatalf("the State reason must reach the caller: %s", body)
	}

	// A FORBIDDEN from State is honest too: it must surface as 403, not as 500.
	spy.setFailure(contracts.ErrForbidden)
	code, body = postSpace(t, srv.URL, "SaveCheckpoint", map[string]any{
		"turn_id": turnID, "sequence": 2,
	}, "tok")
	if code != http.StatusForbidden {
		t.Fatalf("SaveCheckpoint with a State 403 = %d, want 403 (body=%s)", code, body)
	}
	if got, _ := decodeMap(t, body)["code"].(string); got != contracts.ErrForbidden {
		t.Fatalf("body code = %q, want %q (body=%s)", got, contracts.ErrForbidden, body)
	}
}
