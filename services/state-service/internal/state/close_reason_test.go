package state

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// closeReasonEnv is one claimed run whose payload the test can read back raw, because
// the whole point of DEF-A6 is what the payload BYTES look like -- a decoded struct
// cannot show that a key was never added.
type closeReasonEnv struct {
	s     *Store
	ctx   context.Context
	scope contracts.Scope
	run   Run
	fence Fence
}

func closeReasonFixture(t *testing.T, payload string) *closeReasonEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	pool, note := isolatedPool(t)
	t.Logf("state test substrate: %s", note)
	s := New(pool)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	scope := contracts.Scope{Tenant: "t-" + contracts.NewID(), Deployment: "d-" + contracts.NewID(),
		Subject: "subj-" + contracts.NewID()}
	th, err := s.CreateThread(ctx, scope, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := s.CreateRun(ctx, scope, th.ID, "task-"+contracts.NewID(), "k-"+contracts.NewID(),
		json.RawMessage(payload))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	lease, err := s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "w-"+run.ID, time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	return &closeReasonEnv{s: s, ctx: ctx, scope: scope, run: run,
		fence: Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}}
}

func (e *closeReasonEnv) payloadBytes(t *testing.T) []byte {
	t.Helper()
	fresh, err := e.s.GetRun(e.ctx, e.scope, e.run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	return fresh.Payload
}

// TestTerminalReleaseRecordsCloseReasonInExistingPayload is requirement (a) at the
// plane that stores the run: the terminal reason lands in the run's EXISTING payload
// document, next to the keys the run was created with, and nothing else moves.
func TestTerminalReleaseRecordsCloseReasonInExistingPayload(t *testing.T) {
	e := closeReasonFixture(t, `{"max_steps":3,"input":"write the marker"}`)

	if err := e.s.ReleaseWithReason(e.ctx, e.scope, e.run.ID, e.fence.Worker, e.fence.Epoch,
		RunDone, "model_finished"); err != nil {
		t.Fatalf("ReleaseWithReason: %v", err)
	}
	raw := e.payloadBytes(t)
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("payload is not the JSON object it was created as: %v (%s)", err, raw)
	}
	if got[RunPayloadCloseReason] != "model_finished" {
		t.Fatalf("payload %s: want %s=model_finished", raw, RunPayloadCloseReason)
	}
	if got["max_steps"] != float64(3) || got["input"] != "write the marker" {
		t.Fatalf("the run's own payload keys were not preserved verbatim: %s", raw)
	}
	fresh, err := e.s.GetRun(e.ctx, e.scope, e.run.ID)
	if err != nil {
		t.Fatalf("GetRun status: %v", err)
	}
	if fresh.Status != RunDone {
		t.Fatalf("status = %q, want %q", fresh.Status, RunDone)
	}
}

// TestReleaseWithoutReasonLeavesPayloadByteIdentical is requirement (c): a caller that
// states nothing writes nothing. Byte equality (not key equality) is asserted, because
// "behaviour identical to today" is only true if the stored document did not move at
// all -- including on a non-terminal transition.
func TestReleaseWithoutReasonLeavesPayloadByteIdentical(t *testing.T) {
	const payload = `{"max_steps":3,"input":"unchanged"}`
	e := closeReasonFixture(t, payload)
	before := e.payloadBytes(t)

	// A non-terminal transition (back to pending) with no reason: today's behaviour.
	if err := e.s.Release(e.ctx, e.scope, e.run.ID, e.fence.Worker, e.fence.Epoch, RunPending); err != nil {
		t.Fatalf("Release(pending): %v", err)
	}
	if after := e.payloadBytes(t); !bytes.Equal(before, after) {
		t.Fatalf("a reasonless release moved the payload bytes: before=%s after=%s", before, after)
	}
	// And a TERMINAL transition with no reason: refused outright, since DEF-RELEASE-REASON-1.
	// The guarantee this test exists for is unchanged - no invented verdict lands in the
	// payload - only the mechanism got honest: the door says so instead of quietly ending
	// a run nobody explained.
	lease, err := e.s.ClaimRun(e.ctx, e.scope.Tenant, e.scope.Deployment, "w2-"+e.run.ID, time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("re-ClaimRun: %v", err)
	}
	if err := e.s.Release(e.ctx, e.scope, e.run.ID, lease.Worker, lease.Epoch, RunFailed); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("Release(failed) with no reason returned %v, want %s", err, contracts.ErrInvalid)
	}
	if got, gerr := e.s.GetRun(e.ctx, e.scope, e.run.ID); gerr != nil || got.Status != RunRunning {
		t.Fatalf("the refused terminal escaped: run=%+v err=%v", got, gerr)
	}
	if after := e.payloadBytes(t); !bytes.Equal(before, after) {
		t.Fatalf("a reasonless terminal release invented payload: before=%s after=%s", before, after)
	}
	var keys map[string]any
	if err := json.Unmarshal(before, &keys); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, present := keys[RunPayloadCloseReason]; present {
		t.Fatalf("close_reason must be absent when nobody stated one: %s", before)
	}
}

// TestCloseReasonRejectedOnNonTerminalTransition keeps the field honest: a reason
// belongs to an ended run. A worker that tried to stamp a verdict while the run was
// still resumable would put a future fact in the present.
func TestCloseReasonRejectedOnNonTerminalTransition(t *testing.T) {
	e := closeReasonFixture(t, `{"max_steps":3}`)
	before := e.payloadBytes(t)

	err := e.s.ReleaseWithReason(e.ctx, e.scope, e.run.ID, e.fence.Worker, e.fence.Epoch,
		RunPending, "model_finished")
	if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("a reason on a non-terminal release: want INVALID, got %v", err)
	}
	if after := e.payloadBytes(t); !bytes.Equal(before, after) {
		t.Fatalf("the refused write still changed the payload: before=%s after=%s", before, after)
	}
	if _, err := e.s.GetRun(e.ctx, e.scope, e.run.ID); err != nil {
		t.Fatalf("the refused release must not damage the run row: %v", err)
	}
}

// TestReleaseWithReasonCauseRecordsBothKeysSeparately is the J10 contract at the plane
// that stores the run: the dictionary word and the refusing plane's own sentence land in
// the same existing payload document as SEPARATE keys - close_reason keeps the audit
// vocabulary byte-for-byte comparable with the execution plane, failure_cause quotes the
// refuser verbatim - and the run's own keys stay put.
func TestReleaseWithReasonCauseRecordsBothKeysSeparately(t *testing.T) {
	e := closeReasonFixture(t, `{"max_steps":3,"input":"write the marker"}`)
	const cause = "space: run is not rooted on its own child grant"

	if err := e.s.ReleaseWithReasonCause(e.ctx, e.scope, e.run.ID, e.fence.Worker, e.fence.Epoch,
		RunFailed, "error", cause); err != nil {
		t.Fatalf("ReleaseWithReasonCause: %v", err)
	}
	raw := e.payloadBytes(t)
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("payload is not the JSON object it was created as: %v (%s)", err, raw)
	}
	if got[RunPayloadCloseReason] != "error" {
		t.Fatalf("payload %s: want %s=error kept as the dictionary word", raw, RunPayloadCloseReason)
	}
	if got[RunPayloadFailureCause] != cause {
		t.Fatalf("payload %s: want %s=%q verbatim", raw, RunPayloadFailureCause, cause)
	}
	if got["max_steps"] != float64(3) || got["input"] != "write the marker" {
		t.Fatalf("the run's own payload keys were not preserved verbatim: %s", raw)
	}
	fresh, err := e.s.GetRun(e.ctx, e.scope, e.run.ID)
	if err != nil {
		t.Fatalf("GetRun status: %v", err)
	}
	if fresh.Status != RunFailed {
		t.Fatalf("status = %q, want %q", fresh.Status, RunFailed)
	}
}

// TestFailureCauseRequiresTerminalTransition: a cause explains an END. Stamping one on
// a run that is merely going back to pending would let a mid-flight worker write a
// verdict its run never reached - the same rule close_reason already enforces.
func TestFailureCauseRequiresTerminalTransition(t *testing.T) {
	e := closeReasonFixture(t, `{"max_steps":3,"input":"still running"}`)
	err := e.s.ReleaseWithReasonCause(e.ctx, e.scope, e.run.ID, e.fence.Worker, e.fence.Epoch,
		RunPending, "", "a verdict the run has not reached")
	if err == nil {
		t.Fatal("a cause on a non-terminal release must be refused, and it changed nothing")
	}
	if after := e.payloadBytes(t); after == nil {
		t.Fatal("payload must survive the refused release")
	}
}

// TestReleaseWithReasonNeverWritesFailureCause pins the separation from the other side:
// the pre-J10 call keeps its exact wire and byte behavior - it cannot conjure the new
// key, so old senders keep producing exactly the documents they always produced.
func TestReleaseWithReasonNeverWritesFailureCause(t *testing.T) {
	e := closeReasonFixture(t, `{"max_steps":3,"input":"legacy release"}`)
	if err := e.s.ReleaseWithReason(e.ctx, e.scope, e.run.ID, e.fence.Worker, e.fence.Epoch,
		RunFailed, "error"); err != nil {
		t.Fatalf("ReleaseWithReason: %v", err)
	}
	raw := e.payloadBytes(t)
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("payload decode: %v (%s)", err, raw)
	}
	if _, present := got[RunPayloadFailureCause]; present {
		t.Fatalf("a release that stated no cause wrote one anyway: %s", raw)
	}
	if got[RunPayloadCloseReason] != "error" {
		t.Fatalf("the dictionary word must still be written by ReleaseWithReason: %s", raw)
	}
}
