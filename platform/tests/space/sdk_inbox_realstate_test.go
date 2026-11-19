//go:build state_integration

package space_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

func nativeStateClient(t *testing.T, e *cEnv, token string) *sdkstate.Client {
	t.Helper()
	client, err := sdkstate.New(sdkstate.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestGoStateSDKEnqueueReturnsDurableReceiptRealState(t *testing.T) {
	e := cNewEnv(t)
	client := nativeStateClient(t, e, e.signTask(t, "inbox-receipt"))
	thread, err := client.CreateThread(e.ctx, "ctx-1", "inbox-receipt")
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.Enqueue(e.ctx, thread.ID, "stable-input", "hitl_response", json.RawMessage(`{"approved":true}`))
	if err != nil || first.ID == "" || first.ThreadID != thread.ID || first.Sequence != 1 || first.Kind != "hitl_response" {
		t.Fatalf("Go SDK lost State's committed inbox receipt: event=%+v error=%v", first, err)
	}
	replay, err := client.Enqueue(e.ctx, thread.ID, "stable-input", "hitl_response", json.RawMessage(`{"approved": true}`))
	if err != nil || replay.ID != first.ID || replay.Sequence != first.Sequence {
		t.Fatalf("Go SDK lost durable inbox replay identity: event=%+v error=%v", replay, err)
	}
	if _, err := client.Enqueue(e.ctx, thread.ID, "stable-input", "hitl_response", json.RawMessage(`{"approved":false}`)); contracts.Code(err) != contracts.ErrInvalid || contracts.ReasonOf(err) != contracts.ErrKeyReuse {
		t.Fatalf("Go SDK lost inbox key-reuse refusal: code=%s reason=%s error=%v", contracts.Code(err), contracts.ReasonOf(err), err)
	}
}

func TestGoStateSDKAckInboxUsesFenceRealState(t *testing.T) {
	e := cNewEnv(t)
	token := e.signTask(t, "inbox-fence")
	client := nativeStateClient(t, e, token)
	thread, err := client.CreateThread(e.ctx, "ctx-1", "inbox-fence")
	if err != nil {
		t.Fatal(err)
	}
	run, err := client.CreateRun(e.ctx, thread.ID, "inbox-fence", json.RawMessage(`{"type":"inbox-fence"}`))
	if err != nil {
		t.Fatal(err)
	}
	// Seed through the independently exercised Coordinator client. This keeps
	// AckInbox's regression independent of the Enqueue receipt decoder bug.
	event, err := e.client.Enqueue(e.ctx, token, thread.ID, "answer", "hitl_response", json.RawMessage(`{"ok":true}`))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := client.ClaimRunFor(e.ctx, "inbox-worker", 30000, run.ID)
	if err != nil || claim == nil {
		t.Fatalf("claim failed: %+v %v", claim, err)
	}
	bound := nativeStateClient(t, e, claim.TaskToken)
	fence := sdkstate.Fence{RunID: run.ID, Worker: claim.Lease.Worker, Epoch: claim.Lease.Epoch}
	stale := fence
	stale.Epoch++
	if err := bound.AckInbox(e.ctx, stale, []string{event.ID}); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("Go SDK did not preserve State's stale fence refusal: %v", err)
	}
	remaining, err := client.ReadInbox(e.ctx, thread.ID, 0, 10)
	if err != nil || len(remaining) != 1 {
		t.Fatalf("stale ack changed inbox: %d %v", len(remaining), err)
	}
	if err := bound.AckInbox(e.ctx, fence, []string{event.ID}); err != nil {
		t.Fatalf("Go SDK's valid inbox acknowledgment was refused: %v", err)
	}
	if err := bound.AckInbox(e.ctx, fence, []string{event.ID}); err != nil {
		t.Fatalf("Go SDK could not replay an acknowledged event: %v", err)
	}
	remaining, err = client.ReadInbox(e.ctx, thread.ID, 0, 10)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("committed ack did not drain inbox: %d %v", len(remaining), err)
	}
	if err := bound.Release(e.ctx, fence, sdkstate.RunDone); err != nil {
		t.Fatal(err)
	}
}
