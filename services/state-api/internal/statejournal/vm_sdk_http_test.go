package statejournal

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	wire "github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/sdk/go/state/journal"
)

func testVMManagedTurnSDK(t *testing.T, server *httptest.Server, scope contracts.Scope, token func(string, ...string) string) {
	ctx := t.Context()
	newClient := func(credential string) *journal.Client {
		t.Helper()
		client, err := journal.New(journal.Config{
			BaseURL: server.URL, HTTPClient: server.Client(),
			TokenSource: func(context.Context) (string, error) { return credential, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	coord := newClient(token("sdk-coord", "state.coordinate"))
	input := journal.EnqueueRequest{SessionID: "sdk-managed", Harness: "go", FormatVersion: 1,
		ExecutionMode: wire.ExecutionVMManaged, Key: "turn", Input: journal.Payload{Inline: []byte(`"initial"`)}}
	first, err := coord.EnqueueTurn(ctx, input)
	if err != nil || first.HeadSeq != 1 {
		t.Fatalf("enqueue: %+v, %v", first, err)
	}
	id := wire.DeterministicExecutionID(scope, input.SessionID, first.HeadSeq, 1)
	sdk := newClient(token(id, "state.vm_claim"))
	turn, err := sdk.ClaimVMTurn(ctx, input.SessionID, journal.VMTurnClaimRequest{
		TurnSeq: first.HeadSeq, Generation: 1, ExecutionID: id,
	})
	if err != nil || turn.InputSeq != 1 {
		t.Fatalf("claim: %+v, %v", turn, err)
	}
	changes := []journal.KVMutation{{ScopeType: "session", Namespace: "sdk", Key: "key",
		Payload: journal.Payload{Inline: []byte(`"value"`)}}}
	if _, err := sdk.MutateKV(ctx, turn.SessionID, turn.Epoch, turn.HeadSeq, "sdk-kv", changes); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("KV mutation without turn binding: %v", err)
	}
	if _, err := sdk.MutateTurnKV(ctx, turn.SessionID, turn.Epoch, turn.InputSeq+1, turn.HeadSeq, "sdk-kv", changes); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("KV mutation with wrong turn binding: %v", err)
	}
	for range 2 {
		receipt, err := sdk.MutateTurnKV(ctx, turn.SessionID, turn.Epoch, turn.InputSeq, turn.HeadSeq, "sdk-kv", changes)
		if err != nil || receipt.FirstSeq != 0 || receipt.HeadSeq != turn.HeadSeq {
			t.Fatalf("turn KV mutation: %+v, %v", receipt, err)
		}
	}
	values, err := sdk.GetKV(ctx, turn.SessionID, "session", "sdk", "key", 10)
	if err != nil || len(values) != 1 || values[0].Version != 1 || string(values[0].Payload.Inline) != `"value"` {
		t.Fatalf("committed KV: %+v, %v", values, err)
	}
	input.Key, input.DeliveryMode, input.TargetTurnSeq = "steer", wire.DeliverySteer, turn.InputSeq
	steer, err := coord.EnqueueTurn(ctx, input)
	if err != nil || steer.HeadSeq != 2 {
		t.Fatalf("enqueue steer: %+v, %v", steer, err)
	}
	input.Key, input.DeliveryMode, input.TargetTurnSeq = "followup", wire.DeliveryFollowup, 0
	followup, err := coord.EnqueueTurn(ctx, input)
	if err != nil || followup.HeadSeq != 3 {
		t.Fatalf("enqueue followup: %+v, %v", followup, err)
	}
	output := journal.Payload{Inline: []byte(`{"answer":"done"}`)}
	if _, err := sdk.FinishVMTurn(ctx, turn, followup.HeadSeq, output, nil, false); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("finish with pending steer: %v", err)
	}
	ack := &journal.SteerAck{TurnSeq: turn.InputSeq, UpToSeq: steer.HeadSeq, Count: 1}
	finished, err := sdk.FinishVMTurn(ctx, turn, followup.HeadSeq, output, ack, false)
	if err != nil || finished.FirstSeq != 4 || finished.HeadSeq != 5 || finished.Control == nil || finished.Control.NextTurn == nil ||
		finished.Control.NextTurn.InputSeq != followup.HeadSeq || finished.Control.NextTurn.Epoch != turn.Epoch {
		t.Fatalf("finish with continuation: %+v, %v", finished, err)
	}
	replayed, err := sdk.FinishVMTurn(ctx, turn, followup.HeadSeq, output, ack, false)
	if err != nil || !reflect.DeepEqual(replayed, finished) {
		t.Fatalf("replay completion with active continuation: %+v, %v", replayed, err)
	}
	page, err := sdk.GetEvents(ctx, turn.SessionID, followup.HeadSeq, 10, "")
	if err != nil || page.HeadSeq != 5 || len(page.Events) != 2 {
		t.Fatalf("completion events: %+v, %v", page, err)
	}
	result, terminal := page.Events[0], page.Events[1]
	if result.ID != "turn.output:1" || result.Type != "turn.output" || result.Role != "assistant" ||
		result.TurnSeq != turn.InputSeq || !reflect.DeepEqual(result.Payload, output) {
		t.Fatalf("assistant output: %+v", result)
	}
	var completion wire.TurnCompleted
	if err := json.Unmarshal(terminal.Payload.Inline, &completion); err != nil || terminal.ID != "turn.completed:1" ||
		terminal.Type != "turn.completed" || terminal.Role != "system" || terminal.TurnSeq != turn.InputSeq ||
		completion.TurnSeq != turn.InputSeq || completion.Outcome != SessionIdle || completion.EndSeq != 5 || completion.NextTurnSeq != followup.HeadSeq {
		t.Fatalf("canonical completion: %+v, payload %+v, %v", terminal, completion, err)
	}
	next := journal.ClaimedTurn{TurnLease: *finished.Control.NextTurn}
	waiting, err := sdk.FinishVMTurn(ctx, next, finished.HeadSeq, output, nil, true)
	if err != nil || waiting.FirstSeq != 6 || waiting.HeadSeq != 7 || waiting.Control != nil {
		t.Fatalf("finish next turn waiting for input: %+v, %v", waiting, err)
	}
	status, err := sdk.GetTurnStatus(ctx, next.SessionID)
	if err != nil || status.Status != SessionWaitingInput || status.InputSeq != nil || status.HeadSeq != 7 {
		t.Fatalf("session after completion: %+v, %v", status, err)
	}
}
