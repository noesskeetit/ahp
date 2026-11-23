package statejournal

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	wire "github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/sdk/go/state/journal"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testVMManagedTurnSDK(t *testing.T, server, badProofServer *httptest.Server, scope contracts.Scope, token func(string, ...string) string, pool *pgxpool.Pool, verifier, proofVerifier *auth.Verifier) {
	ctx := t.Context()
	newClient := func(server *httptest.Server, credential string) *journal.Client {
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
	coord := newClient(server, token("sdk-coord", "state.coordinate"))
	sessionID := "sdk-managed"
	if badProofServer != nil {
		sessionID += "-bad-proof"
	}
	input := journal.EnqueueRequest{SessionID: sessionID, Harness: "go", FormatVersion: 1,
		ExecutionMode: wire.ExecutionVMManaged, Key: "turn", Input: journal.Payload{Inline: []byte(`"initial"`)}}
	first, err := coord.EnqueueTurn(ctx, input)
	if err != nil || first.HeadSeq != 1 {
		t.Fatalf("enqueue: %+v, %v", first, err)
	}
	id := wire.DeterministicExecutionID(scope, input.SessionID, first.HeadSeq, 1)
	credential := token(id, "state.vm_claim")
	sdk := newClient(server, credential)
	claims, err := verifier.Verify(credential, "ahp", auth.KindTask, auth.WorkloadID(scope))
	if err != nil {
		t.Fatal(err)
	}
	checkProof := func(proof string, want time.Time) {
		t.Helper()
		expires, err := proofVerifier.VerifyToolLease(proof, claims, input.SessionID, 1)
		if err != nil || !expires.Equal(want) {
			t.Fatalf("verified proof deadline = %v, want %v (%v)", expires, want, err)
		}
	}
	turn, err := sdk.ClaimVMTurn(ctx, input.SessionID, journal.VMTurnClaimRequest{
		TurnSeq: first.HeadSeq, Generation: 1, ExecutionID: id,
	})
	if err != nil || turn.InputSeq != 1 {
		t.Fatalf("claim: %+v, %v", turn, err)
	}
	checkProof(turn.ToolLeaseProof, turn.ExpiresAt)
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
	if badProofServer != nil {
		badProofClient := newClient(badProofServer, credential)
		if _, err := badProofClient.FinishVMTurn(ctx, turn, followup.HeadSeq, output, ack, false); contracts.Code(err) != contracts.ErrUnavailable {
			t.Fatalf("post-commit signing error did not report unavailable: %v", err)
		}
		committed, err := sdk.GetTurnStatus(ctx, turn.SessionID)
		if err != nil || committed.InputSeq == nil || *committed.InputSeq != followup.HeadSeq {
			t.Fatalf("post-commit signing error lost the promotion: %+v, %v", committed, err)
		}
	}
	finished, err := sdk.FinishVMTurn(ctx, turn, followup.HeadSeq, output, ack, false)
	if err != nil || finished.FirstSeq != 4 || finished.HeadSeq != 5 || finished.Control == nil || finished.Control.NextTurn == nil ||
		finished.Control.NextTurn.InputSeq != followup.HeadSeq || finished.Control.NextTurn.Epoch != turn.Epoch {
		t.Fatalf("finish with continuation: %+v, %v", finished, err)
	}
	checkProof(finished.Control.ToolLeaseProof, finished.Control.NextTurn.ExpiresAt)
	if !finished.Control.NextTurn.ExpiresAt.After(turn.ExpiresAt) {
		t.Fatal("followup did not extend the claim")
	}
	var durableReceipt string
	if err := pool.QueryRow(ctx, `SELECT turn_receipt::text FROM ahp_journal.session_events
		WHERE tenant_id=$1 AND session_id=$2 AND seq=$3`, scope.Tenant, input.SessionID, finished.FirstSeq).Scan(&durableReceipt); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(durableReceipt, "tool_lease_proof") {
		t.Fatal("signed proof was persisted in the journal receipt")
	}
	short, err := sdk.HeartbeatLease(ctx, turn.SessionID, turn.Epoch, time.Second)
	if err != nil || !short.ExpiresAt.Equal(finished.Control.NextTurn.ExpiresAt) {
		t.Fatalf("short heartbeat changed the VM deadline: %v, %v", short.ExpiresAt, err)
	}
	checkProof(short.ToolLeaseProof, short.ExpiresAt)
	replayed, err := sdk.FinishVMTurn(ctx, turn, followup.HeadSeq, output, ack, false)
	if err != nil || !reflect.DeepEqual(replayed, finished) {
		t.Fatalf("replay completion with active continuation: %+v, %v", replayed, err)
	}
	long, err := sdk.HeartbeatLease(ctx, turn.SessionID, turn.Epoch, 2*time.Minute)
	if err != nil || !long.ExpiresAt.After(finished.Control.NextTurn.ExpiresAt) {
		t.Fatalf("heartbeat did not renew VM lease: %v, %v", long.ExpiresAt, err)
	}
	latest, err := sdk.FinishVMTurn(ctx, turn, followup.HeadSeq, output, ack, false)
	if err != nil || latest.FirstSeq != finished.FirstSeq || latest.HeadSeq != finished.HeadSeq ||
		latest.Control == nil || latest.Control.NextTurn == nil || !latest.Control.NextTurn.ExpiresAt.Equal(long.ExpiresAt) {
		t.Fatalf("replay after renewal: %+v, %v", latest, err)
	}
	checkProof(latest.Control.ToolLeaseProof, long.ExpiresAt)
	page, err := sdk.GetEvents(ctx, turn.SessionID, followup.HeadSeq, 10, "")
	if err != nil || page.HeadSeq != 5 || len(page.Events) != 2 {
		t.Fatalf("completion events: %+v, %v", page, err)
	}
	result, terminal := page.Events[0], page.Events[1]
	if strings.Contains(string(terminal.Payload.Inline), "tool_lease_proof") {
		t.Fatal("proof was published in the durable terminal event")
	}
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
	stale, err := sdk.FinishVMTurn(ctx, turn, followup.HeadSeq, output, ack, false)
	if err != nil || stale.FirstSeq != finished.FirstSeq || stale.HeadSeq != finished.HeadSeq || stale.Control != nil {
		t.Fatalf("historical finish retained authority: %+v, %v", stale, err)
	}
	status, err := sdk.GetTurnStatus(ctx, next.SessionID)
	if err != nil || status.Status != SessionWaitingInput || status.InputSeq != nil || status.HeadSeq != 7 {
		t.Fatalf("session after completion: %+v, %v", status, err)
	}
}

func TestVMSessionContextID(t *testing.T) {
	scope := contracts.Scope{Tenant: "tenant", Deployment: "deployment", Subject: "subject"}
	const session = "session"
	const want = "vm-session:fc4295dfab59f2645eb252420aba621ee35a3b1c25663a3c"
	if got := wire.VMSessionContextID(scope, session); got != want {
		t.Fatalf("VM session context = %q, want %q", got, want)
	}
	scope.Deployment = "another-deployment"
	if got := wire.VMSessionContextID(scope, session); got == want {
		t.Fatal("another deployment derived the same context")
	}
}
