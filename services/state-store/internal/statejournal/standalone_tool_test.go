package statejournal

import (
	"crypto/rand"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestStandaloneToolDispatchIsFencedAndRequiresReconciliation(t *testing.T) {
	ctx, _, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "standalone-" + rand.Text(), Deployment: "agent", Subject: "owner"}
	lease, err := store.AcquireOrCreateSession(ctx, scope, "session", "custom", 1, "first", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("{}"))
	args := StandaloneToolRequest{CallID: "charge", ToolName: "charge", ArgsDigest: digest}
	if _, err := store.AppendEvents(ctx, scope, "session", lease, false, AppendRequest{
		Key: "prepare", Events: []EventInput{{ID: "tool-call", Type: "tool.call", Role: "tool_call", Codec: "test", Payload: Payload{Inline: []byte("{}")}}},
		Intents: []PreparedIntent{{CallID: args.CallID, ToolName: args.ToolName, ArgsDigest: digest, IdempotencyKey: "order-1"}},
	}); err != nil {
		t.Fatal(err)
	}
	first, err := store.BeginStandaloneTool(ctx, scope, "session", lease, args)
	if err != nil || !first.CanExecute || first.Status != IntentDispatched || first.GatewayOperationID == "" {
		t.Fatalf("initial decision = %+v, error = %v", first, err)
	}
	repeat, err := store.BeginStandaloneTool(ctx, scope, "session", lease, args)
	if err != nil || repeat.CanExecute || repeat.Status != IntentDispatched {
		t.Fatalf("repeated admission = %+v, error = %v", repeat, err)
	}
	if err := store.ReleaseLease(ctx, scope, "session", lease); err != nil {
		t.Fatal(err)
	}
	next, err := store.AcquireSession(ctx, scope, "session", "second", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := store.BeginStandaloneTool(ctx, scope, "session", next, args)
	if err != nil || recovered.CanExecute || recovered.Status != IntentUnknown {
		t.Fatalf("recovered decision = %+v, error = %v", recovered, err)
	}
	existing, err := store.GetToolIntent(ctx, scope, "session", args.CallID)
	if err != nil || existing.Status != IntentUnknown {
		t.Fatalf("unknown outcome not persisted: %+v, error = %v", existing, err)
	}
	_, err = store.BeginStandaloneTool(ctx, scope, "session", lease, args)
	if contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("old lease: %v", err)
	}
}

func TestLostStandaloneAdmissionACKClosesIntentInCurrentEpoch(t *testing.T) {
	ctx, _, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "ack-" + rand.Text(), Deployment: "agent", Subject: "owner"}
	lease, err := store.AcquireOrCreateSession(ctx, scope, "session", "custom", 1, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("{}"))
	for _, committedWithoutACK := range []bool{false, true} {
		name := "before-commit"
		if committedWithoutACK {
			name = "after-commit"
		}
		page, err := store.GetEvents(ctx, scope, "session", 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.AppendEvents(ctx, scope, "session", lease, false, AppendRequest{
			Key: name, ExpectedSeq: page.HeadSeq,
			Events:  []EventInput{{ID: name, Type: "tool.call", Role: "tool_call", Codec: "test", Payload: Payload{Inline: []byte("{}")}}},
			Intents: []PreparedIntent{{CallID: name, ToolName: "write", ArgsDigest: digest, IdempotencyKey: name}},
		}); err != nil {
			t.Fatal(err)
		}
		request := StandaloneToolRequest{CallID: name, ToolName: "write", ArgsDigest: digest}
		if committedWithoutACK {
			admitted, err := store.BeginStandaloneTool(ctx, scope, "session", lease, request)
			if err != nil || !admitted.CanExecute {
				t.Fatalf("original admission: %+v, error = %v", admitted, err)
			}
		}
		request.AckLost = true
		recovered, err := store.BeginStandaloneTool(ctx, scope, "session", lease, request)
		if err != nil || recovered.CanExecute || recovered.Status != IntentUnknown {
			t.Fatalf("unknown ACK recovery: %+v, error = %v", recovered, err)
		}
		request.AckLost = false
		late, err := store.BeginStandaloneTool(ctx, scope, "session", lease, request)
		if err != nil || late.CanExecute || late.Status != IntentUnknown {
			t.Fatalf("late admission escaped hold: %+v, error = %v", late, err)
		}
	}
}
