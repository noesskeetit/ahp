package space

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTerminalCheckpointRetainsExactDurableReference(t *testing.T) {
	metadata := json.RawMessage(`{"sequence":7,"messages":[{"role":"user","content":"input"}],"state":{"steps":7}}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]string
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if req["id"] != "snapshot-0" || req["thread_id"] != "thread" || req["namespace"] != CheckpointNamespace ||
			r.Header.Get("Authorization") != "Bearer caller-task" {
			t.Errorf("unexpected GetCheckpoint request: %s %v", r.URL.Path, req)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(checkpointResponse{ID: req["id"], Metadata: metadata})
	}))
	defer server.Close()

	c := NewCoordinator(CoordinatorConfig{StateClient: NewStateClient(server.URL, server.Client())})
	for i := 0; i <= maxTerminalCheckpointEntries; i++ {
		turnID := fmt.Sprintf("turn-%d", i)
		turn := &Turn{ID: turnID, ThreadID: "thread", Status: TurnDone, Steps: 7, Checkpoint: &Checkpoint{
			ID: fmt.Sprintf("snapshot-%d", i), Namespace: CheckpointNamespace, Parent: "parent", TurnID: turnID,
			Sequence: 7, Metadata: metadata, Messages: json.RawMessage(`[{"role":"user","content":"input"}]`),
			State: json.RawMessage(`{"steps":7}`), runtimeSnapshot: true,
		}}
		c.turns[turnID] = turn
		c.rememberTerminalCheckpoint(turn)
	}
	cold := *c.turns["turn-0"]
	if !cold.Checkpoint.payloadCold || cold.Checkpoint.Metadata != nil || cold.Checkpoint.State != nil ||
		cold.Checkpoint.Messages != nil || c.terminalCheckpoints.order.Len() != maxTerminalCheckpointEntries {
		t.Fatalf("old body was retained: %+v", cold.Checkpoint)
	}
	fetched, err := c.loadTerminalCheckpoint(context.Background(), "caller-task", cold)
	if err != nil {
		t.Fatal(err)
	}
	cp := fetched.Checkpoint
	if cp.ID != "snapshot-0" || cp.Parent != "parent" || cp.Sequence != 7 || fetched.Steps != 7 ||
		string(cp.Metadata) != string(metadata) || string(cp.State) != `{"steps":7}` ||
		string(cp.Messages) != `[{"role":"user","content":"input"}]` {
		t.Fatalf("rehydrated a different checkpoint: %+v", cp)
	}
	if !cold.Checkpoint.payloadCold || !c.turns["turn-0"].Checkpoint.payloadCold {
		t.Fatal("read repopulated completed-turn memory")
	}
	c.forgetThread("thread")
	if c.terminalCheckpoints.order.Len() != 0 || c.terminalCheckpoints.bytes != 0 {
		t.Fatal("deleted thread retained checkpoint bodies")
	}
}

func TestTerminalCheckpointByteBudget(t *testing.T) {
	turn := &Turn{ID: "large", Status: TurnDone, Checkpoint: &Checkpoint{
		ID: "snapshot", runtimeSnapshot: true,
		Metadata: make([]byte, 1, maxTerminalCheckpointBytes),
		Messages: make([]byte, 1),
	}}
	c := &Coordinator{turns: map[string]*Turn{turn.ID: turn}}
	c.rememberTerminalCheckpoint(turn)
	if !turn.Checkpoint.payloadCold || cap(turn.Checkpoint.Metadata) != 0 || c.terminalCheckpoints.bytes != 0 {
		t.Fatal("oversized checkpoint exceeded terminal cache budget")
	}
}
