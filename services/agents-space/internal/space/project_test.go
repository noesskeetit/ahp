package space

import (
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestConversationCannotChangeMemoryProjectAfterReload(t *testing.T) {
	run := func(project string) runResponse {
		p, _ := json.Marshal(turnRunPayload{Type: "turn", ProjectID: project})
		return runResponse{Payload: p}
	}
	if got, err := conversationProject([]runResponse{run("alpha")}, ""); err != nil || got != "alpha" {
		t.Fatal(got, err)
	}
	for _, runs := range [][]runResponse{{run("alpha")}, {run("")}, {run("alpha"), run("beta")}} {
		if _, err := conversationProject(runs, "beta"); contracts.Code(err) != contracts.ErrConflict {
			t.Fatal("conversation silently switched project", runs, err)
		}
	}
}

func TestColdTurnRestoresProjectFromDurablePayload(t *testing.T) {
	turn := &Turn{}
	restoreTurnFromRunPayload(turn, json.RawMessage(`{"project_id":"alpha","input":"continue","max_steps":8}`))
	if turn.ProjectID != "alpha" || turn.MaxSteps != 8 || string(turn.Input) != `"continue"` {
		t.Fatal(turn)
	}
	restoreTurnFromRunPayload(turn, json.RawMessage(`{"project_id":"beta"}`))
	if turn.ProjectID != "alpha" {
		t.Fatal("rehydration replaced an existing project", turn)
	}
}

func TestConversationCannotMixMemoryProfilesForSameProject(t *testing.T) {
	runs := []runResponse{
		{Payload: json.RawMessage(`{"type":"turn","project_id":"alpha","project_memory_profile":"shared"}`)},
		{Payload: json.RawMessage(`{"type":"turn","project_id":"alpha"}`)},
	}
	if _, err := conversationProject(runs, "alpha"); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("shared/private project histories mixed: %v", err)
	}
}

func TestColdTurnRestoresProjectMemoryProfile(t *testing.T) {
	turn := &Turn{}
	restoreTurnFromRunPayload(turn, json.RawMessage(`{"project_id":"alpha","project_memory_profile":"shared","input":"continue"}`))
	for range 2 {
		raw, err := json.Marshal(turn)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err = json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		if fields["project_memory_profile"] != "shared" {
			t.Fatalf("durable project profile missing or overwritten: %s", raw)
		}
		restoreTurnFromRunPayload(turn, json.RawMessage(`{"project_id":"alpha","project_memory_profile":"native"}`))
	}
}
