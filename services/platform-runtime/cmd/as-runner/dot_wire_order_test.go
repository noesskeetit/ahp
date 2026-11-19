package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// DEF-DOTE2E-11 on the model leg: a personal Dot turn with memory enabled must
// reach the Model Gateway as a serialized request whose only system message is
// the leading pinned-configuration block. The failed turn (20261109T170628...)
// sent [user(memory), system(config), user(input)] and the provider refused it
// terminally with "System message must be at the beginning."

type dotWireMemory struct{}

func (dotWireMemory) Prepare(context.Context, *space.CoreRequest) ([]core.ChatMessage, error) {
	return []core.ChatMessage{{Role: "user", Content: json.RawMessage(`"personal memory: prefers terse answers"`)}}, nil
}
func (dotWireMemory) Defs() []core.ToolDef { return nil }
func (dotWireMemory) Handles(string) bool  { return false }
func (dotWireMemory) Execute(context.Context, *space.CoreRequest, core.ToolCall) (json.RawMessage, error) {
	return nil, nil
}

func TestDotProfileMemoryReachesModelLegWithLeadingSystem(t *testing.T) {
	bodies := make(chan map[string]json.RawMessage, 4)
	mux := http.NewServeMux()
	capture := func(w http.ResponseWriter, r *http.Request) map[string]json.RawMessage {
		var in map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return nil
		}
		bodies <- in
		return in
	}
	genID := contracts.NewID()
	mux.HandleFunc("/v1/model/SubmitModel", func(w http.ResponseWriter, r *http.Request) {
		if capture(w, r) == nil {
			return
		}
		transport.WriteJSON(w, http.StatusOK, model.Generation{ID: genID, Status: model.GenCompleted, Output: `{"text":"wire fixture"}`})
	})
	// RemoteModel prefers the streaming leg; the fixture answers both with the
	// same completion so the assertion covers whichever the loop drives.
	mux.HandleFunc("/v1/model/StreamModel", func(w http.ResponseWriter, r *http.Request) {
		if capture(w, r) == nil {
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		encoder := json.NewEncoder(w)
		_ = encoder.Encode(map[string]any{"type": "accepted", "generation_id": genID})
		_ = encoder.Encode(map[string]any{"type": "delta", "delta": model.StreamDelta{GenerationID: genID, Sequence: 1, Text: "wire fixture"}})
		_ = encoder.Encode(map[string]any{"type": "terminal", "generation": model.Generation{ID: genID, Status: model.GenCompleted, Output: `{"text":"wire fixture"}`, DeltaSequence: 1}})
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	})
	server := httptest.NewUnstartedServer(mux)
	server.StartTLS()
	defer server.Close()
	remote, err := execbridge.NewRemoteModel(server.URL, "model-one", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	cfg := core.RuntimeConfig{Model: remote, Memory: dotWireMemory{}}
	cfg.Logger = discardLogger()
	runner := newProfileRuntime(cfg, nil)
	scope := contracts.Scope{Tenant: "dot-e2e", Subject: "alice", Deployment: "space"}
	task := "wire-order-" + contracts.NewID()
	req := &space.CoreRequest{
		RunID:     task,
		ContextID: task,
		TaskToken: "wire-fixture-token",
		Scope:     scope,
		Input:     json.RawMessage(`"hello"`),
		AgentConfig: &space.AgentConfigRef{Revision: 1, Dot: &space.DotRuntimeConfig{
			Instructions: "be terse",
			MaxSteps:     4,
		}},
	}
	results := make(chan space.StepResult, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := runner.Run(ctx, req, results); err != nil {
		t.Fatal(err)
	}
	var in map[string]json.RawMessage
	select {
	case in = <-bodies:
	case <-time.After(5 * time.Second):
		t.Fatal("the model leg never received a request")
	}
	var wire []model.Message
	if err := json.Unmarshal(in["messages"], &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire) != 3 {
		t.Fatalf("serialized model-leg request carries %d messages, want 3 (config, memory, input): %s", len(wire), in["messages"])
	}
	systems := 0
	for i, m := range wire {
		if m.Role != "system" {
			continue
		}
		systems++
		if i != 0 {
			t.Fatalf("system message sits at wire index %d of %s: the provider refuses a non-leading system with HTTP 400", i, in["messages"])
		}
	}
	if systems != 1 {
		t.Fatalf("system messages = %d, want exactly the one pinned-configuration block", systems)
	}
	joined := make([]string, 0, len(wire))
	for _, m := range wire {
		joined = append(joined, m.Role+":"+m.Content)
	}
	combined := strings.Join(joined, "\n")
	for _, want := range []string{"[AHP Dot configuration]", "be terse", "personal memory: prefers terse answers", "hello"} {
		if !strings.Contains(combined, want) {
			t.Fatalf("model-leg request lost %q, memory or the pinned prompt never reached the provider: %s", want, combined)
		}
	}
}
