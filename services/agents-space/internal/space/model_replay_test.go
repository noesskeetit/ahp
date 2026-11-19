package space

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestModelReplayDoesNotAttachLatePriorResultToNextIntent(t *testing.T) {
	oldKey, newKey := ModelIntentKey("run", 0, 0), ModelIntentKey("run", 1, 0)
	events := []map[string]any{
		{"sequence": 1, "key": oldKey, "kind": "model_intent", "payload": modelIntent{Step: 0, Request: json.RawMessage(`{"Step":0}`)}},
		{"sequence": 2, "key": newKey, "kind": "model_intent", "payload": modelIntent{Step: 1, Request: json.RawMessage(`{"Step":1}`)}},
		{"sequence": 3, "key": "run:0", "kind": "model", "payload": StepResult{ModelIntentKey: oldKey, StepType: StepModel, Content: json.RawMessage(`"old assistant"`)}},
		{"sequence": 4, "key": "run:1", "kind": "tool", "payload": StepResult{ModelIntentKey: oldKey, StepType: StepTool, Content: json.RawMessage(`"old tool"`)}},
		{"sequence": 5, "key": "run:2", "kind": "done", "payload": StepResult{ModelIntentKey: oldKey, StepType: StepDone}},
	}
	for _, event := range events {
		event["thread_id"] = "thread"
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			After int `json:"after"`
			Limit int `json:"limit"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		page := make([]map[string]any, 0, request.Limit)
		for _, event := range events {
			if event["sequence"].(int) > request.After && len(page) < request.Limit {
				page = append(page, event)
			}
		}
		next := request.After
		if len(page) > 0 {
			next = page[len(page)-1]["sequence"].(int)
		}
		json.NewEncoder(w).Encode(map[string]any{"events": page, "has_more": next < len(events), "next_after": next})
	}))
	defer srv.Close()
	c := NewCoordinator(CoordinatorConfig{StateClient: NewStateClient(srv.URL, srv.Client())})
	got, err := c.modelReplay(context.Background(), "token", "thread", "run")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Step != 1 || got.Completed || got.Done || len(got.Tools) != 0 {
		t.Fatalf("prior result corrupted next intent: %+v", got)
	}
}
