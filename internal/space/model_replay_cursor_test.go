package space

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestModelReplayCursorUsesOnlyContiguousConfirmedDeltas(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(map[bool]string{false: "confirmed-prefix", true: "gap-refused"}[bad], func(t *testing.T) {
			intent := ModelIntentKey("run", 0, 0)
			rows := []streamEventResponse{{ThreadID: "thread", Sequence: 1, Key: intent, Kind: "model_intent", Payload: json.RawMessage(`{"step":0,"attempt":0,"request":{"Step":0}}`)}}
			for n := int64(1); n <= 2; n++ {
				seq := n
				if bad && n == 2 {
					seq = 3
				}
				body, _ := json.Marshal(StepResult{ModelIntentKey: intent, StepType: StepModelDelta, GenerationID: "generation", DeltaSequence: seq, Content: json.RawMessage(`"x"`)})
				rows = append(rows, streamEventResponse{ThreadID: "thread", Sequence: n + 1, Key: ModelDeltaKey("run", "generation", seq), Kind: "model_delta", Payload: body})
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request map[string]any
				_ = json.NewDecoder(r.Body).Decode(&request)
				if request["durable_only"] != true {
					t.Error("recovery cursor was derived from a live/UI page that may contain volatile fragments")
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"events": rows, "has_more": false, "next_after": 3})
			}))
			defer server.Close()
			c := NewCoordinator(CoordinatorConfig{StateClient: NewStateClient(server.URL, server.Client())})
			replay, err := c.modelReplay(context.Background(), "token", "thread", "run")
			if bad {
				if err == nil {
					t.Fatal("recovery skipped a missing fragment")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			body, _ := json.Marshal(replay)
			var got map[string]any
			_ = json.Unmarshal(body, &got)
			if got["GenerationID"] != "generation" || got["DeltaSequence"] != float64(2) {
				t.Fatalf("confirmed stream prefix lost: %s", body)
			}
		})
	}
}
