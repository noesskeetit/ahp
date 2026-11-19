package space

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRecoveryReadsBoundedTranscriptPagesWithoutTruncating(t *testing.T) {
	for _, mode := range []string{"model-receipt", "step-high-water"} {
		t.Run(mode, func(t *testing.T) {
			reads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads++
				var in struct {
					After int64 `json:"after"`
					Limit int   `json:"limit"`
					Bytes int   `json:"max_response_bytes"`
				}
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Error(err)
				}
				end := min(in.After+int64(in.Limit), 513)
				// A bounded owner may return a short page while more data exists.
				// The reader must use has_more, not infer EOF from page length.
				if in.Bytes > 0 && in.After == 0 {
					end = 3
				}
				events := make([]streamEventResponse, 0, end-in.After)
				for n := in.After + 1; n <= end; n++ {
					e := streamEventResponse{ID: "event", ThreadID: "thread", Key: "run:delta:g", Kind: "model_delta", Sequence: n, Payload: json.RawMessage(`{"content":"partial"}`)}
					if n == 1 {
						e.Key = ModelIntentKey("run", 0, 0)
						e.Kind = "model_intent"
						e.Payload = json.RawMessage(`{"step":0,"attempt":0,"request":{"max_tokens":20000}}`)
					}
					if n == 513 {
						e.Key = StepKey("run", 0)
						e.Kind = "model"
						e.Payload = json.RawMessage(`{"model_intent_key":"` + ModelIntentKey("run", 0, 0) + `","content":"confirmed"}`)
					}
					events = append(events, e)
				}
				out := map[string]any{"events": events}
				if in.Bytes > 0 {
					out["has_more"] = end < 513
					out["next_after"] = end
				}
				_ = json.NewEncoder(w).Encode(out)
			}))
			defer server.Close()
			coord := NewCoordinator(CoordinatorConfig{StateClient: NewStateClient(server.URL, server.Client())})
			if mode == "model-receipt" {
				replay, err := coord.modelReplay(context.Background(), "token", "thread", "run")
				if err != nil || replay == nil || !replay.Completed {
					t.Fatal("recovery lost confirmed model receipt on a short byte-limited page", err)
				}
			} else {
				high, frames, err := coord.durableStepTranscript(context.Background(), "token", "thread", "run")
				if err != nil || high != 1 || len(frames) != 1 {
					t.Fatal("recovery lost its durable step high-water", high, len(frames), err)
				}
			}
			if reads > 8 {
				t.Fatalf("recovery used %d reads for 513 small records; byte-bounded paging should avoid hundreds of round trips", reads)
			}
		})
	}
}
