package space

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

func TestAgentResultProjectsNativeA2A10TextAndKeepsOriginalArtifact(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		name := "native-text"
		if invalid {
			name = "ambiguous-part"
		}
		t.Run(name, func(t *testing.T) {
			var writes int
			var projected struct {
				Payload struct {
					Content struct {
						Text string          `json:"text"`
						A2A  json.RawMessage `json:"a2a"`
					} `json:"content"`
				} `json:"payload"`
			}
			state := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/state/AppendStream" {
					t.Errorf("unexpected State call %s", r.URL.Path)
				}
				writes++
				if err := json.NewDecoder(r.Body).Decode(&projected); err != nil {
					t.Error(err)
				}
				_, _ = w.Write([]byte(`{"sequence":1}`))
			}))
			defer state.Close()
			artifact, err := json.Marshal(a2a.Artifact{ID: "native-artifact", Parts: a2a.ContentParts{a2a.NewTextPart("native output")}})
			if err != nil {
				t.Fatal(err)
			}
			if invalid {
				artifact = []byte(`{"artifactId":"native-artifact","parts":[{"text":"hidden ambiguity","data":{"x":1}}]}`)
			}
			c := &Coordinator{state: NewStateClient(state.URL, state.Client())}
			lease := &leaseResponse{Run: runResponse{ID: "run"}, Worker: "worker", Epoch: 1}
			err = c.projectAgentResult(t.Context(), "task-token", &Turn{ID: "turn", ThreadID: "thread"}, lease, &AgentTaskSnapshot{Artifacts: []json.RawMessage{artifact}})
			if invalid {
				if err == nil || writes != 0 {
					t.Fatalf("ambiguous native part was projected: writes=%d error=%v", writes, err)
				}
				return
			}
			if err != nil || writes != 1 || projected.Payload.Content.Text != "native output" || string(projected.Payload.Content.A2A) != string(artifact) {
				t.Fatalf("native artifact lost text or provenance: writes=%d projected=%+v error=%v", writes, projected, err)
			}
		})
	}
}
