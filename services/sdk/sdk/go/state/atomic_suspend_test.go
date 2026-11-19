package state

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Unit wire test only; actual state/lease behavior is tested on PostgreSQL.
func TestRunStatusWaitingPreservesWaitTargetAndDeadlineOnWire(t *testing.T) {
	for _, common := range []bool{false, true} {
		t.Run(map[bool]string{false: "batch", true: "common"}[common], func(t *testing.T) {
			testRunStatusWaitingWire(t, common)
		})
	}
}

func testRunStatusWaitingWire(t *testing.T, common bool) {
	t.Helper()
	got := make(chan map[string]any, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		got <- body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, HTTPClient: server.Client(), TokenSource: func(context.Context) (string, error) { return "fixture-token", nil }})
	if err != nil {
		t.Fatal(err)
	}
	var status RunStatusOp
	if err := json.Unmarshal([]byte(`{"status":"waiting","wait_for":"hitl_response","deadline":"2030-01-02T03:04:05Z"}`), &status); err != nil {
		t.Fatal(err)
	}
	fence := Fence{RunID: "run", Worker: "worker", Epoch: 1}
	if common {
		_, err = client.CommitRecords(context.Background(), CommonCommit{Fence: fence, Namespace: "native/a2a", Key: "park", Document: &CommonDocument{}, RunStatus: &status})
	} else {
		_, err = client.PutBatch(context.Background(), fence, BatchRequest{Key: "park", RunStatus: &status})
	}
	if err != nil {
		t.Fatal(err)
	}
	body := <-got
	if !common {
		body = body["batch"].(map[string]any)
	}
	wait := body["run_status"].(map[string]any)
	if wait["wait_for"] != "hitl_response" || wait["deadline"] != "2030-01-02T03:04:05Z" {
		t.Fatalf("waiting fields disappeared from SDK request: %v", wait)
	}
}

func TestRunStatusWithoutWaitKeepsLegacyWire(t *testing.T) {
	b, err := json.Marshal(RunStatusOp{Status: RunPending})
	if err != nil || string(b) != `{"status":"pending"}` {
		t.Fatalf("legacy status wire changed: %s error=%v", b, err)
	}
}
