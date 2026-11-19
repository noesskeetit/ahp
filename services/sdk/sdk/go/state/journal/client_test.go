package journal_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/sdk/go/state/journal"
)

func TestLostStandaloneAdmissionResponseUsesRecoveryFlag(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Epoch int64 `json:"lease_epoch"`
			journal.StandaloneToolRequest
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.CallID != "call" || input.Epoch != 9 {
			t.Errorf("admission body = %+v, error = %v", input, err)
		}
		call := calls.Add(1)
		if call == 1 {
			if input.AckLost {
				t.Error("first admission claimed to have lost its response")
			}
			w.Header().Set("Content-Length", "100")
			fmt.Fprint(w, `{"can_execute":`)
			return
		}
		if call != 2 || !input.AckLost {
			t.Errorf("retry attempted a second dispatch: attempt %d, body %+v", call, input)
		}
		fmt.Fprint(w, `{"can_execute":false,"status":"OUTCOME_UNKNOWN","call_id":"call"}`)
	}))
	defer server.Close()
	client, err := journal.New(journal.Config{
		BaseURL: server.URL, HTTPClient: server.Client(), TokenSource: func(context.Context) (string, error) {
			return "task-token", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.BeginStandaloneTool(context.Background(), "session", 9, journal.StandaloneToolRequest{
		CallID: "call", ToolName: "reserve", ArgsDigest: sha256.Sum256([]byte("{}")),
	})
	if err != nil || result.CanExecute || result.Status != "OUTCOME_UNKNOWN" || calls.Load() != 2 {
		t.Fatalf("lost ACK handling returned %+v, error = %v, attempts = %d", result, err, calls.Load())
	}
}

func TestJournalCheckpointRoundTripAndFreshToken(t *testing.T) {
	ctx := context.Background()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/state/sessions:acquire", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Session string `json:"session_id"`
			Harness string `json:"harness_type"`
			TTL     int64  `json:"ttl_ms"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Session != "session" || body.Harness != "custom-go" || body.TTL != 60000 {
			t.Errorf("acquire body = %+v, error = %v", body, err)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer task-1" {
			t.Errorf("authorization = %q", got)
		}
		fmt.Fprint(w, `{"worker":"worker","epoch":2,"expires_at":"2026-11-19T18:00:00Z"}`)
	})
	mux.HandleFunc("POST /v1/state/sessions/session/checkpoints", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Epoch int64 `json:"lease_epoch"`
			journal.CheckpointCommit
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Epoch != 2 || body.Key != "op-1" || string(body.Checkpoint.Payload.Inline) != "payload" {
			t.Errorf("checkpoint body = %+v, error = %v", body, err)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer task-2" {
			t.Errorf("authorization = %q", got)
		}
		fmt.Fprint(w, `{"checkpoint_id":"checkpoint"}`)
	})
	mux.HandleFunc("GET /v1/state/sessions/session/checkpoints", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer task-3" {
			t.Errorf("authorization = %q", got)
		}
		if r.URL.Query().Get("checkpoint_ns") != "namespace" || r.URL.Query().Get("checkpoint_id") != "checkpoint" {
			t.Errorf("unexpected query %q", r.URL.RawQuery)
		}
		fmt.Fprint(w, `{"checkpoint":{"id":"checkpoint","codec":"custom-v1","payload":{"inline":"cGF5bG9hZA=="}},"writes":[]}`)
	})
	mux.HandleFunc("POST /v1/state/sessions/session/events:append", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		fmt.Fprint(w, `{"code":"STALE_EPOCH","message":"ownership changed"}`)
	})
	server := httptest.NewTLSServer(mux)
	defer server.Close()
	count := 0
	client, err := journal.New(journal.Config{
		BaseURL: server.URL, HTTPClient: server.Client(), TokenSource: func(context.Context) (string, error) {
			count++
			return fmt.Sprintf("task-%d", count), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := client.AcquireSession(ctx, "session", "custom-go", 1, time.Minute)
	if err != nil || lease.Epoch != 2 {
		t.Fatalf("lease = %+v, error = %v", lease, err)
	}
	id, err := client.PutCheckpoint(ctx, "session", lease.Epoch, journal.CheckpointCommit{Key: "op-1",
		Checkpoint: journal.Checkpoint{Namespace: "namespace", ID: "checkpoint", Codec: "custom-v1",
			Metadata: json.RawMessage(`{}`), ChannelVersions: json.RawMessage(`{}`), Payload: journal.Payload{Inline: []byte("payload")}},
	})
	if err != nil || id != "checkpoint" {
		t.Fatalf("checkpoint ID = %q, error = %v", id, err)
	}
	got, err := client.GetCheckpoint(ctx, "session", "namespace", id)
	if err != nil || string(got.Checkpoint.Payload.Inline) != "payload" {
		t.Fatalf("checkpoint = %+v, error = %v", got, err)
	}
	if _, err := client.AppendEvents(ctx, "session", 1, journal.AppendRequest{Key: "op-2"}); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("expired writer: %v", err)
	}
}
