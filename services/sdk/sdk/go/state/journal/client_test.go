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

func TestCatalogAndCheckpointTupleQueries(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/state/sessions", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if query.Get("view") != "catalog" || query.Get("harness") != "hermes" ||
			query.Get("cursor") != "next+page" || query.Get("search_query") != "test & trace" ||
			len(query["configured_agent_id"]) != 2 {
			t.Errorf("catalog query = %v", query)
		}
		fmt.Fprint(w, `{"sessions":[{"session_id":"session"}],"next_cursor":"more"}`)
	})
	mux.HandleFunc("GET /v1/state/sessions/session/checkpoints", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if query.Get("list") != "true" || query.Get("include_tuples") != "true" ||
			query.Get("before") != "prev" || query.Get("checkpoint_ns") != "namespace" ||
			query.Get("limit") != "20" || query.Get("metadata") != `{"step":4}` {
			t.Errorf("checkpoint list query = %v", query)
		}
		fmt.Fprint(w, `{"checkpoint_ids":["id"],"checkpoint_tuples":[{"checkpoint":{"id":"id"},"writes":[{"channel":"chat"}]}]}`)
	})
	client := newTestJournal(t, mux)
	ctx := context.Background()
	page, err := client.ListCatalog(ctx, journal.CatalogQuery{
		Harness: "hermes", Cursor: "next+page", Search: "test & trace",
		AgentIDs: []string{"one", "two"},
	})
	if err != nil || len(page.Sessions) != 1 || page.Sessions[0].ID != "session" || page.NextCursor != "more" {
		t.Fatalf("catalog = %+v, error = %v", page, err)
	}
	tuples, err := client.ListCheckpointTuples(ctx, "session", "namespace", "prev", 20, json.RawMessage(`{"step":4}`))
	if err != nil || len(tuples) != 1 || tuples[0].Checkpoint.ID != "id" ||
		len(tuples[0].Writes) != 1 || tuples[0].Writes[0].Channel != "chat" {
		t.Fatalf("checkpoint tuples = %+v, error = %v", tuples, err)
	}
}

func TestHeartbeatCancelsOnFenceAndReleaseToWaitingInput(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/state/sessions/session:heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Epoch   int64  `json:"lease_epoch"`
			TTL     int64  `json:"ttl_ms"`
			Release bool   `json:"release"`
			Status  string `json:"status"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Epoch != 5 {
			t.Errorf("heartbeat body = %+v, error = %v", body, err)
		}
		if body.Release {
			if body.Status != "WAITING_INPUT" {
				t.Errorf("release status = %q", body.Status)
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if body.TTL != 60 {
			t.Errorf("heartbeat TTL = %d", body.TTL)
		}
		w.WriteHeader(http.StatusConflict)
		fmt.Fprint(w, `{"code":"STALE_EPOCH","message":"ownership changed"}`)
	})
	client := newTestJournal(t, mux)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	leaseCtx, stop := client.RunHeartbeat(ctx, "session", 5, 60*time.Millisecond)
	defer stop()
	<-leaseCtx.Done()
	if got := contracts.Code(context.Cause(leaseCtx)); got != contracts.ErrStaleEpoch {
		t.Fatalf("heartbeat cancellation cause = %v", context.Cause(leaseCtx))
	}
	if err := client.ReleaseLeaseTo(ctx, "session", 5, "WAITING_INPUT"); err != nil {
		t.Fatal(err)
	}
}

func TestJournalPayloadSpillsAndChecksSizeAndDigest(t *testing.T) {
	data := make([]byte, 256<<10+1)
	data[len(data)-1] = 42
	ref := journal.BlobRef{
		ArtifactID: "blob", Version: "1", SizeBytes: int64(len(data)),
		SHA256: fmt.Sprintf("%x", sha256.Sum256(data)),
	}
	var staged atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/state/sessions/session/blobs:stage", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Epoch int64  `json:"lease_epoch"`
			Data  []byte `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.Epoch != 5 ||
			sha256.Sum256(input.Data) != sha256.Sum256(data) {
			t.Errorf("staged epoch = %d, size = %d, error = %v", input.Epoch, len(input.Data), err)
		}
		staged.Add(1)
		_ = json.NewEncoder(w).Encode(ref)
	})
	mux.HandleFunc("POST /v1/state/sessions/session/blobs:read", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string][]byte{"data": data})
	})
	client := newTestJournal(t, mux)
	ctx := context.Background()
	for _, size := range []int{0, 256 << 10} {
		payload, err := client.EncodePayload(ctx, "session", 5, data[:size])
		if err != nil || payload.Blob != nil || payload.Inline == nil || len(payload.Inline) != size {
			t.Fatalf("inline %d = %d bytes, blob = %+v, error = %v", size, len(payload.Inline), payload.Blob, err)
		}
	}
	payload, err := client.EncodePayload(ctx, "session", 5, data)
	if err != nil || payload.Blob == nil || *payload.Blob != ref || staged.Load() != 1 {
		t.Fatalf("spill = %+v, error = %v, staged = %d", payload.Blob, err, staged.Load())
	}
	if got, err := client.DecodePayload(ctx, "session", payload); err != nil || sha256.Sum256(got) != sha256.Sum256(data) {
		t.Fatalf("decode = %d bytes, error = %v", len(got), err)
	}
	wrongSize, wrongDigest := ref, ref
	wrongSize.SizeBytes++
	wrongDigest.SHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte("different")))
	for _, invalid := range []journal.BlobRef{wrongSize, wrongDigest} {
		if _, err := client.DecodePayload(ctx, "session", journal.Payload{Blob: &invalid}); err == nil {
			t.Fatalf("accepted invalid descriptor %+v", invalid)
		}
	}
}

func newTestJournal(t *testing.T, handler http.Handler) *journal.Client {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	client, err := journal.New(journal.Config{
		BaseURL: server.URL, HTTPClient: server.Client(),
		TokenSource: func(context.Context) (string, error) { return "task-token", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestFinishClaimedTurnReplaysPreparedResult(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got struct {
			Epoch int64 `json:"lease_epoch"`
			journal.AppendRequest
		}
		if r.URL.Path != "/v1/state/sessions/session/events:append" {
			t.Errorf("unexpected terminal route %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil ||
			got.Epoch != 8 || got.ExpectedSeq != 12 || got.Key != "turn.completed:4" ||
			got.FinishTurn != "WAITING_INPUT" || len(got.Events) != 1 ||
			got.Events[0].Type != "turn.completed" || got.Events[0].Payload.Blob == nil {
			t.Errorf("invalid completion request %+v: %v", got, err)
		}
		calls.Add(1)
		fmt.Fprint(w, `{"first_seq":13,"head_seq":13}`)
	}))
	defer server.Close()

	c, err := journal.New(journal.Config{BaseURL: server.URL, HTTPClient: server.Client(),
		TokenSource: func(context.Context) (string, error) { return "test-task", nil }})
	if err != nil {
		t.Fatal(err)
	}
	turn := journal.ClaimedTurn{}
	turn.SessionID, turn.Epoch, turn.InputSeq = "session", 8, 4
	payload := journal.Payload{Blob: &journal.BlobRef{ArtifactID: "fixture", SizeBytes: 300_000}}
	for range 2 {
		receipt, err := c.FinishClaimedTurn(context.Background(), turn, 12, payload, true)
		if err != nil || receipt.HeadSeq != 13 {
			t.Fatalf("terminal replay returned %+v: %v", receipt, err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("terminal method re-staged or retried unexpectedly: %d requests", calls.Load())
	}
}
