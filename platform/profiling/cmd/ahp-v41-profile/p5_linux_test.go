package main

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	state "github.com/ahp-platform/ahp/sdk/go/state/journal"
)

func TestWorkerClaimsWithAdmittedProfileTTL(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/runtime/bootstrap", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(journal.RuntimeBootstrap{SessionID: "session", LaunchTurnSeq: 26, Generation: 1, ExecutionID: "exec"})
	})
	mux.HandleFunc("GET /v1/state/sessions/session/events", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after_seq") != "25" || r.URL.Query().Get("limit") != "1" {
			t.Errorf("unexpected launch input query: %s", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(journal.EventPage{Events: []journal.Event{{Seq: 26, Active: true, EventInput: journal.EventInput{
			Type: "turn.input", TurnSeq: 26, Payload: journal.Payload{Inline: []byte(`{"id":"boot","worker_claim_ttl_ms":15000}`)},
		}}}})
	})
	claims := make(chan journal.VMTurnClaimRequest, 1)
	mux.HandleFunc("POST /v1/state/sessions/session/vm:claim", func(w http.ResponseWriter, r *http.Request) {
		var req journal.VMTurnClaimRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		claims <- req
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	dir, err := os.MkdirTemp("/tmp", "p5-injector-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "state.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: mux}
	t.Cleanup(func() { _ = srv.Close() })
	go func() { _ = srv.Serve(listener) }()
	client, err := state.NewUnixInjectorClient(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = runWorkerTurns(t.Context(), client, nil)
	select {
	case claim := <-claims:
		if claim.TTLMilliseconds != 15000 || claim.TurnSeq != 26 || claim.Generation != 1 || claim.ExecutionID != "exec" {
			t.Fatalf("unexpected claim: %+v", claim)
		}
	default:
		t.Fatal("worker did not attempt a claim")
	}
}

func TestLongOccupancySeparatesReservationsExecutionAndStreams(t *testing.T) {
	stateSampleTime := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	c := &collector{
		started: 100, stoppedAt: 400,
		samples: []sample{{MonotonicNS: 200, StateSampleAt: &stateSampleTime, Queue: map[string]int64{"ACTIVE": 4}}},
		rows: []requestResult{
			{SSEOpenedAt: 195, SSEClosedAt: 201},
			{SSEOpenedAt: 200, SSEClosedAt: 210},
			{SSEOpenedAt: 190, SSEClosedAt: 200},
		},
	}
	intervals := []journalInterval{
		{Tenant: "one", Session: "a", WorkerMonoNS: 180, CommitMonoNS: 205, Completed: &stateSampleTime},
		{Tenant: "one", Session: "b", WorkerMonoNS: 170, CommitMonoNS: 200, Completed: &stateSampleTime},
		{Tenant: "one", Session: "c", WorkerMonoNS: 170, CommitMonoNS: 210, Completed: &stateSampleTime, Superseded: true},
		{Tenant: "one", Session: "d", WorkerMonoNS: 195},
	}
	got := summarizeLongOccupancy(c, intervals, false, true, 4)
	if got.ActiveSessions.P95 != 4 || got.ExecutingTurns.P95 != 1 || got.OpenStreams.P95 != 2 || got.MissingTerminals != 1 {
		t.Fatalf("occupancy reported reservation, execution or SSE incorrectly: %+v", got)
	}
}

func TestExecutionEvidenceCoversVerifiedTurns(t *testing.T) {
	completed := time.Unix(100, 0)
	turns := []requestResult{
		{ID: "p5-c16-turn-1", SessionID: "session", Seq: 30, VerifiedAt: 1},
		{ID: "p5-c16-turn-2", SessionID: "session", Seq: 38, Failure: "sse_interrupted"},
	}
	intervals := []journalInterval{{ID: "p5-c32-turn-1", Session: "session", TurnSeq: 30, Completed: &completed}}
	if err := verifyLongIntervals(turns, intervals); err == nil {
		t.Fatal("verified turn was matched to execution evidence from another stage")
	}
	intervals[0].ID = "p5-c16-turn-1"
	if err := verifyLongIntervals(turns, intervals); err != nil {
		t.Fatalf("verified turn did not match its own execution evidence: %v", err)
	}
}
