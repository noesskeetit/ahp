//go:build state_integration

package space_test

// d4_canceled_stream_realstate_test.go — the SAME D4 probe as
// d4_canceled_stream_terminal_test.go, but over the REAL State API on PostgreSQL
// (cNewEnv: production stateapi handler, mTLS workload identity, Ed25519 task
// tokens, real lease fencing) behind the production Space HTTP handler. Nothing is
// a double here: the canceled run, its lease, its stream rows and the SSE tail are
// all real, on this lane's own database and its own ephemeral port.

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// newD4RealStateSpace is the production Space edge (Handler + Coordinator + real
// StateClient over PG) with a Core that parks and emits nothing: the prod D4 shape.
func newD4RealStateSpace(t *testing.T, core space.CoreRunner) (*httptest.Server, string, *cEnv) {
	t.Helper()
	e := cNewEnv(t)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client,
		Core:        core,
		WorkerID:    "w-d4-" + contracts.NewID(),
	})
	auth := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := transport.WithPrincipal(r.Context(), transport.Principal{
				Scope: contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject},
			})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	h := space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: auth})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, e.signTask(t, "task-"+contracts.NewID()), e
}

// readSSEBearerUntilEnd reads the SSE tail with a bearer token until the server
// closes the body or the deadline expires. Same contract as readSSEUntilEnd, for the
// task-token caller the real-plane harness signs.
func readSSEBearerUntilEnd(t *testing.T, url, bearer string, deadline time.Duration) ([]sseFrame, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("newreq: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer "+bearer)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET sse: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SSE status = %d, want 200 (body=%q)", resp.StatusCode, readAll(resp.Body))
	}
	br := bufio.NewReader(resp.Body)
	var frames []sseFrame
	var evLine, data string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return frames, err == io.EOF
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			if data == "" {
				continue
			}
			var env map[string]any
			_ = json.Unmarshal([]byte(data), &env)
			frames = append(frames, sseFrame{event: strings.TrimSpace(strings.TrimPrefix(evLine, "event:")), envel: env, raw: data})
			evLine, data = "", ""
		case strings.HasPrefix(line, "id:"):
		case strings.HasPrefix(line, "event:"):
			evLine = line
		case strings.HasPrefix(line, "data:"):
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
}

// statusOfPayload reads {"status":...} out of a durable stream payload.
func statusOfPayload(raw json.RawMessage) string {
	var v struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(raw, &v)
	return v.Status
}

// TestD4CanceledThreadStreamTerminates_RealState is the live stand probe: accept a
// turn, run it (Core parked), cancel it, then subscribe to the thread exactly as the
// browser does. On the base head this is the prod symptom: the run is canceled, its
// lease is gone, the worker's terminal frame is refused by the real fence, and the
// tail delivers nothing and never closes.
func TestD4CanceledThreadStreamTerminates_RealState(t *testing.T) {
	core := newSilentCore()
	srv, bearer, e := newD4RealStateSpace(t, core)

	code, body := postSpace(t, srv.URL, "AcceptTurn", map[string]any{
		"key": "k-d4-real-" + contracts.NewID(), "context_id": "ctx-d4-real",
		"input": map[string]any{"role": "user", "content": "go"}, "max_steps": 4,
	}, bearer)
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn over real State = %d (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	threadID, _ := acc["thread_id"].(string)
	turnID, _ := acc["turn_id"].(string)
	if threadID == "" || turnID == "" {
		t.Fatalf("no thread/turn in %s", body)
	}
	if c, b := postSpace(t, srv.URL, "DispatchTurn", nil, bearer); c != http.StatusOK {
		t.Fatalf("DispatchTurn = %d (body=%s)", c, b)
	}
	select {
	case <-core.started:
	case <-time.After(10 * time.Second):
		t.Fatal("core never started on the real plane")
	}

	if c, b := postSpace(t, srv.URL, "CancelTurn", map[string]any{"turn_id": turnID}, bearer); c != http.StatusOK {
		t.Fatalf("CancelTurn = %d (body=%s)", c, b)
	}

	// Durable truth first, read straight from PostgreSQL through the real State API:
	// the run is closed as canceled and its stream carries a terminal frame.
	var (
		runStatus  string
		runFrames  int
		lastStatus string
	)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		runs, err := e.client.ListRuns(e.ctx, bearer, threadID)
		if err != nil {
			t.Fatalf("ListRuns over real State: %v", err)
		}
		if len(runs) == 1 {
			runStatus = runs[0].Status
		}
		evs, err := e.client.ReadStream(e.ctx, bearer, threadID, 0, 200)
		if err != nil {
			t.Fatalf("ReadStream over real State: %v", err)
		}
		runFrames = len(evs)
		lastStatus = ""
		for _, ev := range evs {
			if ev.Kind == "status" {
				lastStatus = statusOfPayload(ev.Payload)
			}
		}
		if runStatus == "canceled" && lastStatus != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if runStatus != "canceled" {
		t.Fatalf("the run is not durably canceled on the real plane: %q", runStatus)
	}
	if lastStatus != "canceled" {
		t.Fatalf("D4 on the real plane: the durable stream of a canceled run has no terminal status frame (frames=%d, last status=%q)", runFrames, lastStatus)
	}

	// The browser leg: the SSE tail must state the terminal status and close.
	frames, closed := readSSEBearerUntilEnd(t, srv.URL+"/v1/space/stream/"+threadID, bearer, 12*time.Second)
	term, ok := terminalStatus(frames)
	if !ok {
		t.Fatalf("D4 on the real plane: the tail of a canceled thread delivered %d frames in 12s and closed=%v (frames=%q)",
			len(frames), closed, firstFrames(frames, 5))
	}
	if term != "canceled" {
		t.Fatalf("tail reported %q, want canceled", term)
	}
	if !closed {
		t.Fatalf("D4 on the real plane: the tail never closed the connection")
	}
}
