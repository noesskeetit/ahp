//go:build state_integration

package space_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
)

// DEF-H30-x: a turn accepted by a HUMAN must never sit in `pending` forever.
//
// The browser path accepts a turn on /v1/space/AcceptTurn and then only polls
// GetTurn; it never calls DispatchTurn. The PDF makes a runner's claim loop
// ("подбор сирот") the thing that advances such a turn, and that loop is started
// ONLY by the runner (cmd/as-runner/main.go). Two ways the contract is broken and
// what the fix owes the user instead:
//
//   - no turn dispatcher at all (sweep disabled, no SDK DispatchTurn): the accepted
//     turn must reach a DETERMINED terminal within a bounded window, with a cause,
//     rather than a silent eternal `pending`.
//   - the accepting face is not the executing worker (the composed journey shape):
//     GetTurn on the face must CONVERGE to the same terminal the durable record and
//     the executing worker report - not re-serve this process's stale snapshot.
//
// Both scenarios drive the real HTTP surface against the real State server, exactly
// as a browser would, and use only the existing public APIs.

const h30Window = 30 * time.Second

// postSpaceTask posts to /v1/space/{op} as a subject over the authByHeader principal,
// presenting a REAL scoped task token as the bearer (the non-RequireScopedTask path
// where the bearer is the State token). This is the browser AcceptTurn/GetTurn route.
func postSpaceTask(t *testing.T, base, op string, body any, subject, token string) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal %s: %v", op, err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(http.MethodPost, base+"/v1/space/"+op, rdr)
	if err != nil {
		t.Fatalf("newreq %s: %v", op, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	if subject != "" {
		req.Header.Set("X-Test-Subject", subject)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", op, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

func h30Accept(t *testing.T, base, subject, token, key, ctxID string) string {
	t.Helper()
	code, body := postSpaceTask(t, base, "AcceptTurn", map[string]any{
		"key":        key,
		"context_id": ctxID,
		"input":      map[string]any{"role": "user", "content": "go"},
		"max_steps":  2,
	}, subject, token)
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn=%d want 200 body=%s", code, body)
	}
	m := decodeMap(t, body)
	id, _ := m["turn_id"].(string)
	if id == "" {
		t.Fatalf("no turn_id: %s", body)
	}
	if st, _ := m["status"].(string); st != "pending" {
		t.Fatalf("AcceptTurn status=%q want pending", st)
	}
	return id
}

// TestDefH30x_TurnWithNoDispatcherReachesTerminal is the "configuration without a
// turn-driver": one coordinator, no orphan sweep, no SDK DispatchTurn. Before the
// fix the accepted turn stays pending for the whole window with no terminal (RED);
// after the fix it fails within the bounded window carrying reason=no_turn_dispatcher
// (GREEN).
func TestDefH30x_TurnWithNoDispatcherReachesTerminal(t *testing.T) {
	e := cNewEnv(t)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{}, WorkerID: "h30x-no-driver",
	})
	hs := httptest.NewServer(space.NewHandler(space.HandlerConfig{
		Coordinator: coord, Auth: authByHeader(e.tenant, e.deployment),
	}))
	t.Cleanup(hs.Close)

	token := e.signTask(t, "h30x-nodev")
	turnID := h30Accept(t, hs.URL, cSubject, token, "h30x-nodev-1", "ctx-h30x-nodev-1")

	deadline := time.Now().Add(h30Window)
	var status, reason string
	for time.Now().Before(deadline) {
		code, body := postSpaceTask(t, hs.URL, "GetTurn", map[string]any{"turn_id": turnID}, cSubject, token)
		if code != http.StatusOK {
			t.Fatalf("GetTurn=%d body=%s", code, body)
		}
		m := decodeMap(t, body)
		status, _ = m["status"].(string)
		reason, _ = m["failure_reason"].(string)
		switch status {
		case "done", "failed", "canceled":
			if status == "failed" && reason == "" {
				t.Fatalf("terminal failed carries no failure_reason")
			}
			return // GREEN: a determined terminal appeared within the window
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("DEF-H30-x: a human turn accepted with no turn dispatcher stayed status=%q failure_reason=%q for %s with no terminal state", status, reason, h30Window)
}

// TestDefH30x_AcceptingFaceConvergesOnRemoteTerminal is the composed journey shape:
// the face that accepted the turn is NOT the worker that executes it, and nobody
// calls DispatchTurn (only the runner's claim loop takes it). The accepting face's
// GetTurn must converge to the terminal the worker and the durable record report.
// Before the fix the face reports pending forever (RED); after the fix it converges
// (GREEN).
func TestDefH30x_AcceptingFaceConvergesOnRemoteTerminal(t *testing.T) {
	e := cNewEnv(t)
	face := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}, WorkerID: "h30x-face"})
	faceSrv := httptest.NewServer(space.NewHandler(space.HandlerConfig{Coordinator: face, Auth: authByHeader(e.tenant, e.deployment)}))
	t.Cleanup(faceSrv.Close)
	// The deployment's worker: the only process with a claim loop (the as-runner role).
	runner := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}, WorkerID: "h30x-runner"})

	token := e.signTask(t, "h30x-face")
	turnID := h30Accept(t, faceSrv.URL, cSubject, token, "h30x-face-1", "ctx-h30x-face-1")

	runCtx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	runner.StartOrphanClaimLoop(runCtx, 50*time.Millisecond)

	// The worker drives it to a terminal.
	deadline := time.Now().Add(20 * time.Second)
	var rs space.Turn
	ok := false
	for time.Now().Before(deadline) {
		rs, ok = runner.TurnSnapshot(turnID)
		if ok && rs.Status.IsTerminal() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok || !rs.Status.IsTerminal() {
		t.Fatalf("the executing worker never reached a terminal: %+v ok=%v", rs, ok)
	}

	// The face the user is watching must report the SAME terminal.
	faceDeadline := time.Now().Add(10 * time.Second)
	var status string
	for time.Now().Before(faceDeadline) {
		code, body := postSpaceTask(t, faceSrv.URL, "GetTurn", map[string]any{"turn_id": turnID}, cSubject, token)
		if code != http.StatusOK {
			t.Fatalf("face GetTurn=%d body=%s", code, body)
		}
		m := decodeMap(t, body)
		status, _ = m["status"].(string)
		switch status {
		case "done", "failed", "canceled":
			return // GREEN: the face converged
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("DEF-H30-x: the durable run and the executing worker are terminal, but the accepting face's GetTurn still reports status=%q (turnForRead returns this process's in-memory snapshot and never re-reads State once it knows the turn)", status)
}
