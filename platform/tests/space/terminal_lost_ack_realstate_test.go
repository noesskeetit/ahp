//go:build state_integration

package space_test

// The real-plane half of the lost-ACK / transient-failure property.
//
// The unit pair (terminal_lost_ack_test.go) proves the writer repeats. This file proves
// the repeat is SAFE where it actually counts: on the real State over PostgreSQL, whose
// (key, kind, payload) digest is what turns "send the same terminal frame again" into
// "read back the one stored event" instead of a second terminal frame. A flaky proxy
// sits between the Run Coordinator and the real State and kills exactly the first
// terminal (kind "status") response AFTER the write commits. Durable truth is
// then read straight from real State, bypassing the proxy.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// flakyStateProxy forwards to real State using the workload certificate, then
// closes the downstream connection after the selected successful AppendStream.
// The durable write has committed but the coordinator never receives its ACK.
func flakyStateProxy(t *testing.T, e *cEnv, kind string, failures int32) (*httptest.Server, *int32) {
	t.Helper()
	var dropped int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("proxy read: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		dropResponse := false
		if strings.HasSuffix(r.URL.Path, "/AppendStream") && atomic.LoadInt32(&dropped) < failures {
			var req struct {
				Kind string `json:"kind"`
			}
			dropResponse = json.Unmarshal(body, &req) == nil && req.Kind == kind
		}
		fwd, err := http.NewRequestWithContext(r.Context(), http.MethodPost, e.srv.URL+r.URL.Path, bytes.NewReader(body))
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		fwd.Header.Set("content-type", "application/json")
		if auth := r.Header.Get("Authorization"); auth != "" {
			fwd.Header.Set("Authorization", auth)
		}
		resp, err := e.srv.Client().Do(fwd)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		if dropResponse && resp.StatusCode == http.StatusOK {
			atomic.AddInt32(&dropped, 1)
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("proxy cannot hijack to drop committed response")
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Errorf("proxy hijack: %v", err)
				return
			}
			_ = conn.Close() // COMMIT succeeded upstream; ACK is lost downstream.
			return
		}
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(out)
	}))
	t.Cleanup(proxy.Close)
	return proxy, &dropped
}

// durableStatusFrames reads the durable thread stream from the REAL State.
func durableStatusFrames(t *testing.T, e *cEnv, token, threadID string) []string {
	t.Helper()
	events, err := e.client.ReadStream(e.ctx, token, threadID, 0, 200)
	if err != nil {
		t.Fatalf("ReadStream over real State: %v", err)
	}
	var out []string
	for _, ev := range events {
		if ev.Kind != "status" {
			continue
		}
		var v struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(ev.Payload, &v); err != nil {
			t.Fatalf("durable status frame %s carries %s: %v", ev.Key, ev.Payload, err)
		}
		out = append(out, v.Status)
	}
	return out
}

func lostackRealScope(e *cEnv) contracts.Scope {
	return contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
}

// TestTerminalFrameSurvivesTransientFailure_RealState: a completed turn whose terminal
// append commits but its ACK is lost must still end with exactly ONE durable terminal
// frame - the coordinator's repeat has to land on the real fence and the real digest.
func TestTerminalFrameSurvivesTransientFailure_RealState(t *testing.T) {
	e := cNewEnv(t)
	proxy, dropped := flakyStateProxy(t, e, "status", 1)

	tok := e.signTask(t, "task-"+contracts.NewID())
	scope := lostackRealScope(e)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(proxy.URL, nil),
		Core:        &claimableCore{},
		WorkerID:    "w-lostack-real",
	})
	acc, err := coord.AcceptTurn(e.ctx, tok, scope, space.AcceptTurnRequest{
		Key:       "k-lostack-real-" + contracts.NewID(),
		ContextID: "ctx-lostack-real-" + contracts.NewID(),
		Input:     json.RawMessage(`{"role":"user","content":"go"}`),
		MaxSteps:  2,
	})
	if err != nil {
		t.Fatalf("AcceptTurn over real State: %v", err)
	}
	if err := coord.DispatchTurn(e.ctx, tok, scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	var statuses []string
	var released bool
	for time.Now().Before(deadline) {
		statuses = durableStatusFrames(t, e, tok, acc.ThreadID)
		runs, err := e.client.ListRuns(e.ctx, tok, acc.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		released = len(runs) == 1 && runs[0].Status == "done"
		if len(statuses) > 0 && released {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if n := atomic.LoadInt32(dropped); n != 1 {
		t.Fatalf("the proxy dropped %d committed terminal responses, want 1 - the probe did not exercise the failure", n)
	}
	if !released {
		t.Fatal("terminal response retry did not complete run release")
	}
	if len(statuses) != 1 || statuses[0] != "done" {
		t.Fatalf("after one lost terminal ACK the real durable stream carries %v, want exactly [done]", statuses)
	}
}

// TestCanceledThreadSurvivesTransientFailure_RealState: same property on the cancel
// path. After the revoke nobody can write the terminal frame, so the cancel itself must
// get it durable - once, and provably not twice.
func TestCanceledThreadSurvivesTransientFailure_RealState(t *testing.T) {
	e := cNewEnv(t)
	proxy, dropped := flakyStateProxy(t, e, "status", 1)

	tok := e.signTask(t, "task-"+contracts.NewID())
	scope := lostackRealScope(e)
	core := newSilentCore()
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(proxy.URL, nil),
		Core:        core,
		WorkerID:    "w-cancelack-real",
	})
	acc, err := coord.AcceptTurn(e.ctx, tok, scope, space.AcceptTurnRequest{
		Key:       "k-cancelack-real-" + contracts.NewID(),
		ContextID: "ctx-cancelack-real-" + contracts.NewID(),
		Input:     json.RawMessage(`{"role":"user","content":"go"}`),
		MaxSteps:  2,
	})
	if err != nil {
		t.Fatalf("AcceptTurn over real State: %v", err)
	}
	if err := coord.DispatchTurn(e.ctx, tok, scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	select {
	case <-core.started:
	case <-time.After(15 * time.Second):
		t.Fatal("core never started on the real plane - nothing to cancel")
	}
	if err := coord.CancelTurn(e.ctx, acc.TurnID, tok); err != nil {
		t.Fatalf("CancelTurn must complete despite a lost terminal append: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	var statuses []string
	for time.Now().Before(deadline) {
		statuses = durableStatusFrames(t, e, tok, acc.ThreadID)
		if len(statuses) > 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if n := atomic.LoadInt32(dropped); n != 1 {
		t.Fatalf("the proxy dropped %d committed terminal responses, want 1 - the probe did not exercise the failure", n)
	}
	if len(statuses) != 1 || statuses[0] != "canceled" {
		t.Fatalf("after one lost cancel append the real durable stream carries %v, want exactly [canceled]", statuses)
	}
	runs, err := e.client.ListRuns(e.ctx, tok, acc.ThreadID)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 || runs[0].Status != "canceled" {
		t.Fatalf("real run state after cancel = %+v, want one canceled run", runs)
	}
}
