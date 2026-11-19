package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

// These isolated transport faults reproduce R1/R2; real-PG lifecycle coverage
// lives in tests/space/runtime_sdk_realstate_test.go.
//
// The double is a STORE, not a script: it records what CreateRun was given and
// answers later operations from that record, and it names the run's task from the
// presented credential (here the bearer token is the task's own token). A double
// that invents a payload or a task identity would let the executor pass tests the
// real State would fail.
func executorFixture(t *testing.T, alter func(http.ResponseWriter, *http.Request) bool) (*Config, *atomic.Int32) {
	t.Helper()
	beats := new(atomic.Int32)
	var mu sync.Mutex
	stored := json.RawMessage(`{}`)
	remember := func(raw json.RawMessage) {
		mu.Lock()
		defer mu.Unlock()
		if len(raw) > 0 {
			stored = raw
		}
	}
	recorded := func() json.RawMessage {
		mu.Lock()
		defer mu.Unlock()
		return stored
	}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if alter != nil && alter(w, r) {
			return
		}
		switch strings.TrimPrefix(r.URL.Path, "/v1/state/") {
		case "CreateThread":
			fmt.Fprint(w, `{"id":"thread-new","context_id":"context-new","scope":{"tenant":"native-unit","deployment":"agent","subject":"owner"}}`)
		case "CreateRun":
			var body struct {
				ThreadID string          `json:"thread_id"`
				Key      string          `json:"key"`
				Payload  json.RawMessage `json:"payload"`
				TaskID   string          `json:"task_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			presented := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if body.TaskID != "" && body.TaskID != presented {
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprint(w, `{"code":"FORBIDDEN","message":"task_id does not match the presenting task token"}`)
				return
			}
			remember(body.Payload)
			fmt.Fprintf(w, `{"id":"run-new","thread_id":"thread-new","task_id":%q,"status":"pending","payload":%s}`, presented, string(recorded()))
		case "ClaimRun":
			fmt.Fprintf(w, `{"lease":{"thread":{"id":"thread-new","context_id":"context-new"},"run":{"id":"run-new","task_id":"task-new","thread_id":"thread-new","payload":%s},"worker":"test","epoch":2,"expires_at":%q},"task_token":"claim-token"}`, string(recorded()), time.Now().Add(time.Second).Format(time.RFC3339Nano))
		case "Heartbeat":
			beats.Add(1)
			fmt.Fprintf(w, `{"expires_at":%q}`, time.Now().Add(time.Second).Format(time.RFC3339Nano))
		case "Release", "Suspend":
			fmt.Fprint(w, `{"ok":true}`)
		default:
			t.Errorf("unexpected operation %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)
	// The credential names the task it may act on, exactly as a real agent
	// workload's task token does.
	client, err := sdkstate.New(sdkstate.Config{BaseURL: srv.URL, HTTPClient: srv.Client(), TokenSource: func(context.Context) (string, error) { return "task-new", nil }})
	if err != nil {
		t.Fatal(err)
	}
	return &Config{StateClient: client, Worker: "test", LeaseTTL: time.Second, HeartbeatInterval: 5 * time.Millisecond}, beats
}
func executeStates(t *testing.T, cfg *Config, graph GraphFunc) []string {
	t.Helper()
	e, err := NewLangGraphExecutor(cfg, graph)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ec := &a2asrv.ExecutorContext{TaskID: "task-new", ContextID: "context-new", Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("NEW"))}
	var states []string
	for ev, err := range e.Execute(ctx, ec) {
		if err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(ev)
		states = append(states, string(b))
	}
	return states
}
func containsState(events []string, state string) bool {
	return strings.Contains(strings.Join(events, "\n"), "TASK_STATE_"+state)
}
func TestExecutorRejectsForeignClaim(t *testing.T) {
	cfg, _ := executorFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/ClaimRun") {
			fmt.Fprint(w, `{"lease":{"thread":{"id":"thread-old"},"run":{"id":"run-old","thread_id":"thread-old","payload":{}},"worker":"test","epoch":2},"task_token":"old"}`)
			return true
		}
		return false
	})
	called := false
	states := executeStates(t, cfg, func(context.Context, *RunContext) (json.RawMessage, error) { called = true; return nil, nil })
	if called || containsState(states, "COMPLETED") {
		t.Fatal("foreign lease executed incoming graph")
	}
}
func TestExecutorUsesClaimPayloadTokenAndHeartbeat(t *testing.T) {
	cfg, beats := executorFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/Heartbeat") || strings.HasSuffix(r.URL.Path, "/Release") {
			if r.Header.Get("Authorization") != "Bearer claim-token" {
				t.Error("operation used bootstrap token")
			}
		}
		return false
	})
	states := executeStates(t, cfg, func(ctx context.Context, rc *RunContext) (json.RawMessage, error) {
		// The graph must see the payload the durable store recorded for this run,
		// not anything the caller could have passed alongside the lease.
		if rc.Lease == nil || string(rc.Task.Payload) != `{"text":"NEW"}` {
			t.Error("graph not bound to durable claim")
		}
		time.Sleep(25 * time.Millisecond)
		return json.RawMessage(`{}`), nil
	})
	if beats.Load() == 0 {
		t.Error("no heartbeat while graph running")
	}
	if !containsState(states, "COMPLETED") {
		t.Fatal(states)
	}
}
func TestExecutorDurableCloseFailureIsNotSuccess(t *testing.T) {
	for _, op := range []string{"Release", "Suspend"} {
		t.Run(op, func(t *testing.T) {
			cfg, _ := executorFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
				if strings.HasSuffix(r.URL.Path, "/"+op) {
					w.WriteHeader(409)
					fmt.Fprint(w, `{"code":"STALE_EPOCH","message":"lease lost"}`)
					return true
				}
				return false
			})
			states := executeStates(t, cfg, func(context.Context, *RunContext) (json.RawMessage, error) {
				if op == "Suspend" {
					return nil, &InterruptError{WaitFor: "input"}
				}
				return json.RawMessage(`{}`), nil
			})
			if containsState(states, "COMPLETED") || containsState(states, "INPUT_REQUIRED") || !containsState(states, "FAILED") {
				t.Fatalf("durable %s rejected but states=%v", op, states)
			}
		})
	}
}
func TestExecutorLeaseLossCancelsGraph(t *testing.T) {
	cfg, _ := executorFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/Heartbeat") {
			w.WriteHeader(409)
			fmt.Fprint(w, `{"code":"STALE_EPOCH","message":"lease lost"}`)
			return true
		}
		return false
	})
	states := executeStates(t, cfg, func(ctx context.Context, rc *RunContext) (json.RawMessage, error) {
		select {
		case <-ctx.Done():
			if rc.Lease == nil || !rc.Lease.IsLost() {
				t.Error("graph stopped without lease-loss evidence")
			}
			return nil, nil
		case <-time.After(200 * time.Millisecond):
			t.Error("lease loss did not cancel graph")
			return nil, nil
		}
	})
	if containsState(states, "COMPLETED") {
		t.Fatal("lease-lost graph completed")
	}
}

func collectExecutorStates(events iter.Seq2[a2a.Event, error]) []string {
	var states []string
	for ev, err := range events {
		if err != nil {
			states = append(states, err.Error())
			continue
		}
		b, _ := json.Marshal(ev)
		states = append(states, string(b))
	}
	return states
}

// The HTTP response gate isolates the cancellation acknowledgement boundary.
// Real State/PG cancellation is also exercised in tests/space.
func TestExecutorCancelWaitsForDurableRelease(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprintf("reject=%v", reject), func(t *testing.T) {
			releasing, allowRelease := make(chan struct{}), make(chan struct{})
			cfg, _ := executorFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
				if !strings.HasSuffix(r.URL.Path, "/Release") {
					return false
				}
				var req struct {
					Status string `json:"status"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				if req.Status != sdkstate.RunCanceled {
					t.Errorf("cancel released as %s", req.Status)
				}
				close(releasing)
				<-allowRelease
				if reject {
					w.WriteHeader(http.StatusConflict)
					fmt.Fprint(w, `{"code":"STALE_EPOCH","message":"rejected release"}`)
				} else {
					fmt.Fprint(w, `{"ok":true}`)
				}
				return true
			})
			started := make(chan struct{})
			executor, err := NewLangGraphExecutor(cfg, func(ctx context.Context, _ *RunContext) (json.RawMessage, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			ec := &a2asrv.ExecutorContext{TaskID: "task-new", ContextID: "context-new"}
			executed, canceled := make(chan []string, 1), make(chan []string, 1)
			go func() { executed <- collectExecutorStates(executor.Execute(ctx, ec)) }()
			<-started
			go func() { canceled <- collectExecutorStates(executor.Cancel(ctx, ec)) }()
			<-releasing
			var early []string
			select {
			case early = <-canceled:
				t.Errorf("Cancel returned before durable Release: %v", early)
			case <-time.After(20 * time.Millisecond):
			}
			close(allowRelease)
			if early == nil {
				early = <-canceled
			}
			execution := <-executed
			for _, states := range [][]string{early, execution} {
				if containsState(states, "CANCELED") == reject || containsState(states, "COMPLETED") {
					t.Errorf("durable reject=%v states=%v", reject, states)
				}
			}
		})
	}
}

func TestExecutorCancelWithoutActiveAttemptIsNotSuccess(t *testing.T) {
	cfg, _ := executorFixture(t, nil)
	executor, err := NewLangGraphExecutor(cfg, func(context.Context, *RunContext) (json.RawMessage, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	states := collectExecutorStates(executor.Cancel(context.Background(), &a2asrv.ExecutorContext{TaskID: "unknown"}))
	if containsState(states, "CANCELED") {
		t.Fatal("unknown task reported canceled")
	}
}

func TestExecutorCancelWaitIsBoundedByCaller(t *testing.T) {
	cfg, _ := executorFixture(t, nil)
	started, finish := make(chan struct{}), make(chan struct{})
	executor, err := NewLangGraphExecutor(cfg, func(context.Context, *RunContext) (json.RawMessage, error) {
		close(started)
		<-finish // Deliberately ignores cancellation until the test releases it.
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ec := &a2asrv.ExecutorContext{TaskID: "task-new", ContextID: "context-new"}
	done := make(chan []string, 1)
	go func() { done <- collectExecutorStates(executor.Execute(context.Background(), ec)) }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	states := collectExecutorStates(executor.Cancel(ctx, ec))
	close(finish)
	<-done
	if containsState(states, "CANCELED") {
		t.Fatal("uncooperative graph reported canceled before stopping")
	}
}

// The three refusals below are the hole this executor used to have: a message
// could be executed under a run that belongs to another task, or under a run that
// already carries a different payload, and the queue would happily hand back a
// lease for it. Each refusal must happen BEFORE a lease is taken, so the durable
// run of the task that is actually waiting stays untouched.
func refusingFixture(t *testing.T, responses map[string]string) (*Config, *atomic.Bool) {
	t.Helper()
	claims := new(atomic.Bool)
	cfg, _ := executorFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
		op := strings.TrimPrefix(r.URL.Path, "/v1/state/")
		if op == "ClaimRun" {
			claims.Store(true)
		}
		body, ok := responses[op]
		if !ok {
			return false
		}
		fmt.Fprint(w, body)
		return true
	})
	return cfg, claims
}

func TestExecutorRefusesRunOfAnotherTask(t *testing.T) {
	cfg, claims := refusingFixture(t, map[string]string{
		"CreateRun": `{"id":"run-x","thread_id":"thread-new","task_id":"another-task","status":"pending","payload":{"text":"NEW"}}`,
	})
	called := false
	states := executeStates(t, cfg, func(context.Context, *RunContext) (json.RawMessage, error) {
		called = true
		return nil, nil
	})
	if called {
		t.Error("graph executed although the run names another task")
	}
	if claims.Load() {
		t.Error("a refused message still claimed a lease")
	}
	if !containsState(states, "FAILED") || containsState(states, "COMPLETED") {
		t.Errorf("want FAILED for a foreign task identity, got %v", states)
	}
	if !strings.Contains(strings.Join(states, " "), "another-task") {
		t.Errorf("refusal did not name the recorded task: %v", states)
	}
}

func TestExecutorRefusesReplayWithDifferentPayload(t *testing.T) {
	// taskId is idempotent: the second send of the same task gets the run that
	// already exists, which carries the FIRST payload. Running this message's
	// intent under it is the exact defect; the run must be left alone.
	cfg, claims := refusingFixture(t, map[string]string{
		"CreateRun": `{"id":"run-old","thread_id":"thread-new","task_id":"task-new","status":"waiting","payload":{"text":"OLD"}}`,
	})
	called := false
	states := executeStates(t, cfg, func(context.Context, *RunContext) (json.RawMessage, error) {
		called = true
		return nil, nil
	})
	if called {
		t.Error("graph ran this message's intent under a run carrying another payload")
	}
	if claims.Load() {
		t.Error("a refused message still claimed a lease")
	}
	if !containsState(states, "FAILED") || containsState(states, "COMPLETED") {
		t.Errorf("want FAILED for a payload conflict, got %v", states)
	}
}

func TestExecutorRefusesClaimThatDisagreesWithTheRun(t *testing.T) {
	cfg, _ := refusingFixture(t, map[string]string{
		"ClaimRun": `{"lease":{"thread":{"id":"thread-new","context_id":"context-new"},"run":{"id":"run-new","task_id":"task-new","thread_id":"thread-new","payload":{"text":"SOMETHING-ELSE"}},"worker":"test","epoch":2,"expires_at":"2099-01-01T00:00:00Z"},"task_token":"claim-token"}`,
	})
	called := false
	states := executeStates(t, cfg, func(context.Context, *RunContext) (json.RawMessage, error) {
		called = true
		return json.RawMessage(`{}`), nil
	})
	if called {
		t.Error("graph ran a payload the created run never carried")
	}
	if containsState(states, "COMPLETED") {
		t.Error("a contradictory claim was reported as completion")
	}
	if !containsState(states, "FAILED") {
		t.Errorf("want FAILED for an inconsistent claim, got %v", states)
	}
}
