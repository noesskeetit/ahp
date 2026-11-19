//go:build state_integration

package space_test

// Task 1 of the SDK lane, measured against the REAL runtime: a real State server
// over real PostgreSQL, reached over real HTTPS with a real client certificate
// (cNewEnv). Nothing here is a double: every refusal and every durable status is
// read back from the same store the server wrote.
//
// What is proved:
//   - a run's task identity is what the STATE recorded from the presenting
//     credential, and the SDK refuses to carry a belief about it that the ledger
//     does not share;
//   - an App claim cannot execute one task's payload under another task's run and
//     fence: the mismatch is refused BEFORE any lease is taken, so the durable run
//     stays untouched;
//   - the same holds for a payload the caller believes but the run does not carry;
//   - a bound claim never reaches past the thread's FIFO head, so a named but
//     non-head run is left for its own worker instead of being stolen.
//
// Boundary of this file: the rejected-closing-Release case (epoch moved, Release
// answers 409 STALE_EPOCH) is proved deterministically in pkg/sdk against a store
// double -- TestExecutorDurableCloseFailureIsNotSuccess -- and at App level in
// tests/sdk (TestAppRejectedReleaseIsNotCompleted); the real State's rejection of
// a stale-epoch write is proved against PostgreSQL by seam_realstate_test.go. The
// State SDK deliberately requires an *http.Transport configured for mTLS and offers
// no transport seam to inject a lost heartbeat into a live lease, so this file does
// not claim to re-measure that one combination end to end.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/sdk"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

// h10TaskClient returns a State client whose credential NAMES the given task.
// That is not a test convenience: the real State binds a run's task identity to
// the verified credential (internal/stateapi/routes.go createRun), so creating a
// run for a task without that task's credential is already a refusal. The same
// shape is what internal/agentworkload does on a real agent front.
func h10TaskClient(t *testing.T, e *cEnv, task string) *sdkstate.Client {
	t.Helper()
	tok := e.signTask(t, task)
	client, err := sdkstate.New(sdkstate.Config{
		BaseURL:     e.srv.URL,
		HTTPClient:  e.srv.Client(),
		TokenSource: func(context.Context) (string, error) { return tok, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestBoundAppRefusesForeignTaskAndPayloadRealState(t *testing.T) {
	e := cNewEnv(t)
	clientA := h10TaskClient(t, e, "taskA")
	clientB := h10TaskClient(t, e, "taskB")

	thread, err := clientA.CreateThread(e.ctx, "ctx-bound", "bound")
	if err != nil {
		t.Fatal(err)
	}
	runA, err := clientA.CreateRun(e.ctx, thread.ID, "task:taskA", json.RawMessage(`{"text":"A"}`))
	if err != nil {
		t.Fatal(err)
	}
	if runA.TaskID != "taskA" {
		t.Fatalf("State recorded task %q for a run created with taskA's credential", runA.TaskID)
	}
	runB, err := clientB.CreateRun(e.ctx, thread.ID, "task:taskB", json.RawMessage(`{"text":"B"}`))
	if err != nil {
		t.Fatal(err)
	}

	var executed []string
	app, err := sdk.NewApp(&sdk.Config{StateClient: clientA, Worker: "w-bound", LeaseTTL: 5 * time.Second, HeartbeatInterval: 500 * time.Millisecond},
		func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
			executed = append(executed, rc.Task.TaskID+":"+string(rc.Task.Payload))
			return json.RawMessage(`{"ok":true}`), nil
		})
	if err != nil {
		t.Fatal(err)
	}

	// (1) right run, WRONG task: refused, and no lease was taken.
	if _, err := app.RunOnceFor(e.ctx, sdk.Task{RunID: runA.ID, TaskID: "taskB", ContextID: "ctx-bound", Payload: json.RawMessage(`{"text":"A"}`)}); err == nil {
		t.Error("RunOnceFor accepted a run belonging to another task")
	} else {
		t.Logf("foreign-task refusal: %v", err)
	}
	// (2) right run+task, payload the caller invented: refused.
	if _, err := app.RunOnceFor(e.ctx, sdk.Task{RunID: runA.ID, TaskID: "taskA", ContextID: "ctx-bound", Payload: json.RawMessage(`{"text":"NEW-UNDER-OLD-RUN"}`)}); err == nil {
		t.Error("RunOnceFor accepted a payload the durable run does not carry")
	} else {
		t.Logf("payload-mismatch refusal: %v", err)
	}
	// (3) a named run that is NOT this thread's head belongs to another worker's
	// turn: nothing is executed, and nothing is written.
	nonHead, err := app.RunOnceFor(e.ctx, sdk.Task{RunID: runB.ID, TaskID: "taskB", ContextID: "ctx-bound", Payload: runB.Payload})
	if err != nil {
		t.Fatalf("non-head claim must be an honest no-op, got: %v", err)
	}
	if nonHead != nil {
		t.Errorf("claimed past the FIFO head: %+v", nonHead)
	}
	for _, probe := range []struct {
		name   string
		client *sdkstate.Client
		id     string
	}{{"A", clientA, runA.ID}, {"B", clientB, runB.ID}} {
		stored, err := probe.client.GetRun(e.ctx, probe.id)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Status != sdkstate.RunPending {
			t.Errorf("refused attempts moved %s to %q: a refusal must take no lease and write nothing", probe.name, stored.Status)
		}
	}
	if len(executed) != 0 {
		t.Errorf("graph ran on refused attempts: %v", executed)
	}

	// (4) the honest call executes exactly taskA, with the payload the run carries.
	res, err := app.RunOnceFor(e.ctx, sdk.Task{RunID: runA.ID, TaskID: "taskA", ContextID: "ctx-bound", Payload: runA.Payload})
	if err != nil {
		t.Fatalf("bound execution: %v", err)
	}
	if res == nil || res.Status != sdkstate.RunDone {
		t.Fatalf("bound execution result: %+v", res)
	}
	if len(executed) != 1 || executed[0] != `taskA:{"text":"A"}` {
		t.Errorf("graph saw %v, want exactly taskA with its own payload", executed)
	}
	if stored, err := clientA.GetRun(e.ctx, runA.ID); err != nil || stored.Status != sdkstate.RunDone {
		t.Errorf("taskA durable status=%q err=%v, want done", stored.Status, err)
	}
	if stored, err := clientB.GetRun(e.ctx, runB.ID); err != nil || stored.Status != sdkstate.RunPending {
		t.Errorf("taskB durable status=%q err=%v, want untouched pending", stored.Status, err)
	}

	// (5) with the head closed, taskB becomes claimable -- by its own identity.
	appB, err := sdk.NewApp(&sdk.Config{StateClient: clientB, Worker: "w-bound-b", LeaseTTL: 5 * time.Second, HeartbeatInterval: 500 * time.Millisecond},
		func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
			executed = append(executed, rc.Task.TaskID+":"+string(rc.Task.Payload))
			return json.RawMessage(`{"ok":true}`), nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if res, err := appB.RunOnceFor(e.ctx, sdk.Task{RunID: runB.ID, TaskID: "taskB", ContextID: "ctx-bound", Payload: runB.Payload}); err != nil || res == nil || res.Status != sdkstate.RunDone {
		t.Fatalf("taskB after the head closed: %+v %v", res, err)
	}
	if len(executed) != 2 || executed[1] != `taskB:{"text":"B"}` {
		t.Errorf("want taskB executed second with its own payload, got %v", executed)
	}
}
