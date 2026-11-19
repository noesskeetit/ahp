package execution_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// stubCore is a deterministic CoreRunner used only to prove the harness drives
// the REAL Space Coordinator -> real State -> durable stream end to end.
type stubCore struct{}

func (stubCore) Run(_ context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	results <- space.StepResult{StepType: space.StepModel, Content: json.RawMessage(`"planning"`)}
	results <- space.StepResult{StepType: space.StepDone, Content: json.RawMessage(`"ok"`), Done: true}
	return nil
}

func TestA2AHarnessAcceptDispatchDurableDone(t *testing.T) {
	e := newA2AEnv(t)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: stubCore{}, WorkerID: "w-smoke",
	})
	subject := "a2a-user"
	scope := e.scope(subject)
	token := e.taskToken(t, subject, "task-"+contracts.NewID())

	accept, err := coord.AcceptTurn(e.ctx, token, scope, space.AcceptTurnRequest{
		Key:       "smoke-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"hello"`),
		MaxSteps:  4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(e.ctx, token, scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	var lastStatus string
	for time.Now().Before(deadline) {
		run, err := e.client.GetRun(e.ctx, token, accept.TurnID)
		if err == nil {
			lastStatus = run.Status
			if run.Status == "done" || run.Status == "completed" {
				events, err := e.client.ReadStream(e.ctx, token, accept.ThreadID, 0, 100)
				if err != nil {
					t.Fatalf("ReadStream: %v", err)
				}
				if len(events) == 0 {
					t.Fatal("durable stream empty after a done run")
				}
				t.Logf("smoke: run %s durable status=%q stream_events=%d", accept.TurnID, run.Status, len(events))
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("run %s never reached durable terminal status (last=%q)", accept.TurnID, lastStatus)
}
