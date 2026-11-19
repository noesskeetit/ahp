package space_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
)

// loopingToolModel always returns a tool call, so the Core loop never reaches a
// natural final answer on its own. This deterministically forces the loop to
// terminate only via the step limit or context cancellation — exactly the two
// contract behaviours under test.
type loopingToolModel struct{ n atomic.Int64 }

func (m *loopingToolModel) Generate(_ context.Context, _ *core.ModelRequest) (*core.ModelResponse, error) {
	i := m.n.Add(1)
	return &core.ModelResponse{
		Content:   json.RawMessage(fmt.Sprintf(`"step-%d"`, i)),
		ToolCalls: []core.ToolCall{{ID: fmt.Sprintf("tc-%d", i), Name: "echo", Args: json.RawMessage(`{"x":1}`)}},
	}, nil
}

func newLoopingRuntime() *core.Runtime {
	reg := core.NewToolRegistry(&echoExecutor{})
	reg.Register(core.ToolDef{Name: "echo", Description: "echoes"})
	return core.NewRuntime(core.RuntimeConfig{Model: &loopingToolModel{}, Tools: reg})
}

func drain(ch chan space.StepResult) []space.StepResult {
	var got []space.StepResult
	for {
		select {
		case r := <-ch:
			got = append(got, r)
		default:
			return got
		}
	}
}

// TestCoreRuntimeCancellation proves contract item 2 "cancellation" at the Core
// model/tool loop level: cancelling the run context mid-loop makes Runtime.Run
// stop and return context.Canceled (not run to the step cap). The turn-level
// CancelTurn and durable crash/resume fencing tests cover the outer layers.
func TestCoreRuntimeCancellation(t *testing.T) {
	rt := newLoopingRuntime()
	results := make(chan space.StepResult, 4096)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		req := &space.CoreRequest{TurnID: "turn-cancel", Input: json.RawMessage(`"go"`), MaxSteps: 1000}
		runErr <- rt.Run(ctx, req, results)
	}()

	// Drive until the model has produced tool work, then cancel mid-loop.
	deadline := time.After(5 * time.Second)
	tools := 0
cancelled:
	for {
		select {
		case r := <-results:
			if r.StepType == space.StepTool {
				tools++
				if tools >= 2 {
					cancel()
					break cancelled
				}
			}
		case <-deadline:
			t.Fatal("cancellation test timed out before observing 2 tool steps")
		}
	}

	select {
	case err := <-runErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run must return context.Canceled on cancel, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if tools < 2 {
		t.Fatalf("expected >=2 tool steps before cancel, saw %d", tools)
	}
}

// TestCoreRuntimeBoundedSteps proves contract item 2 "bounded steps": with a
// model that never finalises, the loop stops at MaxSteps and Run returns a step
// limit error with a terminal StepError/Done, rather than spinning forever.
func TestCoreRuntimeBoundedSteps(t *testing.T) {
	rt := newLoopingRuntime()
	results := make(chan space.StepResult, 4096)
	req := &space.CoreRequest{TurnID: "turn-bounded", Input: json.RawMessage(`"go"`), MaxSteps: 3}

	done := make(chan error, 1)
	go func() { done <- rt.Run(context.Background(), req, results) }()

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("bounded-steps Run did not terminate at MaxSteps")
	}
	if err == nil {
		t.Fatal("expected step-limit error, got nil")
	}
	got := drain(results)
	var terminal bool
	for _, s := range got {
		if s.StepType == space.StepError && s.Done {
			terminal = true
		}
	}
	if !terminal {
		t.Fatalf("expected terminal StepError/Done on step limit; steps=%d", len(got))
	}
}
