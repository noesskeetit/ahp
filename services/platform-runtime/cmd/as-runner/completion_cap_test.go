package main

// DEF-DOTE2E-9: the runner must ask the Model Gateway for what the gateway will
// accept.
//
// buildProfileRuntime used to leave MaxTokens unset, so internal/core filled in
// its own private default of 4096 while the gateway's cap (AHP_EXEC_MAX_TOKENS,
// default 1024) refused it. On a live Dot turn that arrived as
// "model stream refused: HTTP 400" -- the number that broke the turn was chosen
// by a default two packages away from the bound it violated. These legs pin that
// the runner states its completion cap explicitly, from the same source the
// gateway reads, and that a deliberate conflict is a startup failure instead of
// a failed first turn.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
)

// capRecordingModel records the completion bound each step asked for.
type capRecordingModel struct {
	asked []int
}

func (m *capRecordingModel) Generate(_ context.Context, req *core.ModelRequest) (*core.ModelResponse, error) {
	m.asked = append(m.asked, req.MaxTokens)
	return &core.ModelResponse{Content: json.RawMessage(`"done"`)}, nil
}

func runOneStepTurn(t *testing.T, runner space.CoreRunner) {
	t.Helper()
	results := make(chan space.StepResult, 16)
	req := &space.CoreRequest{RunID: "cap-run", Input: json.RawMessage(`"go"`), Checkpoint: &space.Checkpoint{}}
	if err := runner.Run(context.Background(), req, results); err != nil {
		t.Fatalf("the runner could not complete a one-step turn: %v", err)
	}
	for len(results) > 0 {
		<-results
	}
}

// Leg A: the runner's request carries the gateway's cap, not core's default.
func TestRunnerAsksTheGatewaysCompletionCap(t *testing.T) {
	t.Setenv("AHP_EXEC_MAX_TOKENS", "256")
	model := &capRecordingModel{}

	runner, _, err := buildProfileRuntime(config{}, core.RuntimeConfig{Model: model})
	if err != nil {
		t.Fatalf("build profile runtime: %v", err)
	}
	runOneStepTurn(t, runner)

	if len(model.asked) == 0 {
		t.Fatal("the turn never reached a model call")
	}
	for _, asked := range model.asked {
		if asked != 256 {
			t.Fatalf("the runner asked for max_tokens=%d under a gateway cap of 256; "+
				"4096 means internal/core's implicit default is in charge (DEF-DOTE2E-9)", asked)
		}
	}
}

// Leg B: an unset operator value still lands on the gateway default, not on
// core's private 4096 -- this is the exact stand configuration that failed.
func TestRunnerWithoutOperatorValueUsesTheGatewayDefault(t *testing.T) {
	t.Setenv("AHP_EXEC_MAX_TOKENS", "")
	model := &capRecordingModel{}

	runner, _, err := buildProfileRuntime(config{}, core.RuntimeConfig{Model: model})
	if err != nil {
		t.Fatalf("build profile runtime: %v", err)
	}
	runOneStepTurn(t, runner)

	for _, asked := range model.asked {
		if asked != 1024 {
			t.Fatalf("with AHP_EXEC_MAX_TOKENS unset the runner asked for %d, want the gateway default 1024 (got %v)",
				asked, model.asked)
		}
	}
}

// Leg C: a stated runner cap above the gateway's is a startup failure with a
// reason. The mismatch must never survive to the first turn.
func TestRunnerFailsFastWhenItsCapExceedsTheGateways(t *testing.T) {
	t.Setenv("AHP_EXEC_MAX_TOKENS", "1024")
	t.Setenv("AS_MODEL_MAX_TOKENS", "4096")

	_, _, err := buildProfileRuntime(config{}, core.RuntimeConfig{Model: &capRecordingModel{}})
	if err == nil {
		t.Fatal("a runner cap of 4096 above a gateway cap of 1024 started anyway: the first Dot turn pays for it")
	}
	if !strings.Contains(err.Error(), "completion cap") {
		t.Fatalf("startup failure must name the completion cap conflict, got: %v", err)
	}
}

// Leg D: a runner cap at or under the gateway's is accepted, and a non-positive
// stated value is a configuration error rather than a silent default.
func TestRunnerCapAtOrBelowTheGatewayStarts(t *testing.T) {
	t.Setenv("AHP_EXEC_MAX_TOKENS", "1024")
	t.Setenv("AS_MODEL_MAX_TOKENS", "512")
	runner, _, err := buildProfileRuntime(config{}, core.RuntimeConfig{Model: &capRecordingModel{}})
	if err != nil {
		t.Fatalf("a runner cap under the gateway's must start: %v", err)
	}
	model := &capRecordingModel{}
	lower, _, err := buildProfileRuntime(config{}, core.RuntimeConfig{Model: model})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	runOneStepTurn(t, lower)
	if model.asked[0] != 512 {
		t.Fatalf("the operator's stated runner cap was ignored: asked %d", model.asked[0])
	}
	_ = runner

	t.Setenv("AS_MODEL_MAX_TOKENS", "0")
	if _, _, err := buildProfileRuntime(config{}, core.RuntimeConfig{Model: &capRecordingModel{}}); err == nil {
		t.Fatal("AS_MODEL_MAX_TOKENS=0 started: a stated zero is a configuration error, not a default")
	}
}
