package core

import (
	"context"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// silentModel returns a completion with no tool calls, exercising a tool-free loop.
type silentModel struct{ calls int }

func (m *silentModel) Generate(ctx context.Context, req *ModelRequest) (*ModelResponse, error) {
	m.calls++
	return &ModelResponse{Content: []byte(`"done"`)}, nil
}

// TestRuntimeNilToolsDoesNotPanic guards the latent nil-pointer dereference where
// Run() assembled the model request via cfg.Tools.Defs() before checking for tools.
// A model-only Runtime (Tools == nil) is valid and must complete, not crash.
func TestRuntimeNilToolsDoesNotPanic(t *testing.T) {
	rt := NewRuntime(RuntimeConfig{Model: &silentModel{}})
	results := make(chan space.StepResult, 16)
	done := make(chan error, 1)
	go func() {
		err := rt.Run(context.Background(), &space.CoreRequest{Input: []byte(`"go"`)}, results)
		close(results)
		done <- err
	}()

	var sawDone bool
	for res := range results {
		if res.StepType == space.StepDone {
			sawDone = true
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("Run with nil Tools returned error: %v", err)
	}
	if !sawDone {
		t.Fatal("expected a StepDone result for a tool-free completion")
	}
}
