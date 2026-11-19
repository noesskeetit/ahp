package core

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// emptyPayloadModel models a provider that answers with an EMPTY structured
// payload and no tool calls (e.g. content:{} or content:[]) every time. There is
// no text and no tool, so the turn is functionally empty and must not flip the
// run to `done`.
type emptyPayloadModel struct {
	payload string
	calls   int
}

func (m *emptyPayloadModel) Generate(ctx context.Context, req *ModelRequest) (*ModelResponse, error) {
	m.calls++
	return &ModelResponse{Content: json.RawMessage(m.payload)}, nil
}

// TestRuntimeEmptyJSONObjectIsNotCompletion pins the PDF-facing rule that an
// empty model response (no text, no tool) yields a durable StepError, never a
// silent StepDone — regardless of whether the empty completion arrives as an
// empty string, empty object, or empty array.
func TestRuntimeEmptyJSONObjectIsNotCompletion(t *testing.T) {
	rt := NewRuntime(RuntimeConfig{Model: &emptyPayloadModel{payload: `{}`}})
	results := make(chan space.StepResult, 16)
	done := make(chan error, 1)
	go func() {
		err := rt.Run(context.Background(), &space.CoreRequest{Input: json.RawMessage(`"go"`)}, results)
		close(results)
		done <- err
	}()
	var sawDone, sawErr bool
	for r := range results {
		switch r.StepType {
		case space.StepDone:
			sawDone = true
		case space.StepError:
			sawErr = true
		}
	}
	if err := <-done; err == nil {
		t.Fatal("an empty {} model response must fail the run, not complete it")
	}
	if sawDone {
		t.Fatal("an empty {} response must never emit StepDone")
	}
	if !sawErr {
		t.Fatal("an empty {} response must emit a durable error step")
	}
}

func TestRuntimeEmptyJSONArrayIsNotCompletion(t *testing.T) {
	rt := NewRuntime(RuntimeConfig{Model: &emptyPayloadModel{payload: `[]`}})
	results := make(chan space.StepResult, 16)
	done := make(chan error, 1)
	go func() {
		err := rt.Run(context.Background(), &space.CoreRequest{Input: json.RawMessage(`"go"`)}, results)
		close(results)
		done <- err
	}()
	var sawDone, sawErr bool
	for r := range results {
		switch r.StepType {
		case space.StepDone:
			sawDone = true
		case space.StepError:
			sawErr = true
		}
	}
	if err := <-done; err == nil {
		t.Fatal("an empty [] model response must fail the run, not complete it")
	}
	if sawDone {
		t.Fatal("an empty [] response must never emit StepDone")
	}
	if !sawErr {
		t.Fatal("an empty [] response must emit a durable error step")
	}
}
