package core

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
)

type manyDeltasModel struct{}

func (manyDeltasModel) Generate(context.Context, *ModelRequest) (*ModelResponse, error) {
	return nil, fmt.Errorf("stream capability lost")
}
func (manyDeltasModel) GenerateStream(ctx context.Context, req *ModelRequest, emit func(ModelDelta) error) (*ModelResponse, error) {
	for i := 1; i <= 80; i++ {
		if err := emit(ModelDelta{GenerationID: "generation", Sequence: int64(i), Text: "x"}); err != nil {
			return nil, err
		}
	}
	return &ModelResponse{GenerationID: "generation", Content: json.RawMessage(`"final"`)}, nil
}
func TestStreamingBackpressureAndSeparateDurableKeys(t *testing.T) {
	sink := space.NewMemDurabilitySink()
	runtime := NewRuntime(RuntimeConfig{Model: manyDeltasModel{}, Durable: sink})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results := make(chan space.StepResult, 1)
	done := make(chan error, 1)
	go func() {
		done <- runtime.Run(ctx, &space.CoreRequest{RunID: "run", Input: json.RawMessage(`"go"`)}, results)
		close(results)
	}()
	n := 0
	for r := range results {
		n++
		if n <= 80 && (r.StepType != space.StepModelDelta || r.DeltaSequence != int64(n) || r.Done) {
			t.Fatalf("bad delta %+v", r)
		}
		time.Sleep(time.Millisecond)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n != 82 {
		t.Fatalf("dropped events=%d", n)
	}
	for _, key := range []string{space.ModelDeltaKey("run", "generation", 80), space.StepKey("run", 0), space.StepKey("run", 1)} {
		if _, _, ok := sink.Get(key); !ok {
			t.Fatalf("missing canonical key %s", key)
		}
	}
}

type truncatedStreamModel struct{}

func (truncatedStreamModel) Generate(context.Context, *ModelRequest) (*ModelResponse, error) {
	return &ModelResponse{Content: json.RawMessage(`"unfinished"`), Stop: "length"}, nil
}
func TestTruncatedLastModelStepCannotReportDone(t *testing.T) {
	rt := NewRuntime(RuntimeConfig{Model: truncatedStreamModel{}})
	out := make(chan space.StepResult, 8)
	err := rt.Run(context.Background(), &space.CoreRequest{Input: json.RawMessage(`"go"`), MaxSteps: 1}, out)
	if err == nil {
		t.Fatal("truncated answer reported successful completion")
	}
	close(out)
	for frame := range out {
		if frame.StepType == space.StepDone {
			t.Fatal("fake terminal done")
		}
	}
}

type twoStepModel struct{}

func (twoStepModel) Generate(_ context.Context, req *ModelRequest) (*ModelResponse, error) {
	if req.Step == 0 {
		return &ModelResponse{Content: json.RawMessage(`"first"`), Stop: "length"}, nil
	}
	return &ModelResponse{Content: json.RawMessage(`"final"`), Stop: "stop"}, nil
}
func TestNextModelIntentWaitsForDurablePreviousResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	intents := make(chan int, 4)
	out := make(chan space.StepResult, 8)
	done := make(chan error, 1)
	rt := NewRuntime(RuntimeConfig{Model: twoStepModel{}})
	go func() {
		done <- rt.Run(ctx, &space.CoreRequest{RunID: "run", Input: json.RawMessage(`"go"`), MaxSteps: 2, JournalModel: func(_ context.Context, step, attempt int, _ json.RawMessage) error { intents <- step; return nil }}, out)
		close(out)
	}()
	if step := <-intents; step != 0 {
		t.Fatal(step)
	}
	first := <-out
	if first.StepType != space.StepModel || first.ModelIntentKey != space.ModelIntentKey("run", 0, 0) || first.Persisted == nil {
		t.Fatal("model result lacks durable intent acknowledgement")
	}
	select {
	case <-intents:
		t.Fatal("next inference overtook uncommitted assistant")
	case <-time.After(80 * time.Millisecond):
	}
	first.Persisted <- nil
	for frame := range out {
		if frame.Persisted != nil {
			frame.Persisted <- nil
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if step := <-intents; step != 1 {
		t.Fatal(step)
	}
}

func TestResumedModelUsesRemainingLogicalStepBudget(t *testing.T) {
	rt := NewRuntime(RuntimeConfig{Model: truncatedStreamModel{}})
	out := make(chan space.StepResult, 8)
	original, _ := json.Marshal(ModelRequest{Step: 1, Messages: []ChatMessage{{Role: "user", Content: json.RawMessage(`"go"`)}}})
	err := rt.Run(context.Background(), &space.CoreRequest{MaxSteps: 2, ModelReplay: &space.ModelReplay{Step: 1, Request: original}}, out)
	if err == nil {
		t.Fatal("cold restart reset total model budget")
	}
	close(out)
	n := 0
	for frame := range out {
		if frame.StepType == space.StepModel {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("remaining budget allowed %d model steps", n)
	}
}
