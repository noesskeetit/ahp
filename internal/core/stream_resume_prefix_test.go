package core

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

func TestRecoveredGenerationDoesNotReappendConfirmedFragments(t *testing.T) {
	var replay space.ModelReplay
	if err := json.Unmarshal([]byte(`{"Step":0,"Attempt":0,"Request":{"Step":0},"GenerationID":"generation","DeltaSequence":2}`), &replay); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{Model: ackTestStream{}})
	rec := newStepRecorder(nil, "run")
	results := make(chan space.StepResult, 4)
	run := &space.CoreRequest{RunID: "run", ModelReplay: &replay, JournalModel: func(context.Context, int, int, json.RawMessage) error { return nil }}
	_, err := runtime.generate(context.Background(), &ModelRequest{Step: 0}, run, rec, results)
	if err != nil {
		t.Fatal(err)
	}
	close(results)
	var seq []int64
	for event := range results {
		seq = append(seq, event.DeltaSequence)
	}
	if len(seq) != 1 || seq[0] != 3 {
		t.Fatalf("recovery reappended already confirmed fragments: %v", seq)
	}
}
