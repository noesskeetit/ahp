package core

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
)

// Counting at the durability port detects repeated State roundtrips even when
// the real in-memory reference store deduplicates their canonical rows.
type deltaAppendCounter struct {
	mu       sync.Mutex
	store    *space.MemDurabilitySink
	attempts int
}

func newDeltaAppendCounter() *deltaAppendCounter {
	return &deltaAppendCounter{store: space.NewMemDurabilitySink()}
}

func (s *deltaAppendCounter) AppendDurable(ctx context.Context, key, kind, fingerprint string, payload json.RawMessage) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	return s.store.AppendDurable(ctx, key, kind, fingerprint, payload)
}

type ackTestStream struct{}

func (ackTestStream) Generate(context.Context, *ModelRequest) (*ModelResponse, error) {
	return nil, errors.New("streaming capability required")
}

func (ackTestStream) GenerateStream(_ context.Context, _ *ModelRequest, emit func(ModelDelta) error) (*ModelResponse, error) {
	for i, text := range []string{"a", "b", "c"} {
		if err := emit(ModelDelta{GenerationID: "generation", Sequence: int64(i + 1), Text: text}); err != nil {
			return nil, err
		}
	}
	return &ModelResponse{GenerationID: "generation", Content: json.RawMessage(`"abc"`)}, nil
}

func startACKTestGeneration(ctx context.Context, sink *deltaAppendCounter, coordinated bool) (<-chan space.StepResult, <-chan error) {
	runtime := NewRuntime(RuntimeConfig{Model: ackTestStream{}, Durable: sink})
	rec := newStepRecorder(sink, "run")
	rec.requireAck = coordinated
	run := &space.CoreRequest{RunID: "run"}
	if coordinated {
		run.JournalModel = func(context.Context, int, int, json.RawMessage) error { return nil }
	}
	results := make(chan space.StepResult, 1)
	done := make(chan error, 1)
	go func() {
		_, err := runtime.generate(ctx, &ModelRequest{Step: 1}, run, rec, results)
		close(results)
		done <- err
	}()
	return results, done
}

func appendAsCoordinator(ctx context.Context, sink *deltaAppendCounter, sr space.StepResult) error {
	payload, err := json.Marshal(sr)
	if err != nil {
		return err
	}
	fingerprint, err := space.StepFingerprint(string(sr.StepType), payload)
	if err != nil {
		return err
	}
	_, err = sink.AppendDurable(ctx, space.ModelDeltaKey("run", sr.GenerationID, sr.DeltaSequence), string(sr.StepType), fingerprint, payload)
	return err
}

func TestCoordinatedStreamingWritesEachDeltaOnlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sink := newDeltaAppendCounter()
	results, done := startACKTestGeneration(ctx, sink, true)
	count := 0
	for sr := range results {
		count++
		err := appendAsCoordinator(ctx, sink, sr)
		if sr.Persisted != nil {
			sr.Persisted <- err
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if count != 3 || sink.attempts != 3 || sink.store.Len() != 3 {
		t.Fatalf("3 provider deltas must cause 3 authoritative appends, not duplicate roundtrips: emitted=%d attempts=%d rows=%d", count, sink.attempts, sink.store.Len())
	}
	for i, key := range []string{"run:delta:generation:1", "run:delta:generation:2", "run:delta:generation:3"} {
		_, payload, ok := sink.store.Get(key)
		if !ok {
			t.Fatalf("missing canonical delta key %s", key)
		}
		var sr space.StepResult
		if err := json.Unmarshal(payload, &sr); err != nil {
			t.Fatal(err)
		}
		if sr.ModelIntentKey != "run:model-request:1:0" || sr.StepType != space.StepModelDelta || sr.Persisted != nil || sr.DeltaSequence != int64(i+1) || string(sr.Content) != []string{`"a"`, `"b"`, `"c"`}[i] {
			t.Fatalf("canonical persisted payload or wire-only ACK changed: %+v", sr)
		}
	}
}

func TestCoordinatedStreamingPropagatesAuthoritativeACKFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sink := newDeltaAppendCounter()
	results, done := startACKTestGeneration(ctx, sink, true)
	want := errors.New("authoritative state fence refused")
	count := 0
	for sr := range results {
		count++
		if sr.Persisted != nil {
			sr.Persisted <- want
		}
	}
	if err := <-done; !errors.Is(err, want) {
		t.Fatalf("State ACK failure must stop the real model stream, got %v", err)
	}
	if count != 1 || sink.attempts != 0 || sink.store.Len() != 0 {
		t.Fatalf("failed State ACK must not advance or leave a direct-sink write: emitted=%d attempts=%d rows=%d", count, sink.attempts, sink.store.Len())
	}
}

func TestCoordinatedStreamingClosedACKCannotMeanPersisted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results, done := startACKTestGeneration(ctx, newDeltaAppendCounter(), true)
	count := 0
	for sr := range results {
		count++
		if sr.Persisted != nil {
			close(sr.Persisted)
		}
	}
	if err := <-done; err == nil || count != 1 {
		t.Fatalf("closed unacknowledged persistence channel must fail closed: emitted=%d err=%v", count, err)
	}
}

func TestCoordinatedStreamingCancellationWhileWaitingForACK(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sink := newDeltaAppendCounter()
	results, done := startACKTestGeneration(ctx, sink, true)
	first := <-results
	if first.Persisted == nil {
		cancel()
		<-done
		t.Fatal("coordinated delta has no authoritative persistence ACK")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting for persistence must honor cancellation: %v", err)
	}
	if sink.attempts != 0 {
		t.Fatalf("cancelled coordinated delta must not use the standalone sink: attempts=%d", sink.attempts)
	}
}

func TestStandaloneStreamingStillPersistsOriginalDeltaKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sink := newDeltaAppendCounter()
	results, done := startACKTestGeneration(ctx, sink, false)
	for sr := range results {
		if sr.Persisted != nil {
			t.Fatal("standalone Core must not wait for a Coordinator")
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if sink.attempts != 3 || sink.store.Len() != 3 {
		t.Fatalf("standalone durable sink lost delta writes: attempts=%d rows=%d", sink.attempts, sink.store.Len())
	}
	_, payload, ok := sink.store.Get("run:delta:generation:3")
	if !ok {
		t.Fatal("standalone canonical generation/sequence key lost")
	}
	var last space.StepResult
	if err := json.Unmarshal(payload, &last); err != nil || string(last.Content) != `"c"` || last.DeltaSequence != 3 {
		t.Fatalf("standalone original content/sequence lost: %s err=%v", payload, err)
	}
}
