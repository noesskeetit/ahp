package core

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// blankOnceModel returns a configured number of blank completions first, then
// a normal text completion. It models the measured provider hiccup: an empty
// 200 with no tool calls on the first call after a cold rehydrate.
type blankOnceModel struct {
	blanks int
	calls  int
}

func (m *blankOnceModel) Generate(ctx context.Context, req *ModelRequest) (*ModelResponse, error) {
	m.calls++
	if m.calls <= m.blanks {
		return &ModelResponse{Content: json.RawMessage(`""`)}, nil
	}
	return &ModelResponse{Content: json.RawMessage(`"final answer"`)}, nil
}

// TestRuntimeBlankTurnIsNotCompletion pins the PDF-facing rule that a run
// cannot reach `done` on a blank model turn: persistent blanks must produce a
// durable error step and a non-nil error, never StepDone.
func TestRuntimeBlankTurnIsNotCompletion(t *testing.T) {
	rt := NewRuntime(RuntimeConfig{Model: &blankOnceModel{blanks: 99}})
	results := make(chan space.StepResult, 16)
	done := make(chan error, 1)
	go func() {
		err := rt.Run(context.Background(), &space.CoreRequest{Input: json.RawMessage(`"go"`)}, results)
		close(results)
		done <- err
	}()

	var sawDone, sawError bool
	for res := range results {
		switch res.StepType {
		case space.StepDone:
			sawDone = true
		case space.StepError:
			sawError = true
		}
	}
	if err := <-done; err == nil {
		t.Fatal("persistent blank turns must fail the run, not complete it")
	}
	if sawDone {
		t.Fatal("a blank turn must never emit StepDone")
	}
	if !sawError {
		t.Fatal("a persistent blank turn must emit a durable error step")
	}
}

// TestRuntimeBlankTurnRetriesOnce proves the transient case: a single blank
// 200 is retried inside the step budget and the run completes normally.
func TestRuntimeBlankTurnRetriesOnce(t *testing.T) {
	model := &blankOnceModel{blanks: 1}
	rt := NewRuntime(RuntimeConfig{Model: model})
	results := make(chan space.StepResult, 16)
	done := make(chan error, 1)
	go func() {
		err := rt.Run(context.Background(), &space.CoreRequest{Input: json.RawMessage(`"go"`)}, results)
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
		t.Fatalf("one blank turn must be retried, got: %v", err)
	}
	if !sawDone {
		t.Fatal("after a successful retry the run must complete")
	}
	if model.calls != 2 {
		t.Fatalf("want exactly one retry (2 calls), got %d", model.calls)
	}
}

// blankAttemptModel answers like blankOnceModel but records the attempt
// number of every call it received, so a test can prove what a ledger-
// fenced caller needs: each physical attempt arrives with its own identity.
type blankAttemptModel struct {
	blanks   int
	attempts []int
}

func (m *blankAttemptModel) Generate(ctx context.Context, req *ModelRequest) (*ModelResponse, error) {
	m.attempts = append(m.attempts, req.Attempt)
	if len(m.attempts) <= m.blanks {
		return &ModelResponse{Content: json.RawMessage(`""`)}, nil
	}
	return &ModelResponse{Content: json.RawMessage(`"final answer"`)}, nil
}

// TestRuntimeBlankTurnRetryCarriesOwnAttempt is the regression test for the
// sealed-effect defect: the retry used to re-send the identical request, so a
// gateway that fences a model call by effect key answered the retry with the
// sealed conflict of the blank attempt and the retry could never succeed. The
// first call of a step keeps attempt 0 (so already-persisted effects stay
// replayable), and each retry is numbered.
func TestRuntimeBlankTurnRetryCarriesOwnAttempt(t *testing.T) {
	model := &blankAttemptModel{blanks: 1}
	rt := NewRuntime(RuntimeConfig{Model: model})
	results := make(chan space.StepResult, 16)
	done := make(chan error, 1)
	go func() {
		err := rt.Run(context.Background(), &space.CoreRequest{Input: json.RawMessage(`"go"`)}, results)
		close(results)
		done <- err
	}()
	for range results {
	}
	if err := <-done; err != nil {
		t.Fatalf("one blank turn must be retried, got: %v", err)
	}
	if len(model.attempts) != 2 {
		t.Fatalf("want 2 model calls, got %d (%v)", len(model.attempts), model.attempts)
	}
	if model.attempts[0] != 0 {
		t.Fatalf("first call must carry attempt 0, got %d", model.attempts[0])
	}
	if model.attempts[1] == model.attempts[0] {
		t.Fatalf("the retry must not reuse the identity of the attempt it retries: both carried %d", model.attempts[0])
	}
}

// truncatedModel returns one content-only completion carrying finish_reason
// "length" (the token cap cut the answer), then a normal completion.
type truncatedModel struct{ calls int }

func (m *truncatedModel) Generate(ctx context.Context, req *ModelRequest) (*ModelResponse, error) {
	m.calls++
	if m.calls == 1 {
		return &ModelResponse{Content: json.RawMessage(`"part of a long report"`), Stop: "length"}, nil
	}
	return &ModelResponse{Content: json.RawMessage(`"finished report"`)}, nil
}

// TestRuntimeTruncatedCompletionIsNotDone pins that finish_reason "length"
// continues the bounded loop instead of emitting StepDone.
func TestRuntimeTruncatedCompletionIsNotDone(t *testing.T) {
	model := &truncatedModel{}
	rt := NewRuntime(RuntimeConfig{Model: model})
	results := make(chan space.StepResult, 16)
	done := make(chan error, 1)
	go func() {
		err := rt.Run(context.Background(), &space.CoreRequest{Input: json.RawMessage(`"go"`)}, results)
		close(results)
		done <- err
	}()

	var modelSteps int
	var sawDone bool
	for res := range results {
		switch res.StepType {
		case space.StepModel:
			modelSteps++
		case space.StepDone:
			sawDone = true
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("a truncated completion must continue the loop, got: %v", err)
	}
	if model.calls != 2 {
		t.Fatalf("want a continuation call after finish_reason=length, got %d calls", model.calls)
	}
	if modelSteps < 2 || !sawDone {
		t.Fatal("the turn must emit both model steps and only then complete")
	}
}

// lengthBlankModel always answers with a blank completion cut by the output cap
// (finish_reason "length"), the shape a reasoning model produces when it spends
// the whole budget thinking and emits nothing. It records calls so a test can
// prove no futile retry is issued.
type lengthBlankModel struct{ calls int }

func (m *lengthBlankModel) Generate(ctx context.Context, req *ModelRequest) (*ModelResponse, error) {
	m.calls++
	return &ModelResponse{Content: json.RawMessage(`""`), Stop: "length", Usage: &Usage{Output: 1024}}, nil
}

// TestRuntimeBlankWithLengthIsBudgetExhaustion pins the D17 split: a blank turn
// whose finish_reason is "length" is the output budget running dry, not a provider
// hiccup, so it must fail with its own class, must not be retried at the same cap,
// and must never complete the run.
func TestRuntimeBlankWithLengthIsBudgetExhaustion(t *testing.T) {
	model := &lengthBlankModel{}
	rt := NewRuntime(RuntimeConfig{Model: model})
	results := make(chan space.StepResult, 16)
	done := make(chan error, 1)
	go func() {
		err := rt.Run(context.Background(), &space.CoreRequest{Input: json.RawMessage(`"go"`)}, results)
		close(results)
		done <- err
	}()
	var sawDone, sawError bool
	for res := range results {
		switch res.StepType {
		case space.StepDone:
			sawDone = true
		case space.StepError:
			sawError = true
		}
	}
	err := <-done
	if !errors.Is(err, ErrOutputBudgetExhausted) {
		t.Fatalf("a blank finish_reason=length turn must be ErrOutputBudgetExhausted, got: %v", err)
	}
	if errors.Is(err, ErrBlankTurn) {
		t.Fatalf("budget exhaustion must not be reported as the blank-turn class: %v", err)
	}
	if model.calls != 1 {
		t.Fatalf("a budget exhaustion must not be retried at the same cap, got %d calls", model.calls)
	}
	if sawDone {
		t.Fatal("a budget exhaustion must never emit StepDone")
	}
	if !sawError {
		t.Fatal("a budget exhaustion must emit a durable error step")
	}
}

// blankThenLengthModel answers once with a plain blank (a hiccup), then on the
// retry with a blank cut by the cap - proving the retry path also reclassifies
// finish_reason=length instead of looping or reporting a persistent blank.
type blankThenLengthModel struct{ calls int }

func (m *blankThenLengthModel) Generate(ctx context.Context, req *ModelRequest) (*ModelResponse, error) {
	m.calls++
	if m.calls == 1 {
		return &ModelResponse{Content: json.RawMessage(`""`)}, nil
	}
	return &ModelResponse{Content: json.RawMessage(`""`), Stop: "length"}, nil
}

// TestRuntimeBlankRetryThatComesBackTruncatedIsBudgetExhaustion proves the retry
// of a hiccup that returns finish_reason=length is reported once as budget
// exhaustion, not spun further or mislabeled a persistent blank.
func TestRuntimeBlankRetryThatComesBackTruncatedIsBudgetExhaustion(t *testing.T) {
	model := &blankThenLengthModel{}
	rt := NewRuntime(RuntimeConfig{Model: model})
	results := make(chan space.StepResult, 16)
	done := make(chan error, 1)
	go func() {
		err := rt.Run(context.Background(), &space.CoreRequest{Input: json.RawMessage(`"go"`)}, results)
		close(results)
		done <- err
	}()
	for range results {
	}
	err := <-done
	if !errors.Is(err, ErrOutputBudgetExhausted) {
		t.Fatalf("the retried blank that came back truncated must be ErrOutputBudgetExhausted, got: %v", err)
	}
	if model.calls != 2 {
		t.Fatalf("want one hiccup call then one retry, got %d", model.calls)
	}
}
