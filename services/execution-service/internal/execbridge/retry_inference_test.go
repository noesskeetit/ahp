package execbridge

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/internal/retry"
)

// A retry policy that is not tested against a REAL transport error is a guess
// about a classification. This obtains one from the production client (a refused
// connection to a closed port -- no egress, no fixture) and uses that value in
// the fakes below, so "transport-class" means what model.IsTransportError says.
func realTransportError(t *testing.T) error {
	t.Helper()
	client, err := model.NewQwenClientWithHTTP(secretstore.QwenSecret{
		APIKey: "test-key", BaseURL: "http://127.0.0.1:1", Model: "test-model",
	}, &http.Client{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	_, err = client.Stream(context.Background(), model.ChatRequest{
		Messages: []model.Message{{Role: "user", Content: "x"}},
	})
	if err == nil {
		t.Fatal("expected a transport failure against a closed port, got success")
	}
	if !model.IsTransportError(err) {
		t.Fatalf("error is not classified as transport by the product: %v", err)
	}
	return err
}

// retryStream/retryInference are deliberately separate from the package's
// fakeStream/fakeInference: those answer successfully and count calls, while
// the retry path needs a sequence -- fail N times, then succeed -- and the
// assertion is that the caller above sees exactly one model step.
type retryStream struct{}

func (retryStream) Next(context.Context) (model.Delta, error) {
	return model.Delta{}, model.ErrStreamDone
}
func (retryStream) Usage() model.Usage { return model.Usage{} }
func (retryStream) Close() error       { return nil }

type retryInference struct {
	errs  []error
	calls int
}

func (f *retryInference) Stream(_ context.Context, _ model.ChatRequest) (model.Stream, error) {
	i := f.calls
	f.calls++
	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	return retryStream{}, nil
}

func (f *retryInference) Model() string { return "fake-model" }

func newTestRetry(inner model.Inference, attempts int) *retryingInference {
	return &retryingInference{inner: inner, policy: retry.Policy{
		Attempts: attempts, Base: time.Millisecond, Max: 2 * time.Millisecond,
		Sleep: func(context.Context, time.Duration) bool { return true }}}
}

// The whole point of the retry: the SAME model step succeeds after the network
// hiccups, and the caller above (one generation, one effect key, one budget
// reservation) never sees more than one call.
func TestRetryRecoversTransportFailure(t *testing.T) {
	transport := realTransportError(t)
	inner := &retryInference{errs: []error{transport, transport}}
	stream, err := newTestRetry(inner, 3).Stream(context.Background(), model.ChatRequest{})
	if err != nil {
		t.Fatalf("retry did not recover: %v", err)
	}
	if stream == nil {
		t.Fatal("nil stream after recovery")
	}
	if inner.calls != 3 {
		t.Fatalf("provider calls = %d, want 3 (two failures + success)", inner.calls)
	}
}

// A refusal is an answer. Retrying it would spend provider work and time to
// reach the same denial, and would hide the operator's real signal.
func TestRetryNeverTouchesNonTransportFailure(t *testing.T) {
	refusal := errors.New("model: 400 bad request: prompt refused")
	inner := &retryInference{errs: []error{refusal}}
	_, err := newTestRetry(inner, 4).Stream(context.Background(), model.ChatRequest{})
	if !errors.Is(err, refusal) {
		t.Fatalf("refusal was altered: %v", err)
	}
	if inner.calls != 1 {
		t.Fatalf("provider calls = %d, want exactly 1 for a non-transport failure", inner.calls)
	}
}

// An expired context is not a network hiccup: the attempt budget belongs to the
// caller, and retrying past it would leak work beyond the task's lease.
func TestRetryStopsWhenContextEnds(t *testing.T) {
	transport := realTransportError(t)
	inner := &retryInference{errs: []error{transport, transport, transport}}
	ctx, cancel := context.WithCancel(context.Background())
	r := newTestRetry(inner, 4)
	r.policy.Sleep = func(context.Context, time.Duration) bool { cancel(); return true }
	if _, err := r.Stream(ctx, model.ChatRequest{}); err == nil {
		t.Fatal("expected failure with a canceled context")
	}
	if inner.calls != 2 {
		t.Fatalf("provider calls = %d, want 2 (stopped as soon as the context ended)", inner.calls)
	}
}

// "The network gave up" must not read as "the model refused": acceptance and the
// operator's next action depend on telling those apart.
func TestRetryExhaustionIsDistinguishable(t *testing.T) {
	transport := realTransportError(t)
	inner := &retryInference{errs: []error{transport, transport, transport}}
	_, err := newTestRetry(inner, 3).Stream(context.Background(), model.ChatRequest{})
	if err == nil {
		t.Fatal("expected exhaustion error")
	}
	if !strings.Contains(err.Error(), "transport retries exhausted") {
		t.Fatalf("exhaustion is not reported as its own reason: %v", err)
	}
	if !errors.Is(err, transport) {
		t.Fatal("the original transport failure must stay wrapped and inspectable")
	}
	if inner.calls != 3 {
		t.Fatalf("provider calls = %d, want 3 attempts", inner.calls)
	}
}

// The retry is bounded in code, not in documentation, and can be turned off
// without recompiling.
func TestRetryPolicyBoundedAndOptOut(t *testing.T) {
	inner := &retryInference{}
	t.Setenv(retry.EnvAttempts, "1")
	if got := newRetryingInference(inner); got != model.Inference(inner) {
		t.Fatal("attempts=1 must mean no wrapper at all -- the retry is opt-outable")
	}
	t.Setenv(retry.EnvAttempts, "100")
	p := retry.FromEnv()
	if p.Attempts > retry.MaxAttempts {
		t.Fatalf("attempts=%d escaped the ceiling %d", p.Attempts, retry.MaxAttempts)
	}
	if p.Base <= 0 || p.Max < p.Base {
		t.Fatalf("unbounded backoff policy: base=%s max=%s", p.Base, p.Max)
	}
	pol := retry.Policy{Base: 100 * time.Millisecond, Max: 400 * time.Millisecond}
	if got := pol.Delay(50); got != 400*time.Millisecond {
		t.Fatalf("delay escaped the ceiling: %s", got)
	}
	if got := pol.Delay(2); got != 200*time.Millisecond {
		t.Fatalf("delay should grow exponentially, got %s", got)
	}
}
