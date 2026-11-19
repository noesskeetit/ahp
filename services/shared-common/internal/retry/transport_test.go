package retry

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// realRefused produces the error class this package exists for, from the real
// stack (a connection to a closed port) rather than from a hand-written string.
// A classifier tested only against invented messages is a guess.
func realRefused(t *testing.T) error {
	t.Helper()
	_, err := (&http.Client{Timeout: 2 * time.Second}).Post("http://127.0.0.1:1/x", "application/json", strings.NewReader("{}"))
	if err == nil {
		t.Fatal("expected a refused connection")
	}
	return err
}

func TestNetworkClassifiesRealTransportFailures(t *testing.T) {
	t.Parallel()
	if !Network(realRefused(t)) {
		t.Fatal("a refused connection must classify as transport")
	}
	if !Network(io.ErrUnexpectedEOF) {
		t.Fatal("an unexpected EOF mid-body is transport: no response was completed")
	}
	if !Network(&net.DNSError{Err: "i/o timeout", IsTimeout: true}) {
		t.Fatal("a DNS timeout is transport")
	}
	for _, notTransport := range []error{
		nil,
		errors.New("provider returned 400: prompt refused"),
		context.Canceled,
		context.DeadlineExceeded,
	} {
		if Network(notTransport) {
			t.Fatalf("%v must NOT classify as transport", notTransport)
		}
	}
}

func TestDoRepeatsOnlyTransportFailures(t *testing.T) {
	transport := realRefused(t)
	var calls int
	refusal := errors.New("provider returned 400: prompt refused")
	p := Policy{Attempts: 3, Base: time.Millisecond, Max: 2 * time.Millisecond,
		Sleep: func(context.Context, time.Duration) bool { return true }}

	got, err := Do(context.Background(), p, Network, func() (string, error) {
		calls++
		if calls < 3 {
			return "", transport
		}
		return "answer", nil
	})
	if err != nil || got != "answer" {
		t.Fatalf("a hiccup must be absorbed inside one provider call: got %q err %v", got, err)
	}
	if calls != 3 {
		t.Fatalf("attempts = %d, want 3", calls)
	}

	calls = 0
	if _, err := Do(context.Background(), p, Network, func() (string, error) {
		calls++
		return "", refusal
	}); !errors.Is(err, refusal) {
		t.Fatalf("a refusal was altered: %v", err)
	}
	if calls != 1 {
		t.Fatalf("a refusal must be attempted exactly once, got %d", calls)
	}
}

// "The network gave up" must stay readable as that, and must stay inspectable as
// the underlying failure: an operator deciding between node health and a model
// refusal needs both facts.
func TestDoExhaustionNamesItself(t *testing.T) {
	transport := realRefused(t)
	p := Policy{Attempts: 2, Base: time.Millisecond, Max: time.Millisecond,
		Sleep: func(context.Context, time.Duration) bool { return true }}
	_, err := Do(context.Background(), p, Network, func() (string, error) { return "", transport })
	if err == nil {
		t.Fatal("expected an exhaustion error")
	}
	if !strings.Contains(err.Error(), "transport retries exhausted") {
		t.Fatalf("exhaustion must be its own reason: %v", err)
	}
	if !errors.Is(err, transport) {
		t.Fatalf("the original failure must stay wrapped: %v", err)
	}
}

// Retrying past the caller's deadline would leak work beyond the task's lease.
func TestDoStopsWhenContextEnds(t *testing.T) {
	transport := realRefused(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	p := Policy{Attempts: 5, Base: time.Millisecond, Max: time.Millisecond,
		Sleep: func(context.Context, time.Duration) bool { cancel(); return true }}
	if _, err := Do(ctx, p, Network, func() (string, error) {
		calls++
		return "", transport
	}); err == nil {
		t.Fatal("expected failure")
	}
	if calls != 2 {
		t.Fatalf("attempts = %d, want 2 (stop as soon as the context ended)", calls)
	}
}

// One attempt means no wrapper and no behavior change: the retry must be
// switch-off-able without recompiling, and the env ceiling must hold.
func TestPolicyFromEnvIsBoundedAndOptOut(t *testing.T) {
	t.Setenv(EnvAttempts, "1")
	if FromEnv().Enabled() {
		t.Fatal("attempts=1 must disable the retry")
	}
	t.Setenv(EnvAttempts, "999")
	p := FromEnv()
	if p.Attempts > MaxAttempts {
		t.Fatalf("attempts=%d escaped the ceiling %d", p.Attempts, MaxAttempts)
	}
	t.Setenv(EnvAttempts, "not-a-number")
	if p := FromEnv(); !p.Enabled() || p.Attempts != DefaultAttempts {
		t.Fatalf("an unparsable env must fall back to the default policy, got %+v", p)
	}
	t.Setenv(EnvBaseMS, "-5")
	if p := FromEnv(); p.Base <= 0 {
		t.Fatalf("a negative backoff is not a policy: %+v", p)
	}
}

func TestDelayGrowsAndStaysCapped(t *testing.T) {
	t.Parallel()
	p := Policy{Attempts: 8, Base: 100 * time.Millisecond, Max: 400 * time.Millisecond}
	for attempt, want := range map[int]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 400 * time.Millisecond, 9: 400 * time.Millisecond} {
		if got := p.Delay(attempt); got != want {
			t.Fatalf("Delay(%d) = %s, want %s", attempt, got, want)
		}
	}
}
