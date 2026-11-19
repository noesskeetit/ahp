package executor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/retry"
)

// countingTransport answers with scripted errors and records what each attempt
// actually sent. The recording is the point: an http.Request carries a body
// reader that the first attempt consumes, so a naive retry POSTs an empty body
// and the provider answers something nobody asked for.
type countingTransport struct {
	errs   []error
	calls  int
	bodies []string
}

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	i := c.calls
	c.calls++
	if r.Body != nil {
		raw, _ := io.ReadAll(r.Body)
		c.bodies = append(c.bodies, string(raw))
	}
	if i < len(c.errs) && c.errs[i] != nil {
		return nil, c.errs[i]
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body: io.NopCloser(strings.NewReader(
			`{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)),
	}, nil
}

// refused is the real shape of "the connection never came up": a *net.OpError,
// which is what retry.Network is written to recognise.
func refused() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
}

func retryingQwen(transport *countingTransport, attempts int) *qwenModel {
	return &qwenModel{
		completionsURL: "https://model.invalid/v1/chat/completions",
		apiKey:         "k",
		model:          "qwen-test",
		http:           &http.Client{Transport: transport},
		retry: retry.Policy{Attempts: attempts, Base: time.Millisecond, Max: 2 * time.Millisecond,
			Sleep: func(context.Context, time.Duration) bool { return true }},
	}
}

func ping() *core.ModelRequest {
	return &core.ModelRequest{Messages: []core.ChatMessage{
		{Role: "user", Content: json.RawMessage(`"ping"`)},
	}}
}

func TestQwenRetriesTransportFailureWithAFreshBody(t *testing.T) {
	tr := &countingTransport{errs: []error{refused(), refused()}}
	q := retryingQwen(tr, 3)

	res, err := q.Generate(context.Background(), ping())
	if err != nil {
		t.Fatalf("a transport hiccup must be absorbed inside one call: %v", err)
	}
	if tr.calls != 3 {
		t.Fatalf("provider attempts = %d, want 3", tr.calls)
	}
	if len(tr.bodies) != 3 || tr.bodies[0] == "" {
		t.Fatalf("every attempt must carry the prompt body, got %d bodies", len(tr.bodies))
	}
	if tr.bodies[1] != tr.bodies[0] || tr.bodies[2] != tr.bodies[0] {
		t.Fatal("a retry must send the SAME body as the first attempt -- a spent reader would send an empty POST")
	}
	if len(res.Content) == 0 {
		t.Fatal("no answer parsed from the successful attempt")
	}
}

// A status the server actually returned is an answer, not a network fact:
// repeating it spends provider work and hides the operator's real signal.
func TestQwenDoesNotRetryAnAnswer(t *testing.T) {
	tr := &countingTransport{errs: []error{errors.New("executor: qwen status 400: prompt refused")}}
	q := retryingQwen(tr, 4)
	if _, err := q.Generate(context.Background(), ping()); err == nil {
		t.Fatal("expected the refusal to surface")
	}
	if tr.calls != 1 {
		t.Fatalf("provider attempts = %d, want exactly 1 for a non-transport failure", tr.calls)
	}
}

// The zero policy is "ask once": unit tests and any opt-out wiring keep the
// previous behaviour, and only NewQwenModel installs the operator's budget.
func TestQwenZeroPolicyAsksOnce(t *testing.T) {
	tr := &countingTransport{errs: []error{refused()}}
	q := retryingQwen(tr, 1)
	if _, err := q.Generate(context.Background(), ping()); err == nil {
		t.Fatal("expected the transport failure to surface")
	}
	if tr.calls != 1 {
		t.Fatalf("provider attempts = %d, want 1 when the retry is disabled", tr.calls)
	}
}

// The exhaustion path must stay readable as a network giving up, not as a model
// refusal: the operator's next action differs (node/provider health vs prompt).
func TestQwenExhaustionIsNamedAndWrapped(t *testing.T) {
	tr := &countingTransport{errs: []error{refused(), refused(), refused()}}
	q := retryingQwen(tr, 3)
	_, err := q.Generate(context.Background(), ping())
	if err == nil {
		t.Fatal("expected an exhaustion error")
	}
	if !strings.Contains(err.Error(), "transport retries exhausted") {
		t.Fatalf("exhaustion must be its own reason: %v", err)
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("the underlying network failure must stay inspectable: %v", err)
	}
	if tr.calls != 3 {
		t.Fatalf("provider attempts = %d, want 3", tr.calls)
	}
}
