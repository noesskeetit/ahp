// Package retry bounds repeated TRANSPORT attempts of one provider call.
//
// It exists because a saturated node turns "TLS handshake timeout" into a
// regular event, and a single unretried handshake kills a whole model step: the
// run closes as failed before its first snapshot boundary, and an acceptance leg
// becomes a lottery on node health. Retrying is only honest under two rules, and
// this package encodes both:
//
//  1. Only a failure that produced NO response at all may be repeated. The
//     caller supplies the classifier, because the class is defined by the client
//     that observed it (retry.Network for raw net/http, model.IsTransportError
//     for the Model Gateway's inference client). A refusal, an HTTP status, a
//     protocol error or a finished context is an answer, and answers are not
//     retried.
//  2. The repetition must stay INSIDE the unit that owns the effect key. Placed
//     above a generation, a retry would re-dispatch it and spend a task's budget
//     twice (internal/model/service.go settles a terminal generation and replays
//     it instead of re-running it); placed inside one provider call, it is
//     invisible to the ledger -- same generation row, same reservation, same
//     settle.
//
// Exhaustion reports itself as its own reason, distinct from a model refusal, so
// "the network gave up" can never be read as "the model answered no".
package retry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"
)

// Operator knobs. One set of names for every consumer of this package, so a
// deployment cannot end up with two different retry budgets in one process.
const (
	// EnvAttempts is the TOTAL number of provider attempts for one call,
	// including the first. 1 disables the retry entirely.
	EnvAttempts = "AHP_EXEC_MODEL_ATTEMPTS"
	// EnvBaseMS is the first backoff step in milliseconds.
	EnvBaseMS = "AHP_EXEC_MODEL_RETRY_MS"
	// EnvMaxMS caps the backoff.
	EnvMaxMS = "AHP_EXEC_MODEL_RETRY_MAX_MS"

	DefaultAttempts = 3
	DefaultBaseMS   = 500
	DefaultMaxMS    = 5000

	// MaxAttempts ceilings the env: an unbounded loop against a sick endpoint is
	// an outage amplifier, not reliability.
	MaxAttempts = 8
)

// Policy is a bounded retry policy.
type Policy struct {
	Attempts int
	Base     time.Duration
	Max      time.Duration
	// Sleep waits out one backoff step and reports false when the context ended
	// first. Tests inject it so backoff is exercised without burning wall time;
	// nil means the real timer (Sleep).
	Sleep func(ctx context.Context, d time.Duration) bool
}

// Enabled reports whether the policy would repeat anything. One attempt means
// callers should not wrap at all -- no wrapper, no behaviour change.
func (p Policy) Enabled() bool { return p.Attempts > 1 }

// FromEnv reads the policy. Zero/negative or unparsable values fall back to the
// defaults, and the attempt count is capped, so a typo cannot turn one model step
// into an unbounded loop.
func FromEnv() Policy {
	return Policy{
		Attempts: attemptsEnv(EnvAttempts, DefaultAttempts),
		Base:     durationEnv(EnvBaseMS, DefaultBaseMS),
		Max:      durationEnv(EnvMaxMS, DefaultMaxMS),
	}
}

func attemptsEnv(key string, def int) int {
	n := intEnv(key, def)
	if n < 1 {
		return def
	}
	if n > MaxAttempts {
		return MaxAttempts
	}
	return n
}

func durationEnv(key string, defMS int) time.Duration {
	ms := intEnv(key, defMS)
	if ms < 1 {
		ms = defMS
	}
	d := time.Duration(ms) * time.Millisecond
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return d
}

func intEnv(key string, def int) int {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

// Delay is the backoff before the attempt following `attempt` (1-based):
// exponential from Base, capped at Max.
func (p Policy) Delay(attempt int) time.Duration {
	d := p.Base
	for i := 1; i < attempt && d < p.Max; i++ {
		d *= 2
	}
	if d > p.Max {
		return p.Max
	}
	return d
}

// Sleep is the default wait; it reports false when the context ends first.
func Sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Do runs op, repeating it only while the failure is transport-class, the policy
// has attempts left and the caller's context is alive. The returned error on
// exhaustion names the exhaustion itself and still wraps the last transport
// failure, so a caller can classify either.
func Do[T any](ctx context.Context, p Policy, transport func(error) bool, op func() (T, error)) (T, error) {
	var zero T
	if !p.Enabled() || transport == nil {
		return op()
	}
	wait := p.Sleep
	if wait == nil {
		wait = Sleep
	}
	var last error
	for attempt := 1; attempt <= p.Attempts; attempt++ {
		got, err := op()
		if err == nil {
			return got, nil
		}
		if !transport(err) || ctx.Err() != nil {
			return zero, err
		}
		last = err
		if attempt == p.Attempts {
			break
		}
		if !wait(ctx, p.Delay(attempt)) {
			return zero, fmt.Errorf("retry: transport retry aborted after %d attempt(s): %w", attempt, err)
		}
	}
	return zero, fmt.Errorf("retry: transport retries exhausted after %d attempts: %w", p.Attempts, last)
}

// Network classifies the failures of a raw net/http call that produced no
// response: the request may or may not have reached the peer, which is exactly
// the class a bounded repeat is for. Anything else -- a status the server
// actually returned, a decode failure, a canceled context -- is not transport.
func Network(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	text := errors.Unwrap(err)
	for text != nil {
		if _, ok := text.(*net.OpError); ok {
			return true
		}
		text = errors.Unwrap(text)
	}
	return false
}
