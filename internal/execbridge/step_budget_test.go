package execbridge

import (
	"errors"
	"testing"

	"github.com/ahp-platform/ahp/internal/gateway"
)

// The close reason is the only thing that distinguishes a run the model ended
// from a run the budget ended, and the task state alone does not carry it.
func TestCloseReason(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		runErr    error
		errText   string
		sawError  bool
		finalText string
		consumed  int
		budget    int
		want      string
	}{
		{"model answered inside the budget", nil, "", false, "answer", 2, 4, gateway.CloseReasonModelFinished},
		{"core reports the limit on the error", errors.New("step limit 4 reached"), "", false, "partial", 4, 4, gateway.CloseReasonStepLimit},
		{"core reports the limit as an emitted step", nil, "step limit reached", true, "partial", 4, 4, gateway.CloseReasonStepLimit},
		{"exact consumption without a limit error is still the budget", nil, "", false, "answer", 4, 4, gateway.CloseReasonStepLimit},
		{"a provider failure is neither", errors.New("model generate step 1: upstream 503"), "", true, "", 1, 4, gateway.CloseReasonError},
		{"an emitted error step is neither", nil, "tool denied", true, "partial", 2, 4, gateway.CloseReasonError},
		{"no output and no error is not a finished turn", nil, "", false, "   ", 1, 4, gateway.CloseReasonError},
		// Transport is its own fact: it says nothing about the model or the budget.
		{"tls handshake timeout is transport", errors.New("model generate step 0: openai-compat: request: Post https://model.example/v1/chat/completions: net/http: TLS handshake timeout"), "", true, "", 1, 4, gateway.CloseReasonProviderTransport},
		{"connection reset is transport", nil, "read tcp 10.0.0.5:443: read: connection reset by peer", true, "", 1, 4, gateway.CloseReasonProviderTransport},
		{"i/o timeout is transport", nil, "dial tcp 10.0.0.9:443: connect: i/o timeout", true, "", 1, 4, gateway.CloseReasonProviderTransport},
		{"a provider 503 is not transport", errors.New("model generate step 1: openai-compat: status 503"), "", true, "", 1, 4, gateway.CloseReasonError},
		// The classification reads ERRORS, never the turn's content: an answer that
		// merely mentions a timeout must not be reported as a transport close.
		{"an answer mentioning i/o timeout is not transport", nil, "", false, "the upstream reported i/o timeout", 1, 4, gateway.CloseReasonModelFinished},
	}
	for _, tc := range cases {
		got := closeReason(tc.runErr, tc.errText, tc.sawError, tc.finalText, tc.consumed, tc.budget)
		if got != tc.want {
			t.Errorf("%s: closeReason = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// The operator's ceiling is read in one place, so the gateway validates a stated
// budget against the very number the bridge will run with.
func TestMaxStepsFromEnvDefaults(t *testing.T) {
	// no t.Parallel(): t.Setenv mutates the process environment
	t.Setenv("AHP_EXEC_MAX_STEPS", "")
	if got := MaxStepsFromEnv(); got != defaultMaxSteps {
		t.Fatalf("unset AHP_EXEC_MAX_STEPS = %d, want the documented default %d", got, defaultMaxSteps)
	}
	t.Setenv("AHP_EXEC_MAX_STEPS", "12")
	if got := MaxStepsFromEnv(); got != 12 {
		t.Fatalf("AHP_EXEC_MAX_STEPS=12 = %d", got)
	}
	// A nonsense value must not fall through to 0: a zero bound would stop every
	// attempt before its first step.
	t.Setenv("AHP_EXEC_MAX_STEPS", "not-a-number")
	if got := MaxStepsFromEnv(); got <= 0 {
		t.Fatalf("invalid AHP_EXEC_MAX_STEPS produced %d, want a positive fallback", got)
	}
}
