package execbridge

import (
	"context"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/retry"
)

// Where the transport retry sits, and why it cannot sit anywhere else.
//
// A model step that dies on "TLS handshake timeout" is a node/provider health
// fact, not a model fact, and on a saturated node it turns an acceptance leg into
// a lottery. The obvious place to retry -- this package's call into the Model
// Gateway -- is the one place it must NOT happen: by the time
// model.Service.Execute has returned, the reservation is settled and Execute
// replays a terminal generation instead of re-dispatching it
// (internal/model/service.go), so a retry there is either a no-op or a second
// charge. Retrying inside ONE Stream() call is the opposite: one generation row,
// one effect key, one reservation, one settle -- the ledger never learns the
// provider was asked twice, so the task's budget cannot be spent twice.
//
// The policy itself is shared with the direct executor (internal/retry): the
// operator sets one retry budget per process, not one per wiring.
type retryingInference struct {
	inner  model.Inference
	policy retry.Policy
}

// newRetryingInference wraps a provider handle with the bounded transport retry.
// A disabled policy returns the handle unchanged: no wrapper, no behavior change,
// so the retry is opt-out-able without recompiling.
func newRetryingInference(inner model.Inference) model.Inference {
	p := retry.FromEnv()
	if inner == nil || !p.Enabled() {
		return inner
	}
	return &retryingInference{inner: inner, policy: p}
}

func (r *retryingInference) Model() string { return r.inner.Model() }

// Stream issues the provider call, repeating it only while the failure is
// transport-class as the Model Gateway itself defines it (the request may have
// left the process without an answer). A refusal, a protocol error or a canceled
// context is returned untouched.
func (r *retryingInference) Stream(ctx context.Context, req model.ChatRequest) (model.Stream, error) {
	return retry.Do(ctx, r.policy, model.IsTransportError, func() (model.Stream, error) {
		return r.inner.Stream(ctx, req)
	})
}
