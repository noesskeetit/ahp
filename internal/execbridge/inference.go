package execbridge

import (
	"context"
	"errors"
	"fmt"

	"github.com/ahp-platform/ahp/internal/model"
)

// ErrProviderUnconfigured states why a bridged turn cannot run: the Model
// Gateway has no provider runtime secret. It is returned verbatim to the caller
// and journalled as the generation error, so the evidence says "no credential"
// instead of pretending a turn happened.
var ErrProviderUnconfigured = errors.New("execbridge: model provider not configured (no runtime secret): refusing to fabricate a turn")

// UnconfiguredInference is the honest placeholder model.Inference the bridge
// installs when no provider runtime secret exists.
//
// It is deliberately NOT a transport error: model.IsTransportError reports
// false for it, so the Model Gateway classifies the generation as a committed
// deterministic failure (failCommitted), settles the budget reservation to zero
// and records GenFailed. A task drained through it reaches a terminal FAILED
// state with a real reason, and no token is charged for a call that never left
// the process. Nothing is ever canned: Stream returns an error, never a Stream.
type UnconfiguredInference struct{}

// Model names the placeholder so generations stay auditable.
func (UnconfiguredInference) Model() string { return "unconfigured" }

// Stream always fails. Returning a non-nil Stream here would be fabrication.
func (UnconfiguredInference) Stream(context.Context, model.ChatRequest) (model.Stream, error) {
	return nil, fmt.Errorf("%w", ErrProviderUnconfigured)
}
