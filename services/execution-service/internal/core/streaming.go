package core

import (
	"context"
	"fmt"

	"github.com/ahp-platform/ahp/internal/space"
	aot "github.com/ahp-platform/ahp/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ModelDelta carries one real provider fragment, not a slice of a finished reply.
type ModelDelta struct {
	GenerationID string
	Sequence     int64
	Text         string
}

// StreamingModelClient is optional. Legacy/custom models keep Generate semantics.
type StreamingModelClient interface {
	GenerateStream(context.Context, *ModelRequest, func(ModelDelta) error) (*ModelResponse, error)
}

func (r *Runtime) generate(ctx context.Context, req *ModelRequest, run *space.CoreRequest, rec *stepRecorder, results chan<- space.StepResult) (*ModelResponse, error) {
	ctx, span := aot.Tracer("ahp.core").Start(ctx, "core.model", trace.WithAttributes(attribute.String("ahp.component", "core"), attribute.String("ahp.operation", "model")))
	defer span.End()
	if run.JournalModel != nil {
		rec.intentKey = space.ModelIntentKey(run.RunID, req.Step, req.Attempt)
		if err := run.JournalModel(ctx, req.Step, req.Attempt, jsonBytes(req)); err != nil {
			return nil, fmt.Errorf("model intent not durable: %w", err)
		}
	}
	if streaming, ok := r.cfg.Model.(StreamingModelClient); ok {
		wireRequest := *req
		if replay := run.ModelReplay; replay != nil && req.Step == replay.Step && req.Attempt == replay.Attempt && replay.DeltaSequence > 0 {
			wireRequest.ResumeGenerationID, wireRequest.ResumeAfter = replay.GenerationID, replay.DeltaSequence
		}
		lastSequence := wireRequest.ResumeAfter
		response, err := streaming.GenerateStream(ctx, &wireRequest, func(d ModelDelta) error {
			if d.GenerationID == "" || d.Sequence <= 0 || d.Text == "" {
				return fmt.Errorf("model returned invalid incremental fragment")
			}
			if wireRequest.ResumeGenerationID != "" {
				if d.GenerationID != wireRequest.ResumeGenerationID {
					return fmt.Errorf("recovered model generation changed")
				}
				if d.Sequence <= wireRequest.ResumeAfter {
					return nil
				}
				if d.Sequence != lastSequence+1 {
					return fmt.Errorf("recovered model stream cursor gap")
				}
				lastSequence = d.Sequence
			}
			sr := space.StepResult{ModelIntentKey: rec.intentKey, StepType: space.StepModelDelta, Content: jsonBytes(d.Text), GenerationID: d.GenerationID, DeltaSequence: d.Sequence}
			if rec != nil && rec.requireAck {
				// Coordinator is the authoritative State writer for a composed run.
				// Keep recorder recovery alignment, but do not make the same State
				// append through the standalone sink before publishing this delta.
				if rec.sink != nil && !rec.seeded && !rec.blocked {
					rec.seed(ctx)
				}
				sr.Persisted = make(chan error, 1)
			} else {
				rec.record(ctx, sr)
			}
			select {
			case results <- sr:
			case <-ctx.Done():
				return ctx.Err()
			}
			if sr.Persisted != nil {
				select {
				case err, ok := <-sr.Persisted:
					if !ok {
						return fmt.Errorf("model delta persistence acknowledgement closed")
					}
					if err != nil {
						return fmt.Errorf("model delta persistence: %w", err)
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
		if err == nil && wireRequest.ResumeGenerationID != "" && (response == nil || response.GenerationID != wireRequest.ResumeGenerationID) {
			return nil, fmt.Errorf("recovered model terminal generation changed")
		}
		return response, err
	}
	return r.cfg.Model.Generate(ctx, req)
}

func (r *Runtime) chargeRecoveredStep(ctx context.Context, req *space.CoreRequest, step int, alreadyCharged bool) error {
	if alreadyCharged {
		return nil
	}
	return r.chargeStep(ctx, req, step)
}
