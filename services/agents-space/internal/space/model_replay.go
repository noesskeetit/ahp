package space

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ModelReplay is the immutable request accepted before dispatch and the already
// durable consequences of that request. It is execution metadata, not UI state.
type ModelReplay struct {
	Step          int
	Attempt       int
	Request       json.RawMessage
	Completed     bool
	Tools         []StepResult
	Done          bool
	GenerationID  string
	DeltaSequence int64
}
type modelIntent struct {
	Step    int             `json:"step"`
	Attempt int             `json:"attempt"`
	Request json.RawMessage `json:"request"`
}

func ModelIntentKey(runID string, step, attempt int) string {
	return runID + ":model-request:" + strconv.Itoa(step) + ":" + strconv.Itoa(attempt)
}

func (c *Coordinator) modelReplay(ctx context.Context, token, threadID, runID string) (*ModelReplay, error) {
	var replay *ModelReplay
	var currentIntentKey string
	after := int64(0)
	scanned := 0
	for {
		frames, more, err := c.state.readRecoveryStreamPage(ctx, token, threadID, after, recoveryStreamPageSize, recoveryStreamPageBytes)
		if err != nil {
			return nil, err
		}
		for _, frame := range frames {
			if frame.Sequence <= after {
				return nil, contracts.NewError(contracts.ErrConflict, "model replay cursor did not advance")
			}
			after = frame.Sequence
			scanned++
			if !strings.HasPrefix(frame.Key, runID+":") {
				continue
			}
			switch frame.Kind {
			case "model_intent":
				var intent modelIntent
				if err = json.Unmarshal(frame.Payload, &intent); err != nil || intent.Step < 0 || intent.Attempt < 0 || len(intent.Request) == 0 || frame.Key != ModelIntentKey(runID, intent.Step, intent.Attempt) {
					return nil, contracts.NewError(contracts.ErrConflict, "invalid durable model intent")
				}
				replay = &ModelReplay{Step: intent.Step, Attempt: intent.Attempt, Request: intent.Request}
				currentIntentKey = frame.Key
			case string(StepModelDelta):
				var delta StepResult
				if err = json.Unmarshal(frame.Payload, &delta); err != nil {
					return nil, err
				}
				if replay == nil || delta.ModelIntentKey != currentIntentKey {
					continue
				}
				if delta.GenerationID == "" || delta.DeltaSequence != replay.DeltaSequence+1 || frame.Key != ModelDeltaKey(runID, delta.GenerationID, delta.DeltaSequence) || replay.GenerationID != "" && replay.GenerationID != delta.GenerationID {
					return nil, contracts.NewError(contracts.ErrConflict, "invalid confirmed model stream prefix")
				}
				replay.GenerationID, replay.DeltaSequence = delta.GenerationID, delta.DeltaSequence
			case string(StepModel), string(StepTool), string(StepDone):
				var step StepResult
				if err = json.Unmarshal(frame.Payload, &step); err != nil {
					return nil, err
				}
				if replay == nil || step.ModelIntentKey != currentIntentKey {
					continue
				}
				switch frame.Kind {
				case string(StepModel):
					replay.Completed = true
				case string(StepTool):
					replay.Tools = append(replay.Tools, step)
				case string(StepDone):
					replay.Done = true
				}
			}
		}
		if !more {
			return replay, nil
		}
		if scanned >= 200000 {
			return nil, contracts.NewError(contracts.ErrConflict, "model replay scan exceeded bound")
		}
	}
}
