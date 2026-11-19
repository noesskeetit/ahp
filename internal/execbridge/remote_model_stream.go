package execbridge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// GenerateStream consumes real durable provider deltas. Reconnection repeats
// the same logical request with an advancing cursor, never a new generation key.
func (c *RemoteModel) GenerateStream(ctx context.Context, req *core.ModelRequest, emit func(core.ModelDelta) error) (*core.ModelResponse, error) {
	scope, token, ok := core.RunScopeFromContext(ctx)
	binding, bound := core.RunBindingFromContext(ctx)
	if !ok || token == "" || !bound || binding.TaskID == "" {
		return nil, fmt.Errorf("model gateway requires active run authority")
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	digest, err := contracts.Digest([]any{"core.stream.v1", binding.TaskID, req.Step, req.Attempt})
	if err != nil {
		return nil, err
	}
	input := map[string]any{"key": "core-" + digest, "messages": gatewayMessages(req.Messages), "tools": gatewayTools(req.Tools), "max_tokens": req.MaxTokens}
	if c.model != "" {
		input["model"] = c.model
	}
	ctx, stop := context.WithTimeout(ctx, 10*time.Minute)
	defer stop()
	id := req.ResumeGenerationID
	cursor := req.ResumeAfter
	if cursor < 0 || cursor > 0 && id == "" {
		return nil, fmt.Errorf("invalid confirmed model stream cursor")
	}
	defer func() {
		if ctx.Err() != nil && id != "" {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer cancel()
			_, _ = c.call(cleanup, token, "CancelModel", map[string]string{"id": id})
		}
	}()
	client := *c.client
	client.Timeout = 0 // lifetime is bounded above, not a shorter unary timeout
	for attempt := 0; attempt < 5; attempt++ {
		input["after"] = cursor
		data, _ := json.Marshal(input)
		request, e := http.NewRequestWithContext(ctx, http.MethodPost, c.origin+"/v1/model/StreamModel", bytes.NewReader(data))
		if e != nil {
			return nil, e
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+token)
		response, e := client.Do(request)

		if e == nil {
			if response.StatusCode != http.StatusOK {
				// The body names the refusing check; the status does not. Read it
				// before closing so the code and reason survive into the caller's
				// error and, from there, into the durable step frame.
				refused := refusalError("model stream refused", response.StatusCode, response.Body)
				response.Body.Close()
				return nil, refused
			}
			scan := bufio.NewScanner(response.Body)
			scan.Buffer(make([]byte, 4096), model.MaxStreamFrameBytes)
			for scan.Scan() {
				var frame modelStreamFrame
				if json.Unmarshal(scan.Bytes(), &frame) != nil {
					response.Body.Close()
					return nil, fmt.Errorf("invalid model stream frame")
				}
				switch frame.Type {
				case "heartbeat":
					continue
				case "accepted":
					if frame.GenerationID == "" || (id != "" && id != frame.GenerationID) {
						response.Body.Close()
						return nil, fmt.Errorf("model stream generation changed")
					}
					id = frame.GenerationID
				case "delta":
					d := frame.Delta
					if d == nil || d.GenerationID != id || d.Sequence < 1 {
						response.Body.Close()
						return nil, fmt.Errorf("invalid model delta identity")
					}
					if d.Sequence <= cursor {
						continue
					}
					if d.Sequence != cursor+1 {
						response.Body.Close()
						return nil, fmt.Errorf("model stream cursor gap")
					}
					if e = emit(core.ModelDelta{GenerationID: id, Sequence: d.Sequence, Text: d.Text}); e != nil {
						response.Body.Close()
						stop()
						return nil, e
					}
					cursor = d.Sequence
				case "terminal":
					response.Body.Close()
					if frame.Generation == nil || frame.Generation.ID != id || frame.Generation.DeltaSequence != cursor {
						return nil, fmt.Errorf("model terminal before complete stream")
					}
					return generationResponse(*frame.Generation)
				case "error":
					response.Body.Close()
					return nil, fmt.Errorf("model stream stopped: %s", frame.Code)
				default:
					response.Body.Close()
					return nil, fmt.Errorf("unknown model stream frame")
				}
			}
			e = scan.Err()
			response.Body.Close()
			if e == nil {
				e = io.ErrUnexpectedEOF
			}
		}
		err = e
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("model stream interrupted; same key required for recovery: %w", err)
}

func generationResponse(gen model.Generation) (*core.ModelResponse, error) {
	if gen.Status != model.GenCompleted {
		return nil, errors.New("model gateway generation " + gen.Status + ": " + gen.Error)
	}
	content, calls, err := decodeGenerationOutput(gen.Output)
	if err != nil {
		return nil, err
	}
	stop := gen.FinishReason
	if stop == "" {
		stop = "stop"
	}
	if len(calls) > 0 {
		stop = "tool_calls"
	}
	return &core.ModelResponse{GenerationID: gen.ID, Content: content, ToolCalls: calls, Stop: stop, Usage: &core.Usage{Input: gen.Usage.PromptTokens, Output: gen.Usage.CompletionTokens}}, nil
}
