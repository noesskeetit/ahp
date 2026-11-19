package agentworkload

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ahp-platform/ahp/pkg/sdk"
	"github.com/ahp-platform/ahp/sdk/go/state"
)

const nativeConversationFormat = "agentworkload/conversation-v1"
const maxNativeHistoryBytes = 2 << 20

// The reference runtime owns this format. One turn stores only its input and
// output, rather than rewriting the entire conversation at every checkpoint.
// State remains opaque; neither Coordinator nor its transcript is consulted.
// Completed turns form the model history. A failed/canceled attempt retains its
// native run record, but is never invented into a successful assistant answer.
func (s *Service) priorConversation(ctx context.Context, claim *state.Claimed, saver *sdk.CheckpointSaver) ([]map[string]string, error) {
	current := claim.Lease.Run
	if current.Sequence == 1 {
		return nil, nil
	}
	if current.Sequence < 1 {
		return nil, fmt.Errorf("native run has no ordered conversation position")
	}
	runs, err := s.state.ListRuns(ctx, claim.Lease.Thread.ID)
	if err != nil {
		return nil, err
	}
	var messages []map[string]string
	var sequence int64
	found := false
	bytes := 0
	for _, run := range runs {
		if run.ThreadID != claim.Lease.Thread.ID || run.Sequence != sequence+1 {
			return nil, fmt.Errorf("native run history is incomplete or out of order")
		}
		sequence = run.Sequence
		if run.ID == current.ID {
			if run.Sequence != current.Sequence {
				return nil, fmt.Errorf("native run position changed")
			}
			found = true
			break
		}
		if run.Status != state.RunDone {
			continue
		}
		var input payload
		if json.Unmarshal(run.Payload, &input) != nil || input.Text == "" {
			return nil, fmt.Errorf("native prior input is unreadable")
		}
		cp, err := saver.GetLatest(ctx, run.ID)
		if err != nil {
			return nil, err
		}
		var meta struct {
			Format  string `json:"format"`
			AgentID string `json:"agent_id"`
			Digest  string `json:"version_digest"`
			Profile string `json:"profile"`
		}
		if json.Unmarshal(cp.Metadata, &meta) != nil || meta.AgentID != s.cfg.AgentID || meta.Digest != input.Digest || meta.Profile != input.Profile || meta.Format != "" && meta.Format != nativeConversationFormat {
			return nil, fmt.Errorf("native checkpoint format or producer is incompatible")
		}
		output, hasOutput, hasInput := "", false, false
		for _, part := range cp.Parts {
			switch part.Channel {
			case "input":
				if string(part.Data) != input.Text {
					return nil, fmt.Errorf("native checkpoint input differs from its accepted turn")
				}
				hasInput = true
			case "output":
				output, hasOutput = string(part.Data), true
			}
		}
		// Existing reference-runtime checkpoints had an output part and kept
		// input in the immutable run payload. New format requires both parts.
		if !hasOutput || meta.Format == nativeConversationFormat && !hasInput {
			return nil, fmt.Errorf("native checkpoint is missing conversation parts")
		}
		bytes += len(input.Text) + len(output)
		if bytes > maxNativeHistoryBytes {
			return nil, fmt.Errorf("native conversation exceeds reference runtime input budget; compaction is required")
		}
		messages = append(messages, map[string]string{"role": "user", "content": input.Text}, map[string]string{"role": "assistant", "content": output})
	}
	if !found {
		return nil, fmt.Errorf("native run history does not contain the claimed turn")
	}
	return messages, nil
}
