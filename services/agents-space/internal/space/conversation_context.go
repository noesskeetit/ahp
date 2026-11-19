package space

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// priorConversationMessages rebuilds preceding turns from the scoped run list
// and authoritative transcript. Checkpoints are per-run recovery aids, not the
// conversation history. A new run must not inherit the previous run's resume flag.
func (c *Coordinator) priorConversationMessages(ctx context.Context, token string, current runResponse) ([]json.RawMessage, error) {
	if current.Sequence <= 1 {
		return nil, nil
	}
	runs, err := c.state.ListRuns(ctx, token, current.ThreadID)
	if err != nil {
		return nil, err
	}
	if len(runs) > 10000 {
		return nil, contracts.NewError(contracts.ErrConflict, "space: conversation run history exceeds bound")
	}
	const maxBytes = 8 << 20
	size := 0
	prior := make([]runResponse, 0, len(runs))
	byID := make(map[string][]json.RawMessage)
	found := false
	for _, run := range runs {
		if run.ID == current.ID {
			found = true
		}
		if run.ThreadID != current.ThreadID || run.Sequence >= current.Sequence {
			continue
		}
		var payload turnRunPayload
		if err := json.Unmarshal(run.Payload, &payload); err != nil {
			return nil, fmt.Errorf("space: invalid conversation run payload: %w", err)
		}
		if payload.Type != "turn" {
			continue
		}
		if run.Status != "done" && run.Status != "failed" && run.Status != "canceled" {
			return nil, contracts.NewError(contracts.ErrConflict, "space: previous conversation run is unfinished")
		}
		message, err := json.Marshal(map[string]any{"role": "user", "content": payload.Input})
		if err != nil {
			return nil, err
		}
		size += len(message)
		if size > maxBytes {
			return nil, contracts.NewError(contracts.ErrConflict, "space: conversation history exceeds byte bound")
		}
		byID[run.ID] = []json.RawMessage{message}
		prior = append(prior, run)
	}
	if !found {
		return nil, contracts.NewError(contracts.ErrConflict, "space: current run missing from conversation history")
	}
	const maxScan = 200000
	after := int64(0)
	scanned := 0
	for {
		events, more, err := c.state.readRecoveryStreamPage(ctx, token, current.ThreadID, after, recoveryStreamPageSize, recoveryStreamPageBytes)
		if err != nil {
			return nil, err
		}
		for _, ev := range events {
			if ev.Sequence <= after {
				return nil, contracts.NewError(contracts.ErrConflict, "space: conversation transcript cursor did not advance")
			}
			after = ev.Sequence
			scanned++
			if ev.Kind != string(StepModel) && ev.Kind != string(StepTool) {
				continue
			}
			sep := strings.LastIndexByte(ev.Key, ':')
			if sep < 0 {
				continue
			}
			runID := ev.Key[:sep]
			if _, ok := byID[runID]; !ok {
				continue
			}
			if _, err := strconv.Atoi(ev.Key[sep+1:]); err != nil {
				continue
			}
			content := stepFrameContent(ev.Payload)
			if len(content) == 0 || string(content) == "null" {
				continue
			}
			// Tool frames retain their recorded output as conversation evidence;
			// no tool call is synthesized or replayed while rebuilding history.
			message, err := json.Marshal(map[string]any{"role": "assistant", "content": content})
			if err != nil {
				return nil, err
			}
			size += len(message)
			if size > maxBytes {
				return nil, contracts.NewError(contracts.ErrConflict, "space: conversation history exceeds byte bound")
			}
			byID[runID] = append(byID[runID], message)
		}
		if !more {
			break
		}
		if scanned >= maxScan {
			return nil, contracts.NewError(contracts.ErrConflict, "space: conversation transcript exceeds scan bound")
		}
	}
	var messages []json.RawMessage
	for _, run := range prior {
		messages = append(messages, byID[run.ID]...)
	}
	return messages, nil
}

func checkpointBelongsToRun(cp checkpointResponse, run runResponse) bool {
	var metadata struct {
		State struct {
			TurnID string `json:"turn_id"`
		} `json:"state"`
	}
	if json.Unmarshal(cp.Metadata, &metadata) == nil && metadata.State.TurnID != "" {
		return metadata.State.TurnID == run.ID
	}
	if strings.HasPrefix(cp.ID, run.ID+":ckpt:") {
		return true
	}
	// Legacy checkpoints did not name the run. They are unambiguous only in
	// the first run of a thread; later runs recover from their own transcript.
	return run.Sequence <= 1
}
