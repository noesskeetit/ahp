package core

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/space"
)

// MemoryDriver is per-request and stateless. Shared tool registries must never
// hold a user's project, token or bank. Automatic writes come from State's
// durable completion outbox, independently of this model execution hook.
type MemoryDriver interface {
	Prepare(context.Context, *space.CoreRequest) ([]ChatMessage, error)
	Defs() []ToolDef
	Handles(string) bool
	Execute(context.Context, *space.CoreRequest, ToolCall) (json.RawMessage, error)
}

const memorySource = "ahp-memory"

// MemoryReplayAccess revalidates authority without changing an immutable
// checkpointed model request or performing a fresh retrieval on replay.
type MemoryReplayAccess interface {
	ValidateProject(context.Context, *space.CoreRequest) error
}

func memoryMessageIndices(messages []ChatMessage) []int {
	var indices []int
	for i, message := range messages {
		if message.Source == memorySource {
			indices = append(indices, i)
		}
	}
	return indices
}
func restoreMemoryMessages(messages []ChatMessage, indices []int) []ChatMessage {
	out := append([]ChatMessage(nil), messages...)
	for _, i := range indices {
		if i >= 0 && i < len(out) {
			out[i].Source = memorySource
		}
	}
	return out
}

func withoutMemory(messages []ChatMessage) []ChatMessage {
	out := make([]ChatMessage, 0, len(messages))
	for _, message := range messages {
		if message.Source != memorySource {
			out = append(out, message)
		}
	}
	return out
}
func providerMessages(messages []ChatMessage) []ChatMessage {
	out := append([]ChatMessage(nil), messages...)
	for i := range out {
		out[i].Source = ""
	}
	return out
}

// injectMemoryMessages places the injected memory blocks after the leading
// system block of the conversation. The pinned Dot configuration enters the
// loop as the head system message (cmd/as-runner/profile_runtime.go) and the
// provider accepts system messages only at the beginning: prepending memory
// pushed that block off the head and every turn died terminally on HTTP 400
// "System message must be at the beginning." (DEF-DOTE2E-11). The blocks
// themselves travel unmodified -- memory must reach the model, only its
// position is fixed.
func injectMemoryMessages(messages, injected []ChatMessage) []ChatMessage {
	out := make([]ChatMessage, 0, len(messages)+len(injected))
	i := 0
	for i < len(messages) && messages[i].Role == "system" {
		out = append(out, messages[i])
		i++
	}
	out = append(out, injected...)
	return append(out, messages[i:]...)
}
