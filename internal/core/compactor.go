package core

import (
	"context"
	"encoding/json"
)

// TokenCompactor is the default Compactor: it keeps the leading system prompt
// and the most recent messages within a token budget, dropping the oldest
// non-system turns in the middle. Tokens are estimated from byte length at
// roughly four characters per token, which is a stable, provider-independent
// heuristic good enough to bound context-window growth.
type TokenCompactor struct {
	// Budget is the default token budget used when the caller passes <= 0.
	Budget int
	// KeepRecent is the minimum number of trailing messages that are always
	// preserved regardless of budget, so the loop never loses its immediate
	// conversational context. Defaults to 4.
	KeepRecent int
}

// NewTokenCompactor builds a compactor with the given default budget.
func NewTokenCompactor(budget int) *TokenCompactor {
	return &TokenCompactor{Budget: budget, KeepRecent: 4}
}

// EstimateTokens approximates the token cost of a single message.
func EstimateTokens(m ChatMessage) int {
	n := len(m.Role) + len(m.Content) + len(m.ToolCallID) + len(m.ToolCalls)
	return n/4 + 1
}

// EstimateHistory approximates the token cost of a whole history.
func EstimateHistory(msgs []ChatMessage) int {
	total := 0
	for _, m := range msgs {
		total += EstimateTokens(m)
	}
	return total
}

// Compact satisfies the Compactor interface. It returns a history trimmed to
// fit budget and the number of tokens saved. When the history already fits it
// returns the input unchanged with a zero saving, so callers can cheaply
// detect that no compaction was needed.
//
// Compaction only ever removes a contiguous oldest prefix of the non-system
// turns; it never reorders or edits messages. Crucially, the kept tail is
// aligned so it never begins with an orphaned "tool" result: such a message's
// owning assistant-with-tool_calls would have been dropped into the removed
// prefix, and OpenAI-compatible providers reject a tool message that does not
// follow the matching assistant tool_calls (HTTP 400). When the budget cut
// would land inside an assistant/tool pair, the split is advanced past the
// dangling tool result(s) so the resulting history stays provider-valid.
func (c *TokenCompactor) Compact(_ context.Context, msgs []ChatMessage, budget int) ([]ChatMessage, int, error) {
	if budget <= 0 {
		budget = c.Budget
	}
	if budget <= 0 {
		return msgs, 0, nil
	}
	before := EstimateHistory(msgs)
	if before <= budget {
		return msgs, 0, nil
	}

	keepRecent := c.KeepRecent
	if keepRecent <= 0 {
		keepRecent = 4
	}

	// Partition into a leading system block (always kept) and the rest.
	var system []ChatMessage
	rest := msgs
	for i, m := range msgs {
		if m.Role == "system" {
			system = append(system, m)
			continue
		}
		rest = msgs[i:]
		break
	}
	if len(system) == len(msgs) {
		// Nothing but system messages; nothing to drop.
		return msgs, 0, nil
	}

	systemTokens := EstimateHistory(system)
	// Reserve a little headroom for the next assistant/tool turn so a single
	// compaction does not immediately overflow again.
	reserve := 64
	available := budget - systemTokens - reserve
	if available < 0 {
		available = 0
	}

	// Walk backwards from the tail, keeping as many recent messages as fit,
	// but never fewer than keepRecent.
	used := 0
	split := len(rest)
	for i := len(rest) - 1; i >= 0; i-- {
		t := EstimateTokens(rest[i])
		if used+t > available && len(rest)-i >= keepRecent {
			break
		}
		used += t
		split = i
	}
	if split == len(rest) {
		// Budget too small for any message; force-keep the mandatory tail.
		split = len(rest) - keepRecent
		if split < 0 {
			split = 0
		}
	}

	// Align the split so the kept tail never starts with an orphaned tool
	// result. The owning assistant-with-tool_calls for a leading tool message
	// sits in the dropped prefix, which would make the history invalid for the
	// next model call. Advancing forward keeps only a structurally valid tail;
	// if that empties the tail, the system block (plus the elision marker) is
	// still a valid history on its own.
	for split < len(rest) && rest[split].Role == "tool" {
		split++
	}
	kept := len(rest) - split

	out := append([]ChatMessage{}, system...)
	// Emit a marker so the model knows older turns were elided.
	if dropped := len(rest) - kept; dropped > 0 {
		marker := ChatMessage{
			Role:    "system",
			Content: mustJSON(map[string]any{"compacted": dropped, "note": "older turns elided to fit context budget"}),
		}
		out = append(out, marker)
	}
	out = append(out, rest[split:]...)
	saved := before - EstimateHistory(out)
	if saved < 0 {
		saved = 0
	}
	return out, saved, nil
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`"{}"`)
	}
	return b
}
