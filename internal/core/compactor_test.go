package core

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// toolPairIDs returns the set of tool_call ids an assistant message declares.
func toolPairIDs(m ChatMessage) map[string]bool {
	out := map[string]bool{}
	if len(m.ToolCalls) == 0 {
		return out
	}
	var tcs []ToolCall
	if err := json.Unmarshal(m.ToolCalls, &tcs); err != nil {
		return out
	}
	for _, tc := range tcs {
		out[tc.ID] = true
	}
	return out
}

// assertNoOrphanTool enforces the message-sequence invariant that every
// OpenAI-compatible provider requires: a "tool" message must be preceded by an
// "assistant" message carrying a tool_call whose id matches. Compaction must
// never drop that owning assistant while keeping its tool result, or the next
// model call is rejected with a 400 ("tool message ... preceding ... tool_calls").
func assertNoOrphanTool(t *testing.T, src string, msgs []ChatMessage) {
	t.Helper()
	for i, m := range msgs {
		if m.Role != "tool" {
			continue
		}
		if m.ToolCallID == "" {
			t.Fatalf("%s: kept tool message at %d has empty tool_call_id", src, i)
		}
		owner := false
		for j := 0; j < i; j++ {
			if msgs[j].Role == "assistant" && toolPairIDs(msgs[j])[m.ToolCallID] {
				owner = true
				break
			}
		}
		if !owner {
			t.Fatalf("%s: orphaned tool result at index %d (tool_call_id=%s): its owning assistant-with-tool_calls was dropped by compaction — provider would reject this history",
				src, i, m.ToolCallID)
		}
	}
}

// toolHistory builds a long, tool-rich history: a system prompt followed by many
// [assistant-with-tool_calls, tool-result] rounds ending on a tool result — the
// exact shape that occurs mid-turn when the model has just called a tool.
func toolHistory(rounds int) []ChatMessage {
	msgs := []ChatMessage{{Role: "system", Content: json.RawMessage(`"be helpful"`)}}
	msgs = append(msgs, ChatMessage{Role: "user", Content: json.RawMessage(`"` + strings.Repeat("initial question ", 30) + `"`)})
	for i := 0; i < rounds; i++ {
		id := fmt.Sprintf("call_%d", i)
		tcs, _ := json.Marshal([]ToolCall{{ID: id, Name: "emil", Args: json.RawMessage(`{"q":"x"}`)}})
		msgs = append(msgs,
			ChatMessage{Role: "assistant", Content: json.RawMessage(`"thinking"`), ToolCalls: tcs},
			ChatMessage{Role: "tool", Content: json.RawMessage(`"` + strings.Repeat("tool output payload ", 20) + `"`), ToolCallID: id},
		)
	}
	return msgs
}

// TestCompactorNeverOrphansToolResults scans a budget range over a tool-rich
// history and asserts the no-orphan invariant on every compaction output. This
// reproduces the historical defect where a budget cut landed between an
// assistant(tool_calls) and its tool result, keeping the orphaned tool result.
func TestCompactorNeverOrphansToolResults(t *testing.T) {
	c := NewTokenCompactor(0)
	hist := toolHistory(6)
	full := EstimateHistory(hist)
	// Sweep budgets from a single message up to the full history. At least one
	// budget must actually trigger compaction (saved>0), else the test is vacuous.
	compactedAtLeastOnce := false
	for budget := 40; budget <= full; budget += 7 {
		out, saved, err := c.Compact(nil, hist, budget)
		if err != nil {
			t.Fatalf("budget=%d: Compact error: %v", budget, err)
		}
		if saved > 0 {
			compactedAtLeastOnce = true
		}
		src := fmt.Sprintf("budget=%d kept=%d saved=%d", budget, len(out), saved)
		assertNoOrphanTool(t, src, out)
	}
	if !compactedAtLeastOnce {
		t.Fatalf("test vacuous: no budget in [40,%d] triggered compaction", full)
	}
}

// TestCompactorForcedTailKeepsOwner pins the kept==0 forced-tail branch: even
// when the budget cannot fit the mandatory tail, the first kept content message
// must not be an orphaned tool result.
func TestCompactorForcedTailKeepsOwner(t *testing.T) {
	c := &TokenCompactor{Budget: 0, KeepRecent: 1}
	hist := toolHistory(4)
	// A budget just above the system prompt forces the forced-tail path.
	out, saved, err := c.Compact(nil, hist, 80)
	if err != nil {
		t.Fatalf("Compact error: %v", err)
	}
	src := fmt.Sprintf("forced-tail kept=%d saved=%d", len(out), saved)
	assertNoOrphanTool(t, src, out)
}
