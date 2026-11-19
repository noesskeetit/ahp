package execbridge

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/model"
)

// DEF-DOTE2E-11 at the inference boundary: whatever the caller assembled, the
// serialized body handed to the model leg must never carry a system message
// outside the leading positions -- the provider answers such a request with
// HTTP 400 "System message must be at the beginning." Hoisting is the last
// gate before the wire, so no calling path can replay this crash. Nothing may
// be dropped or folded into another role on the way.

// TestGatewayMessagesLeadSystemOnTheWire feeds the boundary the exact order
// recorded in ahp_model.generations.request for the failed personal-Dot turn:
// [user(memory), system(config), user(input)].
func TestGatewayMessagesLeadSystemOnTheWire(t *testing.T) {
	crashed := []core.ChatMessage{
		{Role: "user", Content: json.RawMessage(`"injected memory"`)},
		{Role: "system", Content: json.RawMessage(`"[AHP Dot configuration]\nbe terse"`)},
		{Role: "user", Content: json.RawMessage(`"hello"`)},
	}
	raw, err := json.Marshal(gatewayMessages(crashed))
	if err != nil {
		t.Fatal(err)
	}
	var wire []model.Message
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire) != 3 {
		t.Fatalf("wire messages = %d, want 3 (nothing may be dropped): %s", len(wire), raw)
	}
	assertLeadingSystemWire(t, wire, "injected memory", "[AHP Dot configuration]", "hello")
}

// TestGatewayMessagesKeepConversationOrder verifies the hoist is positional
// only: a conversation already in provider-legal shape reaches the provider
// in exactly the order the loop assembled it.
func TestGatewayMessagesKeepConversationOrder(t *testing.T) {
	legal := []core.ChatMessage{
		{Role: "system", Content: json.RawMessage(`"pinned configuration"`)},
		{Role: "user", Content: json.RawMessage(`"memory block"`)},
		{Role: "user", Content: json.RawMessage(`"question"`)},
		{Role: "assistant", Content: json.RawMessage(`"answer"`)},
		{Role: "user", Content: json.RawMessage(`"follow-up"`)},
	}
	wire := gatewayMessages(legal)
	if len(wire) != 5 {
		t.Fatalf("wire messages = %d, want 5: %+v", len(wire), wire)
	}
	for i, want := range []string{"system", "user", "user", "assistant", "user"} {
		if wire[i].Role != want {
			t.Fatalf("wire[%d].Role = %q, want %q: %+v", i, wire[i].Role, want, wire)
		}
	}
	assertLeadingSystemWire(t, wire, "pinned configuration", "memory block", "question", "answer", "follow-up")
}

func assertLeadingSystemWire(t *testing.T, wire []model.Message, wantContents ...string) {
	t.Helper()
	if len(wire) == 0 {
		t.Fatal("empty wire request")
	}
	if wire[0].Role != "system" {
		t.Fatalf("first serialized wire message is %q, want system: %+v", wire[0].Role, wire)
	}
	systems := 0
	for i, m := range wire {
		if m.Role != "system" {
			continue
		}
		systems++
		if i > 0 && wire[i-1].Role != "system" {
			t.Fatalf("system message at wire index %d is outside the leading block: %+v", i, wire)
		}
	}
	if systems != 1 {
		t.Fatalf("system messages = %d, want exactly 1", systems)
	}
	joined := make([]string, 0, len(wire))
	for _, m := range wire {
		joined = append(joined, m.Role+":"+m.Content)
	}
	combined := strings.Join(joined, "\n")
	for _, want := range wantContents {
		if !strings.Contains(combined, want) {
			t.Fatalf("wire request lost %q: %s", want, combined)
		}
	}
}
