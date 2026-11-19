package gateway_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
)

// contractCard is a contract-shaped public card built from the Go types, so this
// test depends on no other test file's fixture.
func contractCard() *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:    "AHP Execution Plane",
		Version: "1.0.0",
		SupportedInterfaces: []*a2a.AgentInterface{{
			URL:             "https://exec.example.com/a2a/public",
			ProtocolBinding: a2a.TransportProtocolJSONRPC,
			ProtocolVersion: a2a.Version,
		}},
		Capabilities: a2a.AgentCapabilities{
			Streaming: true,
			Extensions: []a2a.AgentExtension{{
				URI:      gateway.AgentExtensionURI,
				Required: true,
				Params: map[string]any{
					"history": "owned", "steering": "queue", "run_timeout_ms": float64(300000),
				},
			}},
		},
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"text/plain"},
		Skills:             []a2a.AgentSkill{{ID: "execute-task", Name: "Execute a durable A2A task"}},
	}
}

// TestTopLevelURLClaimIsNotPartOfThisContract documents the second symptom the
// six-block audit measured on the human edge (agent_card_has_url=false) instead
// of pretending to fix it.
//
// In the A2A version this plane serves, the address of an agent lives on
// supportedInterfaces[].url and a2a.AgentCard has NO top-level url field. So
// "add url to the card file" is not an available answer to that measurement: the
// loader decodes the file into a2a.AgentCard, which drops unknown members, and the
// handler serves json.Marshal of that value -- the claim would vanish between the
// file and the wire while the operator believed it published. This test pins the
// vanishing so nobody "fixes" the audit by editing a document nobody reads.
//
// The card is published WITH a live address today: supportedInterfaces[0].url,
// which the platform validator requires to exist. Making a top-level url a real
// claim is a contract decision (which A2A version the stand speaks), not an edit
// of this file, and it belongs to whoever owns the card contract.
func TestTopLevelURLClaimIsNotPartOfThisContract(t *testing.T) {
	raw, err := json.Marshal(contractCard())
	if err != nil {
		t.Fatalf("marshal card: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal card: %v", err)
	}
	const injected = "http://127.0.0.1:18081/a2a/public"
	doc["url"] = injected
	fileRaw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal doctored card: %v", err)
	}
	path := filepath.Join(t.TempDir(), "agent-card.json")
	if err := os.WriteFile(path, fileRaw, 0o600); err != nil {
		t.Fatalf("write card: %v", err)
	}

	card, err := gateway.LoadAgentCardFile(path, "/a2a/internal")
	if err != nil {
		t.Fatalf("an unknown top-level member must not break loading: %v", err)
	}
	if len(card.SupportedInterfaces) == 0 || card.SupportedInterfaces[0].URL == "" {
		t.Fatalf("the interface url (the address this contract does carry) was lost: %+v", card.SupportedInterfaces)
	}

	served, err := json.Marshal(card)
	if err != nil {
		t.Fatalf("marshal served card: %v", err)
	}
	var servedDoc map[string]any
	if err := json.Unmarshal(served, &servedDoc); err != nil {
		t.Fatalf("unmarshal served card: %v", err)
	}
	if got, ok := servedDoc["url"]; ok {
		t.Fatalf("the contract grew a top-level url (%v): the audit's has_url symptom must be re-read, not re-pinned here", got)
	}
	if bytes.Contains(served, []byte("18081")) {
		t.Fatalf("the dropped claim leaked onto the wire anyway: %s", served)
	}
}
