//go:build state_integration

package space_test

import (
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// hitl_gate_read_realstate_test.go — a gate is readable on the real server by a process that never
// saw it.
//
// This is the second half of the fix, and it is in a file of its own for a reason that is itself the
// defect: the assertion calls an operation the pre-fix product does not have, so no form of this test
// can compile against the tree it is supposed to condemn. The real-wire RED leg therefore runs
// hitl_gate_realstate_test.go alone; the absence of the read is measured where it CAN be said - the
// untagged leg, where the same call answers 404 "space: unknown operation GetHITL" - and disclosed in
// the post rather than hidden behind a build failure reported as a red test.
//
// What is pinned here is the part only the real server can answer: the gate's request event and its
// answer event both live in the thread's durable inbox, and the read must reconstruct one approval
// card's state from both - including that it must NOT report an answer instant, because the published
// inbox event carries none.
func TestAnsweredHITLGateIsReadableOnRealState(t *testing.T) {
	coord, e, ownerTok, turnID, threadID, hitlID := parkedGateRealState(t)

	if err := coord.RespondHITL(e.ctx, ownerTok, space.HITLRespondRequest{
		HITLID: hitlID, Approved: false, Response: json.RawMessage(`{"note":"not this way"}`),
	}, threadID); err != nil {
		t.Fatalf("RespondHITL on a fresh gate: %v", err)
	}

	// A brand-new Coordinator over the same State is the only form a process restart takes in a test.
	cold := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}, WorkerID: "w-gateread-cold"})
	gate, err := cold.GetHITL(e.ctx, ownerTok, threadID, hitlID)
	if err != nil {
		t.Fatalf("GetHITL on a cold reader: %v", err)
	}
	if gate.Status != space.HITLRejected || gate.TurnID != turnID {
		t.Fatalf("cold gate read = %+v, want rejected on turn %s", gate, turnID)
	}
	if string(gate.Response) == "" || string(gate.Response) == "null" {
		t.Fatalf("the recorded decision is missing from the cold gate read: %+v", gate)
	}
	if gate.Prompt == nil || string(gate.Prompt) == "null" {
		t.Fatalf("the read lost the question the gate asked, so the card cannot redraw itself: %+v", gate)
	}
	if gate.RespondedAt != nil {
		t.Fatalf("the read invented an answer instant (%v): the published inbox event carries none", gate.RespondedAt)
	}

	// And a gate nobody submitted stays un-readable rather than being invented.
	if _, err := cold.GetHITL(e.ctx, ownerTok, threadID, "hitl-never-submitted"); err == nil {
		t.Fatalf("GetHITL invented a gate nobody submitted")
	}
}
