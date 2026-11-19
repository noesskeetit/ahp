package main

import "testing"

// The run's capabilities are the whole of what a run may do: Identity honours only what the
// grant literally holds, and State asks Identity about "state" on every task route. A capability
// set without it is a run that dies on its first durable write, so it must be refused at startup.
func TestRunGrantCapsMustBeAbleToReachState(t *testing.T) {
	if err := validateRunGrantCaps([]string{"space:access", "artifact:read", "identity:cancel", "state"}); err != nil {
		t.Fatalf("a capability set that can persist must be accepted: %v", err)
	}
	err := validateRunGrantCaps([]string{"space:access", "artifact:read", "identity:cancel"})
	if err == nil {
		t.Fatal("capabilities without the State capability must be refused")
	}
	if got := holdsCapability(nil, stateCapability); got {
		t.Fatal("an unset capability set cannot hold the State capability")
	}
}
