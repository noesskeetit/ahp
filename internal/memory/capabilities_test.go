package memory

import "testing"

// TestVocabularyIsTheClosedSet pins the ONLY source of truth for the strings the
// Memory plane is authorised by. The gateway asks for "memory"; the store asks
// for the four sub-actions. Adding a fifth action, renaming one, or dropping one
// has to fail here first, because the same list is published to operators by
// infra/memory/capability-roles.sql and validated at provisioning time.
func TestVocabularyIsTheClosedSet(t *testing.T) {
	want := []string{
		CapabilityGateway, CapabilityReview, CapabilityLabel,
		CapabilityDelete, CapabilityDeleteSubject,
	}
	if len(Vocabulary) != len(want) {
		t.Fatalf("Vocabulary has %d entries, want %d: %v", len(Vocabulary), len(want), Vocabulary)
	}
	for _, w := range want {
		if !IsCapability(w) {
			t.Errorf("IsCapability(%q) = false, want true", w)
		}
	}
}

func TestVocabularyRejectsEverythingElse(t *testing.T) {
	for _, s := range []string{"", " memory", "memory:", "memory:review:", "memory:own", "*", "MEMORY", "state", "memoryreview"} {
		if IsCapability(s) {
			t.Errorf("IsCapability(%q) = true, want false", s)
		}
	}
}

// TestGatewayCapabilityIsNamedMemory guards the name the HTTP face authorises
// with: renaming it silently turns every existing operator grant into a deny.
func TestGatewayCapabilityIsNamedMemory(t *testing.T) {
	if CapabilityGateway != "memory" {
		t.Fatalf("CapabilityGateway = %q, want %q", CapabilityGateway, "memory")
	}
	if CapabilityReview != "memory:review" || CapabilityLabel != "memory:label" ||
		CapabilityDelete != "memory:delete" || CapabilityDeleteSubject != "memory:delete-subject" {
		t.Fatalf("sub-action names changed: %v", Vocabulary)
	}
}
