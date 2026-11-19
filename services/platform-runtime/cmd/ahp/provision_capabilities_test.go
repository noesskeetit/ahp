package main

import (
	"strings"
	"testing"
)

// TestProvisionRejectsUnknownMemoryCapability is DEF-MEM-6's fail-loud half: a
// misspelled Memory capability must stop the process at provisioning time,
// because today it is accepted, silently produces a grant that satisfies
// nothing, and only ever surfaces as an unexplainable 403 in production.
func TestProvisionRejectsUnknownMemoryCapability(t *testing.T) {
	for _, bad := range []string{
		"memory:revieww",     // typo of a real sub-action
		"memory:delete-subj", // truncated
		"memory:",            // empty suffix
		"memory :review",     // whitespace is not the same string
	} {
		err := validateProvisionPermissions([]string{bad})
		if err == nil {
			t.Errorf("validateProvisionPermissions(%q) = nil, want rejection", bad)
			continue
		}
		if !strings.Contains(err.Error(), bad) {
			t.Errorf("error for %q must name the offending capability, got: %v", bad, err)
		}
	}
}

// TestProvisionAcceptsCanonicalSetAndOtherPlanes proves this is not a global
// allowlist in disguise: foreign plane capabilities (which Memory has no
// authority to classify) stay free-form, while the whole Memory vocabulary is
// accepted verbatim.
func TestProvisionAcceptsCanonicalSetAndOtherPlanes(t *testing.T) {
	if err := validateProvisionPermissions([]string{
		"memory", "memory:review", "memory:label", "memory:delete", "memory:delete-subject",
		"space:access", "state", "artifacts", "tools", "tools:approve", "registry", "triggers", "compute",
		"identity:delegate", "identity:cancel",
	}); err != nil {
		t.Fatalf("canonical + foreign capabilities rejected: %v", err)
	}
}

// TestProvisionErrorIsActionable: an operator who hits the wall must be told the
// closed set, not just that something was wrong.
func TestProvisionErrorIsActionable(t *testing.T) {
	err := validateProvisionPermissions([]string{"memory:revieww"})
	if err == nil {
		t.Fatal("want error")
	}
	for _, want := range []string{"memory:review", "memory:delete-subject", "AHP_PROVISION_USERS_JSON"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error must list %q so the operator can fix it, got: %v", want, err)
		}
	}
}
