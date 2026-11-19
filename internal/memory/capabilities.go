package memory

import "sort"

// The Memory plane is authorised by exactly these strings, and this file is
// their only definition. Two layers consult it: the HTTP face gates every
// request on CapabilityGateway, and the store gates the four privileged
// lifecycle actions on the sub-actions below (see internal/memory/lifecycle.go).
//
// The list is not private to the process. It is published to operators by
// infra/memory/capability-roles.sql, applied by
// scripts/apply-memory-capabilities.sh, and checked at provisioning time by
// cmd/ahp/provision.go. That closes DEF-MEM-6: before this, the vocabulary
// existed only as literals inside the composition, so an operator granting
// "memory:revieww" was accepted silently and the user then met an
// unexplainable 403 with nothing naming the typo.
//
// Nothing here widens an ACL. Authorisation is still answered by the same
// authority that guards every other plane; this only makes the required names
// enumerable and typos impossible to hide.
const (
	// CapabilityGateway gates the /v1/memory/ face.
	CapabilityGateway = "memory"
	// CapabilityReview activates or rejects a proposal.
	CapabilityReview = "memory:review"
	// CapabilityLabel adds labels to an existing record.
	CapabilityLabel = "memory:label"
	// CapabilityDelete tombstones one record.
	CapabilityDelete = "memory:delete"
	// CapabilityDeleteSubject erases every record of one subject (right to be
	// forgotten); it is the most privileged action in the plane.
	CapabilityDeleteSubject = "memory:delete-subject"
)

// Vocabulary is the closed, sorted set above: the gateway capability plus the
// four store-level actions. Sorted so error text and published listings are
// byte-stable across runs.
var Vocabulary = []string{
	CapabilityGateway,
	CapabilityReview,
	CapabilityLabel,
	CapabilityDelete,
	CapabilityDeleteSubject,
}

func init() { sort.Strings(Vocabulary) }

// IsCapability reports whether s is exactly one of the published capabilities.
// Comparison is exact: no wildcards, no case folding, no trimming, so a string
// that looks right but is not one of the set is refused rather than quietly
// granting nothing.
func IsCapability(s string) bool {
	for _, c := range Vocabulary {
		if c == s {
			return true
		}
	}
	return false
}
