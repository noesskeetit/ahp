package harness

import "strings"

// Run-id sources. One run has exactly one identity: the trio of artifacts, the
// run log and any live sidecar must carry the SAME id, otherwise a published
// number cannot be tied back to the log that produced it. On 2026-11-09 the
// wrapper log was named ...205424Z-6caf42 while the artifacts said
// ...205426Z-mx46gs, and a publication could not be reconciled for that reason
// alone.
const (
	RunIDFromWrapper = "wrapper"
	RunIDGenerated   = "self-generated"
)

// ResolveRunID prefers the id the caller (run-tests.py wrapper) already used
// to name the run log; it falls back to a generated one only when none was
// handed down, and says which happened so the artifact admits it.
func ResolveRunID(provided, generated string) (id string, source string) {
	if p := strings.TrimSpace(provided); p != "" {
		return p, RunIDFromWrapper
	}
	return generated, RunIDGenerated
}
