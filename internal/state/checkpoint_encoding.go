package state

import (
	"encoding/json"
	"strconv"
	"unicode/utf8"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// CheckpointArtifactEncoding explicitly distinguishes SDK artifact descriptors
// from arbitrary legacy bytes. The empty codec always means opaque user data.
const CheckpointArtifactEncoding = "ahp-artifact-v1"

func validatePayloadEncoding(encoding string, data []byte) error {
	if encoding == "" {
		return nil
	}
	if encoding != CheckpointArtifactEncoding {
		return invalid("unsupported checkpoint payload encoding")
	}
	if len(data) > MaxCheckpointPartBytes || !utf8.Valid(data) || !json.Valid(data) || !descriptorUnicodeValid(data) {
		return invalid("invalid checkpoint artifact descriptor JSON")
	}
	// Reuse the platform's strict JSON grammar to reject duplicate keys at
	// every depth, including escaped spellings of the same key. In particular,
	// Go may retain a previous string when a later duplicate is null, while
	// JSONB stores that null. Validation computes but never persists a digest
	// or normalized representation; original checkpoint bytes stay unchanged.
	if _, err := contracts.Digest(json.RawMessage(data)); err != nil {
		return invalid("invalid checkpoint artifact descriptor JSON keys")
	}
	// The descriptor is the immutable SDK projection, not an arbitrary document.
	// Exact key spelling matters: Go's struct decoder accepts case-insensitive
	// keys, while the reference scanner looks up the literal JSONB key "id".
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return invalid("invalid checkpoint artifact descriptor object")
	}
	for key := range fields {
		switch key {
		case "id", "version", "sha256", "size", "tenant", "deployment", "subject", "context_id", "task_id", "labels":
		default:
			return invalid("unknown checkpoint artifact descriptor field")
		}
	}
	if raw, ok := fields["labels"]; ok {
		var set map[string]json.RawMessage
		if json.Unmarshal(raw, &set) != nil {
			return invalid("invalid checkpoint artifact labels")
		}
		for key := range set {
			if key != "subjects" && key != "workspaces" && key != "taints" {
				return invalid("unknown checkpoint artifact label field")
			}
		}
	}
	var ref struct {
		ID         string     `json:"id"`
		Version    string     `json:"version"`
		SHA256     string     `json:"sha256"`
		Size       int64      `json:"size"`
		Tenant     string     `json:"tenant"`
		Deployment string     `json:"deployment"`
		Subject    string     `json:"subject"`
		ContextID  string     `json:"context_id"`
		TaskID     string     `json:"task_id"`
		Labels     labels.Set `json:"labels"`
	}
	if json.Unmarshal(data, &ref) != nil || ref.ID == "" || ref.Version == "" {
		return invalid("invalid checkpoint artifact descriptor")
	}
	return nil
}

// descriptorUnicodeValid checks raw escapes after json.Valid. Go replaces lone
// UTF-16 surrogates and invalid UTF-8 during Unmarshal, whereas PostgreSQL JSONB
// rejects them (and U+0000). Reject these inputs before persisting the original
// bytes; never silently normalize bytes that participate in the fingerprint.
func descriptorUnicodeValid(data []byte) bool {
	for i := 0; i < len(data); i++ {
		if data[i] != '\\' {
			continue
		}
		i++ // json.Valid guarantees an escaped character follows.
		if data[i] != 'u' {
			continue
		} // also skips escaped backslashes, e.g. "\\\\u0000".
		value, _ := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		i += 4
		switch {
		case value == 0:
			return false
		case value >= 0xd800 && value <= 0xdbff:
			if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		case value >= 0xdc00 && value <= 0xdfff:
			return false
		}
	}
	return true
}
