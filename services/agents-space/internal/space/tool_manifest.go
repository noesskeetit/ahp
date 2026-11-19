package space

import "encoding/json"

// GatewayToolDescriptor is a credential-free model-facing definition. The name
// is the existing Tool Gateway alias, not an MCP URL or a credential reference.
type GatewayToolDescriptor struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

// ToolManifestSnapshot pins the server-owned descriptor subset selected by the
// turn's AllowedTools policy. SourceSHA256 identifies the immutable bootstrap
// file; SHA256 covers the canonical selected descriptors actually used by Core.
// Credentials and connection locations remain exclusively in the owner gateway.
type ToolManifestSnapshot struct {
	Version      int                     `json:"version"`
	SourceSHA256 string                  `json:"source_sha256"`
	SHA256       string                  `json:"sha256"`
	Tools        []GatewayToolDescriptor `json:"tools"`
}
