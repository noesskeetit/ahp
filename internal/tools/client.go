package tools

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MCPClient is the narrow surface the tool plane needs from an MCP session.
// The official go-sdk *mcp.ClientSession satisfies it; tests substitute an
// in-memory fake so the whole call path (Guardian -> ledger -> MCP -> ledger
// completion) runs without a network.
type MCPClient interface {
	CallTool(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error)
	Close() error
}

// Connector establishes an MCP session for a tool connection using the
// resolved credential. It returns a ready-to-use client; callers must Close it.
// Connectors are registered per Connection key in Config.
type Connector func(ctx context.Context, cred Credential) (MCPClient, error)

// ToolSpec is the static description of one callable tool.
type ToolSpec struct {
	// Name is the MCP tool name sent to the server in CallTool.
	Name string
	// Connection selects the Connector used to open the MCP session and the
	// credential connection name passed to the CredentialResolver.
	Connection string
	// SideEffecting marks tools that mutate external state. Such calls carry a
	// ledger guard, are Guardian default-deny, and go SIDE_EFFECT_UNKNOWN (never
	// auto-replayed) when the transport outcome is ambiguous.
	SideEffecting bool
	// RequiresApproval forces a matching human-approved canonical digest
	// before the effect is journalled at all.
	RequiresApproval bool
}
