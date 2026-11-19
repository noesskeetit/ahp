// Package tools is the AHP execution-plane tool plane: a real MCP client,
// Vault-backed credentials, human-only canonical-digest approvals, an
// independent Guardian decision path, an OAuth token-exchange (RFC 8693)
// adapter bounded by netpolicy, and a service-owned effect ledger that gives
// every side-effecting call ACCEPTED/DISPATCHED/SUCCEEDED/FAILED/
// SIDE_EFFECT_UNKNOWN semantics that survive a restart.
//
// Security invariants (requirements_source: ahp-agents-space-architecture
// rev 3.2):
//   - An approval is bound to a canonical digest of {tool, arguments} and can
//     only be granted by a human token; task tokens are rejected (auth.KindHuman).
//   - The Guardian is consulted on every call and independently of approval;
//     side-effecting tools are default-deny unless an explicit decision allows.
//   - Outbound provider/OAuth traffic goes through pkg/netpolicy; loopback,
//     link-local, metadata and multicast addresses are never reachable, even
//     when a host is allowlisted as a private host.
//   - Ambiguous side effects (crash between dispatch and completion) are
//     surfaced as SIDE_EFFECT_UNKNOWN and are never automatically replayed.
package tools
