package gateway

import (
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// A2A 1.0 names a message role ROLE_USER / ROLE_AGENT. The v0.3 wire spelling
// this plane's real consumers still speak -- adapters/emil, and the reference
// client in tests/e2e/emil-tool-hop.mjs -- is "user" / "agent". a2a-go v2.7.0
// normalizes neither: MessageRole.UnmarshalJSON keeps whatever string arrived
// (only ROLE_UNSPECIFIED is special-cased), so a message accepted from such a
// client carries role "user" all the way into task_messages. Every consumer
// that then compares against a2a.MessageRoleUser silently misses the row, and
// that is exactly how a task whose own history provably contains the user's
// text reached an executor with a nil input and failed as "no usable text
// input". The text was never lost on the wire; the role made it unreachable.
//
// canonicalInboundRole closes the gap where it is created -- at the trust
// boundary, before the message is journalled -- so the stored row and every
// served copy of it speak one canonical enum, and so a message that cannot be
// a user turn is refused at the door instead of being stored and ignored later.

// normalizeRoleToken renders a role spelling comparable across protocol
// versions: trimmed, lower-cased, enum prefix separators folded.
func normalizeRoleToken(role a2a.MessageRole) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(string(role)), `"`))
}

// isUserTurnRole reports whether a journalled role names the user's turn,
// accepting the legacy spelling so rows written before canonicalization stay
// reachable. A stored row must never strand a task on a spelling alone.
func isUserTurnRole(role a2a.MessageRole) bool {
	switch normalizeRoleToken(role) {
	case "user", "role_user":
		return true
	}
	return false
}

// canonicalInboundRole maps the role of an inbound send request onto the
// canonical a2a.MessageRole. Only a user turn may open or continue a task: an
// agent-role message is the agent's own output and an empty or unknown role is
// not a claim this plane can honor, so both fail closed with ErrInvalid (the
// handler maps that to JSON-RPC -32602) rather than being journalled as
// something the drain will never select.
func canonicalInboundRole(role a2a.MessageRole) (a2a.MessageRole, error) {
	switch normalizeRoleToken(role) {
	case "user", "role_user":
		return a2a.MessageRoleUser, nil
	case "", "role_unspecified", "unspecified":
		return a2a.MessageRoleUnspecified, contracts.NewReasoned(contracts.ErrInvalid, ReasonRoleMissing,
			"gateway: message role is required (A2A 1.0 ROLE_USER)")
	case "agent", "role_agent":
		return a2a.MessageRoleUnspecified, contracts.NewReasoned(contracts.ErrInvalid, ReasonRoleAgent,
			"gateway: a send request must carry a user message, not an agent message")
	default:
		return a2a.MessageRoleUnspecified, contracts.NewReasoned(contracts.ErrInvalid, ReasonRoleUnknown,
			"gateway: unknown message role "+quote(string(role)))
	}
}

// selectLastInbound returns the most recent user message of a journalled
// conversation, or nil when the task carries none (a resume, or a task created
// by a plane that stores no history). Selection is by canonical role, so a
// legacy-spelled row is still found.
func selectLastInbound(hist []*a2a.Message) *a2a.Message {
	for i := len(hist) - 1; i >= 0; i-- {
		if hist[i] != nil && isUserTurnRole(hist[i].Role) {
			return hist[i]
		}
	}
	return nil
}
