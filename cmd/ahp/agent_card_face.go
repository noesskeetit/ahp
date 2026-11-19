//go:build ahp_exec

package main

import (
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/ahp-platform/ahp/internal/gateway"
)

// mountPublicAgentCard publishes the operator-provided public Agent Card at the
// canonical well-known path on a process that does NOT host the execution plane
// - in the live composition, the human edge.
//
// Measured reason (six-block audit, block 2, deputy measurement at 13:32Z, and
// my own re-measurement): the accepted card was published only by the process
// with AHP_ENABLE_EXECUTION=1 (the mTLS service aggregate on :18080), so a
// client that discovers an agent at the browser origin got 404 at
// /.well-known/agent-card.json. The card is a document of the DEPLOYMENT, not of
// one process: it names the interfaces of this stand, and the A2A client's first
// call by specification is GET {origin}/.well-known/agent-card.json. An edge that
// cannot answer it forces every client to be told a second, certificate-bearing
// origin out of band - which is exactly what a browser cannot do.
//
// What this mount deliberately is not: no /a2a/ face, no Space, no Data plane, no
// worker, no executor. The human edge stays a proxy for the user surface
// (single_origin.go) and the authority chain does not move one inch; this route
// returns bytes of an already-validated, already-public document. The gateway
// package documents the well-known card as untrusted, public and served without
// authentication, so adding it grants no capability to anybody.
//
// Nothing is fabricated, in either direction: the document is loaded with
// gateway.LoadAgentCardFile - the same fail-closed validator the execution plane
// uses (PDF rev3.2 6.1: mandatory AHP extension with closed enums and a positive
// run timeout, at least one public interface URL, no internal-face path, no
// embedded credentials, no DSN) - and it is served by the same library handler,
// so the bytes on both entries are identical by construction, not by convention.
//
// Every refusal leaves the path 404 exactly as it was before this file:
//   - no file named by AHP_EXEC_AGENT_CARD_FILE -> nothing is invented;
//   - the file is refused by the validator -> ERROR naming the file, nothing served;
//   - AHP_ENABLE_EXECUTION=1 -> the plane already publishes the same card at the
//     same path, and http.ServeMux panics on a second registration of one
//     pattern, so this mount steps aside instead of killing the process.
func (s *server) mountPublicAgentCard(mux *http.ServeMux) bool {
	file := envOr("AHP_EXEC_AGENT_CARD_FILE", "")
	if file == "" {
		return false
	}
	if envOr("AHP_ENABLE_EXECUTION", "") == "1" {
		s.logger.Info("public Agent Card face not mounted separately: the execution plane publishes the same card",
			"path", gateway.WellKnownAgentCardPath)
		return false
	}
	card, err := gateway.LoadAgentCardFile(file, envOr("AHP_EXEC_INTERNAL_PATH", "/a2a/internal"))
	if err != nil {
		s.logger.Error("public Agent Card NOT published at "+gateway.WellKnownAgentCardPath,
			"file", file, "err", err)
		return false
	}
	mux.Handle(gateway.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	s.logger.Info("public Agent Card published for discovery (no A2A face and no executor added by this mount)",
		"file", file, "path", gateway.WellKnownAgentCardPath,
		"name", card.Name, "version", card.Version, "skills", len(card.Skills))
	return true
}
