//go:build !ahp_exec

package main

import "net/http"

// mountPublicAgentCard in the default (untagged) build serves nothing, for the
// same reason the execution role is a stub here: the fail-closed card loader
// lives in internal/gateway, and the default build deliberately does not link it
// or a2a-go. No fallback loader is written in this file - a second, weaker
// validator would be a way to publish a card the platform validator refuses.
//
// When the operator did name a card, this says so at WARN with the file name, so
// a missing card is never mistaken for a missing file. The stand's human edge
// runs the composed binary built with ahp_exec,ahp_data, so on the live window
// this branch is not the one taken.
func (s *server) mountPublicAgentCard(*http.ServeMux) bool {
	if file := envOr("AHP_EXEC_AGENT_CARD_FILE", ""); file != "" {
		s.logger.Warn("public Agent Card NOT published: this binary has no fail-closed card loader, rebuild with -tags ahp_exec",
			"file", file)
	}
	return false
}
