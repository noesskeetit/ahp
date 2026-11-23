package journal

import "github.com/ahp-platform/ahp/pkg/contracts"

const RuntimeInjectorVSockPort uint32 = 5000

// RuntimeBootstrap is the non-secret identity and turn assignment of a guest.
type RuntimeBootstrap struct {
	Scope         contracts.Scope `json:"scope"`
	SessionID     string          `json:"session_id"`
	Harness       string          `json:"harness_type"`
	LaunchTurnSeq int64           `json:"launch_turn_seq"`
	Generation    int64           `json:"generation"`
	ExecutionID   string          `json:"execution_id"`
}
