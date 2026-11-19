// Package execbridge mounts the execution plane's model turns on the Model
// Gateway instead of on a direct provider credential.
//
// It is the seam the composed cmd/ahp execution role uses: the A2A gateway
// hands every accepted task to a Bridge, which runs the canonical
// core.Runtime loop whose ModelClient is a gatewayModel. Every model call is
// therefore a durable model.Generation: it is admitted against a per-task root
// budget BEFORE any spend, journalled in the effect ledger (so a crash yields
// SIDE_EFFECT_UNKNOWN and is never silently replayed), and settled from real
// usage. The bridge never holds a provider key; the credential lives only
// behind the model.Inference the Model Gateway owns.
//
// Fail-closed by construction. When no provider runtime secret is configured
// the bridge installs unconfiguredInference, whose Stream call returns an
// explicit error (never a canned string), so tasks drain to a terminal FAILED
// and the budget is released rather than a turn being fabricated. This mirrors
// the gateway's honest "no executor configured" default.
//
// Scope of the turn: model.Inference is a text-only streaming boundary, so a
// bridged turn is a model turn. The artifact-producing Qwen+tools executor
// (internal/executor, object-storage tier) stays a separate, explicitly
// selected wiring; the bridge never silently substitutes for it.
package execbridge
