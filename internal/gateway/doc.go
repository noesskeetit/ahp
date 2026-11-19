// Package gateway implements the AHP execution-plane A2A gateway.
//
// It speaks the official A2A wire protocol (github.com/a2aproject/a2a-go) over
// the upstream JSON-RPC/SSE transport while owning every durability decision
// itself:
//
//   - Tasks are durable PostgreSQL rows with a monotonically increasing
//     status_version; a stale writer can never move a task backwards.
//   - Message acceptance is idempotent per (scope, caller, target, messageId).
//     A replay of the same canonical request returns the stored result; a
//     different body under a used key is rejected with KEY_REUSE_MISMATCH.
//   - Contexts are owned: a context id is bound to the (scope, caller) pair
//     that created it and can never be reused by anybody else.
//   - Every status or artifact transition is journalled in a per-task event
//     log, which is the source of truth for SSE replay: a subscriber, even one
//     that connects after a process restart, sees the whole history and then
//     follows the live stream (Redis is only a wake-up signal).
//   - Terminal transitions enqueue outbox entries that are delivered at least
//     once, guarded by the service-owned effect ledger, so a crash mid
//     dispatch yields SIDE_EFFECT_UNKNOWN instead of a silent replay.
//   - Workers hold a lease and bump a fencing token per attempt; effects
//     dispatched by a superseded attempt are fenced, never assumed done.
//
// The gateway never trusts identity from a body: scope always comes from a
// verified transport principal.
package gateway
