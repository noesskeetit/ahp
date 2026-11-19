// Package triggers implements the AHP execution plane's Triggers subsystem:
// schedules and webhooks that start bounded, explicitly delegated background
// work on behalf of a parent task.
//
// Product requirements (PDF rev 3.2, the only requirements source) state that
// background work runs only through Triggers, that a Trigger stores a
// delegation reference rather than a token, and that at fire time Identity &
// Policy re-issues the task token afresh (PDF lines on Triggers, proactive
// cron, and drain). It also states that no task may outlive its parent except
// through an explicitly delegated trigger, and that proactive behaviour is
// bounded.
//
// Security-relevant invariants enforced here:
//
//   - A Trigger never stores a bearer token. It stores an opaque DelegationRef
//     resolved through the DelegationIssuer (the Identity API) at fire time.
//   - A non-delegated Trigger is revoked automatically once its parent task is
//     no longer active (TaskAuthority gate), so background work cannot outlive
//     its parent. A Delegated Trigger is allowed to outlive the parent because
//     the delegation itself is the durable authorisation.
//   - Proactive schedules require a positive MaxFires bound at creation, so a
//     schedule can never run unbounded.
//   - Webhook delivery is authenticated with a constant-time HMAC over a
//     canonical message before any effect is journalled, and is idempotent per
//     caller message id; a bad signature consumes no slot and writes no ledger
//     row.
//   - Every fire is journalled in the service-owned effect ledger with
//     durable, sequence-based idempotency keys, so a crash between dispatch and
//     settlement leaves SIDE_EFFECT_UNKNOWN that is never automatically
//     replayed.
package triggers
