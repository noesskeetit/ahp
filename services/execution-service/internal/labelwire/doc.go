// Package labelwire holds the contract for how data labels reach a routing
// decision. It contains no product code on purpose: what it pins is WHO may write
// the labels, and a contract about a writer is easier to read next to the legs that
// enforce it than buried in the gate.
//
// THE RULING (director, 2026-11-02)
//
//	Routing labels come from the SERVER's record of the deployment -- the operator
//	table ahp_model.deployment_labels (internal/model.DeploymentLabelStore) --
//	addressed by the workload the transport ATTESTED: the tenant and deployment in
//	the verified client certificate. A client-asserted label is not accepted
//	anywhere on the path, because the party asking to be routed to a less
//	restricted upstream would be the party stating the labels.
//
// # WHAT THIS WITHDRAWS
//
// The earlier draft of this contract (commit 0129a4ac, never merged) asked for four
// client-side carriers -- transport.Principal.Labels, gateway.ExecInput.Labels,
// executor.Turn.Labels, core.ModelRequest.Labels -- filled from the identity grant,
// and carried three red legs asking for them. Those legs are withdrawn, not
// softened: a grant labels the SUBJECT, while routing needs to know what the data
// being sent FROM this workload carries, and the deployment is the unit the
// platform attests and isolates. Adding a Labels field to a request-carrying struct
// would create exactly the input the ruling forbids, so the legs here assert its
// ABSENCE instead.
//
// WHAT MUST STAY TRUE
//
//   - One vocabulary: pkg/labels is the only label type, labels.CheckWrite the only
//     write gate (internal/model/routing already consults it on the chosen
//     destination -- the gate is real, this contract is about its input).
//   - Absent record means "the operator classified nothing", which is the EMPTY
//     set, and the empty set is still checked against the destination. It is never
//     a wildcard.
//   - No writer on the request path. The store has no HTTP face for the same
//     reason: an unauthenticated writer would put client assertion back through a
//     different door.
//   - No content in logs (PDF :347/:277): a routing decision names rule, provider,
//     model and ids, never a prompt, a tool argument or an artifact.
//   - Persisting labels onto ahp_model.generations would be a schema migration and
//     is the director's decision, not part of this contract.
package labelwire
