package tools

import (
	"context"
	"encoding/json"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// GuardRequest is what the Guardian inspects before any effect is sent. It is
// evaluated independently of the approval check: an approved call can still be
// denied by the Guardian, and vice versa.
type GuardRequest struct {
	Tool          string
	Connection    string
	SideEffecting bool
	Digest        string
	Arguments     json.RawMessage
}

// Decision is the Guardian verdict. Class tags the decision for audit.
type Decision struct {
	Allow  bool
	Reason string
	Class  string
}

// Guardian is the independent policy oracle consulted on every call.
type Guardian interface {
	Decide(ctx context.Context, scope contracts.Scope, req GuardRequest) (Decision, error)
}

// PolicySource is the DURABLE source of truth a Guardian consults for
// side-effecting tools. It must never be process memory: a side effect is
// permitted only on evidence that survives the process. Two independent,
// default-deny-by-absence signals are recognised:
//
//	(a) a HUMAN approval for the exact canonical digest, already stored in the
//	    shared approvals table (the same row Service.Approve binds -- there is
//	    deliberately no second approval gate); and
//	(b) an OPERATOR policy grant at (tenant, connection, tool) granularity.
//
// A nil PolicySource means "no durable evidence exists", so the Guardian keeps
// its default-deny posture for every side effect.
type PolicySource interface {
	// ApprovalExists reports a human-approved (approved_by IS NOT NULL) row for
	// the scope's tenant, the canonical digest and the tool.
	ApprovalExists(ctx context.Context, scope contracts.Scope, digest, tool string) (bool, error)
	// OperatorPolicyAllows reports an operator grant for (tenant, connection,
	// tool): a standing allow that does not require a per-call human approval.
	OperatorPolicyAllows(ctx context.Context, scope contracts.Scope, connection, tool string) (bool, error)
}

// PolicyGuardian is a deterministic Guardian: read-only tools are always
// allowed; side-effecting tools are default-deny unless an explicit allow is
// proven by either the in-process allow-list (kept for direct unit tests) or
// the durable PolicySource (production).
type PolicyGuardian struct {
	class  string
	allow  map[string]bool
	source PolicySource
}

// NewPolicyGuardian returns a Guardian with no durable source and an empty
// in-process allow-list. Callers that want durable policy must attach one with
// SetPolicySource (the tool Service wires this automatically for its own
// Guardian so the production face never trusts process memory alone).
func NewPolicyGuardian(class string) *PolicyGuardian {
	if class == "" {
		class = "policy"
	}
	return &PolicyGuardian{class: class, allow: map[string]bool{}}
}

// SetPolicySource attaches the durable policy source. Safe to call once at
// wiring time; a nil source keeps default-deny for side effects.
func (g *PolicyGuardian) SetPolicySource(src PolicySource) { g.source = src }

// AllowDigest explicitly permits one side-effecting canonical digest in this
// process. Intended for direct unit tests; production authorisation flows
// through the durable PolicySource instead.
func (g *PolicyGuardian) AllowDigest(digest string) {
	if g.allow == nil {
		g.allow = map[string]bool{}
	}
	g.allow[digest] = true
}

// DenyDigest removes an explicit allow-listing.
func (g *PolicyGuardian) DenyDigest(digest string) {
	delete(g.allow, digest)
}

// Decide applies the default-deny policy for side-effecting tools, consulting
// the durable source before denying. The in-process allow-list is honoured
// first so an explicitly wired test Guardian behaves as before.
func (g *PolicyGuardian) Decide(ctx context.Context, scope contracts.Scope, req GuardRequest) (Decision, error) {
	if !req.SideEffecting {
		return Decision{Allow: true, Reason: "read-only tool", Class: g.class}, nil
	}
	if g.allow[req.Digest] {
		return Decision{Allow: true, Reason: "side-effect explicitly allowed", Class: g.class}, nil
	}
	if g.source != nil {
		approved, err := g.source.ApprovalExists(ctx, scope, req.Digest, req.Tool)
		if err != nil {
			return Decision{}, err
		}
		if approved {
			return Decision{Allow: true, Reason: "human-approved canonical digest", Class: g.class}, nil
		}
		granted, err := g.source.OperatorPolicyAllows(ctx, scope, req.Connection, req.Tool)
		if err != nil {
			return Decision{}, err
		}
		if granted {
			return Decision{Allow: true, Reason: "operator policy grant (tenant,connection,tool)", Class: g.class}, nil
		}
	}
	return Decision{Allow: false, Reason: "side-effecting default deny (no approval or operator policy)", Class: g.class}, nil
}
