package space

import (
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DefaultTaskTokenTTL bounds a minted delegated task-token's lifetime. It stays
// well under the State verifier's maxTaskTTL so a coordinator-minted token is
// always accepted downstream (execution /a2a/public, State) yet short-lived.
const DefaultTaskTokenTTL = 5 * time.Minute

// TaskTokenRequest describes a delegated task-token mint: the RunCoordinator,
// acting on an ALREADY-authenticated turn, asks for a short-lived KindTask token
// bound to that turn's full scope to present to a workload (the execution plane
// at /a2a/public). A human access token carries no Deployment and is therefore
// never valid on the execution plane; the coordinator (server-side, holding an
// INTERNAL task signer -- not the public human verification keys) is the only
// party allowed to mint this delegation, so a browser caller can never escalate
// into a task scope. See forum directive d71a7b0e (B).
type TaskTokenRequest struct {
	// Task is a fresh task id for this delegation; when empty the coordinator
	// generates one.
	Task string
	// Root is the root run id the delegation belongs to (required).
	Root string
	// Context is the turn's context/thread id (required).
	Context string
	// Grant is the capability grant identifier (required).
	Grant string
	// Depth is the delegation depth (>= 0).
	Depth int
	// TTL overrides DefaultTaskTokenTTL when > 0.
	TTL time.Duration
}

// MintTaskToken mints a short-lived KindTask task-token bound to the given full
// scope, for presentation to a delegated workload (execution /a2a/public).
//
// It FAILS CLOSED and never mints when: no task signer is configured (the
// deployment is not authorized to delegate), or the scope is incomplete (a
// missing Tenant/Deployment/Subject would yield a token that either escalates or
// is refused downstream). It never logs or returns token material in errors.
func (c *Coordinator) MintTaskToken(scope contracts.Scope, req TaskTokenRequest) (string, error) {
	if c.signer == nil {
		return "", contracts.NewError(contracts.ErrForbidden, "space: task-token minting is not configured for this deployment")
	}
	if scope.Tenant == "" || scope.Deployment == "" || scope.Subject == "" {
		return "", contracts.NewError(contracts.ErrInvalid, "space: task-token mint requires a complete scope (tenant+deployment+subject)")
	}
	if req.Root == "" || req.Context == "" || req.Grant == "" {
		return "", contracts.NewError(contracts.ErrInvalid, "space: task-token mint requires root+context+grant")
	}
	if req.Depth < 0 {
		return "", contracts.NewError(contracts.ErrInvalid, "space: task-token mint depth must be >= 0")
	}
	task := req.Task
	if task == "" {
		task = "task-" + contracts.NewID()
	}
	ttl := req.TTL
	if ttl <= 0 {
		ttl = DefaultTaskTokenTTL
	}
	tok, err := c.signer.Sign(auth.Claims{
		Audience:   c.audience,
		Kind:       auth.KindTask,
		Tenant:     scope.Tenant,
		Deployment: scope.Deployment,
		Subject:    scope.Subject,
		Task:       task,
		Root:       req.Root,
		Context:    req.Context,
		Grant:      req.Grant,
		Depth:      req.Depth,
	}, ttl)
	if err != nil {
		// signer errors are already log-safe; never attach scope detail.
		return "", err
	}
	return tok, nil
}
