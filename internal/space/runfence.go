package space

import "context"

// runCtxKey keys the per-run fence + task-token carried in context so a single shared
// durable sink can serve concurrent runs without ever placing scope/tenant/subject in
// a request body (those derive from the task-token principal at the State edge).
type runCtxKey struct{}

// RunContext carries the per-run fence and task-token for durable sinks that live
// outside the request itself (the Core engine stays a stateless pool; the sink reads
// the fence/token from context and forwards them to the State API client). It mirrors
// the fields the StateClient fence requires, nothing more.
type RunContext struct {
	Fence     Fence
	ThreadID  string
	TaskToken string
}

// WithRunContext returns a ctx carrying the per-run fence + task-token.
func WithRunContext(ctx context.Context, rc RunContext) context.Context {
	return context.WithValue(ctx, runCtxKey{}, rc)
}

// RunContextFrom recovers the per-run fence + task-token, reporting whether present.
func RunContextFrom(ctx context.Context) (RunContext, bool) {
	rc, ok := ctx.Value(runCtxKey{}).(RunContext)
	return rc, ok
}
