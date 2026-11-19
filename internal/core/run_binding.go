package core

import "context"

// RunBinding names the conversation and the run one tool call executes in.
// The PDF makes tenant, deployment, subject and context the support every
// record carries ("этого хватает для удаления, retention и удержания"), and
// deletion is a cascade the control plane performs by context: "удаление thread
// без хука агента". A tool that writes durable data therefore has to know which
// context it writes into, or the platform cannot clean it up afterwards.
//
// ContextID is the thread the turn runs in (State's thread id, which the PDF
// makes equal to the chat's contextId) and TaskID is the run that carries the
// turn. Both are recorded on whatever the tool produces; neither is an
// authorization input - the task scope in the context stays the isolation
// boundary.
//
// MaxSteps is the per-attempt step budget the CALLER stated for this run, and
// MaxStepsStated is the only way to tell "the caller named 64" from "nobody
// named anything" (vm2-a2a-multinode 20261101T115659Z §2 asked for exactly this
// pair, and for the flag rather than a bare int). A tool that delegates to a
// SECOND DEPLOYMENT has to say which number it is forwarding: the execution plane
// journalled step_budget_source precisely because "8 asked, 4 effective" is
// otherwise invisible after the fact.
//
// The loop's own fallback ceiling is deliberately NOT here. Runtime bounds a run
// with 64 steps when nothing was asked for (runtime.go); that is a fact about this
// life of this loop, not a caller's will, and forwarding it would make a child
// report the parent's process default as the user's request. An unstated budget
// forwards nothing, and the child's own deployment default answers it.
type RunBinding struct {
	ContextID    string `json:"context_id,omitempty"`
	TaskID       string `json:"task_id,omitempty"`
	OperationKey string `json:"operation_key,omitempty"`
	// MaxSteps is the stated per-attempt step budget; 0 when none was stated.
	MaxSteps int `json:"max_steps,omitempty"`
	// MaxStepsStated distinguishes an unstated budget from a stated one.
	MaxStepsStated bool `json:"max_steps_stated,omitempty"`
}

type runBindingKey struct{}

// WithRunBinding annotates ctx with the conversation and run of the current
// turn, alongside the run scope WithRunScope installs.
func WithRunBinding(ctx context.Context, b RunBinding) context.Context {
	return context.WithValue(ctx, runBindingKey{}, b)
}

// RunBindingFromContext returns the binding the runtime injected. The bool is
// false when the tool runs outside a turn, in which case the tool must not
// invent a context.
func RunBindingFromContext(ctx context.Context) (RunBinding, bool) {
	b, ok := ctx.Value(runBindingKey{}).(RunBinding)
	if !ok || (b.ContextID == "" && b.TaskID == "") {
		return RunBinding{}, false
	}
	return b, true
}
