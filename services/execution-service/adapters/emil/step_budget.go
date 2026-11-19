package emil

// MetaStepBudget is the A2A SendMessage metadata key the AHP execution plane
// reads as "the per-attempt step bound the CALLER is asking for". The name is
// the execution plane's wire contract (internal/gateway/budget.go
// MetadataStepBudget = "max_steps"); it is repeated here because this package is
// the producer, exactly as MetaParentTask is.
//
// The contract this key exists to honour: a stated budget is answered with
// either exactly that number or a refusal (-32602) naming the operator's
// ceiling -- never with a quieter number than the one that was asked for
// (internal/gateway/budget.go resolveStepBudget). That is a promise about the
// CHILD's face and it is unchanged: whatever number arrives is honoured exactly.
// What a delegating run STATES is a separate decision, and with a budget tree
// mounted (RunAuthority.Budget) the honest number is no longer the parent's own
// bound but the parent's REMAINDER -- see internal/treedelegate. Saying nothing is also an
// honest answer: the child then runs on its deployment default and journals
// step_budget_source=deployment.
//
// This file used to hold the lane's own context stamp (WithRunStepBudget /
// RunStepBudgetFromContext), which existed only because the step budget of a run
// was not reachable from a tool: internal/core carried it on space.CoreRequest
// and stamped nothing into the tool context. vm2-state-runner a44dd6d put the
// number and its statedness on the binding the runtime already injects
// (internal/core/run_binding.go RunBinding.MaxSteps / MaxStepsStated), which is
// the seam this file predicted, so the stamp and its host wrapper are gone and
// the host fills RunAuthority.MaxSteps from the binding instead.
const MetaStepBudget = "max_steps"
