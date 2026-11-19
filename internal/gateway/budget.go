package gateway

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Step budget: who decides, and what gets told afterwards.
//
// Two numbers can bound one execution attempt: the step budget the CALLER asked
// for (the same `max_steps` the Space run API takes) and the per-attempt ceiling
// the OPERATOR configured for the deployment. Before this file the second one
// simply won on the A2A face, because the budget was never carried to the
// executor at all -- a caller who asked for 8 silently got the deployment
// default. That is the substitution this closes: a stated budget is validated at
// the ingress (a request that exceeds the operator's ceiling is refused with
// -32602, never quietly clamped), it travels to the executor on ExecInput, and
// whichever of the two actually bounded the attempt is journalled on the task.
//
// Resume semantics, stated because the code makes it true: the budget is PER
// ATTEMPT. core.Runtime counts steps from zero on every Run (internal/core
// /runtime.go), and a checkpoint restores the conversation, not the consumed
// budget. So a task resumed after N restarts may consume up to (N+1) times its
// per-attempt budget. That is not corrected here -- it is recorded, per attempt,
// in the task metadata and journal, so the operator can see it instead of
// guessing from a model that happened to stop early.
const (
	// MetadataStepBudget is the client-facing key for the requested per-attempt
	// step budget. It is deliberately the same JSON key the Space run API parses
	// (internal/space `max_steps`), so one client word means one thing.
	MetadataStepBudget = "max_steps"
	// MetadataStepBudgetSource records which number bounded the attempt: the
	// caller's or the deployment default. Without it, "8 requested, 4 effective"
	// is invisible after the fact.
	MetadataStepBudgetSource = "step_budget_source"
	// MetadataStepBudgetEffective is the bound that was actually in force for the
	// attempt, as the executor reported it. Kept separate from the requested
	// `max_steps`: with a deployment default in play the two differ, and an
	// operator tuning the budget needs the number that bound the run.
	MetadataStepBudgetEffective = "step_budget_effective"
	// MetadataStepsConsumed is how many steps the executor emitted for the last
	// attempt of this task.
	MetadataStepsConsumed = "steps_consumed"
	// MetadataCloseReason is why the loop stopped: step_limit, model_finished or
	// error. A `done` that stopped because the model declared itself finished is
	// a different fact from a `done` that ran out of budget, and the acceptance
	// rule "a run that finished without the artifact the user asked for is a
	// FAIL" cannot be checked without it.
	MetadataCloseReason = "close_reason"
	// MetadataAttempt is the attempt number these stats belong to.
	MetadataAttempt = "attempt"

	BudgetSourceCaller     = "caller"
	BudgetSourceDeployment = "deployment"

	CloseReasonStepLimit     = "step_limit"
	CloseReasonModelFinished = "model_finished"
	// CloseReasonAttemptCeiling marks a task the gateway gave up on: the effect
	// ledger still holds an unsettled earlier attempt (an executor crashed with a
	// side effect whose outcome nobody can prove) and the attempt ceiling is
	// reached. It is a lifecycle verdict about the task, NOT a claim about what
	// escaped to the outside world -- the ledger row stays SIDE_EFFECT_UNKNOWN.
	CloseReasonAttemptCeiling = "attempt_ceiling_exceeded"
	CloseReasonError          = "error"
	// CloseReasonProviderTransport marks a step lost to the network rather than to
	// the model: TLS handshake timeout, reset connection, i/o timeout. It is recorded
	// apart from CloseReasonError because the operator's next action differs -- node or
	// provider health versus prompt or policy -- and because a leg that reads "the loop
	// stopped" as "this plane has no snapshot writer" blames the wrong component. A
	// transport-class stop is NOT evidence about the model and NOT evidence about the
	// step budget.
	CloseReasonProviderTransport = "provider_transport"
	// CloseReasonGrantRevoked marks an attempt that the authority plane refused because the
	// grant it ran under -- or an ancestor of that grant -- has been revoked. It is recorded
	// apart from CloseReasonError because the two need opposite operator actions (restore or
	// re-issue authority, versus look at the prompt or the model), and because a delegated
	// child is the case where revocation actually lands: the parent's grant is revoked while
	// the child is mid-flight, so every remaining attempt is refused for a reason that has
	// nothing to do with the work. The spelling follows the word identity itself renders
	// ("identity: grant is revoked") and the code the memory plane already returns
	// ("grant_revoked"), so no new synonym enters the dictionary.
	CloseReasonGrantRevoked = "grant_revoked"
	// CloseReasonAuthorityRefused marks an attempt refused by the authority plane for a
	// reason other than revocation: the grant does not hold a required capability, or the
	// presented identity could not be authenticated at all. It sits between revoked and a
	// plain error on purpose -- "nobody may do this" and "this request was not believed" are
	// neither a model failure nor a revoked grant, and guessing between them from prose is
	// how a scope bug gets filed against the model team.
	CloseReasonAuthorityRefused = "authority_refused"
	// CloseReasonCanceled is why a task ended CANCELED through the normal path: a cancel was
	// journaled and the lease holder honoured it. The spelling is the platform's existing word
	// for the fact (state.RunCanceled, space.CloseReasonCanceled = "canceled"), so the two ends
	// of an A->B chain still compare the same word against the same word.
	//
	// internal/space/close_reason.go records that the execution plane used to reach this state
	// only through a lease and a subtree, never through this key. DEF-J4-CHILD-TERMINAL-REASON
	// is the decision to state the reason anyway: the requirement is that a terminal
	// transition owes a reason in the FIELD, and prose in status_message is not comparable.
	// Flagged rather than slipped in, because it changes an observation another package
	// documented.
	CloseReasonCanceled = "canceled"
	// CloseReasonSubjectDeleted and CloseReasonTenantDeleted name the two cancellations this
	// plane performs on its own initiative, in one statement, with no worker and no executor
	// involved: a subject or tenant was erased and its in-flight tasks were cut off. They
	// reuse the codes the memory plane already returns for the same facts, and the reason must
	// be written by the same statement that moves the row -- a transition that leaves the
	// journal by a different door than the service does is exactly the door a per-code-path
	// rule misses.
	CloseReasonSubjectDeleted = "subject_deleted"
	CloseReasonTenantDeleted  = "tenant_deleted"
	// CloseReasonUnreported says the honest thing about a terminal transition whose executor
	// filed no report: the task ended, and nothing in this plane knows why in the execution
	// vocabulary. It exists so the field is never ABSENT -- absence is what made the
	// requirement unverifiable -- and so that filling the gap does not require inventing a
	// model verdict, a step count or a budget that nobody measured. A reason word costs
	// nothing; a fabricated number costs an acceptance rule.
	CloseReasonUnreported = "executor_unreported"
)

// stepBudget is the resolved per-attempt bound plus who set it.
type stepBudget struct {
	Steps  int
	Source string
}

// budgetFromMetadata reads one `max_steps` value out of a metadata map. It is
// strict on purpose: a budget is a bound, and a value that cannot be read as a
// positive integer is not a bound the plane may quietly reinterpret.
func budgetFromMetadata(md map[string]any) (int, bool, error) {
	raw, ok := md[MetadataStepBudget]
	if !ok || raw == nil {
		return 0, false, nil
	}
	switch v := raw.(type) {
	case float64:
		if v != math.Trunc(v) || v < 1 || v > math.MaxInt32 {
			return 0, true, contracts.NewReasoned(contracts.ErrInvalid, ReasonBudgetInvalid,
				"gateway: metadata.max_steps must be a positive whole number of steps")
		}
		return int(v), true, nil
	case int:
		if v < 1 {
			return 0, true, contracts.NewReasoned(contracts.ErrInvalid, ReasonBudgetInvalid,
				"gateway: metadata.max_steps must be a positive whole number of steps")
		}
		return v, true, nil
	case int64:
		if v < 1 || v > math.MaxInt32 {
			return 0, true, contracts.NewReasoned(contracts.ErrInvalid, ReasonBudgetInvalid,
				"gateway: metadata.max_steps must be a positive whole number of steps")
		}
		return int(v), true, nil
	case json.Number:
		n, err := v.Int64()
		if err != nil || n < 1 || n > math.MaxInt32 {
			return 0, true, contracts.NewReasoned(contracts.ErrInvalid, ReasonBudgetInvalid,
				"gateway: metadata.max_steps must be a positive whole number of steps")
		}
		return int(n), true, nil
	default:
		return 0, true, contracts.NewReasoned(contracts.ErrInvalid, ReasonBudgetInvalid,
			"gateway: metadata.max_steps must be a positive whole number of steps")
	}
}

// resolveStepBudget decides the per-attempt bound for a new task. A stated
// budget that exceeds the operator's ceiling is an outright client error -- the
// caller believes it asked for 8 and the deployment would give 4, and the only
// honest answers are "yes" or "no", not a silent 4. When no ceiling is
// configured the caller's number stands as given. Absent a stated budget, the
// deployment number applies and is labelled as such.
func resolveStepBudget(requestLevel, messageLevel map[string]any, ceiling int) (stepBudget, error) {
	reqSteps, reqOK, err := budgetFromMetadata(requestLevel)
	if err != nil {
		return stepBudget{}, err
	}
	msgSteps, msgOK, err := budgetFromMetadata(messageLevel)
	if err != nil {
		return stepBudget{}, err
	}
	if reqOK && msgOK && reqSteps != msgSteps {
		return stepBudget{}, contracts.NewReasoned(contracts.ErrInvalid, ReasonBudgetConflict,
			"gateway: max_steps is set on both the request and the message with different values")
	}
	stated, ok := reqSteps, reqOK
	if !ok {
		stated, ok = msgSteps, msgOK
	}
	if !ok {
		return stepBudget{Steps: 0, Source: BudgetSourceDeployment}, nil
	}
	if ceiling > 0 && stated > ceiling {
		return stepBudget{}, contracts.NewReasoned(contracts.ErrInvalid, ReasonBudgetCeiling,
			"gateway: max_steps "+strconv.Itoa(stated)+" exceeds the deployment per-attempt ceiling of "+
				strconv.Itoa(ceiling)+" steps")
	}
	return stepBudget{Steps: stated, Source: BudgetSourceCaller}, nil
}

// mergeAttemptStats returns the metadata patch describing what the last attempt
// actually did. Executor stats are written only when the executor reported them:
// metadata without a report stays metadata, never a fabricated zero. `source`
// says whose number bounded the attempt, derived by the caller of this function
// from the task's own metadata rather than from the executor's memory.
func mergeAttemptStats(res ExecResult, source string, attempt int) map[string]any {
	if res.Stats.CloseReason == "" {
		return nil
	}
	out := map[string]any{
		MetadataStepsConsumed: res.Stats.StepsConsumed,
		MetadataCloseReason:   strings.TrimSpace(res.Stats.CloseReason),
		MetadataAttempt:       attempt,
	}
	if res.Stats.StepBudget > 0 {
		out[MetadataStepBudgetEffective] = res.Stats.StepBudget
	}
	if source != "" {
		out[MetadataStepBudgetSource] = source
	}
	return out
}

// budgetSourceOf reports whose number was supposed to bound this task: the
// caller's, if the ingress recorded one, otherwise the deployment default.
func budgetSourceOf(md map[string]any) string {
	if md != nil && md[MetadataStepBudgetSource] == BudgetSourceCaller {
		return BudgetSourceCaller
	}
	return BudgetSourceDeployment
}

// ClassifyCloseReason names, out loud, why an execution loop stopped. Three
// genuinely different facts used to look identical from outside a task: the loop
// ran out of steps, the model decided it was finished, or the turn failed. An
// acceptance rule that treats "finished without the artifact the user explicitly
// asked for" as a FAIL cannot be applied at all without this distinction, and a
// step budget cannot be tuned by evidence without knowing whether the previous
// run was bound by the budget or by the model stopping early.
//
// consumed counts the model turns the loop emitted for this attempt and budget is
// the bound that was in force. Equal counts with no explicit limit error is still
// reported as the budget: the bound, not the model, ended that run.
func ClassifyCloseReason(runErr error, errText string, sawError bool, finalText string, consumed, budget int) string {
	joined := errText
	if runErr != nil {
		joined += " " + runErr.Error()
	}
	if strings.Contains(joined, "step limit") {
		return CloseReasonStepLimit
	}
	if IsTransportFailure(joined) {
		return CloseReasonProviderTransport
	}
	if runErr != nil || sawError {
		return CloseReasonError
	}
	if strings.TrimSpace(finalText) == "" {
		return CloseReasonError
	}
	if budget > 0 && consumed >= budget {
		return CloseReasonStepLimit
	}
	return CloseReasonModelFinished
}

// IsTransportFailure reports the transport-class failure markers this plane can
// distinguish from a model refusal: TLS handshake timeout, connection
// reset/refused, i/o timeout, unexpected EOF. It matches on the rendered error
// text because the transport failure arrives wrapped by whatever layer observed
// it (provider client, net/http, the Model Gateway), and the wrapped text is the
// only thing that survives into the journalled step.
//
// This is a classifier, not a retry policy. Retrying a generation belongs to the
// layer that owns the provider call and its effect key: by the time a generation
// is terminal here, the Model Gateway has settled the reservation and Execute
// will not re-dispatch it (internal/model/service.go), so a retry from this layer
// would either do nothing at all or spend the task's budget a second time.
func IsTransportFailure(text string) bool {
	lower := strings.ToLower(text)
	for _, marker := range []string{
		"tls handshake timeout",
		"connection reset",
		"connection refused",
		"i/o timeout",
		"unexpected eof",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

// IsGrantRevoked reports the revocation markers this plane can read on a refusal. The
// authority plane renders revocation as text ("identity: grant is revoked", "identity: an
// ancestor grant is revoked") and the layer that saw it wraps that sentence in its own
// prefix, so -- exactly as with IsTransportFailure -- the rendered text is the only thing
// that survives into the journalled attempt.
//
// Revocation is checked before the wider authority class because it is the fact an operator
// acts on alone: nothing is wrong with the task or the model, somebody took the authority
// away, and in a delegated chain that somebody is upstream of the workload that is failing.
func IsGrantRevoked(text string) bool {
	lower := strings.ToLower(text)
	for _, marker := range []string{
		"grant is revoked",
		"ancestor grant is revoked",
		"grant revoked",
		"grant_revoked",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

// IsAuthorityFailure reports that a refusal came from the authority plane rather than from
// the model, the step budget or the network: the code is FORBIDDEN, or the rendered text
// carries the shape of an authority verdict (a refusal this plane cannot satisfy by working
// harder -- an identity that was not believed, a grant that does not hold the capability the
// attempt needed, a subject that has been erased).
func IsAuthorityFailure(err error, text string) bool {
	if err != nil && contracts.Code(err) == contracts.ErrForbidden {
		return true
	}
	lower := strings.ToLower(text)
	for _, marker := range []string{
		"forbidden:",
		"unauthenticated",
		"does not hold capability",
		"identity: grant",
		"subject deleted",
		"tenant deleted",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

// ClassifyFailureCause names, in the execution dictionary, why an attempt failed when the
// executor filed no report of its own. This is the classifier behind the transitions that
// have nothing but a refusal sentence to go on: an attempt refused until the attempts budget
// ran out, and a failure replayed out of the effect ledger.
//
// The precedence is deliberate and is the reason this function exists as one place: budget
// first, because a step limit is a decision this plane took; then transport, because a lost
// network is not evidence about the model; then revocation, then the wider authority class,
// because both mean "no amount of retrying helps" and must never be swallowed by the generic
// word; then a plain error; and finally the honest statement that nothing was reported.
// Returning CloseReasonError for everything would satisfy a "field is non-empty" rule and
// verify nothing, so the classes are kept apart here rather than at each call site.
func ClassifyFailureCause(err error, extra ...string) string {
	joined := strings.TrimSpace(strings.Join(extra, " "))
	if err != nil {
		if joined != "" {
			joined = err.Error() + " " + joined
		} else {
			joined = err.Error()
		}
	}
	if joined == "" {
		return CloseReasonUnreported
	}
	switch {
	case strings.Contains(joined, "step limit"):
		return CloseReasonStepLimit
	case IsTransportFailure(joined):
		return CloseReasonProviderTransport
	case IsGrantRevoked(joined):
		return CloseReasonGrantRevoked
	case IsAuthorityFailure(err, joined):
		return CloseReasonAuthorityRefused
	default:
		return CloseReasonError
	}
}
