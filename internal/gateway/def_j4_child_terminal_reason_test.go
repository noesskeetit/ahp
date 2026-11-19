package gateway_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DEF-J4-CHILD-TERMINAL-REASON (P3).
//
// Measured on the canonical A->B stand (inst-11100503171dbd1e, product 4b99a287), read out
// of the live journal rather than inferred:
//
//	task 01a1c3e2-0ced-79cf-8107-e07921fe79a6, parent_task rtask-dot-owner:a2b-revoke-051532
//	state TASK_STATE_FAILED, attempts 3
//	status_message "task failed: execbridge: task budget: FORBIDDEN: identity: grant is revoked"
//	metadata keys: ahp.depth, ahp.grant_*, ahp.parent_attested*, ahp.root, ahp_delegation_ref,
//	               ahp_parent_task, max_steps, step_budget_source     <-- NO close_reason
//
// The sibling that FAILED through the executor's own report does carry one (close_reason
// =error), and every COMPLETED carries model_finished. So the PDF requirement "a terminal
// transition owes a reason" was met here by the prose of status_message and not by the
// reason field -- which is exactly what makes a leg that reads the field (A6b on the B
// plane) impossible to satisfy.
//
// What this file proves, and nothing more:
//   - every terminal transition this plane can perform journals a non-empty close_reason:
//     failure by exhausted attempts, failure replayed from the effect ledger, cancellation,
//     and the two fence-driven bulk cancellations included;
//   - the reason separates the classes an operator acts on differently -- an authority
//     revocation is not an attempt ceiling is not a model/policy refusal is not a transport
//     loss. Prose cannot be compared, so the field has to carry the difference;
//   - the verbatim cause stays in status_message: a reason word is a classification, never a
//     replacement of the sentence that refused.
//
// These legs deliberately do not name the dictionary's spellings: they assert the invariant
// and that the classes are pairwise distinguishable, so they fail on the defect itself
// rather than on a constant the fix has not introduced yet. A separate leg pins spellings.

// The error texts below are copied in shape from what this plane actually renders: each is
// wrapped by the layer that observed it, which is why a classifier has to read the rendered
// text and not only the contract code (the same reason IsTransportFailure exists).
var (
	revokedGrantCause = errors.New("execbridge: task budget: FORBIDDEN: identity: grant is revoked")
	capabilityRefusal = errors.New("execbridge: model turn failed: model generate step 0: FORBIDDEN: identity: grant does not hold capability model")
	modelErrorCause   = errors.New("execbridge: model turn failed: model generate step 0: INVALID_ARGUMENT: prompt rejected by policy")
	transportCause    = errors.New("execbridge: model turn failed: Post \"https://model.internal/v1/chat\": dial tcp: connect: connection refused")
)

// terminalStatesSQL is the terminal half of this plane's state set, spelled as the journal
// stores it.
const terminalStatesSQL = "'TASK_STATE_COMPLETED','TASK_STATE_FAILED','TASK_STATE_CANCELED','TASK_STATE_REJECTED'"

func journalReason(t *testing.T, e *env, scope contracts.Scope, taskID string) string {
	t.Helper()
	row := journalRow(t, e, scope, taskID)
	reason, _ := row.Metadata[gateway.MetadataCloseReason].(string)
	return reason
}

func journalReasonIs(t *testing.T, e *env, scope contracts.Scope, taskID string) bool {
	t.Helper()
	return journalReason(t, e, scope, taskID) != ""
}

func journalRow(t *testing.T, e *env, scope contracts.Scope, taskID string) gateway.TaskRow {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	row, err := e.store.Task(ctx, scope, taskID)
	if err != nil {
		t.Fatalf("Task(%s): %v", taskID, err)
	}
	return row
}

func statusText(t *testing.T, e *env, scope contracts.Scope, taskID string) string {
	t.Helper()
	row := journalRow(t, e, scope, taskID)
	if row.StatusMessage == nil || len(row.StatusMessage.Parts) == 0 {
		return ""
	}
	return row.StatusMessage.Parts[0].Text()
}

// runUntilTerminal drives one service until the task stops being claimable.
func runUntilTerminal(t *testing.T, svc *gateway.Service, e *env, scope contracts.Scope, taskID, why string) {
	t.Helper()
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go svc.Run(runCtx)
	waitFor(t, 40*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		row, err := e.store.Task(ctx, scope, taskID)
		return err == nil && row.Terminal()
	}, why)
}

// failingExecutor refuses every attempt with the same cause: attempts are spent until the
// gateway owes a terminal verdict, which is the path the stand took.
func failingExecutor(err error) gateway.Executor {
	return gateway.ExecutorFunc(func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		return gateway.ExecResult{}, err
	})
}

// ceilingExecutor dies with an unprovable effect, so the guard refuses every later attempt
// and the task ends by the attempt ceiling (the DEF-H22-1 path).
func ceilingExecutor() gateway.Executor {
	return gateway.ExecutorFunc(func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		return gateway.ExecResult{}, gateway.ErrAmbiguous
	})
}

func ceilingService(t *testing.T, e *env) *gateway.Service {
	return e.service(t, ceilingExecutor(), func(cfg *gateway.Config) {
		cfg.MaxAttempts = 2
		cfg.ParkLease = 5 * time.Millisecond
	})
}

func metadataKeys(md map[string]any) []string {
	out := make([]string, 0, len(md))
	for k := range md {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestDefJ4ExhaustedAttemptRecordsWhyItFailed is the stand's case in miniature: the grant a
// child ran under is revoked, every attempt is refused, attempts run out, the task ends
// FAILED. The verdict must name the reason in the field, not only in the sentence.
func TestDefJ4ExhaustedAttemptRecordsWhyItFailed(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, failingExecutor(revokedGrantCause), func(cfg *gateway.Config) { cfg.MaxAttempts = 1 })
	task := mustSubmit(t, svc, e.scope, e.caller, "msg-def-j4-revoked", nil)
	runUntilTerminal(t, svc, e, e.scope, string(task.ID),
		"the task never reached a terminal state: attempts refused until the budget is spent must end FAILED, not stay claimable")

	row := journalRow(t, e, e.scope, string(task.ID))
	if row.State != a2a.TaskStateFailed {
		t.Fatalf("state = %q, want FAILED", row.State)
	}
	if reason := journalReason(t, e, e.scope, string(task.ID)); reason == "" {
		t.Fatalf("%s is absent from metadata (keys %v) while the task is FAILED: this is DEF-J4-CHILD-TERMINAL-REASON -- the reason lives only in status_message %q, so a leg that reads the field cannot be satisfied",
			gateway.MetadataCloseReason, metadataKeys(row.Metadata), statusText(t, e, e.scope, string(task.ID)))
	}
	// The classification must not swallow the sentence: an operator needs the refusing
	// plane's own words, and the requirement asks for a reason IN ADDITION to them.
	if text := statusText(t, e, e.scope, string(task.ID)); text == "" {
		t.Fatal("status_message is empty: a close_reason word must not replace the verbatim cause")
	}
}

// TestDefJ4EveryTerminalTransitionOwesAReason walks every terminal transition this plane
// performs and asks the field rather than the prose. The aggregate leg at the end asks the
// journal itself, so a transition this file did not think of cannot hide.
func TestDefJ4EveryTerminalTransitionOwesAReason(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	type outcome struct{ what, id string }
	var done []outcome

	// 1. COMPLETED with an executor report -- the case that already worked, kept honest so a
	// fix cannot "solve" the defect by dropping this one.
	reported := e.service(t, gateway.ExecutorFunc(func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		return gateway.ExecResult{
			State:   a2a.TaskStateCompleted,
			Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("here you go")),
			Stats:   gateway.ExecStats{CloseReason: gateway.CloseReasonModelFinished, StepsConsumed: 2, StepBudget: 8},
		}, nil
	}), nil)
	reportedTask := mustSubmit(t, reported, e.scope, e.caller, "msg-def-j4-done-reported", nil)
	runUntilTerminal(t, reported, e, e.scope, string(reportedTask.ID), "a reporting executor never committed its result")
	done = append(done, outcome{"completed with an executor report", string(reportedTask.ID)})

	// 2. COMPLETED with no report at all (a hand-rolled ExecResult, or a ledger row written
	// before stats existed). The plane still owes SOMETHING in the field, and must not
	// invent model or budget numbers to fill it.
	unreported := e.service(t, gateway.ExecutorFunc(func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		return gateway.ExecResult{}, nil
	}), nil)
	unreportedTask := mustSubmit(t, unreported, e.scope, e.caller, "msg-def-j4-done-unreported", nil)
	runUntilTerminal(t, unreported, e, e.scope, string(unreportedTask.ID), "an executor that files no report never committed its result")
	unreportedRow := journalRow(t, e, e.scope, string(unreportedTask.ID))
	done = append(done, outcome{"completed without an executor report", string(unreportedTask.ID)})
	if _, invented := unreportedRow.Metadata[gateway.MetadataStepsConsumed]; invented {
		t.Fatalf("steps_consumed appeared on a task whose executor reported nothing (metadata %v): a reason must not be paid for with a fabricated number", metadataKeys(unreportedRow.Metadata))
	}

	// 3. FAILED by exhausted attempts, one task per cause class. On the canon none of these
	// wrote the field at all.
	for _, c := range []struct{ msg, what string }{
		{"msg-def-j4-fail-revoked", "authority revoked"},
		{"msg-def-j4-fail-capability", "authority refused"},
		{"msg-def-j4-fail-model", "model/policy refusal"},
		{"msg-def-j4-fail-transport", "provider transport"},
	} {
		svc := e.service(t, failingExecutor(errorFor(c.what)), func(cfg *gateway.Config) { cfg.MaxAttempts = 1 })
		task := mustSubmit(t, svc, e.scope, e.caller, c.msg, nil)
		runUntilTerminal(t, svc, e, e.scope, string(task.ID),
			"an attempt refused until the budget is spent never reached a terminal verdict ("+c.what+")")
		done = append(done, outcome{"failed by exhausted attempts: " + c.what, string(task.ID)})
	}

	// 4. FAILED by the attempt ceiling with an unsettled effect.
	ceiling := ceilingService(t, e)
	ceilingTask := mustSubmit(t, ceiling, e.scope, e.caller, "msg-def-j4-ceiling", nil)
	runUntilTerminal(t, ceiling, e, e.scope, string(ceilingTask.ID), "an unrecoverable task never reached its bounded terminal verdict")
	done = append(done, outcome{"failed by attempt ceiling", string(ceilingTask.ID)})

	// 5. CANCELED, honoured by the lease holder.
	cancelSvc := e.service(t, echoExecutor(), nil)
	cancelTask := mustSubmit(t, cancelSvc, e.scope, e.caller, "msg-def-j4-cancel", nil)
	if _, err := e.store.RequestCancel(ctx, e.scope, string(cancelTask.ID)); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	runUntilTerminal(t, cancelSvc, e, e.scope, string(cancelTask.ID), "a cancelled task never reached CANCELED")
	done = append(done, outcome{"cancelled by request", string(cancelTask.ID)})

	for _, o := range done {
		if !journalReasonIs(t, e, e.scope, o.id) {
			t.Fatalf("%s: %s is absent or empty on a terminal task -- the transition owed a reason and stated none",
				o.what, gateway.MetadataCloseReason)
		}
	}

	// 6. The fence-driven bulk cancellations (subject and tenant erasure): one statement
	// moves rows straight to CANCELED with no service around them to say why.
	fenceSvc := e.service(t, echoExecutor(), nil)
	fenced := map[string]string{}
	subjectScope := e.scope
	subjectScope.Subject = e.scope.Subject + "-fence"
	tenantScope := e.scope
	tenantScope.Tenant = e.scope.Tenant + "-fence"
	subjectTask := mustSubmit(t, fenceSvc, subjectScope, e.caller, "msg-def-j4-subject-fence", nil)
	tenantTask := mustSubmit(t, fenceSvc, tenantScope, e.caller, "msg-def-j4-tenant-fence", nil)
	if err := e.store.FenceSubject(ctx, subjectScope.Tenant, subjectScope.Subject); err != nil {
		t.Fatalf("FenceSubject: %v", err)
	}
	if err := e.store.FenceTenant(ctx, tenantScope.Tenant); err != nil {
		t.Fatalf("FenceTenant: %v", err)
	}
	for _, pair := range []struct {
		what  string
		scope contracts.Scope
		id    string
	}{
		{"cancelled by a subject fence", subjectScope, string(subjectTask.ID)},
		{"cancelled by a tenant fence", tenantScope, string(tenantTask.ID)},
	} {
		row := journalRow(t, e, pair.scope, pair.id)
		if row.State != a2a.TaskStateCanceled {
			t.Fatalf("%s: state = %q, want CANCELED -- a fence must end the task it cut off", pair.what, row.State)
		}
		reason := journalReason(t, e, pair.scope, pair.id)
		if reason == "" {
			t.Fatalf("%s: %s is absent on a task this plane cancelled out from under its owner -- the fence IS the reason and it was not stated",
				pair.what, gateway.MetadataCloseReason)
		}
		fenced[pair.what] = reason
	}
	if fenced["cancelled by a subject fence"] == fenced["cancelled by a tenant fence"] {
		t.Fatalf("both erasure fences journal %q: one deleted a subject and the other a whole tenant, and an operator reading the row cannot tell which cleanup cut the task off",
			fenced["cancelled by a subject fence"])
	}

	// 7. The rule, asked of the journal: no terminal row in this schema carries a missing or
	// empty reason. A transition nobody thought to test still reddens here.
	var offenders int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM `+e.schema+`.tasks
		WHERE state IN (`+terminalStatesSQL+`)
		  AND coalesce(metadata->>'`+gateway.MetadataCloseReason+`','') = ''`).Scan(&offenders); err != nil {
		t.Fatalf("aggregate query: %v", err)
	}
	if offenders != 0 {
		t.Fatalf("%d terminal task(s) in this schema journal no %s: the rule is per-transition, not per-code-path",
			offenders, gateway.MetadataCloseReason)
	}
}

// TestDefJ4FailureClassesAreDistinguishable is the half the requirement actually needs: a
// reason that is the same word for every failure verifies nothing. Each class is reached by
// a genuinely different cause, so a classifier that collapses them reddens here.
func TestDefJ4FailureClassesAreDistinguishable(t *testing.T) {
	e := newEnv(t)
	classes := []struct{ what, msg string }{
		{"authority revoked", "msg-def-j4-cls-revoked"},
		{"authority refused", "msg-def-j4-cls-capability"},
		{"model/policy refusal", "msg-def-j4-cls-model"},
		{"provider transport", "msg-def-j4-cls-transport"},
	}
	got := map[string]string{}
	for _, c := range classes {
		svc := e.service(t, failingExecutor(errorFor(c.what)), func(cfg *gateway.Config) { cfg.MaxAttempts = 1 })
		task := mustSubmit(t, svc, e.scope, e.caller, c.msg, nil)
		runUntilTerminal(t, svc, e, e.scope, string(task.ID), "class "+c.what+" never reached a terminal verdict")
		got[c.what] = journalReason(t, e, e.scope, string(task.ID))
	}
	// The ceiling class arrives by a different mechanism (an unsettled effect) and already
	// writes a reason; it is here so that collapsing everything into one word cannot pass.
	ceiling := ceilingService(t, e)
	ceilingTask := mustSubmit(t, ceiling, e.scope, e.caller, "msg-def-j4-cls-ceiling", nil)
	runUntilTerminal(t, ceiling, e, e.scope, string(ceilingTask.ID), "the ceiling class never reached a terminal verdict")
	got["attempt ceiling"] = journalReason(t, e, e.scope, string(ceilingTask.ID))

	seen := map[string]string{}
	for _, what := range []string{"authority revoked", "authority refused", "model/policy refusal",
		"provider transport", "attempt ceiling"} {
		reason := got[what]
		if reason == "" {
			t.Fatalf("class %q journaled no %s: distinctness cannot be claimed over absent values",
				what, gateway.MetadataCloseReason)
		}
		if other, clash := seen[reason]; clash {
			t.Fatalf("classes %q and %q both journal %q: the field states nothing an operator could act on differently",
				other, what, reason)
		}
		seen[reason] = what
	}

	// A class must also be stable: the same cause twice is the same word, or the field is a
	// counter in disguise.
	again := e.service(t, failingExecutor(revokedGrantCause), func(cfg *gateway.Config) { cfg.MaxAttempts = 1 })
	second := mustSubmit(t, again, e.scope, e.caller, "msg-def-j4-cls-revoked-again", nil)
	runUntilTerminal(t, again, e, e.scope, string(second.ID), "a repeated revoked-class run never reached a terminal verdict")
	if r := journalReason(t, e, e.scope, string(second.ID)); r != got["authority revoked"] {
		t.Fatalf("the same cause journaled %q and then %q: a classification that moves between runs classifies nothing",
			got["authority revoked"], r)
	}
}

// TestDefJ4ReasonSurvivesAnAttemptAlreadySettledInTheLedger documents the crash-shaped twin of
// the defect and, honestly, its limits. failAttempt settles the attempt in the effect ledger
// OUTSIDE the transaction that writes the task journal, so a crash between the two leaves a
// settled failure and no terminal row.
//
// What was measured here, rather than assumed: the next claim does NOT settle from that row.
// operations.Accept refuses a key it has already finalised ("operation key is sealed and
// permanently rejects new requests"), the worker stands down with CONFLICT, and a later claim
// dispatches under the NEXT attempt key. The StatusSucceeded/StatusFailed arms of process() are
// therefore unreachable while the ledger seals settled keys, and no leg that drives a service
// can reach them. So this leg proves the two reachable facts: the settled row keeps the cause
// the replay door would read, and the seal actually holds (an already-finished attempt is not
// quietly re-opened). The mapping from that payload to a reason word is pinned by
// TestTerminalReasonClassification, which tests the classifier directly instead of pretending
// to drive a door that cannot be opened.
func TestDefJ4ReasonSurvivesAnAttemptAlreadySettledInTheLedger(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	svc := e.service(t, failingExecutor(revokedGrantCause), func(cfg *gateway.Config) { cfg.MaxAttempts = 1 })
	task := mustSubmit(t, svc, e.scope, e.caller, "msg-def-j4-replay", nil)
	seeded := journalRow(t, e, e.scope, string(task.ID))

	opKey := "exec:" + seeded.ID + ":1"
	if _, err := e.ledger.Accept(ctx, e.scope, operations.Request{
		Key: opKey, Kind: "task:" + seeded.Kind, Target: seeded.ID, Guard: seeded.Guard(),
	}); err != nil {
		t.Fatalf("ledger Accept: %v", err)
	}
	if _, err := e.ledger.Dispatch(ctx, e.scope, opKey); err != nil {
		t.Fatalf("ledger Dispatch: %v", err)
	}
	cause, err := json.Marshal(map[string]string{"error": revokedGrantCause.Error()})
	if err != nil {
		t.Fatalf("marshal cause: %v", err)
	}
	if _, err := e.ledger.Complete(ctx, e.scope, opKey, cause, true); err != nil {
		t.Fatalf("ledger Complete: %v", err)
	}

	op, err := e.ledger.Get(ctx, e.scope, opKey)
	if err != nil {
		t.Fatalf("ledger Get: %v", err)
	}
	// The journal canonicalises JSON, so the cause is compared as a value: what matters is
	// that the sentence is still readable out of the settled row, not its byte spacing.
	var stored struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(op.Result, &stored); err != nil {
		t.Fatalf("the settled attempt's result is not readable (%s): %v", string(op.Result), err)
	}
	if stored.Error != revokedGrantCause.Error() {
		t.Fatalf("the settled attempt lost its cause (error %q, want %q): nothing downstream can name a reason that was never kept", stored.Error, revokedGrantCause.Error())
	}
	if _, err := e.ledger.Accept(ctx, e.scope, operations.Request{
		Key: opKey, Kind: "task:" + seeded.Kind, Target: seeded.ID, Guard: seeded.Guard(),
	}); err == nil {
		t.Fatal("Accept succeeded for an attempt the ledger already settled: a sealed key must permanently reject new requests, or the same effect is dispatched twice")
	}
}

// errorFor maps a cause class to the error text this plane would render for it.
func errorFor(class string) error {
	switch class {
	case "authority revoked":
		return revokedGrantCause
	case "authority refused":
		return capabilityRefusal
	case "model/policy refusal":
		return modelErrorCause
	case "provider transport":
		return transportCause
	}
	panic("test asks for an unknown cause class: " + class)
}
