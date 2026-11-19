package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// ErrAmbiguous is returned by an Executor when a side effect may or may not
// have happened (a transport failure after the request left the process). The
// worker records SIDE_EFFECT_UNKNOWN and releases the lease instead of
// replaying, so an external effect is never duplicated by accident.
var ErrAmbiguous = errors.New("gateway: ambiguous side effect")

// ExecInput is what an executor receives for one fenced attempt.
type ExecInput struct {
	Scope        contracts.Scope
	Task         TaskRow
	FencingToken int64
	Input        *a2a.Message // last inbound user message, may be nil on resume
	// MaxSteps is the per-attempt step budget the CALLER stated for this task
	// (validated at the ingress against the operator's ceiling), or 0 when the
	// caller stated none. It travels here rather than being resolved inside the
	// executor, because on this face the deployment default used to win by
	// default: a caller that asked for 8 steps silently got the deployment's 4.
	// An executor applies its own configured bound only when this is 0, and must
	// say which of the two bounded the run (ExecResult.Stats).
	MaxSteps int
}

// ExecResult is the outcome of one attempt. State must be terminal or an
// input/auth required pause; Working and Submitted are rejected.
type ExecResult struct {
	State     a2a.TaskState
	Message   *a2a.Message
	Artifacts []*a2a.Artifact
	// Stats reports what the attempt consumed. The loop can stop for three
	// different reasons and the task state alone cannot tell them apart, so the
	// executor states it: a run that ran out of budget and a run where the model
	// declared itself finished are different facts, and only one of them is a
	// step-budget problem.
	Stats ExecStats
}

// ExecStats is the executor's report on one attempt. StepsConsumed counts the
// steps the execution loop emitted; StepBudget is the bound actually in force
// for this attempt; CloseReason is one of CloseReasonStepLimit /
// CloseReasonModelFinished / CloseReasonError.
type ExecStats struct {
	StepsConsumed int    `json:"steps_consumed"`
	StepBudget    int    `json:"step_budget"`
	CloseReason   string `json:"close_reason"`
}

// Executor performs the real work behind a task. The gateway itself never
// touches external systems; it delegates here (in production wired to the
// model/tools plane, in tests to a fake).
type Executor interface {
	Execute(ctx context.Context, in ExecInput) (ExecResult, error)
}

// ExecutorFunc adapts a function to Executor.
type ExecutorFunc func(ctx context.Context, in ExecInput) (ExecResult, error)

// Execute implements Executor.
func (f ExecutorFunc) Execute(ctx context.Context, in ExecInput) (ExecResult, error) {
	return f(ctx, in)
}

// Notifier delivers best-effort wake-up signals. Redis is used only to shorten
// latency: correctness never depends on it, the event journal is authoritative.
type Notifier interface {
	Notify(ctx context.Context, channel string) error
	Wait(ctx context.Context, channel string, timeout time.Duration)
}

// NullNotifier never signals; consumers fall back to polling.
type NullNotifier struct{}

// Notify implements Notifier.
func (NullNotifier) Notify(context.Context, string) error { return nil }

// Wait implements Notifier.
func (NullNotifier) Wait(ctx context.Context, _ string, timeout time.Duration) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// Config tunes a gateway Service.
type Config struct {
	WorkerID    string
	Lease       time.Duration // task lease duration; default 30s
	Poll        time.Duration // wake-up fallback; default 200ms
	MaxBlock    time.Duration // how long SendMessage blocks without ReturnImmediately; default 30s
	MaxAttempts int           // executor attempts before the task is failed; default 3
	// ParkLease is how long an ambiguous-effect (SIDE_EFFECT_UNKNOWN) task stays
	// held without being re-claimed, i.e. "parked for a human". Default 1h.
	ParkLease time.Duration
	// StepBudgetCeiling is the operator's per-attempt ceiling: a caller-stated
	// max_steps above it is refused at the ingress instead of being clamped.
	// 0 means no ceiling is configured, so a stated budget stands as given.
	StepBudgetCeiling int
	// TaskVerifier and TaskAudience let the acceptance point verify, itself, the
	// task credential a delegating run presents when it names a parent this plane
	// cannot see (PDF rev3.2 :975, :1064). Left unset, the acceptance point trusts
	// only the principal its ingress middleware verified -- the credential still
	// has to be a task credential, so nothing becomes trust-the-header; mounts that
	// must not depend on the ingress configuration set both.
	TaskVerifier *auth.Verifier
	// TaskAudience is the audience TaskVerifier requires of that credential.
	TaskAudience string
	// PlaneDeployment names the deployment THIS process serves -- the workload its
	// own certificate carries, supplied by the composition root that owns the
	// certificate. The gateway cannot read it off a request: a request's scope
	// belongs to whoever presented the credential, and on a delegation hop that is
	// the delegating plane, not the one accepting. Every accepted task journals it
	// as MetaAcceptedByDeployment; left empty, the journal records
	// AcceptedByUnconfigured so the gap stays visible to a query instead of being
	// mistaken for the parent's deployment (DEF-J4-DEPLOY-ATTRIBUTION).
	PlaneDeployment string
	// Logger receives operator diagnostics produced inside the service (task
	// refusals included). Defaults to the process journal.
	Logger *slog.Logger
	// Invocations verifies the artifact an Invoke named against what this tenant
	// actually has installed, so a task runs the artifact it was invoked as or
	// nothing runs (PDF rev3.2 c.42: the version is fixed to the task). Left nil,
	// a claim is REFUSED rather than trusted -- see internal/gateway/invocation.go.
	Invocations InstalledArtifactReader
}

func (c *Config) withDefaults() {
	if c.WorkerID == "" {
		c.WorkerID = "gw-" + contracts.NewID()
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.Poll <= 0 {
		c.Poll = 200 * time.Millisecond
	}
	if c.MaxBlock <= 0 {
		c.MaxBlock = 30 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.ParkLease <= 0 {
		c.ParkLease = time.Hour
	}
}

// Service is the durable A2A application service. It owns task lifecycle,
// idempotent acceptance, the event journal, and the fenced worker loop.
type Service struct {
	store  *Store
	ledger *operations.Store
	exec   Executor
	notify Notifier
	cfg    Config
}

// NewService wires a service over an already-migrated store and effect ledger.
func NewService(store *Store, ledger *operations.Store, exec Executor, notify Notifier, cfg Config) (*Service, error) {
	if store == nil || ledger == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: store and ledger are required")
	}
	if exec == nil {
		exec = ExecutorFunc(func(context.Context, ExecInput) (ExecResult, error) {
			return ExecResult{}, fmt.Errorf("gateway: no executor configured%w", ErrAmbiguous)
		})
	}
	if notify == nil {
		notify = NullNotifier{}
	}
	cfg.withDefaults()
	svc := &Service{store: store, ledger: ledger, exec: exec, notify: notify, cfg: cfg}
	if cfg.PlaneDeployment == "" {
		// Loud once at construction: a plane that cannot name itself will journal
		// every acceptance as "unconfigured", which is a gap an operator can see but
		// cannot act on from a task row alone.
		svc.logger().Warn("gateway: this plane's own deployment is not configured; " +
			"every accepted task will be journalled with ahp.accepted_by_deployment=unconfigured")
	}
	return svc, nil
}

// Store exposes the underlying durable store.
func (s *Service) Store() *Store { return s.store }

func taskChannel(taskID string) string { return "ahp:gw:task:" + taskID }
func claimChannel() string             { return "ahp:gw:claim" }

// messageDigest canonicalises the content of a message (never its messageId,
// which is the idempotency key itself) so a reused key with a different body is
// detectable.
func messageDigest(m *a2a.Message) (string, error) {
	if m == nil {
		return "", contracts.NewReasoned(contracts.ErrInvalid, ReasonMessageMissing, "gateway: message required")
	}
	view := struct {
		ContextID      string           `json:"contextId"`
		TaskID         string           `json:"taskId"`
		Role           string           `json:"role"`
		Parts          a2a.ContentParts `json:"parts"`
		ReferenceTasks []a2a.TaskID     `json:"referenceTaskIds"`
		Metadata       map[string]any   `json:"metadata"`
	}{
		ContextID:      m.ContextID,
		TaskID:         string(m.TaskID),
		Role:           string(m.Role),
		Parts:          m.Parts,
		ReferenceTasks: m.ReferenceTasks,
		Metadata:       m.Metadata,
	}
	return contracts.Digest(view)
}

// SendMessage is the durable, idempotent acceptance path. Scope and caller come
// from a verified principal, never from the request body.
func (s *Service) SendMessage(ctx context.Context, scope contracts.Scope, caller string, req *a2a.SendMessageRequest) (*a2a.Task, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if caller == "" {
		return nil, contracts.NewReasoned(contracts.ErrInvalid, ReasonCallerRequired, "gateway: caller required")
	}
	if req == nil || req.Message == nil {
		return nil, contracts.NewReasoned(contracts.ErrInvalid, ReasonMessageMissing, "gateway: message required")
	}
	msg := req.Message
	// Canonicalize the role before anything is digested or journalled. A v0
	// client says "user"; a2a-go keeps that verbatim, and a row spelled
	// "user" is invisible to the drain's user-turn selection -- which is how a
	// task with text in its history reached an executor with no input at all.
	// Doing it here means the idempotency digest, the journalled row and every
	// served copy of it agree on one spelling, and a message that cannot be a
	// user turn is refused (-32602) instead of being stored and later skipped.
	role, err := canonicalInboundRole(msg.Role)
	if err != nil {
		return nil, err
	}
	msg.Role = role
	// An empty request is a client error, not a task that dies later: refuse it
	// here with -32602 instead of creating a task the worker can only fail.
	if err := validateInboundContent(msg); err != nil {
		return nil, err
	}
	// The step budget is decided here, before anything is persisted. A caller that
	// states max_steps gets either exactly that or a refusal naming the operator's
	// ceiling -- never a quieter number than the one it asked for.
	budget, err := resolveStepBudget(req.Metadata, msg.Metadata, s.cfg.StepBudgetCeiling)
	if err != nil {
		return nil, err
	}
	taskMetadata := make(map[string]any, len(req.Metadata)+2)
	for k, v := range req.Metadata {
		taskMetadata[k] = v
	}
	// Tree fields belong to the gateway (PDF rev3.2, 6.1): a message that
	// states ahp.root/ahp.depth is ignored, not merged. The authoritative
	// values are derived from this store below and journalled on the row.
	delete(taskMetadata, MetaTreeRoot)
	delete(taskMetadata, MetaTreeDepth)
	// The claim is an input to verification, not a field of the task, and the
	// verified pin is this plane's to write: a caller that could author
	// ahp.pinned_* would replace the check with an assertion.
	delete(taskMetadata, MetaInvocation)
	for k := range taskMetadata {
		if isAttestationMarker(k) || isGrantMarker(k) || isInvocationMarker(k) {
			delete(taskMetadata, k)
		}
	}
	grantMetadata, err := s.grantMetadata(ctx, scope)
	if err != nil {
		return nil, err
	}
	for k, v := range grantMetadata {
		taskMetadata[k] = v
	}
	if budget.Source == BudgetSourceCaller {
		taskMetadata[MetadataStepBudget] = budget.Steps
		taskMetadata[MetadataStepBudgetSource] = BudgetSourceCaller
	}
	if msg.ID == "" {
		msg.ID = a2a.NewMessageID()
	}
	digest, err := messageDigest(msg)
	if err != nil {
		return nil, err
	}

	followUp := msg.TaskID != ""
	// An invocation claim is verified here -- after the message is known to be a
	// well-formed turn and before a single row is written. Verifying later would
	// let an artifact this plane cannot name start running, and refusing after the
	// fact is not the same as never having started.
	pinned, err := s.verifyInvocation(ctx, scope, followUp, req.Metadata)
	if err != nil {
		return nil, err
	}
	key := MessageKey{Caller: caller, MessageID: msg.ID}
	if followUp {
		key.Target = string(msg.TaskID)
	}

	// A conversation the client named is verified before the transaction opens;
	// creating a fresh context happens inside the transaction, so the store never
	// borrows a second pool connection while one is held.
	if !followUp && msg.ContextID != "" {
		if _, err := s.store.AcquireContext(ctx, scope, caller, msg.ContextID); err != nil {
			return nil, err
		}
		key.Target = msg.ContextID
	}

	var (
		out      TaskRow
		replayed bool
	)
	err = pgx.BeginFunc(ctx, s.store.pool, func(tx pgx.Tx) error {
		claimTask, replay, err := s.store.ClaimMessage(ctx, tx, scope, key, digest)
		if err != nil {
			return err
		}
		if replay {
			t, err := s.store.Task(ctx, scope, claimTask)
			if err != nil {
				return err
			}
			out, replayed = t, true
			return nil
		}

		if followUp {
			t, err := s.store.Task(ctx, scope, string(msg.TaskID))
			if err != nil {
				return err
			}
			if t.Caller != caller {
				return contracts.NewReasoned(contracts.ErrForbidden, ReasonTaskNotOwned, "gateway: task is not owned by this caller")
			}
			// A follow-up may restate the per-attempt budget for the next attempt
			// (already validated above). Restating it is journalled, not applied in
			// memory, so a worker that picks the task up in another process bounds
			// the attempt by the same number this caller asked for.
			if budget.Source == BudgetSourceCaller {
				if err := s.store.MergeMetadataTx(ctx, tx, t.ID, map[string]any{
					MetadataStepBudget:       budget.Steps,
					MetadataStepBudgetSource: BudgetSourceCaller,
				}); err != nil {
					return err
				}
			}
			inbound := *msg
			inbound.ContextID = t.ContextID
			if err := s.store.AppendMessage(ctx, tx, t.ID, &inbound); err != nil {
				return err
			}
			if err := s.store.RecordMessage(ctx, tx, scope, key, digest, t.ID); err != nil {
				return err
			}
			out = t
			return nil
		}

		// New task: acquire (or verify) the owning context.
		owned, err := s.store.AcquireContextTx(ctx, tx, scope, caller, msg.ContextID)
		if err != nil {
			return err
		}
		kind, parent := taskAttributes(req)
		// Keep "no metadata at all" distinguishable from "an empty metadata
		// object": the task's wire form must not gain a field the caller never sent.
		var newTaskMetadata map[string]any
		if len(taskMetadata) > 0 {
			newTaskMetadata = taskMetadata
		}
		// Server-set tree fields: audit the declared parent inside this
		// tenant+subject and derive root/depth from the gateway's own rows.
		// A task without a declared parent is the root of its own tree.
		treeRoot, treeDepth := "", 0
		var attested *attestedParent
		if parent != "" {
			root, depth, att, err := s.resolveParentChain(ctx, tx, scope, caller, parent, budget)
			if err != nil {
				return err
			}
			treeRoot, treeDepth, attested = root, depth, att
		}
		row, err := s.store.InsertTask(ctx, tx, TaskRow{
			Scope:      scope,
			Caller:     caller,
			ContextID:  owned,
			Kind:       kind,
			ParentTask: parent,
			State:      a2a.TaskStateSubmitted,
			Metadata:   newTaskMetadata,
		})
		if err != nil {
			return err
		}
		if parent == "" {
			treeRoot = row.ID
		}
		treePatch := map[string]any{
			MetaTreeRoot:  treeRoot,
			MetaTreeDepth: treeDepth,
		}
		for k, v := range attested.markers() {
			treePatch[k] = v
		}
		// Which plane took this task is established here, never claimed by the
		// caller, and it is a different fact from the scope column: on a delegation
		// hop the column names the parent's plane. Journaling it is what makes the
		// accepting deployment queryable without reading any payload.
		for k, v := range s.acceptedByMarkers() {
			treePatch[k] = v
		}
		for k, v := range pinned.metadata() {
			treePatch[k] = v
		}
		if err := s.store.MergeMetadataTx(ctx, tx, row.ID, treePatch); err != nil {
			return err
		}
		inbound := *msg
		inbound.ContextID = owned
		inbound.TaskID = a2a.TaskID(row.ID)
		if err := s.store.AppendMessage(ctx, tx, row.ID, &inbound); err != nil {
			return err
		}
		if err := s.store.AppendEvent(ctx, tx, Event{
			TaskID: row.ID, Version: row.StatusVersion, Scope: scope, Kind: "status",
			Payload: statusEvent(row, a2a.TaskStateSubmitted, nil),
		}); err != nil {
			return err
		}
		if err := s.store.RecordMessage(ctx, tx, scope, key, digest, row.ID); err != nil {
			return err
		}
		if pc := pushConfigOf(req); pc != nil {
			clone := *pc
			clone.TaskID = a2a.TaskID(row.ID)
			if _, err := s.store.PutPushConfigTx(ctx, tx, &clone, scope); err != nil {
				return err
			}
		}
		out = row
		return nil
	})
	if err != nil {
		return nil, err
	}

	if replayed {
		return s.snapshot(ctx, scope, out)
	}
	_ = s.notify.Notify(ctx, claimChannel())
	if returnImmediately(req) {
		return s.snapshot(ctx, scope, out)
	}
	// A follow-up can land on an already-settled task; there is nothing to
	// await. Returning now avoids burning the whole block budget on a task that
	// is already terminal.
	if out.Terminal() {
		return s.snapshot(ctx, scope, out)
	}
	return s.awaitTerminal(ctx, scope, out.ID, out.StatusVersion)
}

// snapshot loads a task and its history into an official a2a.Task.
func (s *Service) snapshot(ctx context.Context, scope contracts.Scope, row TaskRow) (*a2a.Task, error) {
	hist, err := s.store.History(ctx, row.ID, 0)
	if err != nil {
		return nil, err
	}
	return toTask(row, hist), nil
}

// awaitTerminal tails the event journal until the task reaches a terminal state
// or the block budget expires, then returns the latest snapshot.
func (s *Service) awaitTerminal(ctx context.Context, scope contracts.Scope, taskID string, from int64) (*a2a.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.MaxBlock)
	defer cancel()
	cursor := from
	for {
		evs, err := s.store.Events(ctx, scope, taskID, cursor)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr == nil &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			// Budget expired mid-tail: read the final snapshot on a live context.
			fctx, fcancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.Poll+time.Second)
			defer fcancel()
			row, rerr := s.store.Task(fctx, scope, taskID)
			if rerr != nil {
				return nil, rerr
			}
			return s.snapshot(fctx, scope, row)
		}
		for _, ev := range evs {
			cursor = ev.Version
			if terminalEvent(ev.Payload) {
				row, err := s.store.Task(ctx, scope, taskID)
				if err != nil {
					return nil, err
				}
				return s.snapshot(ctx, scope, row)
			}
		}
		s.notify.Wait(ctx, taskChannel(taskID), s.cfg.Poll)
		if ctx.Err() != nil {
			// The block budget expired; read the final snapshot on a context that
			// is not already canceled, otherwise the graceful "latest state" return
			// would itself fail with a deadline error.
			fctx, fcancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.Poll+time.Second)
			defer fcancel()
			row, err := s.store.Task(fctx, scope, taskID)
			if err != nil {
				return nil, err
			}
			return s.snapshot(fctx, scope, row)
		}
	}
}

// Events returns a replay-then-live stream of a task's transitions, starting
// strictly after fromVersion. It is the source for SSE replay: after a restart a
// new subscriber still sees every journalled event before following live ones.
func (s *Service) Events(ctx context.Context, scope contracts.Scope, taskID string, fromVersion int64) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		cursor := fromVersion
		for {
			evs, err := s.store.Events(ctx, scope, taskID, cursor)
			if err != nil {
				// A canceled or expired context is a normal end of a live stream
				// (client disconnect, block budget, shutdown), not a journaling
				// fault: close cleanly without surfacing an error event. Genuine
				// store errors are unrelated to ctx and still propagate.
				if ctxErr := ctx.Err(); ctxErr != nil ||
					errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				yield(nil, err)
				return
			}
			done := false
			for _, ev := range evs {
				cursor = ev.Version
				if !yield(ev.Payload, nil) {
					return
				}
				if terminalEvent(ev.Payload) {
					done = true
					break
				}
			}
			if done {
				return
			}
			s.notify.Wait(ctx, taskChannel(taskID), s.cfg.Poll)
			if ctx.Err() != nil {
				return
			}
		}
	}
}

// Run drives the fenced worker loop until ctx is cancelled.
func (s *Service) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		processed, err := s.drainOnce(ctx)
		if err != nil && ctx.Err() == nil {
			// Transient store error: back off via the wake-up wait.
			s.notify.Wait(ctx, claimChannel(), s.cfg.Poll)
			continue
		}
		if processed == 0 {
			s.notify.Wait(ctx, claimChannel(), s.cfg.Poll)
		}
	}
}

// drainOnce claims and executes a single task; it reports whether one ran.
func (s *Service) drainOnce(ctx context.Context) (int, error) {
	row, err := s.store.ClaimTask(ctx, s.cfg.WorkerID, s.cfg.Lease)
	if err != nil || row == nil {
		return 0, err
	}
	if err := s.process(ctx, *row); err != nil {
		return 1, err
	}
	return 1, nil
}

// operationKey is per attempt: Dispatch can never revive a settled operation,
// so every claim (which increments task.Attempts) gets its own ledger row. The
// guard stays stable across attempts, which is what stops two workers from
// executing the same task concurrently.
func operationKey(task TaskRow) string {
	return "exec:" + task.ID + ":" + strconv.Itoa(task.Attempts)
}

// process runs one fenced attempt of a claimed task.
func (s *Service) process(ctx context.Context, task TaskRow) error {
	if task.CancelRequested {
		return s.finalizeCancel(ctx, task)
	}
	scope := task.Scope
	opKey := operationKey(task)
	if task.LeaseOwner != s.cfg.WorkerID {
		return contracts.NewError(contracts.ErrLeaseLost, "gateway: task not held by this worker")
	}

	// Effect-ledger guard: replay-safe dispatch with UNKNOWN protection.
	op, err := s.ledger.Accept(ctx, scope, operations.Request{
		Key:    opKey,
		Kind:   "task:" + task.Kind,
		Target: task.ID,
		Guard:  task.Guard(),
	})
	if err != nil {
		return err
	}
	switch op.Status {
	case operations.StatusSucceeded:
		return s.settleFromLedger(ctx, task, op, false)
	case operations.StatusFailed:
		// This attempt is settled; only the last one may end the task, earlier
		// ones were retried under a new attempt key.
		if task.Attempts >= s.cfg.MaxAttempts {
			return s.settleFromLedger(ctx, task, op, true)
		}
		return s.store.ReleaseLease(ctx, task.ID, s.cfg.WorkerID)
	case operations.StatusSideEffectUnknown:
		// Never auto-replay an ambiguous effect: park the task for a human. The
		// park is bounded: at the attempt ceiling the task gets a terminal verdict
		// instead of being re-parked for the lifetime of the deployment.
		if task.Attempts >= s.cfg.MaxAttempts {
			return s.abandonUnrecoverable(ctx, task, "side effect is unresolved (SIDE_EFFECT_UNKNOWN) and the attempt ceiling is reached")
		}
		s.park(ctx, task)
		return nil
	case operations.StatusDispatched:
		// A DISPATCHED row under *this* attempt key can only mean the previous
		// holder of the (now expired) lease crashed after dispatch. The effect
		// may or may not have happened, so it becomes SIDE_EFFECT_UNKNOWN and is
		// parked; replaying it is a human decision via Reconcile.
		if _, err := s.ledger.MarkUnknown(ctx, scope, opKey); err != nil {
			return err
		}
		if task.Attempts >= s.cfg.MaxAttempts {
			return s.abandonUnrecoverable(ctx, task, "the previous holder crashed after dispatch; the effect may have happened and the attempt ceiling is reached")
		}
		s.park(ctx, task)
		return nil
	case operations.StatusFenced:
		// The guard is held by an earlier attempt whose effect is still unsettled,
		// so this attempt can legitimately do nothing but stand down. Releasing the
		// lease alone made the task claimable forever: attempts and fencing tokens
		// climbed on every wake-up while nothing settled and nothing was ever
		// reported -- a task could sit in WORKING for the lifetime of the
		// deployment, silently (DEF-H22-1). Standing down is therefore bounded by
		// the same attempt ceiling that bounds a plain executor failure.
		if task.Attempts >= s.cfg.MaxAttempts {
			return s.abandonUnrecoverable(ctx, task, "the effect ledger still holds an unsettled earlier attempt; replaying it needs a reconcile decision, and no further attempt is allowed")
		}
		_ = s.store.ReleaseLease(ctx, task.ID, s.cfg.WorkerID)
		return nil
	case operations.StatusAccepted:
		// fall through to (re)dispatch below
	}

	// The caller's budget is read BEFORE the effect ledger is opened: a stored
	// row whose budget cannot be read is an operator problem, and discovering it
	// before dispatch means no external effect was started on a bound nobody
	// agreed to.
	statedBudget, _, err := budgetFromMetadata(task.Metadata)
	if err != nil {
		return err
	}

	if _, err := s.ledger.Dispatch(ctx, scope, opKey); err != nil {
		// Losing the dispatch grant means we must not touch the outside world.
		if !guardRefused(err) {
			return err
		}
		// The guard is held by an operation that has not settled -- in practice an
		// earlier attempt that dispatched and then died, leaving SIDE_EFFECT_UNKNOWN.
		// Standing down is the only safe move, but standing down *forever* is not a
		// design: the ledger refused us, so this attempt must count, and at the
		// attempt ceiling the task gets an explicit terminal verdict. Before this,
		// the CONFLICT escaped process() as a "transient" error and the worker
		// re-claimed the task indefinitely -- attempts and fencing tokens climbed on
		// every wake-up while the task sat in WORKING and nothing was ever written
		// for an operator (DEF-H22-1).
		slog.Warn("gateway: dispatch grant refused by the effect-ledger guard",
			slog.String("task_id", task.ID),
			slog.String("op_key", opKey),
			slog.Int("attempts", int(task.Attempts)),
			slog.Int("max_attempts", s.cfg.MaxAttempts),
			slog.String("err", err.Error()))
		if task.Attempts >= s.cfg.MaxAttempts {
			return s.abandonUnrecoverable(ctx, task,
				"the dispatch grant is refused because an earlier attempt's effect never settled; replaying it needs a reconcile decision, and no further attempt is allowed")
		}
		s.park(ctx, task)
		return nil
	}

	input, err := s.lastInbound(ctx, scope, task)
	if err != nil {
		return err
	}

	execCtx, stopWatching := s.watchOwner(ctx, scope)
	defer stopWatching()
	res, execErr := s.exec.Execute(execCtx, ExecInput{
		Scope: scope, Task: task, FencingToken: task.FencingToken, Input: input,
		MaxSteps: statedBudget,
	})
	if current, err := s.store.Task(ctx, scope, task.ID); err == nil && current.CancelRequested {
		return s.finalizeCancel(ctx, task)
	}
	if execErr != nil && errors.Is(execErr, ErrAmbiguous) {
		if _, err := s.ledger.MarkUnknown(ctx, scope, opKey); err != nil {
			return err
		}
		s.park(ctx, task)
		return nil
	}
	if task.CancelRequested {
		return s.finalizeCancel(ctx, task)
	}
	if execErr != nil {
		return s.failAttempt(ctx, task, opKey, execErr)
	}
	return s.commitResult(ctx, task, opKey, res)
}

// park keeps an ambiguous-effect task out of the claim queue for ParkLease
// instead of releasing it immediately, so no worker re-dispatches a side effect
// whose outcome is unknown. If the lease cannot be extended we still release:
// the ledger row stays SIDE_EFFECT_UNKNOWN, which is what prevents replay.
func (s *Service) park(ctx context.Context, task TaskRow) {
	if err := s.store.RenewLease(ctx, task.ID, s.cfg.WorkerID, s.cfg.ParkLease); err != nil {
		_ = s.store.ReleaseLease(ctx, task.ID, s.cfg.WorkerID)
	}
}

// guardRefused reports whether a dispatch failed because the task's guard is
// still held by an unsettled operation (another attempt in flight, or a side
// effect whose outcome is unknown). Both are the ledger doing its job -- they are
// not transient faults, and retrying them unchanged is what made a crashed task
// loop forever.
func guardRefused(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, operations.ErrAlreadyDispatched) {
		return true
	}
	return contractCode(err) == contracts.ErrConflict
}

// abandonUnrecoverable ends a task with an explicit terminal verdict when no
// further attempt can make progress: the effect ledger still holds an unsettled
// earlier attempt (the guard refuses every new one), and the attempt ceiling is
// reached. Before this existed the worker silently handed the lease back, so the
// task was re-claimed forever and stayed in WORKING for the life of the
// deployment with nothing written for an operator (DEF-H22-1).
//
// The verdict is FAILED with close_reason=attempt_ceiling_exceeded and is
// deliberately NOT a replay and NOT a claim about the outside world: the ledger
// row is left exactly as it is, still keyed and guarded, so a human reconcile can
// decide what actually escaped. The task stops being claimable the moment it is
// terminal, which is what makes the loop bounded.
func (s *Service) abandonUnrecoverable(ctx context.Context, task TaskRow, why string) error {
	slog.Error("gateway: attempt ceiling reached with an unsettled effect; task abandoned",
		slog.String("task_id", task.ID),
		slog.String("guard", task.Guard()),
		slog.Int("attempts", int(task.Attempts)),
		slog.Int("max_attempts", s.cfg.MaxAttempts),
		slog.String("reason", why))
	text := fmt.Sprintf("task abandoned after %d attempts: %s. The effect ledger keeps its "+
		"unresolved row under guard %q so a reconcile decision remains possible; nothing was replayed.",
		task.Attempts, why, task.Guard())
	msg := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(text))
	return pgx.BeginFunc(ctx, s.store.pool, func(tx pgx.Tx) error {
		row, err := s.store.CompleteHeld(ctx, tx, task.Scope, task.ID, s.cfg.WorkerID, task.StatusVersion,
			a2a.TaskStateFailed, msg, nil)
		if err != nil {
			return nilIfLeaseLost(err)
		}
		patch := map[string]any{
			MetadataCloseReason:  CloseReasonAttemptCeiling,
			MetadataAttempt:      int(task.Attempts),
			"abandon_reason":     why,
			"reconcile_required": true,
		}
		if err := s.store.MergeMetadataTx(ctx, tx, row.ID, patch); err != nil {
			return err
		}
		if err := s.store.AppendEvent(ctx, tx, Event{
			TaskID: row.ID, Version: row.StatusVersion, Scope: task.Scope, Kind: "status",
			Payload: statusEvent(row, a2a.TaskStateFailed, msg),
		}); err != nil {
			return err
		}
		return s.enqueueTerminalOutbox(ctx, tx, task.Scope, row)
	})
}

// lastInbound returns the most recent user message of the task.
func (s *Service) lastInbound(ctx context.Context, scope contracts.Scope, task TaskRow) (*a2a.Message, error) {
	hist, err := s.store.History(ctx, task.ID, 0)
	if err != nil {
		return nil, err
	}
	// Selection is by canonical user turn, so a row journalled before the
	// ingress canonicalized roles is still reachable: an already-persisted task
	// must not stay stranded on a spelling alone.
	return selectLastInbound(hist), nil
}

func (s *Service) commitResult(ctx context.Context, task TaskRow, opKey string, res ExecResult) error {
	scope := task.Scope
	if !res.State.Terminal() &&
		res.State != a2a.TaskStateInputRequired && res.State != a2a.TaskStateAuthRequired {
		res.State = a2a.TaskStateCompleted
	}
	// Record the authoritative effect result first so a later crash replays it.
	resultRaw, err := json.Marshal(res)
	if err != nil {
		return err
	}
	if _, err := s.ledger.Complete(ctx, scope, opKey, resultRaw, res.State == a2a.TaskStateFailed); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.store.pool, func(tx pgx.Tx) error {
		row, err := s.store.CompleteHeld(ctx, tx, scope, task.ID, s.cfg.WorkerID, task.StatusVersion,
			res.State, agentMessage(task, res.Message), res.Artifacts)
		if err != nil {
			return err
		}
		if err := s.store.AppendEvent(ctx, tx, Event{
			TaskID: row.ID, Version: row.StatusVersion, Scope: scope, Kind: "status",
			Payload: statusEvent(row, res.State, res.Message),
		}); err != nil {
			return err
		}
		if err := s.journalAttemptStats(ctx, tx, row, res, ""); err != nil {
			return err
		}
		if res.State.Terminal() {
			return s.enqueueTerminalOutbox(ctx, tx, scope, row)
		}
		return nil
	})
}

func (s *Service) settleFromLedger(ctx context.Context, task TaskRow, op operations.Operation, failed bool) error {
	var res ExecResult
	if len(op.Result) > 0 {
		if err := json.Unmarshal(op.Result, &res); err != nil {
			res = ExecResult{State: a2a.TaskStateCompleted}
		}
	}
	if res.State == a2a.TaskStateUnspecified {
		if failed {
			res.State = a2a.TaskStateFailed
		} else {
			res.State = a2a.TaskStateCompleted
		}
	}
	return pgx.BeginFunc(ctx, s.store.pool, func(tx pgx.Tx) error {
		row, err := s.store.CompleteHeld(ctx, tx, task.Scope, task.ID, s.cfg.WorkerID, task.StatusVersion,
			res.State, agentMessage(task, res.Message), res.Artifacts)
		if err != nil {
			if errors.Is(err, contracts.NewError(contracts.ErrLeaseLost, "")) ||
				contractCode(err) == contracts.ErrLeaseLost {
				return nil // superseded; the winning writer already persisted
			}
			return err
		}
		if err := s.store.AppendEvent(ctx, tx, Event{
			TaskID: row.ID, Version: row.StatusVersion, Scope: task.Scope, Kind: "status",
			Payload: statusEvent(row, res.State, res.Message),
		}); err != nil {
			return err
		}
		// A settle replayed from the effect ledger must publish the same facts the
		// original attempt would have: the numbers come from the stored ExecResult,
		// so a crash between the executor and the commit cannot erase the reason a
		// run stopped.
		// The ledger payload is the other evidence source: failAttempt settles a
		// refusal there before the journal sees it, so a replay reads the cause out
		// of that row rather than losing it (DEF-J4-CHILD-TERMINAL-REASON).
		if err := s.journalAttemptStats(ctx, tx, row, res, ledgerFailureCause(op.Result)); err != nil {
			return err
		}
		if res.State.Terminal() {
			return s.enqueueTerminalOutbox(ctx, tx, task.Scope, row)
		}
		return nil
	})
}

// journalAttemptStats records what the last attempt consumed on the task itself.
// The task state cannot express "the model decided it was done" apart from "the
// step budget ran out", and an acceptance rule that treats a finished run which
// never wrote the requested artifact as a FAIL needs exactly that difference.
// The per-attempt history stays in the effect ledger (the marshalled ExecResult),
// so total consumption across resumes is reconstructible, not deniable.
//
// A terminal transition also owes a reason in the metadata field even when the executor
// filed no report at all (DEF-J4-CHILD-TERMINAL-REASON): mergeAttemptStats returns nothing
// in that case, which used to mean the field stayed absent. evidence carries what the
// effect ledger knows about the attempt, so a replayed settle can name its cause; "" on the
// fresh-commit path, where res is itself the whole report.
func (s *Service) journalAttemptStats(ctx context.Context, tx pgx.Tx, row TaskRow, res ExecResult, evidence string) error {
	stats := mergeAttemptStats(res, budgetSourceOf(row.Metadata), int(row.Attempts))
	if len(stats) == 0 {
		// Nothing was reported. A terminal verdict still owes a reason, and it is paid for
		// with a reason word only: steps and budget belong to the executor that filed
		// nothing, and a zero written here would be an invented measurement.
		reason := terminalCloseReason(res, evidence)
		if reason == "" {
			return nil
		}
		stats = map[string]any{
			MetadataCloseReason: reason,
			MetadataAttempt:     int(row.Attempts),
		}
	}
	if err := s.store.MergeMetadataTx(ctx, tx, row.ID, stats); err != nil {
		return err
	}
	slog.Info("gateway: attempt settled",
		slog.String("task_id", row.ID),
		slog.String("state", string(res.State)),
		slog.Int("steps_consumed", res.Stats.StepsConsumed),
		slog.Int("step_budget", res.Stats.StepBudget),
		slog.Any("close_reason", stats[MetadataCloseReason]),
		slog.Int("attempt", int(row.Attempts)))
	return nil
}

// terminalCloseReason names, in the execution dictionary's words, why a transition into a
// terminal state happened -- for the transitions where the executor's own ExecResult.Stats
// carries no verdict.
//
// The precedence is the whole point of having one function: a report the executor filed
// outranks anything inferred here; a cancel is the platform's cancel word whatever the
// surrounding prose says; and everything else is classified from the evidence that actually
// survives into the journal (the ledger's cause sentence, then the agent message), never from
// a guess. A state that is not terminal owes no reason yet and gets "".
//
// PDF requirement: a terminal transition must carry a reason. status_message carries a
// sentence, and a sentence cannot be compared across planes, so the reason has to be a field
// value (DEF-J4-CHILD-TERMINAL-REASON, measured on the canonical A->B stand).
func terminalCloseReason(res ExecResult, evidence string) string {
	if !res.State.Terminal() {
		return ""
	}
	if res.Stats.CloseReason != "" {
		return res.Stats.CloseReason
	}
	if res.State == a2a.TaskStateCanceled {
		return CloseReasonCanceled
	}
	return ClassifyFailureCause(nil, evidence, messageSentence(res.Message))
}

// messageSentence renders the plain text of an agent message so it can be classified. It
// reads the journal, it never replaces it: the verbatim sentence stays in status_message.
func messageSentence(msg *a2a.Message) string {
	if msg == nil {
		return ""
	}
	texts := make([]string, 0, len(msg.Parts))
	for _, part := range msg.Parts {
		if text := part.Text(); text != "" {
			texts = append(texts, text)
		}
	}
	return strings.Join(texts, " ")
}

// ledgerFailureCause reads back the refusal sentence failAttempt settles in the effect ledger
// before the journal ever sees the task. An undecodable payload or a payload with no cause is
// reported as no evidence ("") rather than as an empty reason: absence and a wrong word are
// different claims, and the classifier turns absence into CloseReasonUnreported.
func ledgerFailureCause(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var payload struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	return payload.Error
}

// journalTerminalReason writes the reason for a transition the executor did not report, in
// the same transaction that moves the row -- a reason that arrives by a later, separate write
// is a reason that can be lost, and the invariant is per-transition rather than per-code-path.
func (s *Service) journalTerminalReason(ctx context.Context, tx pgx.Tx, row TaskRow, reason string) error {
	if reason == "" {
		return nil
	}
	return s.store.MergeMetadataTx(ctx, tx, row.ID, map[string]any{MetadataCloseReason: reason})
}

func (s *Service) failAttempt(ctx context.Context, task TaskRow, opKey string, execErr error) error {
	cause := truncate(execErr.Error(), 512)
	if _, err := s.ledger.Complete(ctx, task.Scope, opKey,
		json.RawMessage(fmt.Sprintf("{%q:%q}", "error", cause)), true); err != nil {
		return err
	}
	if task.Attempts >= s.cfg.MaxAttempts {
		msg := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("task failed: "+cause))
		return pgx.BeginFunc(ctx, s.store.pool, func(tx pgx.Tx) error {
			row, err := s.store.CompleteHeld(ctx, tx, task.Scope, task.ID, s.cfg.WorkerID, task.StatusVersion,
				a2a.TaskStateFailed, msg, nil)
			if err != nil {
				return nilIfLeaseLost(err)
			}
			if err := s.store.AppendEvent(ctx, tx, Event{
				TaskID: row.ID, Version: row.StatusVersion, Scope: task.Scope, Kind: "status",
				Payload: statusEvent(row, a2a.TaskStateFailed, msg),
			}); err != nil {
				return err
			}
			// DEF-J4-CHILD-TERMINAL-REASON: the attempts budget is spent and the task is
			// FAILED, so the verdict owes a reason in the field. Nothing reported it -- the
			// only evidence is the refusal this plane rendered -- so classify that refusal.
			// The sentence itself stays in status_message; the word beside it is what a leg
			// on another plane can compare.
			if err := s.journalTerminalReason(ctx, tx, row, ClassifyFailureCause(execErr, cause)); err != nil {
				return err
			}
			return s.enqueueTerminalOutbox(ctx, tx, task.Scope, row)
		})
	}
	return s.store.ReleaseLease(ctx, task.ID, s.cfg.WorkerID)
}

func (s *Service) finalizeCancel(ctx context.Context, task TaskRow) error {
	return pgx.BeginFunc(ctx, s.store.pool, func(tx pgx.Tx) error {
		row, err := s.store.CancelHeld(ctx, tx, task.Scope, task.ID, s.cfg.WorkerID, task.StatusVersion)
		if err != nil {
			return nilIfLeaseLost(err)
		}
		msg := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("task cancelled"))
		if err := s.store.AppendEvent(ctx, tx, Event{
			TaskID: row.ID, Version: row.StatusVersion, Scope: task.Scope, Kind: "status",
			Payload: statusEvent(row, a2a.TaskStateCanceled, msg),
		}); err != nil {
			return err
		}
		// The cancel was journaled and honoured by the lease holder: that fact belongs in the
		// reason field, not only in the sentence, for the same requirement as above.
		if err := s.journalTerminalReason(ctx, tx, row, terminalCloseReason(
			ExecResult{State: a2a.TaskStateCanceled}, "task cancelled")); err != nil {
			return err
		}
		return s.enqueueTerminalOutbox(ctx, tx, task.Scope, row)
	})
}

// RenewLease refreshes the lease of a task this worker holds.
func (s *Service) RenewLease(ctx context.Context, taskID string) error {
	return s.store.RenewLease(ctx, taskID, s.cfg.WorkerID, s.cfg.Lease)
}

func statusEvent(row TaskRow, state a2a.TaskState, msg *a2a.Message) *a2a.TaskStatusUpdateEvent {
	return &a2a.TaskStatusUpdateEvent{
		TaskID:    a2a.TaskID(row.ID),
		ContextID: row.ContextID,
		Status: a2a.TaskStatus{
			State:     state,
			Message:   agentMessage(row, msg),
			Timestamp: ptr(time.Now().UTC()),
		},
	}
}

func agentMessage(row TaskRow, msg *a2a.Message) *a2a.Message {
	if msg == nil {
		return nil
	}
	out := *msg
	if out.Role == a2a.MessageRoleUnspecified {
		out.Role = a2a.MessageRoleAgent
	}
	if out.ID == "" {
		out.ID = a2a.NewMessageID()
	}
	out.ContextID = row.ContextID
	out.TaskID = a2a.TaskID(row.ID)
	return &out
}

// toTask converts a durable row plus history to the official a2a.Task.
func toTask(row TaskRow, history []*a2a.Message) *a2a.Task {
	t := &a2a.Task{
		ID:        a2a.TaskID(row.ID),
		ContextID: row.ContextID,
		Status: a2a.TaskStatus{
			State:     row.State,
			Message:   row.StatusMessage,
			Timestamp: ptr(row.UpdatedAt.UTC()),
		},
		History: history,
	}
	if len(row.Artifacts) > 0 {
		t.Artifacts = row.Artifacts
	}
	if len(row.Metadata) > 0 {
		t.Metadata = row.Metadata
	}
	return t
}

func terminalEvent(ev a2a.Event) bool {
	su, ok := ev.(*a2a.TaskStatusUpdateEvent)
	return ok && su.Status.State.Terminal()
}

func nilIfLeaseLost(err error) error {
	if err != nil && contractCode(err) == contracts.ErrLeaseLost {
		return nil
	}
	return err
}

func contractCode(err error) string {
	if err == nil {
		return ""
	}
	return contracts.Code(err)
}

func ptr[T any](v T) *T { return &v }

// Guard builds a per-scope mutual-exclusion key so a single task can have at
// most one in-flight effect at a time even across workers.
func (t TaskRow) Guard() string { return "task:" + t.ID }
