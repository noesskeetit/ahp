// Package space coordinator implements the Space orchestration logic:
// accepting turns, dispatching to Core, HITL gates, outbox delivery,
// steering, recovery, and checkpointing. All persistence flows through
// StateClient — no direct database access.
package space

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	aot "github.com/ahp-platform/ahp/pkg/otel"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// CoreRunner executes a turn's model/tool loop. Implemented by internal/core.
type CoreRunner interface {
	// Run executes the model/tool loop until completion, cancellation,
	// HITL pause, or step limit. Returns step results incrementally via ch.
	Run(ctx context.Context, req *CoreRequest, results chan<- StepResult) error
}

// CoreRequest carries everything Core needs to execute a turn.
type CoreRequest struct {
	ProjectID            string
	ProjectMemoryProfile string
	TurnID               string
	ThreadID             string
	// ContextID is the conversation the thread belongs to (State's
	// threads.context_id) - the same value a task token carries as its context
	// claim. Whatever a run writes is recorded under it, so the platform can
	// find that data by conversation and delete it with the conversation.
	ContextID       string
	RunID           string
	Worker          string
	Epoch           int64
	TaskToken       string
	Input           json.RawMessage
	Messages        json.RawMessage
	MaxSteps        int
	ModelStepOffset int
	ModelReplay     *ModelReplay
	JournalModel    func(context.Context, int, int, json.RawMessage) error
	Checkpoint      *Checkpoint
	Steering        []SteeringEvent
	Scope           contracts.Scope
	// AgentConfig is the pinned revision+deployment carried to the runner.
	AgentConfig *AgentConfigRef
}

// Coordinator is the central orchestration structure. It accepts turns,
// dispatches Core runs, handles HITL/steering/recovery, and emits outbox.
type Coordinator struct {
	configPinner       DotConfigPinner
	agentPinner        AgentTargetPinner
	agentControl       AgentControl
	projectMemory      bool
	checkProject       func(context.Context, string, string, string, string) error
	checkSharedProject func(context.Context, string, string, string, string) error
	state              *StateClient
	core               CoreRunner
	worker             string
	signer             *auth.Signer
	audience           string

	runGrants           RunGrantIssuer
	requireRunGrant     bool
	runGrantCaps        []string
	runGrantTTL         time.Duration
	runGrantActive      int
	runGrantFanout      int
	mu                  sync.RWMutex
	turns               map[string]*Turn
	active              map[string]context.CancelFunc
	terminalCheckpoints terminalCheckpointCache
	lastSweepErr        string
	// streamAppend bounds ONE durable AppendStream call; see streamAppendBudget.
	streamAppend time.Duration

	// turnHistory is the READ-ONLY durable turn index rebuilt from State after a restart;
	// it never feeds execution, cancel or HITL paths. See turn_history.go.
	turnHistory *turnHistoryIndex
	// turnHistoryThreads bounds the unscoped ListTurns fan-out (0 disables the durable
	// read), turnHistoryBudget bounds its total wall time. Operator knobs, defaulted in
	// NewCoordinator and wired from cmd/as-runner.
	turnHistoryThreads int
	turnHistoryBudget  time.Duration

	// dispatchTimeout bounds how long a turn accepted with NO declared dispatcher stays
	// `pending` before Space terminalizes it with ReasonNoTurnDispatcher (DEF-H30-x).
	// Zero disables the watchdog and restores the old eternal-pending behaviour.
	dispatchTimeout time.Duration
	// requireDispatcher, when set, makes AcceptTurn refuse a turn this process has no
	// dispatcher for (503 no_turn_dispatcher) instead of accepting it into the
	// bounded-window watchdog. Opt-in: only a deployment that must never queue a turn it
	// cannot itself run sets it.
	requireDispatcher bool
	// dispatcherKnown records that a turn dispatcher (the orphan claim loop) has been
	// declared for this coordinator, by StartOrphanClaimLoop with a positive interval. A
	// runner declares it BEFORE it serves, so it is already true when the first
	// AcceptTurn is handled; only a dispatcher-less config is ever seen with it false.
	dispatcherKnown atomic.Bool
}

// CoordinatorConfig configures the Coordinator.
type CoordinatorConfig struct {
	ConfigPinner DotConfigPinner
	AgentPinner  AgentTargetPinner
	AgentControl AgentControl
	// CheckProject binds the conversation through ingress authority before a run
	// is enqueued. Runtime children cannot create or change this binding.
	ProjectMemory      bool
	CheckProject       func(context.Context, string, string, string, string) error
	CheckSharedProject func(context.Context, string, string, string, string) error
	StateClient        *StateClient
	Core               CoreRunner
	WorkerID           string
	// TaskSigner is an OPTIONAL internal Ed25519 signer used by MintTaskToken to
	// delegate a turn to a workload (execution /a2a/public). It is NOT the public
	// human verification key; when nil, MintTaskToken fails closed. See d71a7b0e (B).
	TaskSigner *auth.Signer
	// TokenAudience is the audience for minted task-tokens (defaults to DefaultAudience).
	TokenAudience string

	// Tier-2 per-run runtime grant (decision 2026-10-30-exchange-scope-authority-model).
	RunGrantIssuer    RunGrantIssuer
	RequireRunGrant   bool
	RunGrantCaps      []string
	RunGrantTTL       time.Duration
	RunGrantMaxActive int
	RunGrantMaxFanout int

	// StreamAppendTimeout bounds ONE durable AppendStream call (defaults to
	// DefaultStreamAppendTimeout). It must never be reused as a run-lifetime budget.
	StreamAppendTimeout time.Duration

	// TurnHistoryThreads caps how many of the caller's threads an UNSCOPED ListTurns may
	// walk to rebuild turn history after a restart (defaults to DefaultTurnHistoryThreads;
	// a negative value disables the durable read entirely).
	TurnHistoryThreads int
	// TurnHistoryBudget bounds that same walk (defaults to DefaultTurnHistoryBudget).
	TurnHistoryBudget time.Duration

	// TurnDispatchTimeout bounds the `pending` window for a turn accepted when NO turn
	// dispatcher has been declared for this coordinator (defaults to
	// DefaultTurnDispatchTimeout; <=0 disables the DEF-H30-x watchdog). It never arms
	// once a dispatcher is declared, so it cannot false-fail a busy-but-healthy runner.
	TurnDispatchTimeout time.Duration
	// RequireDispatcher makes AcceptTurn answer 503 ReasonNoTurnDispatcher for a turn
	// this process cannot dispatch, instead of accepting it into the pending watchdog.
	RequireDispatcher bool
}

// NewCoordinator creates a coordinator with the given dependencies.
func NewCoordinator(cfg CoordinatorConfig) *Coordinator {
	aud := cfg.TokenAudience
	if aud == "" {
		aud = DefaultAudience
	}
	return &Coordinator{
		configPinner:  cfg.ConfigPinner,
		agentPinner:   cfg.AgentPinner,
		agentControl:  cfg.AgentControl,
		projectMemory: cfg.ProjectMemory, checkProject: cfg.CheckProject, checkSharedProject: cfg.CheckSharedProject,
		state:    cfg.StateClient,
		core:     cfg.Core,
		worker:   cfg.WorkerID,
		signer:   cfg.TaskSigner,
		audience: aud,

		runGrants:       cfg.RunGrantIssuer,
		requireRunGrant: cfg.RequireRunGrant,
		runGrantCaps:    cfg.RunGrantCaps,
		runGrantTTL:     cfg.RunGrantTTL,
		runGrantActive:  cfg.RunGrantMaxActive,
		runGrantFanout:  cfg.RunGrantMaxFanout,
		streamAppend:    cfg.StreamAppendTimeout,
		turns:           make(map[string]*Turn),
		active:          make(map[string]context.CancelFunc),

		turnHistory:        newTurnHistoryIndex(),
		turnHistoryThreads: turnHistoryThreads(cfg.TurnHistoryThreads),
		turnHistoryBudget:  turnHistoryBudget(cfg.TurnHistoryBudget),

		dispatchTimeout:   turnDispatchTimeout(cfg.TurnDispatchTimeout),
		requireDispatcher: cfg.RequireDispatcher,
	}
}

// turnHistoryThreads normalizes the operator knob: 0 => default, negative => disabled,
// above the ceiling => the ceiling.
func turnHistoryThreads(v int) int {
	if v == 0 {
		return DefaultTurnHistoryThreads
	}
	if v < 0 {
		return 0
	}
	if v > maxTurnHistoryThreads {
		return maxTurnHistoryThreads
	}
	return v
}

// turnHistoryBudget normalizes the wall-time knob: 0 => default, negative => no walk.
func turnHistoryBudget(v time.Duration) time.Duration {
	if v == 0 {
		return DefaultTurnHistoryBudget
	}
	if v < 0 {
		return 0
	}
	return v
}

// turnDispatchTimeout normalizes the DEF-H30-x dispatch window: unset (0) => default,
// negative => disabled (restores the old eternal-pending behaviour for callers that
// opt out), positive => the stated bound.
func turnDispatchTimeout(v time.Duration) time.Duration {
	if v == 0 {
		return DefaultTurnDispatchTimeout
	}
	if v < 0 {
		return 0
	}
	return v
}

// DefaultStreamAppendTimeout bounds a single durable AppendStream call.
const DefaultStreamAppendTimeout = 10 * time.Second

// DefaultTurnDispatchTimeout is the window a turn accepted into a process with NO
// turn dispatcher (no orphan sweep, and no client DispatchTurn in sight) is left in
// `pending` before Space gives it a DETERMINED terminal with a cause. It only arms
// when no dispatcher has been declared for this coordinator, so a healthy runner -
// which starts its claim loop (and declares the dispatcher) BEFORE it serves - is
// never affected no matter how deep its backlog: only a config that will NEVER claim
// the run trips it. DEF-H30-x: a human-accepted turn must never be a silent eternal
// `pending`; either it runs, or Space says so with a reason within this window.
const DefaultTurnDispatchTimeout = 10 * time.Second

// ReasonNoTurnDispatcher is the machine-readable cause Space gives a turn it accepted
// but could never dispatch, and the token an honest AcceptTurn refusal carries when
// the deployment requires a dispatcher this process does not have. It is a statement
// about capability, not a model verdict: there was no execution to classify.
const ReasonNoTurnDispatcher = "no_turn_dispatcher"

// DefaultTurnHistoryThreads caps the unscoped ListTurns durable fan-out, and
// DefaultTurnHistoryBudget bounds it in wall time. They exist because a browser read must
// not become an unbounded walk over State: the bounded page is what makes the restart
// history readable without turning one slow State into a hung UI.
const (
	DefaultTurnHistoryThreads = 25
	DefaultTurnHistoryBudget  = 4 * time.Second
	// maxTurnHistoryThreads is the hard ceiling an operator may raise the knob to.
	maxTurnHistoryThreads = 100
)

// streamAppendBudget returns the per-append deadline. It is deliberately NOT a
// run-lifetime budget: a legitimate model turn runs for minutes, and an
// append-only budget keeps the fail-closed hole detection without charging the
// run's total wall clock to the stream.
// maxRuntimeSnapshotMetadataBytes bounds one runtime snapshot body. State rejects a
// checkpoint batch above its own bound, so the writer stops well below it: the durable
// stream remains authoritative and an oversized history costs one log line, not a
// multi-megabyte POST at every remaining step boundary.
const maxRuntimeSnapshotMetadataBytes = 4 << 20

func (c *Coordinator) streamAppendBudget() time.Duration {
	if c.streamAppend > 0 {
		return c.streamAppend
	}
	return DefaultStreamAppendTimeout
}

const (
	// terminalAppendAttempts bounds how often the writer repeats the terminal frame while
	// the plane has not confirmed it. Small on purpose: a blip clears in milliseconds, and
	// the run has to be closed durably in a bounded time either way.
	terminalAppendAttempts = 3
	// terminalRetryPerAttemptCap shortens the deadline of the RETRIES only. The first
	// attempt keeps the configured budget; a repeat exists to catch a transient failure,
	// so it must not multiply that budget across attempts and delay closing the run by
	// tens of seconds when State is genuinely wedged.
	terminalRetryPerAttemptCap = 2 * time.Second
)

// terminalAppendRetryable reports whether an unconfirmed terminal append may be repeated.
//
// An EMPTY contract code means the failure never carried a refusal: the connection died,
// the deadline passed, or State answered 5xx. In every one of those cases the write may
// already be durable and the ACK is what was lost - which is precisely why the repeat
// sends the SAME key and body: State's (key, kind, payload) digest turns the repeat into
// a read-back of the stored event rather than a second frame.
//
// A DEFINITIVE code says the opposite: a fence (STALE_EPOCH / lease lost), a permission,
// an argument or a key reuse refused the write. A fence does not loosen because the loser
// asks again, and repeating into a takeover turns one stale writer into a stampede, so
// those are returned on the spot.
func terminalAppendRetryable(err error) bool {
	return contracts.Code(err) == ""
}

// appendDurableTerminal writes a run's TERMINAL status frame, repeating the identical
// idempotent append while the plane has not confirmed it.
//
// This single frame is different from every other one: the run's lease closes with the
// run, and a cancel revokes the run's own credential, so once this call returns nobody -
// not this worker, not a takeover, not the orphan sweep - can ever write it again. A
// transient failure here is therefore not a wart in the log, it is a thread whose durable
// stream never ends: the durable twin of the canceled-thread silence (D4).
//
// Failures are still reported, never swallowed: the caller logs the returned error, and
// the SSE edge independently closes a durably finished thread, so no reader is left
// hanging while the durable record is being repaired.
func (c *Coordinator) appendDurableTerminal(ctx context.Context, token, threadID, runID, worker string, epoch int64, key, kind string, payload json.RawMessage) error {
	var lastErr error
	for attempt := 1; attempt <= terminalAppendAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(time.Duration(attempt*attempt) * 50 * time.Millisecond)
		}
		budget := c.streamAppendBudget()
		if attempt > 1 && budget > terminalRetryPerAttemptCap {
			budget = terminalRetryPerAttemptCap
		}
		actx, cancel := context.WithTimeout(ctx, budget)
		_, err := c.state.AppendStream(actx, token, threadID, runID, worker, epoch, key, kind, payload)
		cancel()
		if err == nil {
			if attempt > 1 {
				slog.Info("space: terminal status event became durable on repeat",
					"run_id", runID, "thread_id", threadID, "stream_key", key, "attempt", attempt)
			}
			return nil
		}
		lastErr = err
		if !terminalAppendRetryable(err) {
			return err
		}
		slog.Warn("space: terminal status append unconfirmed, repeating the identical idempotent write",
			"run_id", runID, "thread_id", threadID, "stream_key", key,
			"epoch", epoch, "attempt", attempt, "err", err.Error())
	}
	return lastErr
}

// AcceptTurn creates a new turn: creates or finds a thread, creates a run,
// and returns immediately. The caller then dispatches via DispatchTurn.
func (c *Coordinator) AcceptTurn(ctx context.Context, taskToken string, scope contracts.Scope, req AcceptTurnRequest) (*AcceptTurnResponse, error) {
	// This call is the last point that knows a turn was requested before a run grant can be refused, and a
	// refused grant leaves nothing durable but its log record. So the record's correlation id is stated at
	// THIS entry, not only at the HTTP edge: over HTTP the edge already put one on the context and it is
	// kept verbatim (the caller was answered with exactly that value); a caller that came in without an
	// edge at all still gets a record that names its request.
	ctx = WithRequestID(ctx, resolveRequestID(RequestIDFromContext(ctx)))

	// DEF-H30-x: a deployment that REQUIRES a dispatcher but has none declared here must
	// not accept a human turn into a state nothing will ever advance. Refuse honestly,
	// before any durable write, with the reason naming the missing capability (503). A
	// front that does not require a dispatcher falls through to the bounded-window
	// watchdog below, which still guarantees a determined terminal over an eternal pending.
	if c.requireDispatcher && !c.dispatcherKnown.Load() {
		return nil, contracts.NewError(contracts.ErrUnavailable,
			"space: no turn dispatcher is wired on this front: "+ReasonNoTurnDispatcher)
	}
	var thread *threadResponse
	var err error
	if req.ThreadID != "" {
		// State derives ownership from the verified task token; an arbitrary
		// thread ID from the browser cannot cross its subject/deployment scope.
		thread, err = c.state.GetThread(ctx, taskToken, req.ThreadID)
		if err != nil {
			return nil, fmt.Errorf("continue turn thread: %w", err)
		}
		if req.ContextID != "" && req.ContextID != thread.ContextID {
			return nil, contracts.NewError(contracts.ErrConflict, "space: thread_id and context_id name different conversations")
		}
		req.ContextID = thread.ContextID
	}

	// A context the artifact plane cannot name can never carry this run's
	// artifacts, and the failure would surface as every save_artifact of the run
	// answering INVALID_ARGUMENT (DEF-h26-1). Space cannot make such a context
	// produce artifacts either, so it refuses it here, before any durable write,
	// and names the constraint. The shape is the artifact plane's own predicate,
	// not a copy of it, so the two cannot drift.
	if !artifacts.IsValidContextID(req.ContextID) {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"space: context id "+fmt.Sprintf("%q", req.ContextID)+
				" cannot carry artifacts: a conversation id must be one bounded path-safe token (letters, digits, . _ : -), no '/'")
	}

	// Resolve the conversation executor before consulting the Dot config owner.
	// A custom agent owns its workflow and must never acquire a Dot config by
	// omission on a later turn or after a Coordinator restart.
	var executorRuns []runResponse
	if c.agentPinner != nil || req.AgentInstallationID != "" {
		if req.AgentInstallationID != "" && (c.agentPinner == nil || c.agentControl == nil || !c.requireRunGrant || c.runGrants == nil) {
			return nil, contracts.NewError(contracts.ErrUnavailable, "space: agent executor requires owner transport and a per-run grant")
		}
		if thread == nil {
			thread, err = c.state.CreateThread(ctx, taskToken, scope, req.ContextID, req.Key)
			if err != nil {
				return nil, fmt.Errorf("accept agent conversation: %w", err)
			}
		}
		executorRuns, err = c.state.ListRuns(ctx, taskToken, thread.ID)
		if err != nil {
			return nil, fmt.Errorf("read conversation executor: %w", err)
		}
		req.AgentInstallationID, err = conversationExecutorBinding(executorRuns, req.AgentInstallationID)
		if err != nil {
			return nil, err
		}
		if req.AgentInstallationID != "" && (c.agentControl == nil || !c.requireRunGrant || c.runGrants == nil) {
			return nil, contracts.NewError(contracts.ErrUnavailable, "space: agent executor requires owner transport and a per-run grant")
		}
	}
	if req.AgentInstallationID != "" && req.AgentConfig != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "agent_config is not a custom agent routing contract")
	}
	if c.configPinner != nil && req.AgentInstallationID == "" {
		if req.AgentConfig != nil {
			return nil, contracts.NewError(contracts.ErrInvalid, "agent_config is server-owned")
		}
		// The ingress task is stable across retries; Registry derives ownership
		// from that token and returns the original snapshot for this operation.
		req.AgentConfig, err = c.configPinner.Pin(ctx, taskToken, req.Key, req.ContextID)
		if err != nil {
			return nil, err
		}
		if req.AgentConfig.Dot == nil {
			return nil, contracts.NewError(contracts.ErrUnavailable, "missing pinned Dot config")
		}
		if req.MaxSteps <= 0 || req.MaxSteps > req.AgentConfig.Dot.MaxSteps {
			req.MaxSteps = req.AgentConfig.Dot.MaxSteps
		}
	}
	maxSteps := req.MaxSteps
	// An absent step budget STAYS absent. Filling the ingress default here used to make
	// "the caller asked for 64" and "nobody stated a budget" the same durable bytes, and
	// the durable payload is exactly what a cold-resumed run reads back - so by the time
	// Core saw the request, the one consumer that needs the difference (a run delegating
	// to a SECOND DEPLOYMENT) could only forward a will it had invented, and the execution
	// plane would journal step_budget_source=caller for a number no user named. Core
	// bounds the loop with the same 64 steps (internal/core/runtime.go): what stops here
	// is the false attribution, not the bound. A negative value is not a stated budget
	// either, and is not forwarded.
	if maxSteps < 0 {
		maxSteps = 0
	}

	if thread == nil {
		thread, err = c.state.CreateThread(ctx, taskToken, scope, req.ContextID, req.Key)
		if err != nil {
			return nil, fmt.Errorf("accept turn thread: %w", err)
		}
	}
	if c.projectMemory {
		var runs []runResponse
		if req.ThreadID != "" {
			runs, err = c.state.ListRuns(ctx, taskToken, thread.ID)
			if err != nil {
				return nil, fmt.Errorf("read conversation project binding: %w", err)
			}
		}
		project, profile, err := conversationMemoryBinding(runs, req.ProjectID, req.ProjectMemoryProfile)
		if err != nil {
			return nil, err
		}
		req.ProjectID, req.ProjectMemoryProfile = project, profile
		if req.ProjectID != "" {
			check := c.checkProject
			if profile == "shared" {
				check = c.checkSharedProject
			}
			if check == nil {
				return nil, contracts.NewError(contracts.ErrForbidden, "space: project authority is not configured")
			}
			if err := check(ctx, taskToken, req.ContextID, req.ProjectID, req.Key); err != nil {
				return nil, err
			}
		}
	} else if req.ProjectID != "" || req.ProjectMemoryProfile != "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "space: project memory is not configured")
	}

	pp := map[string]any{
		"type":      "turn",
		"input":     req.Input,
		"max_steps": maxSteps,
	}
	if req.AgentConfig != nil {
		pp["agent_config"] = req.AgentConfig
	}
	if req.ProjectID != "" {
		pp["project_id"] = req.ProjectID
	}
	if req.ProjectMemoryProfile != "" {
		pp["project_memory_profile"] = req.ProjectMemoryProfile
	}

	// Tier-2 order (decision 2026-10-30-exchange-scope-authority-model): the RUN must be
	// created UNDER its own child (run) grant, so the durable Run.TaskID is the child task,
	// not the ingress authority. The child task id is SERVER-OWNED and IDEMPOTENT (derived
	// from scope + the caller idempotency Key). Do NOT bypass the State binding: State's
	// createRun sets Run.TaskID from the PRESENTING token's Claims.Task, so we present the
	// child token to CreateRun.
	runToken := taskToken
	var childTask string
	var ingressTask string
	if c.requireRunGrant {
		if c.runGrants == nil {
			return nil, contracts.NewError(contracts.ErrForbidden,
				"space: run grant required but no delegate issuer configured")
		}
		// The child run grant is a CHILD of the Tier-1 ingress authority root; Identity
		// authorizes the delegate against the authority's task. It is resolved ONLY from
		// the verified request context (never the body/path).
		ingressTask = AuthorityTaskFromContext(ctx)
		if ingressTask == "" {
			return nil, contracts.NewError(contracts.ErrForbidden,
				"space: run grant requires the ingress authority task (human->task exchange)")
		}
		childTask = "rtask-" + scope.Subject + ":" + req.Key
		pp["run_task"] = childTask // durable ref for cold-resume (a resource id, NOT a credential)
		// The grant's context claim is the CONVERSATION (threads.context_id), never the
		// thread row's id (DEF-DOTE2E-1). One source, and it is the one every consumer
		// already reads: CoreRequest.ContextID is set from thread.ContextID below and its
		// own doc calls it "the same value a task token carries as its context claim";
		// cmd/as-runner's scoped core guard refuses a request whose ContextID differs from
		// the claim; and the artifact plane binds each row a run writes to that claim so the
		// data is findable and deletable with the conversation. Claiming thread.ID broke
		// both halves - a Dot turn carrying its context died with "Core request differs from
		// verified task authority", and one carrying no context was silently rebound to a
		// thread id, leaving artifacts outside the conversation that is supposed to own them.
		tok, derr := c.childGrant(ctx, taskToken, ingressTask, childTask, thread.ContextID, scope.Subject)
		if derr != nil {
			return nil, derr
		}
		runToken = tok
	}

	var agentTarget *AgentTargetRef
	if req.AgentInstallationID != "" {
		agentTarget, err = c.agentPinner.PinAgentTurn(ctx, runToken, req.AgentInstallationID)
		if err != nil {
			return nil, err
		}
		if agentTarget == nil || agentTarget.InstallationID != req.AgentInstallationID || agentTarget.PinID == "" {
			return nil, contracts.NewError(contracts.ErrUnavailable, "space: agent owner returned an invalid target")
		}
		if err := compatibleConversationAgentState(executorRuns, agentTarget); err != nil {
			return nil, err
		}
		pp["agent_installation_id"], pp["agent_target"] = req.AgentInstallationID, agentTarget
	}
	if c.agentPinner != nil {
		// Enqueue's scoped unique key + body digest makes the FIRST selection
		// atomic across Coordinator replicas. History alone cannot arbitrate two
		// concurrent first turns. Version/config remain per-turn, outside this key.
		binding, _ := json.Marshal(map[string]string{"installation_id": req.AgentInstallationID})
		if _, err := c.state.Enqueue(ctx, runToken, thread.ID, "space:executor-binding:v1", "executor_binding", binding); err != nil {
			if contracts.Code(err) == contracts.ErrKeyReuse || contracts.Code(err) == contracts.ErrInvalid && contracts.ReasonOf(err) == contracts.ErrKeyReuse {
				return nil, contracts.NewError(contracts.ErrConflict, "space: start a new conversation to change its agent")
			}
			return nil, fmt.Errorf("bind conversation executor: %w", err)
		}
	}

	payload, _ := json.Marshal(pp)
	run, err := c.state.CreateRun(ctx, runToken, thread.ID, req.Key+":run", payload, childTask)
	if err != nil {
		return nil, fmt.Errorf("accept turn run: %w", err)
	}

	c.mu.Lock()
	if live, ok := c.turns[run.ID]; ok {
		// Idempotent replay: the caller's AcceptTurn ACK was lost and it retried the same Key, so
		// State handed back the run it had already created. This is the SAME turn, already tracked
		// and possibly already running. Replacing the map entry with a fresh pending Turn would
		// clobber the live one this worker is executing - wiping status/steps/checkpoint behind a
		// run whose durable row already says running - and would answer the caller with a state the
		// run is not in. Report what is actually there; a replay creates nothing new (D15's rule: a
		// re-delivered request must not rewrite live state the durable layer does not sanction).
		snap := *live
		c.mu.Unlock()
		return &AcceptTurnResponse{TurnID: snap.ID, ThreadID: snap.ThreadID, Status: snap.Status, ProjectID: snap.ProjectID, ProjectMemoryProfile: snap.ProjectMemoryProfile, AgentInstallationID: snap.AgentInstallationID}, nil
	}
	turn := &Turn{
		ProjectID:            req.ProjectID,
		ProjectMemoryProfile: req.ProjectMemoryProfile,
		ID:                   run.ID,
		RunID:                run.ID,
		ThreadID:             thread.ID,
		RunGrant:             runToken,
		RunTask:              childTask,
		Scope:                scope,
		Input:                req.Input,
		Status:               TurnPending,
		CreatedAt:            time.Now().UTC(),
		UpdatedAt:            time.Now().UTC(),
		MaxSteps:             maxSteps,
		AgentConfig:          req.AgentConfig,
		AgentInstallationID:  req.AgentInstallationID,
		AgentTarget:          agentTarget,
	}
	c.turns[turn.ID] = turn
	c.mu.Unlock()

	// DEF-H30-x: with no dispatcher declared for this process, arm a bounded watchdog so a
	// turn that will never be claimed surfaces a determined terminal + cause instead of an
	// eternal `pending`. A declared dispatcher owns the lifecycle (its preflight already
	// terminalizes failures), so no watchdog arms beside a healthy runner.
	if !c.dispatcherKnown.Load() && c.dispatchTimeout > 0 {
		go c.watchDispatch(turn.ID, c.dispatchTimeout)
	}

	return &AcceptTurnResponse{
		AgentInstallationID:  req.AgentInstallationID,
		ProjectID:            req.ProjectID,
		ProjectMemoryProfile: req.ProjectMemoryProfile,
		TurnID:               turn.ID,
		ThreadID:             thread.ID,
		Status:               turn.Status,
	}, nil
}

// childGrant obtains a per-run (child) Identity Delegate grant under the ingress authority
// for a SERVER-OWNED idempotent child task id. parentTask is the Tier-1 ingress authority
// task the child is rooted under (Identity authorizes the delegate against it). Fail closed:
// a missing parent or a failed delegate returns an error, never the ingress token.
// childGrant mints the Tier-2 run grant. contextID is the conversation the run belongs to
// (threads.context_id) - the value the run's writes are recorded under - and deliberately not
// the thread id; see the call site for why only one of the two is honest.
func (c *Coordinator) childGrant(ctx context.Context, ingress, parentTask, childTask, contextID, actor string) (string, error) {
	if c.runGrants == nil {
		return "", contracts.NewError(contracts.ErrForbidden, "space: run grant issuer not configured")
	}
	if parentTask == "" {
		return "", contracts.NewError(contracts.ErrForbidden, "space: child grant needs the ingress authority task (parent)")
	}
	if childTask == "" {
		return "", contracts.NewError(contracts.ErrForbidden, "space: child grant needs a durable child task id")
	}
	ttl := c.runGrantTTL
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	tok, derr := c.runGrants.Delegate(ctx, ingress, parentTask, RunGrantSpec{
		Task: childTask, Context: contextID, Capabilities: c.runGrantCaps, TTL: ttl,
		MaxActive: c.runGrantActive, MaxFanout: c.runGrantFanout,
	})
	if derr != nil {
		logRunGrantRefusal(ctx, derr, contextID)
		return "", derr
	}
	return tok, nil
}

// logRunGrantRefusal records WHY a run grant was refused. The refusal happens before State has a run
// row, so nothing durable carries its cause afterwards: the stand then shows a run that never appeared
// with no reason attached, which is the same unreadable shape as a terminal "failed" without a cause.
// One record, narrow on purpose: the request id, the contract class, the bounded reason (it already
// names the upstream status and Identity's own code), and the conversation the run was for.
//
// It deliberately excludes the bearer/ingress token, the subject, and childTask - that id is
// "rtask-"+subject+":"+key, so it would re-enter the subject through the back door. Trace ids stay
// conditional on a span, because a span is optional composition; they are now a supplement for following
// a trace further, never the join key - request_id is always stated, which is what a reader needs to
// match this record against the failure a caller saw (directive 2026-11-10 п.3).
func logRunGrantRefusal(ctx context.Context, err error, contextID string) {
	// request_id first: it is the join key a reader starts from, and unlike the trace ids below it is
	// always present. "none" means the caller came in without the HTTP edge AND without stating an id at
	// this entry - an honest absence, never a blank that looks like a value.
	requestID := RequestIDFromContext(ctx)
	if requestID == "" {
		requestID = "none"
	}
	attrs := []any{
		"request_id", requestID,
		"context_id", contextID,
		"contract_code", contracts.Code(err),
		"reason", contracts.ReasonOf(err),
		"cause", truncateOneLine(err.Error(), 240),
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		attrs = append(attrs, "trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
	}
	slog.Warn("space: run grant refused before the run row exists", attrs...)
}

// DispatchTurn claims the next pending run and starts Core execution in a
// goroutine. Returns immediately; progress flows through stream events.
//
// scope is the authenticated caller principal scope. Because the State API
// isolates runs by the task-token principal, any run ClaimRun returns belongs
// to this scope; a cold-rehydrated turn (the in-memory turn was lost on a
// process restart) MUST therefore be bound to it, otherwise the per-subject
// authorization at the HTTP boundary would deny the legitimate owner access to
// their own resumed turn after a restart.
func (c *Coordinator) DispatchTurn(ctx context.Context, taskToken string, scope contracts.Scope) error {
	claim, err := c.state.ClaimRun(ctx, taskToken, c.worker, claimLeaseTTL)
	if err != nil {
		return fmt.Errorf("dispatch claim: %w", err)
	}
	if claim.Lease == nil {
		return nil
	}
	c.startClaimedTurn(claim)
	return nil
}

// claimLeaseTTL is the lease requested by EVERY claim path - the client-triggered
// DispatchTurn and the background orphan sweep alike - so both hold the thread for
// exactly the same bounded window and heartbeats extend it the same way.
const claimLeaseTTL = 30 * time.Second

// maxSweepPerTick bounds how many runs one sweep tick may take over, so a backlog of
// orphans can never monopolise the worker or starve the ticker.
const maxSweepPerTick = 8

// startClaimedTurn rehydrates a turn this process has no memory of and starts Core for a
// lease the caller already holds. BOTH claim paths share it, so a cold resume triggered by
// a client and a cold resume triggered by the sweep obey identical ownership rules: the
// turn is bound to the durable LEASE scope, never to whoever happened to trigger it.
//
// It reports whether a Core execution was started. If this very process is already
// executing the run, the previous execution context is canceled first: State only ever
// hands out a lease whose thread lease expired, so a "live" local turn means our own
// heartbeat lapsed and the older epoch is already fenced out of State. Leaving it running
// beside the new epoch would duplicate tool effects, and skipping would wedge the run as
// `running` forever, so the stale epoch is stopped and the new epoch takes over.
func (c *Coordinator) startClaimedTurn(claim *claimResponse) bool {
	lease := claim.Lease
	turnID := lease.Run.ID

	c.mu.Lock()
	if stale, live := c.active[turnID]; live {
		slog.Warn("space: reclaiming a run this worker still executes; canceling the stale epoch",
			"run_id", turnID, "new_epoch", lease.Epoch)
		stale()
	}
	turn := c.turns[turnID]
	if turn == nil {
		turn = &Turn{
			ID:       turnID,
			ThreadID: lease.Thread.ID,
			// Durable Tier-2 grant ref: State bound Run.TaskID to the presenting child
			// grant's Claims.Task at CreateRun. A cold-rehydrated turn MUST recover it so
			// executeTurn re-mints the CHILD grant (never the ingress authority).
			RunTask:        lease.Run.TaskID,
			Scope:          lease.Scope,
			coldRehydrated: true,
			Status:         TurnRunning,
			CreatedAt:      time.Now().UTC(),
		}
		// The persisted run payload carries the original turn input and step
		// budget set at AcceptTurn time. Restore them so a cold-resumed turn keeps its
		// bounded-step limit and original input even when it restarts before any
		// checkpoint exists. No default is filled here either: a payload carrying no
		// stated budget means nobody stated one, and a restart must not manufacture a
		// caller will (Core applies its own fallback bound to the loop, and labels it
		// as its own).
		restoreTurnFromRunPayload(turn, lease.Run.Payload)
		// SP-2: the durable cold-recovery reads (latest checkpoint via ListCheckpoints and
		// the OG-2 step-stream high-water) are performed in executeTurn PREFLIGHT, BEFORE
		// Core starts, with fail-closed error handling. A failed recovery read must never be
		// mistaken for "no checkpoint" (which would silently replay already-applied tool
		// effects from turn.Input). Nothing durable is read here, under the lock.
		c.turns[turnID] = turn
	}
	turn.Status = TurnRunning
	turn.UpdatedAt = time.Now().UTC()
	turn.leaseEpoch = lease.Epoch
	// The lease identity is recorded HERE, under the same lock that registers the
	// execution, so "this process executes the turn" and "the lease identity this
	// process writes durable artifacts with" become true at the same instant. A
	// snapshot saved right after a claim must never see a half-initialized fence
	// (an empty worker or epoch 0) and be told the lease owner is someone else.
	turn.RunID, turn.LeaseWorker, turn.LeaseEpoch = lease.Run.ID, lease.Worker, lease.Epoch

	runCtx, cancel := context.WithCancel(aot.WithTraceParent(context.Background(), lease.Run.TraceParent))
	c.active[turnID] = cancel
	c.mu.Unlock()

	go func() {
		ctx, span := aot.Tracer("ahp.space").Start(runCtx, "space.run", trace.WithAttributes(
			RunTraceAttributes(lease.Scope.Tenant, lease.Scope.Deployment, lease.Run.TaskID, lease.Run.ID, lease.Run.ThreadID)...,
		))
		defer span.End()
		c.executeTurn(ctx, turn, lease, claim.TaskToken)
	}()
	return true
}

// StartOrphanClaimLoop is the runner side of the PDF orphan pickup ("подбор сирот"):
// "Аренда, подбор сирот, снимки и намерения — примитивы State API", and "под упал после
// отправки вызова: другая реплика подбирает задачу". ClaimRun is the deployment-level
// workload route (transport.Workload: identity comes from the verified workload
// certificate alone, no human/task bearer is accepted), so a runner sweeps for work with
// its OWN identity and needs no client present:
//
//   - a run left `running` by a dead/heart-beat-lapsed worker becomes claimable the moment
//     its lease expires in the database clock;
//   - a run that was accepted but never dispatched is claimable immediately.
//
// Without this loop a run advances only while some client calls DispatchTurn, which makes
// recovery contingent on the client and silently no-ops after a worker-only restart.
// interval <= 0 disables the sweep and keeps exactly the previous behaviour.
func (c *Coordinator) StartOrphanClaimLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	// A claim loop IS a turn dispatcher: declaring it here is what lets AcceptTurn tell
	// "no dispatcher will ever claim this" apart from "a dispatcher is running and will."
	// A runner starts this before it serves, so no accepted turn is ever seen with the
	// flag false on a healthy plane (DEF-H30-x).
	c.dispatcherKnown.Store(true)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.sweepClaims(ctx)
			}
		}
	}()
}

// sweepClaims drains up to maxSweepPerTick claimable runs in one tick. A failed claim is
// logged once per distinct error (State outages must not spam the log every tick) and is
// retried on the next tick; it is never treated as "nothing to do".
func (c *Coordinator) sweepClaims(ctx context.Context) {
	for i := 0; i < maxSweepPerTick; i++ {
		claim, err := c.state.ClaimRun(ctx, "", c.worker, claimLeaseTTL)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return // shutdown: the sweep context is gone, nothing was lost
			}
			msg := err.Error()
			c.mu.Lock()
			fresh := c.lastSweepErr != msg
			if fresh {
				c.lastSweepErr = msg
			}
			c.mu.Unlock()
			if fresh {
				slog.Warn("space: orphan sweep claim failed", "worker", c.worker, "err", msg)
			}
			return
		}
		c.mu.Lock()
		hadErr := c.lastSweepErr != ""
		c.lastSweepErr = ""
		c.mu.Unlock()
		if hadErr {
			slog.Info("space: orphan sweep recovered", "worker", c.worker)
		}
		if claim.Lease == nil {
			return
		}
		slog.Info("space: orphan sweep claimed a run", "run_id", claim.Lease.Run.ID,
			"thread_id", claim.Lease.Thread.ID, "worker", claim.Lease.Worker, "epoch", claim.Lease.Epoch)
		c.startClaimedTurn(claim)
	}
}

// stepErrorCause pulls the cause out of a Core StepError frame. Core writes
// {"error": "..."} (internal/core/runtime.go), and that text is what the thread stream
// carries, so the same extraction keeps GetTurn and the stream saying one thing. Anything
// unparseable is reported verbatim rather than dropped: an unreadable cause still beats an
// absent one.
func stepErrorCause(content json.RawMessage) string {
	if len(content) == 0 {
		return ""
	}
	var frame struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(content, &frame); err == nil && frame.Error != "" {
		return frame.Error
	}
	return string(content)
}

// executeTurn runs the Core loop for a single turn with heartbeat keepalive.
func (c *Coordinator) executeTurn(ctx context.Context, turn *Turn, lease *leaseResponse, taskToken string) {
	released := false
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.turns[turn.ID] != turn || turn.leaseEpoch != lease.Epoch {
			return
		}
		delete(c.active, turn.ID)
		if released && turn.Status.IsTerminal() {
			c.rememberTerminalCheckpoint(turn)
		}
	}()

	// failPreflight is used by EVERY preflight branch, all of which run BEFORE Core starts, so a
	// preflight failure guarantees zero Core/model/tool calls and zero durable appends (SP-1/SP-2).
	// It marks the in-memory turn failed (so GetTurn surfaces it to readers) and releases the lease
	// so a retry can reclaim the run. It deliberately performs NO stream append: the trigger for a
	// preflight failure IS a failed durable read, so a status append would be unreliable AND would
	// consume a StepKey that a resumed run must still be able to use.
	failPreflight := func(cause string) {
		c.mu.Lock()
		turn.Status = TurnFailed
		// D8: the cause a preflight refusal knows about reaches the reader too - a turn that
		// failed before Core started is exactly the case where no stream frame explains it.
		turn.FailureReason = cause
		turn.UpdatedAt = time.Now().UTC()
		c.mu.Unlock()
		// J10: the cause a preflight refusal knows is a durable fact the READER needs in
		// the topology that has one - the front answers GetTurn, this runner is what
		// failed - so it is logged here AND written into the Run row on the same release
		// that ends the lease (state RunPayloadFailureCause). Without that, the only
		// process that ever knew why the turn died forgets it on restart, and the front's
		// cold read can only say "no cause frame" - the exact defect measured on the live
		// journey. No stream append (the trigger for a preflight failure IS a failed
		// durable read, and a StepKey a resumed run still needs must not be consumed);
		// the run-row payload is the cause's one durable home.
		slog.Error("space: turn failed in preflight",
			"turn_id", turn.ID, "run_id", lease.Run.ID, "thread_id", turn.ThreadID,
			"tenant", lease.Scope.Tenant, "subject", lease.Scope.Subject,
			"deployment", lease.Scope.Deployment,
			"worker", lease.Worker, "epoch", lease.Epoch, "cause", cause)
		rctx, rcn := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer rcn()
		// A preflight refusal is a failure this plane caused before Core existed, so
		// the dictionary's generic word for it is the honest one: there is no model
		// verdict to classify, and inventing one would be the fabrication this whole
		// field exists to prevent. The plane's own sentence travels beside it verbatim.
		if err := c.state.ReleaseWithReasonCause(rctx, taskToken, lease.Run.ID, lease.Worker, lease.Epoch,
			"failed", gateway.CloseReasonError, cause); err != nil {
			slog.Warn("space: preflight failure could not record its cause on the run row",
				"turn_id", turn.ID, "run_id", lease.Run.ID, "err", err.Error())
		}
	}

	// E-run (frozen contract): State's ClaimRun already reissued a FRESH task token bound to
	// the durable Run.TaskID (the child run task). Present it for BOTH warm and cold runs;
	// never re-delegate/re-mint here and never fall back to the ingress authority.
	runTok := taskToken
	c.mu.Lock()
	turn.RunID, turn.LeaseWorker, turn.LeaseEpoch = lease.Run.ID, lease.Worker, lease.Epoch
	c.mu.Unlock()
	if c.requireRunGrant {
		// Fail closed unless the durable run is rooted on a CHILD run task (not the ingress
		// authority): a run bound to the shared authority means the Tier-2 boundary was
		// bypassed at CreateRun, which must never execute.
		if !isChildRunTask(lease.Run.TaskID) {
			failPreflight("space: run is not rooted on its own child grant")
			return
		}
		c.mu.Lock()
		turn.RunGrant = runTok // current child token, used by CancelTurn as the revoke bearer
		c.mu.Unlock()
	}

	// Addressed agents execute in their own runtime. The Coordinator owns only
	// the conversation projection and its lease, never the agent's workflow.
	if turn.AgentInstallationID != "" || turn.AgentTarget != nil {
		if !c.requireRunGrant {
			failPreflight("space: addressed agent executor requires a per-run child grant")
			return
		}
		if c.agentControl == nil || turn.AgentInstallationID == "" || turn.AgentTarget == nil || turn.AgentTarget.InstallationID != turn.AgentInstallationID {
			failPreflight("space: addressed agent executor is not configured")
			return
		}
		c.executeAgentTurn(ctx, turn, lease, runTok)
		return
	}

	// --- PREFLIGHT: ALL durable recovery reads complete BEFORE Core starts (SP-1). The recovery
	// read budget is deliberately separate from the run's append budget, so a long high-water scan
	// cannot eat the time the streaming phase still needs.
	preflightCtx, cancelPreflight := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancelPreflight()

	// SP-2: a cold rehydrate MUST distinguish "no checkpoint persisted yet" from "the durable
	// read failed". A read error is NOT empty state: starting from turn.Input after a real
	// crash would replay tool effects that already happened, so fail closed on any error.
	if turn.coldRehydrated {
		cps, err := c.state.ListCheckpoints(preflightCtx, runTok, lease.Thread.ID, CheckpointNamespace, 1)
		if err != nil {
			failPreflight("space: durable checkpoint read failed: " + err.Error())
			return
		}
		if len(cps) > 0 && checkpointBelongsToRun(cps[0], lease.Run) {
			cp := cps[0]
			recovered := &Checkpoint{
				ID: cp.ID, Namespace: cp.Namespace, Parent: cp.Parent, Version: cp.Version,
				Metadata: cp.Metadata, Parts: cp.Parts, TurnID: turn.ID, Sequence: int(cp.Version),
				Messages: checkpointMessages(cp.Metadata), State: cp.Metadata, CreatedAt: cp.CreatedAt,
			}
			c.mu.Lock()
			turn.Checkpoint = recovered
			c.mu.Unlock()
		}
	}

	// OG-2: seed the per-turn step-stream sequence from the durable high-water so a cold
	// rehydrate never reuses a prior-life StepKey with a new payload (KEY_REUSE_MISMATCH). A
	// failed/incomplete read fails closed BEFORE Core starts.
	// The turn fields are read under the coordinator mutex and the recovered checkpoint is
	// captured in a local, so the Core goroutine never touches shared turn state.
	c.mu.Lock()
	resumeCheckpoint := turn.Checkpoint
	needScan := turn.Steps > 0 || turn.Checkpoint != nil || turn.coldRehydrated
	c.mu.Unlock()
	seq := 0
	var resumeFrames []durableStepFrame
	if needScan {
		hw, frames, hwe := c.durableStepTranscript(preflightCtx, runTok, turn.ThreadID, turn.ID)
		if hwe != nil {
			failPreflight("space: durable step-key high-water read failed: " + hwe.Error())
			return
		}
		seq = hw
		resumeFrames = frames
	}

	priorMessages, historyErr := c.priorConversationMessages(preflightCtx, runTok, lease.Run)
	if historyErr != nil {
		failPreflight("space: durable conversation history read failed: " + historyErr.Error())
		return
	}

	// --- Recovery complete; only now start the heartbeat and Core. ---
	// Durable streaming: every Core step is appended to the thread stream so readers (chat UI,
	// delegating agent) can follow progress. Detached from the run context so cancellation is
	// still observable to readers.
	// The stream base carries NO deadline: a real model turn legitimately runs for minutes
	// (one long report step alone exceeded the previous shared 30s window), and a single shared
	// deadline turned a healthy long run into a bogus durability failure the moment it expired -
	// every later append, including the terminal status append, was then rejected. The budget
	// belongs to the individual append, never to the whole lifetime of the run.
	streamBase := context.WithoutCancel(ctx)
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go c.heartbeat(hbCtx, runTok, lease.Run.ID, lease.Worker, lease.Epoch)

	// Core runs on a context WE own and cancel on any hazard (stream hole) so execution stops,
	// not merely emission. coreCtx also propagates CancelTurn/parent cancellation.
	coreCtx, coreCancel := context.WithCancel(ctx)
	defer coreCancel()

	// cancelObserved is set when the holder finds a cancel INTENT in this run's durable
	// journal (space.CancelRequestedKey). It is atomic because the watcher below writes it
	// while the terminal path below reads it.
	var cancelObserved atomic.Bool
	var cancelCursor atomic.Int64
	// A cancel that arrives while THIS process holds the lease is not the asker's to write:
	// the fence is ours, and the honest form of "the user asked you to stop" is the append-only
	// journal event the asker writes (space.CancelRequestedKey), not a status stamped from
	// outside the fence. Refusing such a cancel with 409 and writing nothing (the behaviour
	// this replaces) was honest but invisible - a user could not tell "queued" from "lost".
	// So the HOLDER watches the journal and ends the run itself, keeping every effect it
	// already got accepted.
	//
	// Detection bound: one tick, far inside the 30s lease TTL the heartbeat renews (PDF
	// rev3.2 §3.6 c.36), so a run cannot stay leased-and-cancelling without an outcome. A
	// failed read is never read as a cancel and never fails the run either: "State did not
	// answer" must not invent an outcome any more than "State answered" may hide one.
	go func() {
		ticker := time.NewTicker(cancelObserveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-coreCtx.Done():
				return
			case <-ticker.C:
			}
			found, next, err := c.scanCancelIntent(coreCtx, runTok, turn.ThreadID, lease.Run.ID, cancelCursor.Load())
			if err != nil {
				if coreCtx.Err() == nil {
					slog.Warn("space: cancel intent check failed; the run continues, nothing is invented",
						"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "err", err.Error())
				}
				continue
			}
			cancelCursor.Store(next)
			if !found {
				continue
			}
			cancelObserved.Store(true)
			slog.Info("space: cancel intent observed in the durable journal, the lease holder ends the run",
				"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "epoch", lease.Epoch,
				"close_reason", CloseReasonCancelRequested)
			coreCancel()
			return
		}
	}()

	results := make(chan StepResult, 32)
	done := make(chan error, 1)
	go func() {
		defer close(results)
		req := &CoreRequest{
			ProjectID:            turn.ProjectID,
			ProjectMemoryProfile: turn.ProjectMemoryProfile,
			TurnID:               turn.ID, ThreadID: lease.Thread.ID, RunID: lease.Run.ID,
			// The conversation this run belongs to (State's threads.context_id) - the same
			// value a task token carries as its context claim. What a run writes is
			// recorded under it, so the platform can find and delete that data by
			// conversation without asking the deploy for anything.
			ContextID: lease.Thread.ContextID,
			Worker:    lease.Worker, Epoch: lease.Epoch, TaskToken: runTok,
			Input: turn.Input, MaxSteps: turn.MaxSteps, Scope: turn.Scope,
			AgentConfig: turn.AgentConfig,
		}

		if resumeCheckpoint != nil && len(resumeCheckpoint.Messages) > 0 {
			req.Checkpoint = resumeCheckpoint
			req.Messages = resumeCheckpoint.Messages
		}
		// A snapshot is written at step boundaries but it is NOT the transcript, and the two
		// are deliberately held to different contracts: a refused PutCheckpoint is only logged
		// ("space: runtime snapshot write failed") and the checkpoint-batch bound stops the
		// writer for the rest of the run, while a failed AppendStream stops the RUN, because a
		// hole in the transcript is a replay hazard. So the transcript may legitimately run
		// ahead of the snapshot - and step keys continue from the transcript's high-water
		// above. Resuming from a snapshot that is behind it would then hand Core a history
		// missing those boundaries, and Core would produce each of them a SECOND time under a
		// fresh key: the same work twice in the thread, with nothing left to catch it.
		// The transcript is the source of truth, so when it is ahead, resume from it.
		if durable := len(resumeFrames); durable > checkpointStepCount(resumeCheckpoint) {
			if rebuilt := messagesFromTranscript(turn.Input, resumeFrames); rebuilt != nil {
				slog.Warn("space: checkpoint lags the durable transcript, resuming from the transcript",
					"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "epoch", lease.Epoch,
					"durable_steps", durable, "checkpoint_steps", checkpointStepCount(resumeCheckpoint))
				req.Messages = rebuilt
				if req.Checkpoint == nil {
					req.Checkpoint = &Checkpoint{TurnID: turn.ID, Sequence: durable}
				}
			}
		}
		if len(priorMessages) > 0 {
			var resumed []json.RawMessage
			if len(req.Messages) > 0 {
				if err := json.Unmarshal(req.Messages, &resumed); err != nil {
					done <- fmt.Errorf("space: invalid resumed conversation messages: %w", err)
					return
				}
			}
			// With no checkpoint Core appends this turn's new user input once.
			// With a checkpoint, resumed already includes the current input.
			req.Messages, _ = json.Marshal(append(priorMessages, resumed...))
		}
		replay, recoveryErr := c.modelReplay(coreCtx, runTok, turn.ThreadID, turn.ID)
		if recoveryErr != nil {
			done <- recoveryErr
			return
		}
		req.ModelReplay = replay
		if replay != nil {
			req.ModelStepOffset = replay.Step
		}
		req.JournalModel = func(ctx context.Context, step, attempt int, request json.RawMessage) error {
			payload, err := json.Marshal(modelIntent{Step: step, Attempt: attempt, Request: request})
			if err != nil {
				return err
			}
			appendCtx, cancel := context.WithTimeout(ctx, c.streamAppendBudget())
			defer cancel()
			_, err = c.state.AppendStream(appendCtx, runTok, turn.ThreadID, lease.Run.ID, lease.Worker, lease.Epoch, ModelIntentKey(turn.ID, step, attempt), "model_intent", payload)
			return err
		}
		done <- c.core.Run(coreCtx, req, results)
	}()

	var emitErr error
	var emitErrAt time.Time
	// lostLease marks that a durable write was refused with STALE_EPOCH: this process no
	// longer holds the run's lease, so a newer owner owns the verdict. It is deliberately NOT
	// emitErr - a fenced writer is not a durability hole, and the two must not share a status.
	var lostLease bool

	emit := func(kind string, payload json.RawMessage, deltaKey ...string) {
		if emitErr != nil || lostLease {
			return // a durable write already failed; stop rather than scatter keys past the hole
		}
		key := StepKey(turn.ID, seq)
		if len(deltaKey) > 0 {
			key = deltaKey[0]
		} else {
			seq++
		}
		appendCtx, cancelAppend := context.WithTimeout(streamBase, c.streamAppendBudget())
		_, err := c.state.AppendStream(appendCtx, runTok, turn.ThreadID, lease.Run.ID, lease.Worker, lease.Epoch, key, kind, payload)
		cancelAppend()
		if err != nil {
			// A STALE_EPOCH is not a durability hole: it means the lease was taken over (or
			// expired), so THIS worker is no longer entitled to record anything. It must not
			// declare the run failed - a new owner may finish it `done`. Stop emitting and
			// stop execution, but leave the verdict to State (see the status switch).
			if contracts.Code(err) == contracts.ErrStaleEpoch {
				lostLease = true
				slog.Warn("space: append refused with STALE_EPOCH, this worker no longer holds the lease",
					"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "epoch", lease.Epoch,
					"step_key", key, "kind", kind)
				coreCancel()
				return
			}
			// A durable stream hole is a durability failure and an OG-2 replay hazard: STOP
			// execution (not just emission) so no further tool effect is produced on a run we
			// can no longer record faithfully. The cause is logged: a silent hole used to be
			// indistinguishable from a model failure for every reader of the run.
			emitErr, emitErrAt = err, time.Now().UTC()
			slog.Warn("space: durable stream hole, run stopped",
				"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "epoch", lease.Epoch,
				"step_key", key, "kind", kind, "err", err.Error())
			coreCancel()
		}
	}

	// flushBoundary persists the runtime stream's VOLATILE tail at a checkpoint
	// boundary (step or turn) in one durable transaction. In the default chunk mode
	// it is a no-op. A refusal is handled exactly like a failed emit: STALE_EPOCH
	// means this worker lost the lease (leave the verdict to State), any other error
	// is a durability hole that stops execution and prevents a successful terminal.
	// Volatile-tail loss on a crash is an accepted, declared property of the
	// experiment; a flush failure is not -- it must stop the run.
	flushBoundary := func() {
		if contracts.CurrentCheckpointBoundary() == contracts.CheckpointBoundaryChunk {
			return
		}
		if emitErr != nil || lostLease {
			return
		}
		fctx, cancelFlush := context.WithTimeout(streamBase, c.streamAppendBudget())
		_, err := c.state.FlushStream(fctx, runTok, lease.Run.ID, lease.Worker, lease.Epoch)
		cancelFlush()
		if err == nil {
			return
		}
		if contracts.Code(err) == contracts.ErrStaleEpoch {
			lostLease = true
			slog.Warn("space: boundary stream flush refused with STALE_EPOCH, this worker no longer holds the lease",
				"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "epoch", lease.Epoch)
			coreCancel()
			return
		}
		emitErr, emitErrAt = err, time.Now().UTC()
		slog.Warn("space: boundary stream flush failed, run stopped",
			"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "epoch", lease.Epoch, "err", err.Error())
		coreCancel()
	}

	// Experiment cadence flags, decided once per run from the process env. expMode
	// (step or turn) enables tail buffering + boundary flush + success gating; turnMode
	// additionally defers every snapshot to the terminal. expSuccessPending holds the
	// TurnDone publication until the terminal status frame is durably committed.
	expMode := contracts.CurrentCheckpointBoundary() != contracts.CheckpointBoundaryChunk
	turnMode := contracts.CurrentCheckpointBoundary() == contracts.CheckpointBoundaryTurn
	expSuccessPending := false

	// The lease holder is the ONLY legitimate snapshot writer: it persists a snapshot at
	// every step boundary with the exact worker/epoch State granted to THIS process, reusing
	// the proven space/core checkpoint primitive. This gives a run a real durable snapshot a
	// takeover worker cold-resumes from - something the HTTP SaveCheckpoint face alone never
	// provided, because in the composed stand the lease is held by the runner, which has no
	// such HTTP face. No self-signed token, no human-bearer forwarding, no fence relaxation:
	// the write carries the granted lease fence, so a stale or foreign writer is STILL
	// rejected by State with STALE_EPOCH.
	snapshotMessages := []map[string]any{{"role": "user", "content": turn.Input}}
	snapshotSeq := 0
	snapshotTooBigLogged := false
	lastSnapshotID := ""
	var lastSnapshotMsgs, lastSnapshotInner, lastSnapshotMetadata []byte
	// accumulateSnapshot records one step-boundary message in the in-progress snapshot,
	// whether or not a checkpoint is persisted at this boundary (blocker #6: turn mode
	// accumulates every model/tool result but persists only once at the terminal). It
	// returns false when the batch bound is hit, so persistence is skipped.
	accumulateSnapshot := func(kind string, content json.RawMessage) bool {
		snapshotMessages = append(snapshotMessages, map[string]any{"role": "assistant", "step_type": kind, "content": content})
		snapshotSeq++
		msgsJSON, merr := json.Marshal(snapshotMessages)
		if merr != nil {
			return false
		}
		inner, _ := json.Marshal(map[string]any{"turn_id": turn.ID, "thread_id": turn.ThreadID, "steps": snapshotSeq, "source": "runtime-step-boundary"})
		metadata, _ := json.Marshal(map[string]any{"messages": json.RawMessage(msgsJSON), "state": json.RawMessage(inner), "sequence": snapshotSeq})
		if len(metadata) > maxRuntimeSnapshotMetadataBytes {
			if !snapshotTooBigLogged {
				snapshotTooBigLogged = true
				slog.Warn("space: runtime snapshot stopped, history exceeds the checkpoint batch bound",
					"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "seq", snapshotSeq,
					"metadata_bytes", len(metadata), "limit", maxRuntimeSnapshotMetadataBytes)
			}
			return false
		}
		lastSnapshotMsgs, lastSnapshotInner, lastSnapshotMetadata = msgsJSON, inner, metadata
		return true
	}
	// persistSnapshot writes the accumulated snapshot as a checkpoint keyed by the current
	// sequence. State replays an idempotency key only for a byte-identical body, so the key
	// is deterministic in (granted epoch, boundary). A snapshot is a recovery aid, never a
	// stream hole: a failure is logged, never turned into one.
	persistSnapshot := func() {
		if emitErr != nil || len(lastSnapshotMetadata) == 0 {
			return
		}
		snapshotID := fmt.Sprintf("%s:ckpt:e%d:s%d", turn.ID, lease.Epoch, snapshotSeq)
		sctx, cancelSnap := context.WithTimeout(streamBase, c.streamAppendBudget())
		resp, perr := c.state.PutCheckpoint(sctx, runTok, Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}, CheckpointNamespace, snapshotID, lastSnapshotID, snapshotID, int64(snapshotSeq), lastSnapshotMetadata, nil)
		cancelSnap()
		if perr != nil {
			slog.Warn("space: runtime snapshot write failed", "run_id", lease.Run.ID, "thread_id", turn.ThreadID, "epoch", lease.Epoch, "seq", snapshotSeq, "err", perr.Error())
			return
		}
		lastSnapshotID = resp.ID
		c.mu.Lock()
		turn.Checkpoint = &Checkpoint{ID: resp.ID, Namespace: resp.Namespace, Parent: resp.Parent, Version: resp.Version, Metadata: resp.Metadata, TurnID: turn.ID, Sequence: snapshotSeq, State: lastSnapshotInner, Messages: lastSnapshotMsgs, CreatedAt: resp.CreatedAt, runtimeSnapshot: true}
		c.mu.Unlock()
	}
	writeSnapshot := func(kind string, content json.RawMessage, persist bool) {
		if emitErr != nil {
			return // lease/stream already broken; a snapshot write would fail the same way
		}
		if !accumulateSnapshot(kind, content) {
			return
		}
		if persist {
			persistSnapshot()
		}
	}

	status := "done"
	// D8: the cause a run emits has to survive into the turn projection, not only into the
	// stream and a log line. Captured from the FIRST StepError frame, because once a step
	// errored the run stays failed and the first cause is the one that stopped it.
	stepReason := ""
	// DEF-A6: the same facts the execution plane feeds its classifier, collected
	// from this run's own step stream. modelTurns counts MODEL turns (not tool
	// frames), because that is what `consumed` means to ClassifyCloseReason; the
	// last non-empty model text is what tells "the loop stopped" apart from "the
	// model stopped talking".
	modelTurns := 0
	finalText := ""
	sawStepError := false
	for _, frame := range resumeFrames {
		if frame.kind == string(StepModel) {
			modelTurns++
			if text := stepContentText(stepFrameContent(frame.payload)); strings.TrimSpace(text) != "" {
				finalText = text
			}
		}
	}
	for result := range results {
		c.mu.Lock()
		if result.StepType != StepModelDelta {
			turn.Steps++
		}
		c.mu.Unlock()
		if payload, err := json.Marshal(result); err == nil {
			if result.StepType == StepModelDelta {
				emit(string(result.StepType), payload, ModelDeltaKey(turn.ID, result.GenerationID, result.DeltaSequence))
			} else {
				emit(string(result.StepType), payload)
			}
		}
		// A subsequent model intent must not overtake the durable consequence of
		// the previous one. Acknowledging before the optional snapshot preserves
		// transcript authority without making checkpoint availability mandatory.
		if result.Persisted != nil {
			ackErr := emitErr
			if lostLease {
				ackErr = contracts.NewError(contracts.ErrStaleEpoch, "result owner fenced")
			}
			select {
			case result.Persisted <- ackErr:
			default:
			}
		}
		if result.StepType == StepError {
			status = "failed"
			sawStepError = true
			if stepReason == "" {
				stepReason = stepErrorCause(result.Content)
			}
		}
		if result.StepType == StepModel {
			modelTurns++
			if txt := stepContentText(result.Content); strings.TrimSpace(txt) != "" {
				finalText = txt
			}
		}
		// Snapshot at the durable step boundary (model/tool output and the terminal done), so a
		// takeover has the most recent resumable state, not only the raw event stream.
		switch result.StepType {
		case StepModel, StepTool, StepDone:
			// Step mode persists a snapshot at every boundary. Turn mode accumulates every
			// model/tool result here and persists ONE complete snapshot at the successful
			// terminal (persistSnapshot after the terminal commit), so the final checkpoint
			// carries all of them (blocker #6).
			writeSnapshot(string(result.StepType), result.Content, !turnMode)
		}
		// Step mode persists the stream tail at every LLM/tool (step) boundary.
		// Turn mode flushes once, at the terminal boundary after the result loop.
		if contracts.CurrentCheckpointBoundary() == contracts.CheckpointBoundaryStep && result.StepType != StepModelDelta {
			flushBoundary()
		}
		// NOTE: no break — drain results to close so Core (which may be blocked writing) and
		// the join below never leak. Once a step errored the run stays failed.
	}
	// An emission failure caused by the user's cancel is a cancel, not a durability failure: the
	// run's credential was revoked on purpose, so the write that follows it cannot succeed. Only a
	// hole that appeared BEFORE the cancel is a durability failure. Computed inside the join lock
	// below (c.mu is not reentrant, so this is a value, not a closure).

	// JOIN the Core goroutine: `results` is closed by that goroutine's defer, so reaching here means
	// Core has already returned; the buffered `done` then yields its error with no data race and no
	// leaked goroutine (SP-1 synchronization).
	stepErr := <-done
	if stepErr != nil {
		// Core's error used to vanish here: readers only ever saw status=failed with no cause.
		slog.Warn("space: core loop terminated with error",
			"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "epoch", lease.Epoch,
			"steps", turn.Steps, "err", stepErr.Error())
	}

	// The experiment gates success on durable commit. Persist the run's deferred
	// stream tail HERE, before the verdict is decided and published: a boundary-flush
	// failure is a durability hole that must be able to flip the outcome (via
	// emitErr/lostLease), not be reported as done and only logged. No-op in chunk mode.
	flushBoundary()

	c.mu.Lock()
	canceledFirst := !turn.cancelRequestedAt.IsZero() &&
		(emitErrAt.IsZero() || turn.cancelRequestedAt.Before(emitErrAt))
	// D8: whichever branch ends the turn failed, the reason it failed with goes into
	// the projection. A cancel or a clean finish clears it - a cause belongs to a failure
	// only, and two statuses that can disagree are worse than one.
	failureReason := ""
	// A turn that is ALREADY terminal was decided by something a reader has to trust more than
	// this goroutine: a finished CancelTurn (its run row is durably `canceled` and its grant is
	// revoked), or the durable run row whose close this process lost the race to, which
	// CancelTurn adopted verbatim. What the switch below produces is a verdict about the run as
	// it looked from inside the execution - and on the adopted path the execution was just killed
	// for a run State had already closed, so its own verdict is "failed: durable stream hole".
	// Writing that over the status already held reopens the split this turn just closed: GetTurn
	// `failed` next to a run row `done`. Measured, not theorised - tests/space/cancel_terminal_turn_test.go
	// read `failed` here while the run row said `done`, and it did so only SOMETIMES, which is the
	// worst shape a status bug can have. The status already held wins, and only the disagreement
	// is reported.
	if already := turn.Status; already.IsTerminal() {
		if string(already) != status {
			slog.Warn("space: a terminal turn was already decided, the ending run does not restamp it",
				"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "epoch", lease.Epoch,
				"status_held", string(already), "run_verdict", status)
		}
		status = string(already)
		if already == TurnFailed {
			// The cause belongs to the failure that is actually recorded. Re-deriving it here
			// would replace the cause the reader was already given with whatever this dying
			// goroutine prefers.
			failureReason = turn.FailureReason
		}
	} else {
		switch {
		case canceledFirst:
			// Cancel is a user decision and it is terminal; the credential that would have recorded
			// the last steps is gone by design, and the run row was already closed as canceled.
			turn.Status = TurnCanceled
			status = "canceled"
		case lostLease:
			// This worker was fenced out mid-run: a newer lease owner may finish the run
			// `done`, so declaring `failed` here would put a false verdict next to State's
			// real one. Report no verdict at all - `unknown` - and let State decide.
			turn.Status = TurnUnknown
			status = "unknown"
		case emitErr != nil:
			// The durable stream is the source of truth for readers; an AppendStream failure is a
			// durability failure. Core was already cancelled (emit -> coreCancel) so no further tool
			// side effects are emitted on an unrecordable run.
			turn.Status = TurnFailed
			status = "failed"
			failureReason = "space: durable stream hole: " + emitErr.Error()
		case cancelObserved.Load():
			// The user's cancel reached this run as a journal event while we held the lease,
			// and we stopped on it. The status is the user's decision (canceled); the REASON
			// says which mechanism ended the run, which is what distinguishes this from a
			// cancel this process issued itself. Nothing already accepted is rewritten: the
			// transcript frames this run appended stay exactly as they were written.
			//
			// Ordered AFTER the stream-hole case on purpose. A hole in the durable transcript
			// is a fact a reader cannot recover from, and a user cancel happening to arrive in
			// the same run must not launder it into a benign ending - the cancel intent stays
			// journaled on its own, so nothing about the user's decision is lost by saying
			// `failed` here. Ordered BEFORE the generic cancellation case, because this verdict
			// does not depend on how Core chose to report the context we cancelled.
			turn.Status = TurnCanceled
			status = "canceled"
		case ctx.Err() != nil || coreCtx.Err() != nil || errors.Is(stepErr, context.Canceled):
			// Cancellation (CancelTurn / step budget) is not a failure; preserve the canceled status
			// CancelTurn already set rather than overwriting it with failed. A stream-hole cancel is
			// handled by the emitErr case above, so it is never reported as a benign cancel here.
			turn.Status = TurnCanceled
			status = "canceled"
		case stepErr != nil:
			turn.Status = TurnFailed
			status = "failed"
			// The frame's own cause wins over Core's return value: it is the text the reader
			// can already find in the thread stream, and GetTurn must not disagree with it.
			failureReason = stepReason
			if failureReason == "" {
				failureReason = stepErr.Error()
			}
		case status == "failed":
			turn.Status = TurnFailed
			failureReason = stepReason
		default:
			if expMode {
				// Blocker #2: hold the success verdict. TurnDone is published only after
				// the terminal status frame is durably committed (see below), so the turn
				// projection never shows success before its required commit succeeded.
				expSuccessPending = true
			} else {
				turn.Status = TurnDone
			}
		}
	}
	if status == "failed" && failureReason == "" {
		// A failed turn with no cause anywhere is still a fact worth stating; leaving
		// the field empty would put the reader straight back at D8.
		failureReason = "space: run failed without a reported cause"
	}
	turn.FailureReason = failureReason
	turn.UpdatedAt = time.Now().UTC()
	// Cancel may have committed this turn's terminal frame durably on the user's behalf
	// (D4). Read it under the same lock that decided `status`.
	cancelTerminalDurable := turn.cancelTerminalDurable
	c.mu.Unlock()

	// A fenced writer has no verdict to publish and no lease to release: a terminal frame and a
	// Release would both be refused with STALE_EPOCH anyway, and a Release could even disturb the
	// new owner. Leave everything to State and stop touching the run.
	if lostLease {
		slog.Warn("space: this worker lost the run's lease; leaving the verdict to State, not reporting failed",
			"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "epoch", lease.Epoch, "steps", turn.Steps)
		return
	}

	// Terminal status event so readers can close the stream without polling the turn snapshot.
	// A failure here is reported, never swallowed: without it a reader sees a run that stopped
	// emitting with no closing event and no explanation.
	// Skipped when CancelTurn already wrote it: two terminal frames for one turn would make
	// every reader guess which one ends the chat, and the state the user was told (canceled)
	// is already durable.
	terminalCommitted := false
	if payload, err := json.Marshal(map[string]string{"status": status}); err != nil {
		slog.Warn("space: terminal status event could not be encoded",
			"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "status", status, "err", err.Error())
	} else if status == "canceled" && cancelTerminalDurable {
		// CancelTurn already wrote the terminal frame durably; success is not being
		// claimed by this writer, so nothing to gate here.
		terminalCommitted = true
		slog.Debug("space: terminal frame already durable from cancel, not rewriting",
			"run_id", lease.Run.ID, "thread_id", turn.ThreadID)
	} else {
		// Repeated on an unconfirmed outcome (see appendDurableTerminal): this is the last
		// frame this run will ever be able to write, so one lost answer must not end the
		// thread's durable stream without a terminal state.
		appendErr := c.appendDurableTerminal(streamBase, runTok, turn.ThreadID, lease.Run.ID, lease.Worker, lease.Epoch,
			StepKey(turn.ID, seq), "status", payload)
		seq++
		if appendErr != nil {
			slog.Warn("space: terminal status event not durable",
				"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "epoch", lease.Epoch,
				"status", status, "err", appendErr.Error())
		} else {
			terminalCommitted = true
		}
	}

	// Blocker #2: publish the success verdict only now, and only if the terminal status
	// frame is durably committed (the tail was already committed by flushBoundary before
	// the verdict). A run whose terminal could not be committed is NOT a success: flip it
	// to failed before a reader sees it done or it is released done. Chunk mode is
	// unaffected (expSuccessPending stays false).
	if expSuccessPending {
		if terminalCommitted {
			if turnMode {
				// Turn mode deferred every snapshot: write the one complete turn snapshot
				// now that the turn is known good (blocker #6).
				persistSnapshot()
			}
			c.mu.Lock()
			if !turn.Status.IsTerminal() {
				turn.Status = TurnDone
			}
			c.mu.Unlock()
		} else {
			status = "failed"
			failureReason = "space: terminal status frame not durable"
			slog.Warn("space: terminal frame not committed, run is not reported as done",
				"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "epoch", lease.Epoch)
			c.mu.Lock()
			if !turn.Status.IsTerminal() {
				turn.Status = TurnFailed
				turn.FailureReason = failureReason
				turn.UpdatedAt = time.Now().UTC()
			}
			c.mu.Unlock()
		}
	}

	// The run context may be cancelled (cancellation/step-limit). Release must still reach the
	// State API to free the lease, so detach it from ctx.
	releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancelRelease()
	// DEF-A6: the run plane states its terminal reason in the execution plane's own
	// dictionary, so the two ends of an A->B delegation carry comparable values --
	// the child task journals close_reason on its metadata, and this run now carries
	// the same key in its payload. The value comes from the single classifier
	// (gateway.ClassifyCloseReason) applied to this run's own facts, never from a
	// second vocabulary.
	closeReason := runCloseReason(status, stepErr, emitErr, stepReason, sawStepError,
		finalText, modelTurns, turn.MaxSteps)
	if cancelObserved.Load() && status == "canceled" {
		// Not the classifier's job: the classifier has no way to know a cancel was queued
		// out-of-band, and it would report the cancelled context as a plain cancellation.
		closeReason = CloseReasonCancelRequested
	}
	if err := c.state.ReleaseWithReason(releaseCtx, runTok, lease.Run.ID, lease.Worker, lease.Epoch,
		status, closeReason); err != nil {
		slog.Warn("space: run release did not reach State",
			"run_id", lease.Run.ID, "thread_id", turn.ThreadID, "epoch", lease.Epoch,
			"status", status, "close_reason", closeReason, "err", err.Error())
	} else {
		released = true
	}
}

// durableStepFrame is one step boundary the durable thread transcript already carries for a
// turn. Only the kinds the snapshot writer itself treats as a boundary are kept, so a history
// rebuilt from frames says exactly what a snapshot of the same run would have said.
type durableStepFrame struct {
	kind    string
	payload json.RawMessage
}

// isStepBoundary reports whether a stream event is one of the boundaries a runtime snapshot
// records (the switch in the step loop), so the two histories can be counted the same way.
func isStepBoundary(kind string) bool {
	switch kind {
	case string(StepModel), string(StepTool), string(StepDone):
		return true
	}
	return false
}

// durableStepTranscript returns the next unused per-turn step index AND, in durable order, the
// step boundaries this turn's transcript already carries - one ReadStream pass serves both, so
// the resume path cannot get a high-water and a history that were read at different times.
//
// It is FAIL-CLOSED (OG-2): a ReadStream error or a scan that exceeds its bound without
// draining returns an error so the caller refuses to execute under a possibly-colliding key,
// rather than silently reseeding from a wrong high-water and risking KEY_REUSE_MISMATCH.
func (c *Coordinator) durableStepTranscript(ctx context.Context, token, threadID, turnID string) (int, []durableStepFrame, error) {
	const maxScan = 200000
	prefix := turnID + ":"
	highest := -1
	scanned := 0
	after := int64(0)
	var frames []durableStepFrame
	for {
		evs, more, err := c.state.readRecoveryStreamPage(ctx, token, threadID, after, recoveryStreamPageSize, recoveryStreamPageBytes)
		if err != nil {
			return 0, nil, err // cannot know the durable high-water: fail closed
		}
		for _, e := range evs {
			scanned++
			if strings.HasPrefix(e.Key, prefix) {
				if n, cerr := strconv.Atoi(strings.TrimPrefix(e.Key, prefix)); cerr == nil && n > highest {
					highest = n
				}
				if isStepBoundary(e.Kind) {
					frames = append(frames, durableStepFrame{kind: e.Kind, payload: e.Payload})
				}
			}
			after = e.Sequence
		}
		if !more {
			break // fully drained
		}
		if scanned >= maxScan {
			return 0, nil, contracts.NewError(contracts.ErrConflict,
				"space: step high-water scan exceeded bound without draining (incomplete)")
		}
	}
	return highest + 1, frames, nil
}

// checkpointStepCount is how many step boundaries a checkpoint claims to carry. The runtime
// writer stamps the snapshot version with its own boundary count, and a cold rehydrate reads
// that version back into Sequence, so this is the snapshot's own statement rather than a guess
// derived from its payload. A nil checkpoint carries nothing.
func checkpointStepCount(cp *Checkpoint) int {
	if cp == nil || cp.Sequence < 0 {
		return 0
	}
	return int(cp.Sequence)
}

// stepFrameContent pulls the step content out of a durable stream frame. The frame carries the
// whole StepResult (the same JSON a reader of the stream sees); the history entries carry only
// its content, exactly as the snapshot writer wrote them. An unparseable frame still contributes
// its own bytes rather than vanishing - a stale resume is the failure this function exists to
// prevent, and dropping a boundary would repeat it.
func stepFrameContent(payload json.RawMessage) json.RawMessage {
	var frame struct {
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(payload, &frame); err == nil && len(frame.Content) > 0 {
		return frame.Content
	}
	return payload
}

// messagesFromTranscript rebuilds the conversation history a takeover hands Core, straight from
// the durable transcript, in the same entry shape the runtime snapshot writer produces.
func messagesFromTranscript(input json.RawMessage, frames []durableStepFrame) json.RawMessage {
	msgs := make([]map[string]any, 0, len(frames)+1)
	msgs = append(msgs, map[string]any{"role": "user", "content": input})
	for _, f := range frames {
		msgs = append(msgs, map[string]any{"role": "assistant", "step_type": f.kind, "content": stepFrameContent(f.payload)})
	}
	out, err := json.Marshal(msgs)
	if err != nil {
		return nil
	}
	return out
}

// isChildRunTask reports whether t is a durable CHILD run task (a per-run grant ref), as
// opposed to the broad ingress authority task (space-authority:...) or the empty string.
// The check is a denylist on the authority prefix so any server-owned run task id passes.
func isChildRunTask(t string) bool {
	return t != "" && !strings.HasPrefix(t, "space-authority:")
}

// cancelObserveInterval is how often a lease holder looks for a cancel intent in its run's
// durable journal. It is deliberately much shorter than the lease it keeps (30s) so the
// drain has a real bound, and much longer than a step so it is not a per-step State read.
const cancelObserveInterval = time.Second

// scanCancelIntent pages through the thread stream after `cursor` looking for this run's
// cancel-intent key, and returns the cursor to use next.
//
// Paging from a cursor rather than from zero is what makes this affordable at any transcript
// length, and it is why the scan cannot miss: the intent is appended after the holder
// started reading, so it always falls in the window after the cursor. The cursor only ever
// advances over events that were actually read.
func (c *Coordinator) scanCancelIntent(ctx context.Context, token, threadID, runID string,
	cursor int64) (found bool, next int64, err error) {
	key := CancelRequestedKey(runID)
	next = cursor
	for page := 0; page < cancelScanMaxPages; page++ {
		events, err := c.state.ReadStream(ctx, token, threadID, next, cancelScanPageSize)
		if err != nil {
			return false, next, err
		}
		for _, ev := range events {
			if ev.Sequence > next {
				next = ev.Sequence
			}
			if ev.Key == key {
				found = true
			}
		}
		if len(events) < cancelScanPageSize {
			break
		}
	}
	return found, next, nil
}

const (
	cancelScanPageSize = 200
	cancelScanMaxPages = 20
)

// heartbeat keeps the lease alive until cancelled.
func (c *Coordinator) heartbeat(ctx context.Context, token, runID, worker string, epoch int64) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = c.state.Heartbeat(ctx, token, runID, worker, epoch, 30*time.Second)
		}
	}
}

// CancelTurn cancels an active turn execution and revokes ITS OWN run-grant subtree.
//
// Tier-2 fail-closed semantics: under RequireRunGrant the cancellation is only reported
// as successful once THIS run's child grant has been revoked by its durable child task ref
// (Run.TaskID), using the ingress authority that minted it. A missing durable ref or a
// revoke error returns an error (never a false 200) so the caller can retry; a live grant
// must never outlive a cancelled run. Only THIS run's grant is cancelled — sibling chats
// (distinct runs/grants under the same subject) are untouched.
//
// A turn that already finished (`done`/`failed`) is refused with CONFLICT before anything is
// revoked, journaled or released, and a run this process lost the lease to is answered with the
// status State HOLDS instead of a stamped `canceled` — both are spelled out where they happen
// below, and docs/space.md "Turn lifecycle" states the same contract for readers.
func (c *Coordinator) CancelTurn(ctx context.Context, turnID, callerToken string) error {
	c.mu.Lock()
	t, known := c.turns[turnID]
	var childTask, childToken string
	var turn Turn
	if known {
		childTask = t.RunTask
		childToken = t.RunGrant
		turn = *t
	}
	c.mu.Unlock()
	if !known {
		return contracts.NewError(contracts.ErrNotFound, "no turn with id "+turnID)
	}

	// A turn that already finished has no work left to stop, and the mechanism below would
	// give it a cancel anyway: the release of a run whose lease its own completion closed
	// answers STALE_EPOCH, which is the ONE answer the durable close tolerates, so the
	// in-memory stamp at the end of this function landed regardless. Measured on
	// tests/space/cancel_terminal_turn_test.go (RED before this guard): cancelling a `done`
	// turn answered 200, its terminal-frame append was refused with STALE_EPOCH, and GetTurn
	// then reported `canceled` about a run row that says `done` - Space and State split over
	// one run by a call the caller was told had failed. A refusal must mutate NOTHING, so it
	// happens before the revoke, before the append and before the release.
	//
	// Two terminal statuses are deliberately NOT refused here. A repeat cancel of a turn this
	// process already canceled is the same user decision arriving twice: it stays 200 and
	// idempotent, with its keyed journal write so the terminal frame is not doubled
	// (TestCancelTurnReportsTheDurableTerminalFrame). And a turn whose outcome this process
	// cannot read (`unknown`, or a cold row recovered without a status) stays cancellable:
	// stopping a run whose result is unknowable is what a cancel is FOR.
	switch turn.Status {
	case TurnDone, TurnFailed:
		// The refusal below mutates NOTHING, by contract and by test
		// (TestCancelACompletedTurnIsRefusedAndChangesNothing). That is the right
		// answer for the durable record and the wrong answer for the operator: a
		// 409 that leaves no trace anywhere is indistinguishable from a cancel that
		// was never delivered, and a user who pressed Stop twice cannot be told
		// afterwards which of the two happened. So the DECISION is journaled to the
		// log - statuses and ids only, no payload - while the run and the stream stay
		// untouched.
		c.logCancelDecision(turnID, turn, "refused", "turn is already "+string(turn.Status))
		return contracts.NewError(contracts.ErrConflict,
			"space: turn "+turnID+" is already "+string(turn.Status)+", so there is nothing left "+
				"to cancel: a cancel here would report an outcome this turn never had")
	case TurnCanceled:
		if !c.requireRunGrant || turn.grantRevocationConfirmed {
			return nil
		}
	}

	controller, hasController := c.runGrants.(RunGrantController)
	parentTask := AuthorityTaskFromContext(ctx)
	parentControl := hasController && parentTask != "" && callerToken != ""
	if c.requireRunGrant && c.runGrants == nil {
		return contracts.NewError(contracts.ErrConflict, "space: run grant issuer not configured")
	}
	// The durable run supplies the child reference after process restart. The
	// parent's session credential is only used to mint/revoke a verified descendant;
	// the waiting-run transition uses the child's conversation-bound credential.
	parkedClosed, parkedTerminal := false, false
	if turn.RunID != "" && callerToken != "" {
		run, err := c.state.GetRun(ctx, callerToken, turn.RunID)
		if err != nil {
			return err
		}
		if childTask == "" {
			childTask = run.TaskID
		}
		if run.Status == "canceled" {
			parkedClosed = true
			parkedTerminal = c.cancelFrameExists(ctx, callerToken, run.ThreadID, run.ID)
		} else if (run.Status == "waiting" && run.WaitFor == hitlWaitFor) || run.Status == "pending" {
			closeToken := callerToken
			if c.requireRunGrant {
				if childTask == "" || !isChildRunTask(childTask) {
					return contracts.NewError(contracts.ErrConflict, "space: no durable child grant reference")
				}
				if childToken == "" && parentControl {
					childToken, err = controller.ChildToken(ctx, callerToken, childTask)
					if err != nil {
						return err
					}
				}
				if childToken == "" {
					return contracts.NewError(contracts.ErrConflict, "space: missing child credential and parent controller")
				}
				closeToken = childToken
			}
			if err := c.state.CancelWaitingRun(ctx, closeToken, turn.RunID); err != nil {
				// A refused mutation must not leave readers on a stale park after
				// the gate was answered. Refresh only from an authorized State read,
				// preserving any concurrent local transition and the original refusal.
				if latest, readErr := c.state.GetRun(ctx, callerToken, turn.RunID); readErr == nil {
					if status, ok := adoptDurableRun(latest); ok {
						c.mu.Lock()
						if cur := c.turns[turnID]; cur != nil && cur.UpdatedAt.Equal(turn.UpdatedAt) && cur.Status == turn.Status {
							cur.Status = status
							cur.UpdatedAt = time.Now().UTC()
						}
						c.mu.Unlock()
					}
				}
				return err
			}
			parkedClosed, parkedTerminal = true, true
			c.logCancelDecision(turnID, turn, "accepted", "run closed durably while parked on its own gate")
		}
	}
	// No successful cancellation is returned until Identity confirms revocation.
	// A live ancestor can retry even after the child credential was revoked or lost.
	if c.requireRunGrant {
		if childTask == "" || !isChildRunTask(childTask) {
			return contracts.NewError(contracts.ErrConflict, "space: no durable child run grant ref to revoke")
		}
		revokeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if parentControl {
			if err := controller.CancelChild(revokeCtx, callerToken, parentTask, childTask); err != nil {
				return err
			}
		} else {
			if childToken == "" {
				return contracts.NewError(contracts.ErrConflict, "space: no child credential or verified parent authority")
			}
			if err := c.runGrants.Cancel(revokeCtx, childToken, childTask); err != nil {
				return err
			}
		}
	}

	// D4: the durable thread stream MUST end with the terminal state of the canceled turn, and
	// nothing else can write it any more. The revoke above killed the run's credential and the
	// close below drops the lease that fences every stream write, so the worker's own terminal
	// append is refused (measured on the stand: "space: terminal status event not durable ...
	// STALE_EPOCH") and the thread's stream just stopped - which is why a browser subscribing to
	// a canceled chat saw silence. So the CANCEL writes the frame, presented with the CALLER's
	// still-live State token (the same credential the durable close uses) while the lease this
	// turn held is still the current one.
	//
	// A frame that will not land is logged, never swallowed, and is deliberately not fatal: the
	// cancel is a security action that must complete, and the SSE edge independently closes a
	// durably finished thread, so no reader can be left hanging by a failed append.
	terminalDurable := parkedTerminal
	if !parkedClosed && turn.RunID != "" && callerToken != "" {
		if payload, merr := json.Marshal(map[string]string{"status": "canceled"}); merr == nil {
			// The same last-frame rule as the worker's terminal append: after the revoke
			// below nobody can write this frame again, so an unconfirmed answer is repeated
			// with the identical key and body (idempotent at State) instead of being
			// accepted as "not durable".
			aerr := c.appendDurableTerminal(context.WithoutCancel(ctx), callerToken, turn.ThreadID, turn.RunID,
				turn.LeaseWorker, turn.LeaseEpoch, CancelKey(turn.ID), "status", payload)
			if aerr != nil {
				slog.Warn("space: canceled turn has no durable terminal stream frame",
					"run_id", turn.RunID, "thread_id", turn.ThreadID, "turn_id", turn.ID,
					"stream_key", CancelKey(turn.ID), "err", aerr.Error())
			} else {
				terminalDurable = true
			}
		}
	}

	// The run's own credential is now revoked, so the worker can no longer write its terminal
	// status. Close the run durably with the CALLER's State token (same tenant/deployment/subject
	// - State isolates a run by principal scope, not by task) before touching anything in memory:
	// an unclosed run row stays claimable, and an orphan sweep re-executing a canceled turn would
	// turn a user decision into a restart. A stale/lapsed lease is not a failure here: nothing is
	// executing, and with the grant revoked ClaimRun cannot renew a credential for the run, so
	// State itself refuses to hand it out. Any other refusal is returned, never swallowed.
	if turn.RunID != "" && !parkedClosed {
		if callerToken == "" {
			return contracts.NewError(contracts.ErrConflict,
				"space: cancel needs the caller's State token to close the run durably")
		}
		lctx, lc := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer lc()
		// DEF-A6: a canceled run never produced a turn verdict, and saying so is the
		// point -- the reader must not have to guess between "the model finished" and
		// "the user stopped it" from a status column alone.
		err := c.state.ReleaseWithReason(lctx, callerToken, turn.RunID, turn.LeaseWorker,
			turn.LeaseEpoch, "canceled", CloseReasonCanceled)
		if err != nil &&
			contracts.Code(err) != contracts.ErrStaleEpoch && contracts.Code(err) != contracts.ErrNotFound {
			return contracts.NewError(contracts.ErrConflict,
				"space: run grant revoked but the canceled run could not be closed durably: "+err.Error())
		}
		if err != nil {
			// The fence refused THIS cancel's close: State is saying the lease the turn held is
			// not the current one any more, i.e. somebody already closed the run while this
			// process still believed the turn was running - the one case the status guard at the
			// top of this function cannot see, because it reads this process's own memory.
			// STALE_EPOCH is not "nothing happened" here; it is the answer to the question the
			// caller actually asked ("did I stop it?"), so it is READ rather than tolerated: the
			// durable row is asked what the run is, that status is adopted, and the caller gets a
			// conflict naming it. Stamping `canceled` over it would recreate exactly the split
			// the guard above exists to prevent.
			//
			// A durable `canceled` needs no adoption and no conflict: the state the caller wanted
			// is the state State holds, so the normal path below records the cancel. And a refusal
			// to READ the row (the plane is unreachable, the run is outside this caller's scope)
			// is said out loud rather than quietly downgraded to today's behavior.
			//
			// What is read is adopted WHOLE, not only when it is terminal. The first version of this
			// rule adopted done/failed/canceled and stopped there, on the reasoning that a row still
			// saying `running` or `waiting` belongs to somebody else's lease - which is true, and is
			// exactly why it must be reported rather than written over: the row is the fact, this
			// process's memory is not. Leaving a non-terminal row unadopted was the hole that let a
			// cancel stamp `canceled` onto a run that is still claimable (a run parked and released by
			// its gate, or a run whose worker died), where an orphan sweep can take it up again and
			// re-run a turn the user canceled. The status is mapped through the SAME vocabulary the
			// cold read uses (spaceTurnStatus), so the warm reader and the rebuilt reader agree.
			if run, derr := c.durableRun(lctx, callerToken, turn.RunID); derr != nil {
				slog.Warn("space: cancel was refused the durable close and could not read what the run is instead",
					"run_id", turn.RunID, "thread_id", turn.ThreadID, "turn_id", turnID,
					"release_err", err.Error(), "read_err", derr.Error())
			} else if adopted, adoptedOK := adoptDurableRun(run); adoptedOK && adopted != TurnCanceled {
				durable := run.Status
				if run.WaitFor != "" {
					durable = run.Status + ", wait_for=" + run.WaitFor
				}
				terminal := adopted == TurnDone || adopted == TurnFailed || adopted == TurnCanceled
				c.mu.Lock()
				if cur, ok := c.turns[turnID]; ok {
					cur.Status = adopted
					cur.UpdatedAt = time.Now().UTC()
				}
				// Only an execution of a run that is already over is stopped. A row saying `pending`
				// or `waiting` is a correction to what the reader is told, not a reason to kill work
				// this cancel did not get the right to stop.
				stop := c.active[turnID]
				if !terminal {
					stop = nil
				}
				c.mu.Unlock()
				if stop != nil {
					stop()
				}
				return contracts.NewError(contracts.ErrConflict,
					"space: run grant revoked, but the run is durably "+durable+" and this cancel did not close it: "+
						"Space adopted the status State holds instead of calling it canceled")
			}
		}
	}

	c.mu.Lock()
	t.Status = TurnCanceled
	t.cancelRequestedAt = time.Now().UTC()
	t.UpdatedAt = time.Now().UTC()
	t.cancelTerminalDurable = terminalDurable
	t.grantRevocationConfirmed = c.requireRunGrant
	cancel := c.active[turnID]
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.logCancelDecision(turnID, turn, "accepted", "run grant revoked and the run closed by this cancel")
	return nil
}

// logCancelDecision writes one audit line per cancel decision, at the moment the
// decision is made: who was asked (turn/run/thread), whether the cancel was
// accepted or refused, and why.
//
// It exists because "accepted" and "refused" are two different user-visible facts and
// only one of them is durable. An accepted cancel ends in a durable terminal row with
// close_reason=canceled, which the user can read back; a refused one must not write
// anything (a refusal that mutates state is the bug class DEF-A6 and
// TestCancelACompletedTurnIsRefusedAndChangesNothing exist to prevent), which left the
// refusal with no trace at all. The log is the smallest place where the refused half
// can be audited without touching the durable record.
//
// What is deliberately NOT claimed here: this is a log line, not the durable
// "cancel requested" state the run-coordinator acceptance asks for. A durable intent
// for a run that is LEASED to someone else cannot be written from Space without
// writing through a fence the run holder owns - several accepted fixes (the STALE_EPOCH
// adoption path above, DEF-H22-3) exist precisely to forbid that - so the durable
// intent has to be a State-side fenced operation, and that is a contract decision for
// the plane owner, not a unilateral edit.
func (c *Coordinator) logCancelDecision(turnID string, turn Turn, decision, detail string) {
	slog.Info("space: cancel decision",
		"turn_id", turnID, "run_id", turn.RunID, "thread_id", turn.ThreadID,
		"cancel_decision", decision, "detail", detail, "turn_status", string(turn.Status))
}

// durableRun asks State what the run row says, presented with the CALLER's token. The
// run's own credential is revoked by the time the cancel path asks this question, which is the
// point: a cancel must not need a live run credential to find out what happened to the run.
// An unreadable row is an error, never an empty status, because "I cannot read the durable
// truth" and "the run has no status" must not answer the same way.
func (c *Coordinator) durableRun(ctx context.Context, token, runID string) (*runResponse, error) {
	return c.state.GetRun(ctx, token, runID)
}

// adoptDurableRun maps a durable RUN row onto the turn status a reader of GetTurn has to see, and
// reports whether there is a row to adopt at all.
//
// EVERY status the row holds is adoptable, terminal ones included. The rule used to stop at
// done/failed/canceled on the reasoning that a row still saying `running` or `waiting` belongs to a
// lease somebody else holds - which is true, and is the reason it must be REPORTED rather than
// written over: the row is the fact and this process's memory is one reader's opinion of it. Stopping
// at terminal left the hole this file's cancel path fell into twice: a run released from a park, or a
// run whose worker died, stays pending and claimable while the cancel that could not close it stamps
// `canceled` in memory - so a user is told their turn stopped, GetTurn and the run row disagree, and an
// orphan sweep may run that turn again.
//
// The mapping is the cold read's own (spaceTurnStatus), so the process that still holds a turn and the
// process that rebuilt it from State cannot disagree about what it is - the invariant d853cde exists
// for. A nil row is not adoptable: "State could not be read" must not become a status.
func adoptDurableRun(run *runResponse) (TurnStatus, bool) {
	if run == nil || run.Status == "" {
		return "", false
	}
	return spaceTurnStatus(*run), true
}

// CancelTerminalFrameDurable reports whether the terminal frame of a canceled turn is
// actually in the thread's durable stream.
//
// It exists because a cancel answer must not be silent about a journal failure. The cancel
// itself always completes - revoking a run's own credential is a security action that a
// State failure must not hold hostage, and CancelTurn already refuses to report success
// while the revoke or the durable close of the run is unconfirmed - so the one thing a
// caller can still be lied about by omission is the transcript. Measured (forum
// 20261101T104800Z §1 item 4): a canceled thread whose terminal append was refused answered
// {"ok":true} while the durable stream held no terminal frame at all.
//
// The SSE edge is not the witness here. sse.go closes a thread whose durable RUNS are all
// terminal, so the browser looks exactly as healthy as the run row while the durable
// transcript is short by its last event - which is why the honest reader of this field is a
// client that goes to State directly: the SDK, a second deployment, a transcript-vs-checkpoint
// diff.
//
// false also covers "this turn was never canceled". That is not reachable from the cancel
// endpoint - the handler asks only after a CancelTurn that returned no error - but it is the
// honest answer for an unknown turn id, and this accessor does not pretend otherwise.
func (c *Coordinator) CancelTerminalFrameDurable(turnID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.turns[turnID]
	if !ok {
		return false
	}
	return t.cancelTerminalDurable
}

// Steer injects a steering instruction into a running turn.
func (c *Coordinator) Steer(ctx context.Context, taskToken string, req SteerRequest) error {
	turn := c.getTurn(req.TurnID)
	if turn == nil {
		return contracts.NewError(contracts.ErrNotFound, "turn not found")
	}
	if turn.Status != TurnRunning && turn.Status != TurnSteering {
		return contracts.NewError(contracts.ErrConflict, "turn is not steerable in status "+string(turn.Status))
	}
	payload, _ := json.Marshal(SteeringEvent{
		TurnID:      req.TurnID,
		Instruction: req.Instruction,
		CreatedAt:   time.Now().UTC(),
	})
	// The durable write comes first, and only once State has accepted the steering row is the
	// turn entitled to say it is steering. Flipping the status before Enqueue meant a refused
	// enqueue - a transport timeout on the constrained stand - left GetTurn reporting
	// `awaiting_steering` for a gate nothing recorded, and it kept doing so for the rest of the
	// run. A refusal must mutate nothing, exactly as Recover was fixed not to (D15); and because
	// steering never parks the run, the status is the only thing this op claims, so it is the one
	// thing that must not outrun the durable event behind it.
	if _, err := c.state.Enqueue(ctx, taskToken, turn.ThreadID, "steering:"+contracts.NewID(), "steering", payload); err != nil {
		return fmt.Errorf("steer inbox: %w", err)
	}
	c.mu.Lock()
	turn.Status = TurnSteering
	turn.UpdatedAt = time.Now().UTC()
	c.mu.Unlock()
	return nil
}

// Epoch returns the last-known epoch for the turn (thread epoch).
// Epoch returns the last State lease epoch observed for this turn. It is used
// to fence stale writers; before a turn is dispatched it defaults to 1.
func (t *Turn) Epoch() int64 {
	if t.leaseEpoch <= 0 {
		return 1
	}
	return t.leaseEpoch
}

// TurnSnapshot returns a copy of the turn with the given ID, and whether it
// exists. It is the read-only accessor used by the HTTP handler and tests.
func (c *Coordinator) TurnSnapshot(turnID string) (Turn, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.turns[turnID]
	if !ok {
		return Turn{}, false
	}
	cp := *t
	return cp, true
}

// IsExecuting reports whether THIS process is currently running the turn. The read path
// uses it to avoid re-reading State for a turn it is itself executing (DEF-H30-x).
func (c *Coordinator) IsExecuting(turnID string) bool {
	c.mu.RLock()
	_, ok := c.active[turnID]
	c.mu.RUnlock()
	return ok
}

// foldTerminalSnapshot records a durable terminal this process just learned about, so a
// follow-up read is served from memory rather than re-hydrating. It never touches a turn
// this process is actively running, and never overwrites an already-terminal local truth
// (DEF-H30-x).
func (c *Coordinator) foldTerminalSnapshot(t Turn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, active := c.active[t.ID]; active {
		return
	}
	existing, ok := c.turns[t.ID]
	if !ok || existing.Status.IsTerminal() {
		return
	}
	existing.Status = t.Status
	if t.FailureReason != "" {
		existing.FailureReason = t.FailureReason
	}
	existing.UpdatedAt = time.Now().UTC()
}

// watchDispatch gives a turn accepted into a dispatcher-less process a DETERMINED terminal
// instead of a silent eternal `pending` (DEF-H30-x). It is a no-op the instant the turn
// leaves pending - a client DispatchTurn or a late claim loop took it - and only ever
// touches a turn still in its original unleased pending state, so it cannot race a real
// execution. The in-memory projection (what GetTurn serves) says failed + reason; a
// lease-free durable close keeps the run row from dangling too.
func (c *Coordinator) watchDispatch(turnID string, after time.Duration) {
	timer := time.NewTimer(after)
	defer timer.Stop()
	<-timer.C

	c.mu.Lock()
	turn, ok := c.turns[turnID]
	if !ok || turn.Status != TurnPending {
		c.mu.Unlock()
		return // dispatched, canceled, or forgotten: the watchdog has nothing to say
	}
	if _, active := c.active[turnID]; active {
		c.mu.Unlock()
		return // this process is running it after all
	}
	if c.dispatcherKnown.Load() {
		c.mu.Unlock()
		return // a dispatcher declared itself while we waited; it owns the lifecycle
	}
	runTok := turn.RunGrant
	turn.Status = TurnFailed
	turn.FailureReason = ReasonNoTurnDispatcher
	turn.UpdatedAt = time.Now().UTC()
	c.mu.Unlock()

	if runTok == "" {
		return
	}
	// Best-effort durable close so the run is not left `pending` forever either. The only
	// lease-free terminal State offers an UNCLAIMED run is CancelWaitingRun; Space's
	// authoritative cause stays the failed/ReasonNoTurnDispatcher projection above, and a
	// reader converges on the durable terminal through turnForRead regardless.
	rctx, rcn := context.WithTimeout(context.Background(), 10*time.Second)
	defer rcn()
	if cerr := c.state.CancelWaitingRun(rctx, runTok, turnID); cerr != nil {
		slog.Warn("space: no-dispatcher watchdog could not close the run durably",
			"turn_id", turnID, "err", cerr.Error())
	}
}

// RehydrateTurn merges a persisted turn back into the in-memory table after a
// restart. Non-zero fields on t overwrite the existing entry; a turn that is
// not present is inserted. Rehydration is how the coordinator rebuilds its
// view of turns from State before running Recover classification.
func (c *Coordinator) RehydrateTurn(t Turn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	existing, ok := c.turns[t.ID]
	if !ok {
		cp := t
		c.turns[t.ID] = &cp
		return
	}
	if t.ThreadID != "" {
		existing.ThreadID = t.ThreadID
	}
	if t.Status != "" {
		existing.Status = t.Status
	}
	if t.MaxSteps != 0 {
		existing.MaxSteps = t.MaxSteps
	}
	if t.AgentConfig != nil && existing.AgentConfig == nil {
		existing.AgentConfig = t.AgentConfig
	}
	if existing.ProjectID == "" {
		existing.ProjectID = t.ProjectID
		existing.ProjectMemoryProfile = t.ProjectMemoryProfile
	}
	if t.leaseEpoch != 0 {
		existing.leaseEpoch = t.leaseEpoch
	}
	existing.UpdatedAt = time.Now().UTC()
}

// SubmitHITL creates a HITL gate for a turn and suspends execution.
func (c *Coordinator) SubmitHITL(ctx context.Context, taskToken string, req HITLSubmitRequest, runID, worker string, epoch int64) (*HITLRequest, error) {
	hitl := &HITLRequest{
		ID:       contracts.NewID(),
		TurnID:   req.TurnID,
		Kind:     req.Kind,
		Prompt:   req.Prompt,
		Status:   HITLPending,
		Deadline: req.Deadline,
	}
	turn := c.getTurn(req.TurnID)
	if turn == nil {
		return nil, contracts.NewError(contracts.ErrNotFound, "turn not found")
	}
	c.mu.Lock()
	turn.Status = TurnAwaiting
	turn.UpdatedAt = time.Now().UTC()
	c.mu.Unlock()
	payload, _ := json.Marshal(hitl)
	if _, err := c.state.Enqueue(ctx, taskToken, turn.ThreadID, "hitl:"+hitl.ID, "hitl_request", payload); err != nil {
		return nil, fmt.Errorf("hitl inbox: %w", err)
	}
	// The executor parks its own run in durable State until the approval answer
	// arrives (or the deadline passes): PDF "Suspend на interrupt()" + "событие в
	// inbox будит запуск". The State API wakes a waiting run back to pending when
	// an inbox event with kind == waitFor is Enqueued (RespondHITL enqueues
	// kind "hitl_response"), which makes the run claimable again for resume. Only
	// the execution path supplies a live run fence (non-empty worker); a
	// human-facing SubmitHITL (empty worker) records the gate and leaves run
	// parking to the executor that holds the lease.
	if runID != "" && worker != "" {
		if err := c.state.Suspend(ctx, taskToken, runID, worker, epoch, hitlWaitFor, req.Deadline); err != nil {
			return nil, fmt.Errorf("hitl suspend: %w", err)
		}
		// An answer that became durable while this call was in flight woke nothing, because the run
		// was not parked yet and State wakes only on insertion - so the park above waits on a signal
		// that has already been and gone. Repair it from the durable record; releaseRunIfGateAnswered
		// states why neither reordering the two calls nor refusing here is the honest answer. A repair
		// that cannot be made is logged, never returned as a failure of SubmitHITL: the gate and the
		// park are durable, and telling the executor the gate was not created would be the worse lie.
		answered, wakeErr := c.releaseRunIfGateAnswered(ctx, taskToken, runID, turn.ThreadID, hitl.ID)
		if wakeErr != nil {
			slog.Warn("space: a HITL answer recorded before the park could not be re-signalled",
				"hitl_id", hitl.ID, "run_id", runID, "thread_id", turn.ThreadID, "err", wakeErr.Error())
		} else if answered != nil {
			// Say what the gate actually is: handing the caller a `pending` gate whose decision is
			// already durable invites it to wait for something that already happened.
			hitl.Status = answered.decisionStatus()
			hitl.Response = answered.response
		}
	}
	return hitl, nil
}

// RespondHITL records a human decision for a HITL gate.
func (c *Coordinator) RespondHITL(ctx context.Context, taskToken string, req HITLRespondRequest, threadID string) error {
	// The gate's identity is checked against the durable record BEFORE anything is written: a gate
	// that already carries a decision keeps it, and a second, different decision is refused by name
	// instead of being answered on State's behalf by its key-reuse digest check. See checkHITLAnswer
	// for why an unreadable inbox never refuses an answer.
	if err := c.checkHITLAnswer(ctx, taskToken, threadID, req); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]any{
		"hitl_id":  req.HITLID,
		"approved": req.Approved,
		"response": req.Response,
	})
	_, err := c.state.Enqueue(ctx, taskToken, threadID, "hitl_response:"+req.HITLID, "hitl_response", payload)
	if err != nil {
		return fmt.Errorf("hitl respond inbox: %w", err)
	}
	return nil
}

// EmitOutbox appends an at-least-once delivery item via state inbox events.
func (c *Coordinator) EmitOutbox(ctx context.Context, taskToken string, req OutboxEmitRequest, threadID, worker string, epoch int64) (*OutboxEntry, error) {
	entry := &OutboxEntry{
		ID:        contracts.NewID(),
		TurnID:    req.TurnID,
		Key:       req.Key,
		Kind:      req.Kind,
		Payload:   req.Payload,
		CreatedAt: time.Now().UTC(),
	}
	_, err := c.state.Enqueue(ctx, taskToken, threadID, "outbox:"+req.Key, "outbox", req.Payload)
	if err != nil {
		return nil, fmt.Errorf("outbox emit: %w", err)
	}
	return entry, nil
}

// checkpointMessages extracts the serialized message history embedded in a
// checkpoint state blob of shape { "messages": ..., "state": ..., "sequence": ... }.
func checkpointMessages(state json.RawMessage) json.RawMessage {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(state, &m); err != nil {
		return nil
	}
	return m["messages"]
}

// localFence returns the fence built from the lease THIS process actually holds
// for a turn, and reports false when this process is not the run's lease owner
// (nobody claimed it, someone else holds it, or this process already finished
// executing it and released it).
//
// The identity comes from the granted lease (ClaimRun response), never from the
// coordinator's own worker id or from a defaulted epoch: a zero-profile fence
// {run, ingress-worker, epoch 1} is exactly what State rejects with STALE_EPOCH
// when a run is executed by another process, and reporting that as a local 500
// hid the real reason from every caller.
func (c *Coordinator) localFence(turnID string) (Fence, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, executing := c.active[turnID]; !executing {
		return Fence{}, false
	}
	t := c.turns[turnID]
	if t == nil || t.LeaseWorker == "" || t.LeaseEpoch <= 0 {
		return Fence{}, false
	}
	runID := t.RunID
	if runID == "" {
		runID = t.ID
	}
	return Fence{RunID: runID, Worker: t.LeaseWorker, Epoch: t.LeaseEpoch}, true
}

// SaveCheckpoint persists a turn's intermediate state (PDF: снимки — примитив
// State API, fenced by the run lease).
//
// WHO MAY WRITE: only the process that holds the run's lease — i.e. the
// execution face that claimed the run and is running its Core loop right now.
// It writes with the lease's ACTUAL worker/epoch, so State's fence check is a
// real check and not a formality. Any other process (the ingress leg that only
// accepted the turn and never executed it, a worker whose lease has lapsed or
// was taken over) gets a distinguishable contracts.ErrStaleEpoch, which the
// transport renders as a 409 STALE_EPOCH: never a 500, never a false 200. A
// client that wants its snapshot recorded must ask the run's executor, because
// a snapshot written under a fence the caller does not own would silently be
// attributed to the wrong writer (or accepted at the wrong epoch).
//
// Deliberately NOT done here: minting/self-signing a token in Space, weakening
// mTLS, or forwarding the human bearer to State. The caller's already-scoped
// task token is passed through unchanged, so State still authorizes the scope
// and enforces the fence itself.
func (c *Coordinator) SaveCheckpoint(ctx context.Context, taskToken string, req CheckpointRequest) (*CheckpointResponse, error) {
	turn := c.getTurn(req.TurnID)
	if turn == nil {
		return nil, contracts.NewError(contracts.ErrNotFound, "turn not found")
	}
	fence, owner := c.localFence(req.TurnID)
	if !owner {
		return nil, contracts.NewError(contracts.ErrStaleEpoch,
			"space: only the run's lease owner writes snapshots; this process holds no lease for turn "+req.TurnID)
	}
	state, _ := json.Marshal(map[string]any{
		"messages": req.Messages,
		"state":    req.State,
		"sequence": req.Sequence,
	})
	// Checkpoints live in the thread, in Space's own namespace, keyed by a
	// fresh public id and an idempotency key. The write is fenced by the run
	// lease so a stale worker (post-takeover) is rejected with STALE_EPOCH.
	resp, err := c.state.PutCheckpoint(ctx, taskToken, fence, CheckpointNamespace, contracts.NewID(), "", "ckpt", int64(req.Sequence), state, nil)
	if err != nil {
		return nil, fmt.Errorf("checkpoint save: %w", err)
	}
	cp := &Checkpoint{
		ID:        resp.ID,
		Namespace: resp.Namespace,
		Parent:    resp.Parent,
		Version:   resp.Version,
		Metadata:  resp.Metadata,
		TurnID:    req.TurnID,
		Sequence:  req.Sequence,
		State:     req.State,
		Messages:  req.Messages,
		CreatedAt: resp.CreatedAt,
	}
	c.mu.Lock()
	turn.Checkpoint = cp
	c.mu.Unlock()
	return &CheckpointResponse{Checkpoint: *cp}, nil
}

// Recover reports the turns whose outcome this worker can no longer prove.
//
// A turn whose execution is live in this process is not uncertainty, so it keeps
// its `running` status and is left out of the report: stamping it `unknown`
// would advertise healthy work as cancellable (Cancel deliberately accepts an
// unknown turn) and would make GetTurn contradict the lease, the heartbeat, the
// arriving steps and the durable run row, which all still say the run is going.
// A `running` turn with no execution here is exactly the case recovery exists
// for - a worker that owned a turn and vanished - and stays reported as unknown.
func (c *Coordinator) Recover(ctx context.Context, taskToken string) (*RecoveryResult, error) {
	result := &RecoveryResult{}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.turns {
		switch t.Status {
		case TurnRunning:
			if _, executing := c.active[t.ID]; executing {
				continue
			}
			t.Status = TurnUnknown
			result.Unknown = append(result.Unknown, *t)
		case TurnPending:
			result.Recovered = append(result.Recovered, *t)
		}
	}
	return result, nil
}

func (c *Coordinator) getTurn(id string) *Turn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.turns[id]
}

// scopeOfThread returns the scope that owns a thread when this coordinator
// tracks a turn for it. The HTTP boundary uses it to enforce per-subject
// isolation so one subject can never read or mutate another subject's thread.
// Returns ok=false when the thread is unknown to this worker.
// RunRecord is a durable run history entry (PDF §3.4 reseed history / §3.3 run
// ledger), read from State (not the in-memory turn table), so it survives a process
// restart for a thread the caller owns.
type RunRecord struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	TaskID   string          `json:"task_id"`
	Status   string          `json:"status"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload,omitempty"`
}

// ChatSummary is one entry of the durable per-scope chat list (PDF §3.2 Spaces/
// chats). It is read from State (not the in-memory turn table) so it survives a
// restart; no scope/subject is exposed to the browser.
type ChatSummary struct {
	ThreadID  string    `json:"thread_id"`
	ContextID string    `json:"context_id"`
	CreatedAt time.Time `json:"created_at"`
}

// ListChats returns one newest-first page of the caller's own threads, read from
// the durable State ListThreads op. Scope is bounded by the presenting task token;
// the caller's subject is never accepted as an argument.
func (c *Coordinator) ListChats(ctx context.Context, token, cursor string, limit int) ([]ChatSummary, string, bool, error) {
	threads, next, hasMore, err := c.state.ListThreads(ctx, token, cursor, limit)
	if err != nil {
		return nil, "", false, fmt.Errorf("list chats: %w", err)
	}
	out := make([]ChatSummary, 0, len(threads))
	for _, t := range threads {
		out = append(out, ChatSummary{ThreadID: t.ID, ContextID: t.ContextID, CreatedAt: t.CreatedAt})
	}
	return out, next, hasMore, nil
}

// liveRunStatuses are the durable run statuses that mean "this chat still has work
// in flight". The vocabulary is State's own, not Space's invention: runs_status_enum
// (internal/state/schema.sql) allows exactly pending, running, waiting, done, failed,
// canceled, and the State server itself uses this same triple when it decides which
// run of a thread an inbox event may wake (internal/state/events.go). Anything else
// is terminal or unknown and never blocks a delete.
var liveRunStatuses = map[string]bool{"pending": true, "running": true, "waiting": true}

// DeleteChat deletes one of the caller's own chats: the durable thread together
// with everything State cascades from it (runs, intents, stream and inbox events,
// checkpoints), plus whatever this process still remembers about it. Ownership is
// enforced at the HTTP boundary (authorizeThread: a foreign thread is 404, not 403)
// and enforced again by State, which filters BOTH the liveness read and the delete
// by the presenting task token - the thread id in the body carries no scope.
//
// The liveness refusal is the security decision here, not a nicety. A live run's
// INTENT journal is the only barrier that keeps a re-delivered run from repeating an
// external effect (RecordIntent/UpdateIntent/ReconcileIntent), and the cascade would
// delete that journal along with the run row while an executor may still be alive.
// Erasing the evidence under a running turn turns "delete my chat" into "repeat the
// tool call you already performed", so a chat whose run is pending/running/waiting
// is refused with CONFLICT and left completely intact: the user cancels first -
// which revokes the run's credential and closes the row durably - and only then does
// the same delete go through.
//
// The check reads the DURABLE runs, not the in-memory turn table, so it survives a
// restart: a row still marked running by a dead worker blocks the delete instead of
// being quietly erased together with the history a re-claim would need to be safe.
//
// A failure of the delete itself is forwarded unchanged (State owns the verdict). It
// may be a lost ACK rather than a refusal, in which case the caller's retry answers
// NOT_FOUND - the honest signal that the chat really is gone.
func (c *Coordinator) DeleteChat(ctx context.Context, token, threadID string) error {
	if threadID == "" {
		return contracts.NewError(contracts.ErrInvalid, "space: thread_id is required")
	}
	runs, err := c.state.ListRuns(ctx, token, threadID)
	if err != nil {
		return err
	}
	for _, r := range runs {
		if liveRunStatuses[r.Status] {
			return contracts.NewError(contracts.ErrConflict, "space: chat still has run "+r.ID+
				" in status "+r.Status+"; cancel the running turn before deleting the chat")
		}
	}
	if err := c.state.DeleteThread(ctx, token, threadID); err != nil {
		return err
	}
	c.forgetThread(threadID)
	return nil
}

// forgetThread drops everything this process remembers about a thread that is gone
// from State: its execution turns with their cancel funcs, and its entries in the
// read-only durable history index. Both maps are keyed by thread id, and keeping
// rows for a deleted chat would make the user's own views answer with turns that no
// longer exist anywhere - a data-loss bug in the opposite direction from the one it
// fixes. Nothing that is still executing is ever dropped here: DeleteChat refuses
// while any run of the thread is live, so no credential can outlive the thread it
// writes into.
func (c *Coordinator) forgetThread(threadID string) {
	c.mu.Lock()
	c.terminalCheckpoints.forgetThread(threadID)
	for id, t := range c.turns {
		if t.ThreadID == threadID {
			delete(c.turns, id)
			delete(c.active, id)
		}
	}
	c.mu.Unlock()
	c.turnHistory.forgetThread(threadID)
}

// ListRunHistory returns the durable runs of a thread, oldest first, read from State.
// The task token bounds scope server-side; the caller's ownership of the thread is
// enforced at the HTTP boundary via authorizeThread before this is called.
func (c *Coordinator) ListRunHistory(ctx context.Context, token, threadID string) ([]RunRecord, error) {
	runs, err := c.state.ListRuns(ctx, token, threadID)
	if err != nil {
		return nil, fmt.Errorf("list run history: %w", err)
	}
	out := make([]RunRecord, 0, len(runs))
	for _, r := range runs {
		out = append(out, RunRecord{ID: r.ID, ThreadID: r.ThreadID, TaskID: r.TaskID, Status: r.Status, Sequence: r.Sequence, Payload: r.Payload})
	}
	return out, nil
}

// VerifyThreadOwnership confirms a thread belongs to the presenting caller's
// scope. After a process restart the coordinator's in-memory turn table is empty,
// so the per-subject authorization at the HTTP boundary would otherwise deny the
// legitimate owner their own resumed thread. This re-checks ownership against the
// durable State: GetThread is filtered by the presenting task token's
// tenant/deployment/subject, so it resolves the thread ONLY within the caller's
// own scope and returns not-found for any other subject's thread. It therefore
// keeps restart-safe replay for the owner while staying fail-closed (no leak).
func (c *Coordinator) VerifyThreadOwnership(ctx context.Context, token, threadID string) error {
	if threadID == "" {
		return contracts.NewError(contracts.ErrNotFound, "thread not found")
	}
	th, err := c.state.GetThread(ctx, token, threadID)
	if err != nil {
		return err
	}
	if th.ID != threadID {
		return contracts.NewError(contracts.ErrNotFound, "thread not found")
	}
	return nil
}

func (c *Coordinator) scopeOfThread(threadID string) (contracts.Scope, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, t := range c.turns {
		if t.ThreadID == threadID {
			return t.Scope, true
		}
	}
	return contracts.Scope{}, false
}

// restoreTurnFromRunPayload rebinds the original turn input and bounded-step
// budget onto a cold-rehydrated turn from the durable run payload written by
// AcceptTurn. Absent or malformed fields are left untouched (defaults stand).
func restoreTurnFromRunPayload(t *Turn, payload json.RawMessage) {
	if len(payload) == 0 {
		return
	}
	var p struct {
		AgentInstallationID  string          `json:"agent_installation_id,omitempty"`
		AgentTarget          *AgentTargetRef `json:"agent_target,omitempty"`
		ProjectID            string          `json:"project_id,omitempty"`
		ProjectMemoryProfile string          `json:"project_memory_profile,omitempty"`
		Input                json.RawMessage `json:"input"`
		MaxSteps             int             `json:"max_steps"`
		AgentConfig          *AgentConfigRef `json:"agent_config"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return
	}
	if len(p.Input) > 0 {
		t.Input = p.Input
	}
	if p.MaxSteps > 0 {
		t.MaxSteps = p.MaxSteps
	}
	if p.AgentConfig != nil && t.AgentConfig == nil {
		t.AgentConfig = p.AgentConfig
	}
	if t.AgentInstallationID == "" {
		t.AgentInstallationID = p.AgentInstallationID
	}
	if t.AgentTarget == nil {
		t.AgentTarget = p.AgentTarget
	}
	if t.ProjectID == "" {
		t.ProjectID = p.ProjectID
		t.ProjectMemoryProfile = p.ProjectMemoryProfile
	}
}

// cancelFrameExists verifies the terminal event on a cold retry; an unreadable
// journal is not evidence that an earlier cancellation wrote it.
func (c *Coordinator) cancelFrameExists(ctx context.Context, token, threadID, runID string) bool {
	after := int64(0)
	for scanned := 0; scanned < 100000; scanned += stateStreamPageSize {
		events, err := c.state.ReadStream(ctx, token, threadID, after, stateStreamPageSize)
		if err != nil {
			return false
		}
		for _, event := range events {
			if event.Key == CancelKey(runID) {
				return true
			}
			after = event.Sequence
		}
		if len(events) < stateStreamPageSize {
			return false
		}
	}
	return false
}
