// Package space implements the Agents Space Coordinator: the durable
// orchestration layer that accepts turns, dispatches Core runs, manages
// HITL/HITL-outbox flows, steering, recovery, and checkpointing.
//
// Space never reads PostgreSQL directly. All persistence flows through the
// State API (HTTP) which owns the ahp_state schema.
package space

import (
	"encoding/json"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Turn represents a user-initiated interaction with the Space. It is the
// top-level unit that a Core run executes.
type Turn struct {
	AgentInstallationID  string          `json:"agent_installation_id,omitempty"`
	AgentTarget          *AgentTargetRef `json:"agent_target,omitempty"`
	ProjectID            string          `json:"project_id,omitempty"`
	ProjectMemoryProfile string          `json:"project_memory_profile,omitempty"`
	ID                   string          `json:"id"`
	ThreadID             string          `json:"thread_id"`
	Scope                contracts.Scope `json:"scope"`
	Input                json.RawMessage `json:"input"`
	Status               TurnStatus      `json:"status"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
	Checkpoint           *Checkpoint     `json:"checkpoint,omitempty"`
	Steps                int             `json:"steps"`
	MaxSteps             int             `json:"max_steps"`

	// FailureReason is why a turn ended failed (D8, hive-director 20261101T123847Z §4). Before
	// it existed a reader of GetTurn saw `status=failed, steps=1` and nothing else, while the
	// thread stream carried the cause in an `error` frame - so learning what killed a turn meant
	// re-reading and re-deriving the whole durable stream. This is that same cause, the one the
	// run itself emitted (Core's error text, a StepError frame's `error`, or a durability
	// failure), never a paraphrase of it.
	//
	// It is set ONLY for a failed turn: a cause stamped on a healthy one would be a second
	// status, and the two would eventually disagree. Both readers of a failed turn get it: the
	// live projection records what the run emitted, and a turn this process no longer remembers
	// has the cause REBUILT from the durable transcript, which is where it was written in the
	// first place (Coordinator.transcriptCause). It is deliberately never copied into the run
	// row - two places naming a run's death are two places that can disagree about it. Two
	// limits stay stated rather than smoothed over: a turn refused before Core ever started has
	// no cause frame to rebuild (preflight writes none on purpose), and ListTurns does not pay
	// the transcript read, so the cause belongs to the single-turn read.
	FailureReason string `json:"failure_reason,omitempty"`

	// Lease identity of the execution this turn currently owns. Resource identifiers, not
	// credentials: CancelTurn needs them to close the run DURABLY with the caller's own still
	// live State token, because revoking the run's grant (the first, mandatory cancel step)
	// kills the credential the worker would otherwise write its terminal status with. Without
	// this, a canceled run stays non-terminal in the run store and an orphan sweep could claim
	// it again - a user decision must never be resurrected.
	RunID       string `json:"run_id,omitempty"`
	LeaseWorker string `json:"lease_worker,omitempty"`
	LeaseEpoch  int64  `json:"lease_epoch,omitempty"`

	// cancelRequestedAt records WHEN the user asked to cancel, so the end of execution can tell
	// a stream write that failed because the revoked credential died (a cancel) from a genuine
	// durable stream hole (a failure).
	cancelRequestedAt time.Time

	// cancelTerminalDurable records that CancelTurn itself wrote the turn's terminal frame
	// into the durable thread stream (D4). The execution loop skips its own terminal append
	// when this is set, so a canceled turn has exactly one terminal frame and not two. It is
	// process-local bookkeeping and is never serialized.
	cancelTerminalDurable    bool
	grantRevocationConfirmed bool

	// AgentConfig is the revision+deployment pinned at accept time and carried
	// through every step and across cold-resume (PDF rev3.2 L373/948/2399).
	AgentConfig *AgentConfigRef `json:"agent_config,omitempty"`

	// RunGrant is the Tier-2 per-run Identity Delegate grant token, obtained in
	// AcceptTurn under the ingress authority. Transient (NOT serialized to State or any
	// payload): a runtime credential must never be persisted. Downstream execution and
	// cancel use it instead of the ingress authority.
	RunGrant string `json:"-"`

	// RunTask is the durable CHILD run task id State bound to the run (Run.TaskID) when
	// CreateRun presented the child grant. It is a resource identifier (not a credential) and
	// IS persisted so a cold-recovered worker can recover the grant ref; the live token is
	// reissued by State at ClaimRun (claim.TaskToken), never persisted here.
	RunTask string `json:"run_task,omitempty"`

	// leaseEpoch is the last State lease epoch observed for this turn. It is
	// not serialized; it tracks optimistic-concurrency fencing across runs.
	leaseEpoch int64

	// coldRehydrated marks a turn reconstructed from a durable run after a process
	// restart (the in-memory turn was lost). It is not serialized. A cold turn with no
	// checkpoint must recover its step-stream sequence from the durable stream high-water;
	// a fresh turn starts at 0 (no scan).
	coldRehydrated bool
}

// TurnStatus tracks the lifecycle of a turn through Space.
type TurnStatus string

const (
	TurnPending  TurnStatus = "pending"
	TurnRunning  TurnStatus = "running"
	TurnAwaiting TurnStatus = "awaiting_hitl"
	TurnSteering TurnStatus = "awaiting_steering"
	TurnDone     TurnStatus = "done"
	TurnFailed   TurnStatus = "failed"
	TurnCanceled TurnStatus = "canceled"
	TurnUnknown  TurnStatus = "unknown"
)

// IsTerminal reports whether the turn status ends its lifecycle.
func (s TurnStatus) IsTerminal() bool {
	switch s {
	case TurnDone, TurnFailed, TurnCanceled:
		return true
	}
	return false
}

// Message is a chat message within a turn.
type Message struct {
	ID        string          `json:"id"`
	TurnID    string          `json:"turn_id"`
	Role      MessageRole     `json:"role"`
	Content   json.RawMessage `json:"content"`
	CreatedAt time.Time       `json:"created_at"`
}

// MessageRole identifies who authored a message.
type MessageRole string

const (
	RoleUser      MessageRole = "user"
	RoleAssistant MessageRole = "assistant"
	RoleTool      MessageRole = "tool"
	RoleSystem    MessageRole = "system"
)

// Intent describes a side-effect request recorded during a turn execution.
// Space records intents via State API; execution is owned by the execution plane.
type Intent struct {
	ID     string          `json:"id"`
	TurnID string          `json:"turn_id"`
	Node   string          `json:"node"`
	Call   string          `json:"call"`
	Kind   string          `json:"kind"`
	Target string          `json:"target"`
	Digest string          `json:"digest"`
	Status IntentStatus    `json:"status"`
	Args   json.RawMessage `json:"args,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

// IntentStatus mirrors the State API intent lifecycle.
type IntentStatus string

const (
	IntentRecorded   IntentStatus = "recorded"
	IntentDispatched IntentStatus = "dispatched"
	IntentSucceeded  IntentStatus = "succeeded"
	IntentFailed     IntentStatus = "failed"
	IntentUnknown    IntentStatus = "unknown"
)

// CheckpointNamespace is the State checkpoint namespace owned by the Space
// Core loop. Space keeps its own namespace (it does not collapse into the
// published State default) so its snapshots are isolated from other
// producers writing checkpoints against the same thread.
const CheckpointNamespace = "space/core"

// Part is a single content-addressed checkpoint part. Data carries the part
// bytes inline; a nil Data references a blob already stored under the same
// (thread, namespace, channel, version) coordinates, matching the published
// State PutCheckpoint wire shape.
type Part struct {
	Channel string `json:"channel"`
	Version string `json:"version"`
	Data    []byte `json:"data,omitempty"`
}

// Fence carries the optimistic-concurrency lease coordinates used to authorize
// a write against a run. The State service resolves the thread from the run
// lease; it is never taken from the request body.
type Fence struct {
	RunID  string `json:"run_id"`
	Worker string `json:"worker"`
	Epoch  int64  `json:"epoch"`
}

// Checkpoint captures the resumable state of a turn mid-execution. The wire
// fields (Namespace, Parent, Version, Metadata, Parts) mirror the published
// State PutCheckpoint contract so Space snapshots keep their own id, parent
// chain, version and part-versions instead of collapsing onto the thread.
// TurnID/Sequence/State/Messages are Space-side conveniences reconstructed
// from the durable Metadata blob.
type Checkpoint struct {
	ID        string          `json:"id"`
	Namespace string          `json:"namespace,omitempty"`
	Parent    string          `json:"parent,omitempty"`
	Version   int64           `json:"version,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	Parts     []Part          `json:"parts,omitempty"`
	CreatedAt time.Time       `json:"created_at"`

	TurnID   string          `json:"turn_id"`
	Sequence int             `json:"sequence"`
	State    json.RawMessage `json:"state"`
	Messages json.RawMessage `json:"messages"`

	runtimeSnapshot bool
	payloadCold     bool
}

// HITLRequest is a human-in-the-loop approval gate.
type HITLRequest struct {
	ID          string          `json:"id"`
	TurnID      string          `json:"turn_id"`
	Kind        string          `json:"kind"`
	Prompt      json.RawMessage `json:"prompt"`
	Status      HITLStatus      `json:"status"`
	Deadline    *time.Time      `json:"deadline,omitempty"`
	RespondedAt *time.Time      `json:"responded_at,omitempty"`
	Response    json.RawMessage `json:"response,omitempty"`
}

// HITLStatus tracks approval lifecycle.
type HITLStatus string

const (
	HITLPending  HITLStatus = "pending"
	HITLApproved HITLStatus = "approved"
	HITLRejected HITLStatus = "rejected"
	HITLExpired  HITLStatus = "expired"
	HITLCanceled HITLStatus = "canceled"
)

// OutboxEntry is an at-least-once delivery item emitted by the turn.
type OutboxEntry struct {
	ID        string          `json:"id"`
	TurnID    string          `json:"turn_id"`
	Key       string          `json:"key"`
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload"`
	Delivered bool            `json:"delivered"`
	CreatedAt time.Time       `json:"created_at"`
}

// SteeringEvent is an intervention that modifies a running turn's trajectory.
type SteeringEvent struct {
	ID          string    `json:"id"`
	TurnID      string    `json:"turn_id"`
	Instruction string    `json:"instruction"`
	CreatedAt   time.Time `json:"created_at"`
}

// StreamEvent is a real-time event pushed to the client.
type StreamEvent struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
}

// AcceptTurnRequest is the space-level request to start a new turn.
// AgentConfigRef pins the AgentConfig revision and the deployment version to a
// turn, per PDF rev3.2 (L373 "version of AgentConfig is pinned to the turn";
// L948 / L2399 "a new rollout does not change the behavior of already-started
// tasks: the deployment version and AgentConfig are pinned to the task"; L2089
// "it loads the revision of AgentConfig pinned to the turn"). Space is the CARRIER
// of the pin (Registry owns revisions); the runner loads this revision on every
// step and on cold-resume, so a rollout mid-flight cannot alter an in-flight task.
type AgentConfigRef struct {
	ProfileRevision int64             `json:"profile_revision,omitempty"`
	Dot             *DotRuntimeConfig `json:"dot,omitempty"`
	// ToolManifest is supplied only by the server's deployment configuration
	// pinner. It travels with the durable run; a resumed run never reloads a
	// mutable tool schema or accepts definitions from the model/user request.
	ToolManifest      *ToolManifestSnapshot `json:"tool_manifest,omitempty"`
	Revision          int64                 `json:"revision"`
	DeploymentVersion string                `json:"deployment_version,omitempty"`
}

type AcceptTurnRequest struct {
	AgentInstallationID  string `json:"agent_installation_id,omitempty"`
	ProjectID            string `json:"project_id,omitempty"`
	ProjectMemoryProfile string `json:"project_memory_profile,omitempty"`
	// ThreadID continues an existing conversation owned by the caller.
	ThreadID    string          `json:"thread_id,omitempty"`
	Key         string          `json:"key"`
	ContextID   string          `json:"context_id"`
	Input       json.RawMessage `json:"input"`
	MaxSteps    int             `json:"max_steps,omitempty"`
	AgentConfig *AgentConfigRef `json:"agent_config,omitempty"`
}

// AcceptTurnResponse is returned after a turn is created.
type AcceptTurnResponse struct {
	AgentInstallationID  string     `json:"agent_installation_id,omitempty"`
	ProjectID            string     `json:"project_id,omitempty"`
	ProjectMemoryProfile string     `json:"project_memory_profile,omitempty"`
	TurnID               string     `json:"turn_id"`
	ThreadID             string     `json:"thread_id"`
	Status               TurnStatus `json:"status"`
}

// SubmitIntentRequest records a tool/intent from the Core loop.
type SubmitIntentRequest struct {
	TurnID string          `json:"turn_id"`
	Node   string          `json:"node"`
	Call   string          `json:"call"`
	Kind   string          `json:"kind"`
	Target string          `json:"target"`
	Args   json.RawMessage `json:"args"`
}

// SubmitIntentResponse carries the recorded intent back.
type SubmitIntentResponse struct {
	Intent Intent `json:"intent"`
}

// CheckpointRequest saves a mid-execution checkpoint.
type CheckpointRequest struct {
	TurnID   string          `json:"turn_id"`
	Sequence int             `json:"sequence"`
	State    json.RawMessage `json:"state"`
	Messages json.RawMessage `json:"messages"`
}

// CheckpointResponse confirms checkpoint save.
type CheckpointResponse struct {
	Checkpoint Checkpoint `json:"checkpoint"`
}

// HITLSubmitRequest creates or updates a HITL gate.
type HITLSubmitRequest struct {
	TurnID   string          `json:"turn_id"`
	Kind     string          `json:"kind"`
	Prompt   json.RawMessage `json:"prompt"`
	Deadline *time.Time      `json:"deadline,omitempty"`
}

// HITLRespondRequest is a human response to a HITL gate.
type HITLRespondRequest struct {
	HITLID   string          `json:"hitl_id"`
	Approved bool            `json:"approved"`
	Response json.RawMessage `json:"response,omitempty"`
}

// OutboxEmitRequest appends a delivery item.
type OutboxEmitRequest struct {
	TurnID  string          `json:"turn_id"`
	Key     string          `json:"key"`
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

// SteerRequest sends a steering instruction to a running turn.
type SteerRequest struct {
	TurnID      string `json:"turn_id"`
	Instruction string `json:"instruction"`
}

// RecoveryResult reports recovery state after restart.
type RecoveryResult struct {
	Recovered []Turn `json:"recovered"`
	Unknown   []Turn `json:"unknown"`
}

// StepResult from Core after each model/tool iteration.
type StepResult struct {
	// ModelIntentKey links results to their immutable request, even across recovery.
	ModelIntentKey string `json:"model_intent_key,omitempty"`
	// Persisted is an in-process acknowledgement from the durable Coordinator.
	// It never crosses HTTP/State and is optional for custom runtimes.
	Persisted     chan error      `json:"-"`
	GenerationID  string          `json:"generation_id,omitempty"`
	DeltaSequence int64           `json:"delta_sequence,omitempty"`
	StepType      StepType        `json:"step_type"`
	Content       json.RawMessage `json:"content"`
	ToolCall      *ToolCallInfo   `json:"tool_call,omitempty"`
	Done          bool            `json:"done"`
}

// StepType classifies a single step in the Core loop.
type StepType string

const (
	StepModel      StepType = "model"
	StepModelDelta StepType = "model_delta"
	StepTool       StepType = "tool"
	StepCompaction StepType = "compaction"
	StepError      StepType = "error"
	StepDone       StepType = "done"
)

// ToolCallInfo carries tool invocation details from the model response.
type ToolCallInfo struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
	ID   string          `json:"id"`
}
