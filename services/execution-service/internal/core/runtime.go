// Package core implements the Core execution runtime: the model/tool loop
// that drives a turn from input to completion. It respects bounded steps,
// cancellation, checkpointing, and compaction.
package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ModelClient is the interface for calling an LLM. Implementations wrap a
// concrete provider (OpenAI-compatible, Anthropic, local, etc.).
type ModelClient interface {
	// Generate produces one completion given a conversation history and
	// available tools. Returns the assistant message content and any tool
	// calls the model requests.
	Generate(ctx context.Context, req *ModelRequest) (*ModelResponse, error)
}

// ModelRequest carries the conversation and tool definitions for one model call.
type ModelRequest struct {
	Messages  []ChatMessage
	Tools     []ToolDef
	MaxTokens int
	Step      int
	// Attempt distinguishes physical retries of the same loop step. It stays
	// zero for the first call of a step, so the effect key of a normal run is
	// unchanged; a ledger-fenced client uses it to give a retry its own effect
	// key instead of replaying (and being rejected by) the sealed first one.
	Attempt int
	// MutationOffset is the run-local effect coordinate captured with the immutable model intent.
	MutationOffset int
	// MemoryMessages preserves checkpoint provenance in the immutable journal.
	// Provider messages remain clean; the transport builds its own wire body.
	MemoryMessages []int `json:"_ahp_memory_messages,omitempty"`
	// Derived exclusively from confirmed State receipts. This transport cursor
	// is not part of the immutable model request or its operation fingerprint.
	ResumeGenerationID string `json:"-"`
	ResumeAfter        int64  `json:"-"`
}

// ChatMessage is one message in the model conversation.
type ChatMessage struct {
	// Source is checkpoint metadata. It is stripped before every provider call.
	Source     string          `json:"_ahp_source,omitempty"`
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	ToolCalls  json.RawMessage `json:"tool_calls,omitempty"`
}

// ToolDef describes a tool the model may invoke.
type ToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// ModelResponse is the model's reply after one generate call.
type ModelResponse struct {
	GenerationID string          `json:"generation_id,omitempty"`
	Content      json.RawMessage `json:"content"`
	ToolCalls    []ToolCall      `json:"tool_calls,omitempty"`
	Stop         string          `json:"stop,omitempty"`
	Usage        *Usage          `json:"usage,omitempty"`
}

// ToolCall is one tool invocation requested by the model.
type ToolCall struct {
	ID   string          `json:"id"`
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
}

// Usage reports token counts.
type Usage struct {
	Input  int `json:"input"`
	Output int `json:"output"`
}

// ToolExecutor dispatches a tool call and returns the result.
type ToolExecutor interface {
	// Execute runs the named tool. Returns the result payload and whether
	// execution was successful.
	Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error)
}

// ToolRegistry holds available tools and their executor.
type ToolRegistry struct {
	mu       sync.RWMutex
	tools    map[string]ToolDef
	executor ToolExecutor
}

// NewToolRegistry creates a registry with the given executor.
func NewToolRegistry(executor ToolExecutor) *ToolRegistry {
	return &ToolRegistry{
		tools:    make(map[string]ToolDef),
		executor: executor,
	}
}

// Register adds a tool definition to the registry.
func (r *ToolRegistry) Register(def ToolDef) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[def.Name] = def
}

// Defs returns all registered tool definitions. A nil registry yields no tools,
// so a model-only Runtime (cfg.Tools == nil) is a valid configuration and must
// not panic when Run() assembles the model request.
func (r *ToolRegistry) Defs() []ToolDef {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	defs := make([]ToolDef, 0, len(r.tools))
	for _, d := range r.tools {
		defs = append(defs, d)
	}
	return defs
}

// Execute dispatches to the registry's executor.
func (r *ToolRegistry) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	r.mu.RLock()
	_, ok := r.tools[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown tool: %s", name)
	}
	return r.executor.Execute(ctx, name, args)
}

// Compactor manages context window budget.
type Compactor interface {
	// Compact reduces the message history to fit within budget. Returns
	// the compacted messages and the number of tokens saved.
	Compact(ctx context.Context, messages []ChatMessage, budget int) ([]ChatMessage, int, error)
}

// RuntimeConfig configures the Core runtime.
type RuntimeConfig struct {
	// ToolAllowed is an optional per-run tenant ceiling. Nil preserves legacy behavior.
	ToolAllowed func(string) bool
	// Artifacts externalizes large outputs through the owner API.
	Artifacts  OutputArtifacts
	Memory     MemoryDriver
	Model      ModelClient
	Tools      *ToolRegistry
	Compactor  Compactor
	State      *space.StateClient
	MaxTokens  int
	CompBudget int
	// Logger receives bounded lifecycle warnings from the loop (empty model
	// turns, retry decisions). nil falls back to the default logger.
	Logger *slog.Logger
	// Durable, when non-nil, makes the Core loop persist every emitted step
	// under a canonical per-step key (space.StepKey), so a retried step dedupes
	// and a reused key with different content is permanently rejected
	// (space.ErrKeyReuse). nil keeps the loop emit-only (back-compat).
	Durable space.DurabilitySink
	// StepAccountant, when non-nil, books every model step the loop consumes with
	// the platform's budget tree (internal/treedelegate), so a step taken under a
	// caller-stated budget is visible outside this process before the loop spends
	// anything on top of it. nil keeps the loop exactly as it was: tree accounting
	// is a deployment decision, not a property of the loop.
	StepAccountant StepAccountant
}

// Runtime implements space.CoreRunner: the bounded model/tool loop.
type Runtime struct {
	cfg RuntimeConfig
}

// NewRuntime creates a Core runtime from config.
func NewRuntime(cfg RuntimeConfig) *Runtime {
	if cfg.MaxTokens <= 0 {
		// Not a silent one: the Model Gateway refuses a completion cap above
		// the number it stated itself (DEF-DOTE2E-9), so a caller that stated
		// nothing must be able to see that 4096 was filled in on its behalf --
		// that is exactly the value a capped deployment answers with a refusal.
		cfg.MaxTokens = 4096
		logger := cfg.Logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn("core: caller stated no completion cap; substituting MaxTokens=4096",
			"component", "core", "max_tokens", cfg.MaxTokens,
			"consequence", "a gateway with a lower stated cap will refuse this run")
	}
	if cfg.CompBudget <= 0 {
		cfg.CompBudget = 100000
	}
	return &Runtime{cfg: cfg}
}

// blankTurn reports whether a model completion carries no usable content:
// null, an empty/whitespace-only string, or an absent payload. A blank turn
// with no tool calls is NOT a completion - it is a provider hiccup - and the
// loop must not silently flip a run to `done` on it (measured on the stand:
// after a cold rehydrate the provider returned one blank turn and the run
// completed with zero work done, which is a false-positive terminal state).
func blankTurn(content json.RawMessage) bool {
	if len(content) == 0 || string(content) == "null" {
		return true
	}
	var s string
	if err := json.Unmarshal(content, &s); err == nil {
		return strings.TrimSpace(s) == ""
	}
	// A non-string JSON payload may still be an empty container. A completion
	// whose content is {} or [] (no text, no tool) carries no usable payload, so
	// it is blank like an empty string; any non-empty structured payload is content.
	var v any
	if err := json.Unmarshal(content, &v); err == nil {
		switch t := v.(type) {
		case map[string]any:
			return len(t) == 0
		case []any:
			return len(t) == 0
		}
	}
	return false
}

// maxEmptyTurnRetries bounds ONE retry of a blank model turn per loop step.
// The retry exists because providers legitimately hiccup with an empty 200;
// a second blank turn is treated as a hard step failure, never as completion.
const maxEmptyTurnRetries = 1

// finishReasonLength is the OpenAI-compatible finish_reason a provider sets when
// a completion was cut by the output token cap instead of finishing.
const finishReasonLength = "length"

// Two distinct failure classes for a turn that produced nothing usable, split so
// the journal can tell them apart. A provider hiccup (ErrBlankTurn) rides out on
// one retry; the output budget running dry (ErrOutputBudgetExhausted) does not -
// re-sending the same request at the same cap exhausts it again - so it must not
// be retried and must not masquerade as a blank turn.
var (
	ErrBlankTurn             = errors.New("model returned an empty turn twice")
	ErrOutputBudgetExhausted = errors.New("model output budget exhausted")
)

// blankTurnFields is the observability the blank/length journal lines must carry:
// the provider's finish_reason and its completion-token count, so an operator can
// read "the budget was eaten" apart from "the provider returned empty" without
// guessing. run_id names the run. Prompt content and secrets are never logged.
func blankTurnFields(req *space.CoreRequest, step int, resp *ModelResponse) []any {
	completionTokens := 0
	if resp.Usage != nil {
		completionTokens = resp.Usage.Output
	}
	return []any{
		"run_id", req.RunID,
		"step", step,
		"finish_reason", resp.Stop,
		"completion_tokens", completionTokens,
	}
}

// stopOnOutputBudget fails a step whose completion was cut by the output cap with
// no usable content. It is deliberately NOT the blank-turn path: with no content
// there is nothing to continue, and re-sending the same request at the same cap
// would exhaust the budget again, so it gets its own error class and no retry.
func (r *Runtime) stopOnOutputBudget(ctx context.Context, req *space.CoreRequest, step int, resp *ModelResponse, rec *stepRecorder, results chan<- space.StepResult) error {
	rec.push(ctx, results, space.StepResult{
		StepType: space.StepError,
		Content: jsonBytes(map[string]string{
			"error":         ErrOutputBudgetExhausted.Error(),
			"finish_reason": finishReasonLength,
		}),
		Done: true,
	})
	r.log().Warn("core: run stopped on an output-budget exhaustion", blankTurnFields(req, step, resp)...)
	return fmt.Errorf("model output budget exhausted at step %d (finish_reason=length): %w", step, ErrOutputBudgetExhausted)
}

func (r *Runtime) log() *slog.Logger {
	if r.cfg.Logger != nil {
		return r.cfg.Logger
	}
	return slog.Default()
}

// Run implements space.CoreRunner. It executes the model/tool loop, emitting
// StepResults until the turn completes, is cancelled, or hits the step limit.
func (r *Runtime) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	var replayRequest *ModelRequest
	if req.ModelReplay != nil {
		var restored ModelRequest
		if err := json.Unmarshal(req.ModelReplay.Request, &restored); err != nil {
			return fmt.Errorf("invalid pending model request: %w", err)
		}
		copyReq := *req
		req = &copyReq
		req.Messages = jsonBytes(restoreMemoryMessages(restored.Messages, restored.MemoryMessages))
		req.ModelStepOffset = restored.Step
		if req.Checkpoint == nil {
			req.Checkpoint = &space.Checkpoint{}
		}
		replayRequest = &restored
	}

	ctx = WithRunScope(ctx, req.Scope, req.TaskToken)
	ctx = WithRunBinding(ctx, RunBinding{TaskID: req.RunID, ContextID: req.ContextID})
	// When a durable sink is wired, propagate the per-run fence + task-token via
	// context so a shared sink (e.g. the Emil seam StateRunStore bridge) can persist
	// every step to the State API under the correct lease without the engine holding
	// per-run state. Inert when cfg.Durable == nil (production default unchanged).
	if r.cfg.Durable != nil {
		ctx = space.WithRunContext(ctx, space.RunContext{
			Fence:     space.Fence{RunID: req.RunID, Worker: req.Worker, Epoch: req.Epoch},
			ThreadID:  req.ThreadID,
			TaskToken: req.TaskToken,
		})
	}
	messages := r.initMessages(req)
	if replayRequest == nil {
		messages = withoutMemory(messages)
	}
	rec := newStepRecorder(r.cfg.Durable, req.RunID)
	rec.requireAck = req.JournalModel != nil
	if req.ProjectMemoryProfile == "shared" {
		if _, ok := r.cfg.Memory.(MemoryReplayAccess); !ok {
			rec.push(ctx, results, space.StepResult{StepType: space.StepError, Content: jsonBytes(map[string]string{"error": "shared memory authority is not configured"}), Done: true})
			return contracts.NewError(contracts.ErrUnavailable, "shared memory authority is not configured")
		}
	}
	if r.cfg.Memory != nil && replayRequest != nil {
		if guard, ok := r.cfg.Memory.(MemoryReplayAccess); ok {
			if err := guard.ValidateProject(ctx, req); err != nil {
				rec.push(ctx, results, space.StepResult{StepType: space.StepError, Content: jsonBytes(map[string]string{"error": "memory project access unavailable"}), Done: true})
				return fmt.Errorf("validate replay memory project: %w", err)
			}
		}
	}
	if r.cfg.Memory != nil && replayRequest == nil {
		injected, err := r.cfg.Memory.Prepare(ctx, req)
		if err != nil {
			rec.push(ctx, results, space.StepResult{StepType: space.StepError, Content: jsonBytes(map[string]string{"error": "memory context unavailable"}), Done: true})
			return fmt.Errorf("prepare memory: %w", err)
		}
		for i := range injected {
			injected[i].Source = memorySource
		}
		messages = injectMemoryMessages(messages, injected)
	}
	// Every external effect this life produces passes the mutation journal, which is
	// what makes a re-delivery of the same run replay recorded outcomes instead of
	// touching the world twice. Inert when nothing can journal (no State client, no
	// run token) -- see mutation.go for the contract and the failure rules.
	journal := r.newMutationJournal(req)
	if journal != nil && req.ModelReplay != nil {
		journal.next = replayRequest.MutationOffset + len(req.ModelReplay.Tools)
	}
	// A request that carries a restored checkpoint RESUMES a run: an earlier life made
	// earlier steps durable and those bodies cannot be reproduced here, so the recorder
	// continues at the durable high-water. Every other life is a cold (re)delivery of the
	// same request, whose steps must reuse the same logical keys and dedup.
	if req.Checkpoint != nil && len(req.Messages) > 0 {
		rec.resumeFromDurable()
	}
	maxSteps := req.MaxSteps
	if maxSteps <= 0 {
		maxSteps = 64
	}
	// A cold owner receives the remaining logical steps, not a fresh budget.
	maxSteps -= req.ModelStepOffset
	if maxSteps <= 0 {
		return fmt.Errorf("logical model step budget exhausted")
	}

	for step := 0; step < maxSteps; step++ {
		select {
		case <-ctx.Done():
			rec.push(ctx, results, space.StepResult{StepType: space.StepError, Done: true})
			return ctx.Err()
		default:
		}

		if r.cfg.Compactor != nil {
			compacted, saved, err := r.cfg.Compactor.Compact(ctx, messages, r.cfg.CompBudget)
			if err == nil && saved > 0 {
				messages = compacted
				rec.push(ctx, results, space.StepResult{StepType: space.StepCompaction})
			}
		}

		modelReq := &ModelRequest{
			Messages:       providerMessages(messages),
			MemoryMessages: memoryMessageIndices(messages),
			Tools:          r.toolDefs(),
			MaxTokens:      r.cfg.MaxTokens,
			Step:           req.ModelStepOffset + step,
		}
		if journal != nil {
			modelReq.MutationOffset = journal.next
		}
		if step == 0 && replayRequest != nil {
			modelReq = replayRequest
		}
		replayCompleted := step == 0 && req.ModelReplay != nil && req.ModelReplay.Completed
		resp, err := r.generate(ctx, modelReq, req, rec, results)
		if err != nil {
			rec.push(ctx, results, space.StepResult{
				StepType: space.StepError,
				Content:  jsonBytes(map[string]string{"error": err.Error()}),
				Done:     true,
			})
			return fmt.Errorf("model generate step %d: %w", step, err)
		}

		// The turn just taken is charged to the budget tree before the loop does
		// anything more expensive with its answer. A tree that cannot take the
		// charge has no remainder for this run, and that is terminal: continuing
		// would spend budget the platform has already committed elsewhere in the
		// tree, which is the failure this accounting exists to make impossible.
		if err := r.chargeRecoveredStep(ctx, req, req.ModelStepOffset+step, replayCompleted); err != nil {
			rec.push(ctx, results, space.StepResult{
				StepType: space.StepError,
				Content:  jsonBytes(map[string]string{"error": err.Error()}),
				Done:     true,
			})
			return err
		}

		// A blank completion with no tool calls is not a completion, and
		// finish_reason says which of two different failures it is: "length"
		// means the output cap was spent before the model produced anything
		// (a reasoning model routinely burns the whole budget thinking), which
		// no re-send of the same request at the same cap can fix; anything else
		// is a transient empty 200 that one retry is designed to ride out. Both
		// fail the step loudly rather than completing the run, but only the
		// transient one is retried, and each names its own class.
		if blankTurn(resp.Content) && len(resp.ToolCalls) == 0 {
			if resp.Stop == finishReasonLength {
				return r.stopOnOutputBudget(ctx, req, step, resp, rec, results)
			}
			r.log().Warn("core: model returned a blank turn", blankTurnFields(req, step, resp)...)
			var berr error
			for retry := 0; retry < maxEmptyTurnRetries; retry++ {
				// The retry is a second physical spend, so it must carry its own
				// effect key: reusing the sealed key of the blank attempt makes
				// every retry fail as a replayed effect instead of calling out.
				modelReq.Attempt = retry + 1
				resp, berr = r.generate(ctx, modelReq, req, rec, results)
				if berr != nil {
					rec.push(ctx, results, space.StepResult{
						StepType: space.StepError,
						Content:  jsonBytes(map[string]string{"error": berr.Error()}),
						Done:     true,
					})
					return fmt.Errorf("model generate step %d (blank retry): %w", step, berr)
				}
				if !blankTurn(resp.Content) || len(resp.ToolCalls) > 0 {
					break
				}
				// A retry that comes back cut off is the same budget exhaustion
				// the first call would have reported; say it once, no further loop.
				if resp.Stop == finishReasonLength {
					return r.stopOnOutputBudget(ctx, req, step, resp, rec, results)
				}
			}
			if blankTurn(resp.Content) && len(resp.ToolCalls) == 0 {
				rec.push(ctx, results, space.StepResult{
					StepType: space.StepError,
					Content:  jsonBytes(map[string]string{"error": ErrBlankTurn.Error()}),
					Done:     true,
				})
				r.log().Warn("core: run stopped on a persistent blank model turn", blankTurnFields(req, step, resp)...)
				return fmt.Errorf("model blank turn at step %d: %w", step, ErrBlankTurn)
			}
		}

		content, spillErr := r.externalizeOutput(ctx, req, "model-output", resp.Content)
		if spillErr != nil {
			return fmt.Errorf("persist model output: %w", spillErr)
		}
		resp.Content = content
		if !replayCompleted {
			rec.push(ctx, results, space.StepResult{
				StepType:     space.StepModel,
				GenerationID: resp.GenerationID,
				Content:      resp.Content,
			})
		}

		assistantMsg := ChatMessage{Role: "assistant", Content: resp.Content}
		if len(resp.ToolCalls) > 0 {
			tc, _ := json.Marshal(resp.ToolCalls)
			assistantMsg.ToolCalls = tc
		}
		messages = append(messages, assistantMsg)

		if len(resp.ToolCalls) == 0 {
			// finish_reason "length" means the completion was cut by the token
			// cap, not finished: treating it as completion silently truncates
			// the turn mid-sentence and reports success (measured on the stand:
			// a long report step hit the cap, emitted no tool call, and the run
			// went `done` without ever producing the final artifact). Continue
			// the loop inside the bounded step budget instead.
			if resp.Stop == finishReasonLength {
				if step == maxSteps-1 {
					rec.push(ctx, results, space.StepResult{StepType: space.StepError, Content: jsonBytes(map[string]string{"error": "step limit reached with truncated model response"}), Done: true})
					return fmt.Errorf("step limit reached with truncated model response")
				}
				r.log().Warn("core: model completion truncated by token cap, continuing", "step", step)
				messages = append(messages, ChatMessage{
					Role:    "user",
					Content: json.RawMessage(`"Your previous answer was truncated by the output limit. Continue exactly where you stopped and finish the remaining steps."`),
				})
				continue
			}
			if !(step == 0 && req.ModelReplay != nil && req.ModelReplay.Done) {
				rec.push(ctx, results, space.StepResult{StepType: space.StepDone, Done: true})
			}
			return nil
		}

		for _, tc := range resp.ToolCalls {
			if replayCompleted {
				var prior *space.StepResult
				for i := range req.ModelReplay.Tools {
					saved := &req.ModelReplay.Tools[i]
					if saved.ToolCall != nil && saved.ToolCall.ID == tc.ID {
						prior = saved
						break
					}
				}
				if prior != nil {
					if tc.Name == ToolReadOutputChunk {
						data, err := r.readOutputChunk(ctx, req, tc.Args)
						if err != nil {
							return err
						}
						copy := *prior
						copy.Content = data
						prior = &copy
					}
					messages = append(messages, ChatMessage{Role: "tool", Content: prior.Content, ToolCallID: tc.ID})
					continue
				}
			}

			select {
			case <-ctx.Done():
				rec.push(ctx, results, space.StepResult{StepType: space.StepError, Done: true})
				return ctx.Err()
			default:
			}

			toolResult, execErr := r.executeTool(ctx, req, journal, tc)
			if execErr != nil {
				if unconfirmedOutcome(ctx, execErr) {
					return execErr
				}
				toolResult, _ = json.Marshal(map[string]string{"error": execErr.Error()})
			}

			toolMsg := ChatMessage{
				Role:       "tool",
				Content:    toolResult,
				ToolCallID: tc.ID,
			}
			messages = append(messages, toolMsg)

			rec.push(ctx, results, space.StepResult{
				StepType: space.StepTool,
				Content:  toolResult,
				ToolCall: &space.ToolCallInfo{Name: tc.Name, Args: tc.Args, ID: tc.ID},
			})
		}

		if step == maxSteps-1 {
			rec.push(ctx, results, space.StepResult{
				StepType: space.StepError,
				Content:  jsonBytes(map[string]string{"error": "step limit reached"}),
				Done:     true,
			})
			return fmt.Errorf("step limit %d reached", maxSteps)
		}
	}
	return nil
}

func (r *Runtime) initMessages(req *space.CoreRequest) []ChatMessage {
	var msgs []ChatMessage
	if len(req.Messages) > 0 {
		_ = json.Unmarshal(req.Messages, &msgs)
	}
	if len(msgs) > 0 && req.Checkpoint != nil {
		// Resuming from a checkpoint: the persisted history already
		// contains the user input that started the turn, so do not append it again.
		return msgs
	}
	inputMsg := ChatMessage{Role: "user", Content: req.Input}
	msgs = append(msgs, inputMsg)
	return msgs
}

// executeTool performs one tool call, guarded by the run's mutation journal: the
// effect is journalled and dispatched BEFORE it happens, its outcome is written AFTER
// it, and an already-resolved mutation is answered from the journal instead of being
// performed again. A journal that cannot answer is a refusal, not a warning: an effect
// this run cannot reconcile is an effect this run must not perform.
func (r *Runtime) executeTool(ctx context.Context, req *space.CoreRequest, journal *mutationJournal, tc ToolCall) (json.RawMessage, error) {
	if r.cfg.ToolAllowed != nil && !r.cfg.ToolAllowed(tc.Name) {
		return nil, contracts.NewError(contracts.ErrForbidden, "tool is disabled by tenant policy")
	}
	call := journal.callKey()
	if tc.Name == ToolReadOutputChunk {
		return r.readOutputChunk(ctx, req, tc.Args)
	}
	isMemory := r.cfg.Memory != nil && r.cfg.Memory.Handles(tc.Name)
	if r.cfg.Tools == nil && !isMemory {
		return nil, fmt.Errorf("no tool registry")
	}
	if recorded, replayed, refusal := journal.begin(ctx, call, tc.Name, tc.Args); refusal != nil {
		return nil, refusal
	} else if replayed {
		return recorded, nil
	}
	// Give the tool the run's task scope so a scope-aware executor (e.g.
	// save_artifact) seals under the correct task labels, per the PDF: "the
	// file at Seal gets the labels of the task that created it".
	ctx = WithRunScope(ctx, req.Scope, req.TaskToken)
	// ...and which conversation and run it belongs to, so what a tool writes
	// can be cascade-deleted with the thread that produced it. The conversation
	// is the context id (the value a task token carries), not State's internal
	// thread id; the thread id is only a fallback for a caller that has no
	// context to name.
	bindingContext := req.ContextID
	if bindingContext == "" {
		bindingContext = req.ThreadID
	}
	// The budget travels with the binding, not as a per-adapter side channel: a tool
	// that delegates has to repeat the caller's number and say it was the caller's.
	// The loop above bounds itself with its own fallback (64) when nothing was stated,
	// and that fallback is deliberately NOT forwarded here - see RunBinding.MaxStepsStated.
	ctx = WithRunBinding(ctx, RunBinding{
		ContextID:      bindingContext,
		TaskID:         req.RunID,
		OperationKey:   call,
		MaxSteps:       req.MaxSteps,
		MaxStepsStated: req.MaxSteps > 0,
	})

	var result json.RawMessage
	var execErr error
	if isMemory {
		result, execErr = r.cfg.Memory.Execute(ctx, req, tc)
	} else {
		result, execErr = r.cfg.Tools.Execute(ctx, tc.Name, tc.Args)
	}
	if execErr != nil {
		journal.resolve(ctx, call, jsonBytes(map[string]string{"error": execErr.Error()}), execErr)
		return nil, execErr
	}
	result, spillErr := r.externalizeOutput(ctx, req, "tool-output", result)
	if spillErr != nil {
		unknown := contracts.NewReasoned(contracts.ErrConflict, "SIDE_EFFECT_UNKNOWN", "tool completed but its large output could not be persisted")
		journal.resolve(ctx, call, nil, unknown)
		return nil, unknown
	}
	journal.resolve(ctx, call, result, nil)
	return result, nil
}

func emit(ch chan<- space.StepResult, r space.StepResult) {
	select {
	case ch <- r:
	default:
	}
}

func jsonBytes(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func (r *Runtime) toolDefs() []ToolDef {
	defs := r.cfg.Tools.Defs()
	if r.cfg.Artifacts != nil {
		filtered := defs[:0]
		for _, def := range defs {
			if def.Name != ToolReadOutputChunk {
				filtered = append(filtered, def)
			}
		}
		defs = append(filtered, readOutputDef())
	}
	if r.cfg.Memory != nil {
		filtered := make([]ToolDef, 0, len(defs)+3)
		for _, def := range defs {
			if !r.cfg.Memory.Handles(def.Name) {
				filtered = append(filtered, def)
			}
		}
		defs = append(filtered, r.cfg.Memory.Defs()...)
	}
	if r.cfg.ToolAllowed != nil {
		filtered := defs[:0]
		for _, def := range defs {
			if r.cfg.ToolAllowed(def.Name) {
				filtered = append(filtered, def)
			}
		}
		defs = filtered
	}
	return defs
}
