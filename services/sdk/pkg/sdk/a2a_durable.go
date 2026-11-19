package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

const nativeRunType = "ahp.native.a2a/1"

type nativeRunInput struct {
	Type    string       `json:"type"`
	Message *a2a.Message `json:"native_input"`
}

type durableNativeState struct {
	Task *a2a.Task `json:"task"`
	// Retained until the final/waiting commit ACKs this invocation. The
	// input must survive a worker dying after publishing its working status.
	InputIDs []string `json:"input_ids,omitempty"`
}

type durableNativeAnswer struct {
	RunID        string          `json:"run_id"`
	TaskID       string          `json:"task_id"`
	NativeTaskID a2a.TaskID      `json:"native_task_id"`
	MessageID    string          `json:"message_id"`
	QuestionID   string          `json:"question_id"`
	Input        json.RawMessage `json:"input"`
	Message      *a2a.Message    `json:"message"`
}

// NewDurableA2AHandler owns native protocol state in AHP State, instead of the
// upstream in-memory taskstore. State must have CommonRecords enabled. The
// surrounding ingress must verify mTLS/task JWT and install transport.Principal.
//
// Native task ID is the durable State run ID; Identity's grant task remains a
// separate value. The initial message is immutable admission data. Working,
// question and terminal native snapshots are fenced State commits. A fresh
// instance can read history and accept an answer without old process memory.
//
// RunDurableNativeWorker supplies background recovery of eligible native runs;
// it must be started under the host's service lifecycle. Retrying the same
// initial message or continuation can also claim an eligible run.
// Push notifications, list and cancellation are not advertised by this host yet.
func NewDurableA2AHandler(cfg *Config, graph GraphFunc) (a2asrv.RequestHandler, error) {
	if cfg == nil {
		return nil, fmt.Errorf("sdk: native host configuration is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if graph == nil {
		return nil, fmt.Errorf("sdk: graph function must not be nil")
	}
	copy := *cfg
	h := &durableNativeHandler{cfg: &copy, graph: graph}
	return &a2asrv.InterceptedHandler{Handler: h, Interceptors: []a2asrv.CallInterceptor{nativePrincipalInterceptor{}}}, nil
}

type durableNativeHandler struct {
	cfg   *Config
	graph GraphFunc
}

// RunDurableNativeWorker executes only State-admitted runs of nativeRunType,
// including expired leases left by a dead host. State's FIFO and epoch checks
// still choose the eligible head; other runtime profiles are never executed.
//
// Admission is not a caller assertion: scope, task, input and fresh credential
// come from the authenticated State claim response. The returned JWT is checked
// by each receiving service. No old process credential or task map is needed.
// The service supervisor must observe errors and restart on recoverable faults;
// this function never turns an unknown commit outcome into a new operation key.
func RunDurableNativeWorker(ctx context.Context, cfg *Config, graph GraphFunc) error {
	if cfg == nil || graph == nil {
		return fmt.Errorf("sdk: native worker requires configuration and graph")
	}
	copy := *cfg
	if err := copy.Validate(); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		claimed, err := copy.StateClient.ClaimRunWithFilter(ctx, copy.Worker, copy.LeaseTTLMS(), sdkstate.ClaimFilter{PayloadType: nativeRunType})
		if err != nil {
			return fmt.Errorf("sdk: native worker claim: %w", err)
		}
		if claimed != nil {
			scope := claimed.Lease.Thread.Scope
			if scope.Validate() != nil || claimed.Lease.Scope != scope || claimed.Lease.Run.TaskID == "" || claimed.Lease.Thread.ContextID == "" {
				return fmt.Errorf("sdk: native worker received inconsistent scope/task")
			}
			principal := transport.Principal{Scope: scope, Claims: auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject, Task: claimed.Lease.Run.TaskID, Context: claimed.Lease.Thread.ContextID}}
			workCtx := transport.WithPrincipal(ctx, principal)
			workCtx = context.WithValue(workCtx, nativeCredentialKey{}, nativeCredential{token: claimed.TaskToken, principal: principal})
			lease, err := holdClaim(workCtx, &copy, claimed, claimed.Lease.Run.ID)
			if err != nil {
				return err
			}
			h := &durableNativeHandler{cfg: lease.cfg, graph: graph}
			if err = h.execute(workCtx, lease); err != nil {
				return fmt.Errorf("sdk: native worker execution: %w", err)
			}
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func nativeNamespace(run string) string { return "ahp.native." + run }

func nativeDocument(s durableNativeState) (sdkstate.CommonDocument, error) {
	b, err := json.Marshal(s)
	return sdkstate.CommonDocument{Format: sdkstate.CommonFormat, Profile: nativeRunType, Root: "task", Records: map[string]sdkstate.CommonRecord{"task": {Kind: "native-task", Attributes: b, Relations: map[string][]string{}}}}, err
}

// Admission is durable before any protocol response. Reusing the message key
// with changed content is rejected by State's CreateRun receipt.
func (h *durableNativeHandler) admit(ctx context.Context, msg *a2a.Message) (sdkstate.Run, error) {
	p, ok := transport.FromContext(ctx)
	if !ok {
		return sdkstate.Run{}, a2a.ErrUnauthenticated
	}
	thread, err := h.cfg.StateClient.CreateThread(ctx, msg.ContextID, "ctx:"+msg.ContextID)
	if err != nil {
		return sdkstate.Run{}, err
	}
	if thread.Scope != p.Scope {
		return sdkstate.Run{}, a2a.ErrUnauthorized
	}
	key := contracts.OperationKey(p.Scope, p.Claims.Task, "sdk.native.initial", msg.ID)
	b, err := json.Marshal(nativeRunInput{Type: nativeRunType, Message: msg})
	if err != nil {
		return sdkstate.Run{}, err
	}
	run, err := h.cfg.StateClient.CreateRun(ctx, thread.ID, key, b)
	if err != nil {
		return run, err
	}
	if run.TaskID != p.Claims.Task || run.ThreadID != thread.ID {
		return sdkstate.Run{}, a2a.ErrUnauthorized
	}
	return run, nil
}

func (h *durableNativeHandler) load(ctx context.Context, id a2a.TaskID) (sdkstate.Run, durableNativeState, int64, error) {
	var empty durableNativeState
	p, ok := transport.FromContext(ctx)
	if !ok {
		return sdkstate.Run{}, empty, 0, a2a.ErrUnauthenticated
	}
	if !validNativeInputID(string(id)) {
		return sdkstate.Run{}, empty, 0, a2a.ErrInvalidParams
	}
	run, err := h.cfg.StateClient.GetRun(ctx, string(id))
	if contracts.Code(err) == contracts.ErrNotFound {
		return run, empty, 0, a2a.ErrTaskNotFound
	}
	if err != nil {
		return run, empty, 0, err
	}
	if run.TaskID != p.Claims.Task {
		return run, empty, 0, a2a.ErrTaskNotFound
	}
	thread, err := h.cfg.StateClient.GetThread(ctx, run.ThreadID)
	if err != nil {
		return run, empty, 0, err
	}
	if thread.Scope != p.Scope || thread.ContextID != p.Claims.Context {
		return run, empty, 0, a2a.ErrTaskNotFound
	}
	var input nativeRunInput
	if json.Unmarshal(run.Payload, &input) != nil || input.Type != nativeRunType || input.Message == nil || input.Message.ContextID != thread.ContextID {
		return run, empty, 0, a2a.ErrTaskNotFound
	}
	stored, err := h.cfg.StateClient.ReadRecords(ctx, thread.ID, nativeNamespace(run.ID), 0)
	if contracts.Code(err) == contracts.ErrNotFound {
		// No protocol snapshot exists until a worker first owns the run.
		if sdkstate.IsTerminalStatus(run.Status) || run.Status == sdkstate.RunWaiting {
			return run, empty, 0, fmt.Errorf("sdk: native run ended without a protocol snapshot")
		}
		task := &a2a.Task{ID: id, ContextID: thread.ContextID, History: []*a2a.Message{input.Message}, Status: a2a.TaskStatus{State: a2a.TaskStateSubmitted}}
		return run, durableNativeState{Task: task}, 0, nil
	}
	if err != nil {
		return run, empty, 0, err
	}
	if stored.Document.Profile != nativeRunType || stored.Document.Root != "task" || json.Unmarshal(stored.Document.Records["task"].Attributes, &empty) != nil || empty.Task == nil || empty.Task.ID != id || empty.Task.ContextID != thread.ContextID || stored.Receipt.RunID != run.ID {
		return run, durableNativeState{}, 0, fmt.Errorf("sdk: invalid durable native task snapshot")
	}
	return run, empty, stored.Receipt.Revision, nil
}

func trimNativeHistory(task *a2a.Task, length *int) (*a2a.Task, error) {
	if length != nil {
		if *length < 0 {
			return nil, a2a.ErrInvalidParams
		}
		if *length < len(task.History) {
			task.History = task.History[len(task.History)-*length:]
		}
	}
	return task, nil
}

func (h *durableNativeHandler) GetTask(ctx context.Context, req *a2a.GetTaskRequest) (*a2a.Task, error) {
	_, s, _, err := h.load(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	return trimNativeHistory(s.Task, req.HistoryLength)
}

func sameNativeMessage(a, b *a2a.Message) bool {
	x, e1 := contracts.Digest(a)
	y, e2 := contracts.Digest(b)
	return e1 == nil && e2 == nil && x == y
}

func (h *durableNativeHandler) acceptAnswer(ctx context.Context, run sdkstate.Run, s durableNativeState, msg *a2a.Message) error {
	for _, old := range s.Task.History {
		if old.ID == msg.ID {
			if !sameNativeMessage(old, msg) {
				return fmt.Errorf("%w: message ID already has different content", a2a.ErrInvalidParams)
			}
			return nil // Durable replay, including after the task has completed.
		}
	}
	question := s.Task.Status.Message
	if s.Task.Status.State != a2a.TaskStateInputRequired || question == nil || !validNativeInputID(question.ID) {
		return fmt.Errorf("%w: task has no pending question", a2a.ErrInvalidParams)
	}
	wait, _ := question.Metadata[nativeWaitKindMetadata].(string)
	if wait == "" {
		return fmt.Errorf("sdk: durable question is missing wait kind")
	}
	key, err := contracts.Digest([]string{"sdk.native.answer", run.ID, question.ID})
	if err != nil {
		return err
	}
	b, err := json.Marshal(durableNativeAnswer{RunID: run.ID, TaskID: run.TaskID, NativeTaskID: s.Task.ID, MessageID: msg.ID, QuestionID: question.ID, Input: extractPayload(msg), Message: msg})
	if err != nil {
		return err
	}
	// One answer per question: concurrent different messages cannot silently
	// enqueue two commands. A lost ACK is replayed with exactly the same body.
	_, err = h.cfg.StateClient.Enqueue(ctx, run.ThreadID, key, wait, b)
	return err
}

func (h *durableNativeHandler) SendMessage(ctx context.Context, req *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
	if req.Config != nil && req.Config.HistoryLength != nil && *req.Config.HistoryLength < 0 {
		return nil, a2a.ErrInvalidParams
	}
	if req.Config != nil && (req.Config.ReturnImmediately || req.Config.PushConfig != nil) {
		return nil, fmt.Errorf("%w: this host requires synchronous send without push", a2a.ErrInvalidParams)
	}
	msg := req.Message
	var run sdkstate.Run
	var err error
	if msg.TaskID == "" {
		run, err = h.admit(ctx, msg)
	} else {
		var s durableNativeState
		run, s, _, err = h.load(ctx, msg.TaskID)
		if err == nil {
			err = h.acceptAnswer(ctx, run, s, msg)
		}
	}
	if err != nil {
		return nil, err
	}
	for {
		current, s, _, err := h.load(ctx, a2a.TaskID(run.ID))
		if err != nil {
			return nil, err
		}
		if sdkstate.IsTerminalStatus(current.Status) || current.Status == sdkstate.RunWaiting {
			var length *int
			if req.Config != nil {
				length = req.Config.HistoryLength
			}
			return trimNativeHistory(s.Task, length)
		}
		lease, err := ClaimTaskRun(ctx, h.cfg, run.ID)
		if err != nil {
			return nil, err
		}
		if lease != nil {
			if err = h.execute(ctx, lease); err != nil {
				return nil, err
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (h *durableNativeHandler) execute(ctx context.Context, lease *ActiveLease) error {
	defer lease.finish()
	run, s, rev, err := h.load(ctx, a2a.TaskID(lease.RunID()))
	if err != nil {
		return err
	}
	claim := lease.ClaimResult()
	if claim.Lease.Run.TaskID != run.TaskID || claim.Lease.Run.ThreadID != run.ThreadID {
		return a2a.ErrUnauthorized
	}
	var input nativeRunInput
	if err = json.Unmarshal(claim.Lease.Run.Payload, &input); err != nil || input.Type != nativeRunType || input.Message == nil {
		return fmt.Errorf("sdk: claim has no native input")
	}
	bound := lease.cfg
	if s.Task.Status.State == a2a.TaskStateInputRequired {
		items, err := bound.StateClient.ReadInbox(ctx, run.ThreadID, 0, 100)
		if err != nil {
			return err
		}
		question := s.Task.Status.Message
		for _, item := range items {
			var answer durableNativeAnswer
			if json.Unmarshal(item.Payload, &answer) != nil || answer.RunID != run.ID || answer.QuestionID != question.ID {
				continue
			}
			if answer.TaskID != run.TaskID || answer.NativeTaskID != s.Task.ID || answer.Message == nil || answer.Message.ID != answer.MessageID || answer.Message.TaskID != s.Task.ID || answer.Message.ContextID != s.Task.ContextID {
				return fmt.Errorf("sdk: durable answer identity mismatch")
			}
			s.Task.History = append(s.Task.History, answer.Message)
			s.InputIDs = append(s.InputIDs, item.ID)
			break
		}
		if len(s.InputIDs) == 0 {
			return fmt.Errorf("sdk: no durable answer for claimed question")
		}
	}
	s.Task.Status = a2a.TaskStatus{State: a2a.TaskStateWorking}
	doc, err := nativeDocument(s)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("native:%s:%d:working", run.ID, lease.Fence().Epoch)
	receipt, err := bound.StateClient.CommitRecords(ctx, sdkstate.CommonCommit{Fence: lease.Fence(), Namespace: nativeNamespace(run.ID), Key: key, ExpectedRevision: rev, Document: &doc})
	if err != nil {
		return err
	}
	rc := &RunContext{Lease: lease, Task: Task{ThreadID: run.ThreadID, RunID: run.ID, ContextID: s.Task.ContextID, TaskID: run.TaskID, Payload: extractPayload(input.Message)}, CheckpointSaver: NewCheckpointSaver(bound.StateClient, lease.Fence(), run.ThreadID), Store: NewAHPStore(bound.StateClient, lease.Fence()), Intents: NewIntentRecorder(bound.StateClient, lease.Fence(), run.ID), Config: bound}
	output, graphErr := h.graph(lease.Context(), rc)
	if lease.IsLost() {
		return lease.LostReason()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	} // Leave an orphan; disconnect is not user cancel.
	op := &sdkstate.RunStatusOp{Status: sdkstate.RunDone, CloseReason: "graph_completed"}
	s.Task.Status = a2a.TaskStatus{State: a2a.TaskStateCompleted}
	var interrupt *InterruptError
	if isInterruptErr(graphErr, &interrupt) {
		wait := interrupt.WaitFor
		if wait == "" {
			wait = "hitl_response"
		}
		question := dataMessage(interrupt.Payload)
		if question == nil {
			question = a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewDataPart(map[string]any{}))
		}
		question.TaskID = s.Task.ID
		question.ContextID = s.Task.ContextID
		question.SetMeta(nativeWaitKindMetadata, wait)
		s.Task.History = append(s.Task.History, question)
		s.Task.Status = a2a.TaskStatus{State: a2a.TaskStateInputRequired, Message: question}
		op = &sdkstate.RunStatusOp{Status: sdkstate.RunWaiting, WaitFor: wait}
	} else if graphErr != nil {
		s.Task.Status = a2a.TaskStatus{State: a2a.TaskStateFailed, Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("agent execution failed"))}
		op = &sdkstate.RunStatusOp{Status: sdkstate.RunFailed, CloseReason: "error"}
	} else if len(output) > 0 {
		s.Task.Artifacts = []*a2a.Artifact{{ID: a2a.NewArtifactID(), Parts: a2a.ContentParts{a2a.NewDataPart(jsonToAny(output))}}}
	}
	ack := s.InputIDs
	s.InputIDs = nil
	doc, err = nativeDocument(s)
	if err != nil {
		return err
	}
	event, err := json.Marshal(s.Task)
	if err != nil {
		return err
	}
	key = fmt.Sprintf("native:%s:%d:end", run.ID, lease.Fence().Epoch)
	commit := sdkstate.CommonCommit{Namespace: nativeNamespace(run.ID), Key: key, ExpectedRevision: receipt.Revision, Document: &doc, AckInbox: ack, Stream: &sdkstate.StreamOp{Key: key, Kind: "native-a2a", Payload: event}, RunStatus: op}
	endCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err = lease.CommitAndEnd(endCtx, commit)
	return err
}

// Protocol capabilities not yet implemented must not be advertised as durable.
func (h *durableNativeHandler) ListTasks(context.Context, *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	return nil, a2a.ErrMethodNotFound
}
func (h *durableNativeHandler) CancelTask(context.Context, *a2a.CancelTaskRequest) (*a2a.Task, error) {
	return nil, a2a.ErrMethodNotFound
}
func (h *durableNativeHandler) SubscribeToTask(context.Context, *a2a.SubscribeToTaskRequest) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) { yield(nil, a2a.ErrMethodNotFound) }
}
func (h *durableNativeHandler) SendStreamingMessage(context.Context, *a2a.SendMessageRequest) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) { yield(nil, a2a.ErrMethodNotFound) }
}
func (h *durableNativeHandler) GetTaskPushConfig(context.Context, *a2a.GetTaskPushConfigRequest) (*a2a.PushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}
func (h *durableNativeHandler) ListTaskPushConfigs(context.Context, *a2a.ListTaskPushConfigRequest) (*a2a.ListTaskPushConfigResponse, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}
func (h *durableNativeHandler) CreateTaskPushConfig(context.Context, *a2a.PushConfig) (*a2a.PushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}
func (h *durableNativeHandler) DeleteTaskPushConfig(context.Context, *a2a.DeleteTaskPushConfigRequest) error {
	return a2a.ErrPushNotificationNotSupported
}
func (h *durableNativeHandler) GetExtendedAgentCard(context.Context, *a2a.GetExtendedAgentCardRequest) (*a2a.AgentCard, error) {
	return nil, a2a.ErrMethodNotFound
}
