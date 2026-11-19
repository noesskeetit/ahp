package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

// LangGraphExecutor implements a2asrv.AgentExecutor backed by the AHP State API.
//
// PDF rev 3.2, section 4.2: the A2A adapter (AHPLangGraphApp) accepts tasks from
// the gateway; contextId becomes thread_id, taskId becomes a run. The executor
// claims a lease, runs the graph, and emits A2A events (submitted, working,
// artifact, completed/failed/input-required).
//
// This is the "AHPLangGraphApp" from the PDF: a real A2A protocol implementation
// that plugs into sdk.NewA2AHandler(executor, options...), which binds the
// upstream taskstore owner before native history is read or changed.
type LangGraphExecutor struct {
	cfg   *Config
	graph GraphFunc

	mu      sync.Mutex
	running map[a2a.TaskID]*executorAttempt
}

// canceled is published by closing done, only after State accepts RunCanceled.
type executorAttempt struct {
	cancel   context.CancelFunc
	done     chan struct{}
	canceled bool
}

var _ a2asrv.AgentExecutor = (*LangGraphExecutor)(nil)

// NewLangGraphExecutor creates the A2A executor for use with a2asrv.NewHandler.
func NewLangGraphExecutor(cfg *Config, graph GraphFunc) (*LangGraphExecutor, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if graph == nil {
		return nil, fmt.Errorf("sdk: graph function must not be nil")
	}
	return &LangGraphExecutor{
		cfg:     cfg,
		graph:   graph,
		running: make(map[a2a.TaskID]*executorAttempt),
	}, nil
}

// Execute implements a2asrv.AgentExecutor.
func (e *LangGraphExecutor) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		taskID := execCtx.TaskID
		ctxID := execCtx.ContextID
		// A2A task ids are opaque: the upstream server generates one for a
		// first message. State records the task from the verified AHP grant.
		// Only server-installed identity may override the legacy coincident-id
		// form; message metadata can never supply this authority.
		grantTask := string(taskID)
		if principal, ok := transport.FromContext(ctx); ok {
			if principal.Claims.Kind != auth.KindTask || principal.Claims.Task == "" || principal.Claims.Context == "" || principal.Scope != principal.Claims.Scope() || principal.Scope.Validate() != nil || principal.Claims.Context != ctxID {
				yield(nil, fmt.Errorf("%w: request does not carry this conversation's verified AHP task authority", a2a.ErrUnauthorized))
				return
			}
			grantTask = principal.Claims.Task
		}
		payload, continuation, err := nativeInvocationInput(execCtx)
		if err != nil {
			yield(nil, err)
			return
		}
		// Invalid continuations must not replace the stored native task with a
		// failed status. They are request refusals, before any execution event.
		refuse := func(err error) {
			if continuation != nil {
				yield(nil, err)
			} else {
				emitFail(yield, execCtx, err.Error())
			}
		}

		thread, err := e.cfg.StateClient.CreateThread(ctx, ctxID, "ctx:"+ctxID)
		if err != nil {
			refuse(fmt.Errorf("CreateThread: %w", err))
			return
		}
		// A shared host may be misconfigured with a fixed State credential.
		// Matching task/context strings cannot authorize execution in another
		// subject's scope. Check before recording input or taking a lease.
		if principal, ok := transport.FromContext(ctx); ok && thread.Scope != principal.Scope {
			yield(nil, fmt.Errorf("%w: State credential does not belong to the verified request", a2a.ErrUnauthorized))
			return
		}

		run, err := e.cfg.StateClient.CreateRun(ctx, thread.ID, "task:"+string(taskID), payload)
		if err != nil {
			refuse(fmt.Errorf("CreateRun: %w", err))
			return
		}
		// The run must be THIS message's run. A task's identity and input are what
		// State recorded on the run from the verified credential -- not what the
		// caller's taskId string happens to say -- so both disagreements are
		// refused here, before any lease is taken and before anything executes:
		//   - another task on this run: one credential reused across tasks;
		//   - another payload: the task already existed and this message brings a
		//     different input under the same taskId, which would run a new payload
		//     under an old run and its fence (PDF rev 3.2 sec.4.2).
		if run.TaskID != grantTask {
			refuse(fmt.Errorf("task identity mismatch: run %s is recorded under task %q, not %q; present the message's own task credential", run.ID, run.TaskID, grantTask))
			return
		}
		if run.ThreadID != thread.ID {
			refuse(fmt.Errorf("run %s belongs to thread %s, not %s", run.ID, run.ThreadID, thread.ID))
			return
		}
		if !samePayload(run.Payload, payload) {
			refuse(fmt.Errorf("task %s already exists carrying a different payload; a task's input is immutable", taskID))
			return
		}
		if continuation != nil {
			if err := e.enqueueNativeContinuation(ctx, run, grantTask, continuation); err != nil {
				refuse(err)
				return
			}
		} else {
			submitted := a2a.NewSubmittedTask(execCtx, execCtx.Message)
			if execCtx.Message == nil {
				submitted.History = nil // upstream gob cloning rejects nil history entries
			}
			if !yield(submitted, nil) {
				return
			}
		}

		graphCtx, cancel := context.WithCancel(ctx)
		attempt := &executorAttempt{cancel: cancel, done: make(chan struct{})}
		e.mu.Lock()
		if e.running[taskID] != nil {
			e.mu.Unlock()
			cancel()
			emitFail(yield, execCtx, "task already has an active execution")
			return
		}
		e.running[taskID] = attempt
		e.mu.Unlock()
		defer func() {
			cancel()
			e.mu.Lock()
			delete(e.running, taskID)
			e.mu.Unlock()
			close(attempt.done)
		}()
		var lease *ActiveLease
		for {
			lease, err = claimAndHold(graphCtx, e.cfg, run.ID)
			if err != nil {
				emitFail(yield, execCtx, err.Error())
				return
			}
			if lease != nil {
				break
			}
			// Another run owns this thread's FIFO head. Never take its lease.
			timer := time.NewTimer(50 * time.Millisecond)
			select {
			case <-graphCtx.Done():
				timer.Stop()
				emitFail(yield, execCtx, graphCtx.Err().Error())
				return
			case <-timer.C:
			}
		}
		claim := lease.ClaimResult()
		if claim.Lease.Thread.ID != thread.ID {
			lease.stop()
			<-lease.done
			lease.cancel()
			emitFail(yield, execCtx, "ClaimRun returned a foreign thread")
			return
		}
		// The graph is about to run what the CLAIM carries, so the claim must agree
		// with the run this message created. A claim that hands back another
		// task's run or another payload would execute a new message's intent under
		// an old run and its fence; the lease is dropped unexecuted instead.
		if claim.Lease.Run.ID != run.ID || claim.Lease.Run.TaskID != run.TaskID || !samePayload(claim.Lease.Run.Payload, run.Payload) {
			lease.stop()
			<-lease.done
			lease.cancel()
			emitFail(yield, execCtx, fmt.Sprintf("claim returned run %s/task %q instead of the run this message created (%s/task %q)", claim.Lease.Run.ID, claim.Lease.Run.TaskID, run.ID, run.TaskID))
			return
		}
		fence := lease.Fence()
		bound := lease.cfg
		runCtx := &RunContext{
			Lease:           lease,
			CheckpointSaver: NewCheckpointSaver(bound.StateClient, fence, lease.ThreadID()),
			Store:           NewAHPStore(bound.StateClient, fence),
			Intents:         NewIntentRecorder(bound.StateClient, fence, fence.RunID),
			Config:          bound,
			Task: Task{ThreadID: lease.ThreadID(), RunID: lease.RunID(),
				ContextID: claim.Lease.Thread.ContextID, TaskID: claim.Lease.Run.TaskID, Payload: claim.Lease.Run.Payload},
		}
		// Every close this executor makes names why (DEF-RELEASE-REASON-1): a canceled or
		// failed run cannot reach its end through the State door unexplained. done stays
		// unstated, which is what its contract allows.
		closeLease := func(status, closeReason string) error {
			closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer closeCancel()
			return lease.ReleaseWithReason(closeCtx, status, closeReason)
		}

		if !yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateWorking, nil), nil) {
			attempt.canceled = closeLease(sdkstate.RunCanceled, "canceled") == nil
			return
		}

		output, graphErr := e.graph(lease.Context(), runCtx)
		if lease.IsLost() {
			graphErr = lease.LostReason()
		}
		if graphCtx.Err() != nil && !lease.IsLost() {
			if closeErr := closeLease(sdkstate.RunCanceled, "canceled"); closeErr != nil {
				emitFail(yield, execCtx, closeErr.Error())
				return
			}
			attempt.canceled = true
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCanceled, nil), nil)
			return
		}

		if graphErr != nil {
			var ie *InterruptError
			if isInterruptErr(graphErr, &ie) {
				waitFor := ie.WaitFor
				if waitFor == "" {
					waitFor = "hitl_response"
				}
				if suspendErr := lease.Suspend(ctx, waitFor, nil); suspendErr != nil {
					emitFail(yield, execCtx, suspendErr.Error())
					return
				}
				msg := dataMessage(ie.Payload)
				if msg == nil {
					msg = a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewDataPart(map[string]any{}))
				}
				msg.SetMeta(nativeWaitKindMetadata, waitFor)
				if !yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateInputRequired, msg), nil) {
					return
				}
				return
			}
			if closeErr := closeLease(sdkstate.RunFailed, "error"); closeErr != nil {
				graphErr = fmt.Errorf("%w; durable close: %v", graphErr, closeErr)
			}
			emitFail(yield, execCtx, graphErr.Error())
			return
		}

		if closeErr := closeLease(sdkstate.RunDone, ""); closeErr != nil {
			emitFail(yield, execCtx, closeErr.Error())
			return
		}

		if len(output) > 0 {
			part := a2a.NewDataPart(jsonToAny(output))
			if !yield(a2a.NewArtifactEvent(execCtx, part), nil) {
				return
			}
		}
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted, nil), nil)
	}
}

// Cancel implements a2asrv.AgentExecutor.
func (e *LangGraphExecutor) Cancel(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		e.mu.Lock()
		attempt := e.running[execCtx.TaskID]
		e.mu.Unlock()
		if attempt == nil {
			yield(nil, fmt.Errorf("sdk: no active execution whose cancellation can be confirmed"))
			return
		}
		attempt.cancel()
		waitCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		select {
		case <-attempt.done:
			if !attempt.canceled {
				yield(nil, fmt.Errorf("sdk: execution ended without confirmed durable cancellation"))
				return
			}
		case <-waitCtx.Done():
			yield(nil, fmt.Errorf("sdk: cancellation not yet confirmed: %w", waitCtx.Err()))
			return
		}
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCanceled, nil), nil)
	}
}

func emitFail(yield func(a2a.Event, error) bool, execCtx *a2asrv.ExecutorContext, msg string) {
	m := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("error: "+msg))
	yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateFailed, m), nil)
}

func extractPayload(msg *a2a.Message) json.RawMessage {
	if msg == nil {
		return json.RawMessage("{}")
	}
	for _, part := range msg.Parts {
		if d, ok := part.Content.(a2a.Data); ok {
			data, _ := json.Marshal(d.Value)
			return data
		}
	}
	for _, part := range msg.Parts {
		if t, ok := part.Content.(a2a.Text); ok {
			data, _ := json.Marshal(map[string]string{"text": string(t)})
			return data
		}
	}
	return json.RawMessage("{}")
}

func jsonToAny(raw json.RawMessage) any {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	return v
}

func dataMessage(payload json.RawMessage) *a2a.Message {
	if len(payload) == 0 {
		return nil
	}
	part := a2a.NewDataPart(jsonToAny(payload))
	return a2a.NewMessage(a2a.MessageRoleAgent, part)
}

func isInterruptErr(err error, target **InterruptError) bool {
	for err != nil {
		if ie, ok := err.(*InterruptError); ok {
			*target = ie
			return true
		}
		type unwrapper interface{ Unwrap() error }
		if u, ok := err.(unwrapper); ok {
			err = u.Unwrap()
		} else {
			return false
		}
	}
	return false
}
