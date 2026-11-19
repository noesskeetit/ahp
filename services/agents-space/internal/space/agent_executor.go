package space

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// executeAgentTurn observes an independently owned A2A task. No Core request,
// agent checkpoint, model prompt or native workflow state is constructed here.
// The owner resolves the durable task handle and pinned runtime on every call.
func (c *Coordinator) executeAgentTurn(parent context.Context, turn *Turn, lease *leaseResponse, token string) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	// A lease must still be ours before dispatch. Later writes use the same fence.
	if err := c.state.Heartbeat(ctx, token, lease.Run.ID, lease.Worker, lease.Epoch, claimLeaseTTL); err != nil {
		c.agentObservationUnknown(turn, err)
		return
	}
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				beat, stop := context.WithTimeout(heartbeatCtx, 5*time.Second)
				err := c.state.Heartbeat(beat, token, lease.Run.ID, lease.Worker, lease.Epoch, claimLeaseTTL)
				stop()
				if err != nil {
					if heartbeatCtx.Err() != nil {
						return
					}
					cancel() // Losing renewal never permits another remote mutation.
					return
				}
			}
		}
	}()
	stopHeartbeat := func() { cancelHeartbeat(); <-heartbeatDone }
	defer stopHeartbeat()
	finish := func(status TurnStatus, cause string) {
		c.finishAgentTurn(ctx, turn, lease, token, status, cause, stopHeartbeat)
	}

	read := func() (*AgentTaskSnapshot, error) {
		call, stop := context.WithTimeout(ctx, 15*time.Second)
		defer stop()
		return c.agentControl.Read(call, token, turn.AgentInstallationID)
	}
	snapshot, err := read()
	if contracts.Code(err) == contracts.ErrNotFound && contracts.ReasonOf(err) == "AGENT_TASK_UNCONFIRMED" {
		// Lost start ACK replays the SAME owner-derived messageId. A general 404
		// (including a known remote task disappearing) must never enter this path.
		call, stop := context.WithTimeout(ctx, 15*time.Second)
		snapshot, err = c.agentControl.Send(call, token, turn.AgentInstallationID, turn.Input, "")
		stop()
	}
	if err != nil {
		if contracts.Code(err) == contracts.ErrUnavailable || ctx.Err() != nil {
			c.agentObservationUnknown(turn, err)
			return // Leave the durable run recoverable, with no invented verdict.
		}
		finish(TurnFailed, err.Error())
		return
	}
	if snapshot == nil || snapshot.ID == "" {
		c.agentObservationUnknown(turn, fmt.Errorf("agent owner returned no task"))
		return
	}
	nativeTask := snapshot.ID
	// An ACK may retain the old input-required snapshot until the runtime has
	// processed the answer. Observe progress rather than resending in a tight
	// loop. Across a process crash the durable decision is replayed under the
	// same owner-derived message key; native idempotency remains authoritative.
	acknowledgedQuestions := map[string]bool{}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		if snapshot == nil || snapshot.ID != nativeTask {
			finish(TurnFailed, "agent owner changed the task identity")
			return
		}
		switch snapshot.Status.State {
		case "completed":
			if err := c.projectAgentResult(ctx, token, turn, lease, snapshot); err != nil {
				c.agentObservationUnknown(turn, err)
				return
			}
			finish(TurnDone, "")
			return
		case "failed", "rejected", "canceled":
			status := TurnFailed
			if snapshot.Status.State == "canceled" {
				status = TurnCanceled
			}
			finish(status, "native agent reported "+snapshot.Status.State)
			return
		case "input-required":
			question, err := agentQuestionID(snapshot)
			if err != nil {
				c.agentObservationUnknown(turn, err)
				return
			}
			if acknowledgedQuestions[question] {
				break
			}
			next, parked, err := c.agentHumanInput(ctx, token, turn, lease, snapshot, stopHeartbeat)
			if err != nil {
				c.agentObservationUnknown(turn, err)
				return
			}
			if parked {
				return
			}
			acknowledgedQuestions[question] = true
			snapshot = next
			continue
		case "auth-required":
			finish(TurnFailed, "native agent requires a separate authorization flow")
			return
		case "submitted", "working", "unknown":
		default:
			c.agentObservationUnknown(turn, fmt.Errorf("agent owner returned an invalid task state"))
			return
		}
		select {
		case <-ctx.Done():
			c.agentObservationUnknown(turn, ctx.Err())
			return
		case <-ticker.C:
		}
		snapshot, err = read()
		if err != nil {
			if contracts.Code(err) == contracts.ErrNotFound {
				finish(TurnFailed, "known native task is unavailable: "+err.Error())
			} else {
				c.agentObservationUnknown(turn, err)
			}
			return
		}
	}
}

// A stable native question ID identifies one human gate across Coordinator
// crashes. The runtime retains its workflow; this inbox only retains the
// question and the human's decision. No execution token is persisted.
func (c *Coordinator) agentHumanInput(ctx context.Context, token string, turn *Turn, lease *leaseResponse, task *AgentTaskSnapshot, stopHeartbeat func()) (*AgentTaskSnapshot, bool, error) {
	question, err := agentQuestionID(task)
	if err != nil {
		return nil, false, err
	}
	id := contracts.OperationKey(turn.Scope, turn.ID, "space.agent-human-input", task.ID+":"+question)
	gate := HITLRequest{ID: id, TurnID: turn.ID, Kind: "agent_input", Prompt: task.Status.Message, Status: HITLPending}
	body, err := json.Marshal(gate)
	if err != nil {
		return nil, false, err
	}
	if _, err = c.state.Enqueue(ctx, token, turn.ThreadID, hitlRequestKeyPrefix+id, "hitl_request", body); err != nil {
		return nil, false, err
	}
	record, err := c.readHITLGate(ctx, token, turn.ThreadID, id)
	if err != nil {
		return nil, false, err
	}
	if record.answered {
		// The owner's continuation key is durable and resolves the original task
		// handle. A lost remote ACK repeats this message, never a new task.
		decision, err := json.Marshal(map[string]any{"approved": record.approved, "response": record.response})
		if err != nil {
			return nil, false, err
		}
		input, err := json.Marshal(map[string]any{"parts": []any{map[string]any{"kind": "text", "text": string(decision)}}})
		if err != nil {
			return nil, false, err
		}
		call, stop := context.WithTimeout(ctx, 15*time.Second)
		defer stop()
		next, err := c.agentControl.Send(call, token, turn.AgentInstallationID, input, "hitl:"+id)
		return next, false, err
	}
	if err := c.appendAgentProjection(ctx, token, turn, lease, turn.ID+":agent:human:"+id, "hitl_request", body); err != nil {
		return nil, false, err
	}
	// Step/turn buffering must not hide the question after this observer exits.
	if _, err := c.state.FlushStream(ctx, token, lease.Run.ID, lease.Worker, lease.Epoch); err != nil {
		return nil, false, err
	}
	// Suspend releases the lease before its reply arrives. No heartbeat may
	// race that commit and cancel the observer's still pending transition.
	stopHeartbeat()
	if err := c.state.Suspend(ctx, token, lease.Run.ID, lease.Worker, lease.Epoch, hitlWaitFor, nil); err != nil {
		return nil, false, err
	}
	// An answer may have committed between reading the gate and parking. The
	// existing durable re-signal closes that gap without replacing the decision.
	if _, err := c.releaseRunIfGateAnswered(ctx, token, lease.Run.ID, turn.ThreadID, id); err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	if !turn.Status.IsTerminal() {
		turn.Status, turn.UpdatedAt = TurnAwaiting, time.Now().UTC()
	}
	c.mu.Unlock()
	return nil, true, nil
}

func agentQuestionID(task *AgentTaskSnapshot) (string, error) {
	var message struct {
		MessageID string `json:"messageId"`
	}
	if json.Unmarshal(task.Status.Message, &message) != nil || message.MessageID == "" || len(message.MessageID) > 512 {
		return "", fmt.Errorf("native input-required needs a stable message identity")
	}
	return message.MessageID, nil
}

func agentProjectionKey(turn, kind, id string) string {
	sum := sha256.Sum256([]byte(id))
	return turn + ":agent:" + kind + ":" + hex.EncodeToString(sum[:])
}

func (c *Coordinator) appendAgentProjection(ctx context.Context, token string, turn *Turn, lease *leaseResponse, key, kind string, payload json.RawMessage) error {
	call, stop := context.WithTimeout(ctx, c.streamAppendBudget())
	defer stop()
	_, err := c.state.AppendStream(call, token, turn.ThreadID, lease.Run.ID, lease.Worker, lease.Epoch, key, kind, payload)
	return err
}

// Results are user-visible A2A output, not the runtime's resumable state. Stable
// artifact/message identities make a takeover after a partial append idempotent.
func (c *Coordinator) projectAgentResult(ctx context.Context, token string, turn *Turn, lease *leaseResponse, task *AgentTaskSnapshot) error {
	outputs := append([]json.RawMessage(nil), task.Artifacts...)
	if len(task.Status.Message) != 0 && string(task.Status.Message) != "null" {
		outputs = append(outputs, task.Status.Message)
	}
	for _, raw := range outputs {
		var output struct {
			ArtifactID string            `json:"artifactId"`
			MessageID  string            `json:"messageId"`
			Parts      []json.RawMessage `json:"parts"`
		}
		if err := json.Unmarshal(raw, &output); err != nil {
			return fmt.Errorf("invalid native output: %w", err)
		}
		id := output.ArtifactID
		if id == "" {
			id = output.MessageID
		}
		if id == "" {
			return fmt.Errorf("native output has no stable identity")
		}
		var text strings.Builder
		for _, rawPart := range output.Parts {
			var legacy struct {
				Kind string `json:"kind"`
			}
			if err := json.Unmarshal(rawPart, &legacy); err != nil {
				return fmt.Errorf("invalid native output part: %w", err)
			}
			if legacy.Kind != "" && legacy.Kind != "text" {
				// Preserve older non-text artifacts without inventing text for them.
				continue
			}
			var part a2a.Part
			if err := json.Unmarshal(rawPart, &part); err != nil {
				return fmt.Errorf("invalid native output part: %w", err)
			}
			text.WriteString(part.Text())
		}
		content, err := json.Marshal(map[string]any{"text": text.String(), "a2a": json.RawMessage(raw)})
		if err != nil {
			return err
		}
		frame, err := json.Marshal(StepResult{StepType: StepModel, Content: content})
		if err != nil {
			return err
		}
		if err := c.appendAgentProjection(ctx, token, turn, lease, agentProjectionKey(turn.ID, "output", id), string(StepModel), frame); err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) finishAgentTurn(ctx context.Context, turn *Turn, lease *leaseResponse, token string, status TurnStatus, cause string, stopHeartbeat func()) {
	if status == TurnFailed {
		content, _ := json.Marshal(map[string]string{"error": cause})
		frame, _ := json.Marshal(StepResult{StepType: StepError, Content: content})
		if err := c.appendAgentProjection(ctx, token, turn, lease, turn.ID+":agent:error", string(StepError), frame); err != nil {
			c.agentObservationUnknown(turn, err)
			return
		}
	}
	terminal, _ := json.Marshal(map[string]TurnStatus{"status": status})
	if err := c.appendDurableTerminal(ctx, token, turn.ThreadID, lease.Run.ID, lease.Worker, lease.Epoch, turn.ID+":agent:terminal", "status", terminal); err != nil {
		c.agentObservationUnknown(turn, err)
		return
	}
	flush, stop := context.WithTimeout(ctx, c.streamAppendBudget())
	_, err := c.state.FlushStream(flush, token, lease.Run.ID, lease.Worker, lease.Epoch)
	stop()
	if err != nil {
		c.agentObservationUnknown(turn, err)
		return
	}
	closeCtx, closeCancel := context.WithTimeout(ctx, 10*time.Second)
	defer closeCancel()
	reason := runCloseReason(string(status), nil, nil, cause, status == TurnFailed, "", 0, 0)
	stopHeartbeat()
	if err := c.state.ReleaseWithReason(closeCtx, token, lease.Run.ID, lease.Worker, lease.Epoch, string(status), reason); err != nil {
		// A release ACK may be lost after commit. Read the same authoritative row;
		// a different/unknown result never becomes a claimed completion.
		run, readErr := c.state.GetRun(closeCtx, token, lease.Run.ID)
		if readErr != nil || run == nil || run.Status != string(status) {
			c.agentObservationUnknown(turn, err)
			return
		}
	}
	c.mu.Lock()
	if !turn.Status.IsTerminal() {
		turn.Status, turn.FailureReason, turn.UpdatedAt = status, "", time.Now().UTC()
		if status == TurnFailed {
			turn.FailureReason = cause
		}
	}
	c.mu.Unlock()
}

func (c *Coordinator) agentObservationUnknown(turn *Turn, err error) {
	c.mu.Lock()
	if !turn.Status.IsTerminal() {
		turn.Status, turn.UpdatedAt = TurnUnknown, time.Now().UTC()
	}
	c.mu.Unlock()
	slog.Warn("space: native task observation unconfirmed; durable run remains recoverable", "turn_id", turn.ID, "code", contracts.Code(err))
}
