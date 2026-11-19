package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/pkg/contracts"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

const nativeWaitKindMetadata = "ahp.wait_for"

type nativeContinuation struct {
	message    *a2a.Message
	waitFor    string
	questionID string
}

func validNativeInputID(id string) bool {
	return id != "" && len(id) <= 512 && !strings.ContainsAny(id, "\x00\r\n")
}

// StoredTask comes from the native server's taskstore, not message metadata.
// History's first message remains the run's immutable input. A later message
// answers the stored question; it is never a replacement CreateRun payload.
func nativeInvocationInput(ec *a2asrv.ExecutorContext) (json.RawMessage, *nativeContinuation, error) {
	stored := ec.StoredTask
	if stored == nil {
		return extractPayload(ec.Message), nil, nil
	}
	invalid := func() (json.RawMessage, *nativeContinuation, error) {
		return nil, nil, fmt.Errorf("%w: native continuation requires its stored task, initial message and pending question", a2a.ErrInvalidParams)
	}
	if stored.ID != ec.TaskID || stored.ContextID != ec.ContextID || stored.Status.State != a2a.TaskStateInputRequired || len(stored.History) == 0 || stored.Status.Message == nil || !validNativeInputID(stored.Status.Message.ID) {
		return invalid()
	}
	original, message := stored.History[0], ec.Message
	if original == nil || original.Role != a2a.MessageRoleUser || !validNativeInputID(original.ID) || original.ContextID != "" && original.ContextID != ec.ContextID || original.TaskID != "" && original.TaskID != ec.TaskID {
		return invalid()
	}
	if message == nil || message.Role != a2a.MessageRoleUser || !validNativeInputID(message.ID) || message.ID == original.ID || message.TaskID != ec.TaskID || message.ContextID != "" && message.ContextID != ec.ContextID {
		return invalid()
	}
	waitFor, _ := stored.Status.Message.Metadata[nativeWaitKindMetadata].(string)
	return extractPayload(original), &nativeContinuation{message: message, waitFor: waitFor, questionID: stored.Status.Message.ID}, nil
}

func (e *LangGraphExecutor) enqueueNativeContinuation(ctx context.Context, run sdkstate.Run, grant string, answer *nativeContinuation) error {
	waitFor := answer.waitFor
	switch run.Status {
	case sdkstate.RunWaiting:
		if run.WaitFor == "" || waitFor != "" && run.WaitFor != waitFor {
			return fmt.Errorf("%w: native question does not match the run's wait", a2a.ErrInvalidParams)
		}
		waitFor = run.WaitFor
	case sdkstate.RunPending:
		// An already committed answer can have woken the run before its ACK
		// reached the caller. The SDK-generated question retains the wait kind.
		if waitFor == "" {
			return fmt.Errorf("%w: native continuation has no recorded wait kind", a2a.ErrInvalidParams)
		}
	default:
		return fmt.Errorf("%w: native run is not waiting for this answer", a2a.ErrInvalidParams)
	}
	key, err := contracts.Digest([]any{"sdk.a2a.continuation", string(answer.message.TaskID), answer.message.ID})
	if err != nil {
		return err
	}
	payload, err := json.Marshal(struct {
		RunID        string          `json:"run_id"`
		TaskID       string          `json:"task_id"`
		NativeTaskID a2a.TaskID      `json:"native_task_id"`
		MessageID    string          `json:"message_id"`
		QuestionID   string          `json:"question_id"`
		Input        json.RawMessage `json:"input"`
	}{run.ID, grant, answer.message.TaskID, answer.message.ID, answer.questionID, extractPayload(answer.message)})
	if err != nil {
		return err
	}
	_, err = e.cfg.StateClient.Enqueue(ctx, run.ThreadID, key, waitFor, payload)
	return err
}
