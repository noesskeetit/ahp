package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func TestNativeContinuationRefusesInvalidBindingBeforeStateOrTaskEvents(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*a2asrv.ExecutorContext)
		err    error
	}{
		{"another-task", func(ec *a2asrv.ExecutorContext) { ec.StoredTask.ID = "other" }, a2a.ErrInvalidParams},
		{"another-context", func(ec *a2asrv.ExecutorContext) { ec.StoredTask.ContextID = "other" }, a2a.ErrInvalidParams},
		{"not-waiting", func(ec *a2asrv.ExecutorContext) { ec.StoredTask.Status.State = a2a.TaskStateWorking }, a2a.ErrInvalidParams},
		{"missing-history", func(ec *a2asrv.ExecutorContext) { ec.StoredTask.History = nil }, a2a.ErrInvalidParams},
		{"nil-original", func(ec *a2asrv.ExecutorContext) { ec.StoredTask.History[0] = nil }, a2a.ErrInvalidParams},
		{"agent-original", func(ec *a2asrv.ExecutorContext) { ec.StoredTask.History[0].Role = a2a.MessageRoleAgent }, a2a.ErrInvalidParams},
		{"missing-question", func(ec *a2asrv.ExecutorContext) { ec.StoredTask.Status.Message = nil }, a2a.ErrInvalidParams},
		{"missing-answer-id", func(ec *a2asrv.ExecutorContext) { ec.Message.ID = "" }, a2a.ErrInvalidParams},
		{"reused-initial-id", func(ec *a2asrv.ExecutorContext) { ec.Message.ID = "original" }, a2a.ErrInvalidParams},
		{"another-answer-task", func(ec *a2asrv.ExecutorContext) { ec.Message.TaskID = "other" }, a2a.ErrInvalidParams},
		{"another-answer-context", func(ec *a2asrv.ExecutorContext) { ec.Message.ContextID = "other" }, a2a.ErrInvalidParams},
		{"wrong-authority-context", func(*a2asrv.ExecutorContext) {}, a2a.ErrUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stateCalls, graphCalls := 0, 0
			cfg, _ := executorFixture(t, func(http.ResponseWriter, *http.Request) bool { stateCalls++; return false })
			executor, err := NewLangGraphExecutor(cfg, func(context.Context, *RunContext) (json.RawMessage, error) {
				graphCalls++
				return json.RawMessage(`{}`), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			original := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("original input"))
			original.ID, original.ContextID = "original", "context-new"
			answer := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer"))
			answer.ID, answer.ContextID, answer.TaskID = "answer", "context-new", "task-new"
			question := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("continue?"))
			question.SetMeta(nativeWaitKindMetadata, "hitl_response")
			ec := &a2asrv.ExecutorContext{TaskID: "task-new", ContextID: "context-new", Message: answer, StoredTask: &a2a.Task{ID: "task-new", ContextID: "context-new", History: []*a2a.Message{original}, Status: a2a.TaskStatus{State: a2a.TaskStateInputRequired, Message: question}}}
			tc.change(ec)
			ctx := t.Context()
			if tc.name == "wrong-authority-context" {
				scope := contracts.Scope{Tenant: "unit", Deployment: "agent", Subject: "owner"}
				ctx = transport.WithPrincipal(ctx, transport.Principal{Scope: scope, Claims: auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject, Task: "task-new", Context: "other"}})
			}
			var refusal error
			events := 0
			for event, err := range executor.Execute(ctx, ec) {
				if event != nil {
					events++
				}
				if err != nil {
					refusal = err
				}
			}
			if stateCalls != 0 || graphCalls != 0 || events != 0 || !errors.Is(refusal, tc.err) {
				t.Fatalf("invalid continuation changed State/native task: State=%d graph=%d events=%d refusal=%v", stateCalls, graphCalls, events, refusal)
			}
		})
	}
}
