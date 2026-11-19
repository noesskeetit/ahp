package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Invalid presentation options must not admit a run or execute an effect and
// only afterwards discover that the native response cannot be returned.
func TestDurableNativeRejectsInvalidSendBeforeAdmission(t *testing.T) {
	negative := -1
	for _, tc := range []struct {
		name   string
		config a2a.SendMessageConfig
	}{
		{"negative history", a2a.SendMessageConfig{HistoryLength: &negative}},
		{"unimplemented async", a2a.SendMessageConfig{ReturnImmediately: true}},
		{"unimplemented push", a2a.SendMessageConfig{PushConfig: &a2a.PushConfig{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			cfg, _ := executorFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
				calls.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, `{"code":"UNAVAILABLE","message":"unexpected admission"}`)
				return true
			})
			h, err := NewDurableA2AHandler(cfg, func(context.Context, *RunContext) (json.RawMessage, error) {
				t.Error("invalid send executed graph")
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			scope := contracts.Scope{Tenant: "native-unit", Deployment: "agent", Subject: "owner"}
			ctx := transport.WithPrincipal(context.Background(), transport.Principal{Scope: scope, Claims: auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject, Task: "task-new", Context: "context-new"}})
			message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("do work"))
			message.ID = "initial"
			message.ContextID = "context-new"
			_, err = h.SendMessage(ctx, &a2a.SendMessageRequest{Message: message, Config: &tc.config})
			if !errors.Is(err, a2a.ErrInvalidParams) || calls.Load() != 0 {
				t.Fatalf("invalid configuration reached admission: requests=%d err=%v", calls.Load(), err)
			}
		})
	}
}
