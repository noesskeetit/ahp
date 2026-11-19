package model

// D2 budget/ledger consequences of a pre-send refusal, proven against the real
// budget + operations ledger. Reuses the model env from service_test.go (skips
// when AHP_TEST_DATABASE_URL is unset, like every other accounting leg).

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

var errEofSentinel = errors.New("connection reset after headers")

func TestD2PreSendRefusalReturnsReserveNoUnknown(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	inf := &fakeInference{model: "qwen-test"}
	inf.newStr = func(ChatRequest) (Stream, error) {
		return nil, &preSendRefusalError{err: &netpolicy.Error{Sentinel: netpolicy.ErrSchemeNotAllowed, Message: "http to an unregistered host"}}
	}
	svc := env.service(inf)
	root, _ := env.budget.Root(ctx, env.scope, "task1", "root-"+env.u, 100)
	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"),
		Model: "qwen-test", BudgetID: root.ID, ReserveKey: "res-" + env.u, ReserveAmount: 15}
	g, _ := svc.Submit(ctx, args)
	done, err := svc.Execute(ctx, env.scope, g.ID)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if done.Status != GenFailed {
		t.Fatalf("status=%s, want FAILED", done.Status)
	}
	if !strings.Contains(done.Error, ReasonModelEgressRefusedBeforeSend) {
		t.Fatalf("generations.error must carry the new cause class %q, got %q", ReasonModelEgressRefusedBeforeSend, done.Error)
	}
	if strings.Contains(done.Error, "ambiguous") {
		t.Fatalf("pre-send refusal must not read as ambiguous: %q", done.Error)
	}
	// Ledger records a definite FAILED, never SIDE_EFFECT_UNKNOWN.
	op, err := env.ledger.Get(ctx, env.scope, g.EffectKey)
	if err != nil {
		t.Fatalf("ledger get: %v", err)
	}
	if op.Status == operations.StatusSideEffectUnknown {
		t.Fatalf("pre-send refusal must not journal SIDE_EFFECT_UNKNOWN")
	}
	if op.Status != operations.StatusFailed {
		t.Fatalf("ledger status=%s, want FAILED", op.Status)
	}
	// Nothing was sent, so nothing is charged: the reservation is returned.
	b, _ := env.budget.Get(ctx, env.scope, root.ID)
	if b.Spent != 0 || b.Reserved != 0 {
		t.Fatalf("pre-send refusal must return the reserve: spent=%d reserved=%d, want 0/0", b.Spent, b.Reserved)
	}
}

func TestD2AmbiguousStillBurnsNoRegression(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	inf := &fakeInference{model: "qwen-test"}
	inf.newStr = func(ChatRequest) (Stream, error) {
		return &fakeStream{deltas: []Delta{{Text: "partial"}}, usage: Usage{TotalTokens: 2}, errAt: 1,
			err: &transportError{err: errEofSentinel}}, nil
	}
	svc := env.service(inf)
	root, _ := env.budget.Root(ctx, env.scope, "task1", "root-"+env.u, 100)
	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"),
		Model: "qwen-test", BudgetID: root.ID, ReserveKey: "res-" + env.u, ReserveAmount: 15}
	g, _ := svc.Submit(ctx, args)
	done, err := svc.Execute(ctx, env.scope, g.ID)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if done.Status != GenFailed || !strings.Contains(done.Error, "ambiguous") {
		t.Fatalf("post-send transport must stay ambiguous: status=%s error=%q", done.Status, done.Error)
	}
	op, _ := env.ledger.Get(ctx, env.scope, g.EffectKey)
	if op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("post-send transport must still journal SIDE_EFFECT_UNKNOWN, got %s", op.Status)
	}
	b, _ := env.budget.Get(ctx, env.scope, root.ID)
	if b.Spent != 15 {
		t.Fatalf("ambiguous must still charge the full reservation: spent=%d, want 15", b.Spent)
	}
}
