package model

import (
	"context"
	"testing"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestActualUsageExceedingReservationIsNeverLost(t *testing.T) {
	ctx := context.Background()
	e := newModelEnv(t)
	root, err := e.budget.Root(ctx, e.scope, "root", "root", 100)
	if err != nil {
		t.Fatal(err)
	}
	child, err := e.budget.Allocate(ctx, e.scope, root.ID, e.scope, "child", "child", 40)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.budget.Reserve(ctx, e.scope, child.ID, "call", 40); err != nil {
		t.Fatal(err)
	}
	if _, err := e.budget.Settle(ctx, e.scope, child.ID, "call", 160); err != nil {
		t.Fatalf("real provider usage must be recorded: %v", err)
	}
	if _, err := e.budget.Settle(ctx, e.scope, child.ID, "call", 160); err != nil {
		t.Fatal(err)
	}
	snap, err := e.budget.TreeSnapshot(ctx, e.scope, root.ID)
	if err != nil || snap.Totals.Spent != 160 {
		t.Fatalf("actual tree cost=%+v err=%v", snap.Totals, err)
	}
	if _, err := e.budget.Reserve(ctx, e.scope, root.ID, "after-overrun", 1); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("new work after descendant overrun: %v", err)
	}
	if err := e.budget.Close(ctx, e.scope, child.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.budget.Close(ctx, e.scope, child.ID); err != nil {
		t.Fatal(err)
	}
	got, err := e.budget.Get(ctx, e.scope, root.ID)
	if err != nil || got.Spent != 160 || got.Reserved != 0 {
		t.Fatalf("propagated actual=%+v err=%v", got, err)
	}
	if _, err := e.budget.Reserve(ctx, e.scope, root.ID, "after-close", 1); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("overdrawn parent admitted work: %v", err)
	}
}

func TestModelServiceChargesProviderOverrun(t *testing.T) {
	ctx := context.Background()
	e := newModelEnv(t)
	root, err := e.budget.Root(ctx, e.scope, "root", "root", 100)
	if err != nil {
		t.Fatal(err)
	}
	inf := &fakeInference{model: "test"}
	inf.streamText([]string{"answer"}, Usage{PromptTokens: 90, CompletionTokens: 70, TotalTokens: 160})
	svc := e.service(inf)
	g, err := svc.Submit(ctx, SubmitArgs{Scope: e.scope, Task: "root", Key: "call", Request: chatReq("hi"), BudgetID: root.ID, ReserveKey: "call", ReserveAmount: 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Execute(ctx, e.scope, g.ID); err != nil {
		t.Fatal(err)
	}
	got, err := e.budget.Get(ctx, e.scope, root.ID)
	if err != nil || got.Spent != 160 {
		t.Fatalf("provider billed 160, budget=%+v err=%v", got, err)
	}
}

func TestSettlementMissingReservationDoesNotRefundQuota(t *testing.T) {
	ctx := context.Background()
	e, q, provider, _ := quotaCopies(t)
	if err := q.Set(ctx, ProviderQuota{Provider: provider, Tenant: e.scope.Tenant, RPM: 10, TPM: 1000, WindowSeconds: 3600}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Admit(ctx, provider, e.scope.Tenant, "deleted", 1, 100); err != nil {
		t.Fatal(err)
	}
	svc := e.service(&fakeInference{model: "test"}).WithQuota(q, provider)
	err := svc.settle(ctx, e.scope, Generation{Key: "deleted", BudgetID: "removed", ReserveKey: "removed", Reserved: 100}, 0)
	if err == nil {
		t.Error("missing reservation silently accepted")
	}
	if w := copyWindow(t, q, provider, e.scope.Tenant); w.Tokens != 100 {
		t.Fatalf("deleted hold was refunded: %+v", w)
	}
}

func TestPartialProviderFailureKeepsConsumedUsage(t *testing.T) {
	ctx := context.Background()
	e := newModelEnv(t)
	root, err := e.budget.Root(ctx, e.scope, "root", "root", 100)
	if err != nil {
		t.Fatal(err)
	}
	inf := &fakeInference{model: "test", newStr: func(ChatRequest) (Stream, error) {
		return &fakeStream{errAt: 1, err: contracts.NewError(contracts.ErrInvalid, "provider rejected continuation"), deltas: []Delta{{Text: "partial"}}, usage: Usage{TotalTokens: 160}}, nil
	}}
	svc := e.service(inf)
	g, err := svc.Submit(ctx, SubmitArgs{Scope: e.scope, Task: "root", Key: "partial", Request: chatReq("hi"), BudgetID: root.ID, ReserveKey: "partial", ReserveAmount: 100})
	if err != nil {
		t.Fatal(err)
	}
	final, err := svc.Execute(ctx, e.scope, g.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := e.budget.Get(ctx, e.scope, root.ID)
	if err != nil || got.Spent != 160 || final.Usage.TotalTokens != 160 {
		t.Fatalf("partial usage lost: budget=%+v final=%+v err=%v", got, final, err)
	}
}

type canceledUsageStream struct{ cancel context.CancelFunc }

func (s *canceledUsageStream) Next(context.Context) (Delta, error) {
	s.cancel()
	return Delta{}, context.Canceled
}
func (s *canceledUsageStream) Usage() Usage { return Usage{TotalTokens: 160} }
func (s *canceledUsageStream) Close() error { return nil }

func TestCallerDisconnectStillSettlesObservedProviderUsage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := newModelEnv(t)
	root, err := e.budget.Root(ctx, e.scope, "root", "root", 100)
	if err != nil {
		t.Fatal(err)
	}
	inf := &fakeInference{model: "test", newStr: func(ChatRequest) (Stream, error) { return &canceledUsageStream{cancel}, nil }}
	svc := e.service(inf)
	g, err := svc.Submit(ctx, SubmitArgs{Scope: e.scope, Task: "root", Key: "canceled", Request: chatReq("hi"), BudgetID: root.ID, ReserveKey: "canceled", ReserveAmount: 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = svc.Execute(ctx, e.scope, g.ID); err != nil {
		t.Fatalf("disconnect lost settlement: %v", err)
	}
	final, err := e.gens.Get(context.Background(), e.scope, g.ID)
	if err != nil || final.Status != GenCanceled || final.Usage.TotalTokens != 160 {
		t.Fatalf("canceled usage lost: status=%s usage=%+v err=%v", final.Status, final.Usage, err)
	}
	got, err := e.budget.Get(context.Background(), e.scope, root.ID)
	if err != nil || got.Spent != 160 || got.Reserved != 0 {
		t.Fatalf("canceled charge=%+v err=%v", got, err)
	}
}

type canceledUnknownStream struct {
	cancel  context.CancelFunc
	emitted bool
}

func (s *canceledUnknownStream) Next(context.Context) (Delta, error) {
	if !s.emitted {
		s.emitted = true
		return Delta{Text: "partial"}, nil
	}
	s.cancel()
	return Delta{}, context.Canceled
}
func (s *canceledUnknownStream) Usage() Usage { return Usage{} }
func (s *canceledUnknownStream) Close() error { return nil }

func TestCallerDisconnectWithoutUsageKeepsUnknownBillingHold(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := newModelEnv(t)
	root, err := e.budget.Root(ctx, e.scope, "root", "root", 100)
	if err != nil {
		t.Fatal(err)
	}
	inf := &fakeInference{model: "test", newStr: func(ChatRequest) (Stream, error) { return &canceledUnknownStream{cancel: cancel}, nil }}
	svc := e.service(inf)
	g, err := svc.Submit(ctx, SubmitArgs{Scope: e.scope, Task: "root", Key: "unknown-cancel", Request: chatReq("hi"), BudgetID: root.ID, ReserveKey: "unknown-cancel", ReserveAmount: 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = svc.Execute(ctx, e.scope, g.ID); err != nil {
		t.Fatal(err)
	}
	got, err := e.budget.Get(context.Background(), e.scope, root.ID)
	if err != nil || got.Reserved != 100 || got.Spent != 0 {
		t.Fatalf("unknown provider cost was refunded: %+v err=%v", got, err)
	}
	op, err := e.ledger.Get(context.Background(), e.scope, g.EffectKey)
	if err != nil || op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("unknown cancellation billing status=%s err=%v", op.Status, err)
	}
	if _, err = e.budget.Reserve(context.Background(), e.scope, root.ID, "retry", 1); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("cancel created free capacity: %v", err)
	}
}

func TestCancelBeforeDispatchReturnsUnusedReservation(t *testing.T) {
	ctx := context.Background()
	e := newModelEnv(t)
	inf := &fakeInference{model: "test"}
	svc := e.service(inf)
	root, err := e.budget.Root(ctx, e.scope, "root", "root", 100)
	if err != nil {
		t.Fatal(err)
	}
	g, err := svc.Submit(ctx, SubmitArgs{Scope: e.scope, Task: "root", Key: "pending", Request: chatReq("hi"), BudgetID: root.ID, ReserveKey: "pending", ReserveAmount: 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = svc.Cancel(ctx, e.scope, g.ID); err != nil {
		t.Fatal(err)
	}
	got, err := e.budget.Get(ctx, e.scope, root.ID)
	if err != nil || got.Reserved != 0 || got.Spent != 0 {
		t.Fatalf("unsent work retained capacity: %+v err=%v", got, err)
	}
	if _, err = svc.Execute(ctx, e.scope, g.ID); err != nil {
		t.Fatal(err)
	}
	if inf.count() != 0 {
		t.Fatal("canceled unsent generation reached provider")
	}
}

type canceledAtEOFStream struct {
	cancel        context.CancelFunc
	emitted, done bool
}

func (s *canceledAtEOFStream) Next(context.Context) (Delta, error) {
	if !s.emitted {
		s.emitted = true
		return Delta{Text: "answer"}, nil
	}
	s.done = true
	s.cancel()
	return Delta{}, ErrStreamDone
}
func (s *canceledAtEOFStream) Usage() Usage {
	if s.done {
		return Usage{TotalTokens: 160}
	}
	return Usage{}
}
func (s *canceledAtEOFStream) Close() error { return nil }

func TestDisconnectWithFinalUsageFrameStillPersistsActual(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := newModelEnv(t)
	root, err := e.budget.Root(ctx, e.scope, "root", "root", 100)
	if err != nil {
		t.Fatal(err)
	}
	inf := &fakeInference{model: "test", newStr: func(ChatRequest) (Stream, error) { return &canceledAtEOFStream{cancel: cancel}, nil }}
	svc := e.service(inf)
	g, err := svc.Submit(ctx, SubmitArgs{Scope: e.scope, Task: "root", Key: "eof", Request: chatReq("hi"), BudgetID: root.ID, ReserveKey: "eof", ReserveAmount: 100})
	if err != nil {
		t.Fatal(err)
	}
	final, err := svc.Execute(ctx, e.scope, g.ID)
	if err != nil || final.Usage.TotalTokens != 160 {
		t.Fatalf("EOF usage lost: %+v err=%v", final.Usage, err)
	}
	got, err := e.budget.Get(context.Background(), e.scope, root.ID)
	if err != nil || got.Spent != 160 {
		t.Fatalf("EOF charge=%+v err=%v", got, err)
	}
}
