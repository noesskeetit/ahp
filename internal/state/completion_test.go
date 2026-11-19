package state

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func TestCompletionOutboxSharesTerminalTransactionAndPreservesThreadOrder(t *testing.T) {
	pool, _ := isolatedPool(t)
	s := New(pool, WithRunCompletionOutbox())
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := eventScope("completion-" + contracts.NewID())
	thread, err := s.CreateThread(ctx, scope, "conversation", "thread")
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for n := 0; n < 2; n++ {
		task := "task-" + contracts.NewID()
		run, err := s.CreateRun(ctx, scope, thread.ID, task, "run-"+task, json.RawMessage(`{"type":"turn","input":"useful decision"}`))
		if err != nil {
			t.Fatal(err)
		}
		lease, err := s.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "worker", run.ID, time.Minute)
		if err != nil || lease == nil {
			t.Fatal(lease, err)
		}
		// Missing authority fails after UPDATE and must roll the terminal
		// transition back with the unpublished event.
		if err := s.Release(ctx, scope, run.ID, lease.Worker, lease.Epoch, RunDone); err == nil {
			t.Fatal("unsigned completion accepted")
		}
		got, err := s.GetRun(ctx, scope, run.ID)
		if err != nil || got.Status != RunRunning {
			t.Fatal("terminal transition escaped rollback", got, err)
		}
		claims := auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject, Task: task, Root: task, Context: thread.ContextID, Grant: "grant"}
		signed := transport.WithPrincipal(ctx, transport.Principal{Scope: scope, Claims: claims})
		if err := s.Release(signed, scope, run.ID, lease.Worker, lease.Epoch, RunDone); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, run.ID)
	}
	batch, err := s.ClaimRunCompletions(ctx, 20)
	if err != nil || len(batch) != 1 || batch[0].ID != ids[0] {
		t.Fatal("completion overtook predecessor", batch, err)
	}
	first := batch[0]
	if first.Authority.Grant != "grant" || first.CreatedAt.IsZero() {
		t.Fatal(first)
	}
	if err := s.FinishRunCompletion(ctx, first.ID, "stale-key", "", false); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatal("stale ack accepted", err)
	}
	if err := s.FinishRunCompletion(ctx, first.ID, first.LeaseKey, "", false); err != nil {
		t.Fatal(err)
	}
	batch, err = s.ClaimRunCompletions(ctx, 20)
	if err != nil || len(batch) != 1 || batch[0].ID != ids[1] {
		t.Fatal(batch, err)
	}
}
