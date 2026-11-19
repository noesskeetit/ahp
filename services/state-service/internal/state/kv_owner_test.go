package state

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Regression: a deleted session must not retain its value in an orphan receipt.
func TestRev7KVReceiptErasure(t *testing.T) {
	pool, note := isolatedPool(t)
	if isolateName == "" {
		t.Fatal("fresh isolated test DB required; shared fallback refused")
	}
	t.Logf("own test substrate: %s", note)
	s := New(pool)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: "rev7-" + contracts.NewID(), Deployment: "adk-shared", Subject: "alice"}
	th, err := s.CreateThread(ctx, scope, "session-a", contracts.NewID())
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.CreateRun(ctx, scope, th.ID, "task", contracts.NewID(), json.RawMessage("{}"))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "worker", run.ID, time.Minute)
	if err != nil || lease == nil {
		t.Fatal("exact own lease required", err)
	}
	fence := Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
	if _, err := s.PutKV(ctx, scope, fence, []string{"private-session"}, "state", "operation-1", 0, json.RawMessage("{\"private\":\"session-value\"}")); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteThread(ctx, scope, th.ID); err != nil {
		t.Fatal(err)
	}
	var receipts int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM ahp_state.kv_ops WHERE tenant=$1 AND deployment=$2 AND subject=$3", scope.Tenant, scope.Deployment, scope.Subject).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 0 {
		t.Fatalf("deleted thread still retains %d KV receipts", receipts)
	}
}
