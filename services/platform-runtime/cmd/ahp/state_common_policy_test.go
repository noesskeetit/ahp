package main

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCommonConfiguredOwnerStateSurvivesWritingSession(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("own real PostgreSQL required")
	}
	ctx := context.Background()
	pool, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal("own PG unavailable")
	}
	defer pool.Close()
	sc := contracts.Scope{Tenant: "common-config-" + contracts.NewID(), Deployment: "native", Subject: "alice"}
	path := filepath.Join(t.TempDir(), "reviewed.json")
	data, _ := json.Marshal([]state.KVNamespacePolicy{{Tenant: sc.Tenant, Deployment: sc.Deployment, Namespace: []string{"app"}, Owner: "deployment"}})
	if e := os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("AHP_STATE_COMMON_MODE", "interval")
	t.Setenv("AHP_STATE_KV_NAMESPACE_POLICIES_FILE", path)
	options, e := loadStateStoreOptions()
	if e != nil {
		t.Fatal(e)
	}
	s := state.New(pool, options...)
	if e := s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	thread, e := s.CreateThread(ctx, sc, "context", "writer")
	if e != nil {
		t.Fatal(e)
	}
	run, e := s.CreateRun(ctx, sc, thread.ID, "run", "task", json.RawMessage(`{}`))
	if e != nil {
		t.Fatal(e)
	}
	lease, e := s.ClaimRunFor(ctx, sc.Tenant, sc.Deployment, "worker", run.ID, time.Minute)
	if e != nil || lease == nil {
		t.Fatal("lease", e)
	}
	_, e = s.PutKVBatch(ctx, sc, state.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}, state.KVBatchRequest{OperationKey: "app-value", Items: []state.KVPut{{Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment", Value: json.RawMessage(`{"shared":true}`)}}})
	if e != nil {
		t.Fatal("operator policy was not applied", e)
	}
	if e := s.DeleteThread(ctx, sc, thread.ID); e != nil {
		t.Fatal(e)
	}
	bob := sc
	bob.Subject = "bob"
	value, e := s.GetKV(ctx, bob, []string{"app"}, "state")
	if e != nil || value.ThreadID != "" || value.Version != 1 {
		t.Fatal("reviewed shared data acquired writer-session lifetime", e)
	}
}
