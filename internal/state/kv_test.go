package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

type kvEnv struct {
	s     *Store
	ctx   context.Context
	scope contracts.Scope
	fence Fence
}

func kvFixture(t *testing.T) *kvEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	// The run database is per test-binary (isolation_test.go): this suite's
	// in-place DDL used to deadlock against a second run on a shared database.
	pool, note := isolatedPool(t)
	t.Logf("state test substrate: %s", note)
	s := New(pool)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	// The KV tables may predate the tombstone column; migrate in place and
	// then upgrade any legacy table without touching its rows.
	if err := s.MigrateKV(ctx); err != nil {
		t.Fatalf("MigrateKV: %v", err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE ahp_state.kv ADD COLUMN IF NOT EXISTS deleted boolean NOT NULL DEFAULT false`); err != nil {
		t.Fatalf("upgrade kv table: %v", err)
	}
	// The migration runs in its own transaction, so a failing legacy DDL
	// statement cannot poison this connection.
	scope := contracts.Scope{Tenant: "t-" + contracts.NewID(), Deployment: "d-" + contracts.NewID(),
		Subject: "subj-" + contracts.NewID()}
	th, err := s.CreateThread(ctx, scope, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := s.CreateRun(ctx, scope, th.ID, "task-1", "k-"+contracts.NewID(), json.RawMessage(`{"a":1}`)); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	lease, err := s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "w1", time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	return &kvEnv{s: s, ctx: ctx, scope: scope,
		fence: Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}}
}

func kvPut(t *testing.T, e *kvEnv, ns []string, key, op string, exp int64, value string) KVItem {
	t.Helper()
	item, err := e.s.PutKV(e.ctx, e.scope, e.fence, ns, key, op, exp, json.RawMessage(value))
	if err != nil {
		t.Fatalf("PutKV(%s): %v", key, err)
	}
	return item
}

func TestKVWriteUnderClaimedRunAndReadback(t *testing.T) {
	e := kvFixture(t)
	ns := []string{"profile", "v1"}
	created := kvPut(t, e, ns, "user-1", "op-1", 0, `{"b":1,"a":2}`)
	if created.Version != 1 || created.ThreadID == "" {
		t.Fatalf("bad create result: %+v", created)
	}
	got, err := e.s.GetKV(e.ctx, e.scope, ns, "user-1")
	if err != nil {
		t.Fatalf("GetKV: %v", err)
	}
	if got.Version != 1 || got.Key != "user-1" || fmt.Sprint(got.Namespace) != fmt.Sprint(ns) {
		t.Fatalf("bad readback: %+v", got)
	}
	// Duplicate JSON object keys are rejected via the canonical digest.
	if _, err := e.s.PutKV(e.ctx, e.scope, e.fence, ns, "dup", "op-dup", 0,
		json.RawMessage(`{"a":1,"a":2}`)); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("duplicate keys: want INVALID_ARGUMENT, got %v", err)
	}
	// Oversized value is rejected.
	big := append([]byte(`{"x":"`), make([]byte, 1<<20)...)
	big = append(big, []byte(`"}`)...)
	if _, err := e.s.PutKV(e.ctx, e.scope, e.fence, ns, "big", "op-big", 0, big); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("oversized value: want INVALID_ARGUMENT, got %v", err)
	}
	// Namespace and key bounds.
	if _, err := e.s.PutKV(e.ctx, e.scope, e.fence, make([]string, 9), "k", "op-n", 0, json.RawMessage(`{}`)); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("namespace overflow: want INVALID_ARGUMENT, got %v", err)
	}
	if _, err := e.s.PutKV(e.ctx, e.scope, e.fence, ns, string(make([]byte, 257)), "op-k", 0, json.RawMessage(`{}`)); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("key overflow: want INVALID_ARGUMENT, got %v", err)
	}
	// A nil namespace is the empty namespace and round-trips.
	root := kvPut(t, e, nil, "root", "op-root", 0, `{}`)
	if len(root.Namespace) != 0 {
		t.Fatalf("nil namespace must store empty: %+v", root.Namespace)
	}
	if _, err := e.s.GetKV(e.ctx, e.scope, nil, "root"); err != nil {
		t.Fatalf("get under empty namespace: %v", err)
	}
}

func TestKVSubjectsIsolated(t *testing.T) {
	e := kvFixture(t)
	ns := []string{"notes"}
	kvPut(t, e, ns, "secret", "op-1", 0, `{"own":true}`)

	foreign := contracts.Scope{Tenant: e.scope.Tenant, Deployment: e.scope.Deployment, Subject: "subj-" + contracts.NewID()}
	if _, err := e.s.GetKV(e.ctx, foreign, ns, "secret"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign GetKV: want NOT_FOUND, got %v", err)
	}
	items, err := e.s.SearchKV(e.ctx, foreign, nil, "", nil, 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("foreign SearchKV must be empty: %+v, %v", items, err)
	}
	// The owner fence does not authorise writes in a foreign subject scope.
	if _, err := e.s.PutKV(e.ctx, foreign, e.fence, ns, "secret", "op-f", 0, json.RawMessage(`{"own":false}`)); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign put with owner fence: want NOT_FOUND, got %v", err)
	}
	own, err := e.s.SearchKV(e.ctx, e.scope, ns, "", nil, 10)
	if err != nil || len(own) != 1 || own[0].Key != "secret" {
		t.Fatalf("owner search lost data: %+v, %v", own, err)
	}
}

func TestKVSearchNamespacePrefixAndFilter(t *testing.T) {
	e := kvFixture(t)
	kvPut(t, e, []string{"a", "b"}, "alpha", "op-1", 0, `{"tag":"x","n":1}`)
	kvPut(t, e, []string{"a", "b"}, "alphb", "op-2", 0, `{"tag":"y"}`)
	kvPut(t, e, []string{"a", "c"}, "beta", "op-3", 0, `{"tag":"x"}`)
	kvPut(t, e, []string{"z"}, "alpha", "op-4", 0, `{"tag":"x"}`)
	kvPut(t, e, []string{"b"}, "gamma", "op-5", 0, `{"tag":"x"}`)

	all, err := e.s.SearchKV(e.ctx, e.scope, []string{"a"}, "", nil, 10)
	if err != nil || len(all) != 3 {
		t.Fatalf("prefix a: %+v, %v", all, err)
	}
	if all[0].Key != "alpha" || all[2].Namespace[1] != "c" {
		t.Fatalf("unstable order: %+v", all)
	}
	// Prefix is ordered: ["b","a"] must not match the row in namespace ["a"].
	if got, err := e.s.SearchKV(e.ctx, e.scope, []string{"b", "a"}, "", nil, 10); err != nil || len(got) != 0 {
		t.Fatalf("prefix must be ordered, containment leaked: %+v, %v", got, err)
	}
	// A longer prefix than the stored namespace never matches.
	if got, err := e.s.SearchKV(e.ctx, e.scope, []string{"z", "deep"}, "", nil, 10); err != nil || len(got) != 0 {
		t.Fatalf("over-long prefix must not match: %+v, %v", got, err)
	}
	prefixed, err := e.s.SearchKV(e.ctx, e.scope, []string{"a", "b"}, "alph", nil, 10)
	if err != nil || len(prefixed) != 2 {
		t.Fatalf("key prefix: %+v, %v", prefixed, err)
	}
	// LIKE wildcards in the caller prefix are literal, not patterns.
	if got, err := e.s.SearchKV(e.ctx, e.scope, []string{"a"}, "a%a", nil, 10); err != nil || len(got) != 0 {
		t.Fatalf("wildcard must be literal: %+v, %v", got, err)
	}
	// Empty prefix matches every namespace, including the root namespace.
	kvPut(t, e, nil, "root", "op-6", 0, `{"tag":"x"}`)
	filtered, err := e.s.SearchKV(e.ctx, e.scope, nil, "", json.RawMessage(`{"tag":"x"}`), 10)
	if err != nil || len(filtered) != 5 {
		t.Fatalf("containment filter: %+v, %v", filtered, err)
	}
	narrow, err := e.s.SearchKV(e.ctx, e.scope, []string{"a"}, "", json.RawMessage(`{"tag":"x","n":1}`), 10)
	if err != nil || len(narrow) != 1 || narrow[0].Key != "alpha" {
		t.Fatalf("narrow filter: %+v, %v", narrow, err)
	}
	// Limit is honoured with stable order.
	limited, err := e.s.SearchKV(e.ctx, e.scope, nil, "", nil, 2)
	if err != nil || len(limited) != 2 || limited[0].Key != "root" {
		t.Fatalf("limit: %+v, %v", limited, err)
	}
	if _, err := e.s.SearchKV(e.ctx, e.scope, nil, "", json.RawMessage(`not json`), 10); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("bad filter: want INVALID_ARGUMENT, got %v", err)
	}
}

func TestKVCASConflict(t *testing.T) {
	e := kvFixture(t)
	ns := []string{"cas"}
	created := kvPut(t, e, ns, "k", "op-1", 0, `{"v":1}`)
	// Stale expected version conflicts.
	if _, err := e.s.PutKV(e.ctx, e.scope, e.fence, ns, "k", "op-2", created.Version+5, json.RawMessage(`{"v":2}`)); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("cas conflict: want CONFLICT, got %v", err)
	}
	// Create-only over an existing item conflicts.
	if _, err := e.s.PutKV(e.ctx, e.scope, e.fence, ns, "k", "op-3", 0, json.RawMessage(`{"v":2}`)); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("create-only: want CONFLICT, got %v", err)
	}
	// Correct CAS succeeds and allocates old+1.
	updated := kvPut(t, e, ns, "k", "op-4", created.Version, `{"v":2}`)
	if updated.Version != 2 {
		t.Fatalf("version allocation: %+v", updated)
	}
	// Delete with wrong version conflicts; right version succeeds.
	if err := e.s.DeleteKV(e.ctx, e.scope, e.fence, ns, "k", "op-5", 9); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("delete cas: want CONFLICT, got %v", err)
	}
	if err := e.s.DeleteKV(e.ctx, e.scope, e.fence, ns, "k", "op-6", 2); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := e.s.GetKV(e.ctx, e.scope, ns, "k"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("after delete: want NOT_FOUND, got %v", err)
	}
	if got, err := e.s.SearchKV(e.ctx, e.scope, ns, "", nil, 10); err != nil || len(got) != 0 {
		t.Fatalf("search must exclude deleted: %+v, %v", got, err)
	}
	if err := e.s.DeleteKV(e.ctx, e.scope, e.fence, ns, "k", "op-7", 2); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("delete missing: want NOT_FOUND, got %v", err)
	}
}

func TestKVDeleteRecreatePreservesVersion(t *testing.T) {
	e := kvFixture(t)
	ns := []string{"aba"}
	kvPut(t, e, ns, "k", "op-1", 0, `{"v":1}`)
	if err := e.s.DeleteKV(e.ctx, e.scope, e.fence, ns, "k", "op-2", 1); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Create-only after a tombstone is allowed but must NOT restart at 1.
	recreated := kvPut(t, e, ns, "k", "op-3", 0, `{"v":2}`)
	if recreated.Version != 2 {
		t.Fatalf("ABA: version restarted after delete/recreate: %+v", recreated)
	}
	// The pre-delete version is stale now.
	if _, err := e.s.PutKV(e.ctx, e.scope, e.fence, ns, "k", "op-4", 1, json.RawMessage(`{"v":3}`)); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("stale pre-delete version: want CONFLICT, got %v", err)
	}
	// CAS against the tombstone baseline succeeds and keeps incrementing.
	next := kvPut(t, e, ns, "k", "op-5", 2, `{"v":3}`)
	if next.Version != 3 {
		t.Fatalf("version after tombstone CAS: %+v", next)
	}
}

func TestKVIdempotentReplayAfterLaterUpdate(t *testing.T) {
	e := kvFixture(t)
	ns := []string{"idem"}
	first := kvPut(t, e, ns, "k", "op-1", 0, `{"v":1}`)
	replay := kvPut(t, e, ns, "k", "op-1", 0, `{"v": 1}`)
	if replay.Version != first.Version || string(replay.Value) != string(first.Value) {
		t.Fatalf("replay differs: %+v vs %+v", replay, first)
	}
	// Later update moves the item forward.
	kvPut(t, e, ns, "k", "op-2", 1, `{"v":2}`)
	// Replaying the original create still returns the ORIGINAL result.
	again := kvPut(t, e, ns, "k", "op-1", 0, `{"v":1}`)
	if again.Version != 1 || string(again.Value) != `{"v":1}` {
		t.Fatalf("receipt must preserve previous result: %+v", again)
	}
	// Same operation key, different request: KEY_REUSE_MISMATCH.
	if _, err := e.s.PutKV(e.ctx, e.scope, e.fence, ns, "k", "op-1", 0, json.RawMessage(`{"v":3}`)); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("reuse mismatch: want KEY_REUSE_MISMATCH, got %v", err)
	}
	// Delete receipt replays as success even though the item is gone.
	if err := e.s.DeleteKV(e.ctx, e.scope, e.fence, ns, "k", "op-3", 2); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := e.s.DeleteKV(e.ctx, e.scope, e.fence, ns, "k", "op-3", 2); err != nil {
		t.Fatalf("delete replay: %v", err)
	}
	// A put receipt may not be replayed as a delete and vice versa.
	if err := e.s.DeleteKV(e.ctx, e.scope, e.fence, ns, "k", "op-1", 1); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("cross-kind replay: want KEY_REUSE_MISMATCH, got %v", err)
	}
	// Recreating the key does not resurrect the old delete receipt: the
	// receipt no longer matches the item and must fail as a reuse mismatch.
	kvPut(t, e, ns, "k", "op-4", 0, `{"v":4}`)
	if err := e.s.DeleteKV(e.ctx, e.scope, e.fence, ns, "k", "op-3", 2); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("receipt after recreate: want KEY_REUSE_MISMATCH, got %v", err)
	}
}

func TestKVStaleEpochRejected(t *testing.T) {
	e := kvFixture(t)
	ns := []string{"fence"}
	if _, err := e.s.PutKV(e.ctx, e.scope, Fence{RunID: e.fence.RunID, Worker: e.fence.Worker, Epoch: e.fence.Epoch + 1000},
		ns, "k", "op-1", 0, json.RawMessage(`{}`)); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("put stale: want STALE_EPOCH, got %v", err)
	}
	if err := e.s.DeleteKV(e.ctx, e.scope, Fence{RunID: e.fence.RunID, Worker: e.fence.Worker, Epoch: e.fence.Epoch + 1000},
		ns, "k", "op-2", 1); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("delete stale: want STALE_EPOCH, got %v", err)
	}
	// Nothing was written under the stale fence.
	if _, err := e.s.GetKV(e.ctx, e.scope, ns, "k"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("stale write leaked: %v", err)
	}
	// The live fence still works.
	kvPut(t, e, ns, "k", "op-3", 0, `{}`)
}

// TestKVConcurrentCASOneWinner races live threads that each try to CAS the
// same key from version 1 to 2 under the same lease. Exactly one write may
// win; every loser sees CONFLICT and the final version is exactly 2.
func TestKVConcurrentCASOneWinner(t *testing.T) {
	e := kvFixture(t)
	ns := []string{"race"}
	kvPut(t, e, ns, "k", "op-seed", 0, `{"v":0}`)

	const racers = 6
	start := make(chan struct{})
	var wg sync.WaitGroup
	var okCount, conflictCount int64
	errs := make(chan error, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := e.s.PutKV(e.ctx, e.scope, e.fence, ns, "k",
				fmt.Sprintf("op-race-%d", i), 1, json.RawMessage(fmt.Sprintf(`{"winner":%d}`, i)))
			switch {
			case err == nil:
				atomic.AddInt64(&okCount, 1)
			case contracts.Code(err) == contracts.ErrConflict:
				atomic.AddInt64(&conflictCount, 1)
			default:
				errs <- err
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected racer error: %v", err)
	}
	if okCount != 1 {
		t.Fatalf("CAS must admit exactly one winner, got %d", okCount)
	}
	if conflictCount != racers-1 {
		t.Fatalf("losers must all see CONFLICT, got %d", conflictCount)
	}
	got, err := e.s.GetKV(e.ctx, e.scope, ns, "k")
	if err != nil || got.Version != 2 {
		t.Fatalf("final state wrong: %+v, %v", got, err)
	}
}

// TestKVConcurrentSameOperationKeyOneInsert races identical requests under one
// operation key: every racer must observe the same result and exactly one
// version may be allocated.
func TestKVConcurrentSameOperationKeyOneInsert(t *testing.T) {
	e := kvFixture(t)
	ns := []string{"race-op"}

	const racers = 5
	start := make(chan struct{})
	var wg sync.WaitGroup
	versions := make(chan int64, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			item, err := e.s.PutKV(e.ctx, e.scope, e.fence, ns, "k", "op-same", 0, json.RawMessage(`{"v":1}`))
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					t.Errorf("racer: %v", err)
				}
				return
			}
			versions <- item.Version
		}()
	}
	close(start)
	wg.Wait()
	close(versions)
	seen := map[int64]int{}
	for v := range versions {
		seen[v]++
	}
	if len(seen) != 1 || seen[1] == 0 {
		t.Fatalf("all racers must see version 1: %v", seen)
	}
	items, err := e.s.SearchKV(e.ctx, e.scope, ns, "", nil, 10)
	if err != nil || len(items) != 1 || items[0].Version != 1 {
		t.Fatalf("duplicate rows after race: %+v, %v", items, err)
	}
}
