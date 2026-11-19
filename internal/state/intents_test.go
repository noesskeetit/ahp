package state

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func intentStore(t *testing.T) *Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Per-run database (isolation_test.go): see the note in kv_test.go.
	pool, _ := isolatedPool(t)
	s := New(pool)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := s.MigrateIntents(ctx); err != nil {
		t.Fatalf("MigrateIntents: %v", err)
	}
	return s
}

// intentFixture creates a unique scope + thread + pending run and claims it.
func intentFixture(t *testing.T, s *Store) (contracts.Scope, Run, *Lease) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	uniq := contracts.NewID()
	scope := contracts.Scope{Tenant: "t-" + uniq, Deployment: "d-" + uniq, Subject: "s-" + uniq}
	th, err := s.CreateThread(ctx, scope, "ctx-"+uniq, "thread-"+uniq)
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := s.CreateRun(ctx, scope, th.ID, "task-"+uniq, "run-"+uniq, json.RawMessage(`{"n":1}`))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	lease, err := s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "w-"+uniq, time.Minute)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if lease.Run.ID != run.ID {
		t.Fatalf("claimed run %s, want %s", lease.Run.ID, run.ID)
	}
	return scope, run, lease
}

func intentArgs(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal args: %v", err)
	}
	return b
}

func TestIntentRecordReplayAndArgMismatch(t *testing.T) {
	s := intentStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	scope, run, lease := intentFixture(t, s)

	args := intentArgs(t, map[string]any{"amount": 10, "cur": "EUR"})
	first, err := s.RecordIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", "http", "svc-a", args)
	if err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	if first.Status != IntentRecorded || first.ID == "" || first.Digest == "" {
		t.Fatalf("unexpected first intent: %+v", first)
	}

	// Replay with semantically identical args (different key order) must
	// return the exact same intent ID.
	replay := intentArgs(t, map[string]any{"cur": "EUR", "amount": 10})
	again, err := s.RecordIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", "http", "svc-a", replay)
	if err != nil {
		t.Fatalf("replay RecordIntent: %v", err)
	}
	if again.ID != first.ID {
		t.Fatalf("replay returned %s, want %s", again.ID, first.ID)
	}

	// Same key, different fingerprint: KEY_REUSE_MISMATCH.
	other := intentArgs(t, map[string]any{"amount": 99})
	_, err = s.RecordIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", "http", "svc-a", other)
	if contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("arg mismatch code = %v, want %s", err, contracts.ErrKeyReuse)
	}

	// Oversized args are rejected before any write.
	_, err = s.RecordIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callBig", "http", "svc-a",
		intentArgs(t, map[string]any{"blob": string(make([]byte, MaxIntentArgs+1))}))
	if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("oversized args code = %v, want %s", err, contracts.ErrInvalid)
	}

	// Invalid JSON args are rejected.
	_, err = s.RecordIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callBad", "http", "svc-a",
		json.RawMessage(`{oops`))
	if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("bad json code = %v, want %s", err, contracts.ErrInvalid)
	}

	got, err := s.GetIntent(ctx, scope, run.ID, "nodeA", "callA")
	if err != nil || got.ID != first.ID {
		t.Fatalf("GetIntent = %+v, %v", got, err)
	}
}

func TestIntentStaleEpoch(t *testing.T) {
	s := intentStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	scope, run, lease := intentFixture(t, s)

	// Reclaim the run to bump the thread epoch and invalidate `lease`.
	if err := s.Release(ctx, scope, run.ID, lease.Worker, lease.Epoch, RunPending); err != nil {
		t.Fatalf("Release: %v", err)
	}
	fresh, err := s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "w2-"+run.ID, time.Minute)
	if err != nil {
		t.Fatalf("re-ClaimRun: %v", err)
	}
	if fresh.Epoch <= lease.Epoch {
		t.Fatalf("epoch did not advance: %d -> %d", lease.Epoch, fresh.Epoch)
	}

	args := intentArgs(t, map[string]any{"a": 1})
	_, err = s.RecordIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", "http", "svc-a", args)
	if contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("stale record err = %v, want STALE_EPOCH", err)
	}
	_, err = s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentDispatched, nil)
	if contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("stale update err = %v, want STALE_EPOCH", err)
	}

	// The current lease still works.
	if _, err := s.RecordIntent(ctx, scope, run.ID, fresh.Worker, fresh.Epoch, "nodeA", "callA", "http", "svc-a", args); err != nil {
		t.Fatalf("RecordIntent under fresh lease: %v", err)
	}
}

func TestIntentCrossScope(t *testing.T) {
	s := intentStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	scope, run, lease := intentFixture(t, s)

	args := intentArgs(t, map[string]any{"a": 1})
	if _, err := s.RecordIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", "http", "svc-a", args); err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}

	foreign := contracts.Scope{Tenant: "t-" + contracts.NewID(), Deployment: scope.Deployment, Subject: scope.Subject}
	if _, err := s.GetIntent(ctx, foreign, run.ID, "nodeA", "callA"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-scope GetIntent err = %v, want NOT_FOUND", err)
	}
	foreignDep := contracts.Scope{Tenant: scope.Tenant, Deployment: "d-" + contracts.NewID(), Subject: scope.Subject}
	if _, err := s.GetIntent(ctx, foreignDep, run.ID, "nodeA", "callA"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-deployment GetIntent err = %v, want NOT_FOUND", err)
	}
	// A foreign scope never owns the lease, so writes fail as NOT_FOUND too.
	if _, err := s.RecordIntent(ctx, foreign, run.ID, lease.Worker, lease.Epoch, "nodeA", "callB", "http", "svc-a", args); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-scope RecordIntent err = %v, want NOT_FOUND", err)
	}
}

func TestIntentUpdateMonotonicity(t *testing.T) {
	s := intentStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	scope, run, lease := intentFixture(t, s)

	args := intentArgs(t, map[string]any{"a": 1})
	if _, err := s.RecordIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", "http", "svc-a", args); err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}

	// recorded -> recorded is a no-op replay.
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentRecorded, nil); err != nil {
		t.Fatalf("recorded replay: %v", err)
	}
	// recorded -> succeeded is not allowed.
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentSucceeded, nil); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("recorded->succeeded err = %v, want CONFLICT", err)
	}
	// recorded -> dispatched.
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentDispatched, nil); err != nil {
		t.Fatalf("recorded->dispatched: %v", err)
	}
	// dispatched -> recorded is forbidden.
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentRecorded, nil); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("dispatched->recorded err = %v, want CONFLICT", err)
	}
	// dispatched -> succeeded with a result.
	res := intentArgs(t, map[string]any{"ok": true})
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentSucceeded, res); err != nil {
		t.Fatalf("dispatched->succeeded: %v", err)
	}
	// Completion replay with a different result is rejected.
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentSucceeded, intentArgs(t, map[string]any{"ok": false})); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("completion replay mismatch err = %v, want CONFLICT", err)
	}
	// Completion replay with the identical result returns the stored intent.
	again, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentSucceeded, res)
	if err != nil {
		t.Fatalf("completion replay: %v", err)
	}
	if again.Status != IntentSucceeded {
		t.Fatalf("replayed status = %s, want succeeded", again.Status)
	}
	// A completed intent can never move again.
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentFailed, nil); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("succeeded->failed err = %v, want CONFLICT", err)
	}
}

func TestIntentUnknownNeedsPrivilegedReconcile(t *testing.T) {
	s := intentStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	scope, run, lease := intentFixture(t, s)

	args := intentArgs(t, map[string]any{"a": 1})
	rec, err := s.RecordIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", "http", "svc-a", args)
	if err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentDispatched, nil); err != nil {
		t.Fatalf("dispatched: %v", err)
	}
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentUnknown, nil); err != nil {
		t.Fatalf("unknown: %v", err)
	}
	// unknown -> dispatched is impossible through the normal path.
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentDispatched, nil); err == nil {
		t.Fatal("unknown->dispatched must fail")
	}
	// unknown -> succeeded is impossible through the normal path.
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentSucceeded, nil); !errors.Is(err, ErrNotReconcilable) {
		t.Fatalf("unknown->succeeded err = %v, want ErrNotReconcilable", err)
	}
	// The privileged path reconciles with the exact fingerprint.
	res := intentArgs(t, map[string]any{"recovered": true})
	rec2, err := s.ReconcileIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentSucceeded, rec.Digest, res)
	if err != nil {
		t.Fatalf("ReconcileIntent: %v", err)
	}
	if rec2.Status != IntentSucceeded || rec2.ID != rec.ID {
		t.Fatalf("reconciled intent = %+v", rec2)
	}
	// Reconciliation of a settled intent is rejected.
	if _, err := s.ReconcileIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentFailed, rec.Digest, nil); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("re-reconcile err = %v, want CONFLICT", err)
	}
}

func TestIntentFencedFromRecordedAndUnknown(t *testing.T) {
	s := intentStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	scope, run, lease := intentFixture(t, s)

	args := intentArgs(t, map[string]any{"a": 1})
	if _, err := s.RecordIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", "http", "svc-a", args); err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentFenced, nil); err != nil {
		t.Fatalf("recorded->fenced: %v", err)
	}
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeA", "callA", IntentDispatched, nil); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("fenced->dispatched err = %v, want CONFLICT", err)
	}

	if _, err := s.RecordIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeB", "callA", "http", "svc-b", args); err != nil {
		t.Fatalf("RecordIntent nodeB: %v", err)
	}
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeB", "callA", IntentDispatched, nil); err != nil {
		t.Fatalf("nodeB dispatched: %v", err)
	}
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeB", "callA", IntentUnknown, nil); err != nil {
		t.Fatalf("nodeB unknown: %v", err)
	}
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeB", "callA", IntentFenced, nil); err != nil {
		t.Fatalf("unknown->fenced: %v", err)
	}

	// Missing intents surface as NOT_FOUND.
	if _, err := s.UpdateIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "nodeZ", "callZ", IntentDispatched, nil); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("missing intent err = %v, want NOT_FOUND", err)
	}
}
