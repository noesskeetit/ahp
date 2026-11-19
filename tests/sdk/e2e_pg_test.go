//go:build e2e

// Package sdk_test — PG-backed E2E test for the AHP SDK contract.
//
// Requires AHP_TEST_DATABASE_URL (e.g. postgres://user:pass@127.0.0.1:25432/db?sslmode=disable).
// Run: AHP_TEST_DATABASE_URL=... go test -tags e2e ./tests/sdk/ -run TestE2E
//
// This test exercises the full State API checkpoint/intent/KV/lease lifecycle
// via internal/state.Store (direct PG). When the State API HTTP routes are added,
// the same sequence should be tested against the HTTP endpoint.
package sdk_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func testStore(t *testing.T) *state.Store {
	t.Helper()
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AHP_TEST_DATABASE_URL is not set: PG-backed E2E requires a real PostgreSQL")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	store := state.New(pool)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := store.MigrateCheckpoints(context.Background()); err != nil {
		t.Fatalf("MigrateCheckpoints: %v", err)
	}
	if err := store.MigrateIntents(context.Background()); err != nil {
		t.Fatalf("MigrateIntents: %v", err)
	}
	if err := store.MigrateEvents(context.Background()); err != nil {
		t.Fatalf("MigrateEvents: %v", err)
	}
	if err := store.MigrateKV(context.Background()); err != nil {
		t.Fatalf("MigrateKV: %v", err)
	}
	return store
}

// runTag isolates each test-process run into its own deployment so re-runs and
// concurrent verifiers never contend for the same ClaimRun queue on the shared
// acceptance DB (no destructive cleanup required; ClaimRun sees only this run's
// freshly created, pending runs).
var runTag = contracts.NewID()

var e2eScope = contracts.Scope{
	Tenant:     "e2e-tenant",
	Deployment: "e2e-deploy-" + runTag,
	Subject:    "e2e-subject",
}

func TestE2EFullLifecycle(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	thread, err := store.CreateThread(ctx, e2eScope, "e2e-ctx-1", "e2e-key-1")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}

	_, err = store.CreateRun(ctx, e2eScope, thread.ID, "task-e2e-1", "run-e2e-1", json.RawMessage(`{"input":"hello"}`))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	lease, err := store.ClaimRun(ctx, e2eScope.Tenant, e2eScope.Deployment, "e2e-worker-1", 2*time.Minute)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if lease == nil {
		t.Fatal("ClaimRun returned nil (no claimable run)")
	}
	fence := state.Fence{RunID: lease.Run.ID, Worker: "e2e-worker-1", Epoch: lease.Epoch}

	cp := state.Checkpoint{
		ID:        "e2e-cp-1",
		Namespace: "graph",
		Version:   1,
		Parts:     []state.Part{{Channel: "state", Version: "v1", Data: []byte(`{"counter":1}`)}},
	}
	storedCP, err := store.PutCheckpoint(ctx, e2eScope, fence, cp, "e2e-cp-key-1")
	if err != nil {
		t.Fatalf("PutCheckpoint: %v", err)
	}
	if storedCP.ID != "e2e-cp-1" {
		t.Errorf("stored checkpoint ID = %q", storedCP.ID)
	}

	gotCP, err := store.GetCheckpoint(ctx, e2eScope, thread.ID, "graph", "e2e-cp-1")
	if err != nil {
		t.Fatalf("GetCheckpoint: %v", err)
	}
	// GetCheckpoint returns part REFERENCES (Channel/Version); part Data is
	// resolved from ahp_state.checkpoint_blobs, not returned in-band. Assert the
	// reference identity, then prove the payload actually persisted (real
	// round-trip via the blob store) rather than assuming an in-band field.
	if len(gotCP.Parts) != 1 || gotCP.Parts[0].Channel != "state" || gotCP.Parts[0].Version != "v1" {
		t.Fatalf("checkpoint part reference mismatch: %+v", gotCP.Parts)
	}
	var blob []byte
	if err := store.Pool().QueryRow(ctx,
		`SELECT data FROM ahp_state.checkpoint_blobs
		  WHERE thread_id = $1 AND namespace = 'graph' AND channel = 'state' AND version = 'v1'`,
		thread.ID).Scan(&blob); err != nil {
		t.Fatalf("checkpoint blob round-trip: %v", err)
	}
	if string(blob) != `{"counter":1}` {
		t.Errorf("checkpoint blob = %q, want {\"counter\":1}", blob)
	}

	err = store.PutWrites(ctx, e2eScope, fence, "graph", "e2e-cp-1", []state.PendingWrite{
		{Task: "node-a", Index: 0, Channel: "result", Data: []byte(`"ok"`)},
	})
	if err != nil {
		t.Fatalf("PutWrites: %v", err)
	}
	writes, err := store.GetWrites(ctx, e2eScope, thread.ID, "graph", "e2e-cp-1")
	if err != nil {
		t.Fatalf("GetWrites: %v", err)
	}
	if len(writes) != 1 || writes[0].Channel != "result" {
		t.Errorf("writes mismatch: %+v", writes)
	}

	intent, err := store.RecordIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"tool-node", "call-1", "http", "https://api.example.com/data", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	if intent.Status != state.IntentRecorded {
		t.Errorf("intent status = %q", intent.Status)
	}
	if intent.Digest == "" {
		t.Error("intent digest is empty (operation key)")
	}

	// The authoritative intent machine forbids recorded -> succeeded; the effect
	// must pass through dispatched first (SDK-R6-3).
	if _, err := store.UpdateIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"tool-node", "call-1", state.IntentDispatched, nil); err != nil {
		t.Fatalf("UpdateIntent dispatch: %v", err)
	}
	updated, err := store.UpdateIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"tool-node", "call-1", state.IntentSucceeded, json.RawMessage(`{"status":200}`))
	if err != nil {
		t.Fatalf("UpdateIntent: %v", err)
	}
	if updated.Status != state.IntentSucceeded {
		t.Errorf("intent status after update = %q", updated.Status)
	}

	kv, err := store.PutKV(ctx, e2eScope, fence, []string{"user", "profile"}, "name", "kv-op-1", 0, json.RawMessage(`"Alice"`))
	if err != nil {
		t.Fatalf("PutKV: %v", err)
	}
	if kv.Version != 1 {
		t.Errorf("kv version = %d", kv.Version)
	}
	gotKV, err := store.GetKV(ctx, e2eScope, []string{"user", "profile"}, "name")
	if err != nil {
		t.Fatalf("GetKV: %v", err)
	}
	if string(gotKV.Value) != `"Alice"` {
		t.Errorf("kv value = %s", gotKV.Value)
	}

	if err := store.Release(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch, state.RunDone); err != nil {
		t.Fatalf("Release: %v", err)
	}

	finalRun, err := store.GetRun(ctx, e2eScope, fence.RunID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if finalRun.Status != state.RunDone {
		t.Errorf("run status = %q, want done", finalRun.Status)
	}
}

func TestE2EStaleEpochRejection(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	thread, _ := store.CreateThread(ctx, e2eScope, "e2e-ctx-stale", "e2e-key-stale")
	_, _ = store.CreateRun(ctx, e2eScope, thread.ID, "task-stale", "run-stale", json.RawMessage(`{}`))

	// A fabricated epoch-1 is INVALID (epoch must be >=1). Produce a genuinely
	// stale-but-valid fence the way the authoritative tests do: hold a live
	// fence, force lease expiry, requeue, and reclaim at a higher epoch, then the
	// ORIGINAL (valid-valued) fence must be rejected as STALE_EPOCH.
	lease, err := store.ClaimRun(ctx, e2eScope.Tenant, e2eScope.Deployment, "stale-worker", 2*time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("ClaimRun: %v (lease=%v)", err, lease)
	}
	oldFence := state.Fence{RunID: lease.Run.ID, Worker: "stale-worker", Epoch: lease.Epoch}
	if _, err := store.PutCheckpoint(ctx, e2eScope, oldFence, state.Checkpoint{
		ID: "stale-ok", Namespace: "graph", Version: 1,
	}, "stale-ok-key"); err != nil {
		t.Fatalf("PutCheckpoint under live lease: %v", err)
	}
	if _, err := store.Pool().Exec(ctx,
		`UPDATE ahp_state.threads SET lease_expires = clock_timestamp() - interval '1 second'
		  WHERE id = (SELECT thread_id FROM ahp_state.runs WHERE id = $1)`, oldFence.RunID); err != nil {
		t.Fatalf("force expiry: %v", err)
	}
	if _, err := store.Pool().Exec(ctx,
		`UPDATE ahp_state.runs SET status = 'pending' WHERE id = $1`, oldFence.RunID); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	reclaim, err := store.ClaimRun(ctx, e2eScope.Tenant, e2eScope.Deployment, "stale-worker-2", 2*time.Minute)
	if err != nil || reclaim == nil {
		t.Fatalf("reclaim: %v (lease=%v)", err, reclaim)
	}
	if reclaim.Epoch <= oldFence.Epoch {
		t.Fatalf("reclaim epoch %d not above old %d", reclaim.Epoch, oldFence.Epoch)
	}
	if _, err := store.PutCheckpoint(ctx, e2eScope, oldFence, state.Checkpoint{
		ID: "stale-cp", Namespace: "graph", Version: 1,
	}, "stale-key"); err == nil {
		t.Fatal("stale fence should be rejected after reclaim")
	} else if contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Errorf("error code = %s, want STALE_EPOCH", contracts.Code(err))
	}
	// The reclaimed (new) fence is accepted.
	if _, err := store.PutCheckpoint(ctx, e2eScope,
		state.Fence{RunID: reclaim.Run.ID, Worker: reclaim.Worker, Epoch: reclaim.Epoch},
		state.Checkpoint{ID: "stale-new", Namespace: "graph", Version: 1}, "stale-new-key"); err != nil {
		t.Errorf("new fence after reclaim should be accepted: %v", err)
	}
	_ = store.Release(ctx, e2eScope, reclaim.Run.ID, reclaim.Worker, reclaim.Epoch, state.RunDone)
}

// mustFence creates an isolated thread+run and claims it, returning a live
// lease fence. The run is released on test cleanup.
func mustFence(t *testing.T, store *state.Store, suffix string) state.Fence {
	t.Helper()
	ctx := context.Background()
	thread, err := store.CreateThread(ctx, e2eScope, "ctx-"+suffix, "key-"+suffix)
	if err != nil {
		t.Fatalf("CreateThread(%s): %v", suffix, err)
	}
	runID := "run-" + suffix
	_, err = store.CreateRun(ctx, e2eScope, thread.ID, "task-"+suffix, runID, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("CreateRun(%s): %v", suffix, err)
	}
	worker := "worker-" + suffix
	lease, err := store.ClaimRun(ctx, e2eScope.Tenant, e2eScope.Deployment, worker, 2*time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("ClaimRun(%s): %v (lease=%v)", suffix, err, lease)
	}
	t.Cleanup(func() {
		_ = store.Release(context.Background(), e2eScope, lease.Run.ID, worker, lease.Epoch, state.RunDone)
	})
	return state.Fence{RunID: lease.Run.ID, Worker: worker, Epoch: lease.Epoch}
}

// TestE2EKVCompareAndSet exercises the authoritative version/CAS + idempotency
// semantics from internal/state/kv.go against a REAL Postgres (SDK-R6-1/2).
// The version-ignoring in-memory fake is NOT relied on here.
func TestE2EKVCompareAndSet(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	fence := mustFence(t, store, "cas")
	ns := []string{"cas", "ns"}

	// create-only (expected_version 0) on an absent key -> version 1.
	item, err := store.PutKV(ctx, e2eScope, fence, ns, "k", "op-create-1", 0, json.RawMessage(`"v1"`))
	if err != nil {
		t.Fatalf("create PutKV: %v", err)
	}
	if item.Version != 1 {
		t.Fatalf("create version = %d, want 1", item.Version)
	}

	// create-only again on an existing item must CONFLICT.
	if _, err := store.PutKV(ctx, e2eScope, fence, ns, "k", "op-create-2", 0, json.RawMessage(`"again"`)); err == nil {
		t.Fatal("create-only on existing key should conflict")
	} else if contracts.Code(err) != contracts.ErrConflict {
		t.Errorf("create-only conflict code = %s, want CONFLICT", contracts.Code(err))
	}

	// wrong expected version must CONFLICT.
	if _, err := store.PutKV(ctx, e2eScope, fence, ns, "k", "op-bad-1", 99, json.RawMessage(`"x"`)); err == nil {
		t.Fatal("CAS with wrong version should conflict")
	} else if contracts.Code(err) != contracts.ErrConflict {
		t.Errorf("wrong-version code = %s, want CONFLICT", contracts.Code(err))
	}

	// correct CAS (expected_version == current) bumps the version.
	updated, err := store.PutKV(ctx, e2eScope, fence, ns, "k", "op-update-1", item.Version, json.RawMessage(`"v2"`))
	if err != nil {
		t.Fatalf("CAS update: %v", err)
	}
	if updated.Version != 2 {
		t.Errorf("CAS version = %d, want 2", updated.Version)
	}

	// replaying an identical op key returns the receipt (idempotent, no bump).
	replay, err := store.PutKV(ctx, e2eScope, fence, ns, "k", "op-update-1", item.Version, json.RawMessage(`"v2"`))
	if err != nil {
		t.Fatalf("CAS replay: %v", err)
	}
	if replay.Version != updated.Version {
		t.Errorf("replay version = %d, want %d (receipt replay)", replay.Version, updated.Version)
	}

	// reusing an op key for a DIFFERENT request -> KEY_REUSE_MISMATCH.
	if _, err := store.PutKV(ctx, e2eScope, fence, ns, "k", "op-update-1", 2, json.RawMessage(`"other"`)); err == nil {
		t.Fatal("reusing op key with different value should fail KEY_REUSE")
	} else if contracts.Code(err) != contracts.ErrKeyReuse {
		t.Errorf("reuse code = %s, want KEY_REUSE_MISMATCH", contracts.Code(err))
	}

	// delete requires expected_version >= 1.
	if err := store.DeleteKV(ctx, e2eScope, fence, ns, "k", "op-del-bad", 0); err == nil {
		t.Fatal("delete with version 0 should be invalid")
	} else if contracts.Code(err) != contracts.ErrInvalid {
		t.Errorf("delete(0) code = %s, want INVALID", contracts.Code(err))
	}

	// put -> delete -> recreate with a distinct value persists (no stale receipt).
	if _, err := store.PutKV(ctx, e2eScope, fence, ns, "cyc", "cyc-put", 0, json.RawMessage(`"orig"`)); err != nil {
		t.Fatalf("cycle put: %v", err)
	}
	cur, _ := store.GetKV(ctx, e2eScope, ns, "cyc")
	if err := store.DeleteKV(ctx, e2eScope, fence, ns, "cyc", "cyc-del", cur.Version); err != nil {
		t.Fatalf("cycle delete: %v", err)
	}
	re, err := store.PutKV(ctx, e2eScope, fence, ns, "cyc", "cyc-recreate", 0, json.RawMessage(`"recreated"`))
	if err != nil {
		t.Fatalf("cycle recreate: %v", err)
	}
	if string(re.Value) != `"recreated"` {
		t.Errorf("recreate value = %s, want recreated", re.Value)
	}

	// namespace tuple boundaries are unambiguous: ["a:b","c"] != ["a","b:c"].
	if _, err := store.PutKV(ctx, e2eScope, fence, []string{"a:b", "c"}, "k", "ns-a", 0, json.RawMessage(`"first"`)); err != nil {
		t.Fatalf("ns-a put: %v", err)
	}
	if _, err := store.PutKV(ctx, e2eScope, fence, []string{"a", "b:c"}, "k", "ns-b", 0, json.RawMessage(`"second"`)); err != nil {
		t.Fatalf("ns-b put: %v", err)
	}
	a, err := store.GetKV(ctx, e2eScope, []string{"a:b", "c"}, "k")
	if err != nil || string(a.Value) != `"first"` {
		t.Errorf("ns-a read = %+v err=%v, want first", a, err)
	}
	b, err := store.GetKV(ctx, e2eScope, []string{"a", "b:c"}, "k")
	if err != nil || string(b.Value) != `"second"` {
		t.Errorf("ns-b read = %+v err=%v, want second", b, err)
	}
}

// TestE2EIntentCrashWindow exercises the authoritative intent status machine
// and privileged reconciliation from internal/state/intents.go against a REAL
// Postgres (SDK-R6-3): RecordIntent replay must NOT be trusted for the current
// status; an unknown intent resolves only via ReconcileIntent.
func TestE2EIntentCrashWindow(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	fence := mustFence(t, store, "intent")

	// happy path: recorded -> dispatched -> succeeded.
	if _, err := store.RecordIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"tool", "call-ok", "http", "t", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	if _, err := store.UpdateIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"tool", "call-ok", state.IntentDispatched, nil); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if _, err := store.UpdateIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"tool", "call-ok", state.IntentSucceeded, json.RawMessage(`{"r":1}`)); err != nil {
		t.Fatalf("succeed: %v", err)
	}

	// RecordIntent replay returns recorded-from-source, but GetIntent is truth.
	replay, err := store.RecordIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"tool", "call-ok", "http", "t", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("RecordIntent replay: %v", err)
	}
	if replay.Status != state.IntentRecorded {
		t.Errorf("replay status = %q, want recorded (replay returns from-source)", replay.Status)
	}
	got, err := store.GetIntent(ctx, e2eScope, fence.RunID, "tool", "call-ok")
	if err != nil {
		t.Fatalf("GetIntent: %v", err)
	}
	if got.Status != state.IntentSucceeded {
		t.Errorf("GetIntent status = %q, want succeeded (GetIntent is source of truth)", got.Status)
	}

	// illegal transition recorded -> succeeded must be rejected.
	if _, err := store.RecordIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"tool", "call-illegal", "http", "t", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("record illegal: %v", err)
	}
	if _, err := store.UpdateIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"tool", "call-illegal", state.IntentSucceeded, nil); err == nil {
		t.Fatal("recorded->succeeded should be rejected")
	}

	// crash window: effect fired (dispatched) but no completion -> mark unknown,
	// then reconcile to succeeded with the original digest.
	if _, err := store.RecordIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"tool", "call-crash", "http", "t", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("record crash: %v", err)
	}
	if _, err := store.UpdateIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"tool", "call-crash", state.IntentDispatched, nil); err != nil {
		t.Fatalf("dispatch crash: %v", err)
	}
	if _, err := store.UpdateIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"tool", "call-crash", state.IntentUnknown, nil); err != nil {
		t.Fatalf("mark unknown: %v", err)
	}
	// an unknown intent cannot be completed via UpdateIntent.
	if err := func() error {
		_, e := store.UpdateIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
			"tool", "call-crash", state.IntentSucceeded, nil)
		return e
	}(); err == nil {
		t.Fatal("unknown cannot complete via UpdateIntent")
	}
	// only the privileged ReconcileIntent resolves it, with the exact digest.
	crashed, _ := store.GetIntent(ctx, e2eScope, fence.RunID, "tool", "call-crash")
	if _, err := store.ReconcileIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"tool", "call-crash", state.IntentSucceeded, crashed.Digest, json.RawMessage(`{"confirmed":true}`)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	final, _ := store.GetIntent(ctx, e2eScope, fence.RunID, "tool", "call-crash")
	if final.Status != state.IntentSucceeded {
		t.Errorf("reconciled status = %q, want succeeded", final.Status)
	}
}

// opJournal mirrors the SDK's durable operation mapping journalled in the
// intent `result` (the real Intent struct has no args field). Root's scenario:
// PutKV committed, ack lost, process restarted with the SAME node/call. The
// re-entry must read the journalled ORIGINAL (op_key, expected_version, value)
// and replay it, NOT re-derive the version from a freshly-read live item.
type opJournal struct {
	OpKey           string          `json:"op_key"`
	ExpectedVersion int64           `json:"expected_version"`
	Value           json.RawMessage `json:"value"`
}

// TestE2EDurableOpMapping verifies, against a REAL Postgres (internal/state.Store),
// the server-side guarantee that makes the SDK's lost-ack journal correct (Root
// 20261030T2103Z / SDK-R6-2): the receipt replay is keyed on (scope, operation_key)
// and runs BEFORE the version CAS, so re-sending the ORIGINAL (op_key,
// expected_version) after the live version has advanced returns the stored
// receipt (no double write). Re-deriving expected_version from the live item
// instead yields a different request digest under the same key -> KEY_REUSE,
// proving re-derivation is unsafe and why the journal persists the original.
func TestE2EDurableOpMapping(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	fence := mustFence(t, store, "durmap")
	ns := []string{"dur", "ns"}
	const opKey = "op-restart-1"

	// Journal the original mapping into the intent BEFORE the write, exactly as
	// _durable_write does (record -> dispatch with result = {op_key, expected, value}).
	if _, err := store.RecordIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"agent-node", "tc-restart", "kv", "dur/ns/k", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	journal, _ := json.Marshal(opJournal{OpKey: opKey, ExpectedVersion: 0, Value: json.RawMessage(`"v1"`)})
	if _, err := store.UpdateIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"agent-node", "tc-restart", state.IntentDispatched, journal); err != nil {
		t.Fatalf("dispatch(journal): %v", err)
	}

	// Commit the write (version 1) and then "crash" before completion.
	committed, err := store.PutKV(ctx, e2eScope, fence, ns, "k", opKey, 0, json.RawMessage(`"v1"`))
	if err != nil {
		t.Fatalf("PutKV commit: %v", err)
	}
	if committed.Version != 1 {
		t.Fatalf("committed version = %d, want 1", committed.Version)
	}

	// RESTART: re-read the journalled mapping from the intent (not the live item).
	got, err := store.GetIntent(ctx, e2eScope, fence.RunID, "agent-node", "tc-restart")
	if err != nil {
		t.Fatalf("GetIntent: %v", err)
	}
	var j opJournal
	if err := json.Unmarshal(got.Result, &j); err != nil {
		t.Fatalf("unmarshal journal: %v (result=%s)", err, got.Result)
	}
	if j.OpKey != opKey || j.ExpectedVersion != 0 {
		t.Fatalf("journal round-trip = %+v, want op_key=%s expected=0", j, opKey)
	}

	// Replay the ORIGINAL (op_key, expected_version, value): receipt short-circuits
	// the version CAS -> version stays 1, no double write.
	replay, err := store.PutKV(ctx, e2eScope, fence, ns, "k", j.OpKey, j.ExpectedVersion, j.Value)
	if err != nil {
		t.Fatalf("replay PutKV: %v", err)
	}
	if replay.Version != 1 {
		t.Fatalf("replay version = %d, want 1 (receipt replay; a double write would be 2)", replay.Version)
	}

	// Counterfactual: a re-derived expected_version (read live version = 1) reused
	// under the SAME op_key is a different request -> KEY_REUSE, so the buggy path
	// cannot silently double-write either. This is why the journal stores the
	// ORIGINAL expected_version instead of re-deriving it.
	if _, err := store.PutKV(ctx, e2eScope, fence, ns, "k", opKey, 1, json.RawMessage(`"v1"`)); err == nil {
		t.Fatal("re-derived expected under same op key must not silently succeed")
	} else if contracts.Code(err) != contracts.ErrKeyReuse {
		t.Errorf("re-derived code = %s, want KEY_REUSE_MISMATCH", contracts.Code(err))
	}

	// Completing after replay finalizes the intent (SDK succeed path).
	if _, err := store.UpdateIntent(ctx, e2eScope, fence.RunID, fence.Worker, fence.Epoch,
		"agent-node", "tc-restart", state.IntentSucceeded, json.RawMessage(`{"op_key":"op-restart-1"}`)); err != nil {
		t.Fatalf("succeed after replay: %v", err)
	}
	final, _ := store.GetIntent(ctx, e2eScope, fence.RunID, "agent-node", "tc-restart")
	if final.Status != state.IntentSucceeded {
		t.Errorf("final status = %q, want succeeded", final.Status)
	}
}
