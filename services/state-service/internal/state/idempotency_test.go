package state

// These tests pin the idempotency semantics that the SDK caller-alignment
// snapshot (verify/sdk-c8ab894) assumed, so the reconciliation of that snapshot
// into this lane is a tested contract rather than a code shape:
//
//   - a KV operation key replays inside exactly one scope
//     (tenant, deployment, subject, operation_key) and never across scopes;
//   - the same key with a different request digest fails with
//     KEY_REUSE_MISMATCH and changes nothing;
//   - nil bytes (PendingWrite.Data, an empty checkpoint part) materialize as
//     the EMPTY byte string, never as NULL: the columns are NOT NULL and the
//     wire must not invent a second representation of "no bytes".

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestKVOperationKeyReplaysOnlyInsideItsScope proves the replay is scoped by
// (tenant, deployment, subject, operation_key): the same key under another
// subject is a different request and must be executed, not replayed.
func TestKVOperationKeyReplaysOnlyInsideItsScope(t *testing.T) {
	e := kvFixture(t)
	ns := []string{"m2", "scope"}
	opKey := "op-" + contracts.NewID()

	first := kvPut(t, e, ns, "k1", opKey, 0, `{"v":1}`)
	if first.Version != 1 {
		t.Fatalf("first write: want version 1, got %+v", first)
	}
	replay := kvPut(t, e, ns, "k1", opKey, 0, `{"v":1}`)
	if replay.Version != first.Version || replay.Key != first.Key ||
		string(replay.Value) != string(first.Value) {
		t.Fatalf("in-scope replay must return the stored receipt, got %+v vs %+v", replay, first)
	}

	// A second subject inside the same tenant/deployment is a different scope.
	// Release this run first so the deployment queue holds only the new run.
	if err := e.s.Release(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch, RunDone); err != nil {
		t.Fatalf("Release: %v", err)
	}
	other := contracts.Scope{Tenant: e.scope.Tenant, Deployment: e.scope.Deployment,
		Subject: "subj-" + contracts.NewID()}
	th, err := e.s.CreateThread(e.ctx, other, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread(other subject): %v", err)
	}
	if _, err := e.s.CreateRun(e.ctx, other, th.ID, "task-2", "k-"+contracts.NewID(),
		json.RawMessage(`{"a":1}`)); err != nil {
		t.Fatalf("CreateRun(other subject): %v", err)
	}
	lease, err := e.s.ClaimRun(e.ctx, other.Tenant, other.Deployment, "w2", time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("ClaimRun(other subject): %v lease=%v", err, lease)
	}
	fence := Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}

	foreign, err := e.s.PutKV(e.ctx, other, fence, ns, "k1", opKey, 0, json.RawMessage(`{"v":1}`))
	if err != nil {
		t.Fatalf("same operation_key under another subject must not replay: %v", err)
	}
	if foreign.Version != 1 || foreign.ThreadID != th.ID {
		t.Fatalf("another subject wrote into the wrong receipt: %+v", foreign)
	}

	// The original scope still sees its own single-version item.
	unchanged, err := e.s.GetKV(e.ctx, e.scope, ns, "k1")
	if err != nil {
		t.Fatalf("GetKV(original scope): %v", err)
	}
	if unchanged.Version != 1 || unchanged.ThreadID == foreign.ThreadID {
		t.Fatalf("original scope receipt was disturbed: %+v", unchanged)
	}
}

// TestKVOperationKeyReuseWithDifferentDigestFails pins KEY_REUSE_MISMATCH for
// every shape of "same key, different request", and that a refused replay does
// not touch the stored item.
func TestKVOperationKeyReuseWithDifferentDigestFails(t *testing.T) {
	e := kvFixture(t)
	ns := []string{"m2", "reuse"}
	opKey := "op-" + contracts.NewID()
	created := kvPut(t, e, ns, "k1", opKey, 0, `{"v":1}`)

	cases := []struct {
		name string
		key  string
		exp  int64
		val  string
	}{
		{"different value", opKey, 0, `{"v":2}`},
		{"different expected version", opKey, 1, `{"v":1}`},
		{"same key, extra member in the value", opKey, 0, `{"v":1, "w":2}`},
	}
	for _, tc := range cases {
		if _, err := e.s.PutKV(e.ctx, e.scope, e.fence, ns, tc.key, opKey, tc.exp,
			json.RawMessage(tc.val)); contracts.Code(err) != contracts.ErrKeyReuse {
			t.Fatalf("%s: want KEY_REUSE_MISMATCH, got %v", tc.name, err)
		}
	}
	after, err := e.s.GetKV(e.ctx, e.scope, ns, "k1")
	if err != nil {
		t.Fatalf("GetKV after rejections: %v", err)
	}
	if !reflect.DeepEqual(after, created) {
		t.Fatalf("a refused replay changed the item: %+v != %+v", after, created)
	}
}

// TestKVOperationKeyIsTheIdempotencyUnitNotTheItem settles the question the
// SDK snapshot raised: the snapshot's kv.go carried (namespace, key) into the
// replay lookup, which would make one operation_key usable once per item. The
// pinned contract is narrower - the receipt is addressed by
// (tenant, deployment, subject, operation_key) ALONE - so reusing one
// operation key on another item of the same scope is a key reuse violation,
// while two distinct keys on one item are two ordinary requests.
func TestKVOperationKeyIsTheIdempotencyUnitNotTheItem(t *testing.T) {
	e := kvFixture(t)
	nsA := []string{"m2", "unit-a"}
	nsB := []string{"m2", "unit-b"}
	opKey := "op-" + contracts.NewID()

	first := kvPut(t, e, nsA, "k1", opKey, 0, `{"v":1}`)
	if first.Version != 1 {
		t.Fatalf("first write: %+v", first)
	}
	// Same scope, same operation key, another namespace AND another key: the
	// receipt already belongs to a different request body, so this is refused
	// instead of silently becoming a second write.
	if _, err := e.s.PutKV(e.ctx, e.scope, e.fence, nsB, "k2", opKey, 0,
		json.RawMessage(`{"v":1}`)); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("one operation key across items: want KEY_REUSE_MISMATCH, got %v", err)
	}
	// A delete under that same key aims at another item - refused as well. A
	// delete needs a positive expected version, and that argument check runs
	// BEFORE the receipt lookup, so both layers are pinned separately: the
	// shape error first, the key reuse once the request is well formed.
	if err := e.s.DeleteKV(e.ctx, e.scope, e.fence, nsB, "k2", opKey, 0); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("delete without an expected version: want INVALID_ARGUMENT, got %v", err)
	}
	if err := e.s.DeleteKV(e.ctx, e.scope, e.fence, nsB, "k2", opKey, 1); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("one operation key across items (delete): want KEY_REUSE_MISMATCH, got %v", err)
	}
	// Both refusals changed nothing: the first item is untouched and the second
	// item was never created.
	untouched, err := e.s.GetKV(e.ctx, e.scope, nsA, "k1")
	if err != nil {
		t.Fatalf("GetKV(first item): %v", err)
	}
	if !reflect.DeepEqual(untouched, first) {
		t.Fatalf("a refused replay disturbed the first item: %+v != %+v", untouched, first)
	}
	if _, err := e.s.GetKV(e.ctx, e.scope, nsB, "k2"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("the refused request must not create an item, got %v", err)
	}
	// The item itself is not the idempotency unit: a different operation key
	// writes the very same item again, under the normal CAS rules.
	second := kvPut(t, e, nsA, "k1", "op-"+contracts.NewID(), 1, `{"v":2}`)
	if second.Version != 2 {
		t.Fatalf("a distinct operation key must proceed: %+v", second)
	}
}

// TestDeleteKVReceiptReplayAndReuse pins the delete receipt: an identical
// replay is a no-op success, a different request under the same key is a key
// reuse violation.
func TestDeleteKVReceiptReplayAndReuse(t *testing.T) {
	e := kvFixture(t)
	ns := []string{"m2", "tombstone"}
	opKey := "op-del-" + contracts.NewID()
	kvPut(t, e, ns, "k1", "op-put-"+contracts.NewID(), 0, `{"v":1}`)

	del := func(op string, exp int64) error {
		return e.s.DeleteKV(e.ctx, e.scope, e.fence, ns, "k1", op, exp)
	}
	if err := del(opKey, 1); err != nil {
		t.Fatalf("DeleteKV: %v", err)
	}
	if _, err := e.s.GetKV(e.ctx, e.scope, ns, "k1"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("deleted key must read NOT_FOUND, got %v", err)
	}
	if err := del(opKey, 1); err != nil {
		t.Fatalf("identical delete replay must succeed as a no-op: %v", err)
	}
	if err := del(opKey, 7); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("delete replay with another digest: want KEY_REUSE_MISMATCH, got %v", err)
	}
}

// TestPendingWriteNilDataIsStoredAsEmptyBytes pins the wire meaning of a nil
// pending-write payload: the column is NOT NULL, so nil must arrive back as the
// empty byte slice (which JSON-encodes as ""), never as NULL.
func TestPendingWriteNilDataIsStoredAsEmptyBytes(t *testing.T) {
	e := cpFixture(t)
	if err := e.s.PutWrites(e.ctx, e.scope, e.fence, "ns", "cp-nil", []PendingWrite{
		{Task: "t1", Index: 0, Channel: "out", Data: nil},
		{Task: "t1", Index: 1, Channel: "out", Data: []byte{}},
		{Task: "t1", Index: 2, Channel: "out", Data: []byte("bytes")},
	}); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}
	got, err := e.s.GetWrites(e.ctx, e.scope, e.threadID, "ns", "cp-nil")
	if err != nil {
		t.Fatalf("GetWrites: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 writes, got %+v", got)
	}
	for i := 0; i < 2; i++ {
		if got[i].Data == nil {
			t.Fatalf("write %d: nil payload arrived back as NULL, want the empty byte slice", i)
		}
		if len(got[i].Data) != 0 {
			t.Fatalf("write %d: want empty payload, got %q", i, got[i].Data)
		}
	}
	if string(got[2].Data) != "bytes" {
		t.Fatalf("non-empty payload was disturbed: %q", got[2].Data)
	}
	// Replaying the identical batch is a no-op; changing the payload under the
	// same (checkpoint, task, index) is a key reuse violation.
	if err := e.s.PutWrites(e.ctx, e.scope, e.fence, "ns", "cp-nil", []PendingWrite{
		{Task: "t1", Index: 0, Channel: "out", Data: nil},
	}); err != nil {
		t.Fatalf("identical pending-write replay: %v", err)
	}
	if err := e.s.PutWrites(e.ctx, e.scope, e.fence, "ns", "cp-nil", []PendingWrite{
		{Task: "t1", Index: 0, Channel: "out", Data: []byte("different")},
	}); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("pending-write payload change: want KEY_REUSE_MISMATCH, got %v", err)
	}
}

// TestCheckpointEmptyAndReferencedParts pins both empty-byte forms of a part:
// an explicit empty payload stores an empty blob, and nil payload references
// the blob already stored under (channel, version).
func TestCheckpointEmptyAndReferencedParts(t *testing.T) {
	e := cpFixture(t)
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-a",
		Part{Channel: "empty", Version: "v1", Data: []byte{}},
		Part{Channel: "values", Version: "v1", Data: []byte("payload")}), "key-a"); err != nil {
		t.Fatalf("PutCheckpoint: %v", err)
	}
	empty, err := e.s.GetPart(e.ctx, e.scope, e.threadID, "ns", "empty", "v1")
	if err != nil {
		t.Fatalf("GetPart(empty): %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("an empty part must read back as the empty byte slice, got %#v", empty)
	}
	// A nil payload is a reference: the second checkpoint stores no new blob and
	// still resolves the original bytes.
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, Checkpoint{
		ID: "cp-b", Namespace: "ns", Version: 2, Parent: "cp-a",
		Parts: []Part{{Channel: "values", Version: "v1", Data: nil}},
	}, "key-b"); err != nil {
		t.Fatalf("PutCheckpoint(reference): %v", err)
	}
	referenced, err := e.s.GetPart(e.ctx, e.scope, e.threadID, "ns", "values", "v1")
	if err != nil {
		t.Fatalf("GetPart(referenced): %v", err)
	}
	if string(referenced) != "payload" {
		t.Fatalf("part reference resolved the wrong bytes: %q", referenced)
	}
	// A reference to a version that was never stored is NOT_FOUND, not a 500.
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, Checkpoint{
		ID: "cp-c", Namespace: "ns", Version: 3,
		Parts: []Part{{Channel: "values", Version: "v-missing", Data: nil}},
	}, "key-c"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("reference to a missing blob: want NOT_FOUND, got %v", err)
	}
}
