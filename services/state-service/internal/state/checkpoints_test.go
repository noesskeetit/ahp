package state

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"
	_ "unsafe"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

type cpEnv struct {
	s        *Store
	ctx      context.Context
	scope    contracts.Scope
	fence    Fence
	threadID string
}

func cpFixture(t *testing.T) *cpEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	// Per-run database (isolation_test.go): see the note in kv_test.go.
	pool, _ := isolatedPool(t)
	s := New(pool)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := s.MigrateCheckpoints(ctx); err != nil {
		t.Fatalf("MigrateCheckpoints: %v", err)
	}
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
	return &cpEnv{s: s, ctx: ctx, scope: scope, threadID: th.ID,
		fence: Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}}
}

func cp(id string, parts ...Part) Checkpoint {
	return Checkpoint{ID: id, Namespace: "ns", Version: 1, Parts: parts}
}

func TestCheckpointOpaqueBytesRoundtrip(t *testing.T) {
	e := cpFixture(t)
	blob := []byte{0x00, 'o', 'p', 'a', 'q', 'u', 'e', 0xff, 0x00, '\n', 0x1b, 0xff}
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence,
		cp("cp-ob-1", Part{Channel: "ch", Version: "000-v1", Data: blob}), "k1"); err != nil {
		t.Fatalf("PutCheckpoint: %v", err)
	}
	threadID := e.threadID
	got, err := e.s.GetPart(e.ctx, e.scope, threadID, "ns", "ch", "000-v1")
	if err != nil {
		t.Fatalf("GetPart: %v", err)
	}
	if !bytes.Equal(got, blob) {
		t.Fatalf("opaque bytes not preserved: want %v, got %v", blob, got)
	}
	// Empty bytes must roundtrip as the empty string, not nil-vs-data corruption.
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence,
		cp("cp-ob-2", Part{Channel: "ch", Version: "v-empty", Data: []byte{}}), "k2"); err != nil {
		t.Fatalf("PutCheckpoint empty blob: %v", err)
	}
	got, err = e.s.GetPart(e.ctx, e.scope, threadID, "ns", "ch", "v-empty")
	if err != nil {
		t.Fatalf("GetPart empty blob: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty blob: want 0 bytes, got %v", got)
	}
}

func TestCheckpointPartVersionReuseAndConflict(t *testing.T) {
	e := cpFixture(t)
	blob := []byte("payload-v1")
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-rv-1", Part{Channel: "ch", Version: "v-1", Data: blob}), "k1"); err != nil {
		t.Fatalf("PutCheckpoint 1: %v", err)
	}
	// nil Data references the existing blob of (channel, "v-1").
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-rv-2", Part{Channel: "ch", Version: "v-1"}), "k2"); err != nil {
		t.Fatalf("version reuse via nil Data: %v", err)
	}
	// Same version, different bytes must be denied.
	_, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-rv-3", Part{Channel: "ch", Version: "v-1", Data: []byte("other")}), "k3")
	if code := contracts.Code(err); code != contracts.ErrKeyReuse {
		t.Fatalf("want %s, got %v (code %s)", contracts.ErrKeyReuse, err, code)
	}
}

func TestCheckpointKeyAndIDReplayMismatch(t *testing.T) {
	e := cpFixture(t)
	first, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-km-1", Part{Channel: "ch", Version: "v-1", Data: []byte("a")}), "k1")
	if err != nil {
		t.Fatalf("PutCheckpoint: %v", err)
	}
	replay, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-km-1", Part{Channel: "ch", Version: "v-1", Data: []byte("a")}), "k1")
	if err != nil || replay.ID != first.ID {
		t.Fatalf("identical replay: %+v, %v", replay, err)
	}
	// Same key, different body.
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-km-2", Part{Channel: "ch", Version: "v-2", Data: []byte("b")}), "k1"); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("key replay mismatch: want %s, got %v", contracts.ErrKeyReuse, err)
	}
	// Same id under a fresh key with a different body.
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-km-1", Part{Channel: "ch", Version: "v-9", Data: []byte("z")}), "k9"); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("id replay mismatch: want %s, got %v", contracts.ErrConflict, err)
	}
}

func TestCheckpointForeignParentRejected(t *testing.T) {
	e := cpFixture(t)
	other := cpFixture(t)
	if _, err := other.s.PutCheckpoint(other.ctx, other.scope, other.fence, cp("cp-fp-own"), "k1"); err != nil {
		t.Fatalf("foreign PutCheckpoint: %v", err)
	}
	// A missing parent in the leased namespace is NOT_FOUND.
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, Checkpoint{ID: "cp-fp-2", Namespace: "ns", Version: 1, Parent: "cp-missing"}, "k2"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("missing parent: want %s, got %v", contracts.ErrNotFound, err)
	}
	// A checkpoint id that exists only in a foreign thread is no valid parent.
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, Checkpoint{ID: "cp-fp-3", Namespace: "ns", Version: 1, Parent: "cp-fp-own"}, "k3"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign parent: want %s, got %v", contracts.ErrNotFound, err)
	}
}

func TestCheckpointNegativePendingIndexStoredEmpty(t *testing.T) {
	e := cpFixture(t)
	created, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-nw-1"), "k1")
	if err != nil {
		t.Fatalf("PutCheckpoint: %v", err)
	}
	writes := []PendingWrite{
		{Task: "t1", Index: -3, Channel: "ch", Data: nil},
		{Task: "t1", Index: 1, Channel: "ch2", Data: []byte("x")},
	}
	if err := e.s.PutWrites(e.ctx, e.scope, e.fence, "ns", created.ID, writes); err != nil {
		t.Fatalf("PutWrites negative index: %v", err)
	}
	if err := e.s.PutWrites(e.ctx, e.scope, e.fence, "ns", created.ID, writes); err != nil {
		t.Fatalf("PutWrites replay: %v", err)
	}
	got, err := e.s.GetWrites(e.ctx, e.scope, created.ThreadID, "ns", created.ID)
	if err != nil || len(got) != 2 {
		t.Fatalf("GetWrites: %+v, %v", got, err)
	}
	if got[0].Index != -3 || len(got[0].Data) != 0 {
		t.Fatalf("negative index write: %+v", got[0])
	}
	if got[1].Index != 1 || string(got[1].Data) != "x" {
		t.Fatalf("positive index write: %+v", got[1])
	}
}

func TestCheckpointStaleEpochAfterExpiryReclaim(t *testing.T) {
	e := cpFixture(t)
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-se-1"), "k1"); err != nil {
		t.Fatalf("PutCheckpoint under live lease: %v", err)
	}
	// Force expiry in database time, then reclaim through the regular claim
	// path: the old fence must be rejected, the new one accepted.
	if _, err := e.s.Pool().Exec(e.ctx,
		`UPDATE ahp_state.threads SET lease_expires = clock_timestamp() - interval '1 second'
		  WHERE id = (SELECT thread_id FROM ahp_state.runs WHERE id = $1)`, e.fence.RunID); err != nil {
		t.Fatalf("force expiry: %v", err)
	}
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-se-2"), "k2"); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("expired fence: want %s, got %v", contracts.ErrStaleEpoch, err)
	}
	// The expired run must be requeued before it can be reclaimed.
	if _, err := e.s.Pool().Exec(e.ctx,
		`UPDATE ahp_state.runs SET status = 'pending' WHERE id = $1`, e.fence.RunID); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	lease, err := e.s.ClaimRun(e.ctx, e.scope.Tenant, e.scope.Deployment, "w2", time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("reclaim: %v", err)
	}
	if lease.Epoch <= e.fence.Epoch {
		t.Fatalf("reclaim epoch %d not above old %d", lease.Epoch, e.fence.Epoch)
	}
	newFence := Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, newFence, cp("cp-se-3"), "k3"); err != nil {
		t.Fatalf("PutCheckpoint under new fence: %v", err)
	}
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-se-4"), "k4"); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("old fence after reclaim: want %s, got %v", contracts.ErrStaleEpoch, err)
	}
}

func TestCheckpointForeignSubjectNotFound(t *testing.T) {
	e := cpFixture(t)
	created, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-fs-1", Part{Channel: "ch", Version: "v-1", Data: []byte("s")}), "k1")
	if err != nil {
		t.Fatalf("PutCheckpoint: %v", err)
	}
	foreign := contracts.Scope{Tenant: e.scope.Tenant, Deployment: e.scope.Deployment, Subject: "subj-" + contracts.NewID()}
	if _, err := e.s.GetCheckpoint(e.ctx, foreign, created.ThreadID, "ns", "cp-fs-1"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign GetCheckpoint: want %s, got %v", contracts.ErrNotFound, err)
	}
	if _, err := e.s.ListCheckpoints(e.ctx, foreign, created.ThreadID, "ns", 10); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign ListCheckpoints: want %s, got %v", contracts.ErrNotFound, err)
	}
	if _, err := e.s.GetPart(e.ctx, foreign, created.ThreadID, "ns", "ch", "v-1"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign GetPart: want %s, got %v", contracts.ErrNotFound, err)
	}
	if _, err := e.s.GetWrites(e.ctx, foreign, created.ThreadID, "ns", created.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign GetWrites: want %s, got %v", contracts.ErrNotFound, err)
	}
}

func TestCheckpointListReturnsSnapshots(t *testing.T) {
	e := cpFixture(t)
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-ls-1", Part{Channel: "a", Version: "v-1", Data: []byte("1")}), "k1"); err != nil {
		t.Fatalf("PutCheckpoint 1: %v", err)
	}
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, Checkpoint{ID: "cp-ls-2", Namespace: "ns", Version: 2, Parent: "cp-ls-1",
		Parts: []Part{{Channel: "a", Version: "v-1"}, {Channel: "b", Version: "v-2", Data: []byte("2")}}}, "k2"); err != nil {
		t.Fatalf("PutCheckpoint 2: %v", err)
	}
	threadID := e.threadID
	list, err := e.s.ListCheckpoints(e.ctx, e.scope, threadID, "ns", 10)
	if err != nil || len(list) != 2 {
		t.Fatalf("ListCheckpoints: %+v, %v", list, err)
	}
	if list[0].ID != "cp-ls-2" || list[1].ID != "cp-ls-1" {
		t.Fatalf("not newest first: %+v", list)
	}
	if list[0].Parent != "cp-ls-1" || list[0].Version != 2 || list[0].ThreadID != threadID {
		t.Fatalf("bad snapshot header: %+v", list[0])
	}
	if len(list[0].Parts) != 2 || list[0].Parts[0].Channel != "a" || list[0].Parts[0].Version != "v-1" {
		t.Fatalf("bad snapshot parts: %+v", list[0].Parts)
	}
	if len(list[1].Parts) != 1 || list[1].Parts[0].Data != nil {
		t.Fatalf("bad second snapshot: %+v", list[1])
	}
}
