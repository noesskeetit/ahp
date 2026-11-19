package artifacts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestCheckpointGCFreshReferenceAfterBatchScan(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	st := state.New(e.store.pool)
	st.SetCheckpointArtifacts(e.store)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	thread, err := st.CreateThread(ctx, e.scope, "ctx-"+contracts.NewID(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.CreateRun(ctx, e.scope, thread.ID, "task", "run", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := st.ClaimRun(ctx, e.scope.Tenant, e.scope.Deployment, "worker", time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("lease: %v", err)
	}
	ref, upload := e.sealBoundWithKey(t, Binding{ContextID: thread.ContextID, TaskID: "task"}, "checkpoint", "durable-checkpoint-bytes")
	raw := checkpointDescriptor(ref)
	cp := state.Checkpoint{ID: "checkpoint", Version: 1, Metadata: raw, MetadataEncoding: state.CheckpointArtifactEncoding}
	fence := state.Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
	if err := e.store.Delete(ctx, e.scope, ref.ID, "delete-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.pool.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET deleted_at=now()-interval '1 hour' WHERE id=$1`, ref.ID); err != nil {
		t.Fatal(err)
	}
	source := state.ReferenceSource(e.store.pool, "ahp_state")
	first := true
	scan := ReferenceScanFunc(func(ctx context.Context, tenant string, arts []ArtifactQuery) (map[string]bool, error) {
		queries := make([]references.Artifact, len(arts))
		for i, a := range arts {
			queries[i] = references.Artifact{ID: a.ID, Digest: a.Digest}
		}
		result, err := source.InspectReferences(ctx, tenant, nil, queries)
		if err != nil {
			return nil, err
		}
		if first {
			first = false
			// The batch already observed no references. A restore/write/delete
			// completes before its stale result reaches collectOne.
			if _, err := e.store.Restore(ctx, e.scope, ref.ID); err != nil {
				return nil, err
			}
			if _, err := st.PutCheckpoint(ctx, e.scope, fence, cp, "checkpoint"); err != nil {
				return nil, err
			}
			// Simulate a tombstone made by an old/hostile writer. The current
			// Delete API must now reject this committed-reference state.
			if _, err := e.store.pool.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET deleted=true,deleted_at=now() WHERE id=$1`, ref.ID); err != nil {
				return nil, err
			}
		}
		return result.Referenced, nil
	})
	e.store.keep = scan
	e.store.checkpointSettlement = st.CheckpointOperationSettled
	gc, _ := NewGC(e.store, WithReferenceScan(scan))
	report, err := gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := e.rawGet(t, upload.Object, ref.Version)
	if err != nil || string(bytes) != "durable-checkpoint-bytes" || report.PurgedTombstoned != 0 {
		t.Fatalf("committed checkpoint lost after stale scan: report=%+v bytes=%q err=%v", report, bytes, err)
	}
}

func checkpointDescriptor(ref ArtifactRef) []byte {
	raw, _ := json.Marshal(references.CheckpointArtifact{ID: ref.ID, Tenant: ref.Tenant, Deployment: ref.Deployment, Subject: ref.Subject, ContextID: ref.ContextID, TaskID: ref.TaskID, Version: ref.Version, SHA256: ref.SHA256, Size: ref.Size, Labels: ref.Labels})
	return raw
}

type checkpointTestEnv struct {
	*env
	state      *state.Store
	reader     *state.Store
	readPool   *pgxpool.Pool
	thread     state.Thread
	fence      state.Fence
	ref        ArtifactRef
	upload     Upload
	checkpoint state.Checkpoint
	gc         *GC
}

func newCheckpointTestEnv(t *testing.T) *checkpointTestEnv {
	t.Helper()
	e := setup(t)
	ctx := context.Background()
	clone := func(max int32) *pgxpool.Pool {
		cfg := e.store.pool.Config()
		cfg.MaxConns = max
		cfg.MinConns = 0
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	writes, reads, data := clone(2), clone(1), clone(1)
	st := state.New(writes)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	e.store.pool = data
	reader := state.New(reads)
	e.store.checkpointSettlement = reader.CheckpointOperationSettled
	st.SetCheckpointArtifacts(e.store)
	thread, err := st.CreateThread(ctx, e.scope, "ctx-"+contracts.NewID(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.CreateRun(ctx, e.scope, thread.ID, "task", "run", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := st.ClaimRun(ctx, e.scope.Tenant, e.scope.Deployment, "worker", time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("claim: %v", err)
	}
	ref, upload := e.sealBoundWithKey(t, Binding{ContextID: thread.ContextID, TaskID: "task"}, "artifact", "checkpoint-durable-content")
	source := state.ReferenceSource(reads, "ahp_state")
	scan := ReferenceScanFunc(func(ctx context.Context, tenant string, arts []ArtifactQuery) (map[string]bool, error) {
		queries := make([]references.Artifact, len(arts))
		for i, a := range arts {
			queries[i] = references.Artifact{ID: a.ID, Digest: a.Digest}
		}
		result, err := source.InspectReferences(ctx, tenant, nil, queries)
		return result.Referenced, err
	})
	e.store.keep = scan
	gc, err := NewGC(e.store, WithReferenceScan(scan))
	if err != nil {
		t.Fatal(err)
	}
	return &checkpointTestEnv{env: e, state: st, reader: reader, readPool: reads, thread: thread, fence: state.Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}, ref: ref, upload: upload, checkpoint: state.Checkpoint{ID: "checkpoint", Version: 1, Metadata: checkpointDescriptor(ref), MetadataEncoding: state.CheckpointArtifactEncoding}, gc: gc}
}

func (e *checkpointTestEnv) tombstone(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	// GC must still defend legacy tombstones even though current Delete
	// refuses live State references and provisional publication pins.
	if _, err := e.readPool.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET deleted=true,deleted_at=now() WHERE id=$1`, e.ref.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.readPool.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET deleted_at=now()-interval '1 hour' WHERE id=$1`, e.ref.ID); err != nil {
		t.Fatal(err)
	}
}
func (e *checkpointTestEnv) pinCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := e.readPool.QueryRow(context.Background(), `SELECT count(*) FROM ahp_artifacts.checkpoint_pins WHERE tenant=$1`, e.scope.Tenant).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

type checkpointFaultGuard struct {
	*Store
	afterPin       func(context.Context, string) error
	beforePin      func(context.Context, string) error
	releaseFailure bool
}

func (g checkpointFaultGuard) PinCheckpointReferences(ctx context.Context, scope contracts.Scope, op string, refs []references.CheckpointArtifact) error {
	if g.beforePin != nil {
		if err := g.beforePin(ctx, op); err != nil {
			return err
		}
	}
	if err := g.Store.PinCheckpointReferences(ctx, scope, op, refs); err != nil {
		return err
	}
	if g.afterPin != nil {
		return g.afterPin(ctx, op)
	}
	return nil
}
func (g checkpointFaultGuard) ReleaseCheckpointReferences(ctx context.Context, op string) error {
	if g.releaseFailure {
		return errors.New("synthetic release transport unavailable")
	}
	return g.Store.ReleaseCheckpointReferences(ctx, op)
}

func TestCheckpointPinsCommitReleaseLossAndLegalHold(t *testing.T) {
	e := newCheckpointTestEnv(t)
	ctx := context.Background()
	e.state.SetCheckpointArtifacts(checkpointFaultGuard{Store: e.store, releaseFailure: true, afterPin: func(ctx context.Context, op string) error {
		settled, err := e.reader.CheckpointOperationSettled(ctx, op)
		if err != nil || settled {
			return fmt.Errorf("active transaction reported settled=%v err=%v", settled, err)
		}
		return nil
	}})
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, e.checkpoint, "cp"); err != nil {
		t.Fatal(err)
	}
	if n := e.pinCount(t); n != 1 {
		t.Fatalf("lost release must leave durable pin, got %d", n)
	}
	// Restarted owner has no process-local operation map. PostgreSQL proves
	// settlement; retained checkpoint references take over protection.
	e.store.checkpointSettlement = state.New(e.readPool).CheckpointOperationSettled
	e.tombstone(t)
	rep, err := e.gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil || rep.PurgedTombstoned != 0 || e.pinCount(t) != 0 {
		t.Fatalf("reconcile committed ref: %+v %v", rep, err)
	}
	if bytes, err := e.rawGet(t, e.upload.Object, e.ref.Version); err != nil || string(bytes) != "checkpoint-durable-content" {
		t.Fatalf("retained bytes %q %v", bytes, err)
	}
	if err := e.state.SetLegalhold(ctx, e.scope, e.thread.ID, "test hold"); err != nil {
		t.Fatal(err)
	}
	if err := e.state.DeleteThread(ctx, e.scope, e.thread.ID); err == nil {
		t.Fatal("legal hold deletion accepted")
	}
	if err := e.state.RemoveLegalhold(ctx, e.scope, e.thread.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.state.DeleteThread(ctx, e.scope, e.thread.ID); err != nil {
		t.Fatal(err)
	}
	rep, err = e.gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil || rep.PurgedTombstoned != 1 {
		t.Fatalf("GC after settled deletion: %+v %v", rep, err)
	}
	if _, err := e.rawGet(t, e.upload.Object, e.ref.Version); err == nil {
		t.Fatal("deleted State thread leaked permanent pin")
	}
}

func TestCheckpointPinsConnectionLossBeforeCommitReconciles(t *testing.T) {
	e := newCheckpointTestEnv(t)
	ctx := context.Background()
	e.state.SetCheckpointArtifacts(checkpointFaultGuard{Store: e.store, releaseFailure: true, afterPin: func(ctx context.Context, op string) error {
		var pid int32
		err := e.readPool.QueryRow(ctx, `SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND objsubid=1 AND classid::bigint=((hashtextextended($1,0)>>32)&4294967295) AND objid::bigint=(hashtextextended($1,0)&4294967295)`, "ahp_state:checkpoint-reference:"+op).Scan(&pid)
		if err != nil {
			return err
		}
		var killed bool
		if err := e.readPool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&killed); err != nil {
			return err
		}
		if !killed {
			return errors.New("failed to terminate test State transaction")
		}
		return nil
	}})
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, e.checkpoint, "cp"); err == nil {
		t.Fatal("killed State transaction reported commit")
	}
	if _, err := e.state.GetCheckpoint(ctx, e.scope, e.thread.ID, "", e.checkpoint.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("aborted transaction left checkpoint: %v", err)
	}
	if e.pinCount(t) != 1 {
		t.Fatal("unknown release did not retain pin")
	}
	e.tombstone(t)
	original := e.store.checkpointSettlement
	e.store.checkpointSettlement = func(context.Context, string) (bool, error) { return false, errors.New("synthetic State outage") }
	if _, err := e.gc.RunOnce(ctx, e.scope, fastPolicy()); err == nil {
		t.Fatal("unavailable settlement did not fail closed")
	}
	if _, err := e.rawGet(t, e.upload.Object, e.ref.Version); err != nil {
		t.Fatal("unknown outcome lost bytes")
	}
	e.store.checkpointSettlement = original
	rep, err := e.gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil || rep.PurgedTombstoned != 1 || e.pinCount(t) != 0 {
		t.Fatalf("aborted tx reconciliation: %+v %v", rep, err)
	}
}

func TestCheckpointPinsAbortAndRetry(t *testing.T) {
	e := newCheckpointTestEnv(t)
	ctx := context.Background()
	e.state.SetCheckpointArtifacts(checkpointFaultGuard{Store: e.store, afterPin: func(context.Context, string) error { return errors.New("synthetic lost pin ACK") }})
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, e.checkpoint, "same-key"); err == nil {
		t.Fatal("fault ignored")
	}
	if e.pinCount(t) != 0 {
		t.Fatal("known rollback leaked pins")
	}
	e.state.SetCheckpointArtifacts(e.store)
	for i := 0; i < 2; i++ {
		if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, e.checkpoint, "same-key"); err != nil {
			t.Fatalf("retry %d: %v", i, err)
		}
	}
	if e.pinCount(t) != 0 {
		t.Fatal("completed replay leaked pins")
	}
	list, err := e.state.ListCheckpoints(ctx, e.scope, e.thread.ID, "", 10)
	if err != nil || len(list) != 1 {
		t.Fatalf("replayed checkpoint duplicated: %d %v", len(list), err)
	}
}

func TestCheckpointPinsGCWinsAndInvalidReferences(t *testing.T) {
	e := newCheckpointTestEnv(t)
	ctx := context.Background()
	var expected references.CheckpointArtifact
	if err := json.Unmarshal(e.checkpoint.Metadata, &expected); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*references.CheckpointArtifact)
	}{
		{"version", func(r *references.CheckpointArtifact) { r.Version = "wrong" }},
		{"sha", func(r *references.CheckpointArtifact) { r.SHA256 = strings.Repeat("0", 64) }},
		{"size", func(r *references.CheckpointArtifact) { r.Size++ }},
		{"tenant", func(r *references.CheckpointArtifact) { r.Tenant = "foreign" }},
		{"subject", func(r *references.CheckpointArtifact) { r.Subject = "bob" }},
		{"binding", func(r *references.CheckpointArtifact) { r.ContextID = "foreign" }},
		{"task", func(r *references.CheckpointArtifact) { r.TaskID = "foreign" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := expected
			tc.mutate(&r)
			cp := e.checkpoint
			cp.ID = tc.name
			cp.Metadata, _ = json.Marshal(r)
			if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, cp, tc.name); err == nil {
				t.Fatal("invalid immutable descriptor accepted")
			}
		})
	}
	e.allow.Store(false)
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, e.checkpoint, "revoked"); err == nil {
		t.Fatal("revoked artifact authority accepted")
	}
	e.allow.Store(true)
	if _, err := e.state.PutCheckpoint(WithCallerContext(ctx, "foreign-context"), e.scope, e.fence, e.checkpoint, "context"); err == nil {
		t.Fatal("cross-context artifact accepted")
	}
	e.tombstone(t)
	rep, err := e.gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil || rep.PurgedTombstoned != 1 {
		t.Fatalf("GC first: %+v %v", rep, err)
	}
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, e.checkpoint, "gc-wins"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("broken ref after GC accepted: %v", err)
	}
	if e.pinCount(t) != 0 {
		t.Fatal("denied writes leaked pins")
	}
}

func TestCheckpointPinsBoundedPoolsProgressWithAllWritersOccupied(t *testing.T) {
	e := newCheckpointTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	second, err := e.state.CreateThread(ctx, e.scope, "second-context", "second-thread")
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.state.CreateRun(ctx, e.scope, second.ID, "second-task", "second-run", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := e.state.ClaimRun(ctx, e.scope.Tenant, e.scope.Deployment, "second-worker", time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("second lease: %v", err)
	}
	secondRef, _ := e.sealBoundWithKey(t, Binding{ContextID: second.ContextID, TaskID: "second-task"}, "second-artifact", "second-checkpoint")
	garbage, _ := e.sealBoundWithKey(t, Binding{ContextID: "dead-context", TaskID: "dead-task"}, "garbage", "garbage")
	if err := e.store.Delete(ctx, e.scope, garbage.ID, "delete-garbage"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.readPool.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET deleted_at=now()-interval '1 hour' WHERE id=$1`, garbage.ID); err != nil {
		t.Fatal(err)
	}
	reached := make(chan string, 2)
	proceed := make(chan struct{})
	e.state.SetCheckpointArtifacts(checkpointFaultGuard{Store: e.store, beforePin: func(ctx context.Context, op string) error {
		reached <- op
		select {
		case <-proceed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	errorsCh := make(chan error, 2)
	go func() { _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, e.checkpoint, "first"); errorsCh <- err }()
	go func() {
		_, err := e.state.PutCheckpoint(ctx, e.scope, state.Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}, state.Checkpoint{ID: "second", Version: 1, Metadata: checkpointDescriptor(secondRef), MetadataEncoding: state.CheckpointArtifactEncoding}, "second")
		errorsCh <- err
	}()
	for i := 0; i < 2; i++ {
		select {
		case op := <-reached:
			settled, err := e.reader.CheckpointOperationSettled(ctx, op)
			if err != nil || settled {
				t.Fatalf("occupied State writer proof: %v %v", settled, err)
			}
		case <-ctx.Done():
			t.Fatal("writers did not occupy both slots")
		}
	}
	if e.state.Pool().Stat().AcquiredConns() != 2 {
		t.Fatal("test did not exhaust State writer pool")
	}
	rep, err := e.gc.RunOnce(ctx, e.scope, fastPolicy())
	close(proceed)
	if err != nil || rep.PurgedTombstoned != 1 {
		t.Fatalf("GC waiting on exhausted writer pool: %+v %v", rep, err)
	}
	for i := 0; i < 2; i++ {
		if err := <-errorsCh; err != nil {
			t.Fatalf("writer stalled with bounded owner pools: %v", err)
		}
	}
	if e.pinCount(t) != 0 {
		t.Fatal("concurrent writes leaked pins")
	}
}

func TestCheckpointPinsProtectEveryWriterRoute(t *testing.T) {
	for _, route := range []string{"part", "pending", "batch", "import"} {
		t.Run(route, func(t *testing.T) {
			e := newCheckpointTestEnv(t)
			ctx := context.Background()
			raw := checkpointDescriptor(e.ref)
			cp := state.Checkpoint{ID: "parts", Version: 1, Parts: []state.Part{{Channel: "ch", Version: "v1", Data: raw, Encoding: state.CheckpointArtifactEncoding}}}
			write := state.PendingWrite{Task: "pending", Index: -1, Channel: "ch", Data: raw, Encoding: state.CheckpointArtifactEncoding}
			switch route {
			case "part":
				_, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, cp, "part")
				if err != nil {
					t.Fatal(err)
				}
			case "pending":
				if err := e.state.PutWrites(ctx, e.scope, e.fence, "", "ahead-of-header", []state.PendingWrite{write}); err != nil {
					t.Fatal(err)
				}
			case "batch":
				if _, err := e.state.PutBatch(ctx, e.scope, e.fence, state.BatchRequest{Key: "batch", Checkpoint: &cp, Writes: []state.PendingWrite{write}}); err != nil {
					t.Fatal(err)
				}
			case "import":
				if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, e.checkpoint, "source"); err != nil {
					t.Fatal(err)
				}
				exported, err := e.state.ExportThread(ctx, e.scope, e.thread.ID)
				if err != nil {
					t.Fatal(err)
				}
				exported = rehomeGCImport(t, e, exported)
				restored, err := e.state.ImportThread(ctx, e.scope, exported, "restored")
				if err != nil {
					t.Fatal(err)
				}
				if err := e.state.DeleteThread(ctx, e.scope, e.thread.ID); err != nil {
					t.Fatal(err)
				}
				e.thread = restored
			}
			if e.pinCount(t) != 0 {
				t.Fatal("known commit left pin")
			}
			e.tombstone(t)
			rep, err := e.gc.RunOnce(ctx, e.scope, fastPolicy())
			if err != nil || rep.PurgedTombstoned != 0 {
				t.Fatalf("retained route collected: %+v %v", rep, err)
			}
			if _, err := e.rawGet(t, e.upload.Object, e.ref.Version); err != nil {
				t.Fatal(err)
			}
			if err := e.state.DeleteThread(ctx, e.scope, e.thread.ID); err != nil {
				t.Fatal(err)
			}
			rep, err = e.gc.RunOnce(ctx, e.scope, fastPolicy())
			if err != nil || rep.PurgedTombstoned != 1 {
				t.Fatalf("route leaked after deletion: %+v %v", rep, err)
			}
		})
	}
}

func TestCheckpointPinReconcilerRotatesActiveBatch(t *testing.T) {
	e := newCheckpointTestEnv(t)
	ctx := context.Background()
	for i := 0; i < 257; i++ {
		if _, err := e.readPool.Exec(ctx, `INSERT INTO ahp_artifacts.checkpoint_pins(operation_id,tenant,artifact_id) VALUES($1,$2,$3)`, fmt.Sprintf("rotation-%03d", i), e.scope.Tenant, e.ref.ID); err != nil {
			t.Fatal(err)
		}
	}
	e.store.checkpointSettlement = func(_ context.Context, id string) (bool, error) { return id == "rotation-256", nil }
	if err := e.store.ReconcileCheckpointPins(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if e.pinCount(t) != 257 {
		t.Fatal("active first batch unexpectedly released")
	}
	if err := e.store.ReconcileCheckpointPins(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if e.pinCount(t) != 256 {
		t.Fatal("active operations starved abandoned pin after bounded batch")
	}
	e.store.checkpointSettlement = func(context.Context, string) (bool, error) { return true, nil }
	if err := e.store.ReconcileCheckpointPins(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointPinAttemptsAreNeverReused(t *testing.T) {
	e := newCheckpointTestEnv(t)
	ctx := context.Background()
	var operations []string
	guard := checkpointFaultGuard{Store: e.store, afterPin: func(_ context.Context, id string) error {
		operations = append(operations, id)
		if len(operations) == 1 {
			return errors.New("aborted first attempt")
		}
		return nil
	}}
	e.state.SetCheckpointArtifacts(guard)
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, e.checkpoint, "same-key"); err == nil {
		t.Fatal("first attempt should abort")
	}
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, e.checkpoint, "same-key"); err != nil {
		t.Fatal(err)
	}
	if len(operations) != 2 || operations[0] == operations[1] {
		t.Fatalf("physical transactions reused operation IDs: %#v", operations)
	}
}

func TestCheckpointReplayDoesNotRepublishTombstonedReference(t *testing.T) {
	for _, route := range []string{"checkpoint", "pending", "batch"} {
		t.Run(route, func(t *testing.T) {
			e := newCheckpointTestEnv(t)
			ctx := context.Background()
			invoke := func(key string) error {
				cp := e.checkpoint
				cp.ID = key
				switch route {
				case "checkpoint":
					_, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, cp, key)
					return err
				case "pending":
					return e.state.PutWrites(ctx, e.scope, e.fence, "", key, []state.PendingWrite{{Task: "pending", Channel: "ch", Data: cp.Metadata, Encoding: state.CheckpointArtifactEncoding}})
				default:
					_, err := e.state.PutBatch(ctx, e.scope, e.fence, state.BatchRequest{Key: key, Checkpoint: &cp, Writes: []state.PendingWrite{{Task: "pending", Channel: "ch", Data: cp.Metadata, Encoding: state.CheckpointArtifactEncoding}}})
					return err
				}
			}
			if err := invoke("same-key"); err != nil {
				t.Fatal(err)
			}
			e.tombstone(t)
			if err := invoke("same-key"); err != nil {
				t.Fatalf("committed replay tried to publish a new artifact reference: %v", err)
			}
			if err := invoke("new-key"); contracts.Code(err) != contracts.ErrNotFound {
				t.Fatalf("new write accepted unreadable artifact: %v", err)
			}
		})
	}
}

func TestCheckpointMixedReplayOnlyAdmitsNewReferences(t *testing.T) {
	for _, route := range []string{"pending", "batch"} {
		t.Run(route, func(t *testing.T) {
			e := newCheckpointTestEnv(t)
			ctx := context.Background()
			old := state.PendingWrite{Task: "pending", Index: 0, Channel: "ch", Data: e.checkpoint.Metadata, Encoding: state.CheckpointArtifactEncoding}
			invoke := func(writes []state.PendingWrite) error {
				if route == "pending" {
					return e.state.PutWrites(ctx, e.scope, e.fence, "", e.checkpoint.ID, writes)
				}
				_, err := e.state.PutBatch(ctx, e.scope, e.fence, state.BatchRequest{Key: "same-batch", Checkpoint: &e.checkpoint, Writes: writes})
				return err
			}
			if err := invoke([]state.PendingWrite{old}); err != nil {
				t.Fatal(err)
			}
			e.tombstone(t)
			fresh := state.PendingWrite{Task: "pending", Index: 1, Channel: "inline", Data: []byte(`"new-inline"`)}
			if err := invoke([]state.PendingWrite{old, fresh}); err != nil {
				t.Fatalf("new inline row caused replayed reference admission: %v", err)
			}
			// Inline work preceding a genuinely new invalid reference must roll
			// back with it, including the already-replayed checkpoint in PutBatch.
			inline := fresh
			inline.Index = 2
			broken := old
			broken.Index = 3
			if err := invoke([]state.PendingWrite{old, inline, broken}); contracts.Code(err) != contracts.ErrNotFound {
				t.Fatalf("new reference to tombstoned artifact was accepted: %v", err)
			}
			writes, err := e.state.GetWrites(ctx, e.scope, e.thread.ID, "", e.checkpoint.ID)
			if err != nil || len(writes) != 2 || writes[0].Index != 0 || writes[1].Index != 1 {
				t.Fatalf("mixed batch lost atomicity: %#v %v", writes, err)
			}
			if e.pinCount(t) != 0 {
				t.Fatal("mixed replay or known abort leaked a pin")
			}
		})
	}
}

func TestCheckpointNewHeaderMayReuseCommittedArtifactBlob(t *testing.T) {
	e := newCheckpointTestEnv(t)
	ctx := context.Background()
	cp := state.Checkpoint{ID: "first", Version: 1, Parts: []state.Part{{Channel: "ch", Version: "v1", Data: checkpointDescriptor(e.ref), Encoding: state.CheckpointArtifactEncoding}}}
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, cp, "first"); err != nil {
		t.Fatal(err)
	}
	e.tombstone(t)
	cp.ID = "second"
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, cp, "second"); err != nil {
		t.Fatalf("existing immutable blob caused new admission: %v", err)
	}
	cp.ID = "third"
	cp.Parts[0].Version = "v2"
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, cp, "third"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("new blob containing tombstoned reference accepted: %v", err)
	}
	if _, err := e.state.GetCheckpoint(ctx, e.scope, e.thread.ID, "", "third"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("failed blob admission committed a header: %v", err)
	}
}
