package memory

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func TestMemoryMigrationRefusesLegacyDeploymentKeyCollisions(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	for i, subject := range []string{"alice", "bob"} {
		_, err := e.mem.Propose(ctx, e.scope(subject), ProposeRequest{Kind: KindUser, Text: subject, OpKey: []string{"first", "second"}[i]})
		mustOK(t, err)
	}
	// Reproduce the previous schema with memory rows but no operation backfill.
	_, err := e.pool.Exec(ctx, `DELETE FROM ahp_memory.operations;
DROP INDEX ahp_memory.operations_deployment_key;
ALTER TABLE ahp_memory.operations DROP COLUMN status;
UPDATE ahp_memory.memories SET op_key='legacy-collision';`)
	mustOK(t, err)
	if err := e.mem.Migrate(ctx); err == nil {
		t.Fatal("migration silently discarded a historical receipt with the same deployment key")
	}
	var records int
	mustOK(t, e.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_memory.memories WHERE tenant=$1`, e.tenant).Scan(&records))
	if records != 2 {
		t.Fatalf("failed upgrade modified memory data: %d", records)
	}
}

func TestFenceDuringProposalEmbeddingPreventsCommit(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	scope := e.scope("alice")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	e.mem.embedder = func(ctx context.Context, text string) ([]float32, error) {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return embedText(ctx, text)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: "cancel before database write", OpKey: "in-flight"})
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	v, err := e.mem.Fence(ctx, scope, "in-flight")
	close(release)
	mustOK(t, err)
	if v.Status != "FENCED" {
		t.Fatalf("fence while embedding: %+v", v)
	}
	if err := <-done; contracts.Code(err) != contracts.ErrConflict || contracts.ReasonOf(err) != "FENCED" {
		t.Fatalf("in-flight proposal after fence: %v", err)
	}
	var records int
	mustOK(t, e.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_memory.memories WHERE tenant=$1`, e.tenant).Scan(&records))
	if records != 0 {
		t.Fatalf("fenced proposal committed %d records", records)
	}
}

func TestFenceWaitsForCommittingMemoryMutation(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	scope := e.scope("alice")
	p, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: "one committed narrowing", OpKey: "proposal"})
	mustOK(t, err)
	blocker, err := e.pool.Begin(ctx)
	mustOK(t, err)
	defer blocker.Rollback(ctx)
	_, err = blocker.Exec(ctx, `SELECT id FROM ahp_memory.memories WHERE id=$1 FOR UPDATE`, p.Memory.ID)
	mustOK(t, err)
	mutation := make(chan error, 1)
	go func() {
		_, err := e.mem.AddLabels(ctx, scope, p.Memory.ID, "narrow", labels.Set{Taints: []string{labels.TaintExternal}})
		mutation <- err
	}()
	waitLocks := func(want int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			var n int
			mustOK(t, e.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid<>pg_backend_pid() AND query LIKE '%ahp_memory.%'`).Scan(&n))
			if n >= want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("wanted %d real database lock waiters, got %d", want, n)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitLocks(1) // Actual mutation reserved its key, then waits on the record.
	type result struct {
		op  Operation
		err error
	}
	fenced := make(chan result, 1)
	go func() { op, err := e.mem.Fence(ctx, scope, "narrow"); fenced <- result{op, err} }()
	waitLocks(2) // Fence actually contends with the uncommitted reservation.
	mustOK(t, blocker.Commit(ctx))
	mustOK(t, <-mutation)
	f := <-fenced
	mustOK(t, f.err)
	if f.op.Status != "SUCCEEDED" || f.op.ResultID != p.Memory.ID {
		t.Fatalf("fence lost the committed outcome: %+v", f.op)
	}
	got, err := e.mem.Get(ctx, scope, p.Memory.ID)
	mustOK(t, err)
	if len(got.Labels.Taints) != 1 || got.Labels.Taints[0] != labels.TaintExternal {
		t.Fatalf("narrowing not preserved: %v", got.Labels.Taints)
	}
}

func TestOperationReceiptDoesNotLeakRevokedWorkspaceReference(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	scope := e.scope("alice")
	e.addWorkspace("alice", "shared")
	p, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindWorkspace, Workspace: "shared", Text: "shared note", OpKey: "shared", Evidence: []Evidence{{SourceKind: "user_statement"}}})
	mustOK(t, err)
	e.members["alice"] = nil
	for _, f := range []func(context.Context, contracts.Scope, string) (Operation, error){e.mem.GetOperation, e.mem.Fence} {
		v, err := f(ctx, scope, "shared")
		mustOK(t, err)
		if v.Status != "SUCCEEDED" || v.ResultID != "" {
			t.Fatalf("revoked receipt leaked object %s: %+v", p.Memory.ID, v)
		}
	}
}
