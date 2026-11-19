package state

// Disaster recovery by the product's own door (matrix row 15, PDF section 9.8
// "driver change: export and import of scopes through the State API").
//
// The physical copy in internal/artifacts/checkpoint_dr_test.go restores a
// whole database. This leg restores ONE identity through the API the product
// exposes for the purpose, into a database that has never held the thread, on a
// fresh migration. That is the path a restore takes when the cluster itself is
// gone, and it is also the path a driver change takes, so the byte-level
// fidelity of checkpoints and parts is the claim under test.
//
// The negative half is just as important: an export that carries an artifact
// descriptor cannot be poured into a different context by whoever happens to
// hold the export file. The reference has to be rehomed by its owner first, or
// the restore would launder a reference into a scope that never earned it.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// drCreateDatabase provisions one isolated database for this run and drops it
// afterwards. CREATE DATABASE takes no bind parameters, so the name is built
// from characters that are safe to splice.
func drCreateDatabase(t *testing.T, maintenance *pgxpool.Pool, suffix string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Kept well under PostgreSQL's 63-byte identifier limit: a truncated name
	// would silently collide with another run.
	name := "ahp_stdr_" + isolateSafeID(contracts.NewID())[:24] + suffix
	if _, err := maintenance.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		t.Fatalf("could not provision an isolated database for this run")
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = maintenance.Exec(cleanup, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1`, name)
		if _, err := maintenance.Exec(cleanup, `DROP DATABASE IF EXISTS "`+name+`"`); err != nil {
			t.Logf("cleanup: could not drop %s", name)
		}
	})
	parsed, err := url.Parse(isolateDSN)
	if err != nil {
		t.Fatalf("parse the configured dsn: %v", err)
	}
	parsed.Path = "/" + name
	return parsed.String()
}

func drOpen(t *testing.T, ctx context.Context, dsn string) *Store {
	t.Helper()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	s := New(pool)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := s.MigrateCheckpoints(ctx); err != nil {
		t.Fatalf("MigrateCheckpoints: %v", err)
	}
	return s
}

func TestLogicalRestoreRebuildsTheThreadWithItsFiles(t *testing.T) {
	// Resolve this run's isolated database first: it is what provisions the
	// maintenance DSN the helper below needs, and it carries the established
	// skip-without-a-database semantics of this suite.
	_, _ = isolatedPool(t)
	if isolateDSN == "" {
		t.Skip("state: no configured database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// The maintenance connection outlives every drop: registered first, so it
	// is closed last, and the databases are gone before the pool goes away.
	maintenance, err := pgxpool.New(ctx, isolateDSN)
	if err != nil {
		t.Fatalf("maintenance pool: %v", err)
	}
	t.Cleanup(maintenance.Close)
	sourceDSN := drCreateDatabase(t, maintenance, "_src")
	targetDSN := drCreateDatabase(t, maintenance, "_dst")
	source := drOpen(t, ctx, sourceDSN)
	target := drOpen(t, ctx, targetDSN)

	scope := contracts.Scope{Tenant: "t-" + contracts.NewID(), Deployment: "d-" + contracts.NewID(),
		Subject: "subj-" + contracts.NewID()}
	thread, err := source.CreateThread(ctx, scope, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := source.CreateRun(ctx, scope, thread.ID, "task-1", "k-"+contracts.NewID(),
		json.RawMessage(`{}`)); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	lease, err := source.ClaimRun(ctx, scope.Tenant, scope.Deployment, "w1", time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	fence := Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}

	const steps = 4
	bodies := make([]string, steps)
	for i := 0; i < steps; i++ {
		bodies[i] = fmt.Sprintf("durable channel value %d", i)
		checkpoint := Checkpoint{ID: fmt.Sprintf("cp-%d", i), Namespace: "ns", Version: int64(i + 1),
			Parts: []Part{{Channel: "values", Version: fmt.Sprintf("v%d", i), Data: []byte(bodies[i])}}}
		if i > 0 {
			checkpoint.Parent = fmt.Sprintf("cp-%d", i-1)
		}
		if _, err := source.PutCheckpoint(ctx, scope, fence, checkpoint, "k-cp-"+fmt.Sprint(i)); err != nil {
			t.Fatalf("PutCheckpoint: %v", err)
		}
	}
	if _, err := source.AppendStream(ctx, scope, fence.RunID, fence.Worker, fence.Epoch,
		"k-msg-1", "transcript", json.RawMessage(`{"text":"first"}`)); err != nil {
		t.Fatalf("AppendStream: %v", err)
	}

	exported, err := source.ExportThread(ctx, scope, thread.ID)
	if err != nil {
		t.Fatalf("ExportThread: %v", err)
	}
	if len(exported.Checkpoints) != steps {
		t.Fatalf("export carried %d checkpoints, want %d", len(exported.Checkpoints), steps)
	}

	// The target database has never seen this identity: a fresh migration and
	// an explicit retarget context, nothing else.
	targetContext := "ctx-restored-" + contracts.NewID()
	imported, err := target.ImportThread(ctx, scope, exported, targetContext)
	if err != nil {
		t.Fatalf("ImportThread into a freshly migrated database: %v", err)
	}
	restored, err := target.ListCheckpoints(ctx, scope, imported.ID, "ns", 0)
	if err != nil {
		t.Fatalf("ListCheckpoints after restore: %v", err)
	}
	if len(restored) != steps {
		t.Fatalf("restored history = %d checkpoints, want %d", len(restored), steps)
	}
	for i := 0; i < steps; i++ {
		got, err := target.GetPart(ctx, scope, imported.ID, "ns", "values", fmt.Sprintf("v%d", i))
		if err != nil || string(got) != bodies[i] {
			t.Fatalf("part v%d did not survive the logical restore: %q err=%v", i, got, err)
		}
	}
	// The chain is walkable again, in the restored database.
	head := restored[0]
	for head.Parent != "" {
		parent, err := target.GetCheckpoint(ctx, scope, imported.ID, "ns", head.Parent)
		if err != nil {
			t.Fatalf("restored chain is not walkable at %s: %v", head.Parent, err)
		}
		head = parent
	}
	messages, err := target.ReadStream(ctx, scope, imported.ID, 0, 100)
	if err != nil {
		t.Fatalf("ReadStream after restore: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("restored stream holds %d entries, want 1", len(messages))
	}

	// The reference half of the same door refuses to launder: an export naming
	// an artifact descriptor cannot be poured into a different context.
	guarded := drOpen(t, ctx, sourceDSN)
	guarded.SetCheckpointArtifacts(&recordingGuard{})
	descriptor, err := json.Marshal(references.CheckpointArtifact{
		ID: "art-" + contracts.NewID(), Tenant: scope.Tenant, Deployment: scope.Deployment,
		Subject: scope.Subject, ContextID: thread.ContextID, TaskID: "task-1", Version: "1",
		SHA256: "sha256:" + contracts.NewID(), Size: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := guarded.PutCheckpoint(ctx, scope, fence, Checkpoint{
		ID: "cp-ref", Namespace: "ns", Version: 10, Parent: "cp-3",
		Parts: []Part{{Channel: "artifact", Version: "artifact-1",
			Encoding: CheckpointArtifactEncoding, Data: descriptor}},
	}, "k-cp-ref"); err != nil {
		t.Fatalf("descriptor checkpoint: %v", err)
	}
	withReference, err := guarded.ExportThread(ctx, scope, thread.ID)
	if err != nil {
		t.Fatalf("ExportThread with descriptor: %v", err)
	}
	if _, err := target.ImportThread(ctx, scope, withReference, "ctx-not-earned-"+contracts.NewID()); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("a reference was restored into a context that did not earn it: %v", err)
	}
	// The refusal left nothing behind: the failed import created no thread.
	var threads int
	if err := target.Pool().QueryRow(ctx, `SELECT count(*) FROM ahp_state.threads WHERE tenant = $1`,
		scope.Tenant).Scan(&threads); err != nil {
		t.Fatalf("count threads after the refused import: %v", err)
	}
	if threads != 1 {
		t.Fatalf("the refused import left %d threads behind, want only the earlier successful one", threads)
	}
}
