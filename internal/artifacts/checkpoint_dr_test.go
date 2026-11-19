package artifacts

// Disaster recovery for the State/Artifacts plane (matrix row 15, PDF section
// 4 guarantee 6 "durability", section 6.8, section 9.8).
//
// The claim is the one an operator actually needs: a database backup taken
// while the platform is running can be restored into a clean database, and the
// restored copy still MEANS what the source meant -
//
//   - the checkpoint history and the bytes of every part come back identical,
//   - the durable administrative journal (legal hold, and the reason recorded
//     with it) is still readable, and administrative work can continue and is
//     still journaled afterwards,
//   - the garbage collector on the restored copy still refuses to delete data
//     the restored State plane references, still honours a durable pin, and -
//     so that this is not a story about a collector that never deletes
//     anything - still collects what is genuinely dead.
//
// A restore that "passes" because the restored copy is inert proves nothing, so
// the dead-artifact leg is part of the same sweep as the protected legs.
//
// The snapshot is PostgreSQL's own physical copy (CREATE DATABASE ... TEMPLATE),
// which is what a base backup restores: a byte-level copy of the cluster's
// files for one database, not a re-run of the application's writes. It requires
// no sessions attached to the source, so every pool this fixture opened is
// closed first and the run's own pools are reopened against the copy.
// pg_dump/pg_restore are not present in the Linux check image, and reaching for
// a host binary would make the leg depend on the machine rather than the
// product; this is stated here so nobody reads the mechanism as stronger than
// it is.
//
// ISOLATION: the fixture builds its source database, its clone and its MinIO
// bucket per run and drops them afterwards. Nothing here touches the shared
// stand or any lane database, and the destructive sweep runs only inside the
// restored copy.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type drEnv struct {
	dsn         string
	maintenance *pgxpool.Pool
	client      *minio.Client
	bucket      string
	scope       contracts.Scope
	restored    string
	source      string
}

// drNames are lowercase and underscore-safe so they can be spliced into
// CREATE/DROP DATABASE, which takes no bind parameters.
func drNonce(t *testing.T) string {
	t.Helper()
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatal(err)
	}
	return isolateSafeID(hex.EncodeToString(rnd[:]))
}

func drDSN(t *testing.T, base, database string) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse configured dsn: %v", err)
	}
	u.Path = "/" + database
	return u.String()
}

// newDREnv builds an isolated source database holding BOTH planes, plus a
// per-run MinIO bucket. Every pool it opens is tracked so the snapshot step can
// close them all.
func newDREnv(t *testing.T) *drEnv {
	t.Helper()
	base := os.Getenv("AHP_TEST_DATABASE_URL")
	if base == "" {
		base = os.Getenv("AHP_DATABASE_URL")
	}
	raw := os.Getenv("AHP_S3_ENDPOINT")
	if base == "" || raw == "" || os.Getenv("AHP_MINIO_ROOT_USER") == "" ||
		os.Getenv("AHP_MINIO_ROOT_PASSWORD") == "" {
		t.Skip("backup/restore needs AHP_TEST_DATABASE_URL and MinIO credentials")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	nonce := drNonce(t)
	source := "ahp_h17dr_" + nonce
	restored := "ahp_h17dr_" + nonce + "_restored"
	logical := "ahp_h17dr_" + nonce + "_logical"

	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT: %v", err)
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""),
		Secure: u.Scheme == "https",
	})
	if err != nil {
		t.Fatalf("minio: %v", err)
	}
	e := &drEnv{dsn: base, client: client, bucket: "go-h17-dr-" + nonce, scope: contracts.Scope{
		Tenant: "t-dr-" + nonce, Deployment: "d-dr", Subject: "subj-dr"},
		source: source, restored: restored}

	maintenance, err := pgxpool.New(ctx, drDSN(t, base, "postgres"))
	if err != nil {
		t.Fatalf("maintenance pool: %v", err)
	}
	e.maintenance = maintenance
	t.Cleanup(maintenance.Close)
	for _, name := range []string{source, logical} {
		if _, err := maintenance.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
			t.Fatalf("could not provision an isolated database for this run")
		}
		t.Cleanup(func() { drDrop(t, maintenance, name) })
	}
	t.Cleanup(func() { drDrop(t, maintenance, restored) })
	t.Cleanup(func() { drRemoveBucket(t, client, e.bucket) })
	return e
}

func drDrop(t *testing.T, maintenance *pgxpool.Pool, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = maintenance.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1`, name)
	if _, err := maintenance.Exec(ctx, `DROP DATABASE IF EXISTS "`+name+`"`); err != nil {
		t.Logf("cleanup: could not drop %s", name)
	}
}

func drRemoveBucket(t *testing.T, client *minio.Client, bucket string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for obj := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
		if obj.Err != nil {
			return
		}
		_ = client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
	}
	_ = client.RemoveBucket(ctx, bucket)
}

// drPlane is one (database, bucket) pair with both planes wired the way the
// composition root wires them.
type drPlane struct {
	pool   *pgxpool.Pool
	arts   *Store
	st     *state.Store
	gc     *GC
	source interface {
		InspectReferences(context.Context, string, []references.Ref, []references.Artifact) (references.Result, error)
	}
}

func (e *drEnv) plane(t *testing.T, ctx context.Context, database string) *drPlane {
	t.Helper()
	pool, err := pgxpool.New(ctx, drDSN(t, e.dsn, database))
	if err != nil {
		t.Fatalf("pool for %s: %v", database, err)
	}
	t.Cleanup(pool.Close)
	arts, err := New(pool, e.client, e.bucket, func(context.Context, contracts.Scope, string, string) error { return nil })
	if err != nil {
		t.Fatalf("artifacts.New on %s: %v", database, err)
	}
	if err := arts.Migrate(ctx); err != nil {
		t.Fatalf("artifacts Migrate on %s: %v", database, err)
	}
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("state Migrate on %s: %v", database, err)
	}
	if err := st.MigrateCheckpoints(ctx); err != nil {
		t.Fatalf("state MigrateCheckpoints on %s: %v", database, err)
	}
	refs := state.ReferenceSource(pool, "ahp_state")
	scan := ReferenceScanFunc(func(ctx context.Context, tenant string, query []ArtifactQuery) (map[string]bool, error) {
		arts := make([]references.Artifact, len(query))
		for i, a := range query {
			arts[i] = references.Artifact{ID: a.ID, Digest: a.Digest}
		}
		result, err := refs.InspectReferences(ctx, tenant, nil, arts)
		if err != nil {
			return nil, err
		}
		return result.Referenced, nil
	})
	// The same owner API the data role installs: State asks Artifacts to hold a
	// reference, and Artifacts asks State which references are still live. The
	// settlement proof is State's, so the artifact owner cannot drop a pin
	// before the transaction that created it has finished.
	arts.keep = scan
	arts.checkpointSettlement = st.CheckpointOperationSettled
	st.SetCheckpointArtifacts(arts)
	gc, err := NewGC(arts, WithReferenceScan(scan))
	if err != nil {
		t.Fatalf("NewGC on %s: %v", database, err)
	}
	return &drPlane{pool: pool, arts: arts, st: st, gc: gc, source: refs}
}

func (e *drEnv) seal(t *testing.T, ctx context.Context, plane *drPlane, contextID, taskID, body string) ArtifactRef {
	t.Helper()
	sum := sha256.Sum256([]byte(body))
	up, err := plane.arts.BeginUploadWithBinding(ctx, e.scope, Binding{ContextID: contextID, TaskID: taskID},
		"k-"+contracts.NewID(), UploadRequest{Name: "dr.bin", MIME: "application/octet-stream",
			SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body))})
	if err != nil {
		t.Fatalf("BeginUploadWithBinding: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, up.PresignedURL, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT stage failed: %T", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("PUT stage status %d", resp.StatusCode)
	}
	ref, err := plane.arts.Seal(ctx, e.scope, up.ID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return ref
}

func (e *drEnv) rawGet(t *testing.T, object, version string) ([]byte, error) {
	t.Helper()
	obj, err := e.client.GetObject(context.Background(), e.bucket, object, minio.GetObjectOptions{VersionID: version})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	return io.ReadAll(obj)
}

func drObject(t *testing.T, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var object string
	if err := pool.QueryRow(context.Background(),
		`SELECT object FROM ahp_artifacts.artifacts WHERE id = $1`, id).Scan(&object); err != nil {
		t.Fatalf("read the object path of %s out of band: %v", id, err)
	}
	return object
}

// drRowExists asks the table, not the read gate, whether the row is still
// there. A refused purge leaves a TOMBSTONED row behind - the bytes and the row
// both survive, the deletion simply did not go through - so "row present" is the
// observation, and "not deleted" would be wrong.
func drRowExists(t *testing.T, pool *pgxpool.Pool, id string) bool {
	t.Helper()
	var present bool
	err := pool.QueryRow(context.Background(),
		`SELECT EXISTS(SELECT 1 FROM ahp_artifacts.artifacts WHERE id = $1)`, id).Scan(&present)
	if err != nil {
		t.Fatalf("probe the artifact row %s out of band: %v", id, err)
	}
	return present
}

func (e *drEnv) tombstone(t *testing.T, ctx context.Context, plane *drPlane, id string) {
	t.Helper()
	// Restore/GC must protect legacy tombstones from pre-fix writers. Normal
	// Delete now refuses to make referenced State artifacts unreadable.
	if _, err := plane.pool.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET deleted=true,deleted_at=now() WHERE id=$1`, id); err != nil {
		t.Fatalf("seed legacy tombstone: %v", err)
	}
	past := time.Now().UTC().Add(-time.Hour)
	if err := plane.arts.SetRetention(ctx, e.scope, id, &past); err != nil {
		t.Fatalf("expire retention of the tombstone: %v", err)
	}
}

// TestCheckpointBackupRestorePhysical is the whole DR claim in one run.
func TestCheckpointBackupRestorePhysical(t *testing.T) {
	e := newDREnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	plane := e.plane(t, ctx, e.source)

	// --- durable state of the source -------------------------------------
	thread, err := plane.st.CreateThread(ctx, e.scope, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := plane.st.CreateRun(ctx, e.scope, thread.ID, "task-live", "k-"+contracts.NewID(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	lease, err := plane.st.ClaimRun(ctx, e.scope.Tenant, e.scope.Deployment, "w1", time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	fence := state.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}

	const parts = 3
	bodies := make([]string, parts)
	for i := range bodies {
		bodies[i] = fmt.Sprintf("checkpoint payload %d, must survive the restore byte for byte", i)
	}
	for i := 0; i < parts; i++ {
		checkpoint := state.Checkpoint{ID: fmt.Sprintf("cp-%d", i), Namespace: "ns", Version: int64(i + 1),
			Parts: []state.Part{{Channel: "values", Version: fmt.Sprintf("v%d", i), Data: []byte(bodies[i])}}}
		if i > 0 {
			checkpoint.Parent = fmt.Sprintf("cp-%d", i-1)
		}
		if _, err := plane.st.PutCheckpoint(ctx, e.scope, fence, checkpoint, "k-cp-"+fmt.Sprint(i)); err != nil {
			t.Fatalf("PutCheckpoint: %v", err)
		}
	}

	// Three artifacts, each protected by a DIFFERENT durable mechanism, and a
	// fourth protected by nothing (the anti-vacuum control).
	// The purge phase of a sweep asks three owners, in this order: a durable
	// pin, the reference owner (State), and the legal hold. One artifact per
	// mechanism, so the sweep on the restored copy cannot pass by leaning on the
	// wrong one, and one with nothing at all, so it cannot pass by never
	// deleting anything.
	referenced := e.seal(t, ctx, plane, thread.ContextID, "task-desc", "referenced-by-checkpoint-descriptor")
	pinned := e.seal(t, ctx, plane, "ctx-"+contracts.NewID(), "task-pinned", "protected-only-by-a-durable-pin")
	held := e.seal(t, ctx, plane, "ctx-"+contracts.NewID(), "task-held", "protected-only-by-a-legal-hold")
	dead := e.seal(t, ctx, plane, "ctx-"+contracts.NewID(), "task-dead", "genuinely-dead-data")

	descriptor, err := json.Marshal(references.CheckpointArtifact{
		ID: referenced.ID, Tenant: referenced.Tenant, Deployment: referenced.Deployment,
		Subject: referenced.Subject, ContextID: referenced.ContextID, TaskID: referenced.TaskID,
		Version: referenced.Version, SHA256: referenced.SHA256, Size: referenced.Size, Labels: referenced.Labels})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plane.st.PutCheckpoint(ctx, e.scope, fence, state.Checkpoint{
		ID: "cp-descriptor", Namespace: "ns", Version: 9, Parent: "cp-2",
		Parts: []state.Part{{Channel: "artifact", Version: "artifact-1",
			Encoding: state.CheckpointArtifactEncoding, Data: descriptor}},
	}, "k-cp-descriptor"); err != nil {
		t.Fatalf("descriptor checkpoint: %v", err)
	}
	// A durable marker pin on the task that owns `pinned`. Nothing in the State
	// plane mentions this artifact, so after the restore the pin row is the only
	// thing that can save it.
	if err := plane.arts.PinRef(ctx, e.scope, PinKindTask, "task-pinned"); err != nil {
		t.Fatalf("PinRef: %v", err)
	}
	// Everything below is tombstoned with its retention floor elapsed: the
	// sweep on the restored copy has every reason to delete it, and three of
	// the four must still be there afterwards.
	for _, id := range []string{referenced.ID, pinned.ID, held.ID, dead.ID} {
		e.tombstone(t, ctx, plane, id)
	}
	// The hold goes on after the tombstone: a hold on live data refuses the
	// delete outright, and this leg is about what a sweep does to data that is
	// already tombstoned with its retention floor elapsed.
	if err := plane.arts.SetLegalHold(ctx, e.scope, held.ID, true); err != nil {
		t.Fatalf("SetLegalHold: %v", err)
	}
	// The durable administrative journal, on a thread of its own so that the
	// hold cannot be what protects the artifacts above.
	audited, err := plane.st.CreateThread(ctx, e.scope, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread journal subject: %v", err)
	}
	if err := plane.st.SetLegalhold(ctx, e.scope, audited.ID, "restore-drill-hold"); err != nil {
		t.Fatalf("SetLegalhold: %v", err)
	}

	objects := map[string]string{}
	versions := map[string]string{}
	for name, ref := range map[string]ArtifactRef{"referenced": referenced, "pinned": pinned,
		"held": held, "dead": dead} {
		versions[name] = ref.Version
		objects[name] = drObject(t, plane.pool, ref.ID)
	}
	before, err := plane.st.ListCheckpoints(ctx, e.scope, thread.ID, "ns", 0)
	if err != nil {
		t.Fatalf("ListCheckpoints before the snapshot: %v", err)
	}
	if len(before) != parts+1 {
		t.Fatalf("source history = %d checkpoints, want %d", len(before), parts+1)
	}

	// --- snapshot ---------------------------------------------------------
	// A physical copy needs the source free of sessions: close every pool this
	// run opened against the source, then copy the database.
	plane.pool.Close()
	if _, err := e.maintenance.Exec(ctx, `CREATE DATABASE "`+e.restored+`" TEMPLATE "`+e.source+`"`); err != nil {
		t.Fatalf("snapshot (CREATE DATABASE TEMPLATE) failed - the source still has sessions attached, or the role lacks CREATEDB")
	}
	// The clone must be usable on its own: it inherits the schemas, rows and
	// settings of the source without any migration being re-run.
	restored := e.plane(t, ctx, e.restored)

	// --- divergence on the source AFTER the snapshot ----------------------
	// Reopen the source: the snapshot had to be taken with no sessions on it,
	// and a real DR drill continues to serve traffic from the original after a
	// copy is taken.
	source := e.plane(t, ctx, e.source)
	diverged, err := source.st.CreateThread(ctx, e.scope, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatalf("post-snapshot CreateThread: %v", err)
	}
	if err := source.st.SetLegalhold(ctx, e.scope, diverged.ID, "written-after-the-snapshot"); err != nil {
		t.Fatalf("post-snapshot SetLegalhold: %v", err)
	}
	// Checkpoint writes after the snapshot stay on the source too.
	if _, err := source.st.PutCheckpoint(ctx, e.scope, fence, state.Checkpoint{
		ID: "cp-after-snapshot", Namespace: "ns", Version: 42, Parent: "cp-descriptor",
		Parts: []state.Part{{Channel: "values", Version: "v-after", Data: []byte("only on the source")}},
	}, "k-cp-after"); err != nil {
		t.Fatalf("post-snapshot PutCheckpoint: %v", err)
	}

	// --- the restore is a point in time, not a view -----------------------
	after, err := restored.st.ListCheckpoints(ctx, e.scope, thread.ID, "ns", 0)
	if err != nil {
		t.Fatalf("ListCheckpoints on the restored copy: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("restored history = %d checkpoints, want %d (the copy is not point-in-time)", len(after), len(before))
	}
	for i := range before {
		if before[i].ID != after[i].ID || before[i].Version != after[i].Version {
			t.Fatalf("checkpoint %d differs after restore: %s/%d vs %s/%d",
				i, before[i].ID, before[i].Version, after[i].ID, after[i].Version)
		}
	}
	for i := 0; i < parts; i++ {
		got, err := restored.st.GetPart(ctx, e.scope, thread.ID, "ns", "values", fmt.Sprintf("v%d", i))
		if err != nil || string(got) != bodies[i] {
			t.Fatalf("part v%d did not survive the restore: %q err=%v", i, got, err)
		}
	}
	if _, err := restored.st.GetCheckpoint(ctx, e.scope, thread.ID, "ns", "cp-after-snapshot"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("the restored copy contains a checkpoint written after the snapshot: %v", err)
	}
	var copiedAhead int
	if err := restored.pool.QueryRow(ctx,
		`SELECT count(*) FROM ahp_state.legalhold WHERE tenant = $1 AND reason = 'written-after-the-snapshot'`,
		e.scope.Tenant).Scan(&copiedAhead); err != nil {
		t.Fatalf("probe the restored journal: %v", err)
	}
	if copiedAhead != 0 {
		t.Fatal("the restored copy contains an administrative write made after the snapshot")
	}

	// --- the durable administrative journal survived ----------------------
	var reason string
	if err := restored.pool.QueryRow(ctx,
		`SELECT reason FROM ahp_state.legalhold WHERE tenant = $1 AND thread_id = $2`,
		e.scope.Tenant, audited.ID).Scan(&reason); err != nil {
		t.Fatalf("the administrative journal did not survive the restore: %v", err)
	}
	if reason != "restore-drill-hold" {
		t.Fatalf("journal reason changed: %q", reason)
	}
	// Administrative work continues on the restored copy, and is still durable:
	// read the new row back out of band, not through the call that wrote it.
	if err := restored.st.SetLegalhold(ctx, e.scope, audited.ID, "post-restore-review"); err != nil {
		t.Fatalf("post-restore SetLegalhold: %v", err)
	}
	if err := restored.pool.QueryRow(ctx,
		`SELECT reason FROM ahp_state.legalhold WHERE tenant = $1 AND thread_id = $2`,
		e.scope.Tenant, audited.ID).Scan(&reason); err != nil || reason != "post-restore-review" {
		t.Fatalf("an administrative write after the restore is not reviewable: %q err=%v", reason, err)
	}

	// --- live references still mean what they meant -----------------------
	probe := func(label string, ids ...string) map[string]bool {
		t.Helper()
		query := make([]references.Artifact, 0, len(ids))
		for _, id := range ids {
			query = append(query, references.Artifact{ID: id})
		}
		result, err := restored.source.InspectReferences(ctx, e.scope.Tenant, nil, query)
		if err != nil {
			t.Fatalf("%s: reference probe on the restored copy: %v", label, err)
		}
		return result.Referenced
	}
	if !probe("restored", referenced.ID)[referenced.ID] {
		t.Fatal("the restored copy lost the checkpoint-descriptor reference")
	}
	// The run is still open on the restored copy: the execution state an
	// operator would reason about came back too, and a task-level pin still
	// points at a task that exists.
	restoredRun, err := restored.st.GetRun(ctx, e.scope, run.ID)
	if err != nil {
		t.Fatalf("the run did not survive the restore: %v", err)
	}
	if restoredRun.Status == "done" || restoredRun.Status == "failed" || restoredRun.Status == "canceled" {
		t.Fatalf("the restored copy ended a run that was running on the source: %s", restoredRun.Status)
	}
	if probe("restored", pinned.ID)[pinned.ID] {
		t.Fatal("the pinned artifact is expected to be protected by the pin, not by a State reference - this leg would be vacuous")
	}
	if probe("restored", dead.ID)[dead.ID] {
		t.Fatal("the restored copy invented a reference for dead data")
	}
	// The pin is a marker pin: the durable row names the task, not the artifact,
	// which is exactly what the sweep matches against the locked row.
	var pins int
	if err := restored.pool.QueryRow(ctx,
		`SELECT count(*) FROM ahp_artifacts.gc_pins
		  WHERE tenant = $1 AND kind = 'task' AND ref_id = 'task-pinned'`,
		e.scope.Tenant).Scan(&pins); err != nil || pins == 0 {
		t.Fatalf("the durable pin did not survive the restore: pins=%d err=%v", pins, err)
	}

	// --- the collector on the restored copy -------------------------------
	dryPolicy := fastPolicy()
	dryPolicy.DryRun = true
	dry, err := restored.gc.RunOnce(ctx, e.scope, dryPolicy)
	if err != nil {
		t.Fatalf("dry-run sweep on the restored copy: %v", err)
	}
	if !dry.DryRun {
		t.Fatal("the first sweep was not a dry run")
	}
	sweepPolicy := fastPolicy()
	sweepPolicy.Key = "dr-sweep-" + contracts.NewID()
	sweep, err := restored.gc.RunOnce(ctx, e.scope, sweepPolicy)
	if err != nil {
		t.Fatalf("sweep on the restored copy: %v", err)
	}
	// Anti-vacuum first: genuinely dead data really is gone, bytes included.
	if drRowExists(t, restored.pool, dead.ID) {
		t.Fatalf("the restored collector kept dead data instead of deleting it: %+v", sweep)
	}
	if _, err := e.rawGet(t, objects["dead"], dead.Version); err == nil {
		t.Fatal("the restored collector left the bytes of dead data in object storage")
	}
	if sweep.PurgedTombstoned == 0 {
		t.Fatalf("the sweep reported no purge although dead data existed: %+v", sweep)
	}
	// And the three protected ones are still there, each for its own reason.
	for name, id := range map[string]string{"referenced": referenced.ID, "pinned": pinned.ID, "held": held.ID} {
		if !drRowExists(t, restored.pool, id) {
			t.Fatalf("%s was deleted by the collector on the restored copy: %+v", name, sweep)
		}
		raw, err := e.rawGet(t, objects[name], versions[name])
		if err != nil || len(raw) == 0 {
			t.Fatalf("%s lost its bytes on the restored copy: %v", name, err)
		}
	}
	// The refusals are counted by cause: the descriptor and the pin both answer
	// "live reference", the hold answers "legal hold", and each protection is
	// additionally proven by the durable row above.
	if sweep.SkippedLive < 2 {
		t.Fatalf("the sweep on the restored copy did not consult the pin and the reference owner: %+v", sweep)
	}
	if sweep.SkippedHold == 0 {
		t.Fatalf("the sweep on the restored copy ignored the legal hold: %+v", sweep)
	}
	// The sweep itself is durable and reviewable on the restored copy.
	events, err := restored.arts.GCAudit(ctx, e.scope, GCAuditFilter{ArtifactID: dead.ID})
	if err != nil {
		t.Fatalf("GCAudit on the restored copy: %v", err)
	}
	purges := 0
	for _, event := range events {
		if event.ArtifactID == dead.ID {
			purges++
		}
	}
	if purges == 0 {
		t.Fatal("the collector on the restored copy left no durable audit trail for the purge it performed")
	}
}
