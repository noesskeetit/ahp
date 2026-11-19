package statejournal

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

func TestDeletedSessionRetentionPostgresMinIO(t *testing.T) {
	if os.Getenv("AHP_JOURNAL_TEST_S3") == "" {
		t.Skip("requires the disposable local journal artifact store")
	}
	ctx, pool, _ := journalTestStore(t)
	reader, err := pgxpool.NewWithConfig(ctx, pool.Config())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	scope := contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "runtime", Subject: "owner"}
	foreign := scope
	foreign.Tenant += "-foreign"
	bucket := "ahp-journal-" + strings.ToLower(rand.Text()[:12])
	owner, s3 := blobTestOwner(t, pool, reader, bucket)
	if err := owner.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	store := NewWithArtifacts(pool, owner)
	payload := bytes.Repeat([]byte("r"), 307246)
	type fixture struct {
		lease  Lease
		upload artifacts.Upload
		ref    artifacts.ArtifactRef
	}
	seed := func(scope contracts.Scope, session string) fixture {
		t.Helper()
		lease, err := store.AcquireOrCreateSession(ctx, scope, session, "langgraph", 1, "writer-"+session, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		upload := putArtifactFixture(t, ctx, owner, scope, session, lease.Worker, payload, false)
		ref, err := owner.Seal(ctx, scope, upload.ID)
		if err != nil {
			t.Fatal(err)
		}
		value := Payload{Blob: &BlobRef{ArtifactID: ref.ID, Version: ref.Version, SHA256: ref.SHA256, SizeBytes: ref.Size}}
		if _, err := store.AppendEvents(ctx, scope, session, lease, false, AppendRequest{
			Key:    "input",
			Events: []EventInput{{ID: "input", Type: "message", Role: "user", Codec: "json", Payload: value}},
			KV:     []KVMutation{{ScopeType: "session", Namespace: "runtime", Key: "setting", Payload: Payload{Inline: []byte("state")}}},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.PutCheckpoint(ctx, scope, session, lease, CheckpointCommit{
			Key: "checkpoint",
			Checkpoint: Checkpoint{ID: "checkpoint", Metadata: json.RawMessage("{}"),
				ChannelVersions: json.RawMessage("{}"), Codec: "msgpack", Payload: value},
			Writes:  []PendingWrite{{TaskID: "node", Channel: "output", Codec: "json", Payload: value}},
			Intents: []PreparedIntent{{CallID: "effect", ToolName: "tool", ArgsDigest: SHA256Digest{1}, IdempotencyKey: "effect"}},
		}); err != nil {
			t.Fatal(err)
		}
		return fixture{lease, upload, ref}
	}
	const session = "deleted"
	target := seed(scope, session)
	live := seed(scope, "live")
	held := seed(scope, "held")
	other := seed(foreign, session)
	if err := owner.SetLegalHold(artifacts.WithCallerContext(ctx, "held"), scope, held.ref.ID, true); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{session, "held"} {
		if err := store.DeleteSession(ctx, scope, key); err != nil {
			t.Fatal(err)
		}
		if _, err := store.GetEvents(ctx, scope, key, 0, 1); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("deleted session was still readable: %v", err)
		}
	}
	if err := store.PutWrites(ctx, scope, session, target.lease, "", "old", nil); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("old writer accessed deleted session: %v", err)
	}
	countRows := func(scope contracts.Scope, session string) int {
		t.Helper()
		var n int
		err := pool.QueryRow(ctx, `
SELECT (SELECT count(*) FROM ahp_journal.session_events WHERE tenant_id=$1 AND session_id=$2)
     + (SELECT count(*) FROM ahp_journal.checkpoints WHERE tenant_id=$1 AND session_id=$2)
     + (SELECT count(*) FROM ahp_journal.checkpoint_writes WHERE tenant_id=$1 AND session_id=$2)
     + (SELECT count(*) FROM ahp_journal.tool_intents WHERE tenant_id=$1 AND session_id=$2)
     + (SELECT count(*) FROM ahp_journal.scoped_kv WHERE tenant_id=$1 AND deployment_id=$3
          AND scope_type='session' AND scope_owner=$2)`, scope.Tenant, session, scope.Deployment).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	versions := func(upload artifacts.Upload) int {
		t.Helper()
		var n int
		for row := range s3.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix: upload.Object, Recursive: true, WithVersions: true,
		}) {
			if row.Err != nil {
				t.Fatal(row.Err)
			}
			n++
		}
		return n
	}
	gc, err := artifacts.NewGC(owner,
		artifacts.WithLiveness(artifacts.LivenessFunc(store.ArtifactLiveness)),
		artifacts.WithJournalRetention(store.SweepDeletedSessions))
	if err != nil {
		t.Fatal(err)
	}
	sweep := func(retention time.Duration, dry bool) {
		t.Helper()
		report, err := gc.RunOnce(ctx, scope, artifacts.GCPolicy{TombstoneRetention: retention, BatchSize: 1, DryRun: dry})
		if err != nil || report.Errors != 0 {
			t.Fatalf("GC report: %+v (%v)", report, err)
		}
	}
	sweep(time.Hour, false)
	if countRows(scope, session) != 5 || versions(target.upload) != 1 {
		t.Fatal("retention must keep the journal and bytes until expiry")
	}
	time.Sleep(100 * time.Millisecond)
	sweep(time.Millisecond, true)
	if countRows(scope, session) != 5 || versions(target.upload) != 1 {
		t.Fatal("dry run changed the journal or bytes")
	}
	referenceOwner := artifacts.ReferenceScanFunc(New(reader).ArtifactsReferenced)
	unavailable, err := artifacts.New(pool, s3, bucket, func(context.Context, contracts.Scope, string, string) error {
		return contracts.NewError(contracts.ErrUnavailable, "test interrupted Artifacts retirement")
	}, artifacts.WithReferenceKeeper(referenceOwner))
	if err != nil {
		t.Fatal(err)
	}
	interrupted, err := artifacts.NewGC(owner,
		artifacts.WithJournalRetention(NewWithArtifacts(pool, unavailable).SweepDeletedSessions))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := interrupted.RunOnce(ctx, scope, artifacts.GCPolicy{TombstoneRetention: time.Millisecond}); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("retention interruption did not propagate: %v", err)
	}
	if countRows(scope, session) != 0 || versions(target.upload) != 1 {
		t.Fatal("interruption did not leave a committed State purge and pending Artifacts retirement")
	}
	sweep(time.Millisecond, false)
	if rows, remaining := countRows(scope, session), versions(target.upload); rows != 0 || remaining != 0 {
		t.Fatalf("expired session still has %d journal rows and %d object versions", rows, remaining)
	}
	if countRows(scope, "held") != 0 || versions(held.upload) != 1 {
		t.Fatal("retention did not remove held session's State rows or removed held bytes")
	}
	if err := owner.SetLegalHold(artifacts.WithCallerContext(ctx, "held"), scope, held.ref.ID, false); err != nil {
		t.Fatal(err)
	}
	sweep(time.Millisecond, false)
	if versions(held.upload) != 0 {
		t.Fatal("formerly held bytes survived a retry after releasing legal hold")
	}
	if _, err := store.AcquireOrCreateSession(ctx, scope, session, "langgraph", 1, "new", time.Minute); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("a purged session ID could be reused: %v", err)
	}
	for _, sample := range []struct {
		scope    contracts.Scope
		id       string
		artifact fixture
	}{{scope, "live", live}, {foreign, session, other}} {
		if countRows(sample.scope, sample.id) != 5 || versions(sample.artifact.upload) != 1 {
			t.Fatal("live or foreign session lost State rows or versions")
		}
		body, ref, err := owner.Open(artifacts.WithCallerContext(ctx, sample.id), sample.scope, sample.artifact.ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := io.ReadAll(body)
		body.Close()
		if err != nil || ref.Version != sample.artifact.ref.Version || !bytes.Equal(actual, payload) {
			t.Fatalf("live or foreign bytes changed: %v", err)
		}
	}
	t.Log("retention expired: interrupted between State purge and Artifacts; retry cleared bytes; tombstone, live/foreign versions and legal hold preserved; next retry collected released hold")
}
