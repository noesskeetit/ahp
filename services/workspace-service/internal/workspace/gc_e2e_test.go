package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// workspaceScan mirrors how cmd/ahp composes the collector: the artifact GC
// asks every owner plane (here: the Workspace plane's public ReferenceSource)
// whether an artifact is still referenced, and fail-closed protects on error.
func workspaceScan(e *env) artifacts.ReferenceScan {
	src := ReferenceSource(e.pool, Schema)
	return artifacts.ReferenceScanFunc(func(ctx context.Context, tenant string, arts []artifacts.ArtifactQuery) (map[string]bool, error) {
		in := make([]references.Artifact, len(arts))
		for i, a := range arts {
			in[i] = references.Artifact{ID: a.ID, Digest: a.Digest}
		}
		res, err := src.InspectReferences(ctx, tenant, nil, in)
		if err != nil {
			return nil, err
		}
		return res.Referenced, nil
	})
}

func aggressivePolicy() artifacts.GCPolicy {
	return artifacts.GCPolicy{
		StagedGrace:        time.Millisecond,
		TombstoneRetention: time.Millisecond,
		OrphanGrace:        time.Millisecond,
	}
}

// rowState reports whether an artifact row still exists and its deleted flag.
func rowState(t *testing.T, e *env, id string) (exists, deleted bool) {
	t.Helper()
	var n int
	var del bool
	err := e.pool.QueryRow(context.Background(),
		`SELECT count(*), COALESCE(bool_or(deleted), false) FROM ahp_artifacts.artifacts WHERE id=$1`, id).Scan(&n, &del)
	if err != nil {
		t.Fatal(err)
	}
	return n > 0, del
}

func objectAlive(t *testing.T, e *env, key, version string) bool {
	t.Helper()
	_, err := e.minio.StatObject(context.Background(), e.bucket, key, minio.StatObjectOptions{VersionID: version})
	return err == nil
}

func hashBytes(r io.Reader) string {
	h := sha256.New()
	_, _ = io.Copy(h, r)
	return hex.EncodeToString(h.Sum(nil))
}

// Workspace GC/lifecycle E2E on real PostgreSQL + versioned MinIO (root-аудит
// 11-workspace-store.md п.4, PDF §7 с.48: ревизия не удаляется, пока её
// использует рабочая копия; reference retention для checkout/веток при GC).
//
// Proven here against live infrastructure, not fixtures:
//   - the sweep collects a truly orphaned tombstone but refuses artifacts that
//     head, a pin, a conflict branch, or a pending working copy still names;
//   - a sealed version pinned by a revision survives an external overwrite at
//     the same key, and the sweep then collects only the surplus version;
//   - backup copies of every revision's pinned versions hash equal to the CAS
//     digests recorded on the revisions;
//   - restore inside the retention window returns the exact sealed version id
//     and digest, and re-verification against MinIO passes;
//   - a foreign tenant cannot read, cannot restore, and its own sweep leaves
//     this tenant's rows and object versions untouched; a same-tenant stranger
//     cannot read workspace bytes.
func TestWorkspaceGCE2EReferencesRestoreAndBackup(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	alice := "alice"

	w, err := e.ws.Create(ctx, e.scope(alice), "proj")
	mustOK(t, err)

	// R1: one file, then pinned — the pinned revision's artifact must survive
	// every sweep even after its row is tombstoned.
	aV1 := e.uploadSeal(t, alice, "a.txt", "alpha-v1", labels.Set{})
	rev1 := e.pub(t, alice, w.Name, "", []File{aV1})
	mustOK(t, e.ws.Pin(ctx, e.scope(alice), w.Name, rev1.ID))

	// R2 (head): a.txt advanced, b.md added. aV1 now lives only through the
	// pin and the conflict branch below.
	aV2 := repath(e.uploadSeal(t, alice, "a2.txt", "alpha-v2", labels.Set{}), "a.txt")
	bMd := e.uploadSeal(t, alice, "b.md", "# beta", labels.Set{})
	e.share(t, alice, w.Name, aV2, bMd)
	rev2 := e.pub(t, alice, w.Name, rev1.ID, []File{aV2, bMd})

	// A conflict branch kept for a conscious merge: its snapshot references
	// the pinned revision's bytes.
	candC, err := e.ws.CreateCandidate(ctx, e.scope(alice), w.Name, rev1.ID)
	mustOK(t, err)
	_, err = e.ws.UpdateCandidateFiles(ctx, e.scope(alice), candC.ID, []File{aV1})
	mustOK(t, err)
	mustOK(t, e.ws.SetCheck(ctx, e.scope(alice), candC.ID, "ci", "passed", ""))
	_, err = e.ws.Publish(ctx, e.scope(alice), candC.ID, "") // head moved → conflict recorded
	mustErr(t, err, string(contracts.ErrConflict))
	branchRev, err := e.ws.GetBranch(ctx, e.scope(alice), w.Name, "conflicts/"+candC.ID)
	mustOK(t, err)

	// A live working copy (pending candidate) naming a fresh artifact.
	dMd := e.uploadSeal(t, alice, "d.md", "# delta", labels.Set{})
	e.share(t, alice, w.Name, dMd)
	candW, err := e.ws.CreateCandidate(ctx, e.scope(alice), w.Name, rev2.ID)
	mustOK(t, err)
	_, err = e.ws.UpdateCandidateFiles(ctx, e.scope(alice), candW.ID, []File{aV2, bMd, dMd})
	mustOK(t, err)
	mustOK(t, e.ws.SetCheck(ctx, e.scope(alice), candW.ID, "ci", "passed", ""))

	// A true orphan: sealed, deleted, referenced by nobody.
	orph := e.uploadSeal(t, alice, "orphan.txt", "junk", labels.Set{})

	// Capture CAS identity before any deletion.
	type ident struct{ key, version, sha string }
	idents := map[string]ident{}
	for _, f := range []File{aV1, aV2, bMd, dMd} {
		ref, err := e.arts.Get(ctx, e.scope(alice), f.ArtifactID)
		mustOK(t, err)
		var key string
		if err := e.pool.QueryRow(ctx, `SELECT object FROM ahp_artifacts.artifacts WHERE id=$1`, f.ArtifactID).Scan(&key); err != nil {
			t.Fatal(err)
		}
		idents[f.ArtifactID] = ident{key: key, version: ref.Version, sha: ref.SHA256}
		if "sha256:"+ref.SHA256 != f.Digest && ref.SHA256 != strings.TrimPrefix(f.Digest, "sha256:") {
			t.Fatalf("CAS digest drift for %s: row %s vs file %s", f.Path, ref.SHA256, f.Digest)
		}
	}

	// --- backup: copy every pinned version behind every reachable revision
	// into a dedicated bucket and verify the copies against the CAS digests
	// the revisions recorded. This is the property a restore depends on:
	// backup preserved the exact versions and their content hashes.
	backup := "ahp-go-ws-bak-" + strings.TrimPrefix(e.bucket, "ahp-go-ws-")
	mustOK(t, e.minio.MakeBucket(ctx, backup, minio.MakeBucketOptions{}))
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for obj := range e.minio.ListObjects(c, backup, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if obj.Err != nil {
				break
			}
			_ = e.minio.RemoveObject(c, backup, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
		}
		_ = e.minio.RemoveBucket(c, backup)
	})
	backedUp := 0
	for _, rev := range []string{rev1.ID, rev2.ID, branchRev.ID} {
		files, err := e.ws.ListFiles(ctx, e.scope(alice), w.Name, rev)
		mustOK(t, err)
		for _, f := range files {
			id := idents[f.ArtifactID]
			_, err := e.minio.CopyObject(ctx,
				minio.CopyDestOptions{Bucket: backup, Object: f.ArtifactID},
				minio.CopySrcOptions{Bucket: e.bucket, Object: id.key, VersionID: id.version})
			mustOK(t, err)
			obj, err := e.minio.GetObject(ctx, backup, f.ArtifactID, minio.GetObjectOptions{})
			mustOK(t, err)
			got := hashBytes(obj)
			obj.Close()
			if got != strings.TrimPrefix(f.Digest, "sha256:") {
				t.Fatalf("backup copy of %s hashes %s, revision CAS says %s", f.Path, got, f.Digest)
			}
			backedUp++
		}
	}
	if backedUp != 4 { // a.txt@R1 (twice: R1 + branch tip), a.txt@R2, b.md
		t.Fatalf("backup covered only %d revision files", backedUp)
	}

	// --- tombstone four artifacts: the orphan (dead) plus three that the
	// workspace still references from head, a pin/branch, and a working copy.
	mustOK(t, e.arts.Delete(ctx, e.scope(alice), orph.ArtifactID, "orphan.txt"))
	mustOK(t, e.arts.Delete(ctx, e.scope(alice), bMd.ArtifactID, "b.md"))  // head R2
	mustOK(t, e.arts.Delete(ctx, e.scope(alice), aV1.ArtifactID, "a.txt")) // pinned R1 + branch
	mustOK(t, e.arts.Delete(ctx, e.scope(alice), dMd.ArtifactID, "d.md"))  // pending candidate
	time.Sleep(20 * time.Millisecond)

	gc, err := artifacts.NewGC(e.arts, artifacts.WithReferenceScan(workspaceScan(e)))
	mustOK(t, err)
	rep, err := gc.RunOnce(ctx, e.scope(alice), aggressivePolicy())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if rep.PurgedTombstoned != 1 || rep.SkippedLive < 3 {
		t.Fatalf("sweep decisions wrong: %+v", rep)
	}

	// The orphan is gone: row and bytes.
	if exists, _ := rowState(t, e, orph.ArtifactID); exists {
		t.Fatal("orphan row survived the sweep")
	}
	if objectAlive(t, e, idents[orph.ArtifactID].key, idents[orph.ArtifactID].version) {
		t.Fatal("orphan object version survived the sweep")
	}
	// Live references survived: rows still tombstoned (not collected), bytes
	// still there at exactly the pinned version.
	for _, id := range []string{bMd.ArtifactID, aV1.ArtifactID, dMd.ArtifactID} {
		exists, deleted := rowState(t, e, id)
		if !exists || !deleted {
			t.Fatalf("referenced artifact %s was collected (exists=%v deleted=%v)", id, exists, deleted)
		}
		if !objectAlive(t, e, idents[id].key, idents[id].version) {
			t.Fatalf("referenced bytes of %s were collected", id)
		}
	}

	// --- foreign tenant: cannot read, cannot restore, cannot touch.
	foreign := contracts.Scope{Tenant: e.tenant + "-foreign", Deployment: "d-1", Subject: "eve"}
	if _, err := e.arts.Get(ctx, foreign, bMd.ArtifactID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign tenant read a workspace artifact: %v", err)
	}
	if _, err := e.arts.Restore(ctx, foreign, bMd.ArtifactID); err == nil {
		t.Fatal("foreign tenant restored a tombstone it does not own")
	} else if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign restore failed with %s, want NOT_FOUND", contracts.Code(err))
	}
	if _, err := e.ws.GetWorkspace(ctx, foreign, w.Name); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign tenant saw the project: %v", err)
	}
	if _, _, _, err := e.ws.ReadFile(ctx, foreign, w.Name, rev2.ID, "a.txt", 0, 0); err == nil {
		t.Fatal("foreign tenant read revision bytes")
	}
	// A same-tenant subject outside every audience cannot read either.
	if _, _, _, err := e.ws.ReadFile(ctx, e.scope("eve-same-tenant"), w.Name, rev2.ID, "a.txt", 0, 0); err == nil {
		t.Fatal("non-member read revision bytes")
	}
	// The foreign tenant's own sweep must leave this tenant's plane alone.
	before := countTenantObjects(t, e)
	if _, err := gc.RunOnce(ctx, foreign, aggressivePolicy()); err != nil {
		t.Fatalf("foreign sweep: %v", err)
	}
	if after := countTenantObjects(t, e); after != before {
		t.Fatalf("foreign sweep changed this tenant's object store: %d → %d", before, after)
	}
	for _, id := range []string{bMd.ArtifactID, aV1.ArtifactID, dMd.ArtifactID} {
		if exists, _ := rowState(t, e, id); !exists {
			t.Fatalf("foreign sweep collected %s", id)
		}
	}

	// --- restore inside the retention window returns the exact sealed
	// version and digest, and integrity re-verifies against live MinIO.
	restored, err := e.arts.Restore(ctx, e.scope(alice), bMd.ArtifactID)
	mustOK(t, err)
	if restored.Version != idents[bMd.ArtifactID].version || restored.SHA256 != idents[bMd.ArtifactID].sha {
		t.Fatalf("restore changed the sealed identity: %+v", restored)
	}
	_, err = e.arts.Restore(ctx, e.scope(alice), aV1.ArtifactID)
	mustOK(t, err)
	_, err = e.arts.Restore(ctx, e.scope(alice), dMd.ArtifactID)
	mustOK(t, err)

	report, err := e.ws.VerifyRevisionIntegrity(ctx, e.scope(alice), w.Name, rev2.ID)
	mustOK(t, err)
	if !report.OK {
		t.Fatalf("head revision failed CAS verification after restore: %+v", report)
	}
	report1, err := e.ws.VerifyRevisionIntegrity(ctx, e.scope(alice), w.Name, rev1.ID)
	mustOK(t, err)
	if !report1.OK {
		t.Fatalf("pinned revision failed CAS verification: %+v", report1)
	}
	// A restore reopens the retention window: the next sweep must still find
	// these rows (and the head reference protects them anyway).
	rep2, err := gc.RunOnce(ctx, e.scope(alice), aggressivePolicy())
	mustOK(t, err)
	if rep2.PurgedTombstoned != 0 {
		t.Fatalf("sweep after restore collected rows again: %+v", rep2)
	}
	for _, id := range []string{bMd.ArtifactID, aV1.ArtifactID, dMd.ArtifactID} {
		ref, err := e.arts.Get(ctx, e.scope(alice), id)
		mustOK(t, err)
		if ref.Version != idents[id].version {
			t.Fatalf("artifact %s lost its version across GC/restore", id)
		}
	}

	// --- external overwrite at a sealed key cannot rewrite what a revision
	// reads (reads are version-pinned), and the sweep collects only the
	// surplus version afterwards.
	aKey := idents[aV2.ArtifactID].key
	mustOK(t, putRaw(e, aKey, "TAMPERED"))
	rc, fb, _, err := e.ws.ReadFile(ctx, e.scope(alice), w.Name, rev2.ID, "a.txt", 0, 0)
	mustOK(t, err)
	body, _ := io.ReadAll(rc)
	rc.Close()
	if string(body) != "alpha-v2" {
		t.Fatalf("revision served tampered bytes: %q", body)
	}
	if strings.TrimPrefix(fb.Digest, "sha256:") != idents[aV2.ArtifactID].sha {
		t.Fatalf("revision digest moved after overwrite: %s", fb.Digest)
	}
	policy := aggressivePolicy()
	policy.CollectUnpinnedVersions = true
	rep3, err := gc.RunOnce(ctx, e.scope(alice), policy)
	mustOK(t, err)
	if rep3.VersionsRemoved < 1 {
		t.Fatalf("surplus version at a sealed key survived the sweep: %+v", rep3)
	}
	rc2, _, _, err := e.ws.ReadFile(ctx, e.scope(alice), w.Name, rev2.ID, "a.txt", 0, 0)
	mustOK(t, err)
	body2, _ := io.ReadAll(rc2)
	rc2.Close()
	if string(body2) != "alpha-v2" {
		t.Fatalf("pinned bytes damaged by the surplus sweep: %q", body2)
	}

	// --- revision-level lifecycle guards on the same live data: head, pin,
	// branch tip and working-copy bases refuse purge; the pending working
	// copy still merges, proving the guards do not freeze the project.
	mustErr(t, e.ws.PurgeRevision(ctx, e.scope(alice), w.Name, rev2.ID), string(contracts.ErrConflict)) // head
	mustErr(t, e.ws.PurgeRevision(ctx, e.scope(alice), w.Name, rev1.ID), string(contracts.ErrConflict)) // pinned, and the base of an open conflict copy
	rev3 := e.pub(t, alice, w.Name, rev2.ID, []File{aV2, bMd, dMd})
	if head, err := e.ws.GetHead(ctx, e.scope(alice), w.Name); err != nil || head != rev3.ID {
		t.Fatalf("working copy could not merge: %v", err)
	}
}

func countTenantObjects(t *testing.T, e *env) int {
	t.Helper()
	n := 0
	for obj := range e.minio.ListObjects(context.Background(), e.bucket,
		minio.ListObjectsOptions{Prefix: e.tenant + "/", Recursive: true, WithVersions: true}) {
		if obj.Err != nil {
			t.Fatal(obj.Err)
		}
		n++
	}
	return n
}

func putRaw(e *env, key, body string) error {
	_, err := e.minio.PutObject(context.Background(), e.bucket, key,
		strings.NewReader(body), int64(len(body)), minio.PutObjectOptions{})
	return err
}
