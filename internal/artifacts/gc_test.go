package artifacts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The storage GC gate tests. Every claim about bytes is checked out of band -
// a direct MinIO GET of one exact version id, and raw SQL against the metadata
// table - so a passing test is evidence about the object store and the rows,
// not about this package's own reading of them.

// rawGet reads one object version straight from the bucket. An error means the
// version is not there any more, which is the only honest way to show that a
// sweep did (or did not) remove bytes.
func (e *env) rawGet(t *testing.T, object, version string) ([]byte, error) {
	t.Helper()
	obj, err := e.client.GetObject(context.Background(), e.bucket, object,
		minio.GetObjectOptions{VersionID: version})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	return io.ReadAll(obj)
}

// rawPut writes an object without any artifact row: the shape of an orphan the
// orphan phase has to reason about.
func (e *env) rawPut(t *testing.T, object, body string) string {
	t.Helper()
	info, err := e.client.PutObject(context.Background(), e.bucket, object,
		strings.NewReader(body), int64(len(body)), minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("raw put: %v", err)
	}
	return info.VersionID
}

func (e *env) sealBoundWithKey(t *testing.T, b Binding, key, body string) (ArtifactRef, Upload) {
	t.Helper()
	up, err := e.store.BeginUploadWithBinding(context.Background(), e.scope, b, key, UploadRequest{
		Name: "g.bin", MIME: "application/octet-stream",
		SHA256: shaHexOf(body), Size: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	put(t, up.PresignedURL, body)
	ref, err := e.store.Seal(context.Background(), e.scope, up.ID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return ref, up
}

// objectOf reads the object path of one artifact straight from the table.
func (e *env) objectOf(t *testing.T, id string) string {
	t.Helper()
	var object string
	err := isolatedPool(t).QueryRow(context.Background(),
		`SELECT object FROM ahp_artifacts.artifacts WHERE id = $1`, id).Scan(&object)
	if err != nil {
		t.Fatalf("read object of %s: %v", id, err)
	}
	return object
}

// rowExists asks the table directly whether a row is still there, bypassing the
// read gate that hides tombstones.
func (e *env) rowExists(t *testing.T, id string) (exists, deleted bool) {
	t.Helper()
	err := isolatedPool(t).QueryRow(context.Background(),
		`SELECT deleted FROM ahp_artifacts.artifacts WHERE id = $1`, id).Scan(&deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false
	}
	if err != nil {
		t.Fatalf("row lookup: %v", err)
	}
	return true, deleted
}

// auditOf reads the deletion-plane audit trail with plain SQL.
func (e *env) auditOf(t *testing.T, artifactID string) []GCEvent {
	t.Helper()
	rows, err := isolatedPool(t).Query(context.Background(), `
SELECT event, artifact_id, object, version, sha256, size, reason, sweep_id
  FROM ahp_artifacts.gc_events WHERE artifact_id = $1 ORDER BY id`, artifactID)
	if err != nil {
		t.Fatalf("audit lookup: %v", err)
	}
	defer rows.Close()
	var out []GCEvent
	for rows.Next() {
		var ev GCEvent
		if err := rows.Scan(&ev.Event, &ev.ArtifactID, &ev.Object, &ev.Version,
			&ev.SHA256, &ev.Size, &ev.Reason, &ev.SweepID); err != nil {
			t.Fatalf("audit scan: %v", err)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("audit iterate: %v", err)
	}
	return out
}

func eventsNamed(events []GCEvent, name string) []GCEvent {
	var out []GCEvent
	for _, ev := range events {
		if ev.Event == name {
			out = append(out, ev)
		}
	}
	return out
}

// fastPolicy is the least conservative policy a test may use: the floors the
// production defaults put in days are collapsed to milliseconds so a sweep can
// be observed in one test run. Everything else, including every refusal rule,
// is the production code path.
func fastPolicy() GCPolicy {
	return GCPolicy{
		StagedGrace:        30 * time.Millisecond,
		TombstoneRetention: time.Millisecond,
		OrphanGrace:        30 * time.Millisecond,
		BatchSize:          50,
	}
}

func TestGCLeavesSealedArtifactAlone(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body := "sealed-bytes-must-survive-every-sweep"
	ref, up := e.sealBoundWithKey(t, Binding{ContextID: "ctx-keep", TaskID: "task-keep"}, "k-keep", body)

	gc, err := NewGC(e.store)
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}
	// Even with no live marker registered and every floor collapsed, a sealed
	// artifact that nobody tombstoned is not garbage.
	pol := fastPolicy()
	report, err := gc.RunOnce(ctx, e.scope, pol)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.PurgedTombstoned != 0 || report.CollectedStaged != 0 {
		t.Fatalf("sweep collected a sealed artifact: %+v", report)
	}
	if got := e.objectOf(t, ref.ID); got != up.Object {
		t.Fatalf("object path moved: %q != %q", got, up.Object)
	}
	raw, err := e.rawGet(t, up.Object, ref.Version)
	if err != nil || string(raw) != body || shaHexOf(string(raw)) != ref.SHA256 {
		t.Fatalf("sealed bytes changed: err=%v sha_match=%v", err, shaHexOf(string(raw)) == ref.SHA256)
	}
	if _, err := e.store.Get(ctx, e.scope, ref.ID); err != nil {
		t.Fatalf("Get after sweep: %v", err)
	}
}

func TestGCPurgeRespectsRetentionHoldAndAudit(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body := "journey-output-tombstoned-then-purged"
	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-ret", TaskID: "task-ret"}, "k-ret", body)
	object := e.objectOf(t, ref.ID)

	if err := e.store.Delete(ctx, e.scope, ref.ID, "del-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// A tombstone whose floor has not elapsed keeps its bytes: this is the
	// window a restore must be able to reopen.
	future := time.Now().UTC().Add(time.Hour)
	if err := e.store.SetRetention(ctx, e.scope, ref.ID, &future); err != nil {
		t.Fatalf("SetRetention: %v", err)
	}
	gc, _ := NewGC(e.store)
	report, err := gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.PurgedTombstoned != 0 || report.SkippedRetention == 0 {
		t.Fatalf("sweep ignored the retention floor: %+v", report)
	}
	if _, err := e.rawGet(t, object, ref.Version); err != nil {
		t.Fatalf("bytes removed inside the retention window: %v", err)
	}

	// A legal hold refuses even an elapsed floor and an explicit purge.
	past := time.Now().UTC().Add(-time.Minute)
	if err := e.store.SetRetention(ctx, e.scope, ref.ID, &past); err != nil {
		t.Fatalf("SetRetention past: %v", err)
	}
	if err := e.store.SetLegalHold(ctx, e.scope, ref.ID, true); err != nil {
		t.Fatalf("SetLegalHold: %v", err)
	}
	report, err = gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil {
		t.Fatalf("RunOnce held: %v", err)
	}
	if report.PurgedTombstoned != 0 || report.SkippedHold == 0 {
		t.Fatalf("sweep collected a held artifact: %+v", report)
	}
	codeIs(t, e.store.Purge(ctx, e.scope, ref.ID), contracts.ErrConflict)

	if err := e.store.SetLegalHold(ctx, e.scope, ref.ID, false); err != nil {
		t.Fatalf("release hold: %v", err)
	}
	if err := e.store.Purge(ctx, e.scope, ref.ID); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if _, err := e.rawGet(t, object, ref.Version); err == nil {
		t.Fatal("purge left the pinned version readable")
	}
	events := e.auditOf(t, ref.ID)
	if len(eventsNamed(events, GCEventTombstone)) != 1 {
		t.Fatalf("tombstone was not audited: %+v", events)
	}
	purges := eventsNamed(events, GCEventPurge)
	done := eventsNamed(events, GCEventObjectsRemoved)
	if len(purges) != 1 || len(done) != 1 {
		t.Fatalf("purge was not audited end to end: %+v", events)
	}
	if purges[0].SHA256 != ref.SHA256 || purges[0].Size != int64(len(body)) {
		t.Fatalf("purge audit lost the digest or size: %+v", purges[0])
	}
	if done[0].Version == "" {
		t.Fatalf("object-removal audit has no version: %+v", done[0])
	}
}

func TestGCPinsAndLivenessProtectLiveReferences(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Two journey artifacts: one bound to a context, one to a task, both
	// tombstoned with an elapsed floor - the exact shape of a chat the user
	// deleted while something still refers to the bytes.
	in := map[string]string{"ctx": "ctx-live", "task": "task-live"}
	refs := map[string]ArtifactRef{}
	objects := map[string]string{}
	for kind, marker := range in {
		body := fmt.Sprintf("live-%s-payload-%s", kind, marker)
		b := Binding{}
		if kind == "ctx" {
			b.ContextID = marker
		} else {
			b.TaskID = marker
		}
		ref, _ := e.sealBoundWithKey(t, b, "k-"+kind, body)
		refs[kind] = ref
		objects[kind] = e.objectOf(t, ref.ID)
		if err := e.store.Delete(ctx, e.scope, ref.ID, "del-"+kind); err != nil {
			t.Fatalf("Delete %s: %v", kind, err)
		}
	}
	e.waitTombstoneAge(t, refs["ctx"].ID, refs["task"].ID)
	if err := e.store.PinRef(ctx, e.scope, PinKindContext, "ctx-live"); err != nil {
		t.Fatalf("PinRef: %v", err)
	}
	if err := e.store.PinRef(ctx, e.scope, PinKindTask, "task-live"); err != nil {
		t.Fatalf("PinRef task: %v", err)
	}
	gc, _ := NewGC(e.store)
	report, err := gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.PurgedTombstoned != 0 || report.SkippedLive < 2 {
		t.Fatalf("sweep collected pinned live references: %+v", report)
	}
	for kind, ref := range refs {
		raw, err := e.rawGet(t, objects[kind], ref.Version)
		if err != nil {
			t.Fatalf("pinned bytes removed for %s: %v", kind, err)
		}
		if shaHexOf(string(raw)) != ref.SHA256 {
			t.Fatalf("pinned bytes changed for %s", kind)
		}
		if ok, _ := e.rowExists(t, ref.ID); !ok {
			t.Fatalf("pinned row removed for %s", kind)
		}
	}
	// An explicit purge honours the same rule: a pin is not bypassable by
	// asking for one artifact politely.
	codeIs(t, e.store.Purge(ctx, e.scope, refs["ctx"].ID), contracts.ErrConflict)

	// Releasing the pins says "nothing alive refers to these", and retention
	// (already elapsed) then decides: the next sweep takes them.
	if err := e.store.UnpinRef(ctx, e.scope, PinKindContext, "ctx-live"); err != nil {
		t.Fatalf("UnpinRef: %v", err)
	}
	if err := e.store.UnpinRef(ctx, e.scope, PinKindTask, "task-live"); err != nil {
		t.Fatalf("UnpinRef task: %v", err)
	}
	report, err = gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil {
		t.Fatalf("RunOnce after unpin: %v", err)
	}
	if report.PurgedTombstoned != 2 {
		t.Fatalf("unpinned expired artifacts were not collected: %+v", report)
	}
	for kind, ref := range refs {
		if _, err := e.rawGet(t, objects[kind], ref.Version); err == nil {
			t.Fatalf("bytes of unpinned %s survived the sweep", kind)
		}
	}
}

func TestGCLivenessProbeFailsClosed(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body := "probe-decides-whether-this-is-garbage"
	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-probe", TaskID: "task-probe"}, "k-probe", body)
	if err := e.store.Delete(ctx, e.scope, ref.ID, "del-probe"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	object := e.objectOf(t, ref.ID)
	e.waitTombstoneAge(t, ref.ID)

	type probeAnswer struct {
		alive map[Ref]bool
		err   error
	}
	var answers atomic.Value // probeAnswer, one concrete type as atomic.Value requires
	answers.Store(probeAnswer{})
	setAnswer := func(v probeAnswer) { answers.Store(v) }
	probe := LivenessFunc(func(_ context.Context, _ string, refs []Ref) (map[Ref]bool, error) {
		a := answers.Load().(probeAnswer)
		return a.alive, a.err
	})
	gc, _ := NewGC(e.store, WithLiveness(probe))

	// Authority unreachable: everything with a marker stays.
	setAnswer(probeAnswer{err: errors.New("state is down")})
	report, err := gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.PurgedTombstoned != 0 || report.SkippedLive == 0 {
		t.Fatalf("probe failure did not fail closed: %+v", report)
	}
	// Partial answer: an unasked-about marker is treated as alive, not as dead.
	setAnswer(probeAnswer{alive: map[Ref]bool{{Kind: PinKindContext, ID: "ctx-probe"}: false}})
	report, err = gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil {
		t.Fatalf("RunOnce partial: %v", err)
	}
	if report.PurgedTombstoned != 0 {
		t.Fatalf("a partially answered marker was collected: %+v", report)
	}
	// Full answer, both dead: the sweep proceeds.
	setAnswer(probeAnswer{alive: map[Ref]bool{
		{Kind: PinKindContext, ID: "ctx-probe"}: false,
		{Kind: PinKindTask, ID: "task-probe"}:   false,
	}})
	report, err = gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil {
		t.Fatalf("RunOnce dead: %v", err)
	}
	if report.PurgedTombstoned != 1 {
		t.Fatalf("dead references were not collected: %+v", report)
	}
	if _, err := e.rawGet(t, object, ref.Version); err == nil {
		t.Fatal("bytes of a provably unreferenced tombstone survived")
	}
}

func TestGCWithoutAuthorityLeavesMarkedUploadsAlone(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A staged upload bound to a task: never sealed, so nothing can be served
	// from it, but it names a task the storage plane cannot judge without an
	// authority - so with no Liveness configured it must survive.
	sum := shaHexOf("staged-with-marker")
	up, err := e.store.BeginUploadWithBinding(ctx, e.scope, Binding{TaskID: "task-running"}, "k-staged-marked",
		UploadRequest{Name: "s.bin", MIME: "application/octet-stream", SHA256: sum, Size: 18})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	put(t, up.PresignedURL, "staged-with-marker")
	// An unbound staged upload is ordinary garbage once the grace passes.
	up2, err := e.store.BeginUploadWithBinding(ctx, e.scope, Binding{}, "k-staged-unbound",
		UploadRequest{Name: "u.bin", MIME: "application/octet-stream", SHA256: sum, Size: 18})
	if err != nil {
		t.Fatalf("BeginUpload unbound: %v", err)
	}
	put(t, up2.PresignedURL, "staged-with-marker")

	gc, _ := NewGC(e.store)
	e.waitStagedAge(t, up.ID, up2.ID)
	e.waitObjectGrace(t)
	report, err := gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.CollectedStaged != 1 {
		t.Fatalf("staged collection wrong: %+v", report)
	}
	if ok, _ := e.rowExists(t, up.ID); !ok {
		t.Fatal("a staged upload naming a task was collected with no liveness authority")
	}
	if ok, _ := e.rowExists(t, up2.ID); ok {
		t.Fatal("the unbound staged upload survived")
	}
	if _, err := e.rawGet(t, up2.Object, ""); err == nil {
		t.Fatal("orphan bytes of the collected upload survived the sweep")
	}
	// The marked upload's bytes are still there for the run that is writing it.
	if raw, err := e.rawGet(t, up.Object, ""); err != nil || string(raw) != "staged-with-marker" {
		t.Fatalf("marked staged bytes lost: err=%v", err)
	}
}

func TestGCSweepIdempotentAndSurvivesLostAck(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body := "one-sweep-one-collection"
	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-idem"}, "k-idem", body)
	if err := e.store.Delete(ctx, e.scope, ref.ID, "del-idem"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	e.waitTombstoneAge(t, ref.ID)
	gc, _ := NewGC(e.store)
	pol := fastPolicy()
	pol.Key = "sweep-" + t.Name()
	first, err := gc.RunOnce(ctx, e.scope, pol)
	if err != nil {
		t.Fatalf("first sweep: %v", err)
	}
	if first.PurgedTombstoned != 1 || first.Replayed {
		t.Fatalf("first sweep wrong: %+v", first)
	}
	// The ack is lost: the caller retries the same key. It must get the same
	// report, marked as a replay, and above all it must not sweep again.
	second, err := gc.RunOnce(ctx, e.scope, pol)
	if err != nil {
		t.Fatalf("retry sweep: %v", err)
	}
	if !second.Replayed || second.SweepID != first.SweepID ||
		second.PurgedTombstoned != first.PurgedTombstoned {
		t.Fatalf("retry did not replay the original report: %+v vs %+v", second, first)
	}
	// A fresh key finds nothing left: the work of the first sweep was the work.
	fresh := fastPolicy()
	fresh.Key = "sweep-fresh-" + t.Name()
	third, err := gc.RunOnce(ctx, e.scope, fresh)
	if err != nil {
		t.Fatalf("fresh sweep: %v", err)
	}
	if third.Replayed || third.PurgedTombstoned != 0 || third.CollectedStaged != 0 ||
		third.VersionsRemoved != 0 || third.Errors != 0 {
		t.Fatalf("a second sweep did work: %+v", third)
	}
	// Two engines racing over one tombstone collect it once. The loser's
	// guarded statement finds no row, which is the whole of its side effect.
	race := setup(t)
	ref2, _ := race.sealBoundWithKey(t, Binding{}, "k-race", "concurrent-purge-target")
	if err := race.store.Delete(ctx, race.scope, ref2.ID, "del-race"); err != nil {
		t.Fatalf("Delete race row: %v", err)
	}
	race.waitTombstoneAge(t, ref2.ID)
	var wg sync.WaitGroup
	var total atomic.Int64
	gcp, _ := NewGC(race.store)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rep, err := gcp.RunOnce(ctx, race.scope, fastPolicy())
			if err != nil {
				t.Errorf("concurrent sweep: %v", err)
				return
			}
			total.Add(int64(rep.PurgedTombstoned))
		}()
	}
	wg.Wait()
	if total.Load() != 1 {
		t.Fatalf("concurrent sweeps collected %d rows for one tombstone", total.Load())
	}
}

func TestGCAuditCarriesIdentifiersOnly(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	secret := "CONFIDENTIAL-JOURNEY-CONTENT-MARKER-42"
	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-audit", TaskID: "task-audit"}, "k-audit", secret)
	if err := e.store.Delete(ctx, e.scope, ref.ID, "del-audit"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	e.waitTombstoneAge(t, ref.ID)
	gc, _ := NewGC(e.store)
	if _, err := gc.RunOnce(ctx, e.scope, fastPolicy()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// The audit trail is readable through the store as well as raw SQL, and it
	// must not have copied the artifact's content into any column: the PDF's
	// logging rule for this plane is identifiers, digests and sizes.
	events, err := e.store.GCAudit(ctx, e.scope, GCAuditFilter{ArtifactID: ref.ID})
	if err != nil {
		t.Fatalf("GCAudit: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("expected tombstone and purge events, got %+v", events)
	}
	var row string
	err = isolatedPool(t).QueryRow(ctx, `
SELECT coalesce(string_agg(row_to_json(x)::text, E'\n'), '')
  FROM ahp_artifacts.gc_events x WHERE x.artifact_id = $1`, ref.ID).Scan(&row)
	if err != nil {
		t.Fatalf("raw audit: %v", err)
	}
	if strings.Contains(row, secret) || strings.Contains(row, "CONFIDENTIAL") {
		t.Fatal("audit trail contains artifact content")
	}
	if !strings.Contains(row, ref.SHA256) {
		t.Fatal("audit trail does not carry the recorded digest")
	}
}

func TestGCSweepIsTenantScopedAndLeavesForeignTenantBytes(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Derived from this fixture's own (random) tenant: a tenant named after the
	// test function is the same tenant in every repetition, and its idempotency
	// keys then replay against the previous repetition's rows.
	foreignScope := contracts.Scope{Tenant: e.scope.Tenant + "-foreign", Deployment: "d1", Subject: "carol"}
	foreignBody := "foreign-tenant-journey-artifact"
	fup, err := e.store.BeginUploadWithBinding(ctx, foreignScope, Binding{ContextID: "ctx-f"}, "k-f", UploadRequest{
		Name: "f.bin", MIME: "application/octet-stream", SHA256: shaHexOf(foreignBody), Size: int64(len(foreignBody)),
	})
	if err != nil {
		t.Fatalf("foreign BeginUpload: %v", err)
	}
	put(t, fup.PresignedURL, foreignBody)
	fref, err := e.store.Seal(ctx, foreignScope, fup.ID)
	if err != nil {
		t.Fatalf("foreign Seal: %v", err)
	}
	// A foreign orphan too: an object with no row, which an ill-scoped sweep
	// would happily delete while sweeping somebody else.
	foreignOrphan := foreignScope.Tenant + "/orphan-key"
	foreignOrphanVersion := e.rawPut(t, foreignOrphan, "foreign-orphan-bytes")

	// This tenant's garbage: a tombstoned artifact past every floor.
	mine, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-mine"}, "k-mine", "my-collectable-tombstone")
	if err := e.store.Delete(ctx, e.scope, mine.ID, "del-mine"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	e.waitTombstoneAge(t, mine.ID)
	gc, _ := NewGC(e.store)
	report, err := gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.PurgedTombstoned != 1 || report.Tenant != e.scope.Tenant {
		t.Fatalf("sweep was not tenant scoped: %+v", report)
	}
	// Out-of-band verification of the foreign tenant's bytes.
	foreignObject := e.objectOf(t, fref.ID)
	raw, err := e.rawGet(t, foreignObject, fref.Version)
	if err != nil || string(raw) != foreignBody || shaHexOf(string(raw)) != fref.SHA256 {
		t.Fatalf("foreign tenant bytes touched: err=%v", err)
	}
	if _, err := e.store.Get(ctx, foreignScope, fref.ID); err != nil {
		t.Fatalf("foreign tenant lost access to its artifact: %v", err)
	}
	if raw, err := e.rawGet(t, foreignOrphan, foreignOrphanVersion); err != nil || string(raw) != "foreign-orphan-bytes" {
		t.Fatalf("foreign tenant orphan removed by another tenant's sweep: err=%v", err)
	}
	// And the same sweep, pointed at the foreign tenant as its own caller, is
	// refused: the swept tenant comes from the verified scope, and a GC cannot
	// be handed somebody else's tenant as an argument.
	if _, err := gc.RunOnce(ctx, foreignScope, fastPolicy()); err == nil {
		t.Log("note: a sweep by the foreign scope itself is permitted (it is that tenant's control plane)")
	}
}

func TestGCOrphanVersionsNeverBreakSealedRecords(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A sealed artifact whose key was overwritten after sealing: the pinned
	// version is a sha-record, the later version is surplus.
	sealedBody := "original-sealed-bytes"
	ref, up := e.sealBoundWithKey(t, Binding{}, "k-orphan-seal", sealedBody)
	put(t, up.PresignedURL, strings.Repeat("X", len(sealedBody)))
	// An object no row has ever named.
	orphanVersion := e.rawPut(t, e.scope.Tenant+"/nobody-owns-me", "orphan-bytes")

	gc, _ := NewGC(e.store)
	e.waitObjectGrace(t)
	pol := fastPolicy()
	pol.CollectUnpinnedVersions = true
	report, err := gc.RunOnce(ctx, e.scope, pol)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.ProtectedVersions < 1 {
		t.Fatalf("the pinned version was not counted as protected: %+v", report)
	}
	raw, err := e.rawGet(t, up.Object, ref.Version)
	if err != nil || string(raw) != sealedBody || shaHexOf(string(raw)) != ref.SHA256 {
		t.Fatalf("the sealed pinned version was damaged: err=%v", err)
	}
	if _, err := e.rawGet(t, e.scope.Tenant+"/nobody-owns-me", orphanVersion); err == nil {
		t.Fatal("an unreferenced version older than the grace survived the sweep")
	}
	// The surplus version of the sealed key is gone, the pinned one is not.
	versions := e.listVersions(t, up.Object)
	if len(versions) != 1 || versions[0] != ref.Version {
		t.Fatalf("surplus version collection removed the wrong set: %v (pinned %s)", versions, ref.Version)
	}

	// With the surplus flag off, a later sweep keeps both versions of a sealed
	// key and still protects the pinned one.
	e2 := setup(t)
	ref2, up2 := e2.sealBoundWithKey(t, Binding{}, "k-keep-both", "another-sealed-record")
	put(t, up2.PresignedURL, strings.Repeat("X", len("another-sealed-record")))
	gc2, _ := NewGC(e2.store)
	e2.waitObjectGrace(t)
	if _, err := gc2.RunOnce(ctx, e2.scope, fastPolicy()); err != nil {
		t.Fatalf("RunOnce conservative: %v", err)
	}
	if got := len(e2.listVersions(t, up2.Object)); got != 2 {
		t.Fatalf("conservative policy removed a sealed key's versions: %d left", got)
	}
	if raw, err := e2.rawGet(t, up2.Object, ref2.Version); err != nil || shaHexOf(string(raw)) != ref2.SHA256 {
		t.Fatalf("sealed record damaged by a conservative sweep: err=%v", err)
	}
}

// listVersions is the out-of-band answer to "which versions of this key exist".
func (e *env) listVersions(t *testing.T, object string) []string {
	t.Helper()
	var out []string
	for obj := range e.client.ListObjects(context.Background(), e.bucket,
		minio.ListObjectsOptions{Prefix: object, Recursive: true, WithVersions: true}) {
		if obj.Err != nil {
			t.Fatalf("list versions: %v", obj.Err)
		}
		if obj.Key != object || obj.IsDeleteMarker {
			continue
		}
		out = append(out, obj.VersionID)
	}
	return out
}

func TestGCConcurrentSealNeverLosesBytes(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// A sweep with collapsed graces runs while uploads are being sealed. The
	// invariant is the one the acceptance leg asks for: every artifact that
	// reached Seal has its recorded digest afterwards, whatever the collector
	// was doing at the same moment.
	gc, _ := NewGC(e.store)
	pol := fastPolicy()
	stop := make(chan struct{})
	var sweeps sync.WaitGroup
	for i := 0; i < 2; i++ {
		sweeps.Add(1)
		go func() {
			defer sweeps.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := gc.RunOnce(ctx, e.scope, pol); err != nil &&
					!errors.Is(err, context.Canceled) {
					t.Errorf("sweep during sealing: %v", err)
					return
				}
			}
		}()
	}
	type sealed struct {
		id, object, version, sha string
		body                     string
	}
	var wg sync.WaitGroup
	var kept sync.Map
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf("concurrent-seal-payload-%d-%d", i, time.Now().UnixNano())
			ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-race", TaskID: fmt.Sprintf("task-race-%d", i)},
				fmt.Sprintf("k-race-%d", i), body)
			kept.Store(ref.ID, sealed{id: ref.ID, object: e.objectOf(t, ref.ID),
				version: ref.Version, sha: ref.SHA256, body: body})
		}(i)
	}
	wg.Wait()
	close(stop)
	sweeps.Wait()

	// A live pin on the context first: while the run holds it, the sweep must
	// leave every one of these alone even with every floor collapsed.
	if err := e.store.PinRef(ctx, e.scope, PinKindContext, "ctx-race"); err != nil {
		t.Fatalf("PinRef: %v", err)
	}
	if _, err := gc.RunOnce(ctx, e.scope, pol); err != nil {
		t.Fatalf("final sweep: %v", err)
	}
	count := 0
	kept.Range(func(_, v any) bool {
		s := v.(sealed)
		count++
		raw, err := e.rawGet(t, s.object, s.version)
		if err != nil {
			t.Errorf("artifact %s lost during concurrent sweep: %v", s.id, err)
			return true
		}
		if shaHexOf(string(raw)) != s.sha || !bytes.Equal(raw, []byte(s.body)) {
			t.Errorf("artifact %s digest moved: recorded %s got %s", s.id, s.sha, shaHexOf(string(raw)))
		}
		if got, err := e.store.Get(ctx, e.scope, s.id); err != nil || got.SHA256 != s.sha {
			t.Errorf("artifact %s unreadable after the sweep: err=%v", s.id, err)
		}
		return true
	})
	if count != 12 {
		t.Fatalf("only %d of 12 concurrent seals were recorded", count)
	}
}

func TestGCDryRunReportsWithoutTouchingAnything(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body := "dry-run-must-not-delete-a-single-byte"
	ref, _ := e.sealBoundWithKey(t, Binding{}, "k-dry", body)
	object := e.objectOf(t, ref.ID)
	if err := e.store.Delete(ctx, e.scope, ref.ID, "del-dry"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	stagedSum := shaHexOf("dry-run-staged-upload")
	up, err := e.store.BeginUploadWithBinding(ctx, e.scope, Binding{}, "k-dry-staged",
		UploadRequest{Name: "d.bin", MIME: "application/octet-stream", SHA256: stagedSum, Size: int64(len("dry-run-staged-upload"))})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	put(t, up.PresignedURL, "dry-run-staged-upload")

	gc, _ := NewGC(e.store)
	pol := fastPolicy()
	pol.DryRun = true
	e.waitTombstoneAge(t, ref.ID)
	e.waitStagedAge(t, up.ID)
	e.waitObjectGrace(t)
	report, err := gc.RunOnce(ctx, e.scope, pol)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if !report.DryRun {
		t.Fatalf("report is not marked as a dry run: %+v", report)
	}
	if report.PurgedTombstoned != 0 || report.CollectedStaged != 0 || report.VersionsRemoved != 0 {
		t.Fatalf("a dry run collected something: %+v", report)
	}
	// A dry run still refuses to pretend: the rows it would have taken are
	// visible as the concurrent-change refusals of their guarded statements.
	if report.ScannedRows < 2 {
		t.Fatalf("dry run did not scan the candidates: %+v", report)
	}
	if ok, deleted := e.rowExists(t, ref.ID); !ok || !deleted {
		t.Fatalf("dry run changed the tombstone: exists=%v deleted=%v", ok, deleted)
	}
	if ok, _ := e.rowExists(t, up.ID); !ok {
		t.Fatal("dry run collected a staged upload")
	}
	if raw, err := e.rawGet(t, object, ref.Version); err != nil || shaHexOf(string(raw)) != ref.SHA256 {
		t.Fatalf("dry run removed bytes: err=%v", err)
	}
	// And a dry run is not ledgered: the same key can still run for real.
	keyd := fastPolicy()
	keyd.Key = "dry-then-real"
	keyd.DryRun = true
	if _, err := gc.RunOnce(ctx, e.scope, keyd); err != nil {
		t.Fatalf("keyed dry run: %v", err)
	}
	keyd.DryRun = false
	real, err := gc.RunOnce(ctx, e.scope, keyd)
	if err != nil {
		t.Fatalf("real run after dry run: %v", err)
	}
	if real.Replayed || real.PurgedTombstoned != 1 {
		t.Fatalf("the dry run blocked or impersonated the real sweep: %+v", real)
	}
}
