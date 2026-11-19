package artifacts

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestGCChaosUnderJourneyRun is the acceptance leg: the collector runs
// continuously, with every floor collapsed and surplus-version collection on,
// while a journey writes, seals, tombstones and restores artifacts.
//
// The claims it has to support are all checked out of band - a direct MinIO GET
// of one exact version id and raw SQL against the metadata table - because the
// point of the leg is that the collector cannot lie about what it left alone:
//
//   - every sealed journey artifact still hashes to the digest recorded at
//     seal time, for the whole duration it is referenced by a live context;
//   - a tombstone whose context is still pinned keeps its bytes, and loses them
//     only after the pin is released;
//   - a lost ack re-issues a sweep without re-doing its work;
//   - an interrupted sweep leaves the plane consistent;
//   - another tenant's artifacts and orphans, in the same bucket, are untouched.
func TestGCChaosUnderJourneyRun(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const (
		contexts = 3
		perRun   = 8
	)

	type record struct {
		id, object, version, sha, body string
		// wasTombstone is the state at the mid-run checkpoint: those are the
		// rows the post-release sweep is supposed to take, and only those.
		wasTombstone bool
	}
	var (
		journeyMu sync.Mutex
		journey   []*record
	)

	// ---------------------------------------------------------------------
	// Foreign tenant: manifest taken BEFORE the storm, checked after.
	// ---------------------------------------------------------------------
	foreign := contracts.Scope{Tenant: e.scope.Tenant + "-foreign", Deployment: "d1", Subject: "carol"}
	type foreignRec struct {
		id, object, version, sha, body string
	}
	var foreignManifest []foreignRec
	for i := 0; i < 3; i++ {
		body := fmt.Sprintf("foreign-tenant-journey-artifact-%d", i)
		up, err := e.store.BeginUploadWithBinding(ctx, foreign, Binding{ContextID: fmt.Sprintf("ctx-f-%d", i)},
			fmt.Sprintf("k-f-%d", i), UploadRequest{
				Name: "f.bin", MIME: "application/octet-stream",
				SHA256: shaHexOf(body), Size: int64(len(body)),
			})
		if err != nil {
			t.Fatalf("foreign BeginUpload: %v", err)
		}
		put(t, up.PresignedURL, body)
		ref, err := e.store.Seal(ctx, foreign, up.ID)
		if err != nil {
			t.Fatalf("foreign Seal: %v", err)
		}
		foreignManifest = append(foreignManifest, foreignRec{ref.ID, up.Object, ref.Version, ref.SHA256, body})
	}
	foreignOrphanKey := foreign.Tenant + "/foreign-orphan-before-storm"
	foreignOrphanVersion := e.rawPut(t, foreignOrphanKey, "foreign-orphan-bytes-before-storm")

	// ---------------------------------------------------------------------
	// Journey: three contexts, pinned as live for the whole run.
	// ---------------------------------------------------------------------
	for i := 0; i < contexts; i++ {
		if err := e.store.PinRef(ctx, e.scope, PinKindContext, fmt.Sprintf("ctx-j-%d", i)); err != nil {
			t.Fatalf("PinRef: %v", err)
		}
	}
	var pinnedPurgeRefusals atomic.Int64

	gc, err := NewGC(e.store)
	if err != nil {
		t.Fatalf("NewGC: %v", err)
	}
	pol := fastPolicy()
	pol.CollectUnpinnedVersions = true

	stop := make(chan struct{})
	var sweepers sync.WaitGroup
	var sweepCount atomic.Int64
	for i := 0; i < 2; i++ {
		sweepers.Add(1)
		go func() {
			defer sweepers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// A short deadline per sweep: a sweep that is cut off mid-flight
				// must leave the plane consistent, which is asserted after the
				// run over every artifact.
				sctx, scancel := context.WithTimeout(ctx, 2*time.Second)
				if _, err := gc.RunOnce(sctx, e.scope, pol); err != nil {
					if !strings.Contains(err.Error(), "context deadline exceeded") {
						t.Errorf("sweeper: %v", err)
						scancel()
						return
					}
				}
				scancel()
				sweepCount.Add(1)
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	var journeys sync.WaitGroup
	for j := 0; j < contexts; j++ {
		journeys.Add(1)
		go func(j int) {
			defer journeys.Done()
			ctxID := fmt.Sprintf("ctx-j-%d", j)
			for i := 0; i < perRun; i++ {
				body := fmt.Sprintf("journey-%d-%d-payload-with-some-length-%d", j, i, time.Now().UnixNano())
				ref, _ := e.sealBoundWithKey(t, Binding{ContextID: ctxID, TaskID: fmt.Sprintf("task-j%d-%d", j, i)},
					fmt.Sprintf("k-j%d-%d", j, i), body)
				rec := &record{id: ref.ID, object: e.objectOf(t, ref.ID),
					version: ref.Version, sha: ref.SHA256, body: body}
				journeyMu.Lock()
				journey = append(journey, rec)
				journeyMu.Unlock()

				// The journey reads what it just wrote, mid-storm.
				if got, err := e.store.Get(ctx, e.scope, rec.id); err != nil || got.SHA256 != rec.sha {
					t.Errorf("artifact unreadable mid-sweep: %v", err)
					return
				}
				// Out-of-band proof, while the collector is running: the exact
				// pinned version still hashes to the recorded digest.
				if raw, err := e.rawGet(t, rec.object, rec.version); err != nil ||
					shaHexOf(string(raw)) != rec.sha {
					t.Errorf("pinned bytes moved mid-sweep: err=%v", err)
					return
				}
				// A user deletes the artifact; the control plane then restores it.
				switch i % 4 {
				case 0:
					if err := e.store.Delete(ctx, e.scope, rec.id, "del-j"+rec.id); err != nil {
						t.Errorf("Delete: %v", err)
						return
					}
					if _, err := e.store.Restore(ctx, e.scope, rec.id); err != nil {
						t.Errorf("Restore: %v", err)
						return
					}
				case 2:
					// Deleting and leaving it tombstoned: the live context pin
					// must keep the bytes through the whole sweep storm.
					if err := e.store.Delete(ctx, e.scope, rec.id, "del2-j"+rec.id); err != nil {
						t.Errorf("Delete tombstone: %v", err)
						return
					}
					// An explicit purge against a live reference is refused.
					if err := e.store.Purge(ctx, e.scope, rec.id); err != nil {
						if contracts.Code(err) == contracts.ErrConflict &&
							strings.Contains(err.Error(), "pinned") {
							pinnedPurgeRefusals.Add(1)
						}
					} else {
						t.Errorf("purge of a pinned artifact succeeded: %s", rec.id)
						return
					}
				}
			}
		}(j)
	}
	journeys.Wait()
	close(stop)
	sweepers.Wait()

	if sweepCount.Load() < 5 {
		t.Fatalf("the collector barely ran during the journey: %d sweeps", sweepCount.Load())
	}
	if pinnedPurgeRefusals.Load() == 0 {
		t.Fatal("no explicit purge was refused for a live reference")
	}

	journeyMu.Lock()
	observed := append([]*record(nil), journey...)
	journeyMu.Unlock()

	// ---------------------------------------------------------------------
	// Invariant 1: nothing the journey sealed was damaged, and nothing was lost.
	// ---------------------------------------------------------------------
	tombstoned := 0
	for _, rec := range observed {
		exists, deleted := e.rowExists(t, rec.id)
		if !exists {
			t.Fatalf("journey artifact %s vanished while its context was pinned", rec.id)
		}
		if deleted {
			tombstoned++
			rec.wasTombstone = true
		}
		raw, err := e.rawGet(t, rec.object, rec.version)
		if err != nil {
			t.Fatalf("journey artifact %s lost its pinned version: %v", rec.id, err)
		}
		if shaHexOf(string(raw)) != rec.sha {
			t.Fatalf("journey artifact %s: sha(GET) != recorded", rec.id)
		}
	}
	if len(observed) != contexts*perRun {
		t.Fatalf("journey produced %d artifacts, want %d", len(observed), contexts*perRun)
	}
	if tombstoned == 0 {
		t.Fatal("the journey left no tombstones, so the pin protection was never exercised")
	}

	// ---------------------------------------------------------------------
	// Invariant 2: a lost ack replays the same sweep instead of doing it again.
	// ---------------------------------------------------------------------
	keyed := fastPolicy()
	keyed.Key = "journey-sweep-1"
	first, err := gc.RunOnce(ctx, e.scope, keyed)
	if err != nil {
		t.Fatalf("keyed sweep: %v", err)
	}
	before := e.tenantRowCount(t, e.scope.Tenant)
	// The ack never arrives: a different collector instance retries the key.
	retryGC, _ := NewGC(e.store)
	retry, err := retryGC.RunOnce(ctx, e.scope, keyed)
	if err != nil {
		t.Fatalf("retry after lost ack: %v", err)
	}
	if !retry.Replayed || retry.SweepID != first.SweepID ||
		retry.PurgedTombstoned != first.PurgedTombstoned ||
		retry.CollectedStaged != first.CollectedStaged {
		t.Fatalf("retry did not replay the original sweep: %+v vs %+v", retry, first)
	}
	if after := e.tenantRowCount(t, e.scope.Tenant); after != before {
		t.Fatalf("the replayed sweep did work: %d rows before, %d after", before, after)
	}
	// Every tombstone is still there: the live pins held them through all of it.
	if got := e.tenantRowCount(t, e.scope.Tenant); got != len(observed) {
		t.Fatalf("row count after pinned sweeps: %d, want %d", got, len(observed))
	}

	// ---------------------------------------------------------------------
	// Invariant 3: an interrupted sweep leaves the plane consistent.
	// ---------------------------------------------------------------------
	tight := fastPolicy()
	tight.Key = "journey-sweep-interrupted"
	tctx, tcancel := context.WithTimeout(ctx, 3*time.Millisecond)
	_, _ = gc.RunOnce(tctx, e.scope, tight) // may fail by design
	tcancel()
	for _, rec := range observed {
		exists, _ := e.rowExists(t, rec.id)
		if !exists {
			t.Fatalf("interrupted sweep removed %s while pinned", rec.id)
		}
		raw, err := e.rawGet(t, rec.object, rec.version)
		if err != nil || shaHexOf(string(raw)) != rec.sha {
			t.Fatalf("interrupted sweep damaged %s: err=%v", rec.id, err)
		}
	}

	// ---------------------------------------------------------------------
	// Invariant 4: the foreign tenant is provably untouched, in the same bucket.
	// ---------------------------------------------------------------------
	for _, f := range foreignManifest {
		if got := e.tenantRowCount(t, foreign.Tenant); got != len(foreignManifest) {
			t.Fatalf("foreign tenant row count: %d, want %d", got, len(foreignManifest))
		}
		raw, err := e.rawGet(t, f.object, f.version)
		if err != nil || shaHexOf(string(raw)) != f.sha || string(raw) != f.body {
			t.Fatalf("foreign tenant artifact %s damaged: err=%v", f.id, err)
		}
		if got, err := e.store.Get(ctx, foreign, f.id); err != nil || got.SHA256 != f.sha {
			t.Fatalf("foreign tenant lost its artifact: %v", err)
		}
	}
	if raw, err := e.rawGet(t, foreignOrphanKey, foreignOrphanVersion); err != nil ||
		string(raw) != "foreign-orphan-bytes-before-storm" {
		t.Fatalf("foreign tenant orphan removed by another tenant's sweep: err=%v", err)
	}

	// ---------------------------------------------------------------------
	// Invariant 5: releasing the pins is what makes the tombstones collectable,
	// and even then the live artifacts of the same contexts survive, with their
	// digests intact.
	// ---------------------------------------------------------------------
	for j := 0; j < contexts; j++ {
		if err := e.store.UnpinRef(ctx, e.scope, PinKindContext, fmt.Sprintf("ctx-j-%d", j)); err != nil {
			t.Fatalf("UnpinRef: %v", err)
		}
	}
	released := fastPolicy()
	released.Key = "journey-sweep-released"
	final, err := gc.RunOnce(ctx, e.scope, released)
	if err != nil {
		t.Fatalf("post-release sweep: %v", err)
	}
	if final.PurgedTombstoned != tombstoned {
		t.Fatalf("post-release sweep collected %d tombstones, want %d", final.PurgedTombstoned, tombstoned)
	}
	live := 0
	for _, rec := range observed {
		exists, deleted := e.rowExists(t, rec.id)
		if rec.wasTombstone {
			// Released and expired: the row and its bytes are expected to be
			// gone, and nothing else may have disappeared.
			if exists {
				t.Fatalf("tombstone %s survived the post-release sweep (deleted=%v)", rec.id, deleted)
			}
			if raw, err := e.rawGet(t, rec.object, rec.version); err == nil && len(raw) > 0 {
				t.Fatalf("tombstone %s was purged from the table but its bytes survived", rec.id)
			}
			continue
		}
		if deleted {
			t.Fatalf("live artifact %s was tombstoned by the sweep itself", rec.id)
		}
		if !exists {
			// Only the tombstoned ones were eligible: a live artifact of a
			// released context is still not garbage.
			t.Fatalf("live artifact %s was collected after the pin release", rec.id)
		}
		live++
		raw, err := e.rawGet(t, rec.object, rec.version)
		if err != nil || shaHexOf(string(raw)) != rec.sha {
			t.Fatalf("live artifact %s damaged by the post-release sweep: err=%v", rec.id, err)
		}
		if got, err := e.store.Get(ctx, e.scope, rec.id); err != nil || got.SHA256 != rec.sha {
			t.Fatalf("live artifact %s unreadable after the post-release sweep: %v", rec.id, err)
		}
	}
	if live != len(observed)-tombstoned {
		t.Fatalf("live artifacts: %d, want %d", live, len(observed)-tombstoned)
	}
	// Named evidence for the acceptance report: recorded digest vs the digest of
	// an out-of-band GET of the pinned version, for a sample of the journey.
	sample := 0
	for _, rec := range observed {
		if sample >= 6 {
			break
		}
		raw, err := e.rawGet(t, rec.object, rec.version)
		if err != nil {
			continue
		}
		t.Logf("chaos sha evidence: artifact=%s recorded=%s… got=%s… pinned_version=%s",
			rec.id[:24], rec.sha[:16], shaHexOf(string(raw))[:16], rec.version)
		sample++
	}
	for _, f := range foreignManifest {
		raw, err := e.rawGet(t, f.object, f.version)
		if err != nil {
			continue
		}
		t.Logf("foreign tenant evidence: artifact=%s recorded=%s… got=%s…",
			f.id[:24], f.sha[:16], shaHexOf(string(raw))[:16])
	}
	t.Logf("chaos evidence: %d sweeps, %d artifacts, %d tombstones purged after release, %d live records intact",
		sweepCount.Load(), len(observed), final.PurgedTombstoned, live)
}

// tenantRowCount counts metadata rows of one tenant straight in the table.
func (e *env) tenantRowCount(t *testing.T, tenant string) int {
	t.Helper()
	var n int
	if err := isolatedPool(t).QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_artifacts.artifacts WHERE tenant = $1`, tenant).Scan(&n); err != nil {
		t.Fatalf("row count: %v", err)
	}
	return n
}
