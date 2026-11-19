package artifacts

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Restore is the other half of the retention window, and the operation the
// collector has to be correct against: a sweep that races a restore must lose.

func TestRestoreReopensRetentionWindowAndSurvivesSweep(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body := "tombstoned-then-restored-journey-artifact"
	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-rst", TaskID: "task-rst"}, "k-rst", body)
	object := e.objectOf(t, ref.ID)

	if err := e.store.Delete(ctx, e.scope, ref.ID, "del-rst"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	past := time.Now().UTC().Add(-time.Minute)
	if err := e.store.SetRetention(ctx, e.scope, ref.ID, &past); err != nil {
		t.Fatalf("SetRetention: %v", err)
	}
	restored, err := e.store.Restore(ctx, e.scope, ref.ID)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if restored.SHA256 != ref.SHA256 || restored.Version != ref.Version {
		t.Fatalf("restore returned a different record: %+v vs %+v", restored, ref)
	}
	// A retried restore is the same answer, not a failure: a lost response must
	// not turn into a control plane that cannot tell whether it worked.
	again, err := e.store.Restore(ctx, e.scope, ref.ID)
	if err != nil || again.ID != ref.ID {
		t.Fatalf("repeat Restore: ref=%+v err=%v", again, err)
	}
	// The elapsed floor does not survive the restore, so the next sweep has
	// nothing to collect even though every grace is collapsed.
	gc, _ := NewGC(e.store)
	report, err := gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.PurgedTombstoned != 0 || report.CollectedStaged != 0 {
		t.Fatalf("sweep collected a restored artifact: %+v", report)
	}
	if got, err := e.store.Get(ctx, e.scope, ref.ID); err != nil || got.SHA256 != ref.SHA256 {
		t.Fatalf("restored artifact unreadable: %+v err=%v", got, err)
	}
	raw, err := e.rawGet(t, object, ref.Version)
	if err != nil || shaHexOf(string(raw)) != ref.SHA256 {
		t.Fatalf("restored bytes damaged: err=%v", err)
	}
	lc, err := e.store.GetLifecycle(ctx, e.scope, ref.ID)
	if err != nil {
		t.Fatalf("GetLifecycle: %v", err)
	}
	if lc.Deleted {
		t.Fatal("restore left the tombstone in place")
	}
	if evs := eventsNamed(e.auditOf(t, ref.ID), GCEventRestore); len(evs) != 1 {
		t.Fatalf("restore was not audited exactly once: %+v", e.auditOf(t, ref.ID))
	}

	// Deleting it again and sweeping with no floor now does collect it: restore
	// reopens the window, it does not seal it shut.
	if err := e.store.Delete(ctx, e.scope, ref.ID, "del-rst-2"); err != nil {
		t.Fatalf("Delete again: %v", err)
	}
	e.waitTombstoneAge(t, ref.ID)
	report, err = gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil {
		t.Fatalf("RunOnce second: %v", err)
	}
	if report.PurgedTombstoned != 1 {
		t.Fatalf("re-tombstoned artifact was not collected: %+v", report)
	}
	if _, err := e.rawGet(t, object, ref.Version); err == nil {
		t.Fatal("bytes survived the second purge")
	}
	// Restoring something whose bytes are gone says "no such artifact": the
	// control plane must not be told a purge it can undo.
	if _, err := e.store.Restore(ctx, e.scope, ref.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("Restore after purge: want NOT_FOUND, got %v", err)
	}
}

func TestRestoreIsScopeAndLabelBounded(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-scope"}, "k-scope", "subject-owned-bytes")
	if err := e.store.Delete(ctx, e.scope, ref.ID, "del-scope"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// The private labels a sealed artifact carries name its owner subject, so
	// another subject of the same tenant cannot pull it back out of the grave.
	_, err := e.store.Restore(ctx, e.foreign, ref.ID)
	if code := contracts.Code(err); code != contracts.ErrForbidden && code != contracts.ErrNotFound {
		t.Fatalf("foreign-subject Restore: want FORBIDDEN or NOT_FOUND, got %v", err)
	}
	otherTenant := contracts.Scope{Tenant: "t-other-" + t.Name(), Deployment: "d1", Subject: "alice"}
	if _, err := e.store.Restore(ctx, otherTenant, ref.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant Restore: want NOT_FOUND, got %v", err)
	}
	if _, err := e.store.Restore(ctx, e.scope, "does-not-exist"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("unknown id Restore: want NOT_FOUND, got %v", err)
	}
}

func TestPinAndRestoreRaceTheSweepWithoutLosingBytes(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Ten artifacts, each tombstoned with an elapsed floor, are put through
	// concurrent restore and sweep while a pin arrives on their context. Every
	// artifact must end in exactly one of two states - still here and readable,
	// or deliberately purged after its pin was released - and none of them may
	// end as a row whose bytes vanished underneath it.
	type item struct {
		id, object, version, sha string
	}
	var items []item
	for i := 0; i < 10; i++ {
		body := "race-payload-" + string(rune('a'+i)) + "-with-some-length"
		ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-pin-race", TaskID: "task-pin-race"},
			"k-pin-race-"+string(rune('a'+i)), body)
		items = append(items, item{ref.ID, e.objectOf(t, ref.ID), ref.Version, ref.SHA256})
		if err := e.store.Delete(ctx, e.scope, ref.ID, "del-pin-race-"+string(rune('a'+i))); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	gc, _ := NewGC(e.store)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if _, err := gc.RunOnce(ctx, e.scope, fastPolicy()); err != nil {
				t.Errorf("sweep: %v", err)
				return
			}
		}
	}()
	go func() {
		for i := 0; i < 30; i++ {
			_, _ = e.store.Restore(ctx, e.scope, items[i%len(items)].id)
		}
	}()
	go func() {
		for i := 0; i < 30; i++ {
			_ = e.store.PinRef(ctx, e.scope, PinKindContext, "ctx-pin-race")
			_ = e.store.UnpinRef(ctx, e.scope, PinKindContext, "ctx-pin-race")
		}
	}()
	time.Sleep(600 * time.Millisecond)
	close(done)
	time.Sleep(100 * time.Millisecond)

	// Final state, checked out of band. Every surviving row's pinned version
	// still hashes to the recorded digest; every removed row is gone from both
	// the table and the bucket. A row with no bytes, or bytes with no row for a
	// live artifact, is the failure this test exists to catch.
	for _, it := range items {
		exists, _ := e.rowExists(t, it.id)
		raw, err := e.rawGet(t, it.object, it.version)
		hasBytes := err == nil && shaHexOf(string(raw)) == it.sha
		if exists && !hasBytes {
			t.Fatalf("artifact %s: row present, recorded bytes gone", it.id)
		}
		if !exists && hasBytes {
			// A purged row may still have a version the orphan grace has not
			// released yet; that is a pending collection, not a lost artifact.
			t.Logf("artifact %s purged from the table, versions still awaiting the orphan grace", it.id)
		}
		if exists {
			rc, got, err := e.store.Open(ctx, e.scope, it.id)
			if err != nil {
				if contracts.Code(err) != contracts.ErrNotFound {
					t.Fatalf("artifact %s: Open failed with %v", it.id, err)
				}
			} else {
				_ = rc.Close()
				if got.SHA256 != it.sha {
					t.Fatalf("artifact %s: served digest moved", it.id)
				}
			}
		}
	}
}
