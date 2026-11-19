package artifacts

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The three legs below are Goodall's red reproduction from
// helper24/space-canonical (6301cad5), taken as the acceptance axiom for
// DEF-H24-8 rather than reinvented here; only the duplicate shaOf helper and the
// import list are adjusted to this package. The FIX is this lane's, because the
// write path of the upload key is one owner's job: 6301cad5 patched the same
// arbitration this package already runs (advisory xact lock + re-read, the
// DEF-STAND-1 arbiter), and re-landing it would have moved that arbiter twice.
//
// DEF-H24-8. The runner names an upload by (name, digest), so the idempotency key
// a turn used for "report.txt" is the key the NEXT turn will use for the same
// bytes. Deleting a chat tombstones the artifact but left that key reserved
// forever, so every later turn that saved the same file hit
// CONFLICT "upload key was deleted" and the user could never save that file
// again in that tenant. Deleting must free the key: the next upload is a new
// generation with a new id and a new object path, while a key that names a LIVE
// artifact keeps its two existing meanings - same request replays the same
// upload, a different request under a live key stays KEY_REUSE_MISMATCH.

func TestUploadKeyIsFreedByDeletion(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	key := "h24-8-reuse-" + t.Name()
	body := "the first attempt at this file"
	first, firstUp := e.uploadSeal(t, key, body)

	if err := e.store.Delete(ctx, e.scope, first.ID, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := e.store.Get(ctx, e.scope, first.ID); err == nil {
		t.Fatal("a deleted artifact must stop reading as found")
	} else {
		codeIs(t, err, string(contracts.ErrNotFound))
	}

	body2 := "the second attempt at the same file, after the chat was deleted"
	second, err := e.store.BeginUpload(ctx, e.scope, key, UploadRequest{
		Name: "a.bin", MIME: "application/octet-stream", SHA256: shaOf(body2), Size: int64(len(body2)),
	})
	if err != nil {
		t.Fatalf("re-upload under a deleted key must be accepted: %v", err)
	}
	if second.ID == first.ID {
		t.Fatal("a deleted artifact must not be resurrected under its own id")
	}
	if second.ID == firstUp.ID {
		t.Fatal("the new generation must have its own id")
	}

	put(t, second.PresignedURL, body2)
	sealed, err := e.store.Seal(ctx, e.scope, second.ID)
	if err != nil {
		t.Fatalf("Seal the new generation: %v", err)
	}
	rc, opened, err := e.store.Open(ctx, e.scope, sealed.ID)
	if err != nil {
		t.Fatalf("Open the new generation: %v", err)
	}
	readAll(t, rc, body2)
	if opened.Version != sealed.Version || opened.SHA256 != shaOf(body2) {
		t.Fatalf("the new generation reads a different version than it sealed: %s vs %s", opened.Version, sealed.Version)
	}

	// The tombstone still reads as gone, and the freed key did not quietly
	// hand back the old row's object path.
	if _, err := e.store.Get(ctx, e.scope, first.ID); err == nil {
		t.Fatal("the deleted generation became visible again")
	}
	if second.Object == firstUp.Object {
		t.Fatal("a new generation must not reuse the deleted object path")
	}
}

func TestLiveUploadKeyKeepsReplayAndMismatch(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	key := "h24-8-live-" + t.Name()
	body := "a live artifact holds its key"
	req := UploadRequest{Name: "a.bin", MIME: "application/octet-stream", SHA256: shaOf(body), Size: int64(len(body))}

	first, err := e.store.BeginUpload(ctx, e.scope, key, req)
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	again, err := e.store.BeginUpload(ctx, e.scope, key, req)
	if err != nil {
		t.Fatalf("replay of an identical request must stay idempotent: %v", err)
	}
	if again.ID != first.ID || again.Object != first.Object {
		t.Fatalf("replay must name the same upload: got %s/%s want %s/%s",
			again.ID, again.Object, first.ID, first.Object)
	}

	// While the key names a live artifact, a different request under it is a
	// reuse, not a second generation.
	if _, err := e.store.BeginUpload(ctx, e.scope, key, UploadRequest{
		Name: "a.bin", MIME: "application/octet-stream", SHA256: shaOf("other bytes"), Size: 10,
	}); err == nil {
		t.Fatal("a live key must refuse a different request")
	} else {
		codeIs(t, err, string(contracts.ErrKeyReuse))
	}

	other := "different bytes under the same key, once freed"
	put(t, again.PresignedURL, body)
	if _, err := e.store.Seal(ctx, e.scope, again.ID); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := e.store.Delete(ctx, e.scope, again.ID, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// With the key freed, a different body is no longer a reuse of a live key.
	if _, err := e.store.BeginUpload(ctx, e.scope, key, UploadRequest{
		Name: "a.bin", MIME: "application/octet-stream", SHA256: shaOf(other), Size: int64(len(other)),
	}); err != nil {
		t.Fatalf("a deleted key must accept a different request too: %v", err)
	}
}

func TestConcurrentReuploadUnderFreedKeyIsOneGeneration(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	key := "h24-8-race-" + t.Name()
	body := "two turns raced to save the same file"
	ref, _ := e.uploadSeal(t, key, body)
	if err := e.store.Delete(ctx, e.scope, ref.ID, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	req := UploadRequest{Name: "a.bin", MIME: "application/octet-stream", SHA256: shaOf(body), Size: int64(len(body))}
	const racers = 4
	type outcome struct {
		id  string
		err error
	}
	var wg sync.WaitGroup
	out := make([]outcome, racers)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			up, err := e.store.BeginUpload(ctx, e.scope, key, req)
			out[i] = outcome{id: up.ID, err: err}
		}(i)
	}
	close(start)
	wg.Wait()

	ids := map[string]int{}
	for i, o := range out {
		if o.err != nil {
			t.Fatalf("racer %d: concurrent re-upload under a freed key must not fail: %v", i, o.err)
		}
		ids[o.id]++
	}
	if len(ids) != 1 {
		t.Fatalf("concurrent identical requests must agree on one generation, got %d ids", len(ids))
	}
	for id := range ids {
		if id == ref.ID {
			t.Fatal("the race must not hand back the deleted generation")
		}
	}
}
