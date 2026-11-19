package conformance

import (
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestCreateThreadIdempotentReplayAndMismatch pins the thread creation
// contract: the same (scope, key, body) replays the original record and a
// different body under the same key is rejected with KEY_REUSE_MISMATCH.
func TestCreateThreadIdempotentReplayAndMismatch(t *testing.T) {
	ctx, _, store, scope := testStore(t)

	const (
		key       = "thread-idem-key"
		contextID = "ctx-" + "00000000-0000-4000-8000-000000000001"
	)

	first, err := store.CreateThread(ctx, scope, contextID, key)
	mustNoErr(t, err, "CreateThread (first)")
	if first.ContextID != contextID || first.Scope != scope {
		t.Fatalf("conformance: created thread carries %q / %+v, want %q / %+v",
			first.ContextID, first.Scope, contextID, scope)
	}
	if first.ID == "" {
		t.Fatal("conformance: created thread has an empty id")
	}
	if first.Epoch != 0 {
		t.Fatalf("conformance: a fresh thread must have epoch 0, got %d", first.Epoch)
	}

	replay, err := store.CreateThread(ctx, scope, contextID, key)
	mustNoErr(t, err, "CreateThread (replay)")
	if replay.ID != first.ID {
		t.Fatalf("conformance: replaying the same key returned thread %s, want the original %s",
			replay.ID, first.ID)
	}
	if replay.ContextID != first.ContextID || !replay.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("conformance: replay returned %q created at %s, want %q created at %s",
			replay.ContextID, replay.CreatedAt, first.ContextID, first.CreatedAt)
	}

	// The same key with a different body must not create or return anything.
	_, err = store.CreateThread(ctx, scope, "ctx-"+contracts.NewID(), key)
	mustCode(t, err, contracts.ErrKeyReuse, "CreateThread with a reused key and a different context")

	// A different key for the same context resolves to the original thread:
	// one context is one lane, never two.
	other, err := store.CreateThread(ctx, scope, contextID, "thread-idem-key-2")
	mustNoErr(t, err, "CreateThread (same context, new key)")
	if other.ID != first.ID {
		t.Fatalf("conformance: recreating the same context under a new key returned %s, want %s",
			other.ID, first.ID)
	}

	// A different subject inside the same tenant/deployment has its own key
	// namespace and its own lane for the same context id.
	peer := scope
	peer.Subject = "usr-" + contracts.NewID()
	elsewhere, err := store.CreateThread(ctx, peer, contextID, key)
	mustNoErr(t, err, "CreateThread (same key in another subject scope)")
	if elsewhere.ID == first.ID {
		t.Fatal("conformance: two subjects shared one thread row")
	}

	if got, err := store.GetThread(ctx, scope, first.ID); err != nil || got.ID != first.ID {
		t.Fatalf("conformance: GetThread returned %+v (err %v), want thread %s", got, err, first.ID)
	}
}

// TestCreateRunIdempotentReplayAndMismatch pins the run creation contract:
// replaying the identical body returns the original run, and a differing task
// id or payload under the same key fails with KEY_REUSE_MISMATCH.
func TestCreateRunIdempotentReplayAndMismatch(t *testing.T) {
	ctx, _, store, scope := testStore(t)
	thread := newThread(t, ctx, store, scope)

	const key = "run-idem-key"
	payload := jsonPayload(t, "original")

	first, err := store.CreateRun(ctx, scope, thread.ID, "task-1", key, payload)
	mustNoErr(t, err, "CreateRun (first)")
	if first.Sequence != 1 {
		t.Fatalf("conformance: the first run of a thread must have sequence 1, got %d", first.Sequence)
	}
	if first.Status != "pending" {
		t.Fatalf("conformance: a new run must be pending, got %q", first.Status)
	}
	if first.ThreadID != thread.ID || first.TaskID != "task-1" {
		t.Fatalf("conformance: created run is %s/%s, want %s/task-1",
			first.ThreadID, first.TaskID, thread.ID)
	}
	// jsonb reorders keys and normalises whitespace, so compare canonical
	// digests of the JSON values, never raw text.
	storedDigest, err := contracts.Digest(json.RawMessage(first.Payload))
	mustNoErr(t, err, "digest stored payload")
	wantDigest, err := contracts.Digest(json.RawMessage(payload))
	mustNoErr(t, err, "digest expected payload")
	if storedDigest != wantDigest {
		t.Fatalf("conformance: stored payload %s does not round-trip %s", first.Payload, payload)
	}

	replay, err := store.CreateRun(ctx, scope, thread.ID, "task-1", key, payload)
	mustNoErr(t, err, "CreateRun (replay)")
	if !sameRun(first, replay) {
		t.Fatalf("conformance: replaying the same run key returned %s, want the original %s",
			replay.ID, first.ID)
	}
	if replay.Sequence != first.Sequence || replay.Status != first.Status {
		t.Fatalf("conformance: replay returned seq %d/%s, want %d/%s",
			replay.Sequence, replay.Status, first.Sequence, first.Status)
	}

	// Same key, different payload. The store compares canonical digests, so
	// the differing document must differ in value, not whitespace.
	_, err = store.CreateRun(ctx, scope, thread.ID, "task-1", key, jsonPayload(t, "changed"))
	mustCode(t, err, contracts.ErrKeyReuse, "CreateRun with a reused key and a different payload")

	// Same key, different task id.
	_, err = store.CreateRun(ctx, scope, thread.ID, "task-2", key, payload)
	mustCode(t, err, contracts.ErrKeyReuse, "CreateRun with a reused key and a different task id")

	// Same key, same value in a different textual form: a replay, not a
	// mismatch — the fingerprint covers the canonical payload.
	reformatted, err := store.CreateRun(ctx, scope, thread.ID, "task-1", key,
		json.RawMessage(`{ "n" : 1, "label" : "original" }`))
	mustNoErr(t, err, "CreateRun with a key-equivalent payload in a different textual form")
	if !sameRun(first, reformatted) {
		t.Fatalf("conformance: key-equivalent payload returned %s, want the original %s",
			reformatted.ID, first.ID)
	}

	// Nothing above appended to the FIFO.
	runs, err := store.ListRuns(ctx, scope, thread.ID)
	mustNoErr(t, err, "ListRuns")
	if len(runs) != 1 {
		ids := make([]string, 0, len(runs))
		for _, r := range runs {
			ids = append(ids, r.ID)
		}
		t.Fatalf("conformance: thread holds %d runs %v after idempotent replays, want exactly 1", len(runs), ids)
	}

	// A fresh key appends the next FIFO position.
	next, err := store.CreateRun(ctx, scope, thread.ID, "task-2", "run-idem-key-2", jsonPayload(t, "second"))
	mustNoErr(t, err, "CreateRun (second run)")
	if next.Sequence != 2 {
		t.Fatalf("conformance: the second run must have sequence 2, got %d", next.Sequence)
	}

	// Key scope is the thread, not the tenant: the same key in another thread
	// is a different run.
	other := newThread(t, ctx, store, scope)
	cross, err := store.CreateRun(ctx, scope, other.ID, "task-1", key, payload)
	mustNoErr(t, err, "CreateRun (same key in another thread)")
	if cross.ID == first.ID || cross.ThreadID != other.ID {
		t.Fatalf("conformance: run key leaked across threads: %+v", cross)
	}
}

// TestScopeIsolationIsNotFound proves that the State API never confirms the
// existence of a foreign thread or run: wrong subject, wrong deployment and
// wrong tenant all answer NOT_FOUND on every read path.
func TestScopeIsolationIsNotFound(t *testing.T) {
	ctx, _, store, scope := testStore(t)
	thread := newThread(t, ctx, store, scope)
	run := createRun(t, ctx, store, scope, thread.ID, "task-iso", jsonPayload(t, "iso"))

	otherTenant := contracts.Scope{Tenant: "tnt-" + contracts.NewID(), Deployment: scope.Deployment, Subject: scope.Subject}
	otherDeployment := contracts.Scope{Tenant: scope.Tenant, Deployment: "dep-" + contracts.NewID(), Subject: scope.Subject}
	otherSubject := contracts.Scope{Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: "usr-" + contracts.NewID()}
	foreign := map[string]contracts.Scope{
		"other tenant":     otherTenant,
		"other deployment": otherDeployment,
		"other subject":    otherSubject,
	}

	for name, s := range foreign {
		if _, err := store.GetThread(ctx, s, thread.ID); contracts.Code(err) != contracts.ErrNotFound {
			t.Errorf("conformance: GetThread from %s: want NOT_FOUND, got %v", name, err)
		}
		if _, err := store.GetThreadByContext(ctx, s, thread.ContextID); contracts.Code(err) != contracts.ErrNotFound {
			t.Errorf("conformance: GetThreadByContext from %s: want NOT_FOUND, got %v", name, err)
		}
		if _, err := store.GetRun(ctx, s, run.ID); contracts.Code(err) != contracts.ErrNotFound {
			t.Errorf("conformance: GetRun from %s: want NOT_FOUND, got %v", name, err)
		}
		if _, err := store.ListRuns(ctx, s, thread.ID); contracts.Code(err) != contracts.ErrNotFound {
			t.Errorf("conformance: ListRuns from %s: want NOT_FOUND, got %v", name, err)
		}
		// A foreign caller cannot even append work to somebody else's lane.
		// The key must be a valid identifier: a descriptive map name with
		// spaces would fail key validation before ever reaching the scope
		// check, masking the very behaviour this assertion pins.
		if _, err := store.CreateRun(ctx, s, thread.ID, "task-iso", "k-"+contracts.NewID(), jsonPayload(t, "x")); contracts.Code(err) != contracts.ErrNotFound {
			t.Errorf("conformance: CreateRun into a foreign thread from %s: want NOT_FOUND, got %v", name, err)
		}
	}

	// The owner still sees everything.
	if got, err := store.GetThread(ctx, scope, thread.ID); err != nil || got.ID != thread.ID {
		t.Fatalf("conformance: owner lost its own thread: %v (%+v)", err, got)
	}
	if got, err := store.GetRun(ctx, scope, run.ID); err != nil || got.ID != run.ID {
		t.Fatalf("conformance: owner lost its own run: %v (%+v)", err, got)
	}

	// Claims are deployment-level: another deployment never sees these runs,
	// not even as a candidate.
	empty, err := store.ClaimRun(ctx, otherTenant.Tenant, otherTenant.Deployment, "worker-foreign", 60_000_000)
	mustNoErr(t, err, "ClaimRun in a foreign deployment")
	if empty != nil {
		t.Fatalf("conformance: a foreign deployment claimed %s", leaseOf(empty))
	}

	// And a lease operation with a foreign scope is refused as NOT_FOUND,
	// never as a successful no-op.
	if _, err := store.Heartbeat(ctx, otherSubject, run.ID, "worker-a", 1, 60_000_000); contracts.Code(err) != contracts.ErrNotFound {
		t.Errorf("conformance: Heartbeat from a foreign subject: want NOT_FOUND, got %v", err)
	}
	if err := store.Release(ctx, otherDeployment, run.ID, "worker-a", 1, "done"); contracts.Code(err) != contracts.ErrNotFound {
		t.Errorf("conformance: Release from a foreign deployment: want NOT_FOUND, got %v", err)
	}
}
