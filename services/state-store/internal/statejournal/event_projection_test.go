package statejournal

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestJournalVisibilityQueryAndVersionedKVDeletion(t *testing.T) {
	ctx, _, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "view-" + rand.Text(), Deployment: "agent", Subject: "owner"}
	const session = "session"
	lease, err := store.AcquireOrCreateSession(ctx, scope, session, "custom", 1, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	inline := Payload{Inline: []byte("{}")}
	before, err := store.AppendEvents(ctx, scope, session, lease, true, AppendRequest{
		Key: "initial", Events: []EventInput{
			{ID: "root", Type: "message", Role: "user", Codec: "json", Text: "common", Payload: inline},
			{ID: "left", ParentID: "root", Type: "message", Role: "assistant", Codec: "json", Text: "left answer", Payload: inline},
			{ID: "right", ParentID: "root", Type: "message", Role: "assistant", Codec: "json", Text: "right answer", Payload: inline},
		}, KV: []KVMutation{{ScopeType: "store", Namespace: "test", Key: "setting", Payload: inline}},
	})
	if err != nil {
		t.Fatal(err)
	}
	inactive, compacted := false, true
	mutation := AppendRequest{Key: "metadata-change", ExpectedSeq: before.HeadSeq,
		EventPatches: []EventPatch{{Seq: 2, Active: &inactive, Compacted: &compacted}},
		KV:           []KVMutation{{ScopeType: "store", Namespace: "test", Key: "setting", ExpectedVersion: 1, Delete: true}},
	}
	receipt, err := store.AppendEvents(ctx, scope, session, lease, true, mutation)
	if err != nil || receipt.HeadSeq != 4 {
		t.Fatalf("metadata-only commit = %+v, error = %v", receipt, err)
	}
	replayed, err := store.AppendEvents(ctx, scope, session, lease, true, mutation)
	if err != nil || replayed != receipt {
		t.Fatalf("metadata commit replay = %+v, error = %v", replayed, err)
	}
	right, err := store.filteredEvents(ctx, scope, session, 0, 10, "",
		EventFilters{Search: "answer", ActiveOnly: true, BranchLeaf: "right"})
	if err != nil || len(right.Events) != 1 || right.Events[0].ID != "right" {
		t.Fatalf("active search in branch: %+v, error = %v", right, err)
	}
	left, err := store.filteredEvents(ctx, scope, session, 0, 10, "",
		EventFilters{Search: "left", BranchLeaf: "left"})
	if err != nil || len(left.Events) != 1 || left.Events[0].Active || !left.Events[0].Compacted {
		t.Fatalf("archived view: %+v, error = %v", left, err)
	}
	entries, err := store.ListKV(ctx, scope, KVQuery{ScopeType: "store", Namespace: "test", Limit: 10})
	if err != nil || len(entries) != 1 || !entries[0].Deleted || entries[0].Version != 2 {
		t.Fatalf("state tombstone: %+v, error = %v", entries, err)
	}
	foreign := scope
	foreign.Subject = "another-user"
	entries, err = store.ListKV(ctx, foreign, KVQuery{ScopeType: "store", Namespace: "test", Limit: 10})
	if err != nil || len(entries) != 0 {
		t.Fatalf("subject isolation failed: %+v, error = %v", entries, err)
	}
	mutation.Key, mutation.ExpectedSeq, mutation.EventPatches = "stale", receipt.HeadSeq, nil
	mutation.KV[0].Delete = false
	mutation.KV[0].Payload = inline
	if _, err := store.AppendEvents(ctx, scope, session, lease, true, mutation); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("deleted value reused the old revision: %v", err)
	}
}
