package statejournal

import (
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestMutateKVWithoutJournalEvents(t *testing.T) {
	ctx, _, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "kv-" + rand.Text(), Deployment: "app", Subject: "user"}
	lease, err := store.AcquireOrCreateSession(ctx, scope, "session", "adk", 1, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	first := KVBatch{Key: "op-a", KV: []KVMutation{
		{ScopeType: "session", Namespace: "ns", Key: "a", Payload: Payload{Inline: []byte("A")}},
		{ScopeType: "session", Namespace: "ns", Key: "b", Payload: Payload{Inline: []byte("B")}},
	}}
	receipt, err := store.MutateKV(ctx, scope, "session", lease, false, first)
	if err != nil || receipt != (AppendReceipt{}) {
		t.Fatalf("first receipt %+v: %v", receipt, err)
	}
	second := KVBatch{Key: "op-b", KV: []KVMutation{
		{ScopeType: "session", Namespace: "ns", Key: "a", ExpectedVersion: 1, Payload: Payload{Inline: []byte("new A")}},
		{ScopeType: "session", Namespace: "ns", Key: "b", ExpectedVersion: 5, Payload: Payload{Inline: []byte("new B")}},
	}}
	if _, err := store.MutateKV(ctx, scope, "session", lease, false, second); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("expected batch CAS conflict, got %v", err)
	}
	second.KV[1].ExpectedVersion = 1
	if _, err := store.MutateKV(ctx, scope, "session", lease, false, second); err != nil {
		t.Fatal(err)
	}
	if replay, err := store.MutateKV(ctx, scope, "session", lease, false, first); err != nil || replay != receipt {
		t.Fatalf("old ACK replay %+v: %v", replay, err)
	}
	first.KV[0].Payload = Payload{Inline: []byte("different")}
	if _, err := store.MutateKV(ctx, scope, "session", lease, false, first); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("mutated idempotency key: %v", err)
	}
	page, err := store.GetEvents(ctx, scope, "session", 0, 100)
	if err != nil || page.HeadSeq != 0 || len(page.Events) != 0 || page.Epoch != lease.Epoch {
		t.Fatalf("KV mutation polluted session journal: %+v %v", page, err)
	}
	rows, err := store.ListKV(ctx, scope, KVQuery{Session: "session", ScopeType: "session", Namespace: "ns", Keys: []string{"a", "b"}, Limit: 2})
	if err != nil || len(rows) != 2 || rows[0].Version != 2 || rows[1].Version != 2 {
		t.Fatalf("batch read %+v: %v", rows, err)
	}
	if _, err := store.ListKV(ctx, scope, KVQuery{Session: "session", ScopeType: "session", Namespace: kvReceiptNamespace, Limit: 10}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("private receipts unexpectedly exposed: %v", err)
	}
}

func TestBatchCheckpointTuplesAndCatalog(t *testing.T) {
	ctx, _, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "cp-" + rand.Text(), Deployment: "app", Subject: "user"}
	for _, session := range []string{"parent", "child"} {
		lease, err := store.AcquireOrCreateSession(ctx, scope, session, "langgraph", 1, "worker", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if session == "parent" {
			continue
		}
		parent := "parent"
		title := "LangGraph child"
		if _, err := store.MutateKV(ctx, scope, session, lease, false, KVBatch{Key: "catalog",
			Catalog: &OpenClawCatalogUpdate{Title: &title, ParentSessionID: &parent}}); err != nil {
			t.Fatal(err)
		}
		catalog, err := store.ListCatalog(ctx, scope, CatalogQuery{Harness: "langgraph", Limit: 5})
		if err != nil || len(catalog.Sessions) != 2 || catalog.Sessions[0].ID != session ||
			catalog.Sessions[0].ParentSessionID == nil || *catalog.Sessions[0].ParentSessionID != parent {
			t.Fatalf("generic catalog: %+v %v", catalog, err)
		}
		if err := store.DeleteSession(ctx, scope, parent); err != nil {
			t.Fatal(err)
		}
		title = "Child after parent deletion"
		if _, err := store.MutateKV(ctx, scope, session, lease, false, KVBatch{Key: "rename-child",
			Catalog: &OpenClawCatalogUpdate{Title: &title, ParentSessionID: &parent}}); err != nil {
			t.Fatalf("retaining established lineage after parent deletion: %v", err)
		}
		for _, id := range []string{"a", "b", "c"} {
			meta := json.RawMessage(`{"source":"loop"}`)
			if id == "b" {
				meta = json.RawMessage(`{"source":"other"}`)
			}
			req := CheckpointCommit{Key: "commit-" + id, Checkpoint: Checkpoint{
				ID: id, Codec: "text", Metadata: meta, ChannelVersions: json.RawMessage(`{}`),
				Payload: Payload{Inline: []byte(id)},
			}, Writes: []PendingWrite{{TaskID: "node", Index: 0, Channel: "output", Codec: "text", Payload: Payload{Inline: []byte(id)}}}}
			if _, err := store.PutCheckpoint(ctx, scope, session, lease, req); err != nil {
				t.Fatal(err)
			}
		}
		page, err := store.ListCheckpointTuples(ctx, scope, session, "", "", 10, json.RawMessage(`{"source":"loop"}`), false)
		if err != nil || len(page.IDs) != 2 || page.IDs[0] != "c" || page.IDs[1] != "a" {
			t.Fatalf("checkpoint metadata page %+v: %v", page, err)
		}
		for _, tuple := range page.Tuples {
			if len(tuple.Writes) != 1 || string(tuple.Writes[0].Payload.Inline) != tuple.Checkpoint.ID {
				t.Fatalf("pending writes attached to wrong tuple: %+v", tuple)
			}
		}
	}
}
