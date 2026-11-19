package memory

import (
	"context"
	"testing"
	"time"
)

func TestTenantFenceInventoriesOwnersAndRejectsLateWritesAndReads(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	if _, err := f.s.Remember(ctx, f.id, "first", RememberRequest{Target: Personal, Text: "preference", SourceRefs: []string{"s"}}); err != nil {
		t.Fatal(err)
	}
	f.step(t, 2)
	if err := f.s.Store.FenceTenant(ctx, f.id.TenantID); err != nil {
		t.Fatal(err)
	}
	owners, err := f.s.Store.TenantOwners(ctx, f.id.TenantID)
	if err != nil || len(owners) != 1 || owners[0].UserID != f.id.UserID || owners[0].ActorID != f.id.ActorID {
		t.Fatal(owners, err)
	}
	for _, user := range []string{f.id.UserID, "new-owner"} {
		id := f.id
		id.UserID = user
		_, err := f.s.Remember(ctx, id, "late", RememberRequest{Target: Personal, Text: "late", SourceRefs: []string{"s"}})
		assertCode(t, err, "tenant_deleted")
	}
	_, err = f.s.Search(ctx, f.id, SearchRequest{Scope: "personal", Query: "preference"})
	assertCode(t, err, "tenant_deleted")
	owners, err = f.s.Store.TenantOwners(ctx, f.id.TenantID)
	if err != nil || len(owners) != 1 {
		t.Fatal("late owner escaped inventory", owners, err)
	}
	other := f.id
	other.TenantID = "another-tenant"
	if _, err := f.s.Remember(ctx, other, "allowed", RememberRequest{Target: Personal, Text: "other", SourceRefs: []string{"s"}}); err != nil {
		t.Fatal(err)
	}
}

func TestFencedTenantPreservesKnowledgeUntilExplicitSubjectErasure(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	req := f.turn("alpha", 1, Personal, Project)
	req.Flush = true
	f.ingest(t, req)
	f.step(t, 4)
	if err := f.s.Store.FenceTenant(ctx, f.id.TenantID); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(40 * 24 * time.Hour)
	f.step(t, 5)
	if len(f.e.deleted) != 0 {
		t.Fatal("tenant retention deleted held knowledge", f.e.deleted)
	}
	for _, b := range records[Bank](t, f, "bank") {
		if b.State != "active" {
			t.Fatal(b)
		}
	}
	pending, _, err := f.s.EraseSubject(ctx, f.id.TenantID, f.id.UserID)
	if err != nil || !pending {
		t.Fatal(pending, err)
	}
	f.now = f.now.Add(2 * time.Second)
	f.step(t, 8)
	pending, _, err = f.s.EraseSubject(ctx, f.id.TenantID, f.id.UserID)
	if err != nil || pending {
		t.Fatal(pending, err)
	}
	owners, err := f.s.Store.TenantOwners(ctx, f.id.TenantID)
	if err != nil || len(owners) != 0 {
		t.Fatal("erasure retained raw owner directory", owners, err)
	}
	if len(f.e.facts) != 0 {
		t.Fatal("native banks survived explicit erasure")
	}
}

func TestTenantFenceStopsAlreadyQueuedBankPurges(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	_, err := f.s.Remember(ctx, f.id, "first", RememberRequest{Target: Personal, Text: "preference", SourceRefs: []string{"s"}})
	if err != nil {
		t.Fatal(err)
	}
	f.step(t, 2)
	if _, err := f.s.Clear(ctx, f.id, "clear", ClearRequest{Scope: Personal}); err != nil {
		t.Fatal(err)
	}
	if err := f.s.Store.FenceTenant(ctx, f.id.TenantID); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(time.Hour)
	f.step(t, 4)
	if len(f.e.deleted) != 0 {
		t.Fatal("frozen bank purge was dispatched", f.e.deleted)
	}
}
