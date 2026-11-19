package memory

import (
	"context"
	"testing"
	"time"
)

func TestIngressProjectBindingIsImmutableAndOwnerScoped(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	id := f.id
	id.ContextID = "conversation"
	req := BindRequest{ContextID: id.ContextID, ProjectID: "alpha"}
	_, err := f.s.BindProject(ctx, id, "runtime-attempt", req)
	assertCode(t, err, "console_required")
	id.ActorKind = "console"
	if _, err := f.s.BindProject(ctx, id, "first-turn", req); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(time.Hour)
	if _, err := f.s.BindProject(ctx, id, "second-turn", req); err != nil {
		t.Fatal(err)
	}
	if !records[ProjectState](t, f, "project")[0].LastActivity.Equal(f.now) {
		t.Fatal("new turn did not renew activity")
	}
	p, err := ProjectForContext(ctx, f.s.Store, id.TenantID, id.UserID, id.ContextID)
	if err != nil || p != "alpha" {
		t.Fatal(p, err)
	}
	if _, err := ProjectForContext(ctx, f.s.Store, id.TenantID, "another-owner", id.ContextID); err != ErrMissing {
		t.Fatal("binding crossed owner", err)
	}
	req.ProjectID = "beta"
	_, err = f.s.BindProject(ctx, id, "change", req)
	assertCode(t, err, "project_binding_conflict")
	req.ContextID = "another-context"
	_, err = f.s.BindProject(ctx, id, "context", req)
	assertCode(t, err, "wrong_context")
}

func TestPersonalOnlyChatCreatesNoProjectBank(t *testing.T) {
	f := setup(t)
	req := f.turn("", 1, Personal)
	req.Flush = true
	f.ingest(t, req)
	f.step(t, 3)
	if len(records[ProjectState](t, f, "project")) != 0 {
		t.Fatal("personal chat created a project")
	}
	banks := records[Bank](t, f, "bank")
	if len(banks) != 1 || banks[0].Scope != Personal {
		t.Fatal(banks)
	}
}

func TestSubjectErasureBlocksLateTurnsAndReenable(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	req := f.turn("alpha", 1, Personal, Project)
	req.Flush = true
	f.ingest(t, req)
	f.step(t, 4)
	pending, _, err := f.s.EraseSubject(ctx, f.id.TenantID, f.id.UserID)
	if err != nil || !pending {
		t.Fatal(pending, err)
	}
	_, err = f.s.Ingest(ctx, f.id, "late", f.turn("alpha", 2, Personal, Project))
	assertCode(t, err, "subject_deleted")
	on := true
	_, err = f.s.UpdateSettings(ctx, f.id, "reenable", SettingsRequest{PersonalEnabled: &on})
	assertCode(t, err, "subject_deleted")
	f.now = f.now.Add(2 * time.Second)
	f.step(t, 8)
	pending, _, err = f.s.EraseSubject(ctx, f.id.TenantID, f.id.UserID)
	if err != nil || pending {
		t.Fatal(pending, err)
	}
	if len(records[Turn](t, f, "turn")) != 0 || len(records[ProjectBinding](t, f, "binding")) != 0 {
		t.Fatal("erasure retained source records")
	}
	for _, b := range records[Bank](t, f, "bank") {
		if b.ProjectID != "" || len(b.Labels) > 0 || b.State != "deleted" {
			t.Fatal(b)
		}
	}
	_, err = f.s.Remember(ctx, f.id, "resurrect", RememberRequest{Target: Personal, Text: "old knowledge", SourceRefs: []string{"old"}})
	assertCode(t, err, "subject_deleted")
}

func (f *fakeEngine) List(ctx context.Context, b Bank, limit, offset int) ([]NativeFact, int, error) {
	facts, err := f.Recall(ctx, b, "", 0)
	if offset >= len(facts) {
		return []NativeFact{}, len(facts), err
	}
	return facts[offset:min(offset+limit, len(facts))], len(facts), err
}
func TestListingBindsPrivateLabelsBeforeReturningFacts(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	_, err := f.s.Remember(ctx, f.id, "r", RememberRequest{Target: Personal, Text: "preference", SourceRefs: []string{"s"}})
	if err != nil {
		t.Fatal(err)
	}
	f.step(t, 3)
	page, err := f.s.List(ctx, f.id, Personal, "", 10, 0)
	if err != nil || page.Total != 1 || len(page.Facts) != 1 {
		t.Fatal(page, err)
	}
	f.a.bindError = Fail(403, "bind_denied", "denied")
	page, err = f.s.List(ctx, f.id, Personal, "", 10, 0)
	assertCode(t, err, "bind_denied")
	if len(page.Facts) > 0 {
		t.Fatal("text escaped failed label binding")
	}
}
