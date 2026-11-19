package identity

import (
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
	"time"
)

func TestSubjectDeletionCredentialsGrantsAndPermanentBarrier(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	user := BrowserUser{UserID: contracts.NewID(), Tenant: scope.Tenant, Subject: scope.Subject, DisplayName: "test"}
	login := contracts.NewID()
	if err := s.SeedBrowserUser(ctx, user, login, "synthetic-password", []string{"user"}, []string{"read"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateRoot(ctx, scope, rootRequest("erase-root", "erase-context", scope.Deployment, time.Hour, "read")); err != nil {
		t.Fatal(err)
	}
	other := scope
	other.Subject += "-other"
	if _, err := s.CreateRoot(ctx, other, rootRequest("foreign-root", "foreign-context", other.Deployment, time.Hour, "read")); err != nil {
		t.Fatal(err)
	}
	if err := s.FenceSubject(ctx, scope.Tenant, scope.Subject); err != nil {
		t.Fatal(err)
	}
	if owner, err := s.TaskSubject(ctx, scope.Tenant, "erase-root"); err != nil || owner != scope.Subject {
		t.Fatalf("binding lost before cleanup %q %v", owner, err)
	}
	if err := s.DeleteSubject(ctx, scope.Tenant, scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.AuthenticateBrowser(ctx, login, "synthetic-password"); err != nil || ok {
		t.Fatalf("credentials survived %v %v", ok, err)
	}
	if err := s.SeedBrowserUser(ctx, user, login, "synthetic-password", nil, nil); err == nil {
		t.Fatal("erased user resurrected")
	}
	if _, err := s.CreateRoot(ctx, scope, rootRequest("late", "late", scope.Deployment, time.Hour, "read")); err == nil {
		t.Fatal("late grant admitted")
	}
	if _, err := s.Evaluate(ctx, other, "foreign-root", "read"); err != nil {
		t.Fatalf("foreign affected: %v", err)
	}
	if err := s.DeleteSubject(ctx, scope.Tenant, scope.Subject); err != nil {
		t.Fatal(err)
	}
}

func TestSubjectDeletionLegacyTaskOwnerAmbiguityFailsClosed(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	for _, subject := range []string{scope.Subject, scope.Subject + "-other"} {
		owner := scope
		owner.Subject = subject
		if _, err := s.CreateRoot(ctx, owner, rootRequest("same-task", contracts.NewID(), owner.Deployment, time.Hour, "read")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.TaskSubject(ctx, scope.Tenant, "same-task"); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("ambiguous owner must not fall back to another authority: %v", err)
	}
	if _, err := s.TaskSubject(ctx, scope.Tenant, "absent"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("missing owner must permit an authoritative fallback: %v", err)
	}
}
