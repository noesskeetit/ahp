package identity

import (
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestTenantRevocationRejectsExistingAndUnknownSubjects(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	if _, err := s.CreateRoot(ctx, scope, rootRequest("root", "context", scope.Deployment, time.Hour, "read")); err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Evaluate(ctx, scope, "root", "read"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("old task authorized: %v", err)
	}
	scope.Subject = "new-subject"
	if _, err := s.CreateRoot(ctx, scope, rootRequest("late", "late-context", scope.Deployment, time.Hour, "read")); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("new subject admitted: %v", err)
	}
	scope.Tenant += "-other"
	if _, err := s.CreateRoot(ctx, scope, rootRequest("foreign", "foreign-context", scope.Deployment, time.Hour, "read")); err != nil {
		t.Fatal(err)
	}
}

func TestTenantDeletionRevokesBrowserReadAndPurgesCredentials(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	u := BrowserUser{UserID: contracts.NewID(), Tenant: scope.Tenant, Subject: scope.Subject, DisplayName: "tenant member"}
	login := "tenant-" + contracts.NewID()
	if err := s.SeedBrowserUser(ctx, u, login, "synthetic-test-password", []string{"admin"}, []string{"tenant:delete"}); err != nil {
		t.Fatal(err)
	}
	session, err := s.StartBrowserSession(ctx, u.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RevokeTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = s.BrowserSession(ctx, session.ID); err == nil {
		t.Fatal("revoked tenant browser can still read")
	}
	if err = s.DeleteTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.AuthenticateBrowser(ctx, login, "synthetic-test-password"); err != nil || ok {
		t.Fatalf("deleted credentials authenticate: %v %v", ok, err)
	}
	if _, err = s.BrowserUserBySubject(ctx, scope.Tenant, scope.Subject); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("browser user survived: %v", err)
	}
}
