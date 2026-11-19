package registry_test

import (
	"context"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
)

func TestSubjectDeletionRegistryPinsPreserveSharedCatalog(t *testing.T) {
	s, tenant := newStore(t)
	ctx := context.Background()
	agent := "subject-" + randSuffix(t)
	v := publishAs(t, s, "publisher-"+randSuffix(t), agent, manifestFor(t, "system.subject", nil, nil))
	in, err := s.Install(ctx, tenant, agent, v.ID, "dev", "install", registry.Capabilities{})
	if err != nil {
		t.Fatal(err)
	}
	own := contracts.Scope{Tenant: tenant, Deployment: "dep", Subject: "alice"}
	other := own
	other.Subject = "bob"
	if _, err = s.PinForSubject(ctx, own, in.ID, "own-pin"+tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PinForSubject(ctx, other, in.ID, "foreign-pin"+tenant); err != nil {
		t.Fatal(err)
	}
	if err = s.FenceSubject(ctx, tenant, own.Subject); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PinForSubject(ctx, own, in.ID, "late"+tenant); err == nil {
		t.Fatal("late pin admitted")
	}
	if err = s.AliasTaskPin(ctx, tenant, in.ID, "own-pin"+tenant, "late-alias"+tenant); err == nil {
		t.Fatal("late alias admitted")
	}
	if err = s.DeleteSubject(ctx, tenant, own.Subject); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PinnedVersion(ctx, tenant, "own-pin"+tenant); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("own pin survives: %v", err)
	}
	if _, err = s.PinnedVersion(ctx, tenant, "foreign-pin"+tenant); err != nil {
		t.Fatalf("foreign pin lost: %v", err)
	}
	if _, err = s.GetInstallation(ctx, tenant, in.ID); err != nil {
		t.Fatal("shared installation lost", err)
	}
}

func TestSubjectDeletionRegistryLegacyOwnerRequiresEvidence(t *testing.T) {
	s, tenant := newStore(t)
	ctx := context.Background()
	agent := "legacy-" + randSuffix(t)
	v := publishAs(t, s, "publisher-"+randSuffix(t), agent, manifestFor(t, "system.legacy", nil, nil))
	in, err := s.Install(ctx, tenant, agent, v.ID, "dev", "install", registry.Capabilities{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pin(ctx, tenant, in.ID, "legacy-own"+tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pin(ctx, tenant, in.ID, "legacy-foreign"+tenant); err != nil {
		t.Fatal(err)
	}
	if err = s.FenceSubject(ctx, tenant, "alice"); err == nil {
		t.Fatal("owner guessed")
	}
	if err = s.DeleteSubject(ctx, tenant, "alice"); err == nil {
		t.Fatal("unknown pins erased")
	}
	resolved := registry.New(s.Pool(), registry.WithTaskSubjectResolver(func(_ context.Context, gotTenant, id string) (string, error) {
		if gotTenant != tenant {
			t.Fatal("tenant lost")
		}
		if id == "legacy-own"+tenant {
			return "alice", nil
		}
		return "bob", nil
	}))
	if err = resolved.FenceSubject(ctx, tenant, "alice"); err != nil {
		t.Fatal(err)
	}
	if err = resolved.DeleteSubject(ctx, tenant, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PinnedVersion(ctx, tenant, "legacy-foreign"+tenant); err != nil {
		t.Fatalf("foreign legacy pin lost: %v", err)
	}
}
