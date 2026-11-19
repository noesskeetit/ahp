package registry_test

import (
	"context"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
)

func TestTenantFenceFreezesInstallationsAndPins(t *testing.T) {
	s, tenant := newStore(t)
	ctx := context.Background()
	agent := "fence-" + randSuffix(t)
	m := manifestFor(t, "system.fence", nil, nil)
	v := publishAs(t, s, "publisher-"+randSuffix(t), agent, m)
	in, err := s.Install(ctx, tenant, agent, v.ID, "dev", "install", registry.Capabilities{})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.FenceTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func() error{
		"install": func() error {
			_, e := s.Install(ctx, tenant, agent, v.ID, "stage", "late", registry.Capabilities{})
			return e
		},
		"pin":      func() error { _, e := s.Pin(ctx, tenant, in.ID, "late-task"); return e },
		"promote":  func() error { _, e := s.Promote(ctx, tenant, in.ID, v.ID); return e },
		"rollback": func() error { _, e := s.Rollback(ctx, tenant, in.ID); return e },
		"publish":  func() error { return s.Publish(ctx, tenant, in.ID, true) },
	} {
		if err = call(); contracts.Code(err) != contracts.ErrForbidden {
			t.Errorf("%s admitted after erasure: %v", name, err)
		}
	}
	if _, err = s.Install(ctx, tenant+"-other", agent, v.ID, "dev", "foreign", registry.Capabilities{}); err != nil {
		t.Fatal(err)
	}
}
