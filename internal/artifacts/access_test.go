package artifacts

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func TestOpenRange(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body := "hello ahp artifacts" // 19 bytes
	ref, _ := e.uploadSeal(t, "k-range", body)

	rc, _, err := e.store.OpenRange(ctx, e.scope, ref.ID, 0, 0)
	if err != nil {
		t.Fatalf("OpenRange full: %v", err)
	}
	readAll(t, rc, body)

	rc, _, err = e.store.OpenRange(ctx, e.scope, ref.ID, 6, 3)
	if err != nil {
		t.Fatalf("OpenRange slice: %v", err)
	}
	readAll(t, rc, body[6:9])

	rc, _, err = e.store.OpenRange(ctx, e.scope, ref.ID, 6, 0)
	if err != nil {
		t.Fatalf("OpenRange tail: %v", err)
	}
	readAll(t, rc, body[6:])

	// Past the end of the artifact.
	if _, _, err := e.store.OpenRange(ctx, e.scope, ref.ID, int64(len(body)), 1); err == nil {
		t.Fatal("offset past end must fail")
	} else if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("want invalid for unsatisfiable range, got %v", err)
	}

	// Range reads honour the same admission as full reads.
	e.foreign.Tenant = e.scope.Tenant
	if _, _, err := e.store.OpenRange(ctx, e.foreign, ref.ID, 0, 5); err == nil {
		t.Fatal("foreign subject must not range-read a private artifact")
	}
}

func TestCheckPublication(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref, _ := e.uploadSeal(t, "k-pubcheck", "private-to-owner")

	// A private audience matching the owner is allowed.
	if err := e.store.CheckPublication(ctx, e.scope, []string{ref.ID},
		labels.Audience{Kind: labels.KindPrivate, Subject: "alice"}); err != nil {
		t.Fatalf("owner private publication must pass: %v", err)
	}
	// A workspace sink is refused for a subject-restricted artifact.
	if err := e.store.CheckPublication(ctx, e.scope, []string{ref.ID},
		labels.Audience{Kind: labels.KindWorkspace, Workspace: "w1"}); err == nil {
		t.Fatal("private artifact must not be publishable to a workspace sink")
	} else if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("want forbidden, got %v", err)
	}
	// A tenant sink is refused too.
	if err := e.store.CheckPublication(ctx, e.scope, []string{ref.ID},
		labels.Audience{Kind: labels.KindTenant}); err == nil {
		t.Fatal("private artifact must not be publishable to a tenant sink")
	}
	// A foreign caller cannot publish what it cannot read.
	e.foreign.Tenant = e.scope.Tenant
	if err := e.store.CheckPublication(ctx, e.foreign, []string{ref.ID},
		labels.Audience{Kind: labels.KindPrivate, Subject: "mallory"}); err == nil {
		t.Fatal("foreign caller must not publish another subject's artifact")
	}
}

func TestPublishToWorkspace(t *testing.T) {
	member := func(_ context.Context, s contracts.Scope, ws string) error {
		if ws == "w1" && s.Subject == "alice" {
			return nil
		}
		return contracts.NewError(contracts.ErrForbidden, "not a member")
	}
	e := setup(t, WithWorkspaceAccess(member))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref, _ := e.uploadSeal(t, "k-wspub", "publish-me")

	// Only the owner may publish.
	e.foreign.Tenant = e.scope.Tenant
	if err := e.store.PublishToWorkspace(ctx, e.foreign, ref.ID, "w1"); err == nil {
		t.Fatal("foreign subject must not publish")
	}
	// Owner publishes to w1.
	if err := e.store.PublishToWorkspace(ctx, e.scope, ref.ID, "w1"); err != nil {
		t.Fatalf("PublishToWorkspace: %v", err)
	}
	// Owner (a member) can now read it; effective labels carry w1.
	got, err := e.store.Get(ctx, e.scope, ref.ID)
	if err != nil {
		t.Fatalf("Get after publish: %v", err)
	}
	if len(got.EffectiveLabels.Workspaces) != 1 || got.EffectiveLabels.Workspaces[0] != "w1" {
		t.Fatalf("expected workspace w1 in effective labels: %+v", got.EffectiveLabels)
	}
	// A non-member of the same tenant still cannot read it.
	if _, err := e.store.Get(ctx, e.foreign, ref.ID); err == nil {
		t.Fatal("non-member must not read a workspace-published artifact")
	}
	// Republishing to a different workspace conflicts.
	if err := e.store.PublishToWorkspace(ctx, e.scope, ref.ID, "w2"); err == nil {
		t.Fatal("republish to a different workspace must conflict")
	}
}
