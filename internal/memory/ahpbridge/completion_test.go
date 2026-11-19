package ahpbridge

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
)

func TestStateConsumerDoesNotGrantPublicRuntimeAuthority(t *testing.T) {
	f := fixture(t)
	f.access.d.RuntimeDeployments = nil
	request := memory.AccessRequest{Action: "runtime:turn", ProjectID: "alpha", Scopes: []memory.Scope{memory.Project}}
	if _, err := f.access.Authorize(f.ctx, "", request); err == nil {
		t.Fatal("ordinary workload published a runtime event")
	}
	service := memory.New(memory.NewMemoryStore(), nil, f.access)
	event := Completion{ID: "run", ThreadID: "thread", ProjectID: "alpha", Sequence: 1, CreatedAt: time.Now(), Claims: f.claims, Sources: []memory.Source{{Ref: "state://runs/run/input", Role: "user", Text: "Remember this useful decision", At: time.Now().Add(-time.Second), AllowedScopes: []memory.Scope{memory.Project, memory.Personal}}}}
	if err := ConsumeCompletion(context.Background(), service, f.access, event); err != nil {
		t.Fatal(err)
	}
	// Newly accumulated context labels may change between a lost ACK and retry.
	// They must not alter the stable event identity or create a second turn.
	f.set.Workspaces = []string{"alpha"}
	if err := ConsumeCompletion(context.Background(), service, f.access, event); err != nil {
		t.Fatal("policy augmentation broke redelivery", err)
	}
	if _, err := f.access.Authorize(f.ctx, "", request); err == nil {
		t.Fatal("internal consumer marker leaked to HTTP principal")
	}
	f.revoked = true
	if err := ConsumeCompletion(context.Background(), service, f.access, event); err == nil {
		t.Fatal("State consumer bypassed live revocation")
	}
}

func TestPrivateReadAlwaysBindsOwnerEvenWithoutSourceLabels(t *testing.T) {
	f := fixture(t)
	id, err := f.access.Authorize(f.ctx, "", memory.AccessRequest{Action: "memory:read"})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.access.Bind(f.ctx, id, nil); err != nil {
		t.Fatal(err)
	}
	if len(f.bound.Subjects) != 1 || f.bound.Subjects[0] != "owner" {
		t.Fatal("missing intrinsic private restriction", f.bound)
	}
}
