//go:build state_integration

package space_test

import (
	"testing"

	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestDurableThreadOwnershipRealState proves, on the REAL State over PostgreSQL,
// the security property the durable authorizeThread fallback relies on: a thread is
// owned iff a scope-isolated GetThread resolves it for the PRESENTING token. The
// owner's task token resolves it (nil); a foreign-scope token does not (error).
// This is what keeps restart-safe replay fail-closed: after the in-memory turn
// table is dropped, ownership is re-checked against durable State, never widened.
func TestDurableThreadOwnershipRealState(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	owner := e.signTask(t, "task-"+contracts.NewID())

	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
	th, err := e.client.CreateThread(ctx, owner, scope, "ctx-own-"+contracts.NewID(), "own-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}

	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, WorkerID: "w-own"})

	// Owner resolves it.
	if err := coord.VerifyThreadOwnership(ctx, owner, th.ID); err != nil {
		t.Fatalf("owner VerifyThreadOwnership = %v, want nil", err)
	}
	// A never-created thread does not resolve.
	if err := coord.VerifyThreadOwnership(ctx, owner, "ghost-"+contracts.NewID()); err == nil {
		t.Fatalf("ghost VerifyThreadOwnership = nil, want error")
	}
	// A foreign-scope token must NOT resolve the owner's thread.
	foreign, err := e.signer.Sign(auth.Claims{
		Audience: cAudience, Kind: auth.KindTask,
		Tenant: e.tenant, Deployment: e.deployment,
		Subject: "intruder-" + contracts.NewID(), Task: "task-f", Root: "run-f",
		Context: "ctx-f", Grant: "grant-f", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatalf("sign foreign: %v", err)
	}
	if err := coord.VerifyThreadOwnership(ctx, foreign, th.ID); err == nil {
		t.Fatalf("foreign VerifyThreadOwnership = nil, want error (no cross-subject ownership)")
	}
}

// TestListChatsRealState exercises the durable per-scope chat list against the real
// State ListThreads op. ListThreads lands in State at 7df96b34 (integrated successor
// of base 012a849); this base's State (461aa2ce) predates it, so the test capability-
// gates with an explicit reason and runs for real once State exposes the op.
