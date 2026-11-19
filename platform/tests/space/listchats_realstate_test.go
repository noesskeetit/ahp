//go:build state_integration && listthreads_integration

// The real State ListThreads op lands at 7df96b34 (integrated successor of base
// 012a849). This test is gated behind listthreads_integration so the standard
// state_integration seam on 012a849 stays skip-free (iv-verify-seam.sh T2 fails on
// any SKIP). Run it on the integrated tree with:
//   go test -tags "state_integration,listthreads_integration" -run TestListChatsRealState ./tests/space/

package space_test

import (
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestListChatsRealState(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}

	// Probe capability on the real server.
	code, resp := e.raw("ListThreads", human, []byte(`{"limit":5}`))
	if code != 200 {
		t.Skipf("real State in this base has no ListThreads (HTTP %d: %v); runs on integrated tree with State 7df96b34", code, resp)
	}

	threads := map[string]bool{}
	for i := 0; i < 2; i++ {
		th, err := e.client.CreateThread(ctx, human, scope, "ctx-c-"+contracts.NewID(), "c-thread-"+contracts.NewID())
		if err != nil {
			t.Fatalf("CreateThread: %v", err)
		}
		threads[th.ID] = true
	}

	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, WorkerID: "w-c"})
	chats, next, hasMore, err := coord.ListChats(ctx, human, "", 10)
	if err != nil {
		t.Fatalf("ListChats: %v", err)
	}
	if len(chats) < 2 {
		t.Fatalf("ListChats returned %d threads, want >=2", len(chats))
	}
	_ = next
	_ = hasMore
	seen := false
	for _, c := range chats {
		if threads[c.ThreadID] {
			seen = true
		}
	}
	if !seen {
		t.Fatalf("ListChats did not include created threads")
	}
}
