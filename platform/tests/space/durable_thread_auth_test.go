package space_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// sharedSpace wires a coordinator+handler over a caller-supplied mock State so a
// test can build TWO independent coordinators (before/after a "restart") on the
// same durable State.
func sharedSpace(t *testing.T, ms *mockState, core space.CoreRunner, auth func(http.Handler) http.Handler) *httptest.Server {
	t.Helper()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: core, WorkerID: "w-restart"})
	h := space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: auth})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)
	return hs
}

// TestDurableThreadAuthSurvivesRestart proves the per-subject thread authorization
// does NOT depend solely on the in-memory turn table: after a restart drops the
// table (fresh Coordinator), the legitimate owner still reads their thread (run
// history / stream) via the scope-isolated durable GetThread fallback, while a
// never-created thread stays 404.
func TestDurableThreadAuthSurvivesRestart(t *testing.T) {
	ms := newMockState()
	// "Before restart": accept + dispatch a turn as alice.
	before := sharedSpace(t, ms, artifactCore{}, authPerSubject)
	accCode, accBody := postSpaceAs(t, before.URL, "AcceptTurn", map[string]any{
		"key": "rs-a1", "context_id": "ctx-restart-alice",
		"input": map[string]any{"role": "user", "content": "hi"}, "max_steps": 2,
	}, "alice")
	if accCode != 200 {
		t.Fatalf("AcceptTurn(before) = %d (body=%s)", accCode, accBody)
	}
	threadID, _ := decodeMap(t, accBody)["thread_id"].(string)
	if c, b := postSpaceAs(t, before.URL, "DispatchTurn", nil, "alice"); c != 200 {
		t.Fatalf("DispatchTurn(before) = %d (body=%s)", c, b)
	}

	// "After restart": a brand-new coordinator over the SAME State has an EMPTY
	// in-memory turn table. authorizeThread must fall back to durable GetThread.
	after := sharedSpace(t, ms, artifactCore{}, authPerSubject)

	code, body := postSpaceAs(t, after.URL, "ListRunHistory", map[string]any{"thread_id": threadID}, "alice")
	if code != 200 {
		t.Fatalf("ListRunHistory(after restart, owner) = %d, want 200 (body=%s)", code, body)
	}
	runs, _ := decodeMap(t, body)["runs"].([]any)
	if len(runs) != 1 {
		t.Fatalf("after-restart run history = %d runs, want 1 (body=%s)", len(runs), body)
	}

	// Stream read also survives the restart for the owner.
	if c, b := postSpaceAs(t, after.URL, "ReadStream", map[string]any{"thread_id": threadID, "after": 0, "limit": 10}, "alice"); c != 200 {
		t.Fatalf("ReadStream(after restart, owner) = %d, want 200 (body=%s)", c, b)
	}

	// A thread that was never created remains 404 even via the durable path.
	if c, b := postSpaceAs(t, after.URL, "ListRunHistory", map[string]any{"thread_id": "ghost-thread"}, "alice"); c != 404 {
		t.Fatalf("ListRunHistory(ghost) = %d, want 404 (body=%s)", c, b)
	}
}

// TestListChatsWireShape proves the Space ListChats facade op returns the accepted
// State ListThreads OBJECT envelope {threads, next_cursor, has_more}, newest-first,
// with keyset pagination. (Subject isolation is enforced by the real State and is
// proven on the real-state suite; the offline double is unscoped.)
func TestListChatsWireShape(t *testing.T) {
	ms := newMockState()
	hs := sharedSpace(t, ms, &stubCore{}, authPerSubject)

	// Two distinct threads (distinct context_id) as alice.
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		code, body := postSpaceAs(t, hs.URL, "AcceptTurn", map[string]any{
			"key": "lc-" + contracts.NewID(), "context_id": "ctx-lc-" + contracts.NewID(),
			"input": map[string]any{"role": "user", "content": "hi"}, "max_steps": 1,
		}, "alice")
		if code != 200 {
			t.Fatalf("AcceptTurn = %d (body=%s)", code, body)
		}
		seen[decodeMap(t, body)["thread_id"].(string)] = true
	}

	code, body := postSpaceAs(t, hs.URL, "ListChats", map[string]any{"limit": 10}, "alice")
	if code != 200 {
		t.Fatalf("ListChats = %d, want 200 (body=%s)", code, body)
	}
	env := decodeMap(t, body)
	threads, ok := env["threads"].([]any)
	if !ok {
		t.Fatalf("ListChats body not object envelope {threads,...}: %s", body)
	}
	if len(threads) != 2 {
		t.Fatalf("ListChats returned %d threads, want 2 (body=%s)", len(threads), body)
	}
	for _, raw := range threads {
		th, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("thread not object: %#v", raw)
		}
		tid, _ := th["thread_id"].(string)
		if !seen[tid] {
			t.Fatalf("ListChats returned unknown thread %q", tid)
		}
		if _, ok := th["context_id"].(string); !ok {
			t.Fatalf("chat summary missing context_id: %#v", th)
		}
	}
	// No scope/subject/tenant must leak into the browser payload.
	for _, key := range []string{"scope", "tenant", "subject", "deployment"} {
		if _, ok := env[key]; ok {
			t.Fatalf("ListChats leaked %q into browser payload: %s", key, body)
		}
	}

	// Pagination: limit=1 -> one thread + next_cursor + has_more=true.
	code, body = postSpaceAs(t, hs.URL, "ListChats", map[string]any{"limit": 1}, "alice")
	if code != 200 {
		t.Fatalf("ListChats(limit=1) = %d (body=%s)", code, body)
	}
	p := decodeMap(t, body)
	if got := len(p["threads"].([]any)); got != 1 {
		t.Fatalf("ListChats(limit=1) returned %d threads, want 1", got)
	}
	if next, _ := p["next_cursor"].(string); next == "" {
		t.Fatalf("ListChats(limit=1) missing next_cursor: %s", body)
	}
	if hm, _ := p["has_more"].(bool); !hm {
		t.Fatalf("ListChats(limit=1) has_more = false, want true: %s", body)
	}
}
