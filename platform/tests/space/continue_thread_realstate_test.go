//go:build state_integration

package space_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

func TestAcceptTurnContinuesOwnedThreadRealState(t *testing.T) {
	e := cNewEnv(t)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}})
	srv := httptest.NewServer(space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment))}))
	defer srv.Close()
	code, body := postSpaceAs(t, srv.URL, "AcceptTurn", map[string]any{"key": "first", "context_id": "conversation", "input": map[string]any{"content": "first"}}, "ada")
	if code != http.StatusOK {
		t.Fatalf("first turn: %d %s", code, body)
	}
	thread := decodeMap(t, body)["thread_id"].(string)
	second := map[string]any{"key": "second", "thread_id": thread, "input": map[string]any{"content": "second"}}
	code, body = postSpaceAs(t, srv.URL, "AcceptTurn", second, "ada")
	if code != http.StatusOK {
		t.Fatalf("continued turn: %d %s", code, body)
	}
	accepted := decodeMap(t, body)
	if accepted["thread_id"] != thread {
		t.Fatalf("new chat instead of continuation: %s", body)
	}
	code, replay := postSpaceAs(t, srv.URL, "AcceptTurn", second, "ada")
	if code != 200 || decodeMap(t, replay)["turn_id"] != accepted["turn_id"] {
		t.Fatalf("continuation retry duplicated run: %d %s", code, replay)
	}
	code, body = postSpaceAs(t, srv.URL, "AcceptTurn", map[string]any{"key": "foreign", "thread_id": thread, "input": map[string]any{"content": "foreign"}}, "grace")
	if code != http.StatusNotFound {
		t.Fatalf("foreign thread accepted: %d %s", code, body)
	}
	code, body = postSpaceAs(t, srv.URL, "AcceptTurn", map[string]any{"key": "mismatch", "thread_id": thread, "context_id": "other", "input": map[string]any{"content": "wrong context"}}, "ada")
	if code != http.StatusConflict {
		t.Fatalf("context mismatch accepted: %d %s", code, body)
	}
	tok, _, err := realTaskExchanger{e}.Exchange(e.ctx, space.HumanPrincipal{Subject: "ada"}, "")
	if err != nil {
		t.Fatal(err)
	}
	runs, err := e.client.ListRuns(e.ctx, tok, thread)
	if err != nil || len(runs) != 2 {
		t.Fatalf("conversation durable runs=%v err=%v", runs, err)
	}
}
