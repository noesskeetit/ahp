package space

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DEF-h26-1.
//
// A context id containing '/' reaches the artifact plane through the run's
// binding, and there it is refused: artifacts bind a context by one bounded
// path-safe identifier (internal/artifacts idPattern), so every save_artifact of
// a run whose context was "h24/api" failed with INVALID_ARGUMENT long after the
// turn was accepted. Space cannot make such a context produce artifacts, so it
// must not pretend to accept one: the refusal belongs at the enqueue, with the
// reason, before anything durable is written.

// stateRecorder is a State stand that answers nothing and remembers being called.
// The point of the test is that a refused enqueue never reaches it.
func stateRecorder(t *testing.T) (*StateClient, func() []string) {
	t.Helper()
	var (
		mu    sync.Mutex
		paths []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": "INTERNAL", "message": "stand: unreachable on purpose"})
	}))
	t.Cleanup(srv.Close)
	return NewStateClient(srv.URL, srv.Client()), func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), paths...)
	}
}

func acceptReq(contextID string) AcceptTurnRequest {
	return AcceptTurnRequest{
		Key:       "h26-" + strings.ReplaceAll(contextID, "/", "_"),
		ContextID: contextID,
		Input:     json.RawMessage(`{"parts":[{"kind":"text","text":"hi"}]}`),
	}
}

func TestAcceptTurnRefusesContextIDThatCannotCarryArtifacts(t *testing.T) {
	client, seen := stateRecorder(t)
	c := NewCoordinator(CoordinatorConfig{StateClient: client, WorkerID: "w1"})
	scope := contracts.Scope{Tenant: "t-h26", Deployment: "space", Subject: "u-h26"}

	_, err := c.AcceptTurn(context.Background(), "tok", scope, acceptReq("h26/ck"))
	if got := contracts.Code(err); got != contracts.ErrInvalid {
		t.Fatalf("a slashed context id must be refused at the enqueue: want %s, got %q (%v)",
			contracts.ErrInvalid, got, err)
	}
	if msg := err.Error(); !strings.Contains(msg, "context") || !strings.Contains(msg, "artifact") {
		t.Fatalf("the refusal must say what is wrong and why, got: %s", msg)
	}
	if calls := seen(); len(calls) != 0 {
		t.Fatalf("refusal must happen before any durable write, State saw: %v", calls)
	}
}

// TestAcceptTurnStillAcceptsCanonicalContextIDs is the regression leg: the
// canonical ids the platform actually uses stay accepted, empty keeps meaning
// "no context binding", and the enqueue proceeds to State exactly as before.
func TestAcceptTurnStillAcceptsCanonicalContextIDs(t *testing.T) {
	for _, id := range []string{"h26ctx", "thread-1", "ctx.2", "a:b", "a-b_c", ""} {
		t.Run("id="+id, func(t *testing.T) {
			client, seen := stateRecorder(t)
			c := NewCoordinator(CoordinatorConfig{StateClient: client, WorkerID: "w1"})
			scope := contracts.Scope{Tenant: "t-h26", Deployment: "space", Subject: "u-h26"}

			_, err := c.AcceptTurn(context.Background(), "tok", scope, acceptReq(id))
			if contracts.Code(err) == contracts.ErrInvalid && strings.Contains(err.Error(), "context id") {
				t.Fatalf("a canonical context id must not be refused: %v", err)
			}
			if calls := seen(); len(calls) == 0 {
				t.Fatal("the turn must still reach State (the refusal is only for ids that cannot carry artifacts)")
			}
		})
	}
}
