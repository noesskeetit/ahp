package space_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/ahp-platform/ahp/sdk/go/agent"
)

// deployment is a standalone Space process for one deployment scope: its own
// coordinator + HTTP handler backed by the shared State service, addressed
// through a dedicated SDK client.
type deployment struct {
	name   string
	client *agent.Client
	coord  *space.Coordinator
	srv    *httptest.Server
}

// newDeployment stands up one Agents Space deployment behind its own HTTP
// server. The auth middleware injects the deployment's verified principal so
// the handler derives scope from the request context, never the body.
func newDeployment(t *testing.T, stateURL string, scope contracts.Scope, core space.CoreRunner) *deployment {
	t.Helper()
	sc := space.NewStateClient(stateURL, nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        core,
		WorkerID:    scope.Deployment + "-worker",
	})
	auth := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p := transport.Principal{Scope: scope}
			next.ServeHTTP(w, r.WithContext(transport.WithPrincipal(r.Context(), p)))
		})
	}
	h := space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: auth})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	client := agent.NewClient(agent.Config{BaseURL: srv.URL, Token: "deployment-token"})
	return &deployment{name: scope.Deployment, client: client, coord: coord, srv: srv}
}

// TestTwoDeploymentsDelegateViaSDK drives two custom Agents Space deployments
// end-to-end through the SDK: deployment "acme" accepts a user turn, delegates
// a subtask to a third agent deployment "tools" via Client.Delegate, and both
// complete their Core runs with distinct threads, scopes and stream history.
//
// It validates the SDK + handler + coordinator + delegation contract without a
// live LLM; the real LLM/tool/artifact/download leg is exercised once the
// execution/identity/data neighbours expose their APIs.
func TestTwoDeploymentsDelegateViaSDK(t *testing.T) {
	ms := newMockState()
	stateSrv := ms.Server()
	defer stateSrv.Close()

	ctx := context.Background()

	acmeCore := &stubCore{done: make(chan struct{})}
	toolsCore := &stubCore{done: make(chan struct{})}

	acme := newDeployment(t, stateSrv.URL, contracts.Scope{Tenant: "t1", Deployment: "acme", Subject: "user-1"}, acmeCore)
	tools := newDeployment(t, stateSrv.URL, contracts.Scope{Tenant: "t1", Deployment: "tools", Subject: "agent-2"}, toolsCore)

	// 1. acme accepts the human turn.
	acmeOut, err := acme.client.AcceptTurn(ctx, agent.TurnInput{
		Key:       "acme-" + contracts.NewID(),
		ContextID: "chat-1",
		Input:     json.RawMessage(`"summarise this quarter"`),
		MaxSteps:  4,
	})
	if err != nil {
		t.Fatalf("acme AcceptTurn: %v", err)
	}
	if err := acme.client.Dispatch(ctx); err != nil {
		t.Fatalf("acme Dispatch: %v", err)
	}
	select {
	case <-acmeCore.done:
	case <-time.After(5 * time.Second):
		t.Fatal("acme core did not complete")
	}
	time.Sleep(150 * time.Millisecond) // let acme release its lease

	// 2. acme delegates a subtask to the third-agent deployment.
	toolsOut, err := acme.client.Delegate(ctx, tools.client, agent.TurnInput{
		Key:       "tools-" + contracts.NewID(),
		ContextID: "acme-subtask",
		Input:     json.RawMessage(`"fetch revenue table"`),
		MaxSteps:  4,
	})
	if err != nil {
		t.Fatalf("Delegate acme->tools: %v", err)
	}
	if toolsOut.TurnID == "" || toolsOut.TurnID == acmeOut.TurnID {
		t.Fatalf("expected a distinct delegated turn id, got %q", toolsOut.TurnID)
	}

	if err := tools.client.Dispatch(ctx); err != nil {
		t.Fatalf("tools Dispatch: %v", err)
	}
	select {
	case <-toolsCore.done:
	case <-time.After(5 * time.Second):
		t.Fatal("tools core did not complete")
	}
	time.Sleep(150 * time.Millisecond)

	// 3. Both deployments recorded independent, correctly-scoped turns.
	acmeTurn, ok := acme.coord.TurnSnapshot(acmeOut.TurnID)
	if !ok {
		t.Fatal("acme turn missing from its coordinator")
	}
	if acmeTurn.Scope.Deployment != "acme" {
		t.Fatalf("acme turn scope deployment = %q", acmeTurn.Scope.Deployment)
	}
	if acmeTurn.Status != space.TurnDone {
		t.Fatalf("acme turn status = %s, want done", acmeTurn.Status)
	}

	toolsTurn, ok := tools.coord.TurnSnapshot(toolsOut.TurnID)
	if !ok {
		t.Fatal("tools turn missing from its coordinator")
	}
	if toolsTurn.Scope.Deployment != "tools" || toolsTurn.Scope.Subject != "agent-2" {
		t.Fatalf("tools turn scope = %+v", toolsTurn.Scope)
	}
	if toolsTurn.Status != space.TurnDone {
		t.Fatalf("tools turn status = %s, want done", toolsTurn.Status)
	}
	if toolsTurn.ThreadID == acmeTurn.ThreadID {
		t.Fatal("delegated turn must live in its own thread")
	}

	// 4. The delegated deployment's execution produced stream events.
	stream, err := tools.client.ReadStream(ctx, toolsOut.ThreadID, 0, 100)
	if err != nil {
		t.Fatalf("tools ReadStream: %v", err)
	}
	var got struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(stream, &got); err != nil {
		t.Fatalf("decode tools stream: %v", err)
	}
	if len(got.Events) == 0 {
		t.Fatal("expected stream events on the delegated thread")
	}
}
