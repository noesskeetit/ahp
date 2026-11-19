//go:build state_integration

package space_test

// sdk_agent_realstate_test.go — the REAL-API upgrade of TestSDKTwoDeploymentsDelegate.
// It drives the SHIPPED Go SDK (sdk/go/agent) against the PRODUCTION Space HTTP
// handler (space.Handler + Coordinator) whose StateClient talks to the REAL State
// service over PostgreSQL (mTLS workload certs, Ed25519 task tokens) -- NOT the
// newMockState test double used by the contract-level sdk_agent_test.go.
//
// It proves acceptance #4 ("two custom deployments via SDK + third-agent
// delegation") at the real-API level: two independent Space deployments (distinct
// tenant/deployment, each on its own State client over the same PG) each accept +
// run a turn through agent.NewClient, and a follow-up turn is DELEGATED from A to B
// via the SDK. The authoritative evidence is that the produced events are read back
// from REAL State (e.client.ReadStream), i.e. they were committed to PostgreSQL, not
// held in an in-process double. Nothing here is presented as a real-LLM proof -- the
// Core loop is the deterministic stubCore (the real LLM+tool+artifact journey is the
// live_real suite). Missing real PG => cNewEnv t.Skip (state_integration harness).
//
// Author-side candidate evidence; independent live verification is iv's.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/ahp-platform/ahp/sdk/go/agent"
)

// newRealStateSpace builds a PRODUCTION Space HTTP handler whose Coordinator uses a
// REAL StateClient (stateapi over PG from cNewEnv). It returns the SDK-facing server,
// a valid Ed25519 human task token (the SDK bearer) and the underlying cEnv so the
// test can assert against real State directly.
func newRealStateSpace(t *testing.T) (*httptest.Server, string, *cEnv) {
	t.Helper()
	e := cNewEnv(t)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client,
		Core:        &stubCore{},
		WorkerID:    "w-sdk-" + contracts.NewID(),
	})
	// Auth middleware injecting the caller's verified scope, exactly matching the
	// subject/tenant/deployment the SDK bearer token was signed with (mimics the
	// real upstream auth middleware the Space handler is mounted behind).
	auth := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := transport.WithPrincipal(r.Context(), transport.Principal{
				Scope: contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject},
			})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	h := space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: auth})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	bearer := e.signTask(t, "task-"+contracts.NewID())
	return srv, bearer, e
}

// sdkRealStateStreamCount reads the OBJECT {events:[...]} envelope back from REAL
// State (not the SDK, not a double) for a thread, proving the events are in PG.
func sdkRealStateStreamCount(t *testing.T, e *cEnv, token, threadID string) int {
	t.Helper()
	evs, err := e.client.ReadStream(e.ctx, token, threadID, 0, 200)
	if err != nil {
		t.Fatalf("ReadStream over REAL State (tenant=%s thread=%s): %v", e.tenant, threadID, err)
	}
	return len(evs)
}

func TestSDKTwoDeploymentsDelegate_RealState(t *testing.T) {
	srvA, tokA, eA := newRealStateSpace(t)
	srvB, tokB, eB := newRealStateSpace(t)
	ctx := context.Background()

	a := agent.NewClient(agent.Config{BaseURL: srvA.URL, Token: tokA})
	b := agent.NewClient(agent.Config{BaseURL: srvB.URL, Token: tokB})

	// 1. Deployment A: accept + dispatch + run to Done through the SDK.
	outA, err := a.AcceptTurn(ctx, agent.TurnInput{
		Key: "rsdk-a-" + contracts.NewID(), ContextID: "rsdk-test",
		Input: json.RawMessage(`"hello real a"`), MaxSteps: 2,
	})
	if err != nil {
		t.Fatalf("SDK AcceptTurn(A) over real State: %v", err)
	}
	if outA.TurnID == "" || outA.ThreadID == "" {
		t.Fatalf("SDK AcceptTurn(A) missing ids: %+v", outA)
	}
	if err := a.Dispatch(ctx); err != nil {
		t.Fatalf("SDK Dispatch(A) over real State: %v", err)
	}
	if st := sdkWaitDone(t, a, outA.TurnID); st != string(space.TurnDone) {
		t.Fatalf("deployment A turn not done via SDK over real State: status=%s", st)
	}
	// Authoritative: A's stream events are committed to REAL State (PG).
	if n := sdkRealStateStreamCount(t, eA, tokA, outA.ThreadID); n == 0 {
		t.Fatalf("REAL State shows no events for deployment A thread %s (turn not durably run)", outA.ThreadID)
	}

	// 2. Third-agent delegation: A hands a follow-up turn to deployment B via the SDK.
	outB, err := a.Delegate(ctx, b, agent.TurnInput{
		Key: "rsdk-b-" + contracts.NewID(), ContextID: "rsdk-test.delegated",
		Input: json.RawMessage(`"delegated by A over real State"`), MaxSteps: 2,
	})
	if err != nil {
		t.Fatalf("SDK Delegate(A->B) over real State: %v", err)
	}
	if outB.TurnID == "" || outB.ThreadID == "" {
		t.Fatalf("SDK delegation returned no turn on B: %+v", outB)
	}

	// 3. Deployment B runs the delegated turn to Done through the SDK.
	if err := b.Dispatch(ctx); err != nil {
		t.Fatalf("SDK Dispatch(B) over real State: %v", err)
	}
	if st := sdkWaitDone(t, b, outB.TurnID); st != string(space.TurnDone) {
		t.Fatalf("delegated B turn not done via SDK over real State: status=%s", st)
	}
	if n := sdkRealStateStreamCount(t, eB, tokB, outB.ThreadID); n == 0 {
		t.Fatalf("REAL State shows no events for delegated deployment B thread %s", outB.ThreadID)
	}

	// 4. Scope isolation: the two deployments are distinct State scopes (independent
	//    custom deployments), so B's delegated thread must NOT appear in A's State.
	if _, err := eA.client.ReadStream(eA.ctx, tokA, outB.ThreadID, 0, 10); err == nil {
		t.Fatalf("scope leak: deployment A read deployment B's thread %s across tenants", outB.ThreadID)
	}

	// 5. SDK ListTurns on B surfaces the delegated turn (developer surface over real State).
	listRaw, err := b.ListTurns(ctx)
	if err != nil {
		t.Fatalf("SDK ListTurns(B) over real State: %v", err)
	}
	if !containsTurnID(listRaw, outB.TurnID) {
		t.Fatalf("SDK ListTurns(B) missing delegated turn %s: %s", outB.TurnID, listRaw)
	}
	t.Logf("SDK two deployments + delegation over REAL State: A events=%d (tenant=%s), B delegated events=%d (tenant=%s), scope isolation held",
		sdkRealStateStreamCount(t, eA, tokA, outA.ThreadID), eA.tenant,
		sdkRealStateStreamCount(t, eB, tokB, outB.ThreadID), eB.tenant)
}
