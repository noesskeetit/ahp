package space_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestAcceptDispatchCarriesPinnedAgentConfig (acceptance #2/#5; PDF rev3.2
// L373/948/2399) proves Space is the CARRIER of the AgentConfig+deployment pin:
// the revision pinned at AcceptTurn must reach Core on the normal dispatch path,
// so a mid-flight rollout cannot silently change an in-flight task's behavior.
func TestAcceptDispatchCarriesPinnedAgentConfig(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}
	pin := &space.AgentConfigRef{Revision: 42, DeploymentVersion: "v1.7.3"}

	captured := make(chan *space.CoreRequest, 1)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{capture: captured}, WorkerID: "w1"})
	_, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:         "pin-hot-" + contracts.NewID(),
		ContextID:   "ctx-pin-hot",
		Input:       json.RawMessage(`{"role":"user","content":"hi"}`),
		MaxSteps:    3,
		AgentConfig: pin,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	var req *space.CoreRequest
	select {
	case req = <-captured:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch never reached Core")
	}
	if req.AgentConfig == nil {
		t.Fatal("Core got nil AgentConfig (pin dropped before runner)")
	}
	if !reflect.DeepEqual(*req.AgentConfig, *pin) {
		t.Fatalf("Core AgentConfig = %+v, want %+v", *req.AgentConfig, *pin)
	}
}

// TestColdDispatchPinsAgentConfigAfterRestart is the restart leg: a turn accepted
// with a pinned revision, whose process then restarts before any checkpoint, must
// hand Core the SAME pinned revision after a cold rehydrate out of durable State.
// This is exactly "a new rollout does not change already-started tasks" made
// concrete: the pin is durable, not just in-memory.
func TestColdDispatchPinsAgentConfigAfterRestart(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}
	pin := &space.AgentConfigRef{Revision: 7, DeploymentVersion: "v2.0.0-rc1"}

	// Process 1: accept with pin, then die (drop in-memory coordinator).
	coord1 := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{}, WorkerID: "w1"})
	_, err := coord1.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:         "pin-cold-" + contracts.NewID(),
		ContextID:   "ctx-pin-cold",
		Input:       json.RawMessage(`{"role":"user","content":"hi"}`),
		MaxSteps:    4,
		AgentConfig: pin,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}

	// Process 2 (post-restart): fresh coordinator over the SAME durable State.
	captured := make(chan *space.CoreRequest, 1)
	coord2 := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{capture: captured}, WorkerID: "w2"})
	if err := coord2.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("cold DispatchTurn: %v", err)
	}
	var req *space.CoreRequest
	select {
	case req = <-captured:
	case <-time.After(2 * time.Second):
		t.Fatal("cold dispatch never reached Core")
	}
	if req.AgentConfig == nil {
		t.Fatal("cold-resumed Core got nil AgentConfig (durable pin lost on restart)")
	}
	if !reflect.DeepEqual(*req.AgentConfig, *pin) {
		t.Fatalf("cold-resumed AgentConfig = %+v, want %+v", *req.AgentConfig, *pin)
	}
}
