//go:build state_integration

package space_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// State API, PostgreSQL, mTLS and task JWT verification are real. Identity
// delegation and the Registry snapshot are controlled seams, not live owners.
type agentAcceptanceIssuer struct{ e *cEnv }

func (i agentAcceptanceIssuer) Delegate(_ context.Context, _, _ string, spec space.RunGrantSpec) (string, error) {
	return i.e.signer.Sign(auth.Claims{Audience: cAudience, Kind: auth.KindTask, Tenant: i.e.tenant, Deployment: i.e.deployment, Subject: cSubject, Task: spec.Task, Context: spec.Context, Root: "root", Grant: spec.Task, Depth: 1}, time.Minute)
}
func (agentAcceptanceIssuer) Cancel(context.Context, string, string) error { return nil }

type agentAcceptancePin struct {
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
	e       *cEnv
}

func (p *agentAcceptancePin) PinAgentTurn(ctx context.Context, token, installation string) (*space.AgentTargetRef, error) {
	claims, err := p.e.verifier.Verify(token, cAudience, auth.KindTask, auth.WorkloadID(contracts.Scope{Tenant: p.e.tenant, Deployment: p.e.deployment}))
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(claims.Task, "rtask-") || claims.Context != "ctx-1" {
		return nil, contracts.NewError(contracts.ErrForbidden, "pin did not receive this conversation's child authority")
	}
	if p.calls.Add(1) <= 2 && p.entered != nil {
		p.entered <- struct{}{}
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &space.AgentTargetRef{InstallationID: installation, AgentID: "agent-" + installation, VersionID: "v1", Digest: "sha256:" + strings.Repeat("a", 64), PinID: claims.Task, StateCompat: "v1", StatePartition: "agent"}, nil
}

func TestAddressedChatFirstSelectionIsAtomicAcrossReplicas_RealState(t *testing.T) {
	e := cNewEnv(t)
	parent := e.signTask(t, "parent")
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
	thread, err := e.client.CreateThread(e.ctx, parent, scope, "ctx-1", "conversation")
	if err != nil {
		t.Fatal(err)
	}
	control, err := space.NewAgentControlClient(e.srv.URL, e.srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	pinner := &agentAcceptancePin{e: e, entered: make(chan struct{}, 2), release: make(chan struct{})}
	newCoordinator := func() *space.Coordinator {
		return space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, AgentPinner: pinner, AgentControl: control, RequireRunGrant: true, RunGrantIssuer: agentAcceptanceIssuer{e}, TurnDispatchTimeout: -1})
	}
	type result struct {
		response *space.AcceptTurnResponse
		err      error
		req      space.AcceptTurnRequest
	}
	results := make(chan result, 2)
	ctx := space.WithAuthorityTask(e.ctx, "parent")
	for _, installation := range []string{"one", "two"} {
		req := space.AcceptTurnRequest{Key: "choose-" + installation, ThreadID: thread.ID, ContextID: "ctx-1", AgentInstallationID: installation, Input: json.RawMessage(`"hello"`)}
		go func() {
			response, err := newCoordinator().AcceptTurn(ctx, parent, scope, req)
			results <- result{response, err, req}
		}()
	}
	// Both replicas completed history reads while no run existed. Only the
	// database's keyed Enqueue may choose the winner, not process-local locks.
	for range 2 {
		select {
		case <-pinner.entered:
		case <-e.ctx.Done():
			t.Fatal("both replicas did not reach pin")
		}
	}
	close(pinner.release)
	var winner result
	accepted, refused := 0, 0
	for range 2 {
		r := <-results
		if r.err == nil {
			accepted++
			winner = r
		} else if contracts.Code(r.err) == contracts.ErrConflict {
			refused++
		} else {
			t.Fatal(r.err)
		}
	}
	if accepted != 1 || refused != 1 {
		t.Fatalf("two replicas chose %d winners and %d refusals", accepted, refused)
	}
	runs, err := e.client.ListRuns(e.ctx, parent, thread.ID)
	if err != nil || len(runs) != 1 {
		t.Fatalf("durable turns: %d %v", len(runs), err)
	}
	var payload struct {
		Installation string                `json:"agent_installation_id"`
		Target       *space.AgentTargetRef `json:"agent_target"`
	}
	if err = json.Unmarshal(runs[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Installation != winner.req.AgentInstallationID || payload.Target == nil || payload.Target.InstallationID != payload.Installation {
		t.Fatal("durable target differs from winning chat")
	}
	inbox, err := e.client.ReadInbox(e.ctx, parent, thread.ID, 0, 10)
	if err != nil || len(inbox) != 1 || inbox[0].Kind != "executor_binding" {
		t.Fatal("binding was not durably recorded once", err)
	}
	// A fresh Coordinator handles an ACK-loss replay and the next turn without
	// either sharing the original process map or changing this conversation.
	replayed, err := newCoordinator().AcceptTurn(ctx, parent, scope, winner.req)
	if err != nil || replayed.TurnID != winner.response.TurnID {
		t.Fatal("ACK replay created another turn", err)
	}
	next := winner.req
	next.Key = "next-turn"
	next.AgentInstallationID = ""
	inherited, err := newCoordinator().AcceptTurn(ctx, parent, scope, next)
	if err != nil || inherited.AgentInstallationID != winner.req.AgentInstallationID {
		t.Fatal("new process lost chat target", err)
	}
	foreign, err := e.signer.Sign(auth.Claims{Audience: cAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment, Subject: "other-user", Task: "foreign", Context: "ctx-1", Root: "foreign", Grant: "foreign"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.client.GetThread(e.ctx, foreign, thread.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("foreign subject accessed conversation", err)
	}
}
