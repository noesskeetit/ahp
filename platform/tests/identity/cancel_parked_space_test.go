package identity_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestColdParkedSpaceCancelRevokesDurablyAndRetries(t *testing.T) {
	h := newHarnessAudience(t, "ahp")
	ctx := testContext(t)
	scope := uniqueScope(t)
	pool, _ := isolatedPool(t)
	t.Cleanup(pool.Close)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	caps := append(append([]string{}, fullCaps...), "state")
	_, err := h.store.CreateRoot(ctx, scope, identity.RootRequest{Task: "parent", Context: ctxID(scope, "authority"), Deployment: scope.Deployment, Capabilities: caps, TTL: time.Hour, AllowSelf: true})
	if err != nil {
		t.Fatal(err)
	}
	parentToken := h.mint(t, scope, "parent")
	handler, err := stateapi.New(st, h.verifier, h.store.Authorize, func(ctx context.Context, s contracts.Scope, task string) (string, error) {
		return h.store.MintTask(ctx, s, task, h.signer, "ahp", 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{h.ca.serverCert(t)}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: h.caPool()}
	srv.StartTLS()
	defer srv.Close()
	client := h.taskClient(t, scope)
	sc := space.NewStateClient(srv.URL, client)
	issuer, err := space.NewIdentityDelegateIssuer(h.baseURL, client)
	if err != nil {
		t.Fatal(err)
	}
	issuer.Audience = "ahp"
	th, err := sc.CreateThread(ctx, parentToken, scope, "chat", contracts.NewID())
	if err != nil {
		t.Fatal(err)
	}
	childTask := "rtask-child"
	childToken, err := issuer.Delegate(ctx, parentToken, "parent", space.RunGrantSpec{Task: childTask, Context: th.ID, Capabilities: caps, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	siblingToken, err := issuer.Delegate(ctx, parentToken, "parent", space.RunGrantSpec{Task: "rtask-sibling", Context: ctxID(scope, "sibling"), Capabilities: caps, TTL: time.Minute})
	if err != nil || siblingToken == "" {
		t.Fatal(err)
	}
	run, err := sc.CreateRun(ctx, childToken, th.ID, "run", json.RawMessage(`{"type":"turn","run_task":"rtask-child"}`), childTask)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := sc.ClaimRun(ctx, "", "worker", time.Minute)
	if err != nil || claim == nil || claim.Lease == nil {
		t.Fatalf("claim %v", err)
	}
	if err := sc.Suspend(ctx, childToken, run.ID, claim.Lease.Worker, claim.Lease.Epoch, "hitl_response", nil); err != nil {
		t.Fatal(err)
	}
	cfg := space.CoordinatorConfig{StateClient: sc, RunGrantIssuer: issuer, RequireRunGrant: true}
	for _, status := range []space.TurnStatus{space.TurnAwaiting, space.TurnCanceled} {
		cold := space.NewCoordinator(cfg)
		cold.RehydrateTurn(space.Turn{ID: run.ID, RunID: run.ID, ThreadID: th.ID, RunTask: childTask, Scope: scope, Status: status})
		if err := cold.CancelTurn(space.WithAuthorityTask(ctx, "parent"), run.ID, parentToken); err != nil {
			t.Fatalf("cold cancellation (%s): %v", status, err)
		}
		got, err := st.GetRun(ctx, scope, run.ID)
		if err != nil || got.Status != state.RunCanceled {
			t.Fatalf("durable cancellation: %+v %v", got, err)
		}
		grant, err := h.store.Get(ctx, scope, childTask)
		if err != nil || grant.Status != identity.StatusRevoked {
			t.Fatalf("child grant remains live: %+v %v", grant, err)
		}
	}
	sibling, err := h.store.Get(ctx, scope, "rtask-sibling")
	if err != nil || sibling.Status != identity.StatusActive {
		t.Fatalf("sibling revoked: %+v %v", sibling, err)
	}
}
