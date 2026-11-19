package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5/pgxpool"
)

type profileUnaryModel struct{ core.ModelClient }

// PostgreSQL + Registry/State owner HTTP with real mTLS and Identity grants.
// The model endpoint is a named contract fixture, not actual Qwen inference.
func TestDotRuntimePinSurvivesCoordinatorRestartAndChangesNextRun(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("real PostgreSQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ids, reg, st := identity.New(pool), registry.New(pool), state.New(pool)
	for _, m := range []func(context.Context) error{ids.Migrate, reg.Migrate, st.Migrate} {
		if err = m(ctx); err != nil {
			t.Fatal(err)
		}
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("dot-pin", "key", priv)
	verifier := auth.NewVerifier("dot-pin", map[string]ed25519.PublicKey{"key": pub})
	scope := contracts.Scope{Tenant: "dot-" + contracts.NewID(), Subject: "alice", Deployment: "space"}
	task := "root-" + contracts.NewID()
	if _, err = ids.CreateRoot(ctx, scope, identity.RootRequest{Task: task, Context: task, Deployment: scope.Deployment, Capabilities: []string{"registry", "state", "model"}, TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	token, err := ids.MintTask(ctx, scope, task, signer, "ahp", 0)
	if err != nil {
		t.Fatal(err)
	}
	rh, err := registry.NewHTTPHandler(reg, verifier, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	sh, err := stateapi.New(st, verifier, ids.Authorize, func(ctx context.Context, s contracts.Scope, taskID string) (string, error) {
		return ids.MintTask(ctx, s, taskID, signer, "ahp", 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/registry/", rh)
	mux.Handle("/v1/state/", sh)
	calls := make(chan map[string]json.RawMessage, 8)
	mux.Handle("/v1/model/SubmitModel", transport.Task(verifier, "ahp", "model", ids.Authorize)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		calls <- in
		transport.WriteJSON(w, 200, model.Generation{ID: contracts.NewID(), Status: model.GenCompleted, Output: `{"text":"model contract fixture"}`})
	})))
	ca := newOwnerCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	server := httptest.NewUnstartedServer(mux)
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	defer server.Close()
	tr := server.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}
	pinner, err := space.NewProfileClient(server.URL, client)
	if err != nil {
		t.Fatal(err)
	}
	sc := space.NewStateClient(server.URL, client)
	if _, err = reg.PutConfig(ctx, scope.Tenant, registry.DotAgentID, 0, json.RawMessage(`{"instructions":"tenant-one","model_route":"fast","max_steps":4,"skills":["writing"],"policy":{"allowed_model_routes":["fast"],"allowed_skills":["writing"],"allowed_tools":[],"allow_personal_instructions":true}}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = reg.PutModelRoute(ctx, scope.Tenant, "fast", 0, json.RawMessage(`{"model":"model-one"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = reg.PutScopedSkill(ctx, scope, false, "writing", "user", 0, json.RawMessage(`{"instructions":"skill-one"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = reg.PutProfile(ctx, scope, 0, json.RawMessage(`{"instructions":"profile-one"}`)); err != nil {
		t.Fatal(err)
	}
	runner := newProfileRuntime(core.RuntimeConfig{}, func(name string) (core.ModelClient, error) {
		m, err := execbridge.NewRemoteModel(server.URL, name, client)
		return profileUnaryModel{m}, err
	})
	newCoordinator := func(worker string) *space.Coordinator {
		return space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, ConfigPinner: pinner, Core: runner, WorkerID: worker})
	}
	firstCoordinator := newCoordinator("old-worker")
	req := space.AcceptTurnRequest{Key: "turn-one", ContextID: task, Input: json.RawMessage(`"hello"`)}
	accepted, err := firstCoordinator.AcceptTurn(ctx, token, scope, req)
	if err != nil {
		t.Fatal(err)
	}
	// Change every mutable source before any worker executes the accepted run.
	if _, err = reg.PutConfig(ctx, scope.Tenant, registry.DotAgentID, 1, json.RawMessage(`{"instructions":"tenant-two","model_route":"fast","max_steps":2,"skills":["writing"],"policy":{"allowed_model_routes":["fast"],"allowed_skills":["writing"],"allowed_tools":[],"allow_personal_instructions":true}}`)); err != nil {
		t.Fatal(err)
	}

	if _, err = reg.PutModelRoute(ctx, scope.Tenant, "fast", 1, json.RawMessage(`{"model":"model-two"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = reg.PutScopedSkill(ctx, scope, false, "writing", "user", 1, json.RawMessage(`{"instructions":"skill-two"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = reg.PutProfile(ctx, scope, 1, json.RawMessage(`{"instructions":"profile-two"}`)); err != nil {
		t.Fatal(err)
	}
	restarted := newCoordinator("restarted-worker")
	replay, err := restarted.AcceptTurn(ctx, token, scope, req)
	if err != nil || replay.TurnID != accepted.TurnID {
		t.Fatalf("retry changed accepted run: %+v %v", replay, err)
	}
	// Cold-dispatch with no in-memory turn from AcceptTurn proves durable restoration.
	cold := newCoordinator("cold-worker")
	if err = cold.DispatchTurn(ctx, token, scope); err != nil {
		t.Fatal(err)
	}
	assertCall := func(want string) {
		t.Helper()
		select {
		case call := <-calls:
			raw, _ := json.Marshal(call)
			if !strings.Contains(string(raw), "model-"+want) || !strings.Contains(string(raw), "skill-"+want) || !strings.Contains(string(raw), "profile-"+want) || !strings.Contains(string(raw), "tenant-"+want) {
				t.Fatalf("wrong pinned model request: %s", raw)
			}
		case <-ctx.Done():
			t.Fatal("no model call after cold dispatch")
		}
	}
	waitDone := func(id string) {
		t.Helper()
		for {
			run, e := sc.GetRun(ctx, token, id)
			if e != nil {
				t.Fatal(e)
			}
			if run.Status == "done" {
				return
			}
			if run.Status == "failed" {
				t.Fatalf("run failed: %s", run.Payload)
			}
			select {
			case <-ctx.Done():
				t.Fatal("run not done")
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	assertCall("one")
	waitDone(accepted.TurnID)
	req.Key = "turn-two"
	req.ThreadID = accepted.ThreadID
	next, err := cold.AcceptTurn(ctx, token, scope, req)
	if err != nil {
		t.Fatal(err)
	}
	if err = cold.DispatchTurn(ctx, token, scope); err != nil {
		t.Fatal(err)
	}
	assertCall("two")
	waitDone(next.TurnID)
}
