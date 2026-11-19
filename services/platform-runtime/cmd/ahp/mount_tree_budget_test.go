//go:build ahp_exec

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/modelapi"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestTreeBudgetMountRequiresComposedStateAuthority(t *testing.T) {
	s := &server{}
	t.Setenv("AHP_MODEL_TREE_REPORTER_WORKLOADS", "")
	if err := s.mountTreeBudget(context.Background(), http.NewServeMux(), nil, nil); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AHP_MODEL_TREE_REPORTER_WORKLOADS", "spiffe://ahp/tenant/test/deployment/space")
	if err := s.mountTreeBudget(context.Background(), http.NewServeMux(), nil, nil); err == nil {
		t.Fatal("standalone Model accepted reporter configuration without State owner")
	}
}

func TestComposedTreeBudgetUsesStateOwnerAndLiveIdentity(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("real PG required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("invalid PG configuration")
	}
	defer pool.Close()
	states := state.New(pool)
	if err := states.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: fmt.Sprintf("tree-mount-%d", time.Now().UnixNano()), Deployment: "space", Subject: "alice"}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("composed-tree", "k", priv)
	v := auth.NewVerifier("composed-tree", map[string]ed25519.PublicKey{"k": pub})
	_, err = ids.CreateRoot(ctx, scope, identity.RootRequest{Task: "grant-task", Context: "ctx-" + scope.Tenant, Deployment: scope.Deployment, Capabilities: []string{"state", "model"}, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	token, err := ids.MintTask(ctx, scope, "grant-task", signer, "ahp", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	thread, err := states.CreateThread(ctx, scope, "ctx-"+scope.Tenant, "thread")
	if err != nil {
		t.Fatal(err)
	}
	run, err := states.CreateRun(ctx, scope, thread.ID, "grant-task", "run", []byte(`{"max_steps":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if run.ID == run.TaskID {
		t.Fatal("fixture must exercise distinct State run and Identity task IDs")
	}
	t.Setenv("AHP_MODEL_TREE_REPORTER_WORKLOADS", auth.WorkloadID(scope))
	s := &server{pool: pool, stateOwner: states, stateOwnerAuthorize: ids.Authorize, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	mux := http.NewServeMux()
	if err := s.mountTreeBudget(ctx, mux, v, ids.Authorize); err != nil {
		t.Fatal(err)
	}
	ca := newMountCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	defer srv.Close()
	hc := srv.Client()
	tr := hc.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
	hc.Transport = tr
	client, err := modelapi.NewTreeBudgetClient(srv.URL, hc)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ForRun(token, run.ID, 1000).ChargeStep(ctx, 0); err == nil {
		t.Fatal("HTTP caller enlarged State owner bound")
	}
	remote := client.ForRun(token, run.ID, 2)
	if err := remote.ChargeStep(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if err := ids.CancelSubtree(ctx, scope, "grant-task"); err != nil {
		t.Fatal(err)
	}
	if err := remote.ChargeStep(ctx, 1); err == nil {
		t.Fatal("revoked Identity grant used tree API")
	}
}
