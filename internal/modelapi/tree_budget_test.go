package modelapi_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/modelapi"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/treedelegate"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestTreeBudgetOwnerAPI(t *testing.T) {
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
	budgets := model.NewBudgetStore(pool)
	if err = budgets.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("budget-test", "k", priv)
	v := auth.NewVerifier("budget-test", map[string]ed25519.PublicKey{"k": pub})
	scope := contracts.Scope{Tenant: fmt.Sprintf("budget-api-%d", time.Now().UnixNano()), Subject: "alice", Deployment: "runner"}
	states := state.New(pool)
	if err := states.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	thread, err := states.CreateThread(ctx, scope, "ctx", "thread")
	if err != nil {
		t.Fatal(err)
	}
	rootRun, err := states.CreateRun(ctx, scope, thread.ID, "root-task", "root", []byte(`{"max_steps":3}`))
	if err != nil {
		t.Fatal(err)
	}
	cancelRun, err := states.CreateRun(ctx, scope, thread.ID, "cancel-task", "cancel", []byte(`{"max_steps":2}`))
	if err != nil {
		t.Fatal(err)
	}
	var stateRevoked atomic.Bool
	authority, err := modelapi.NewStateTreeAuthority(states, []string{auth.WorkloadID(scope)}, func(_ context.Context, _ auth.Claims, capability string) error {
		if capability != "state" || stateRevoked.Load() {
			return contracts.NewError(contracts.ErrForbidden, "State read denied")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var revoked atomic.Bool
	h, err := modelapi.NewTreeBudgetHandler(budgets, v, func(context.Context, auth.Claims, string) error {
		if revoked.Load() {
			return contracts.NewError(contracts.ErrForbidden, "revoked")
		}
		return nil
	}, authority)
	if err != nil {
		t.Fatal(err)
	}
	ca := newCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	defer srv.Close()
	hc := srv.Client()
	tr := hc.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
	hc.Transport = tr
	token := func(subject, task string) string {
		tok, err := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: subject, Task: task, Root: task, Context: "ctx", Grant: "grant"}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	c, err := modelapi.NewTreeBudgetClient(srv.URL, hc)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ForRun(token("alice", "unbound-task"), rootRun.ID, 1000000).ChargeStep(ctx, 0); err == nil {
		t.Error("body invented an unconfirmed root budget")
	}
	if err := c.ForRun(token("alice", "root-task"), rootRun.ID, 1000000).ChargeStep(ctx, 0); err == nil {
		t.Fatal("body overrode persisted run bound")
	}
	// A running task may already have charged steps before the API migration.
	legacyLedger, _ := treedelegate.New(budgets)
	legacyRun, _ := legacyLedger.ForRun(scope, rootRun.ID, 3)
	if err := legacyRun.ChargeStep(ctx, 0); err != nil {
		t.Fatal(err)
	}
	run := c.ForRun(token("alice", "root-task"), rootRun.ID, 3)
	if err = run.ChargeStep(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if err = run.ChargeStep(ctx, 0); err != nil {
		t.Fatal("step replay", err)
	}
	var wins atomic.Int32
	var wg sync.WaitGroup
	var grant modelapi.DelegationGrant
	var lock sync.Mutex
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			g, e := run.Reserve(ctx, fmt.Sprintf("child-%d", i), 2)
			if e == nil {
				wins.Add(1)
				lock.Lock()
				grant = g
				lock.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("competing grants won=%d want1", wins.Load())
	}
	replay, err := run.Reserve(ctx, grant.Ref, 2)
	if err != nil || replay != grant {
		t.Fatalf("reserve replay: %+v %v", replay, err)
	}
	for _, foreign := range []*modelapi.TreeRun{c.ForRun(token("mallory", "root-task"), rootRun.ID, 3), c.ForRun(token("alice", "other-task"), rootRun.ID, 3)} {
		if err := foreign.Settle(ctx, grant, modelapi.DelegationOutcome{}); err == nil {
			t.Fatal("foreign task/subject settled grant")
		}
	}
	if err := run.Settle(ctx, modelapi.DelegationGrant{Ref: "missing", Granted: 2}, modelapi.DelegationOutcome{}); err == nil {
		t.Fatal("settle created missing grant")
	}
	if err := run.Settle(ctx, grant, modelapi.DelegationOutcome{ReachedChild: true, ChildTaskID: "child-task", Reported: false}); err != nil {
		t.Fatal(err)
	}
	if err := run.Settle(ctx, grant, modelapi.DelegationOutcome{ReachedChild: true, ChildTaskID: "child-task", Reported: false}); err != nil {
		t.Fatal("settle replay", err)
	}
	ledger, _ := treedelegate.New(budgets)
	local, _ := ledger.ForRun(scope, rootRun.ID, 3)
	snap, err := local.EnsureRoot(ctx)
	if err != nil || snap.Spent != 3 || snap.Reserved != 0 {
		t.Fatalf("UNKNOWN must charge grant: %+v %v", snap, err)
	}
	if err := run.ChargeStep(ctx, 1); err == nil {
		t.Fatal("overspent root")
	}
	if err := c.ForRun(token("alice", "root-task"), rootRun.ID, 4).ChargeStep(ctx, 1); err == nil {
		t.Fatal("changed bound accepted")
	}
	canceled := c.ForRun(token("alice", "cancel-task"), cancelRun.ID, 2)
	g, err := canceled.Reserve(ctx, "unsent", 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := canceled.Settle(ctx, g, modelapi.DelegationOutcome{ReachedChild: false}); err != nil {
		t.Fatal(err)
	}
	if err := canceled.ChargeStep(ctx, 0); err != nil {
		t.Fatal("canceled hold not released", err)
	}

	customScope := scope
	customScope.Deployment = "custom-agent"
	customThread, err := states.CreateThread(ctx, customScope, "ctx", "thread")
	if err != nil {
		t.Fatal(err)
	}
	customRun, err := states.CreateRun(ctx, customScope, customThread.ID, "custom-task", "run", []byte(`{"max_steps":3}`))
	if err != nil {
		t.Fatal(err)
	}
	customHC := *hc
	customTR := tr.Clone()
	customTR.TLSClientConfig = tr.TLSClientConfig.Clone()
	customTR.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(customScope))}
	customHC.Transport = customTR
	customClient, err := modelapi.NewTreeBudgetClient(srv.URL, &customHC)
	if err != nil {
		t.Fatal(err)
	}
	customToken, err := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: customScope.Deployment, Subject: scope.Subject, Task: "custom-task", Root: "custom-task", Context: "ctx", Grant: "custom"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	custom := customClient.ForRun(customToken, customRun.ID, 3)
	if err := custom.ChargeStep(ctx, 0); err == nil {
		t.Fatal("custom workload obtained tree reporter privilege")
	}
	if err := custom.Settle(ctx, grant, modelapi.DelegationOutcome{ReachedChild: false, Reported: true, StepsConsumed: 0}); err == nil {
		t.Fatal("custom workload submitted a forged zero outcome")
	}
	foreignContext, err := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject, Task: "cancel-task", Root: "cancel-task", Context: "foreign-context", Grant: "foreign"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ForRun(foreignContext, cancelRun.ID, 2).ChargeStep(ctx, 1); err == nil {
		t.Fatal("wrong context charged run")
	}
	stateRevoked.Store(true)
	if err := canceled.ChargeStep(ctx, 1); err == nil {
		t.Fatal("State owner read gate bypassed")
	}
	stateRevoked.Store(false)
	revoked.Store(true)
	if err := canceled.ChargeStep(ctx, 1); err == nil {
		t.Fatal("revoked token accepted")
	}
}
