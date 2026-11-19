package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/treedelegate"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type realDelegationBudget struct{ run *treedelegate.Run }

func (b realDelegationBudget) Reserve(ctx context.Context, ref string, n int) (emil.DelegationGrant, error) {
	r, err := b.run.ReserveDelegation(ctx, contracts.Scope{}, ref, int64(n))
	return emil.DelegationGrant{Ref: ref, Granted: int(r.Granted), Handle: r}, err
}
func (b realDelegationBudget) Settle(ctx context.Context, g emil.DelegationGrant, o emil.DelegationOutcome) error {
	return b.run.SettleDelegation(ctx, g.Handle.(treedelegate.Reservation), treedelegate.Outcome{ChildTaskID: o.ChildTaskID, ReachedChild: o.ReachedChild, StepsConsumed: int64(o.StepsConsumed), Reported: o.Reported})
}

// Only response delivery is fault-injected: admission/dedup, child status and
// parent budget accounting all use production services and real PostgreSQL.
func TestRuntimeDelegationLostAckReconcilesRealGatewayAndBudget(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	bs := model.NewBudgetStore(e.pool)
	if err := bs.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ledger, err := treedelegate.New(bs)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := ledger.ForRun(e.scope, "parent", 3)
	if err != nil {
		t.Fatal(err)
	}
	var executed atomic.Int32
	svc := e.service(t, gateway.ExecutorFunc(func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		executed.Add(1)
		return gateway.ExecResult{State: a2a.TaskStateCompleted, Stats: gateway.ExecStats{StepsConsumed: 1, StepBudget: 3, CloseReason: gateway.CloseReasonModelFinished}}, nil
	}), nil)
	go svc.Run(ctx)
	var served http.Handler
	var dropped atomic.Bool
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		var req struct {
			Method string `json:"method"`
		}
		json.Unmarshal(body, &req)
		rec := httptest.NewRecorder()
		served.ServeHTTP(rec, r)
		if req.Method == "SendMessage" && rec.Code == 200 && dropped.CompareAndSwap(false, true) {
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		w.Write(rec.Body.Bytes())
	}))
	defer proxy.Close()
	cfg := baseCfg()
	cfg.AgentCard = platformCard()
	cfg.AgentCard.SupportedInterfaces[0].URL = proxy.URL + "/a2a/public"
	cfg.Public = gateway.AuthConfig{Middleware: func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: e.scope, Claims: auth.Claims{Kind: auth.KindTask, Tenant: e.scope.Tenant, Deployment: e.scope.Deployment, Subject: e.scope.Subject, Task: e.caller, Root: "root-" + e.caller, Context: "conversation", Grant: "grant-" + e.caller}})))
		})
	}}
	served, err = gateway.NewServer(gateway.NewHandler(svc, gateway.HandlerConfig{}), cfg)
	if err != nil {
		t.Fatal(err)
	}
	a, err := emil.New(emil.Config{BaseURL: proxy.URL, RunAuthorityFor: func(context.Context) *emil.RunAuthority {
		return &emil.RunAuthority{Scope: e.scope, TaskID: "parent", ContextID: "conversation", OperationKey: "core:0", MaxSteps: 3, Budget: realDelegationBudget{budget}}
	}})
	if err != nil {
		t.Fatal(err)
	}
	result, _, err := a.Delegate(ctx, "work")
	if err != nil {
		t.Fatal(err)
	}
	if result.TaskID == "" || !dropped.Load() {
		t.Fatal("did not exercise dropped acknowledgement")
	}
	tasks, _, _, err := e.store.ListTasks(ctx, e.scope, "conversation", "", 100, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || executed.Load() != 1 {
		t.Fatalf("duplicate child after ACK loss: tasks=%d executions=%d", len(tasks), executed.Load())
	}
	root, err := budget.EnsureRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if root.Reserved != 0 || root.Spent != 1 {
		t.Fatalf("reconciled budget spent=%d reserved=%d", root.Spent, root.Reserved)
	}
	// A fresh adapter and fresh Run handle retain the same operation identity.
	recovered, _ := ledger.ForRun(e.scope, "parent", 3)
	again, _ := emil.New(emil.Config{BaseURL: proxy.URL, RunAuthorityFor: func(context.Context) *emil.RunAuthority {
		return &emil.RunAuthority{Scope: e.scope, TaskID: "parent", ContextID: "conversation", OperationKey: "core:0", MaxSteps: 3, Budget: realDelegationBudget{recovered}}
	}})
	replay, _, err := again.Delegate(ctx, "work")
	if err != nil {
		t.Fatal(err)
	}
	if replay.TaskID != result.TaskID || executed.Load() != 1 {
		t.Fatal("restart duplicated child")
	}
	root, _ = recovered.EnsureRoot(ctx)
	if root.Reserved != 0 || root.Spent != 1 {
		t.Fatalf("restart changed settlement %+v", root)
	}
}
