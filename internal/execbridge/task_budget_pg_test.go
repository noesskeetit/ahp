package execbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ahp-platform/ahp/internal/gateway"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func TestDelegatedModelHTTPUsesIdentityParentBudget(t *testing.T) {
	ctx := context.Background()
	pool := dbPool(t)
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := uniqueScope()
	caps := []string{"model", "identity:delegate"}
	_, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: "parent", Context: contracts.NewID(), Deployment: scope.Deployment, Capabilities: caps, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	childScope := scope
	childScope.Deployment = "child-runtime"
	_, err = ids.DelegateToTarget(ctx, scope, "parent", identity.DelegateRequest{Task: "child", Context: contracts.NewID(), Deployment: childScope.Deployment, Capabilities: []string{"model"}, TTL: time.Minute}, identity.DelegationTarget{Tenant: scope.Tenant, Deployment: childScope.Deployment, Capabilities: []string{"model"}})
	if err != nil {
		t.Fatal(err)
	}
	inf := &fakeInference{text: "child answer", usage: model.Usage{TotalTokens: 250}}
	b, err := New(ctx, pool, Config{Inference: inf, TaskTokenLimit: 4000, StepReserve: 2000, MaxTokens: 256})
	if err != nil {
		t.Fatal(err)
	}
	b.WithBudgetLineage(func(ctx context.Context, s contracts.Scope, task string) ([]model.BudgetTask, error) {
		gs, err := ids.TaskAncestry(ctx, s, task)
		if err != nil {
			return nil, err
		}
		var out []model.BudgetTask
		for _, g := range gs {
			out = append(out, model.BudgetTask{Scope: g.Scope(), Task: g.Task})
		}
		return out, nil
	})
	parent, err := b.TaskBudget(ctx, scope, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.budgets.Reserve(ctx, scope, parent.ID, "parent-call", 1000); err != nil {
		t.Fatal(err)
	}
	if _, err = b.budgets.Settle(ctx, scope, parent.ID, "parent-call", 1000); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/v1/model/SubmitModel", bytes.NewBufferString(`{"key":"child-call","messages":[{"role":"user","content":"hello"}]}`))
	r = r.WithContext(transport.WithPrincipal(ctx, transport.Principal{Scope: childScope, Claims: auth.Claims{Kind: auth.KindTask, Task: "child", Context: "ctx"}}))
	w := httptest.NewRecorder()
	b.serveModel(w, r)
	if w.Code != 200 {
		t.Fatalf("%d: %s", w.Code, w.Body.String())
	}
	var g model.Generation
	if err = json.Unmarshal(w.Body.Bytes(), &g); err != nil {
		t.Fatal(err)
	}
	child, err := b.budgets.Get(ctx, childScope, g.BudgetID)
	if err != nil || child.Parent != parent.ID || child.Limit != 2000 || child.Spent != 250 {
		t.Fatalf("child=%+v err=%v", child, err)
	}
	if _, err = b.TaskBudget(ctx, childScope, "unknown"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("unknown task got fresh budget: %v", err)
	}
	if err = ids.CancelSubtree(ctx, scope, "parent"); err != nil {
		t.Fatal(err)
	}
	if _, err = b.TaskBudget(ctx, childScope, "child"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("revoked chain got capacity: %v", err)
	}
}

func TestModelHTTPReservesPromptAndCompletionBeforeDispatch(t *testing.T) {
	pool := dbPool(t)
	inf := &fakeInference{text: "answer", usage: model.Usage{TotalTokens: 10}}
	b, err := New(context.Background(), pool, Config{Inference: inf, TaskTokenLimit: 1000, StepReserve: 100, MaxTokens: 64})
	if err != nil {
		t.Fatal(err)
	}
	scope := uniqueScope()
	body, _ := json.Marshal(map[string]any{"key": "large", "messages": []map[string]string{{"role": "user", "content": strings.Repeat("x", 1500)}}})
	r := httptest.NewRequest("POST", "/v1/model/SubmitModel", bytes.NewReader(body))
	r = r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope, Claims: auth.Claims{Kind: auth.KindTask, Task: "large-task", Context: "ctx"}}))
	w := httptest.NewRecorder()
	b.serveModel(w, r)
	if w.Code == 200 || inf.count() != 0 {
		t.Fatalf("large prompt reached provider: status=%d calls=%d", w.Code, inf.count())
	}
}

func TestGatewayRepresentationSharesCurrentRunBudget(t *testing.T) {
	ctx := context.Background()
	pool := dbPool(t)
	scope := uniqueScope()
	inf := &fakeInference{text: "answer", usage: model.Usage{TotalTokens: 120}}
	b, err := New(ctx, pool, Config{Inference: inf, TaskTokenLimit: 20000, StepReserve: 8000, MaxTokens: 4096})
	if err != nil {
		t.Fatal(err)
	}
	b.WithBudgetLineage(func(context.Context, contracts.Scope, string) ([]model.BudgetTask, error) {
		return []model.BudgetTask{{Scope: scope, Task: "human-authority"}, {Scope: scope, Task: "run"}}, nil
	})
	runBudget, err := b.TaskBudget(ctx, scope, "run")
	if err != nil {
		t.Fatal(err)
	}
	res, err := b.Execute(ctx, gateway.ExecInput{Scope: scope, Task: gateway.TaskRow{ID: "a2a-representation", ContextID: "ctx", Metadata: map[string]any{gateway.MetaGrantTask: "run", gateway.MetaGrantDeployment: scope.Deployment}}, Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("hello"))})
	if err != nil || res.State != a2a.TaskStateCompleted || inf.count() != 1 {
		t.Fatalf("one run was charged a second child allocation: state=%v calls=%d err=%v", res.State, inf.count(), err)
	}
	_, _, id, _, _, _ := generationRow(t, pool, scope.Tenant, "a2a-representation")
	if id != runBudget.ID {
		t.Fatalf("same run billed to independent budget %s instead of %s", id, runBudget.ID)
	}
	trees, err := b.budgets.TreeCostByTask(ctx, scope, "a2a-representation")
	if err != nil || len(trees) != 1 || trees[0].Spent != 120 || len(trees[0].Nodes) != 2 {
		t.Fatalf("alias lost tree cost: %+v err=%v", trees, err)
	}
}
