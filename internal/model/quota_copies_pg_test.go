package model

// Provider file COPIES under their own quotas (PDF rev3.2 :1620-1630 "квоты
// провайдеров" among the things Model Gateway owns; :323 "квоты RPM/TPM, попытки";
// :3494 names RPM/TPM as what runs out first).
//
// A provider table can list the same upstream twice -- a trusted copy inside the
// tenant and a second copy outside it -- and label-aware routing picks between them
// per request. That only means anything if the LIMITS are per copy as well: two
// copies under one shared window is one copy with a longer name. These legs ask the
// two questions that follow, against real Postgres and a real generation:
//
//   - the window that pays is the copy that SERVED, not the copy the environment
//     happened to name, and exhausting the served copy refuses the next attempt
//     instead of quietly falling back to the copy that still has room;
//   - what each copy spent is what the tree says it spent, so "what did this task
//     cost" (PDF :347) is answered across copies without double counting.

import (
	"context"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// quotaCopies mounts a quota store and gives each leg two distinct copy names.
func quotaCopies(t *testing.T) (*modelEnv, *QuotaStore, string, string) {
	t.Helper()
	env := newModelEnv(t)
	q := NewQuotaStore(env.pool)
	if err := q.Migrate(context.Background()); err != nil {
		t.Fatalf("quota migrate: %v", err)
	}
	return env, q, "copy-in-tenant-" + randHex(4), "copy-external-" + randHex(4)
}

// copyWindow reads one copy's window counters out of band. Limits are set by the
// leg for every copy it names, so an untouched copy answers zero -- which is the
// fact the legs need: "this copy was not charged", not "we could not tell".
func copyWindow(t *testing.T, q *QuotaStore, provider, tenant string) QuotaWindow {
	t.Helper()
	w, err := q.Window(context.Background(), provider, tenant)
	if err != nil {
		t.Fatalf("window(%s): %v", provider, err)
	}
	return w
}

// Leg: the copy that served pays. A turn routed to the external copy must not be
// booked against the in-tenant copy, and exhausting the served copy must refuse
// rather than fall through to the copy with room.
func TestQuotaFollowsTheRoutedProviderCopy(t *testing.T) {
	ctx := context.Background()
	env, q, inTenant, external := quotaCopies(t)
	tenant := env.scope.Tenant
	// Both copies are bounded, with DIFFERENT limits, so a booking on one is
	// distinguishable from a booking on the other.
	if err := q.Set(ctx, ProviderQuota{Provider: inTenant, Tenant: tenant, RPM: 10, TPM: 100_000, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set in-tenant limit: %v", err)
	}
	if err := q.Set(ctx, ProviderQuota{Provider: external, Tenant: tenant, RPM: 1, TPM: 100_000, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set external limit: %v", err)
	}
	inf := &fakeInference{model: "qwen-test"}
	inf.streamText([]string{"ok"}, Usage{PromptTokens: 40, CompletionTokens: 10, TotalTokens: 50})
	// The deployment's configured copy is the in-tenant one -- what the environment
	// names. The routing decision overrides it per request.
	svc := env.service(inf).WithQuota(q, inTenant).WithProviderClients(testProviderClients{inTenant: inf, external: inf})

	gen, err := svc.Submit(ctx, SubmitArgs{
		Scope: env.scope, Task: "task-copies-1", Key: "cp-gen-1",
		Request: chatReq("answer"), Provider: external,
	})
	if err != nil {
		t.Fatalf("submit routed to the external copy: %v", err)
	}
	if _, err := svc.Execute(ctx, env.scope, gen.ID); err != nil {
		t.Fatalf("execute: %v", err)
	}

	charged := copyWindow(t, q, external, tenant)
	if charged.Unlimited {
		t.Fatal("the copy that served reports no limits at all: its quota row did not take effect, so nothing bounds it")
	}
	if charged.Requests != 1 {
		t.Fatalf("external copy window = %d requests, want 1 (the turn was routed there)", charged.Requests)
	}
	if other := copyWindow(t, q, inTenant, tenant); other.Requests != 0 || other.Tokens != 0 {
		t.Fatalf("the in-tenant copy was booked (%+v) for a turn routing sent elsewhere: a per-deployment window cannot express per-copy limits, so the restricted copy would be unlimited", other)
	}

	// The served copy is at its RPM limit. The next attempt to the SAME copy must
	// be refused -- and must not be served by the other copy, which still has room.
	// Falling back would be the silent over-spend the PDF rules out (:1793 "исчерпанный
	// резерв -- отказ, а не перерасход", same principle for provider windows).
	_, err = svc.Submit(ctx, SubmitArgs{
		Scope: env.scope, Task: "task-copies-1", Key: "cp-gen-2",
		Request: chatReq("answer"), Provider: external,
	})
	if err == nil {
		t.Fatal("Submit accepted a generation over the ROUTED copy's RPM limit")
	}
	if contracts.ReasonOf(err) != ReasonProviderRPMExhausted {
		t.Fatalf("reason = %q, want %q", contracts.ReasonOf(err), ReasonProviderRPMExhausted)
	}
	var gens int
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM ahp_model.generations WHERE tenant=$1`, tenant).Scan(&gens); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if gens != 1 {
		t.Fatalf("generations rows = %d, want 1: a refused attempt must not exist", gens)
	}
	if calls := inf.count(); calls != 1 {
		t.Fatalf("provider called %d times, want 1: exhaustion of the routed copy must not be answered by dialling another copy", calls)
	}

	// And the refusal is per copy, not per tenant: the same tenant on the copy that
	// still has room is admitted normally.
	other, err := svc.Submit(ctx, SubmitArgs{
		Scope: env.scope, Task: "task-copies-1", Key: "cp-gen-3",
		Request: chatReq("answer"), Provider: inTenant,
	})
	if err != nil {
		t.Fatalf("submit to the copy with room: %v", err)
	}
	if _, err := svc.Execute(ctx, env.scope, other.ID); err != nil {
		t.Fatalf("execute on the in-tenant copy: %v", err)
	}
	if w := copyWindow(t, q, inTenant, tenant); w.Requests != 1 {
		t.Fatalf("in-tenant copy window = %+v, want exactly 1 request", w)
	}
}

// Leg: what each copy spent is what the tree says it spent. Parent served by one
// copy, delegated child by the other, one tree cost that adds up, no copy charged
// for the other's turn.
func TestRoutedCopySpendLandsInTheTreeCost(t *testing.T) {
	ctx := context.Background()
	env, q, inTenant, external := quotaCopies(t)
	scope := env.scope
	if err := q.Set(ctx, ProviderQuota{Provider: inTenant, Tenant: scope.Tenant, RPM: 10, TPM: 100_000, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set in-tenant limit: %v", err)
	}
	if err := q.Set(ctx, ProviderQuota{Provider: external, Tenant: scope.Tenant, RPM: 10, TPM: 100_000, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set external limit: %v", err)
	}
	inf := &fakeInference{model: "qwen-test"}
	inf.streamText([]string{"ok"}, Usage{PromptTokens: 300, CompletionTokens: 100, TotalTokens: 400})
	svc := env.service(inf).WithQuota(q, inTenant).WithProviderClients(testProviderClients{inTenant: inf, external: inf})

	root, err := env.budget.Root(ctx, scope, "a2a-task:task-copies", budgetKeyTaskTokenRoot, 20000)
	if err != nil {
		t.Fatalf("root budget: %v", err)
	}
	// Parent turn, served by the copy the environment names.
	parent, err := svc.Submit(ctx, SubmitArgs{
		Scope: scope, Task: "task-copies", Key: "tree-p-1", Request: chatReq("answer"),
		BudgetID: root.ID, ReserveKey: "tree-p-1:reserve", ReserveAmount: 5000,
	})
	if err != nil {
		t.Fatalf("parent submit: %v", err)
	}
	if _, err := svc.Execute(ctx, scope, parent.ID); err != nil {
		t.Fatalf("parent execute: %v", err)
	}

	// Delegated child, routed to the SECOND copy with its own limit row.
	childScope := scope
	childScope.Deployment = "d-child-copies"
	child, err := env.budget.Allocate(ctx, scope, root.ID, childScope, "a2a-task:task-copies-child", "delegated:run", 8000)
	if err != nil {
		t.Fatalf("allocate child: %v", err)
	}
	childGen, err := svc.Submit(ctx, SubmitArgs{
		Scope: childScope, Task: "task-copies-child", Key: "tree-c-1", Request: chatReq("answer"),
		BudgetID: child.ID, ReserveKey: "tree-c-1:reserve", ReserveAmount: 5000,
		Provider: external,
	})
	if err != nil {
		t.Fatalf("child submit on the second copy: %v", err)
	}
	if _, err := svc.Execute(ctx, childScope, childGen.ID); err != nil {
		t.Fatalf("child execute: %v", err)
	}

	trees, err := env.budget.TreeCostByTask(ctx, scope, "task-copies")
	if err != nil {
		t.Fatalf("tree cost: %v", err)
	}
	if len(trees) != 1 {
		t.Fatalf("trees = %d, want one", len(trees))
	}
	tr := trees[0]
	if len(tr.Nodes) != 2 {
		t.Fatalf("nodes = %d, want parent + child: %+v", len(tr.Nodes), tr.Nodes)
	}
	if tr.Generations != 2 || tr.TokensTotal != 800 {
		t.Fatalf("tree = gens %d tokens %d, want 2 generations and 800 tokens (400 per copy)", tr.Generations, tr.TokensTotal)
	}
	if !tr.Reconciles {
		t.Fatalf("a consistent two-copy tree reported a mismatch: %v", tr.Mismatches)
	}
	// The cross-check the verifier asked for: the tree total equals the sum of what
	// the two provider windows booked, each copy charged for its own turn only.
	parentWindow := copyWindow(t, q, inTenant, scope.Tenant)
	childWindow := copyWindow(t, q, external, scope.Tenant)
	if parentWindow.Requests != 1 || childWindow.Requests != 1 {
		t.Fatalf("window requests = in-tenant %d external %d, want 1 and 1 (each copy served exactly one turn)",
			parentWindow.Requests, childWindow.Requests)
	}
	if parentWindow.Tokens != 400 || childWindow.Tokens != 400 {
		t.Fatalf("window tokens = in-tenant %d external %d, want 400 and 400 converged to real usage",
			parentWindow.Tokens, childWindow.Tokens)
	}
	if parentWindow.Tokens+childWindow.Tokens != tr.TokensTotal {
		t.Fatalf("per-copy spend %d + %d does not equal the tree total %d: the tree either drops a copy or counts one twice",
			parentWindow.Tokens, childWindow.Tokens, tr.TokensTotal)
	}
}

// Explicit provider binding for quota-only tests. Physical destinations are tested by execbridge.
type testProviderClients map[string]Inference

func (p testProviderClients) InferenceFor(_ context.Context, provider string) (Inference, error) {
	inf, ok := p[provider]
	if !ok {
		return nil, contracts.NewError(contracts.ErrForbidden, "provider not configured")
	}
	return inf, nil
}
