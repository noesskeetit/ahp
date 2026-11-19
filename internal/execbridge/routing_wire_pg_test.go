package execbridge

// Product-level legs for label-aware routing on the face that actually serves
// tasks (PDF rev3.2 :1620-1630, "маршруты с учётом меток"). The routing package's
// own tests prove the decision function; these prove the decision is wired into
// a bridged turn, and specifically that a routing refusal happens BEFORE the
// gateway books anything -- no generation row, no budget hold, no provider call.
// A refusal that reserved first would leak budget on every rejected request and
// would make "route refused" indistinguishable from "provider failed".

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/model/routing"
)

// writeProviderTable drops a provider table on disk for one leg.
func writeProviderTable(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "providers.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write table: %v", err)
	}
	return path
}

const tableSingleTenant = `{
  "version": 1,
  "providers": [{
    "name": "p-shared", "class": "standard",
    "audience": {"kind": "external"},
    "secret_ref": "ahp-hive/execution/routing-test",
    "models": ["fake-gateway-model"]
  }],
  "routes": [{
    "name": "someone-else", "when": {"tenants": ["tenant-this-leg-is-not"]},
    "then": {"provider": "p-shared"}
  }]
}`

const tableOverrideModel = `{
  "version": 1,
  "providers": [{
    "name": "p-override", "class": "standard",
    "audience": {"kind": "external"},
    "secret_ref": "ahp-hive/execution/routing-test",
    "models": ["model-the-route-chose"]
  }],
  "routes": [{
    "name": "policy-chooses-the-model", "when": {"tenants": ["*"]},
    "then": {"provider": "p-override", "model": "model-the-route-chose"}
  }]
}`

const tableClassOnly = `{
  "version": 1,
  "providers": [{
    "name": "p-restricted", "class": "restricted",
    "audience": {"kind": "tenant"},
    "secret_ref": "ahp-hive/execution/routing-test",
    "models": ["fake-gateway-model"]
  }],
  "routes": [{
    "name": "restricted-class-only", "when": {"classes": ["restricted"]},
    "then": {"provider": "p-restricted"}
  }]
}`

// taskFailure is where a refused turn becomes visible: a bridged turn that the
// gateway will not run ends as a FAILED task carrying the reason, and some
// transports surface it as a returned error instead. A leg must accept either
// shape, because both mean the turn did not run.
func taskFailure(res gateway.ExecResult, err error) string {
	if err != nil {
		return err.Error()
	}
	return messageText(res.Message)
}

func modelUsage() model.Usage {
	return model.Usage{PromptTokens: 30, CompletionTokens: 8, TotalTokens: 38}
}

// Leg 1: a tenant no route accepts is refused, and the refusal books nothing.
func TestRoutingRefusalBooksNothing(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "must never be produced", usage: modelUsage()}

	b, err := New(ctx, pool, Config{
		Inference:      inf,
		MaxSteps:       2,
		TaskTokenLimit: 20000,
		StepReserve:    4000,
		RoutingFile:    writeProviderTable(t, tableSingleTenant),
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	if !b.RoutingMounted() {
		t.Fatal("a configured provider table must report routing as mounted")
	}
	if b.RoutingDigest() == "" {
		t.Fatal("routing digest is empty: an operator cannot tell which bytes are in force")
	}

	taskID := "rtask-" + randHex(4)
	res, err := b.Execute(ctx, gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer")),
	})
	if err == nil && res.State == a2a.TaskStateCompleted {
		t.Fatal("a tenant no route names completed: routing is a hint here, not a gate")
	}
	if !strings.Contains(taskFailure(res, err), routing.ReasonNoRoute) {
		t.Fatalf("the refusal must name %q where an operator can see it, got: %v",
			routing.ReasonNoRoute, taskFailure(res, err))
	}
	if inf.count() != 0 {
		t.Fatalf("provider was called %d times after a routing refusal", inf.count())
	}
	assertNothingBooked(t, pool, scope.Tenant)
}

// Leg 2: the deployment's policy, not the environment, decides the model that
// ends up on the durable generation row.
func TestRoutedModelOverridesTheEnvironmentModel(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "routed answer", usage: modelUsage()}

	b, err := New(ctx, pool, Config{
		Inference:      inf,
		MaxSteps:       2,
		TaskTokenLimit: 20000,
		StepReserve:    4000,
		RoutingFile:    writeProviderTable(t, tableOverrideModel),
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	if b.Model() != inf.Model() {
		t.Fatalf("the bridge still reports its own model %q, got %q", inf.Model(), b.Model())
	}

	taskID := "rtask-" + randHex(4)
	res, err := b.Execute(ctx, gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer")),
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.State != a2a.TaskStateCompleted {
		t.Fatalf("state = %v, want COMPLETED", res.State)
	}
	_, mdl, _, _, _, _ := generationRow(t, pool, scope.Tenant, taskID)
	if mdl != "model-the-route-chose" {
		t.Fatalf("generation model = %q, want the model the ROUTE chose (%q): the env value won",
			mdl, "model-the-route-chose")
	}
}

// Leg 3: an unconfigured deployment is untouched -- same model, same behaviour,
// no routing claim. This is the leg that protects every existing acceptance run.
func TestNoRoutingFileKeepsTodayBehaviour(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "plain answer", usage: modelUsage()}

	b, err := New(ctx, pool, Config{
		Inference:      inf,
		MaxSteps:       2,
		TaskTokenLimit: 20000,
		StepReserve:    4000,
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	if b.RoutingMounted() || b.RoutingDigest() != "" {
		t.Fatal("routing must not claim to be mounted when no table was configured")
	}
	taskID := "rtask-" + randHex(4)
	if _, err := b.Execute(ctx, gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer")),
	}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	_, mdl, _, _, _, _ := generationRow(t, pool, scope.Tenant, taskID)
	if mdl != inf.Model() {
		t.Fatalf("generation model = %q, want the environment model %q", mdl, inf.Model())
	}
}

// Leg 4: the execution class takes part in the decision. A route written for the
// restricted class must not answer an unclassified request.
func TestRoutingRefusalNamesTheClassDimension(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "must not run", usage: modelUsage()}

	b, err := New(ctx, pool, Config{
		Inference:      inf,
		MaxSteps:       2,
		TaskTokenLimit: 20000,
		StepReserve:    4000,
		RoutingFile:    writeProviderTable(t, tableClassOnly),
		ExecutionClass: "",
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	taskID := "rtask-" + randHex(4)
	res, err := b.Execute(ctx, gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer")),
	})
	if err == nil && res.State == a2a.TaskStateCompleted {
		t.Fatal("a restricted-class route answered an unclassified request")
	}
	if !strings.Contains(taskFailure(res, err), routing.ReasonNoRoute) {
		t.Fatalf("refusal must name %q, got: %v", routing.ReasonNoRoute, taskFailure(res, err))
	}
	assertNothingBooked(t, pool, scope.Tenant)

	// The same table admits the request once the class is stated, which is what
	// makes the previous refusal a routing decision rather than a broken table.
	b2, err := New(ctx, pool, Config{
		Inference:      &fakeInference{text: "classified answer", usage: modelUsage()},
		MaxSteps:       2,
		TaskTokenLimit: 20000,
		StepReserve:    4000,
		RoutingFile:    writeProviderTable(t, tableClassOnly),
		ExecutionClass: "restricted",
	})
	if err != nil {
		t.Fatalf("new bridge (classified): %v", err)
	}
	scope2 := uniqueScope()
	task2 := "rtask-" + randHex(4)
	if _, err := b2.Execute(ctx, gateway.ExecInput{
		Scope: scope2,
		Task:  gateway.TaskRow{ID: task2, ContextID: "ctx-" + task2},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer")),
	}); err != nil {
		t.Fatalf("classified request must be routed, got: %v", err)
	}
}

// assertNothingBooked is the teeth of leg 1: a refused request left no
// generation row and no hold anywhere in this tenant. The root budget row itself
// is opened before the model loop, so it is expected and is asserted to be
// untouched instead.
func assertNothingBooked(t *testing.T, pool *pgxpool.Pool, tenant string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var gens int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM ahp_model.generations WHERE tenant=$1`, tenant).Scan(&gens); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if gens != 0 {
		t.Fatalf("routing refusal left %d generation rows: the request was booked before it was refused", gens)
	}
	var holds int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ahp_model.budget_reservations r
		JOIN ahp_model.budgets b ON b.id = r.budget_id WHERE b.tenant=$1`, tenant).Scan(&holds); err != nil {
		t.Fatalf("count reservations: %v", err)
	}
	if holds != 0 {
		t.Fatalf("routing refusal left %d budget reservations: a refused route must not hold budget", holds)
	}
	var spent, reserved int64
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(sum(spent),0), coalesce(sum(reserved),0) FROM ahp_model.budgets WHERE tenant=$1`, tenant).
		Scan(&spent, &reserved); err != nil {
		t.Fatalf("read budgets: %v", err)
	}
	if spent != 0 || reserved != 0 {
		t.Fatalf("routing refusal left spent=%d reserved=%d on the tenant budgets", spent, reserved)
	}
}
