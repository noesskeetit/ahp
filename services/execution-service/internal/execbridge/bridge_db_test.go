package execbridge

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// These tests need the isolated Postgres only. No provider credential and no
// object storage is involved: the point of the gate is precisely that a model
// turn is admitted, journalled and settled by the Model Gateway even though
// this process holds no key at all.
//
// Set AHP_TEST_DATABASE_URL to run them.

// dbPool returns a pool over the real isolated PostgreSQL the bridge's durability
// claims are meaningless without. A missing real dependency is a FAIL with the
// reason and the remedy, never a silent pass: a quiet t.Skip here is how "N PASS"
// comes to mean nothing. The only allowed downgrade is the explicit
// AHP_ALLOW_SKIP_REAL_DB=1 opt-out, and it prints SKIP_EXPLICIT so a run that
// used it cannot be counted as acceptance.
func dbPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("AHP_ALLOW_SKIP_REAL_DB") == "1" {
			t.Log("SKIP_EXPLICIT AHP_TEST_DATABASE_URL unset -- opt-out skip, NOT counted as acceptance")
			t.Skip("SKIP_EXPLICIT: real PostgreSQL required for the Model Gateway bridge")
		}
		t.Fatalf("missing real dependency: AHP_TEST_DATABASE_URL is not set, so no budget, " +
			"ledger or task row was ever written and the bridge proved nothing. Export the " +
			"isolated test DSN (private/root-acceptance-pg/execution.env) and rerun, or set " +
			"AHP_ALLOW_SKIP_REAL_DB=1 for a SKIP_EXPLICIT skip that acceptance does not count.")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping isolated postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func uniqueScope() contracts.Scope {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	sfx := hex.EncodeToString(b)
	return contracts.Scope{Tenant: "dlv-br-" + sfx, Deployment: "dep-1", Subject: "user:dlv-" + sfx}
}

type fakeStream struct {
	emitted bool
	text    string
	usage   model.Usage
}

// Next yields the whole answer as one delta, then reports end of stream.
func (s *fakeStream) Next(context.Context) (model.Delta, error) {
	if !s.emitted {
		s.emitted = true
		return model.Delta{Text: s.text, FinishReason: "stop"}, nil
	}
	return model.Delta{}, model.ErrStreamDone
}

func (s *fakeStream) Usage() model.Usage { return s.usage }
func (s *fakeStream) Close() error       { return nil }

// fakeInference counts provider calls: the admission test asserts it stays zero.
type fakeInference struct {
	mu      sync.Mutex
	calls   int
	text    string
	usage   model.Usage
	modelNm string
}

func (f *fakeInference) Model() string {
	if f.modelNm != "" {
		return f.modelNm
	}
	return "fake-gateway-model"
}

func (f *fakeInference) Stream(context.Context, model.ChatRequest) (model.Stream, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return &fakeStream{text: f.text, usage: f.usage}, nil
}

func (f *fakeInference) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// generationRow finds the bridged generation for one task. Steps are zero-based
// in core.Runtime, so the key is matched by task rather than by step index.
func generationRow(t *testing.T, pool *pgxpool.Pool, tenant, taskID string) (status, mdl, budgetID, key string, reserved, total int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var st, bm, bd sql.NullString
	err := pool.QueryRow(ctx, `SELECT status, model, budget_id, key, reserved, usage_total
		FROM ahp_model.generations
		WHERE tenant=$1 AND task=$2 AND key LIKE 'execbridge:%'
		ORDER BY created_at, key LIMIT 1`, tenant, taskID).
		Scan(&st, &bm, &bd, &key, &reserved, &total)
	if errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("no Model Gateway generation row for tenant %q task %q", tenant, taskID)
	}
	if err != nil {
		t.Fatalf("query generation: %v", err)
	}
	return st.String, bm.String, bd.String, key, reserved, total
}

// Gate 1: a bridged turn is a durable Model Gateway generation, billed to the
// task's root budget, with usage settled from real provider accounting.
func TestExecuteRoutesTurnThroughModelGateway(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "gateway answer", usage: model.Usage{PromptTokens: 40, CompletionTokens: 12, TotalTokens: 52}}

	b, err := New(ctx, pool, Config{
		Inference:      inf,
		MaxSteps:       2,
		TaskTokenLimit: 20000,
		StepReserve:    4000,
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	if !b.ProviderConfigured() {
		t.Fatal("a configured handle must be reported")
	}
	if b.Model() != inf.Model() {
		t.Fatalf("model = %q, want %q", b.Model(), inf.Model())
	}

	taskID := "dlv-task-" + randHex(4)
	res, err := b.Execute(ctx, gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer via the gateway")),
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.State != a2a.TaskStateCompleted {
		t.Fatalf("state = %v (%s), want COMPLETED", res.State, messageText(res.Message))
	}
	if got := messageText(res.Message); got != "gateway answer" {
		t.Fatalf("agent message = %q", got)
	}
	if inf.count() == 0 {
		t.Fatal("the fake inference was never called: the turn did not go through the gateway")
	}

	status, mdl, budgetID, genKey, reserved, total := generationRow(t, pool, scope.Tenant, taskID)
	if status != model.GenCompleted {
		t.Fatalf("generation status = %s", status)
	}
	if want := stepKey(taskID, 0, 0); genKey != want {
		t.Fatalf("generation key = %q, want the first (zero-based) step key %q", genKey, want)
	}
	if mdl != inf.Model() {
		t.Fatalf("generation model = %q", mdl)
	}
	if budgetID == "" {
		t.Fatal("generation is not billed to a budget")
	}
	if total != 52 {
		t.Fatalf("generation usage_total = %d, want 52", total)
	}
	if reserved != 4000 {
		t.Logf("note: generation.reserved stays %d (the row records the hold; the budget is what settles)", reserved)
	}

	root, err := b.TaskBudget(ctx, scope, taskID)
	if err != nil {
		t.Fatalf("task budget: %v", err)
	}
	if root.ID != budgetID {
		t.Fatalf("generation billed to %s, task root budget is %s", budgetID, root.ID)
	}
	if root.Task != taskBudgetTask(taskID) || root.Key != RootBudgetKey {
		t.Fatalf("unexpected root budget identity: task=%q key=%q", root.Task, root.Key)
	}
	if root.Limit != 20000 {
		t.Fatalf("root limit = %d, want 20000", root.Limit)
	}
	if root.Reserved != 0 {
		t.Fatalf("reservation was not released: reserved=%d", root.Reserved)
	}
	if root.Spent != 52 {
		t.Fatalf("root spent = %d, want the real usage 52", root.Spent)
	}
}

// Gate 1 fail-closed half: a task whose ceiling cannot admit one step is denied
// by budget admission BEFORE any provider call is made.
func TestExecuteDeniesOverBudgetTaskBeforeProviderCall(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "should never run"}

	b, err := New(ctx, pool, Config{
		Inference:      inf,
		MaxSteps:       2,
		TaskTokenLimit: 100, // cannot admit a single 4000-token step
		StepReserve:    4000,
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	taskID := "dlv-over-" + randHex(4)
	res, err := b.Execute(ctx, gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("spend everything")),
	})
	if err != nil {
		t.Fatalf("an over-budget turn is a terminal task failure, not a transport error: %v", err)
	}
	if res.State != a2a.TaskStateFailed {
		t.Fatalf("state = %v, want FAILED", res.State)
	}
	if got := messageText(res.Message); !strings.Contains(got, "budget admission") {
		t.Fatalf("failure must name budget admission, got %q", got)
	}
	if calls := inf.count(); calls != 0 {
		t.Fatalf("provider was called %d times despite a denied budget", calls)
	}
	root, err := b.TaskBudget(ctx, scope, taskID)
	if err != nil {
		t.Fatalf("task budget: %v", err)
	}
	if root.Reserved != 0 || root.Spent != 0 {
		t.Fatalf("denied task left spend behind: reserved=%d spent=%d", root.Reserved, root.Spent)
	}
}

// The no-provider composition must drain a task to FAILED through a real
// GenFailed generation, with the reservation released and zero charged.
func TestExecuteWithoutProviderSettlesBudgetToZero(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()

	b, err := New(ctx, pool, Config{
		Inference:      UnconfiguredInference{},
		MaxSteps:       1,
		TaskTokenLimit: 20000,
		StepReserve:    4000,
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	if b.ProviderConfigured() {
		t.Fatal("no provider is configured here")
	}
	taskID := "dlv-noprov-" + randHex(4)
	res, err := b.Execute(ctx, gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("hello")),
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.State != a2a.TaskStateFailed {
		t.Fatalf("state = %v, want FAILED", res.State)
	}
	if got := messageText(res.Message); !strings.Contains(got, "not configured") {
		t.Fatalf("task failure must state the missing provider, got %q", got)
	}
	status, _, budgetID, _, _, total := generationRow(t, pool, scope.Tenant, taskID)
	if status != model.GenFailed {
		t.Fatalf("generation status = %s, want FAILED", status)
	}
	if budgetID == "" || total != 0 {
		t.Fatalf("failed generation must be billed with zero usage: budget=%q total=%d", budgetID, total)
	}
	root, err := b.TaskBudget(ctx, scope, taskID)
	if err != nil {
		t.Fatalf("task budget: %v", err)
	}
	if root.Reserved != 0 || root.Spent != 0 {
		t.Fatalf("a turn that never left the process must cost nothing: reserved=%d spent=%d", root.Reserved, root.Spent)
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
