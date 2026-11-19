package modelapi

// Legs for the read-only tree cost face on real PostgreSQL. The read model has
// its own legs; these prove the HTTP shape a verifier and the UI actually call:
// the caller's tenant comes from the verified principal, another tenant's task
// answers absent, no credential means no answer, and no writer is mounted next
// to the reader.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func costFixture(t *testing.T) (*HTTPHandler, *model.BudgetStore, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("missing real dependency: AHP_TEST_DATABASE_URL is not set, so no budget row exists and the face proved nothing. Export the isolated test DSN (source /tmp/ahp-test-env.sh).")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(pool.Close)
	budgets := model.NewBudgetStore(pool)
	if err := budgets.Migrate(ctx); err != nil {
		t.Fatalf("budgets migrate: %v", err)
	}
	if err := model.NewGenerationStore(pool).Migrate(ctx); err != nil {
		t.Fatalf("generations migrate: %v", err)
	}
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{})
	allow := func(context.Context, auth.Claims, string) error { return nil }
	h, err := NewHTTPHandler(budgets, verifier, allow)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	return h.(*HTTPHandler), budgets, pool
}

func costScope(tenant string) contracts.Scope {
	return contracts.Scope{Tenant: tenant, Subject: "s-h21-cost", Deployment: "d-h21-cost"}
}

// chargeGenerationRow writes what the provider reported for one charge. It is the
// observation side: reconciles compares the ledger against exactly these rows.
func chargeGenerationRow(t *testing.T, pool *pgxpool.Pool, scope contracts.Scope, task, key, budgetID string, in, out int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := json.Marshal(model.ChatRequest{Model: "model-under-test", Messages: []model.Message{{Role: "user", Content: "x"}}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO ahp_model.generations
  (id, tenant, subject, deployment, task, key, model, status, request, effect_key, budget_id,
   usage_prompt, usage_completion, usage_total, digest)
VALUES ($1,$2,$3,$4,$5,$6,'model-under-test','COMPLETED',$7,$8,$9,$10,$11,$12,'digest')
ON CONFLICT (tenant, subject, deployment, key) DO NOTHING`,
		"h21api-g-"+key, scope.Tenant, scope.Subject, scope.Deployment, task, key, req,
		"effect-"+key, budgetID, in, out, in+out); err != nil {
		t.Fatalf("insert generation: %v", err)
	}
}

// seedTree writes a parent task root, a delegated child carved out of it, and the
// charges both sides of the ledger recorded, and returns the numbers the answer
// must reproduce. withObservations controls whether the provider's own generation
// rows exist: without them a charge is unbacked, and the face must say so.
func seedTree(t *testing.T, budgets *model.BudgetStore, pool *pgxpool.Pool, tag string, withObservations bool) (contracts.Scope, string, int64) {
	t.Helper()
	ctx := context.Background()
	scope := costScope(fmt.Sprintf("h21api%s%06d", tag, time.Now().UnixNano()%1000000))
	taskID := "task-" + tag + "-" + fmt.Sprintf("%06d", time.Now().UnixNano()%1000000)
	root, err := budgets.Root(ctx, scope, "a2a-task:"+taskID, "execbridge-task-root", 20000)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	for _, c := range [][2]int64{{700, 661}, {800, 766}} {
		key := fmt.Sprintf("gen:%s:%d", tag, c[0])
		if _, err := budgets.Reserve(ctx, scope, root.ID, key, c[0]); err != nil {
			t.Fatalf("reserve: %v", err)
		}
		if _, err := budgets.Settle(ctx, scope, root.ID, key, c[1]); err != nil {
			t.Fatalf("settle: %v", err)
		}
	}
	childScope := scope
	if withObservations {
		chargeGenerationRow(t, pool, scope, "a2a-task:"+taskID, tag+"-k1-"+scope.Tenant, root.ID, 400, 261)
		chargeGenerationRow(t, pool, scope, "a2a-task:"+taskID, tag+"-k2-"+scope.Tenant, root.ID, 500, 266)
	}
	child, err := budgets.Allocate(ctx, scope, root.ID, childScope, "a2a-task:"+taskID+":child", "delegated:run", 5000)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if _, err := budgets.Reserve(ctx, childScope, child.ID, "gen:child", 100); err != nil {
		t.Fatalf("child reserve: %v", err)
	}
	if _, err := budgets.Settle(ctx, childScope, child.ID, "gen:child", 100); err != nil {
		t.Fatalf("child settle: %v", err)
	}
	if withObservations {
		chargeGenerationRow(t, pool, childScope, "a2a-task:"+taskID+":child", tag+"-k3-"+scope.Tenant, child.ID, 60, 40)
	}
	return scope, taskID, 1527
}

func callCost(t *testing.T, hh *HTTPHandler, body any, scope contracts.Scope, withPrincipal bool) (int, map[string]any) {
	t.Helper()
	fn, ok := hh.routes["TreeCost"]
	if !ok {
		t.Fatal("TreeCost is not wired (would be 404 unknown operation)")
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/model/TreeCost", bytes.NewReader(raw))
	if withPrincipal {
		req = req.WithContext(transport.WithPrincipal(req.Context(), transport.Principal{Scope: scope}))
	}
	w := httptest.NewRecorder()
	fn(w, req)
	out := map[string]any{}
	if b := w.Body.Bytes(); len(b) > 0 {
		_ = json.Unmarshal(b, &out)
	}
	return w.Code, out
}

// Leg 1: the whole tree answers over HTTP, in the numbers the ledger holds.
func TestTreeCostFaceAnswersOverHTTP(t *testing.T) {
	hh, budgets, pool := costFixture(t)
	scope, taskID, wantSpent := seedTree(t, budgets, pool, "ok", true)

	code, out := callCost(t, hh, map[string]any{"task_id": taskID}, scope, true)
	if code != http.StatusOK {
		t.Fatalf("TreeCost for a real tree must be 200, got %d body=%v", code, out)
	}
	if out["task_id"] != taskID {
		t.Fatalf("answer is not about the asked task: %v", out["task_id"])
	}
	units, _ := out["units"].([]any)
	if len(units) != 1 || units[0] != "tokens" {
		t.Fatalf("units = %v, want exactly [tokens] for this tree", out["units"])
	}
	trees, _ := out["trees"].([]any)
	if len(trees) != 1 {
		t.Fatalf("trees = %d, want 1", len(trees))
	}
	tr, _ := trees[0].(map[string]any)
	if int64(tr["spent"].(float64)) != wantSpent {
		t.Fatalf("tree spent = %v, want %d (661+766 parent + 100 child)", tr["spent"], wantSpent)
	}
	nodes, _ := tr["nodes"].([]any)
	if len(nodes) != 2 {
		t.Fatalf("nodes = %d, want parent + delegated child", len(nodes))
	}
	if tr["reconciles"] != true {
		t.Fatalf("a consistent tree must answer reconciles=true, got %v", tr["mismatches"])
	}
	if tr["exhausted"] != false {
		t.Fatal("a tree that spent 1527 of 20000 is not exhausted")
	}
	if ga, _ := out["generated_at"].(string); ga == "" {
		t.Fatal("generated_at missing: a cost figure with no as-of time is not reproducible")
	}
}

// Leg 2: another tenant's task answers absent, never as a forbidden peek at it.
func TestTreeCostFaceDoesNotAnswerForAnotherTenant(t *testing.T) {
	hh, budgets, pool := costFixture(t)
	owner, taskID, _ := seedTree(t, budgets, pool, "fgn", true)

	other := costScope("h21api-foreign")
	other.Subject = owner.Subject
	code, out := callCost(t, hh, map[string]any{"task_id": taskID}, other, true)
	if code != http.StatusNotFound {
		t.Fatalf("a foreign task must answer 404 (same as absent), got %d body=%v", code, out)
	}
	if body, _ := json.Marshal(out); len(body) > 400 {
		t.Fatalf("a refusal must stay short and content-free, got %s", body)
	}
}

// Leg 3: no verified principal -> refused, both at the handler and through the
// real middleware chain (an anonymous HTTP request never reaches the reader).
func TestTreeCostFaceRefusesAnonymousCallers(t *testing.T) {
	hh, budgets, pool := costFixture(t)
	scope, taskID, _ := seedTree(t, budgets, pool, "anon", true)

	if code, out := callCost(t, hh, map[string]any{"task_id": taskID}, scope, false); code != http.StatusForbidden {
		t.Fatalf("a call with no verified principal must be 403, got %d body=%v", code, out)
	}

	// The same request through ServeHTTP goes through transport.Task, which has
	// no key to verify any token with: the answer must be a denial, not data.
	raw, _ := json.Marshal(map[string]any{"task_id": taskID})
	req := httptest.NewRequest(http.MethodPost, "/v1/model/TreeCost", bytes.NewReader(raw))
	w := httptest.NewRecorder()
	hh.ServeHTTP(w, req)
	if w.Code == http.StatusOK {
		t.Fatalf("an unverified request reached the reader through the middleware: %s", w.Body.String())
	}
	if w.Code < 400 {
		t.Fatalf("expected a 4xx denial through the middleware, got %d", w.Code)
	}
}

// Leg 3b: a charge with no provider observation behind it is reported as a
// mismatch instead of being presented as a cost that adds up.
func TestTreeCostFaceReportsMismatchInsteadOfAPrettyNumber(t *testing.T) {
	hh, budgets, pool := costFixture(t)
	scope, taskID, _ := seedTree(t, budgets, pool, "mism", false)

	code, out := callCost(t, hh, map[string]any{"task_id": taskID}, scope, true)
	if code != http.StatusOK {
		t.Fatalf("the tree exists, so the answer is 200 with a warning, got %d body=%v", code, out)
	}
	trees, _ := out["trees"].([]any)
	tr, _ := trees[0].(map[string]any)
	if tr["reconciles"] != false {
		t.Fatalf("1527 charged against zero reported usage must answer reconciles=false, got %v", tr["reconciles"])
	}
	mm, _ := tr["mismatches"].([]any)
	if len(mm) == 0 {
		t.Fatal("reconciles=false without a stated mismatch is not an answer an operator can act on")
	}
}

// Leg 4: the face answers nothing but the read operation -- there is no writer
// hiding under the same prefix.
func TestTreeCostFaceExposesNoWriter(t *testing.T) {
	hh, _, _ := costFixture(t)
	for _, op := range []string{"Reserve", "Settle", "Release", "Allocate", "Budget", ""} {
		req := httptest.NewRequest(http.MethodPost, "/v1/model/"+op, bytes.NewReader([]byte(`{}`)))
		req = req.WithContext(transport.WithPrincipal(req.Context(), transport.Principal{Scope: costScope("h21api-writer")}))
		w := httptest.NewRecorder()
		hh.ServeHTTP(w, req)
		if w.Code != http.StatusNotFound {
			t.Fatalf("POST /v1/model/%s = %d, want 404: the read face must expose no other operation", op, w.Code)
		}
	}
}
