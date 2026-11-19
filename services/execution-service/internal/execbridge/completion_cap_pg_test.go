package execbridge

// DEF-DOTE2E-9: one completion cap, both legs, no silent anything.
//
// The as-runner builds its Core runtime without MaxTokens, internal/core then
// defaults that to 4096, and the Model Gateway refuses anything above its own
// cap (AHP_EXEC_MAX_TOKENS, default 1024): a real Dot turn died with
// "model stream refused: HTTP 400". The cap itself is governance and stays:
// over-cap must be refused, never quietly clamped down. What has to change is
// that both legs -- the in-process bridge and the remote runner -- ask for the
// same number the gateway will accept, and that the refusal says which cap it
// hit instead of returning an unhelpful "bounded max_tokens are required".

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func submitBodyWithCap(key string, maxTokens int) string {
	return `{"key":"` + key + `","max_tokens":` + itoa(maxTokens) +
		`,"messages":[{"role":"user","content":"` + capSentinel + `"}]}`
}

const capSentinel = "SENTINEL-cap-prompt-must-not-be-journalled"

// Leg A: what the runner is entitled to ask for -- exactly the cap the gateway
// publishes -- completes on both faces of the same operation.
func TestCompletionAtTheCapCompletes(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "answer at the cap", usage: modelUsage()}

	b, err := New(ctx, pool, Config{Inference: inf, TaskTokenLimit: 40000, StepReserve: 8000, MaxTokens: 256})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	cap := b.maxTokens
	if cap != 256 {
		t.Fatalf("the leg configures a cap of 256, got %d", cap)
	}
	for _, op := range []string{"SubmitModel", "StreamModel"} {
		w := httpModelCallCtx(t, b, ctx, scope, "captask-"+randHex(4), submitBodyWithCap("answer-1", cap))
		if w.Code != http.StatusOK {
			t.Fatalf("%s at the cap (%d) got %d body=%s -- the runner's own legitimate number must work",
				op, cap, w.Code, w.Body.String())
		}
	}
	if inf.count() != 2 {
		t.Fatalf("provider calls=%d, want one per operation", inf.count())
	}
}

// Leg B: over the cap is refused with the cap named, before any spend. The
// status stays 400 (governance), the reason is what makes it diagnosable.
func TestCompletionAboveTheCapIsRefusedAndNamesTheCap(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "must never be produced", usage: modelUsage()}

	b, err := New(ctx, pool, Config{Inference: inf, TaskTokenLimit: 40000, StepReserve: 8000, MaxTokens: 256})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	logs := captureJournal(t)

	w := httpModelCallCtx(t, b, ctx, scope, "captask-"+randHex(4), submitBodyWithCap("answer-1", 4096))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("over-cap got %d body=%s, want 400 (never clamp, never serve)", w.Code, w.Body.String())
	}
	if got := contracts.ReasonOf(decodeContractError(t, w.Body.Bytes())); got != ReasonCompletionCapExceeded {
		t.Fatalf("refusal reason=%q, want %q body=%s -- an operator must see which bound was hit",
			got, ReasonCompletionCapExceeded, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "256") {
		t.Fatalf("the refusal must name the cap the caller exceeded, got %s", w.Body.String())
	}
	if inf.count() != 0 {
		t.Fatalf("provider was called %d times after an over-cap refusal", inf.count())
	}
	assertNothingBooked(t, pool, scope.Tenant)
	if strings.Contains(logs.String(), capSentinel) || strings.Contains(w.Body.String(), capSentinel) {
		t.Fatal("the over-cap refusal carried request content")
	}
}

// Leg C: the in-process leg enforces the same bound as the HTTP leg. Today the
// bridge substitutes its own number only when the caller stated none, so any
// stated over-cap value walks straight through to the provider while the same
// number on the HTTP face is refused -- two legs, two rules.
func TestInProcessStepAboveTheCapIsRefusedLikeTheHTTPLeg(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "must never be produced", usage: modelUsage()}

	b, err := New(ctx, pool, Config{Inference: inf, TaskTokenLimit: 40000, StepReserve: 8000, MaxTokens: 256})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	taskID := "captask-" + randHex(4)
	budget, err := b.TaskBudget(ctx, scope, taskID)
	if err != nil {
		t.Fatalf("task budget: %v", err)
	}
	gm := &gatewayModel{svc: b.svc, budgetID: budget.ID, modelName: b.modelName,
		stepReserve: b.stepReserve, maxTokens: b.maxTokens, class: b.executionClass}
	callCtx := withTurnInfo(ctx, turnInfo{scope: scope, taskID: taskID})

	_, err = gm.Generate(callCtx, &core.ModelRequest{
		Messages:  []core.ChatMessage{{Role: "user", Content: json.RawMessage(`"` + capSentinel + `"`)}},
		MaxTokens: 4096,
	})
	if err == nil {
		t.Fatalf("the in-process leg accepted max_tokens=4096 under a cap of %d: the two legs do not share a rule",
			b.maxTokens)
	}
	if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("over-cap in-process error code=%s, want %s (err=%v)",
			contracts.Code(err), contracts.ErrInvalid, err)
	}
	if got := contracts.ReasonOf(err); got != ReasonCompletionCapExceeded {
		t.Fatalf("in-process refusal reason=%q, want %q", got, ReasonCompletionCapExceeded)
	}
	if inf.count() != 0 {
		t.Fatalf("provider was called %d times after an in-process over-cap refusal", inf.count())
	}
	var gens int64
	if qerr := pool.QueryRow(ctx, `SELECT count(*) FROM ahp_model.generations WHERE tenant=$1`,
		scope.Tenant).Scan(&gens); qerr != nil {
		t.Fatalf("count generations: %v", qerr)
	}
	if gens != 0 {
		t.Fatalf("an over-cap in-process step booked %d generations", gens)
	}
}

// Leg D: a step at or under the cap still passes the in-process leg, and the
// generation carries the caller's number -- the guard bounds, it does not clamp.
func TestInProcessStepAtTheCapKeepsItsOwnNumber(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "in-process answer", usage: modelUsage()}

	b, err := New(ctx, pool, Config{Inference: inf, TaskTokenLimit: 40000, StepReserve: 8000, MaxTokens: 256})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	taskID := "captask-" + randHex(4)
	budget, err := b.TaskBudget(ctx, scope, taskID)
	if err != nil {
		t.Fatalf("task budget: %v", err)
	}
	gm := &gatewayModel{svc: b.svc, budgetID: budget.ID, modelName: b.modelName,
		stepReserve: b.stepReserve, maxTokens: b.maxTokens, class: b.executionClass}
	callCtx := withTurnInfo(ctx, turnInfo{scope: scope, taskID: taskID})

	if _, err = gm.Generate(callCtx, &core.ModelRequest{MaxTokens: 128}); err != nil {
		t.Fatalf("a step at 128 under a cap of 256 was refused: %v", err)
	}
	var asked int
	if qerr := pool.QueryRow(ctx, `SELECT (request->>'MaxTokens')::int FROM ahp_model.generations WHERE tenant=$1`,
		scope.Tenant).Scan(&asked); qerr != nil {
		t.Fatalf("read generation request: %v", qerr)
	}
	if asked != 128 {
		t.Fatalf("the durable generation recorded max_tokens=%d, want the caller's 128 (a clamp would hide the mismatch)", asked)
	}
}
