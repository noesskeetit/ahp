package model

// Cost honesty legs on real PostgreSQL.
//
// Root's finding was that the cost face "reports tokens, not currency". The fix is
// NOT to start calling tokens money. The ledger holds tokens and steps; no
// confirmed price list exists for the provider this platform runs against, and a
// number invented from an assumed price is worse than no number -- it reaches a
// report and is never retracted. So the product answers in the units it actually
// holds, and these legs pin that promise down:
//
//   - the answer's vocabulary contains no currency denomination, at any depth;
//   - every quantity in it is an integer ledger/usage counter, so nothing was
//     multiplied by a per-token price on the way out;
//   - the two denominations that do exist (tokens for a task root, steps for a
//     delegated run) are returned as separate trees and are never summed;
//   - the provider configuration refuses to carry a price field, so a tariff
//     cannot be smuggled in silently and read back as a cost.
//
// Each leg also asserts a positive number, so a response that simply went empty
// cannot pass.

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"
)

// currencyDenomination matches any key that would present the answer in money.
var currencyDenomination = regexp.MustCompile(`(?i)(usd|eur|rub|ruble|dollar|cent|money|price|pric|tariff|fiat|currenc|invoice|cash|payable|amount_due)`)

// integerOnlyKeys are the quantities this answer is allowed to contain. Any key
// not listed here is refused by name, which is what catches a future cost_usd
// the first time someone adds it.
var accountingVocabulary = map[string]bool{
	// envelope
	"trees": true,
	// trees
	"tree_root": true, "unit": true, "root": true, "nodes": true,
	"limit": true, "held_for_children": true, "spent": true, "open_holds": true,
	"committed": true, "remaining": true, "available_to_root": true, "exhausted": true,
	"generations": true, "tokens_in": true, "tokens_out": true, "tokens_total": true,
	"reconciles": true, "mismatches": true,
	// nodes
	"budget_id": true, "parent_budget_id": true, "task": true, "budget_key": true,
	"scope": true, "tenant": true, "deployment": true, "subject": true,
	"depth": true, "reserved": true, "closed": true, "settled_actual": true,
}

// answerFact is one leaf of the marshalled answer, addressed by path.
type answerFact struct {
	path, key string
	value     any
}

// walkAnswer flattens the decoded answer into leaves. Objects are descended into;
// their own keys are recorded so a missing-value object cannot hide one.
func walkAnswer(v any, path string, out *[]answerFact) {
	switch node := v.(type) {
	case map[string]any:
		for k, val := range node {
			sub := path + "." + k
			switch val.(type) {
			case map[string]any, []any:
				*out = append(*out, answerFact{path: sub, key: k})
				walkAnswer(val, sub, out)
			default:
				*out = append(*out, answerFact{path: sub, key: k, value: val})
			}
		}
	case []any:
		for i, el := range node {
			walkAnswer(el, fmt.Sprintf("%s[%d]", path, i), out)
		}
	}
}

// decodeAnswer marshals the read model exactly as a caller sees it.
func decodeAnswer(t *testing.T, trees []CostTree) map[string]any {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"trees": trees})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return decoded
}

// assertNoCurrencyVocabulary is the shared guard: no money key, no fractional
// quantity, no key outside the accounting vocabulary.
func assertNoCurrencyVocabulary(t *testing.T, decoded map[string]any) {
	t.Helper()
	var leaves []answerFact
	walkAnswer(decoded, "", &leaves)
	if len(leaves) == 0 {
		t.Fatal("the answer has no leaves at all: a guard that matches nothing proves nothing")
	}
	sawQuantity := false
	for _, leaf := range leaves {
		if currencyDenomination.MatchString(leaf.key) {
			t.Fatalf("the cost answer speaks in money: key %q (path %s) is a currency denomination", leaf.key, leaf.path)
		}
		if !accountingVocabulary[leaf.key] {
			t.Fatalf("unexpected key %q at %s: the cost answer's vocabulary is closed, and an unlisted number is how an assumed price reaches a report",
				leaf.key, leaf.path)
		}
		switch v := leaf.value.(type) {
		case float64:
			if v != float64(int64(v)) {
				t.Fatalf("leaf %s holds %v, a fractional quantity: token and step counts are integers, so a fraction means a price was applied",
					leaf.path, v)
			}
			sawQuantity = true
		case string:
			if v == "tokens" || v == "steps" {
				sawQuantity = true
			}
		}
	}
	if !sawQuantity {
		t.Fatal("the answer carried no accounting quantity at all")
	}
}

// TestTreeCostAnswerContainsNoCurrencyDenomination is the negative root asked for:
// a real token tree, marshalled the way the face serves it, cannot contain money.
func TestTreeCostAnswerContainsNoCurrencyDenomination(t *testing.T) {
	pool := costPool(t)
	budgets := NewBudgetStore(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scope := costScope(costTenant("nocurr"))
	root, err := budgets.Root(ctx, scope, "a2a-task:task-nocurr", budgetKeyTaskTokenRoot, 20000)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	reserveAndSettle(t, budgets, scope, root.ID, "gen:1", 700, 661)
	reserveAndSettle(t, budgets, scope, root.ID, "gen:2", 800, 766)
	chargeGenerationRow(t, pool, scope, "a2a-task:task-nocurr", "h21k-nc1-"+scope.Tenant, root.ID, 400, 261)
	chargeGenerationRow(t, pool, scope, "a2a-task:task-nocurr", "h21k-nc2-"+scope.Tenant, root.ID, 500, 266)

	trees, err := budgets.TreeCostByTask(ctx, scope, "task-nocurr")
	if err != nil {
		t.Fatalf("tree cost: %v", err)
	}
	if len(trees) != 1 {
		t.Fatalf("trees = %d, want one", len(trees))
	}
	if trees[0].Unit != CostUnitTokens {
		t.Fatalf("unit = %q, want %q: this leg is about a token tree", trees[0].Unit, CostUnitTokens)
	}
	// Positive control: the numbers a caller would be tempted to price are here.
	if trees[0].Spent != 1427 {
		t.Fatalf("spent = %d, want 1427 tokens", trees[0].Spent)
	}
	if trees[0].TokensTotal != 1427 {
		t.Fatalf("tokens_total = %d, want 1427 (the provider's own report must be present, not replaced)", trees[0].TokensTotal)
	}
	assertNoCurrencyVocabulary(t, decodeAnswer(t, trees))
}

// TestTreeCostUnitsAreLedgerUnitsOnly pins the unit vocabulary itself: an
// unrecognised denomination is reported as unknown, never as a currency, and
// never as a guess.
func TestTreeCostUnitsAreLedgerUnitsOnly(t *testing.T) {
	pool := costPool(t)
	budgets := NewBudgetStore(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scope := costScope(costTenant("unitvoc"))
	root, err := budgets.Root(ctx, scope, "treedelegate-run:run-unitvoc", budgetKeyRunStepRoot, 3)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	reserveAndSettle(t, budgets, scope, root.ID, StepKeyForTest(0), 1, 1)

	trees, err := budgets.TreeCostByTask(ctx, scope, "run-unitvoc")
	if err != nil {
		t.Fatalf("tree cost: %v", err)
	}
	if len(trees) != 1 {
		t.Fatalf("trees = %d, want one step tree", len(trees))
	}
	allowed := map[string]bool{CostUnitTokens: true, CostUnitSteps: true, CostUnitUnknown: true}
	for _, tr := range trees {
		if !allowed[tr.Unit] {
			t.Fatalf("tree unit %q is outside the ledger vocabulary %v", tr.Unit, allowed)
		}
		for _, n := range tr.Nodes {
			if !allowed[n.Unit] {
				t.Fatalf("node %s unit %q is outside the ledger vocabulary", n.BudgetID, n.Unit)
			}
		}
	}
	if trees[0].Unit != CostUnitSteps {
		t.Fatalf("unit = %q, want %q", trees[0].Unit, CostUnitSteps)
	}
	if trees[0].Spent != 1 {
		t.Fatalf("spent = %d, want 1 step", trees[0].Spent)
	}
	assertNoCurrencyVocabulary(t, decodeAnswer(t, trees))
}

// TestTreeCostNeverSumsTwoDenominations is the arithmetic half of honesty. One
// task id can carry a token tree and a step tree; the answer keeps them apart,
// because 1427 tokens + 3 steps is not a number.
func TestTreeCostNeverSumsTwoDenominations(t *testing.T) {
	pool := costPool(t)
	budgets := NewBudgetStore(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scope := costScope(costTenant("twodenom"))
	tokenRoot, err := budgets.Root(ctx, scope, "a2a-task:task-shared", budgetKeyTaskTokenRoot, 20000)
	if err != nil {
		t.Fatalf("token root: %v", err)
	}
	reserveAndSettle(t, budgets, scope, tokenRoot.ID, "gen:1", 700, 661)
	reserveAndSettle(t, budgets, scope, tokenRoot.ID, "gen:2", 800, 766)

	stepRoot, err := budgets.Root(ctx, scope, "treedelegate-run:task-shared", budgetKeyRunStepRoot, 4)
	if err != nil {
		t.Fatalf("step root: %v", err)
	}
	reserveAndSettle(t, budgets, scope, stepRoot.ID, StepKeyForTest(0), 1, 1)
	reserveAndSettle(t, budgets, scope, stepRoot.ID, StepKeyForTest(1), 1, 1)

	trees, err := budgets.TreeCostByTask(ctx, scope, "task-shared")
	if err != nil {
		t.Fatalf("tree cost: %v", err)
	}
	if len(trees) != 2 {
		t.Fatalf("trees = %d, want two (one per denomination)", len(trees))
	}
	byUnit := map[string]CostTree{}
	for _, tr := range trees {
		if _, clash := byUnit[tr.Unit]; clash {
			t.Fatalf("two trees share unit %q: the answer must not split one denomination", tr.Unit)
		}
		byUnit[tr.Unit] = tr
	}
	tokens, ok := byUnit[CostUnitTokens]
	if !ok {
		t.Fatalf("no token tree in %v", byUnit)
	}
	steps, ok := byUnit[CostUnitSteps]
	if !ok {
		t.Fatalf("no step tree in %v", byUnit)
	}
	if tokens.Spent != 1427 || steps.Spent != 2 {
		t.Fatalf("spent must stay in its own unit: tokens=%d want 1427, steps=%d want 2", tokens.Spent, steps.Spent)
	}
	// No tree is allowed to carry the other denomination's total.
	if tokens.TokensTotal != 0 {
		t.Fatalf("the token tree reports a provider total of %d with no generation rows: unbacked numbers are not cost", tokens.TokensTotal)
	}
	if steps.TokensTotal != 0 {
		t.Fatalf("the step tree reports %d tokens: a step tree's spend is not a token count", steps.TokensTotal)
	}
	assertNoCurrencyVocabulary(t, decodeAnswer(t, trees))
}

// TestABudgetNamedLikeMoneyYieldsNoDenomination closes the contract from the
// writer's side. A denomination is a property of the ledger key the product's own
// writers use, not of whatever a caller calls a budget: a root opened under a
// money-sounding key reports NO denomination (unknown) and refuses to reconcile,
// rather than lending its numbers a currency nobody priced.
func TestABudgetNamedLikeMoneyYieldsNoDenomination(t *testing.T) {
	pool := costPool(t)
	budgets := NewBudgetStore(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scope := costScope(costTenant("moneynamed"))
	root, err := budgets.Root(ctx, scope, "a2a-task:task-moneynamed", "money:dollars-budget", 500)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	reserveAndSettle(t, budgets, scope, root.ID, "gen:1", 100, 50)

	trees, err := budgets.TreeCostByTask(ctx, scope, "task-moneynamed")
	if err != nil {
		t.Fatalf("tree cost: %v", err)
	}
	if len(trees) != 1 {
		t.Fatalf("trees = %d, want one", len(trees))
	}
	if trees[0].Unit != CostUnitUnknown {
		t.Fatalf("unit = %q: a denomination cannot be minted by naming a budget; only the ledger's own keys carry one", trees[0].Unit)
	}
	if trees[0].checkReconciliation() {
		t.Fatal("a tree whose denomination this plane does not know reconciled anyway: an unknown number was presented as a settled figure")
	}
	if len(trees[0].Mismatches) == 0 {
		t.Fatal("the refusal to reconcile carries no reason")
	}
	// What is served still speaks only in ledger quantities.
	assertNoCurrencyVocabulary(t, decodeAnswer(t, trees))
}
