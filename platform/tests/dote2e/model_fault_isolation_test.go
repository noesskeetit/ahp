package dote2e_test

// DEF-DOTE2E-2 fault isolation: /v1/model/Reserve and /v1/model/SubmitModel
// share the task-budget admission leg (Bridge.TaskBudget -> taskBudgetChain /
// EnsureTaskTree) but only SubmitModel adds routing and the generation insert.
// Probing both on one task token localises the 500 for the Model owner.

import (
	"testing"
	"time"
)

func TestModelPlaneFaultIsolation(t *testing.T) {
	stand := loadStand(t)
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	token, _ := stand.taskGrantContext(t, stand.HTTP, human, "dot-fault-"+stamp,
		"dot-fault-ctx-"+stamp, []string{"model"})
	checks := map[string]any{}

	var reserve map[string]any
	errReserve := stand.call(stand.HTTP, "/v1/model/Reserve",
		map[string]any{"key": "probe-" + stamp, "amount": 100}, token, &reserve)
	checks["reserve"] = map[string]any{"http": status(errReserve), "code": errorCode(errReserve),
		"error": truncate(errText(errReserve), 160), "body": reserve}

	var submit map[string]any
	errSubmit := stand.call(stand.HTTP, "/v1/model/SubmitModel", map[string]any{"key": "probe-" + stamp,
		"messages": []any{map[string]any{"role": "user", "content": "ping"}}}, token, &submit)
	checks["submit"] = map[string]any{"http": status(errSubmit), "code": errorCode(errSubmit),
		"error": truncate(errText(errSubmit), 160)}

	// StreamModel default-cap probe: the as-runner core loop defaults MaxTokens
	// to 4096 (internal/core/runtime.go) while the model bridge caps at 1024
	// (internal/execbridge/bridge.go defaultMaxTokens). Both sides answer 400
	// INVALID above the cap, so every fresh browser turn dies before the
	// provider. Captured here as a precise, reproducible fact.
	streamCheck := func(maxTokens int) map[string]any {
		var out map[string]any
		err := stand.call(stand.HTTP, "/v1/model/StreamModel", map[string]any{
			"key": "probe-stream-" + stamp, "max_tokens": maxTokens,
			"messages": []any{map[string]any{"role": "user", "content": "ping"}}}, token, &out)
		return map[string]any{"http": status(err), "code": errorCode(err), "error": truncate(errText(err), 120)}
	}
	checks["stream_1024"] = streamCheck(1024)
	checks["stream_4096"] = streamCheck(4096)

	localisation := "unknown"
	switch {
	case status(errReserve) == 500 && status(errSubmit) == 500:
		localisation = "task-budget admission (Bridge.TaskBudget: taskBudgetChain/EnsureTaskTree or reconcileBudgetScope)"
	case status(errReserve) == 200 && status(errSubmit) == 500:
		localisation = "post-budget leg (gatewayModel.route or BudgetStore/generation Submit)"
	case errReserve == nil && errSubmit == nil:
		localisation = "no fault on this tenant - defect not reproduced"
	}
	checks["localisation"] = localisation
	verdict := "INFO"
	if status(errReserve) == 500 || status(errSubmit) == 500 {
		verdict = "DEFECT"
		checks["defect"] = "DEF-DOTE2E-2"
		checks["root_cause_area"] = "Model / execbridge"
	}
	stand.record(t, "model-fault-isolation", verdict, checks)
	if verdict == "INFO" {
		t.Skipf("DEF-DOTE2E-2 not reproduced on this tenant: %v", checks["localisation"])
	}
}
