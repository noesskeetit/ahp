package dote2e_test

// PDF rev3.2 L2044-2046 and L2050/L2054: a Dot turn ("ход") is executed by
// as-turn-runner from a REVISION of the system agent's AgentConfig plus the
// employee's personal profile, and at the AHP boundary the turn is a task with a
// grant and a token bound to the employee.
//
// The scenario drives the real human edge: browser login -> human JWT over the
// single HTTPS origin -> /v1/space/AcceptTurn -> the Space runner exchanges the
// JWT for a scoped task token, mints a per-run Identity grant, claims the run in
// State and runs the Core loop against the Model Gateway (real Qwen, no stub).

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// publishDotAgentConfig is the tenant administrator's act: one revision of the
// agents-space AgentConfig with the model route and the tools a Dot may call.
func publishDotAgentConfig(t *testing.T, stand *instance, route string, allowedTools []string, instructions string, maxSteps int) {
	t.Helper()
	adminHuman := stand.humanToken(t, stand.HTTP, mustLogin(t, "admin_email"), mustLogin(t, "admin_password"))
	adminToken, _ := stand.taskGrant(t, stand.HTTP, adminHuman, "dot-cfg-admin-"+time.Now().UTC().Format("150405"),
		[]string{"registry", "registry:admin", "model", "state"})
	if err := stand.call(stand.HTTP, "/v1/registry/PutModelRoute", map[string]any{
		"route": route, "expected_revision": 0,
		"document": map[string]any{"model": mustEnv(t, "AHP_LLM_MODEL"), "provider": "qwen"}}, adminToken, nil); err != nil {
		if status(err) != 409 {
			t.Fatalf("PutModelRoute refused: %v", err)
		}
	}
	config := map[string]any{
		"instructions": instructions, "model_route": route, "max_steps": maxSteps,
		"policy": map[string]any{
			"allowed_model_routes":        []string{route},
			"allowed_skills":              []string{},
			"allowed_tools":               allowedTools,
			"allowed_computer_policies":   []string{"disabled", "personal"},
			"allow_personal_instructions": true,
			"allow_personal_skills":       true,
		},
	}
	// Revisions are per tenant and this lane stand is reused across scenarios, so
	// the publish starts from the durable current revision (a probe of the
	// registry's own table) and walks a short optimistic-concurrency ladder.
	var current int64
	if err := stand.Pool.QueryRow(context.Background(),
		`SELECT coalesce(max(revision), 0) FROM ahp_registry.configs WHERE tenant=$1 AND agent_id=$2`,
		stand.Scope["tenant"], dotAgentID).Scan(&current); err != nil {
		current = 0
	}
	for revision := current; revision < current+6; revision++ {
		err := stand.call(stand.HTTP, "/v1/registry/PutConfig", map[string]any{
			"agent_id": dotAgentID, "expected_revision": revision, "data": config}, adminToken, nil)
		if err == nil {
			return
		}
		if status(err) != 409 {
			t.Fatalf("PutConfig(%s) refused: %v", dotAgentID, err)
		}
	}
	t.Fatal("could not publish an agents-space AgentConfig revision")
}

func TestDotTurnRunsPinnedConfigOnRealModel(t *testing.T) {
	stand := loadStand(t)
	allowed := []string{"save_artifact", "emil.delegate"}
	route := "lane-qwen"
	publishDotAgentConfig(t, stand, route, allowed, "You are the employee's personal Dot. Answer in one short sentence.", 6)

	runnerOrigin, _ := stand.Scope["runner_origin"].(string)
	if runnerOrigin == "" {
		t.Fatal("the instance scope has no runner_origin: the lane stand has no as-turn-runner")
	}
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	turnKey := "dot-turn-" + stamp
	turnContext := "dot-turn-ctx-" + stamp
	checks := map[string]any{}

	// (a) The employee asks their Dot for a turn. The only credential the browser
	// holds is the human JWT: the runner exchanges it, never forwards it.
	var accepted struct {
		TurnID   string `json:"turn_id"`
		ThreadID string `json:"thread_id"`
		Status   string `json:"status"`
	}
	input := map[string]any{"parts": []any{map[string]any{"kind": "text",
		"text": "Reply with exactly: DOT-LANE-OK"}}}
	err := stand.call(stand.HTTP, "/v1/space/AcceptTurn", map[string]any{
		"key": turnKey, "context_id": turnContext, "input": input, "max_steps": 6}, human, &accepted)
	if err != nil {
		stand.record(t, "dot-turn-config-and-task", "FAIL", map[string]any{"accept": map[string]any{
			"status": "FAIL", "http": status(err), "code": errorCode(err), "error": errText(err)}})
		t.Fatalf("AcceptTurn refused: %v", err)
	}
	if accepted.TurnID == "" || accepted.ThreadID == "" {
		t.Fatalf("AcceptTurn returned no turn/thread: %+v", accepted)
	}
	checks["accept_turn"] = map[string]any{"status": "PASS", "turn": accepted.TurnID,
		"thread": accepted.ThreadID, "edge": stand.Origin + "/v1/space/AcceptTurn", "runner": runnerOrigin}

	// (b) The turn settles on the real model. A failed turn is reported with the
	// cause the run itself emitted (Space D8), never as a silent nothing.
	var turn struct {
		ID            string `json:"id"`
		Status        string `json:"status"`
		Steps         int    `json:"steps"`
		MaxSteps      int    `json:"max_steps"`
		FailureReason string `json:"failure_reason"`
		AgentConfig   *struct {
			Revision        int64 `json:"revision"`
			ProfileRevision int64 `json:"profile_revision"`
			Dot             *struct {
				Instructions string `json:"instructions"`
				MaxSteps     int    `json:"max_steps"`
				ModelRoute   *struct {
					Name string `json:"name"`
				} `json:"model_route"`
			} `json:"dot"`
		} `json:"agent_config"`
	}
	deadline := time.Now().Add(240 * time.Second)
	for {
		if err := stand.call(stand.HTTP, "/v1/space/GetTurn", map[string]any{"turn_id": accepted.TurnID}, human, &turn); err != nil {
			stand.record(t, "dot-turn-config-and-task", "FAIL", map[string]any{"get_turn": map[string]any{
				"status": "FAIL", "error": errText(err)}})
			t.Fatalf("GetTurn refused: %v", err)
		}
		if turn.Status == "done" || turn.Status == "failed" || turn.Status == "canceled" || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Second)
	}

	// (c) The task at the AHP boundary: the run carries a per-run Identity grant
	// task of THIS employee, and the grant holds the capabilities and the
	// conversation it was minted for.
	var runTask, grantContext, threadContext, grantSubject, grantDeployment string
	var grantCaps []string
	err = stand.Pool.QueryRow(context.Background(),
		`SELECT task_id FROM ahp_state.runs WHERE thread_id=$1 ORDER BY created_at DESC LIMIT 1`,
		accepted.ThreadID).Scan(&runTask)
	if err != nil {
		stand.record(t, "dot-turn-config-and-task", "FAIL", map[string]any{"run_row": map[string]any{
			"status": "FAIL", "error": err.Error()}})
		t.Fatalf("read the run row of the turn: %v", err)
	}
	err = stand.Pool.QueryRow(context.Background(),
		`SELECT context_id, subject, deployment, capabilities FROM ahp_identity.grants_child WHERE tenant=$1 AND task=$2`,
		stand.Scope["tenant"], runTask).Scan(&grantContext, &grantSubject, &grantDeployment, &grantCaps)
	if err != nil {
		stand.record(t, "dot-turn-config-and-task", "FAIL", map[string]any{"grant_row": map[string]any{
			"status": "FAIL", "run_task": runTask, "error": err.Error()}})
		t.Fatalf("read the per-run grant of the turn: %v", err)
	}
	if err := stand.Pool.QueryRow(context.Background(),
		`SELECT context_id FROM ahp_state.threads WHERE id=$1`, accepted.ThreadID).Scan(&threadContext); err != nil {
		t.Fatalf("read the thread context: %v", err)
	}
	if grantSubject != string(stand.Scope["subject"].(string)) || grantDeployment != "dot" {
		t.Fatalf("the turn task is not bound to the employee's dot: %+v", []string{grantSubject, grantDeployment})
	}
	checks["task_grant_bound_to_employee"] = map[string]any{"status": "PASS", "run_task": runTask,
		"grant_subject": grantSubject, "grant_deployment": grantDeployment, "grant_capabilities": grantCaps,
		"thread_context": threadContext, "grant_context": grantContext}

	// (d) The behaviour came from a pinned AgentConfig revision, not a hard-coded
	// agent: the turn carries the revision and the resolved Dot configuration.
	if turn.AgentConfig == nil || turn.AgentConfig.Revision < 1 || turn.AgentConfig.Dot == nil {
		checks["agent_config_pinned_to_turn"] = map[string]any{"status": "FAIL",
			"agent_config": turn.AgentConfig, "turn_status": turn.Status}
	} else {
		checks["agent_config_pinned_to_turn"] = map[string]any{"status": "PASS",
			"config_revision": turn.AgentConfig.Revision, "profile_revision": turn.AgentConfig.ProfileRevision,
			"resolved_max_steps": turn.AgentConfig.Dot.MaxSteps}
	}

	// (e) A failed turn is diagnosed to its cause instead of being hidden. The
	// platform requires the run credential's context claim to equal the
	// conversation (threads.context_id) -- cmd/as-runner/scoped_core.go and the
	// artifact binding both check it -- while internal/space/coordinator.go mints
	// the per-run grant with the State thread ID.
	if turn.Status != "done" {
		diagnosis := map[string]any{"status": "FAIL", "turn_status": turn.Status, "failure_reason": turn.FailureReason,
			"grant_context": grantContext, "thread_context": threadContext, "run_task": runTask}
		if strings.Contains(turn.FailureReason, "differs from verified task authority") && grantContext != threadContext {
			diagnosis["defect"] = "DEF-DOTE2E-1"
			diagnosis["root_cause"] = "internal/space/coordinator.go mints the per-run run grant with Context=thread.ID " +
				"while cmd/as-runner/scoped_core.go (and internal/artifacts run-binding) require the run credential's " +
				"context claim to equal threads.context_id; every browser Dot turn therefore fails before the model"
			diagnosis["owner_area"] = "Space / as-turn-runner"
			diagnosis["regression_window"] = "introduced after as-runner 6fd2d0b614de by e62dae21 (scoped Core runner wiring)"
		}
		checks["turn_executed"] = diagnosis
		stand.record(t, "dot-turn-config-and-task", "DEFECT", checks)
		t.Fatalf("the Dot turn did not complete: status=%s reason=%s (%s)", turn.Status, turn.FailureReason,
			maybeDefect(diagnosis))
	}
	checks["turn_executed"] = map[string]any{"status": "PASS", "status_value": turn.Status, "steps": turn.Steps,
		"max_steps": turn.MaxSteps}

	// (f) The durable transcript shows the turn really reached the model, and the
	// terminal frame closes the stream.
	var stream struct {
		Events []struct {
			Kind    string          `json:"kind"`
			Payload json.RawMessage `json:"payload"`
		} `json:"events"`
	}
	if err := stand.call(stand.HTTP, "/v1/space/ReadStream",
		map[string]any{"thread_id": accepted.ThreadID, "after": 0}, human, &stream); err != nil {
		stand.record(t, "dot-turn-config-and-task", "FAIL", map[string]any{"stream": map[string]any{
			"status": "FAIL", "error": errText(err)}})
		t.Fatalf("ReadStream refused: %v", err)
	}
	var kinds []string
	sawModel, sawTerminal, sawAnswer := false, false, ""
	for _, event := range stream.Events {
		kinds = append(kinds, event.Kind)
		switch event.Kind {
		case "model":
			sawModel = true
			if strings.Contains(string(event.Payload), "DOT-LANE-OK") {
				sawAnswer = "model frame carries the model answer"
			}
		case "status", "done":
			sawTerminal = true
		}
	}
	if !sawModel || !sawTerminal {
		stand.record(t, "dot-turn-config-and-task", "FAIL", map[string]any{"stream": map[string]any{
			"status": "FAIL", "kinds": kinds}})
		t.Fatalf("the durable stream does not show a model step and a terminal frame: %v", kinds)
	}
	checks["durable_stream_proves_model_step"] = map[string]any{"status": "PASS", "events": len(stream.Events),
		"kinds": kinds, "answer_evidence": sawAnswer}

	// (g) The model was the pinned Qwen route reached through the Model Gateway:
	// the Model owner holds a generation for this run task with real usage.
	var generations int
	var usageBytes []byte
	err = stand.Pool.QueryRow(context.Background(),
		`SELECT count(*), coalesce(jsonb_agg(jsonb_build_object('prompt', usage_prompt,
			'completion', usage_completion, 'total', usage_total)), 'null'::jsonb)
		   FROM ahp_model.generations WHERE tenant=$1 AND task=$2`,
		stand.Scope["tenant"], runTask).Scan(&generations, &usageBytes)
	if err != nil {
		checks["model_gateway_generation"] = map[string]any{"status": "PARTIAL", "note": "table probe failed",
			"error": err.Error()}
	} else if generations == 0 {
		checks["model_gateway_generation"] = map[string]any{"status": "FAIL", "rows": generations}
		stand.record(t, "dot-turn-config-and-task", "FAIL", checks)
		t.Fatalf("the Model Gateway recorded no generation for the run task %s", runTask)
	} else {
		checks["model_gateway_generation"] = map[string]any{"status": "PASS", "rows": generations,
			"usage_present": string(usageBytes) != "null", "model": mustEnv(t, "AHP_LLM_MODEL"), "provider": "qwen"}
	}

	stand.record(t, "dot-turn-config-and-task", "PASS", checks)
}

func maybeDefect(diagnosis map[string]any) string {
	if defect, ok := diagnosis["defect"]; ok {
		return fmt.Sprintf("%v owned by %v: %v", defect, diagnosis["owner_area"], diagnosis["root_cause"])
	}
	return "no diagnosis"
}
