package dote2e_test

// Personal-Dot addressing and process recovery over the actual deployed owners.
//
// L2256-2257 ("agent as interlocutor"): a chat whose turns are executed BY the
// addressed agent, separated from the Dot's own turns.
// L930 (recovery): a started turn finishes on its own version before the drain
// ends, without losing budget or effects.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRestartPreflightPreservesProcessOnInvalidCandidate(t *testing.T) {
	child := exec.Command("sleep", "30")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer child.Process.Kill()
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	dir := t.TempDir()
	record, _ := json.Marshal(map[string]any{"backend": map[string]any{"pid": child.Process.Pid, "binary": "ahp", "sha256": "old"}})
	if err := os.WriteFile(filepath.Join(dir, "processes.json"), record, 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("python3", filepath.Join("harness", "restart.py"), "--instance", dir, "--role", "backend", "--bin-dir", filepath.Join(dir, "missing"))
	if err := command.Run(); err == nil {
		t.Fatal("restart accepted a nonexistent candidate")
	}
	select {
	case <-done:
		t.Fatal("restart killed a running process before validating the candidate")
	case <-time.After(150 * time.Millisecond):
	}
}

// TestAgentAsInterlocutorIsNotAddressable records what the Space face accepts.
// A caller cannot name an agent: agent_config is server-owned (refused before any
// durable write), and AcceptTurnRequest carries no field that addresses another
// agent, so there is no second execution path to separate from the Dot's.
func TestAgentAsInterlocutorIsNotAddressable(t *testing.T) {
	stand := loadStand(t)
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	checks := map[string]any{}

	err := stand.call(stand.HTTP, "/v1/space/AcceptTurn", map[string]any{
		"key": "dot-interlocutor-" + stamp, "context_id": "dot-interlocutor-ctx-" + stamp,
		"input":        map[string]any{"parts": []any{map[string]any{"kind": "text", "text": "hello"}}},
		"agent_config": map[string]any{"agent_id": "some-other-agent"}}, human, nil)
	// The Space face decodes strictly and refuses to let a caller name the agent:
	// either by owning agent_config outright or by rejecting the addressing field.
	cannotName := err != nil && (strings.Contains(strings.ToLower(errText(err)), "server-owned") ||
		strings.Contains(errText(err), "agent_id"))
	checks["caller_cannot_name_the_agent_runtime"] = map[string]any{
		"status": map[bool]string{true: "PASS", false: "FAIL"}[cannotName],
		"http":   status(err), "code": errorCode(err), "message": truncate(errText(err), 200)}

	// An addressing field of its own does not exist in the contract at all: the
	// transport names it as unknown, which is the strict answer to "chat with this
	// agent instead".
	var accepted map[string]any
	errAddressed := stand.call(stand.HTTP, "/v1/space/AcceptTurn", map[string]any{
		"key": "dot-interlocutor-ignored-" + stamp, "context_id": "dot-interlocutor-ctx2-" + stamp,
		"agent_id": "some-other-agent",
		"input":    map[string]any{"parts": []any{map[string]any{"kind": "text", "text": "hello"}}}}, human, &accepted)
	unaddressable := errAddressed != nil && status(errAddressed) == 400 &&
		strings.Contains(errText(errAddressed), "unknown field")
	checks["agent_id_field_is_not_part_of_the_contract"] = map[string]any{
		"status":               map[bool]string{true: "PASS", false: "FAIL"}[unaddressable],
		"turn_accepted_anyway": errAddressed == nil, "http": status(errAddressed),
		"message": truncate(errText(errAddressed), 200),
		"note": "the request is refused as an unknown field, so no chat can be bound to another agent's runtime; " +
			"AcceptTurn (internal/space/types.go) carries no agent addressing field"}
	if !cannotName || !unaddressable {
		stand.record(t, "agent-as-interlocutor", "FAIL", checks)
		t.Fatalf("the Space face did not refuse agent addressing as expected: %v / %v",
			errText(err), errText(errAddressed))
	}

	checks["verdict"] = map[string]any{"status": "BLOCKED",
		"defect": "GAP-DOTE2E-6", "owner_area": "Space (Agents Space)",
		"detail": "the PDF separates 'agent as a tool of the Dot' from 'agent as an interlocutor' whose turns " +
			"the agent itself executes. The Space request surface accepts no agent addressing and owns the agent " +
			"configuration, so only the Dot's own execution path exists; the second path cannot be built or " +
			"separated from outside. Separating them inside the Dot's tools (emil.delegate) is covered by the " +
			"delegation scenario, not by this one."}
	stand.record(t, "agent-as-interlocutor", "BLOCKED", checks)
}

// TestRecoveryKeepsDurableStateAcrossRestart checks artifact durability after a
// backend restart, then kills the Dot runner during a real model generation.
// The orphan sweep must resume the same run, config revision and generation after
// lease expiry. This does not claim crash safety of an external side effect.
// Explicit opt-in confines the destructive action to the recorded lane instance.
func TestRecoveryKeepsDurableStateAcrossRestart(t *testing.T) {
	if os.Getenv("AHP_DOT_E2E_ALLOW_RESTART") != "1" {
		// The registry (tools/skip-tags.json) classes dot_e2e_optin as opt_in_only and
		// asked for the form correction: nobody enabling a destructive leg is a CHOICE, so
		// it is SKIP_EXPLICIT - yellow, never green - and not an environment absence.
		t.Skip("SKIP_EXPLICIT[tag=dot_e2e_optin] reason=leg_opt_in_only missing=AHP_DOT_E2E_ALLOW_RESTART -- a skip here means not-covered, not passed: restarting a stand role must be enabled explicitly")
	}
	stand := loadStand(t)
	restoreConfig := preserveRecoveryConfig(t, stand)
	t.Cleanup(func() {
		if err := restoreConfig(); err != nil {
			t.Errorf("restore recovery config: %v", err)
		}
	})
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	contextID := "dot-recovery-ctx-" + stamp
	token, task := stand.taskGrantContext(t, stand.HTTP, human, "dot-recovery-"+stamp, contextID,
		[]string{"artifacts", "state", "memory"})
	checks := map[string]any{}

	blob := []byte("state that must survive a restart " + stamp)
	artifactID, _, _ := stand.upload(t, stand.HTTP, token, task+"-file", "survivor.bin", blob, map[string]any{})
	code, before, err := stand.postRaw(stand.HTTP, "/v1/artifacts/Download", map[string]any{"id": artifactID}, token, nil)
	if err != nil || code != 200 {
		stand.record(t, "recovery-durable-state", "FAIL", map[string]any{"baseline": map[string]any{
			"status": "FAIL", "http": code, "error": errText(err)}})
		t.Fatalf("the artifact is not readable before the restart: %v", errText(err))
	}
	checks["before_restart"] = map[string]any{"status": "PASS", "bytes": len(before)}

	raw, restartErr := restartLaneRole(stand, "backend")
	if restartErr != nil {
		stand.record(t, "recovery-durable-state", "FAIL", map[string]any{"restart": map[string]any{
			"status": "FAIL", "error": restartErr.Error(), "output_tail": truncate(string(raw), 300)}})
		t.Fatalf("the lane stand could not restart its backend: %v", restartErr)
	}
	checks["backend_restarted"] = map[string]any{"status": "PASS", "output_tail": truncate(string(raw), 200)}

	// The new process must hand back the same bytes: the durable plane, not the
	// process, holds the state a resumed run continues from.
	deadline := time.Now().Add(60 * time.Second)
	var after []byte
	var afterCode int
	for {
		afterCode, after, err = stand.postRaw(stand.HTTP, "/v1/artifacts/Download",
			map[string]any{"id": artifactID}, token, nil)
		if err == nil && afterCode == 200 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Second)
	}
	same := afterCode == 200 && digestHex(after) == digestHex(blob)
	checks["artifact_readable_after_restart"] = map[string]any{
		"status": map[bool]string{true: "PASS", false: "FAIL"}[same], "http": afterCode,
		"bytes": len(after), "sha256": digestHex(after)}
	if !same {
		stand.record(t, "recovery-durable-state", "FAIL", checks)
		t.Fatalf("the artifact did not survive the restart: http %d", afterCode)
	}

	var runs int
	if err := stand.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_state.threads WHERE tenant=$1 AND context_id=$2`,
		stand.Scope["tenant"], contextID).Scan(&runs); err != nil {
		stand.record(t, "recovery-durable-state", "FAIL", map[string]any{"state_rows": map[string]any{
			"status": "FAIL", "error": err.Error()}})
		t.Fatalf("read the durable threads of the conversation: %v", err)
	}
	checks["durable_conversation_rows"] = map[string]any{"status": "PASS", "context_id": contextID, "rows": runs}

	// Start real work, then interrupt only its host. The backend and model
	// gateway keep running, so a repeated dispatch must reuse their receipt.
	publishDotAgentConfig(t, stand, "lane-recovery", nil,
		"Write about 250 words explaining durable state. Finish with RECOVERY-MODEL-OK. Do not use tools.", 4)
	var accepted struct {
		TurnID   string `json:"turn_id"`
		ThreadID string `json:"thread_id"`
	}
	err = stand.call(stand.HTTP, "/v1/space/AcceptTurn", map[string]any{
		"key": "recovery-turn-" + stamp, "context_id": "recovery-turn-ctx-" + stamp,
		"input":     map[string]any{"parts": []any{map[string]any{"kind": "text", "text": "Explain the recovery protocol as instructed."}}},
		"max_steps": 4}, human, &accepted)
	if err != nil {
		t.Fatalf("accept recovery turn: %v", err)
	}
	var generation, generationState, runID string
	var epochBefore, pinnedRevision int64
	var confirmedPrefixCount, confirmedPrefixSequence int64
	deadline = time.Now().Add(30 * time.Second)
	for {
		err = stand.Pool.QueryRow(context.Background(), `SELECT g.id,g.status,r.id,t.epoch,
            coalesce((r.payload->'agent_config'->>'revision')::bigint,0),
            (SELECT count(*) FROM ahp_state.stream_events e WHERE e.run_id=r.id AND e.kind='model_delta' AND e.payload->>'generation_id'=g.id),
            coalesce((SELECT max((e.payload->>'delta_sequence')::bigint) FROM ahp_state.stream_events e WHERE e.run_id=r.id AND e.kind='model_delta' AND e.payload->>'generation_id'=g.id),0)
            FROM ahp_state.runs r JOIN ahp_state.threads t ON t.id=r.thread_id
            JOIN ahp_model.generations g ON g.tenant=t.tenant AND g.deployment=t.deployment
                AND (g.task=r.task_id OR g.task=r.id)
            WHERE r.thread_id=$1 AND r.status='running'
                AND g.status IN ('PENDING','RUNNING','STREAMING')
                AND EXISTS(SELECT 1 FROM ahp_state.stream_events e WHERE e.thread_id=t.id AND e.kind='model_intent')
                AND (SELECT count(*) FROM ahp_state.stream_events e WHERE e.run_id=r.id AND e.kind='model_delta' AND e.payload->>'generation_id'=g.id)>=3
            ORDER BY g.created_at LIMIT 1`, accepted.ThreadID).
			Scan(&generation, &generationState, &runID, &epochBefore, &pinnedRevision, &confirmedPrefixCount, &confirmedPrefixSequence)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no in-flight model generation with at least three confirmed fragments to interrupt: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	raw, restartErr = restartLaneRole(stand, "runner")
	if restartErr != nil {
		t.Fatalf("restart runner: %v; %s", restartErr, truncate(string(raw), 200))
	}
	// A new catalog revision must not replace the revision pinned to this run.
	publishDotAgentConfig(t, stand, "lane-recovery", nil,
		"Reply only NEW-CONFIG-MUST-NOT-REPLACE-INFLIGHT-TURN.", 4)
	var final struct {
		Status        string `json:"status"`
		FailureReason string `json:"failure_reason"`
		AgentConfig   struct {
			Revision int64 `json:"revision"`
		} `json:"agent_config"`
	}
	deadline = time.Now().Add(150 * time.Second)
	for {
		err = stand.call(stand.HTTP, "/v1/space/GetTurn", map[string]any{"turn_id": accepted.TurnID}, human, &final)
		if err == nil && (final.Status == "done" || final.Status == "failed" || final.Status == "canceled") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("interrupted turn did not settle: status=%s err=%v", final.Status, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	var epochAfter int64
	var generationCount, completedCount int
	var output string
	if err = stand.Pool.QueryRow(context.Background(), `SELECT epoch FROM ahp_state.threads WHERE id=$1`, accepted.ThreadID).Scan(&epochAfter); err != nil {
		t.Fatal(err)
	}
	if err = stand.Pool.QueryRow(context.Background(), `SELECT count(*),count(*) FILTER(WHERE status='COMPLETED'),coalesce(string_agg(output,''),'')
        FROM ahp_model.generations WHERE tenant=$1 AND deployment='dot' AND task IN
        (SELECT task_id FROM ahp_state.runs WHERE thread_id=$2 UNION SELECT id FROM ahp_state.runs WHERE thread_id=$2)`,
		stand.Scope["tenant"], accepted.ThreadID).Scan(&generationCount, &completedCount, &output); err != nil {
		t.Fatal(err)
	}
	ok := final.Status == "done" && pinnedRevision > 0 && final.AgentConfig.Revision == pinnedRevision && epochAfter > epochBefore &&
		generationCount == 1 && completedCount == 1 && strings.Contains(output, "RECOVERY-MODEL-OK") && !strings.Contains(output, "NEW-CONFIG-MUST-NOT")
	checks["started_turn_resumes_on_its_version"] = map[string]any{
		"status": map[bool]string{true: "PASS", false: "FAIL"}[ok], "turn_id": accepted.TurnID, "run_id": runID,
		"generation_id": generation, "generation_state_at_interrupt": generationState, "epoch_before": epochBefore, "epoch_after": epochAfter,
		"confirmed_fragment_count_at_interrupt": confirmedPrefixCount, "confirmed_fragment_sequence_at_interrupt": confirmedPrefixSequence,
		"config_revision_before": pinnedRevision, "config_revision_after": final.AgentConfig.Revision,
		"generation_count": generationCount, "completed_generation_count": completedCount, "final_status": final.Status,
		"failure_reason": final.FailureReason, "model_marker_present": strings.Contains(output, "RECOVERY-MODEL-OK"),
		"external_side_effect_crash_tested": false}
	if !ok {
		stand.record(t, "recovery-durable-state", "FAIL", checks)
		t.Fatalf("in-flight recovery failed: %+v", checks["started_turn_resumes_on_its_version"])
	}
	if err := restoreConfig(); err != nil {
		t.Fatalf("restore original Dot config: %v", err)
	}
	checks["original_dot_config_restored"] = map[string]any{"status": "PASS"}
	stand.record(t, "recovery-durable-state", "PASS", checks)
}

// The fault test publishes a deliberately different rollout. Restore the
// original data as a new revision so later chats keep their normal behavior.
func preserveRecoveryConfig(t *testing.T, stand *instance) func() error {
	t.Helper()
	admin := stand.humanToken(t, stand.HTTP, mustLogin(t, "admin_email"), mustLogin(t, "admin_password"))
	token, _ := stand.taskGrant(t, stand.HTTP, admin, "recovery-config-restore-"+time.Now().UTC().Format("150405"), []string{"registry", "registry:admin", "state"})
	type configResponse struct {
		Config struct {
			Revision int64           `json:"revision"`
			Data     json.RawMessage `json:"data"`
		} `json:"config"`
	}
	var original configResponse
	if err := stand.call(stand.HTTP, "/v1/registry/GetConfig", map[string]any{"agent_id": dotAgentID}, token, &original); err != nil {
		t.Fatalf("snapshot original Dot config: %v", err)
	}
	if len(original.Config.Data) == 0 {
		t.Fatal("original Dot config is missing")
	}
	restored := false
	return func() error {
		if restored {
			return nil
		}
		var current configResponse
		if err := stand.call(stand.HTTP, "/v1/registry/GetConfig", map[string]any{"agent_id": dotAgentID}, token, &current); err != nil {
			return err
		}
		if err := stand.call(stand.HTTP, "/v1/registry/PutConfig", map[string]any{"agent_id": dotAgentID, "expected_revision": current.Config.Revision, "data": original.Config.Data}, token, nil); err != nil {
			return err
		}
		restored = true
		return nil
	}
}

func restartLaneRole(stand *instance, role string) ([]byte, error) {
	if stand.BinDir == "" {
		return nil, fmt.Errorf("build manifest must record binary_dir before a restart")
	}
	return exec.Command("python3", filepath.Join("harness", "restart.py"), "--instance", stand.Dir, "--role", role, "--bin-dir", stand.BinDir).CombinedOutput()
}
