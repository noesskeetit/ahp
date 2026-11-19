package dote2e_test

// TestPersonalDotUserJourneyOnRealOwners is the single coherent personal-Dot
// user path on the real owners (PDF rev3.2): profile -> model (real Qwen) ->
// tool result via GetResult -> memory -> file (bytes+SHA) -> history, with two
// users isolated (foreign denied), HITL approve and cancel, and reload of the
// durable record on a fresh session. Data isolation: every official run uses a
// fresh tenant/bucket produced by the lane harness prepare (plus a per-run
// stamp inside the tenant). The browser-turn completion branch is expected RED
// while DEF-DOTE2E-1 (Space coordinator run-grant context, owner: Banach)
// stands; it is recorded, never faked. The restart-without-duplicate-effect leg
// runs as TestRecoveryKeepsDurableStateAcrossRestart in the same suite run
// (run-journey.sh), because it restarts this lane stand.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPersonalDotUserJourneyOnRealOwners(t *testing.T) {
	stand := loadStand(t)
	publishDotAgentConfig(t, stand, "lane-qwen", []string{"record_note", "save_artifact"},
		"You are the employee's personal Dot. Answer in one short sentence.", 6)
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	ctxID := "dot-journey-ctx-" + stamp
	token, task := stand.taskGrantContext(t, stand.HTTP, human, "dot-journey-"+stamp, ctxID,
		[]string{"state", "model", "tools", "memory", "artifacts", "registry", "space:access"})
	checks := map[string]any{}
	fail := func(leg string) {
		checks["failed_legs"] = append(legList(checks["failed_legs"]), leg)
		// Soft-fail (re-shoot directive): a failed leg is recorded and the run
		// CONTINUES, so one broken leg can never hide the terminal status of the
		// legs after it - the artifact must carry J1..J11 honestly. t.Errorf keeps
		// the go test red, and the overall verdict below still fails the run.
		t.Errorf("journey leg %s failed (recorded, continuing)", leg)
	}

	// J1 - the turn boundary: the task carries a grant and a token (L2054).
	var grantRows int
	if err := stand.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_identity.grants_child WHERE tenant=$1 AND task=$2 AND context_id=$3`,
		stand.Scope["tenant"], task, ctxID).Scan(&grantRows); err != nil {
		t.Fatalf("grant probe: %v", err)
	}
	// Honest gate (DEF-VERDICT-1): PASS is written only after the grant row is
	// proven; a task without a grant is a FAIL, never a PASS retracted later.
	if grantRows == 0 {
		checks["j1_task_grant_token"] = map[string]any{"status": "FAIL", "grant_rows": grantRows}
		fail("j1")
	}
	setLeg(checks, "j1_task_grant_token", map[string]any{"status": "PASS", "grant_rows": grantRows})

	// J2 - the Dot runs from the pinned AgentConfig revision and profile (L2044).
	var pinned struct {
		Snapshot struct {
			Revision int64 `json:"revision"`
		} `json:"snapshot"`
	}
	if err := stand.call(stand.HTTP, "/v1/registry/PinDotConfig",
		map[string]any{"key": task + "-pin", "context_id": ctxID}, token, &pinned); err != nil {
		checks["j2_config_profile_pin"] = map[string]any{"status": "FAIL", "error": errText(err)}
		fail("j2")
	}
	setLeg(checks, "j2_config_profile_pin", map[string]any{"status": "PASS", "config_revision": pinned.Snapshot.Revision})

	// J3 - a real model answer through the Model Gateway (Qwen only).
	var generation struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Output string `json:"output"`
		Model  string `json:"model"`
	}
	// The Qwen deployment is a reasoning model: a small token budget can be consumed
	// entirely by thinking (finish=length with no text). The leg allows one honest
	// restatement with a fresh idempotency key; the durable row remains the proof.
	genAttempts := 0
	for genAttempts < 2 && generation.Status != "COMPLETED" {
		genAttempts++
		err := stand.call(stand.HTTP, "/v1/model/SubmitModel", map[string]any{
			"key": fmt.Sprintf("%s-gen-%d", task, genAttempts), "model": mustEnv(t, "AHP_LLM_MODEL"),
			"max_tokens": 1024,
			"messages": []any{map[string]any{"role": "user",
				"content": "Say hello to the employee in five words."}}}, token, &generation)
		if err != nil && generation.Status == "" {
			checks["j3_model_real_qwen"] = map[string]any{"status": "FAIL", "http": status(err),
				"error": errText(err)}
			fail("j3")
		}
	}
	if generation.Status != "COMPLETED" {
		checks["j3_model_real_qwen"] = map[string]any{"status": "FAIL", "attempts": genAttempts,
			"status_value": generation.Status}
		fail("j3")
	}
	var genRows int
	if err := stand.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_model.generations WHERE tenant=$1 AND task=$2 AND status='COMPLETED'`,
		stand.Scope["tenant"], task).Scan(&genRows); err != nil {
		t.Fatalf("generation probe: %v", err)
	}
	// Honest gate (DEF-VERDICT-1): a COMPLETED in-memory generation without a
	// durable COMPLETED row is a FAIL, not a PASS.
	if genRows == 0 {
		checks["j3_model_real_qwen"] = map[string]any{"status": "FAIL", "generation": generation.ID,
			"model": generation.Model, "durable_rows": genRows, "attempts": genAttempts}
		fail("j3-pg")
	}
	setLeg(checks, "j3_model_real_qwen", map[string]any{"status": "PASS", "generation": generation.ID,
		"model": generation.Model, "durable_rows": genRows, "attempts": genAttempts})

	// J4 - HITL approve, then the tool RESULT reaches the owner via GetResult.
	note := "journey note " + stamp
	callKey := task + "-note"
	errHold := stand.call(stand.HTTP, "/v1/tools/Call", map[string]any{"key": callKey,
		"tool": "record_note", "arguments": map[string]any{"note": note}}, token, nil)
	if status(errHold) != 409 {
		checks["j4_tool_hitl_result"] = map[string]any{"status": "FAIL", "held_http": status(errHold)}
		fail("j4-hold")
	}
	var worklist struct {
		Approvals []map[string]any `json:"approvals"`
	}
	var pending map[string]any
	for attempt := 0; attempt < 10 && pending == nil; attempt++ {
		if err := stand.call(stand.HTTP, "/u-ahp/v1/tools/Approvals", map[string]any{"limit": 50}, human, &worklist); err != nil {
			checks["j4_tool_hitl_result"] = map[string]any{"status": "FAIL", "worklist": errText(err)}
			fail("j4-worklist")
		}
		for _, item := range worklist.Approvals {
			opKey, _ := item["operation_key"].(string)
			if item["tool"] == "record_note" && strings.Contains(opKey, callKey) {
				pending = item
			}
		}
		if pending == nil {
			time.Sleep(400 * time.Millisecond)
		}
	}
	if pending == nil {
		checks["j4_tool_hitl_result"] = map[string]any{"status": "FAIL", "reason": "no pending approval"}
		fail("j4-pending")
	}
	if err := stand.call(stand.HTTP, "/u-ahp/v1/tools/Approve", map[string]any{
		"request_id": field(pending, "request_id", "id"), "digest": field(pending, "digest"),
		"tool": "record_note"}, human, nil); err != nil {
		checks["j4_tool_hitl_result"] = map[string]any{"status": "FAIL", "approve": errText(err)}
		fail("j4-approve")
	}
	var operation struct {
		Status string `json:"status"`
	}
	if err := stand.call(stand.HTTP, "/v1/tools/Call", map[string]any{"key": callKey,
		"tool": "record_note", "arguments": map[string]any{"note": note}}, token, &operation); err != nil ||
		operation.Status != "SUCCEEDED" {
		checks["j4_tool_hitl_result"] = map[string]any{"status": "FAIL", "operation": operation.Status,
			"error": errText(err)}
		fail("j4-execute")
	}
	var toolResult map[string]any
	getResultStatus, errResult := stand.callWithStatus(stand.HTTP, "/v1/tools/GetResult",
		map[string]any{"key": callKey}, token, &toolResult)
	resultPayload, _ := json.Marshal(toolResult)
	carriesNote := strings.Contains(string(resultPayload), stamp)
	checks["j4_tool_hitl_result"] = map[string]any{
		"get_result_http": getResultStatus, "result_carries_note": carriesNote,
		"get_result_error": truncate(errText(errResult), 140)}
	// Honest gate (DEF-VERDICT-1): the owner must READ the tool result back and it
	// must carry the note this leg wrote. An unreadable or contentless GetResult
	// is a FAIL, never a PASS with a merely recorded "carries" field.
	// The gate is on the PUBLIC read-back itself: HTTP 200 (the durable stream is
	// not a substitute), no error, and the note content present.
	if getResultStatus != http.StatusOK || errResult != nil || !carriesNote {
		checks["j4_tool_hitl_result"].(map[string]any)["status"] = "FAIL"
		fail("j4-result")
	}
	checks["j4_tool_hitl_result"].(map[string]any)["status"] = "PASS"

	// J5 - the personal memory of THIS Dot records and re-tells a fact.
	// Hindsight's semantic extraction is probabilistic (an LLM decides which
	// sentences become facts), so the leg polls the durable operation and
	// allows a small number of honest restatements. Attempts and states are
	// recorded; a persistent zero is still a hard failure, never a fake pass.
	codeWord := "journey-otter-" + stamp
	input := "The employee's personal Dot keeps research notes. " +
		"The employee stores the journey code word " + codeWord + " for this conversation."
	runID := laneRun(t, stand, token, task+"-mem", ctxID, input)
	attempts := 0
	operationStates := []string{}
	foundFact := ""
	for attempts < 3 && foundFact == "" {
		attempts++
		var remembered struct {
			ID string `json:"id"`
		}
		if err := stand.call(stand.HTTP, "/v1/memory/Remember", map[string]any{
			"target": "personal", "text": input,
			"op_key":      fmt.Sprintf("%s-remember-%d", task, attempts),
			"source_refs": []string{"state://runs/" + runID + "/input"}}, token, &remembered); err != nil {
			checks["j5_memory"] = map[string]any{"status": "FAIL", "error": errText(err)}
			fail("j5-remember")
			continue
		}
		opDeadline := time.Now().Add(90 * time.Second)
		state := ""
		for time.Now().Before(opDeadline) {
			var operation struct {
				State string `json:"state"`
			}
			if err := stand.call(stand.HTTP, "/v1/memory/Operation",
				map[string]any{"id": remembered.ID}, token, &operation); err == nil {
				state = operation.State
			}
			if state == "succeeded" || state == "failed" {
				break
			}
			time.Sleep(2 * time.Second)
		}
		operationStates = append(operationStates, state)
		if state != "succeeded" {
			continue
		}
		searchDeadline := time.Now().Add(45 * time.Second)
		for time.Now().Before(searchDeadline) && foundFact == "" {
			var personal struct {
				Facts []memoryFact `json:"facts"`
			}
			if err := stand.call(stand.HTTP, "/v1/memory/Search",
				map[string]any{"query": "What is the employee's journey code word?", "scope": "personal"},
				token, &personal); err == nil {
				for _, fact := range personal.Facts {
					if strings.Contains(fact.Text, codeWord) {
						foundFact = fact.Text
					}
				}
			}
			if foundFact == "" {
				time.Sleep(3 * time.Second)
			}
		}
	}
	// Honest gate (DEF-VERDICT-1): the search must return THIS leg's code word, not
	// any fact the extraction happened to keep. A first-arbitrary fact is not proof
	// the personal memory round-tripped the stored value.
	codeWordFound := strings.Contains(foundFact, codeWord)
	if !codeWordFound {
		checks["j5_memory"] = map[string]any{"status": "FAIL", "attempts": attempts,
			"operation_states": operationStates, "code_word_found": false,
			"note": "extraction never surfaced the code word; an arbitrary first fact is not accepted. " +
				"If reached on an otherwise healthy stand this is the PRODUCT extraction config " +
				"(concise retain mode / store_document_text=false: internal/memory/hindsight/client.go, " +
				"retain_mission in internal/memory/longterm/missions.go) - see " +
				"reports/mac-director/dot-j5-root-cause-20261109.md; recorded as a defect pointer, never as a softener"}
		fail("j5-code-word")
	}
	setLeg(checks, "j5_memory", map[string]any{"status": "PASS", "attempts": attempts,
		"operation_states": operationStates, "code_word_found": codeWordFound,
		// Deterministic criterion, derived from data: the code word is REQUIRED
		// (absence above is a hard FAIL). The old blanket
		// "extraction_nondeterministic": true was a hedge - replaced by whether a
		// retry was actually needed for THIS run.
		"extraction_required_retry": attempts > 1})

	// J6 - a file: upload, seal, download byte-for-byte with SHA.
	blob := []byte("journey artifact bytes " + stamp)
	artifactID, artifactVersion, sealedSHA := stand.upload(t, stand.HTTP, token, task+"-file",
		"journey.bin", blob, map[string]any{})
	code, got, err := stand.postRaw(stand.HTTP, "/v1/artifacts/Download", map[string]any{"id": artifactID}, token, nil)
	checks["j6_file_sha"] = map[string]any{"status": "PASS", "artifact": artifactID, "http": code,
		"sha256": digestHex(got), "sealed": sealedSHA, "expected": digestHex(blob)}
	if err != nil || code != http.StatusOK || digestHex(got) != digestHex(blob) {
		checks["j6_file_sha"].(map[string]any)["status"] = "FAIL"
		fail("j6")
	}

	// J7 - history reloads on a brand-new session (new TLS connection, new JWT).
	fresh := &http.Client{Transport: stand.HTTP.Transport.(*http.Transport).Clone(), Timeout: 60 * time.Second, Jar: newCookieJar()}
	freshHuman := stand.humanToken(t, fresh, mustLogin(t, "email"), mustLogin(t, "password"))
	freshToken, _ := stand.taskGrant(t, fresh, freshHuman, "dot-journey-reload-"+stamp,
		[]string{"state"})
	var reloadedRun struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	errReload := stand.call(fresh, "/v1/state/GetRun", map[string]any{"run_id": runID}, freshToken, &reloadedRun)
	checks["j7_history_reload"] = map[string]any{"status": "PASS", "same_run": reloadedRun.ID == runID,
		"error": truncate(errText(errReload), 120)}
	if errReload != nil || reloadedRun.ID != runID {
		checks["j7_history_reload"].(map[string]any)["status"] = "FAIL"
		fail("j7")
	}

	// J8 - HITL cancel: an unsigned second effect must never execute.
	cancelKey := task + "-never"
	if err := stand.call(stand.HTTP, "/v1/tools/Call", map[string]any{"key": cancelKey,
		"tool": "record_note", "arguments": map[string]any{"note": "never written " + stamp}}, token, nil); status(err) != 409 {
		checks["j8_hitl_cancel"] = map[string]any{"status": "FAIL", "http": status(err)}
		fail("j8")
	} else {
		store, _ := os.ReadFile(filepath.Join(stand.Dir, "notes", "notes.jsonl"))
		heldAgain := status(stand.call(stand.HTTP, "/v1/tools/Call", map[string]any{"key": cancelKey,
			"tool": "record_note", "arguments": map[string]any{"note": "never written " + stamp}}, token, nil)) == 409
		noEffect := !strings.Contains(string(store), "never written")
		checks["j8_hitl_cancel"] = map[string]any{"held_again": heldAgain, "no_effect_in_store": noEffect}
		// Honest gate (DEF-VERDICT-1): cancel must BOTH keep the call held (a second
		// unsigned call is still refused with 409) and leave no effect in the store.
		if !heldAgain || !noEffect {
			checks["j8_hitl_cancel"].(map[string]any)["status"] = "FAIL"
			fail("j8-cancel")
		} else {
			checks["j8_hitl_cancel"].(map[string]any)["status"] = "PASS"
		}
	}

	// J9 - a second user is denied EVERYTHING of the first, not three endpoints.
	// The requirement is about personal data, so the stranger gets a credential as
	// wide as its OWN permissions allow and every personal-resource kind this lane
	// can reach is attempted and classified by its CONTRACT CODE (see
	// isolation_resources_test.go): artifact bytes; memory personal search and one
	// owner item; the run record and the thread record; the run list of the owner's
	// thread; the thread's durable stream; the chat list; the tool operation history
	// and the note behind the tool result; the HITL worklist and an approval attempt;
	// the Dot's AgentConfig and personal profile; and the owner's grant tree
	// (delegate-into and cancel). A 200-empty answer is booked as its own fact only
	// where the contract deliberately answers that way, naming the decision; a
	// refusal caused by the stranger's OWN missing entitlement is recorded but never
	// counted as isolation. Owner data reaching the stranger is a product BLOCKER.
	foreignHuman := stand.humanToken(t, stand.Foreign, mustLogin(t, "foreign_email"), mustLogin(t, "foreign_password"))
	foreignToken, foreignTask, errForeignGrant := stand.isoCredentials(stand.Foreign, foreignHuman,
		"dot-journey-foreign-"+stamp,
		[]string{"state", "model", "tools", "memory", "artifacts", "registry", "space:access",
			"identity:delegate", "identity:cancel"})
	isolationLeg := map[string]any{"status": "FAIL"}
	if errForeignGrant != nil {
		// No stranger credential means nothing about isolation was observed. That is
		// stated, never credited as a denial.
		isolationLeg["failure_reason"] = "no stranger credential: " + truncate(errText(errForeignGrant), 140)
		fail("j9-credential")
	} else {
		foreignTenant := fmt.Sprint(stand.Scope["foreign_tenant"])
		foreignCapabilities, errForeignCapabilities := stand.grantCapabilities(foreignTenant, foreignTask)
		if errForeignCapabilities != nil {
			// Without the stranger's real capability set no capability-gated probe is
			// admissible, so none is attempted and every kind is declared not_attempted.
			foreignCapabilities = nil
		}
		isolationLeg = stand.runForeignIsolation(t, token, human, foreignToken, foreignTask, foreignHuman,
			foreignTenant, mustLogin(t, "foreign_subject"), foreignCapabilities, ownerPersonalData{
				ownerTask: task, contextID: ctxID, runID: runID, artifactID: artifactID,
				artifactMark: "journey artifact bytes " + stamp, toolKey: callKey,
				noteMark: note, pendingKey: cancelKey, memoryMark: codeWord,
				agentID: dotAgentID, agentRoute: "lane-qwen", stamp: stamp})
		leaked := legStrings(isolationLeg["leaked_resources"])
		unproven := legStrings(isolationLeg["unproven_resources"])
		grantRows, _ := isolationLeg["foreign_subject_grant_rows"].(int)
		artifactDenied, _ := isolationLeg["artifact_denied"].(bool)
		runDenied, _ := isolationLeg["run_denied"].(bool)
		memorySafe, _ := isolationLeg["memory_empty_or_denied"].(bool)
		isolationLeg["status"] = "PASS"
		if len(leaked) > 0 {
			isolationLeg["status"] = "FAIL"
			fail("j9-leak")
		}
		// -1 means the durable grant probe itself failed: missing evidence is a
		// failure of the claim, never a silent zero.
		if grantRows != 0 {
			isolationLeg["status"] = "FAIL"
			fail("j9-grant-breach")
		}
		if !artifactDenied || !runDenied || !memorySafe {
			isolationLeg["status"] = "FAIL"
			fail("j9-core")
		}
		// The owner-control trap: the leg asks one route the platform does not mount at
		// all, and only the owner control keeps it away from the stranger. Any other
		// answer here means that gate was removed and a 404 from a dead path is being
		// credited as an isolation denial - the exact laundering this leg exists to refuse.
		if sentinel, _ := isolationLeg["owner_control_sentinel"].(string); sentinel != "not_attempted" {
			isolationLeg["status"] = "FAIL"
			fail("j9-control-gate")
		}
		// And the trap's own disappearance: its recorded default fails, so deleting the
		// probe - the reviewer's mutation M4, which previously gave rc=0 - reddens this
		// leg as well as the pure table test. A guard that passes when removed is not a
		// guard, and a missing field is never read as a pass.
		if ran, _ := isolationLeg["owner_control_sentinel_ran"].(bool); !ran {
			isolationLeg["status"] = "FAIL"
			fail("j9-sentinel-gone")
		}
		if len(unproven) > 0 {
			// Attempted, and the answer is not a refusal the contract states. Recorded
			// here and gated in coverage_test.go: the coverage claim degrades to
			// not_covered rather than being credited from an unexpected answer.
			isolationLeg["breadth_note"] = fmt.Sprintf(
				"%d of %d attempted kinds answered without a contract refusal code: %s",
				len(unproven), isolationLeg["resources_attempted"], strings.Join(unproven, ", "))
		}
	}
	setLeg(checks, "j9_two_user_isolation", isolationLeg)

	// J10 - the browser Dot turn (DEF-DOTE2E-1 branch): accepted, pinned, but its
	// completion is owned by Space/Banach. Recorded, not failed.
	var accepted struct {
		TurnID   string `json:"turn_id"`
		ThreadID string `json:"thread_id"`
	}
	inputParts := map[string]any{"parts": []any{map[string]any{"kind": "text",
		"text": "Reply with exactly: JOURNEY-TURN-OK " + stamp}}}
	var turnCheck = map[string]any{"status": "PENDING"}
	if err := stand.call(stand.HTTP, "/v1/space/AcceptTurn", map[string]any{
		"key": task + "-turn", "context_id": ctxID + "-turn", "input": inputParts, "max_steps": 4},
		human, &accepted); err != nil {
		turnCheck["accept"] = map[string]any{"status": "FAIL", "error": errText(err)}
		checks["j10_browser_turn_def1_branch"] = turnCheck
		fail("j10-accept")
	}
	turnCheck["turn"] = accepted.TurnID
	terminal := ""
	reason := ""
	// If AcceptTurn was refused there is no turn to poll: soft-fail recorded the leg
	// already, and the terminal branch below files the honest FAIL without burning
	// the 120s poll window on an empty turn id.
	acceptRefused := accepted.TurnID == ""
	for deadline := time.Now().Add(120 * time.Second); !acceptRefused && time.Now().Before(deadline); {
		var turn struct {
			Status        string `json:"status"`
			FailureReason string `json:"failure_reason"`
		}
		if err := stand.call(stand.HTTP, "/v1/space/GetTurn", map[string]any{"turn_id": accepted.TurnID}, human, &turn); err == nil {
			terminal = turn.Status
			reason = turn.FailureReason
			if terminal == "done" || terminal == "completed" || terminal == "failed" || terminal == "canceled" {
				break
			}
		}
		time.Sleep(2 * time.Second)
	}
	turnCheck["terminal_status"] = terminal
	turnCheck["failure_reason"] = truncate(reason, 240)
	// Self-proving J10 (director mandate 2026-11-09 20:5xZ): whenever the browser turn
	// did NOT reach a completed terminal, the durable + log fingerprint of THIS night's
	// turn goes into the artifact itself. Attribution then survives the instance being
	// stopped or reused - no post-hoc psql on a live stand required. Only identifiers,
	// statuses, counts and timings are recorded, never credentials or payloads.
	if !acceptRefused && terminal != "done" && terminal != "completed" {
		turnCheck["durable_forensics"] = j10DurableForensics(stand, accepted.TurnID, accepted.ThreadID)
	}
	if terminal == "completed" || terminal == "done" {
		// Honest gate (DEF-VERDICT-1): reaching a terminal state is NOT proof - the
		// durable stream must carry the model answer with the marker this leg asked
		// for ("JOURNEY-TURN-OK <stamp>"). Completion without it is a FAIL, never PASS.
		marker := "JOURNEY-TURN-OK " + stamp
		var stream struct {
			Events []struct {
				Kind    string          `json:"kind"`
				Payload json.RawMessage `json:"payload"`
			} `json:"events"`
		}
		answerFound := false
		streamErr := stand.call(stand.HTTP, "/v1/space/ReadStream",
			map[string]any{"thread_id": accepted.ThreadID, "after": 0}, human, &stream)
		for _, event := range stream.Events {
			if event.Kind == "model" && strings.Contains(string(event.Payload), marker) {
				answerFound = true
			}
		}
		turnCheck["answer_marker"] = marker
		turnCheck["answer_found"] = answerFound
		if streamErr != nil {
			turnCheck["read_stream_error"] = truncate(errText(streamErr), 120)
		}
		if !answerFound {
			turnCheck["status"] = "FAIL"
			turnCheck["note"] = "turn reached " + terminal + " but the durable stream never carried the expected answer marker"
			checks["j10_browser_turn_def1_branch"] = turnCheck
			fail("j10-answer")
		}
		turnCheck["status"] = "PASS"
		turnCheck["note"] = "browser turn completed AND the durable stream carries the expected JOURNEY-TURN-OK marker"
	} else if terminal == "failed" && strings.Contains(reason, "Core request differs") {
		turnCheck["defect"] = "DEF-DOTE2E-1"
		turnCheck["root_cause_area"] = "Space / as-turn-runner (Banach): coordinator mints the run grant with Context=thread.ID while scoped_core requires threads.context_id"
		turnCheck["status"] = "BLOCKED"
		turnCheck["note"] = "expected RED while DEF-DOTE2E-1 stands; recorded, not faked"
	} else if terminal == "failed" && strings.Contains(reason, "System message must be at the beginning") {
		turnCheck["defect"] = "DEF-DOTE2E-11"
		turnCheck["root_cause_area"] = "Runner core (internal/core/runtime.go:329, owner Space/Banach): memory injection does messages = append(injected, messages...) PREPENDS the user-role memory block ahead of the leading system Dot-configuration message that profileRuntime places at index 0 (cmd/as-runner/profile_runtime.go:91), so the provider receives a non-leading system message and refuses with 400 \"System message must be at the beginning\". DEF-DOTE2E-9 fixed, this is the next layer. Repro: generation on turn 20261109T170628.230929000Z_e2svvmpxfdfclki2d2oiobl3ka, ahp_model.generations.request shows messages order [user(memory), system(config), user(input)]. Fix owner-side: inject after the leading system block or re-partition system to the front before the wire call."
		turnCheck["note"] = "browser-turn completion blocked by DEF-DOTE2E-11; recorded, not faked"
		turnCheck["status"] = "BLOCKED"
	} else if terminal == "failed" && strings.Contains(reason, "model stream refused: HTTP 400") {
		turnCheck["defect"] = "DEF-DOTE2E-9"
		turnCheck["root_cause_area"] = "Runner core (core.NewRuntime default MaxTokens 4096, internal/core/runtime.go:185) exceeds the model bridge cap (execbridge defaultMaxTokens 1024, internal/execbridge/bridge.go:43); no env wires the two, so StreamModel answers 400 INVALID before the provider. Repro: /v1/model/StreamModel max_tokens=4096 -> 400, 1024 -> 200 (see model-fault-isolation evidence)"
		turnCheck["status"] = "BLOCKED"
		turnCheck["note"] = "browser-turn completion blocked by DEF-DOTE2E-9; recorded, not faked"
	} else if terminal == "failed" && preflightBranchName(reason) != "" {
		// Named preflight refusal (possible since Jason's durable failure_cause fix on
		// e83a00df: the coordinator's own sentence now survives to the cold reader).
		// The leg records the EXACT coordinator branch that refused - never a generic
		// stub - with the verbatim durable cause, and stays BLOCKED (red gate).
		turnCheck["defect"] = "DEF-DOTE2E-15"
		turnCheck["root_cause_area"] = "Space coordinator preflight branch " + preflightBranchName(reason) + " refused this turn before Core (owner Space/Jason); verbatim durable failure_cause: " + truncate(reason, 220)
		turnCheck["preflight_branch"] = preflightBranchName(reason)
		turnCheck["status"] = "BLOCKED"
		turnCheck["note"] = "preflight refusal recorded with its named branch and durable cause; not faked, not guessed past the sentence"
	} else if terminal == "failed" && strings.Contains(reason, "carries no cause frame") {
		// DEF-DOTE2E-14 (found on accepted rev 062d0f62, 2026-11-09 19:53Z/19:58Z,
		// reproduced 2/2): the run row goes created->failed within ~150-265ms of
		// AcceptTurn returning, with ZERO stream frames, NO payload.close_reason, and NO
		// trace of a claim or executeTurn span in the runner log - so none of the known
		// coordinator closures (failPreflight stamps close_reason=error; every post-Core
		// terminal appends a status frame) wrote it. GetTurn's cold projection can only
		// say the transcript has no cause frame. Durable fingerprint recorded below; the
		// root-cause forensics belong to the Space/State owner, not to this harness.
		turnCheck["defect"] = "DEF-DOTE2E-14"
		turnCheck["root_cause_area"] = "Space/State plane (owner Space/Jason, per director 2026-11-09: the preflight cause never reaches the cold reader served on the front origin, which falls back to the \"no cause frame\" stub): run closed failed with zero stream frames, no close_reason and no claim trace in the runner; full durable fingerprint in durable_forensics. Repro on rev 062d0f62: turns 20261109T195312.510825000Z_mn62ltykjgwi25jdgn7me7vdmm and 20261109T195810.195281000Z_k2krfsagylenmbwlhhxi4gfjkm"
		turnCheck["status"] = "BLOCKED"
		turnCheck["note"] = "browser turn terminalized failed with no cause anywhere; recorded as DEF-DOTE2E-14, not faked, not attributed past the fingerprint"
		// Since Jason's durable failure_cause fix (66dbf968), a "no cause frame" stub
		// beside a NON-empty durable failure_cause is a READER defect, not the norm
		// (director 2026-11-09): relabel with the named reader defect.
		if f, ok := turnCheck["durable_forensics"].(map[string]any); ok {
			if fc, _ := f["failure_cause"].(string); fc != "" {
				turnCheck["defect"] = "DEF-DOTE2E-16"
				turnCheck["root_cause_area"] = "Cold reader precedence (internal/space/turn_history.go, owner Space/Jason): the run row carries a named failure_cause (" + truncate(fc, 200) + ") but GetTurn still projected the generic \"no cause frame\" stub - the projection dropped a cause that exists durably"
				turnCheck["note"] = "stub beside a non-empty durable failure_cause: reader defect recorded, generic stub is no longer a valid norm"
			}
		}
	} else if terminal == "pending" || terminal == "" {
		// The durable run row decides: if State closed the run while GetTurn still
		// answers pending, the projection never converged - that is a defect of its
		// own, recorded with its durable evidence, not a fake terminal.
		var runStatus string
		_ = stand.Pool.QueryRow(context.Background(),
			`SELECT status FROM ahp_state.runs WHERE id=$1`, accepted.TurnID).Scan(&runStatus)
		turnCheck["durable_run_status"] = runStatus
		if runStatus == "failed" {
			turnCheck["defect"] = "DEF-DOTE2E-10"
			turnCheck["root_cause_area"] = "Space / as-turn-runner (Banach): the turn's run failed in ahp_state (create+175ms) without a claim, without a stream frame and without a log line, while GetTurn answers pending forever. failPreflight (internal/space/coordinator.go:713) performs no log and no stream append, and the first-claim path leaves the run to the orphan sweep; when the sweep misses, the turn projection never converges. Repro: run 20261109T164510.416228000Z_2uvprdldhgiatzsg3nnuje6hne (status=failed, zero ahp_state.stream_events, zero runner log lines within the window)"
			turnCheck["status"] = "BLOCKED"
			turnCheck["note"] = "recorded as DEF-DOTE2E-10; the leg does not fabricate a terminal"
		} else {
			turnCheck["status"] = "FAIL"
			checks["j10_browser_turn_def1_branch"] = turnCheck
			fail("j10-terminal")
		}
	} else {
		turnCheck["status"] = "FAIL"
		checks["j10_browser_turn_def1_branch"] = turnCheck
		fail("j10-terminal")
	}
	checks["j10_browser_turn_def1_branch"] = turnCheck
	// Cause guarantee (director 2026-11-10): a non-completed terminal must
	// carry a named cause in the artifact. If every attribution source is
	// empty, the gap itself is stated - never silently omitted.
	if gap := causeAttribution(turnCheck); gap != "" {
		turnCheck["cause_attribution"] = gap
	}

	// J11 - the human may read their own artifact through the browser face.
	humanDownloadCode, humanDownloadBody, humanDownloadErr := stand.rawGet(stand.HTTP,
		"/u-ahp/v1/artifacts/download?artifact_id="+artifactID+"&version="+artifactVersion, human, nil)
	// The body used to be discarded, so this leg proved "the browser face answered",
	// not "the human received the sealed bytes". The digest is now RECORDED next to
	// the sealed SHA (the gate stays the HTTP answer until one run confirms the
	// browser face serves byte-identical content - recorded, never hidden).
	humanBytesMatchSealed := digestHex(humanDownloadBody) == sealedSHA
	checks["j11_human_artifact_read"] = map[string]any{"status": "PASS", "http": humanDownloadCode,
		"received_sha256": digestHex(humanDownloadBody), "sealed_sha256": sealedSHA,
		"bytes_match_sealed": humanBytesMatchSealed,
		"bytes_gate_note":    "recorded, not yet gated: the leg's gate is the HTTP answer; content equality is declared in coverage",
		"error":              truncate(errText(humanDownloadErr), 120)}
	if humanDownloadCode != http.StatusOK {
		checks["j11_human_artifact_read"].(map[string]any)["status"] = "FAIL"
		fail("j11")
	}

	// Honest verdict (DEF-VERDICT-1): the report generator itself refuses PASS.
	// journeyVerdict downgrades any leg whose terminal_status is not a completed
	// terminal (so terminal_status=failed can never read as PASS) and fails an
	// empty run. TestVerdictSemanticsCannotBePassWithIncompleteLeg proves it against
	// the ...165158Z anti-fixture that once recorded PASS on a failed J10.
	// Consistency guard (DEF-VERDICT-1): a leg whose sub-check recorded a failure
	// must never carry PASS in the artifact, whatever later rewrites did to the
	// map. With the soft-fail mode above, every recorded fail() tag (j1..j11) is
	// forced back onto its leg entry as FAIL before the verdict is generated.
	for _, leg := range legList(checks["failed_legs"]) {
		tag := leg
		if i := strings.IndexByte(tag, '-'); i > 0 {
			tag = tag[:i]
		}
		for key, entry := range checks {
			if key == tag || strings.HasPrefix(key, tag+"_") {
				if m, ok := entry.(map[string]any); ok {
					if s, _ := m["status"].(string); s == "PASS" || s == "PENDING" || s == "" {
						m["status"] = "FAIL"
						m["status_note"] = "forced FAIL by consistency guard: leg recorded a failed sub-check " + leg
					}
				}
			}
		}
	}
	verdict, _, blocked, failed := journeyVerdict(checks)
	// AHP_DOT_E2E_RECORD_DEFECTS=1 is the deliberate record-the-defect mode: the
	// artifact then carries the defect-recorded marker in its name and the test
	// stays green on BLOCKED only. Without it, BLOCKED is a red test.
	scenario := "personal-dot-journey"
	recordDefects := os.Getenv("AHP_DOT_E2E_RECORD_DEFECTS") == "1"
	if verdict == "BLOCKED" && recordDefects {
		scenario = "personal-dot-journey-defect-recorded"
		// LOUD marker (DEF-VERDICT-1): this artifact is green ONLY because the
		// record-defect mode was requested. The user path is still BLOCKED - a
		// defect-recorded artifact must never be read as a passing journey.
		checks["RECORD_MODE_MASKS_BLOCKED_TO_GREEN"] = "AHP_DOT_E2E_RECORD_DEFECTS=1 turned a BLOCKED user path into a green go test; verdict is still BLOCKED; blocked legs are NOT complete: " + fmt.Sprint(blocked)
	}
	// Self-declared coverage travels WITH the verdict: a reader of this artifact
	// alone must see which PDF requirements the journey proves, how, and which it
	// does not touch at all (A2A, compute isolation, bank rows, parser, load).
	stand.Scope["coverage"] = journeyCoverage(checks, stand.Scope, stand.Dir)
	stand.record(t, scenario, verdict, checks)
	if verdict == "FAIL" {
		t.Fatalf("journey legs failed: %v", failed)
	}
	if verdict == "BLOCKED" && !recordDefects {
		t.Fatalf("journey blocked on known defects %v; rerun with AHP_DOT_E2E_RECORD_DEFECTS=1 to record the defect and keep green", blocked)
	}
}

func legList(existing any) []string {
	if list, ok := existing.([]string); ok {
		return list
	}
	return []string{}
}

// preflightBranchName names the exact coordinator failPreflight branch whose
// sentence the durable failure_cause carries (branch lines quoted as of the
// rev this lane is pinned to; the artifact always records its revision, so a
// stale line number reads as historical, never as a live claim).
func preflightBranchName(reason string) string {
	for _, b := range []struct{ sub, name string }{
		{"run is not rooted on its own child grant", "run-not-rooted-on-child-grant (internal/space/coordinator.go:839)"},
		{"durable checkpoint read failed", "durable-checkpoint-read-failed (internal/space/coordinator.go:859)"},
		{"durable step-key high-water read failed", "step-key-high-water-read-failed (internal/space/coordinator.go:889)"},
		{"durable conversation history read failed", "conversation-history-read-failed (internal/space/coordinator.go:898)"},
	} {
		if strings.Contains(reason, b.sub) {
			return b.name
		}
	}
	return ""
}

// j10DurableForensics captures the durable-state and process-log fingerprint of a
// browser turn that did NOT reach a completed terminal, and returns it for embedding
// in the scenario artifact. Identifiers, statuses, counts and millisecond timings
// only - no credentials, DSNs, tokens or payload bodies.
func j10DurableForensics(stand *instance, turnID, threadID string) map[string]any {
	out := map[string]any{"turn_id": turnID, "thread_id": threadID}
	var (
		status      string
		createdMS   int64
		updatedMS   int64
		hasCR       bool
		closeReason string
		waitFor     string
		deadlineS   string
	)
	err := stand.Pool.QueryRow(context.Background(),
		`SELECT status,
		        floor(extract(epoch from created_at)*1000)::bigint,
		        floor(extract(epoch from updated_at)*1000)::bigint,
		        payload ? 'close_reason', coalesce(payload->>'close_reason',''),
		        coalesce(wait_for,''), coalesce(deadline::text,'')
		   FROM ahp_state.runs WHERE id=$1`, turnID).
		Scan(&status, &createdMS, &updatedMS, &hasCR, &closeReason, &waitFor, &deadlineS)
	if err != nil {
		out["runs_query_error"] = errText(err)
	} else {
		out["runs"] = map[string]any{"status": status, "has_close_reason": hasCR,
			"close_reason": closeReason, "wait_for": waitFor, "deadline": deadlineS,
			"created_ms": createdMS, "terminal_ms": updatedMS, "fail_after_ms": updatedMS - createdMS}
	}
	// failure_cause (Jason's durable refusal sentence, state.RunPayloadFailureCause)
	// is read as its own key: close_reason carries the execution-plane vocabulary,
	// failure_cause quotes the refusing plane verbatim - the dump must show both.
	var (
		hasFC bool
		failC string
	)
	fcErr := stand.Pool.QueryRow(context.Background(),
		`SELECT payload ? 'failure_cause', coalesce(payload->>'failure_cause','')
		   FROM ahp_state.runs WHERE id=$1`, turnID).
		Scan(&hasFC, &failC)
	if fcErr != nil {
		out["failure_cause_query_error"] = errText(fcErr)
	} else {
		out["has_failure_cause"] = hasFC
		out["failure_cause"] = truncate(failC, 300)
	}
	rows, qerr := stand.Pool.Query(context.Background(),
		`SELECT kind, count(*) FROM ahp_state.stream_events WHERE thread_id=$1 GROUP BY kind ORDER BY kind`, threadID)
	if qerr != nil {
		out["stream_query_error"] = errText(qerr)
	} else {
		byKind := map[string]int{}
		total := 0
		for rows.Next() {
			var kind string
			var n int
			if rows.Scan(&kind, &n) == nil {
				byKind[kind] = n
				total += n
			}
		}
		rows.Close()
		out["stream_frames_total"] = total
		out["stream_frames_by_kind"] = byKind
	}
	logHits := map[string]int{}
	for _, name := range []string{"runner.log", "agent.log", "backend.log"} {
		data, rerr := os.ReadFile(filepath.Join(stand.Dir, "logs", name))
		if rerr != nil {
			logHits[name] = -1 // log unavailable at record time
			continue
		}
		logHits[name] = strings.Count(string(data), turnID)
	}
	out["turn_id_log_hits"] = logHits
	return out
}
