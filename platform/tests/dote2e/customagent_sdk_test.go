package dote2e_test

// PDF rev3.2 requires a CUSTOMER-WRITTEN agent to run against AHP without the
// Agent Space: the standalone path is "task token -> State -> Model Gateway ->
// Artifacts", driven from an ordinary program with the platform SDKs. This
// scenario is that program. It calls no /v1/space/ route at all, uses the real
// Qwen route through the Model Gateway, and stores its answer as an artifact.
//
// The same scenario in Python lives in customagent/python/custom_agent.py and is
// executed by TestCustomAgentRunsStandaloneViaPythonSDK below.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	artifactsdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
	modelsdk "github.com/ahp-platform/ahp/sdk/go/model"
	statesdk "github.com/ahp-platform/ahp/sdk/go/state"
)

// sdkTransport builds the mTLS transport the SDKs demand: the lane's own
// workload certificate against the lane CA.
func sdkTransport(t *testing.T, stand *instance) *http.Transport {
	t.Helper()
	transport, ok := stand.HTTP.Transport.(*http.Transport)
	if !ok || transport == nil {
		t.Fatal("the lane client does not expose an *http.Transport for the SDKs")
	}
	return transport.Clone()
}

// customAgentLegMaxTokens is the token ceiling BOTH standalone legs ask the
// Model Gateway for. 64 was the ceiling that produced the mass of empty
// completions in the LLM profile (finish_reason=length, provider_empty_completion);
// 1024 is the constant the accepted benchmark already uses (Goodall raised the
// profiling default to 1024 in 330ba4f). This is a TEST-LEG configuration: the
// product and the SDK defaults are untouched. A leg running at 64 can fail on
// its own configuration while being reported as a product defect.
const customAgentLegMaxTokens = 1024

func TestCustomAgentRunsStandaloneOnGoSDKs(t *testing.T) {
	stand := loadStand(t)
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	token, task := stand.taskGrantContext(t, stand.HTTP, human, "dot-sdk-go-"+stamp,
		"dot-sdk-go-ctx-"+stamp, []string{"state", "model", "artifacts", "execution"})
	staticToken := func(context.Context) (string, error) { return token, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	checks := map[string]any{}
	origin := stand.Origin

	// (a) The custom agent opens its own durable work in State - its thread and
	// run - with nothing but a task token.
	stateClient, err := statesdk.New(statesdk.Config{BaseURL: origin,
		HTTPClient:  &http.Client{Transport: sdkTransport(t, stand), Timeout: 120 * time.Second},
		TokenSource: staticToken})
	if err != nil {
		stand.record(t, "custom-agent-standalone-go", "FAIL", map[string]any{"state_client": map[string]any{
			"status": "FAIL", "error": err.Error()}})
		t.Fatalf("state SDK client: %v", err)
	}
	thread, err := stateClient.CreateThread(ctx, "dot-sdk-go-ctx-"+stamp, task+"-thread")
	if err != nil {
		stand.record(t, "custom-agent-standalone-go", "FAIL", map[string]any{"state_thread": map[string]any{
			"status": "FAIL", "error": err.Error()}})
		t.Fatalf("the custom agent could not create its thread: %v", err)
	}
	run, err := stateClient.CreateRun(ctx, thread.ID, task+"-run", json.RawMessage(`{"goal":"answer one line"}`))
	if err != nil {
		stand.record(t, "custom-agent-standalone-go", "FAIL", map[string]any{"state_run": map[string]any{
			"status": "FAIL", "thread": thread.ID, "error": err.Error()}})
		t.Fatalf("the custom agent could not create its run: %v", err)
	}
	checks["state_work_created"] = map[string]any{"status": "PASS", "thread": thread.ID, "run": run.ID,
		"run_status": run.Status, "run_task": run.TaskID, "context_id": thread.ContextID}

	// (b) The model call goes through the Model Gateway to the real Qwen route.
	modelClient, err := modelsdk.New(modelsdk.Config{BaseURL: origin,
		HTTPClient:  &http.Client{Transport: sdkTransport(t, stand), Timeout: 180 * time.Second},
		TokenSource: staticToken})
	if err != nil {
		t.Fatalf("model SDK client: %v", err)
	}
	generation, err := modelClient.Submit(ctx, modelsdk.Request{Key: task + "-gen",
		Model: mustEnv(t, "AHP_LLM_MODEL"),
		Messages: []modelsdk.Message{{Role: "user",
			Content: "Reply with exactly: CUSTOM-AGENT-GO-OK " + stamp}},
		MaxTokens: customAgentLegMaxTokens})
	modelCheck := map[string]any{"status": "PASS", "generation": generation.ID,
		"status_value": generation.Status, "provider": generation.Provider,
		"model": generation.Model, "output_bytes": len(generation.Output)}
	checks["model_gateway_on_real_qwen"] = modelCheck
	// The real Qwen route is nondeterministic about echoing the full stamp, so
	// the durable claim is the generation row (COMPLETED with usage) plus the
	// marker token; the exact echo is recorded, not demanded.
	echoed := generation.Output
	if len(echoed) > 120 {
		echoed = echoed[:120]
	}
	modelCheck["output_excerpt"] = echoed
	if err != nil || generation.Status != "COMPLETED" ||
		!strings.Contains(generation.Output, "CUSTOM-AGENT") {
		modelCheck["status"] = "FAIL"
		modelCheck["error"] = errText(err)
		// The cause comes from what THIS leg read back (status, finish_reason,
		// output bytes, transport result), never from a remembered narrative: an
		// earlier version of this leg printed a ready-made "500 INTERNAL inside
		// task-budget establishment" text for ANY failure, which lied whenever the
		// real failure was the 64-token ceiling (provider_empty_completion /
		// finish_reason=length, 216 cases in the LLM profile).
		cause := modelFailureCause(modelObserved{err: err, httpStatus: status(err),
			generationID: generation.ID, status: generation.Status, finishReason: generation.FinishReason,
			output: generation.Output, model: generation.Model, maxTokens: customAgentLegMaxTokens})
		for key, value := range cause {
			modelCheck[key] = value
		}
		recorded := "FAIL"
		if cause["cause"] == "submit_http_500_no_generation_row" {
			// DEF-DOTE2E-2 is assigned only when this run actually observed HTTP 500
			// with no generation row - the shape that defect describes.
			recorded = "DEFECT"
			modelCheck["defect"] = "DEF-DOTE2E-2"
			modelCheck["root_cause_area"] = "Model / execbridge"
			modelCheck["root_cause_note"] = "assigned because THIS run observed HTTP 500 with no generation row"
		}
		stand.record(t, "custom-agent-standalone-go", recorded, checks)
		t.Fatalf("the custom agent's model call did not answer: status=%q err=%v (cause=%v)", generation.Status,
			errText(err), cause["cause"])
	}

	// (c) The answer is stored as an artifact and read back through the SDK.
	artifactsClient, err := artifactsdk.New(artifactsdk.Config{BaseURL: origin,
		HTTPClient:       &http.Client{Transport: sdkTransport(t, stand), Timeout: 120 * time.Second},
		UploadHTTPClient: &http.Client{Transport: sdkTransport(t, stand), Timeout: 120 * time.Second},
		TokenSource:      staticToken})
	if err != nil {
		t.Fatalf("artifacts SDK client: %v", err)
	}
	answer := []byte(generation.Output)
	// The answer travels as an opaque byte stream. text/* artifacts first pass
	// the extraction pipeline, and the lane's macOS host cannot run the Linux
	// sandboxed parser - that gate and prerequisite are captured separately
	// below instead of being faked ready.
	ref, err := artifactsClient.Put(ctx, artifactsdk.PutRequest{Key: task + "-answer",
		Name: "answer.bin", MIME: "application/octet-stream", Data: answer})
	if err != nil {
		checks["artifact_round_trip"] = map[string]any{"status": "FAIL", "error": err.Error()}
		stand.record(t, "custom-agent-standalone-go", "FAIL", checks)
		t.Fatalf("the custom agent could not store its answer: %v", err)
	}
	metaRef, errGet := artifactsClient.Get(ctx, ref.ID)
	checks["sdk_get_metadata"] = map[string]any{"status": "PASS", "error": errText(errGet),
		"version": metaRef.Version}
	if errGet != nil {
		checks["sdk_get_metadata"].(map[string]any)["status"] = "FAIL"
		checks["artifact_round_trip"] = map[string]any{"status": "FAIL",
			"failed_op": "Get", "error": errText(errGet)}
		stand.record(t, "custom-agent-standalone-go", "FAIL", checks)
		t.Fatalf("SDK Get refused on the owner's own artifact: %v", errText(errGet))
	}
	back, err := artifactsClient.Download(ctx, ref.ID)
	if err != nil {
		rawCode, rawBytes, rawErr := stand.machinePost(stand.HTTP, "/v1/artifacts/Download",
			map[string]any{"id": ref.ID}, token)
		checks["artifact_round_trip"] = map[string]any{"status": "FAIL",
			"failed_op": "Download", "error": errText(err),
			"same_request_via_plain_http": map[string]any{"http": rawCode,
				"bytes": len(rawBytes), "error": errText(rawErr), "sha256": digestHex(rawBytes)}}
		stand.record(t, "custom-agent-standalone-go", "FAIL", checks)
		t.Fatalf("SDK Download refused on the owner's own artifact: %v", errText(err))
	}
	sum := sha256.Sum256(back)
	roundTrip := map[string]any{"status": "PASS", "artifact": ref.ID, "sha256": hex.EncodeToString(sum[:]),
		"bytes": len(back), "digest_matches": hex.EncodeToString(sum[:]) == digestHex(answer)}
	if err != nil || hex.EncodeToString(sum[:]) != digestHex(answer) {
		roundTrip["status"] = "FAIL"
		roundTrip["error"] = errText(err)
		roundTrip["reason"] = contracts.ReasonOf(err)
	}
	checks["artifact_round_trip"] = roundTrip
	// Seal processes supported text synchronously when the owner has its Linux
	// parser configured. Verify the completed derivation and its actual bytes;
	// a historical refusal on a parser-less Mac is not a successful happy path.
	textRef, errTextPut := artifactsClient.Put(ctx, artifactsdk.PutRequest{Key: task + "-text",
		Name: "answer.txt", MIME: "text/plain", Data: answer})
	gate := map[string]any{"status": "PASS"}
	if errTextPut == nil {
		ready, readyErr := artifactsClient.Readiness(ctx, textRef.ID)
		gate["readiness"] = ready
		gate["extraction_ready"] = ready.ExtractionReady
		gate["search_ready"] = ready.SearchReady
		gateCode, textBytes, gateErr := stand.machinePost(stand.HTTP, "/v1/artifacts/Download",
			map[string]any{"id": textRef.ID}, token)
		gate["http"] = gateCode
		gate["error"] = truncate(errText(gateErr), 100)
		chunk, chunkErr := artifactsClient.ReadTextChunk(ctx, textRef.ID, textRef.Version, 0, 32768, 32768)
		textMatches := chunkErr == nil && chunk.EOF && chunk.Version == textRef.Version &&
			strings.TrimSpace(chunk.Text) == strings.TrimSpace(string(answer))
		gate["extracted_text_matches"] = textMatches
		gate["extraction_error"] = errText(chunkErr)
		gate["original_digest_matches"] = digestHex(textBytes) == digestHex(answer)
		rows := durableExtractionObservation(stand, digestHex(answer)).Observed["durable_extraction_rows"]
		count, rowsOK := rows.(int)
		if readyErr != nil || !ready.Sealed || !ready.ContentReady || !ready.Ready ||
			!ready.ExtractionReady || !ready.SearchReady || ready.Quarantined || gateErr != nil ||
			gateCode != http.StatusOK || digestHex(textBytes) != digestHex(answer) || !textMatches || !rowsOK || count < 1 {
			gate["status"] = "FAIL"
			gate["readiness_error"] = errText(readyErr)
		}
	} else {
		gate["status"] = "FAIL"
		gate["error"] = errText(errTextPut)
	}
	checks["text_extraction_gate"] = gate
	if gate["status"] == "FAIL" {
		stand.record(t, "custom-agent-standalone-go", "FAIL", checks)
		t.Fatalf("the text extraction gate did not behave as designed: %v", gate)
	}
	if roundTrip["status"] == "FAIL" {
		stand.record(t, "custom-agent-standalone-go", "FAIL", checks)
		t.Fatalf("the artifact round trip through the SDK lost bytes: %v", roundTrip["error"])
	}

	// (c2) The durable terminal this scenario otherwise has no way to state.
	cycle, cycleErr := durableTerminalCycle(t, ctx, stand, stateClient, thread.ID, run.ID, stamp)
	if cycleErr != nil {
		cycle["status"] = "FAIL"
		// The cause is what THIS run observed, never a remembered narrative.
		cycle["failure_reason"] = cycleErr.Error()
		checks["state_run_terminal_cycle"] = cycle
		stand.record(t, "custom-agent-standalone-go", "FAIL", checks)
		t.Fatalf("the standalone agent never reached a durable terminal: %v", cycleErr)
	}
	checks["state_run_terminal_cycle"] = cycle

	// (d) The honesty of the scenario itself, recorded as an ATTESTATION and not as
	// a leg. It states a fact about the traffic this program generated, not about
	// the platform, and no durable row anywhere says "this scenario called no Space
	// route" - declaring it frame-free would have meant inventing a locator, which
	// is the failure mode DEF-DOTSDK-TERMINAL-1 exists to prevent. Without a
	// "status" key the classifier cannot read it as a leg, so it can neither add a
	// false completion nor lose a real one.
	checks["agent_space_not_used"] = map[string]any{
		"claim":         "this scenario called no /v1/space/ route",
		"planes":        []string{"/v1/state/", "/v1/model/", "/v1/artifacts/", "/v1/identity/control/exchange"},
		"why_not_a_leg": "it attests the scenario's own traffic, not a platform behaviour; it has no durable stand-in for a terminal frame, so it is not counted as a completed leg",
		"how_to_check":  "runtime/<instance>/logs/backend.log spans carry attributes.\"http.target\"; the Space traffic of an acceptance series belongs to the journey scenario, never to this one",
	}

	// Every frame-free leg of this scenario now carries the durable row the harness
	// read for it (DEF-DOTSDK-TERMINAL-1, director variant (a)): a declaration in
	// frameFreeDeclarations without these facts leaves the leg
	// frame_free_unproven (label vocabulary v2), which under STRICT_TERMINAL is
	// BLOCKED.
	for legName, observation := range map[string]durableRow{
		"state_work_created":         durableRunRowObservation(stand, run.ID),
		"model_gateway_on_real_qwen": durableGenerationObservation(stand, generation.ID),
		"sdk_get_metadata":           durableArtifactObservation(stand, ref.ID),
		"artifact_round_trip":        durableArtifactObservation(stand, ref.ID),
		"text_extraction_gate":       durableExtractionObservation(stand, digestHex(answer)),
	} {
		legMap, ok := checks[legName].(map[string]any)
		if !ok {
			continue
		}
		attachDurableEvidence(legMap, observation)
	}
	stand.record(t, "custom-agent-standalone-go", "PASS", checks)
}

// durableTerminalCycle drives the standalone agent's own durable terminal: claim
// the run it created, work under the claim's OWN task token, write a checkpoint and
// a KV answer, then release the run. What a turn says with a terminal frame, a
// stand-alone agent says with this row ending in a terminal status - and the row is
// read back from PostgreSQL by the harness, not asserted by the agent
// (DEF-DOTSDK-TERMINAL-1: frame-free is never evidence-free).
func durableTerminalCycle(t *testing.T, ctx context.Context, stand *instance,
	stateClient *statesdk.Client, threadID, runID, stamp string) (map[string]any, error) {
	t.Helper()
	worker := "lane-go-agent-" + stamp
	namespace := "lane-dot-sdk-go-" + runID
	checkpointKey := "cp-" + stamp
	check := map[string]any{"worker": worker, "namespace": namespace, "run": runID}
	claimed, err := stateClient.ClaimRunFor(ctx, worker, 60_000, runID)
	if err != nil || claimed == nil {
		check["error"] = errText(err)
		check["claimed"] = false
		return check, fmt.Errorf("the standalone agent could not claim its own run: %v", err)
	}
	fence := statesdk.Fence{RunID: runID, Worker: claimed.Lease.Worker, Epoch: claimed.Lease.Epoch}
	check["epoch"] = fence.Epoch
	claimToken := claimed.TaskToken
	// Fenced work runs on the token THIS claim issued; the ingress token cannot do it.
	taskClient, err := statesdk.New(statesdk.Config{BaseURL: stand.Origin,
		HTTPClient:  &http.Client{Transport: sdkTransport(t, stand), Timeout: 120 * time.Second},
		TokenSource: func(context.Context) (string, error) { return claimToken, nil }})
	if err != nil {
		check["error"] = errText(err)
		return check, fmt.Errorf("fenced state client: %v", err)
	}
	if _, err = taskClient.Heartbeat(ctx, fence, 60_000); err != nil {
		check["error"] = errText(err)
		return check, fmt.Errorf("heartbeat under the claim fence failed: %v", err)
	}
	checkpoint := statesdk.Checkpoint{ID: checkpointKey, Namespace: namespace, Version: 1,
		Metadata: json.RawMessage(`{"writer":"customagent_sdk_test.go"}`),
		Parts:    []statesdk.CheckpointPart{{Channel: "answer", Version: "1", Data: []byte("answer " + stamp)}}}
	if _, err = taskClient.PutCheckpoint(ctx, fence, checkpoint, checkpointKey); err != nil {
		check["error"] = errText(err)
		return check, fmt.Errorf("the agent could not store its checkpoint: %v", err)
	}
	if _, err = taskClient.PutKV(ctx, fence, []string{namespace}, "answer-key", "kvop-"+runID, 0,
		json.RawMessage(`{"stamp":"`+stamp+`"}`)); err != nil {
		check["error"] = errText(err)
		return check, fmt.Errorf("the agent could not write its KV answer: %v", err)
	}
	if err = taskClient.Release(ctx, fence, "done"); err != nil {
		check["error"] = errText(err)
		return check, fmt.Errorf("the agent could not release its run: %v", err)
	}
	// The calls above are the program's own words. These three reads are the lane's.
	for _, observation := range []durableRow{
		durableRunRowObservation(stand, runID),
		durableCheckpointObservation(stand, threadID, namespace, checkpointKey),
		durableKVObservation(stand, threadID, []string{namespace}, "answer-key"),
	} {
		attachDurableEvidence(check, observation)
	}
	if status, _ := check["durable_run_status"].(string); status != "done" {
		check["error"] = fmt.Sprintf("the run row is %q after release, not the terminal %q", status, "done")
		return check, fmt.Errorf("%s", check["error"])
	}
	if row, _ := check["durable_checkpoint_row"].(string); row != "present" {
		check["error"] = "the checkpoint row the agent wrote is not in ahp_state.checkpoints"
		return check, fmt.Errorf("%s", check["error"])
	}
	if row, _ := check["durable_kv_row"].(string); row != "present" {
		check["error"] = "the KV answer the agent wrote is not in ahp_state.kv"
		return check, fmt.Errorf("%s", check["error"])
	}
	check["status"] = "PASS"
	return check, nil
}

// TestCustomAgentRunsStandaloneViaPythonSDK runs the same standalone scenario in
// Python against the published SDKs - sdk/python/ahp_state (durable State work),
// sdk/python/ahp_clients (Model, Artifacts) - and records the child's own verdict.
func TestCustomAgentRunsStandaloneViaPythonSDK(t *testing.T) {
	stand := loadStand(t)
	interpreter := os.Getenv("AHP_DOT_E2E_PYTHON")
	if interpreter == "" {
		interpreter = filepath.Join(stand.Dir, "..", "..", "..", "python-venv", "bin", "python")
	}
	if _, err := os.Stat(interpreter); err != nil {
		stand.record(t, "custom-agent-standalone-python", "BLOCKED", map[string]any{"python": map[string]any{
			"status": "BLOCKED", "interpreter": interpreter, "error": err.Error(),
			"note": "the lane Python interpreter with httpx is an external prerequisite of this scenario"}})
		// Token grammar owned by scripts/skip-verdict.mjs (Russell). The interpreter is
		// an external prerequisite of the Python leg, so its absence is an environment
		// skip, never a silent one.
		t.Skipf("SKIP_ENV[tag=dot_e2e_python] reason=lane_interpreter_missing missing=AHP_DOT_E2E_PYTHON -- a skip here means not-covered, not passed: no lane Python interpreter at %s", interpreter)
	}
	script := filepath.Join("customagent", "python", "custom_agent.py")
	if _, err := os.Stat(script); err != nil {
		t.Fatalf("the Python custom agent is missing: %v", err)
	}
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	token, task := stand.taskGrantContext(t, stand.HTTP, human, "dot-sdk-py-"+stamp,
		"dot-sdk-py-ctx-"+stamp, []string{"state", "model", "artifacts", "execution"})
	command := exec.Command(interpreter, script, "--origin", stand.Origin, "--token", token,
		"--task", task, "--model", mustEnv(t, "AHP_LLM_MODEL"), "--stamp", stamp,
		"--cert", filepath.Join(stand.Dir, "certs", "operator.crt"),
		"--key", filepath.Join(stand.Dir, "certs", "operator.key"),
		"--ca", filepath.Join(stand.Dir, "certs", "ca.crt"))
	raw, runErr := command.CombinedOutput()
	verdict := map[string]any{"exit_error": errText(runErr), "output_bytes": len(raw)}
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "{") {
			var parsed map[string]any
			if json.Unmarshal([]byte(trimmed), &parsed) == nil {
				verdict = parsed
				verdict["exit_error"] = errText(runErr)
			}
		}
	}
	// The child program speaks for itself; the lane checks it against PostgreSQL.
	// These three reads are the durable stand-in for a terminal frame that the
	// python_agent declaration names: the run row, the checkpoint row and the KV row
	// the child claims it wrote (DEF-DOTSDK-TERMINAL-1).
	for _, observation := range pythonDurableObservations(stand, verdict, stamp) {
		attachDurableEvidence(verdict, observation)
	}
	if verdict["status"] == "PASS" {
		// A child that says "released" while the run row says otherwise is a FAIL
		// with the observed value, never a PASS with a recorded disagreement.
		if status, _ := verdict["durable_run_status"].(string); status != "done" {
			verdict["status"] = "FAIL"
			verdict["failure_reason"] = fmt.Sprintf(
				"the child reported PASS but its run row is %v in ahp_state.runs, not the terminal \"done\"; durable_read=%v",
				verdict["durable_run_status"], verdict["terminal_evidence"])
		}
	}
	if verdict["status"] != "PASS" {
		recorded := "FAIL"
		if modelCheck, ok := verdict["checks"].(map[string]any); ok {
			if inner, ok := modelCheck["model_gateway_on_real_qwen"].(map[string]any); ok {
				// The child derives its own cause from the generation it read; the
				// wrapper only names a known defect when that observed cause matches.
				if causeName, _ := inner["cause"].(string); causeName == "submit_http_500_no_generation_row" {
					recorded = "DEFECT"
					inner["defect"] = "DEF-DOTE2E-2"
					inner["root_cause_area"] = "Model / execbridge"
					inner["root_cause_note"] = "assigned because the child observed HTTP 500 with no generation row"
				}
			}
		}
		stand.record(t, "custom-agent-standalone-python", recorded, map[string]any{"python_agent": verdict})
		t.Fatalf("the Python custom agent did not pass: %s (%s)", fmt.Sprint(verdict["detail"]), recorded)
	}
	stand.record(t, "custom-agent-standalone-python", "PASS", map[string]any{"python_agent": verdict})
}

// pythonDurableObservations reads, from the lane side, the rows the Python child
// says it wrote. The identifiers come from the child's own JSON - which is exactly
// the point: the child cannot make the artifact claim a durable terminal that
// PostgreSQL does not agree with.
func pythonDurableObservations(stand *instance, verdict map[string]any, stamp string) []durableRow {
	childChecks, _ := verdict["checks"].(map[string]any)
	created, _ := childChecks["state_work_created"].(map[string]any)
	runID, _ := created["run"].(string)
	threadID, _ := created["thread"].(string)
	if runID == "" || threadID == "" {
		return nil
	}
	namespace := "lane-dot-sdk-py"
	checkpointKey := "cp-" + stamp
	if cp, ok := childChecks["state_checkpoint"].(map[string]any); ok {
		if value, ok := cp["namespace"].(string); ok && value != "" {
			namespace = value
		}
	}
	observations := []durableRow{durableRunRowObservation(stand, runID),
		durableCheckpointObservation(stand, threadID, namespace, checkpointKey),
		durableKVObservation(stand, threadID, []string{namespace}, "answer-key")}
	return observations
}
