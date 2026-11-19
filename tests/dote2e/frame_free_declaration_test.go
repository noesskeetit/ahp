package dote2e_test

// Frame-free declarations must be explicit, counted, and EVIDENCED
// (DEF-DOTSDK-TERMINAL-1, director variant (a), 2026-11-10).
//
// The hole this closes: AHP_DOT_E2E_STRICT_TERMINAL=1 made every leg without a
// terminal frame unknown, so the two SDK standalone scenarios could not be
// certified on any SHA - they have no turn and therefore no frame. The tempting
// wrong fix was a blanket exemption ("scenarios without turns are not strict"),
// which is a collective crack: any red leg could be declared "a scenario without
// a turn" after the fact. The accepted fix keeps ONE mechanism for every scenario:
// a leg is frame-free only by an entry in frameFreeDeclarations, only for the
// reason that entry states, and only while it presents the durable stand-in the
// entry names (table+query, or a public face whose request survives as an access
// span). These tests are pure - no stand, no PostgreSQL.

import (
	"strings"
	"testing"
)

func slicesContains(haystack []string, needle string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}

// strictScenario classifies a scenario under the acceptance rule and returns the
// whole classification.
func strictScenario(t *testing.T, scenario string, checks map[string]any) legClassification {
	t.Helper()
	t.Setenv("AHP_DOT_E2E_STRICT_TERMINAL", "1")
	return classifyLegs(scenario, checks)
}

// declaredFacts gives a synthetic leg the durable facts its declaration names, so a
// PASS fixture proves the rule instead of dodging it.
func declaredFacts(leg map[string]any, scenario, name string) map[string]any {
	decl, ok := declaredFrameFree(scenario, name)
	if !ok {
		return leg
	}
	for _, key := range decl.Locator.ObservedKeys {
		if _, present := leg[key]; present {
			continue
		}
		// A fixture that satisfies a declaration by writing any blob in every key would
		// prove nothing, so it obeys the value rules the declaration states.
		if want, found := mustEqualValue(decl, key); found {
			leg[key] = want
			continue
		}
		leg[key] = "fixture-durable-fact"
	}
	for _, key := range decl.Locator.MustBeEmpty {
		if _, present := leg[key]; !present {
			leg[key] = []string{}
		}
	}
	return leg
}

func mustEqualValue(decl frameFreeDeclaration, key string) (any, bool) {
	for _, want := range decl.Locator.MustEqual {
		if want.Key == key {
			return want.Value, true
		}
	}
	return nil, false
}

// scenarioChecks builds the PASS shape of a scenario: every declared frame-free leg,
// each carrying the facts its declaration names.
func scenarioChecks(scenario string) map[string]any {
	checks := map[string]any{}
	for name := range frameFreeDeclarations[scenario] {
		checks[name] = declaredFacts(map[string]any{"status": "PASS"}, scenario, name)
	}
	return checks
}

func TestFrameFreeDeclarationCountsAreExact(t *testing.T) {
	// The counts are the guard. A new frame-free leg must change a number HERE, in
	// the same commit, with a reason - it cannot arrive quietly in a leg's code.
	expects := map[string]int{
		// ten observation legs; j10 has a real terminal frame and never belongs here
		scenarioPersonalDotJourney: 10,
		// state work, the durable terminal cycle, model, metadata, artifact, text gate
		scenarioStandaloneGoSDK: 6,
		// one leg: the child program, checked against its own durable rows
		scenarioStandalonePythonSDK: 1,
	}
	total := 0
	for scenario, want := range expects {
		got := len(frameFreeDeclarations[scenario])
		if got != want {
			t.Fatalf("scenario %s declares %d frame-free legs, guard expects %d - change this number only with a reason in the same commit", scenario, got, want)
		}
		total += got
	}
	if len(frameFreeDeclarations) != len(expects) {
		scenarios := []string{}
		for scenario := range frameFreeDeclarations {
			scenarios = append(scenarios, scenario)
		}
		t.Fatalf("frame-free declarations cover %d scenarios %v, guard knows %d - a new declaring scenario is a conscious change",
			len(frameFreeDeclarations), scenarios, len(expects))
	}
	if total != 17 {
		t.Fatalf("the lane declares 17 frame-free legs in total, got %d", total)
	}
	if journeyFrameFreeLegs["j10_browser_turn_def1_branch"] {
		t.Fatal("the browser turn leg has a durable terminal frame and must never be declared frame-free")
	}
	for scenario, decls := range frameFreeDeclarations {
		for leg, decl := range decls {
			if strings.TrimSpace(decl.Reason) == "" {
				t.Fatalf("%s/%s is declared frame-free with no reason", scenario, leg)
			}
		}
	}
}

func TestFrameFreeDeclarationsNameRealLocators(t *testing.T) {
	// A locator that names nothing is decoration. Every declaration must name a
	// table, the query/predicate over it, and the artifact keys that carry the value.
	for scenario, decls := range frameFreeDeclarations {
		for leg, decl := range decls {
			switch decl.Locator.Kind {
			case locatorDurableRow, locatorPublicFaceRead, locatorDurableStoreFile:
			default:
				t.Fatalf("%s/%s uses unknown locator kind %q", scenario, leg, decl.Locator.Kind)
			}
			if strings.TrimSpace(decl.Locator.Table) == "" {
				t.Fatalf("%s/%s names no table/store", scenario, leg)
			}
			if strings.TrimSpace(decl.Locator.Query) == "" {
				t.Fatalf("%s/%s names no query/predicate", scenario, leg)
			}
			if len(decl.Locator.ObservedKeys) == 0 {
				t.Fatalf("%s/%s names no observed facts, so nothing could ever prove it", scenario, leg)
			}
			for _, key := range decl.Locator.ObservedKeys {
				if key == "status" || key == "terminal_status" {
					t.Fatalf("%s/%s tries to use %q as its own durable evidence", scenario, leg, key)
				}
			}
			// A must-be-empty key is judged by its VALUE, so putting it in observed_keys
			// too would make an honest empty list read as "absent" and deadlock the leg.
			for _, key := range decl.Locator.MustBeEmpty {
				if slicesContains(decl.Locator.ObservedKeys, key) {
					t.Fatalf("%s/%s lists %q in both observed_keys and must_be_empty", scenario, leg, key)
				}
			}
			for _, want := range decl.Locator.MustEqual {
				if !slicesContains(decl.Locator.ObservedKeys, want.Key) {
					t.Fatalf("%s/%s pins %q to a value but does not require it as an observed fact", scenario, leg, want.Key)
				}
				if strings.TrimSpace(want.Why) == "" {
					t.Fatalf("%s/%s pins %q without saying why the value matters", scenario, leg, want.Key)
				}
			}
			if decl.Locator.Kind == locatorPublicFaceRead && decl.Locator.ExpectedHTTP == 0 &&
				len(decl.Locator.ExpectedHTTPAnyOf) == 0 {
				t.Fatalf("%s/%s names a public face without any expected status", scenario, leg)
			}
			if decl.Locator.Kind != locatorPublicFaceRead && (decl.Locator.ExpectedHTTP != 0 ||
				len(decl.Locator.ExpectedHTTPAnyOf) > 0) {
				t.Fatalf("%s/%s states an expected HTTP code without being a face read", scenario, leg)
			}
		}
	}
}

func TestStandaloneScenariosPassOnDeclaredEvidence(t *testing.T) {
	// The shape the acceptance series must produce: declared legs, facts present,
	// verdict PASS - and the artifact index says so with locators, not flags.
	for _, scenario := range []string{scenarioStandaloneGoSDK, scenarioStandalonePythonSDK} {
		c := strictScenario(t, scenario, scenarioChecks(scenario))
		if v := overallVerdict(c.completed, c.blocked, c.failed); v != "PASS" {
			t.Fatalf("%s with declared evidence must PASS, got %s blocked=%v reasons=%v",
				scenario, v, c.blocked, c.blockedReasons)
		}
		if len(c.frameFreeUnproven) != 0 {
			t.Fatalf("%s must show every declared leg as proven, got %v", scenario, c.frameFreeUnproven)
		}
		for name, evidence := range c.frameFree {
			if evidence["proven"] != true {
				t.Fatalf("%s/%s: index says not proven: %v", scenario, name, evidence)
			}
			if evidence["frame_free_reason"] == "" || evidence["locator_table"] == "" || evidence["locator_query"] == "" {
				t.Fatalf("%s/%s: the artifact index carries no locator: %v", scenario, name, evidence)
			}
			observed, ok := evidence["observed"].(map[string]any)
			if !ok || len(observed) == 0 {
				t.Fatalf("%s/%s: the index must carry the OBSERVED durable values a verifier re-reads, got %v", scenario, name, evidence["observed"])
			}
		}
	}
}

func TestDeclaredFrameFreeLegWithoutDurableEvidenceIsBlocked(t *testing.T) {
	// RED against a declaration-only implementation: a promise is not evidence.
	for _, scenario := range []string{scenarioPersonalDotJourney, scenarioStandaloneGoSDK, scenarioStandalonePythonSDK} {
		checks := scenarioChecks(scenario)
		var victim string
		for name := range checks {
			victim = name
			break
		}
		if victim == "" {
			t.Fatalf("%s declares no legs", scenario)
		}
		decl := frameFreeDeclarations[scenario][victim]
		leg := checks[victim].(map[string]any)
		for _, key := range decl.Locator.ObservedKeys {
			delete(leg, key)
		}
		c := strictScenario(t, scenario, checks)
		if got := c.terminalStates[victim]; got != terminalDeclaredUnproven {
			t.Fatalf("%s/%s must classify as %q, got %q", scenario, victim, terminalDeclaredUnproven, got)
		}
		found := false
		for _, name := range c.blocked {
			found = found || name == victim
		}
		if !found {
			t.Fatalf("%s/%s without durable evidence must be blocked, blocked=%v", scenario, victim, c.blocked)
		}
		if reason := c.blockedReasons[victim]; !strings.Contains(reason, "frame_free_without_durable_evidence") {
			t.Fatalf("%s/%s blocked reason must name the missing evidence, got %q", scenario, victim, reason)
		}
		if len(c.frameFreeUnproven) != 1 || c.frameFreeUnproven[0] != victim {
			t.Fatalf("%s/%s must be listed in frame_free_unproven_legs, got %v", scenario, victim, c.frameFreeUnproven)
		}
		if v := overallVerdict(c.completed, c.blocked, c.failed); v == "PASS" {
			t.Fatalf("regression: PASS while %s/%s presented no durable terminal equivalent", scenario, victim)
		}
	}
}

func TestLegCannotSelfDeclareFrameFree(t *testing.T) {
	// The magic string "no_frame_expected" used to be honoured from the leg itself.
	// It is not any more: frame-free status is granted by the declaration table, and
	// a leg that writes it lands in non_completed - a wrong terminal, never a waiver.
	t.Setenv("AHP_DOT_E2E_STRICT_TERMINAL", "1")
	checks := scenarioChecks(scenarioStandaloneGoSDK)
	var victim string
	for name := range checks {
		victim = name
		break
	}
	checks[victim].(map[string]any)["terminal_status"] = "no_frame_expected"
	c := classifyLegs(scenarioStandaloneGoSDK, checks)
	if got := c.terminalStates[victim]; got == terminalDeclaredNoFrame || got == terminalSeenCompleted {
		t.Fatalf("a leg self-declared %q as completed state %q", "no_frame_expected", got)
	}
	if v := overallVerdict(c.completed, c.blocked, c.failed); v == "PASS" {
		t.Fatalf("regression: PASS while a leg self-declared its way out of a terminal frame")
	}
}

func TestUndeclaredStandaloneLegIsBlocked(t *testing.T) {
	// A new leg arriving in a scenario without a declaration is unknown, not
	// completed - the journey rule, now applied to the SDK scenarios too.
	checks := scenarioChecks(scenarioStandaloneGoSDK)
	const renamed = "state_something_new_unregistered"
	for name := range checks {
		checks[renamed] = checks[name]
		delete(checks, name)
		break
	}
	c := strictScenario(t, scenarioStandaloneGoSDK, checks)
	if got := c.terminalStates[renamed]; got != terminalAbsent {
		t.Fatalf("an undeclared leg must be %q, got %q", terminalAbsent, got)
	}
	if v := overallVerdict(c.completed, c.blocked, c.failed); v == "PASS" {
		t.Fatalf("regression: PASS with an undeclared leg")
	}
	if reason := c.blockedReasons[renamed]; !strings.Contains(reason, "missing_terminal_frame") {
		t.Fatalf("the undeclared leg's reason must name missing_terminal_frame, got %q", reason)
	}
}

func TestHarnessEvidenceIsCheckedAgainstItsDeclaration(t *testing.T) {
	// Evidence a leg attaches is validated, not admired: the table must agree with the
	// declaration, the query must be there, and every required fact must be inside the
	// observation. Anything else leaves the leg unproven.
	const scenario = scenarioStandaloneGoSDK
	const legName = "state_run_terminal_cycle"
	decl := frameFreeDeclarations[scenario][legName]
	leg := declaredFacts(map[string]any{"status": "PASS"}, scenario, legName)
	if proven, _ := frameFreeProof(decl, leg); !proven {
		t.Fatal("the fixture leg must be proven on its declared facts")
	}
	leg["terminal_evidence"] = map[string]any{"table": "ahp_something_else", "query": "SELECT 1",
		"observed": map[string]any{"durable_run_status": "done", "durable_checkpoint_row": "present",
			"durable_kv_row": "present"}, "provenance": "harness-reread"}
	if proven, evidence := frameFreeProof(decl, leg); proven {
		t.Fatalf("evidence pointed at another table must not prove the leg: %v", evidence)
	} else if evidence["table_agrees_with_declaration"] != false {
		t.Fatalf("the index must record the disagreement with the declared table: %v", evidence)
	}
	leg["terminal_evidence"] = map[string]any{"table": decl.Locator.Table, "query": decl.Locator.Query,
		"observed": map[string]any{"read_error": "dial tcp: connection refused"}, "provenance": "harness-reread"}
	if proven, evidence := frameFreeProof(decl, leg); proven {
		t.Fatalf("a read that failed must not prove the leg: %v", evidence)
	} else if evidence["read_error"] == nil {
		t.Fatalf("the index must publish why the evidence failed: %v", evidence)
	}
	leg["terminal_evidence"] = map[string]any{"table": decl.Locator.Table, "query": "",
		"observed": map[string]any{"durable_run_status": "done"}, "provenance": "harness-reread"}
	if proven, _ := frameFreeProof(decl, leg); proven {
		t.Fatal("evidence without the query that produced it must not prove the leg")
	}
	delete(leg, "terminal_evidence")
	leg["durable_run_status"] = "done"
	leg["durable_checkpoint_row"] = "present"
	leg["durable_kv_row"] = "present"
	if proven, evidence := frameFreeProof(decl, leg); !proven || evidence["provenance"] != "leg-declared" {
		t.Fatalf("declared facts on the leg must prove it as leg-declared: %v", evidence)
	}
}

func TestAcceptanceModeIsOptInAndExplicit(t *testing.T) {
	// rc=0 is not acceptance: the flag that makes a non-PASS verdict fail the run is
	// read here, so the rule has one definition (record() uses the same one).
	t.Setenv("AHP_DOT_E2E_ACCEPT", "")
	if acceptanceEnforced() {
		t.Fatal("acceptance mode must be off unless AHP_DOT_E2E_ACCEPT=1")
	}
	t.Setenv("AHP_DOT_E2E_ACCEPT", "1")
	if !acceptanceEnforced() {
		t.Fatal("AHP_DOT_E2E_ACCEPT=1 must enable acceptance mode")
	}
	t.Setenv("AHP_DOT_E2E_ACCEPT", "0")
	if acceptanceEnforced() {
		t.Fatal("AHP_DOT_E2E_ACCEPT=0 must not enable acceptance mode")
	}
}

func TestHistoricalGreenJourneyShapeStillProves(t *testing.T) {
	// Anti-underfit check. These are the leg facts of the REAL green journey
	// (evidence/dot-sdk-live-44c466e2-20261110T043026Z/artifacts/
	// dot-e2e-scenario-personal-dot-journey-20261110T043557Z.json: verdict PASS,
	// j1..j11 completed, strict terminal enforcement on). The frame-free rule must
	// accept the shape that already passed - otherwise the rule was tuned to a
	// fixture instead of to the platform. Values, not just key names, are copied.
	checks := map[string]any{
		"j1_task_grant_token":   map[string]any{"status": "PASS", "grant_rows": 1},
		"j2_config_profile_pin": map[string]any{"status": "PASS", "config_revision": 4},
		"j3_model_real_qwen": map[string]any{"status": "PASS", "attempts": 1, "durable_rows": 1,
			"generation": "gen-1", "model": "qwen"},
		"j4_tool_hitl_result": map[string]any{"status": "PASS", "get_result_http": 200,
			"result_carries_note": true, "get_result_error": ""},
		"j5_memory": map[string]any{"status": "PASS", "attempts": 2, "code_word_found": true,
			"operation_states": []string{"PENDING", "COMPLETED"}, "extraction_required_retry": true},
		"j6_file_sha": map[string]any{"status": "PASS", "artifact": "art-1", "http": 200,
			"sha256": "1f7852f46535", "sealed": "1f7852f46535", "expected": "1f7852f46535"},
		"j7_history_reload": map[string]any{"status": "PASS", "same_run": true, "error": ""},
		"j8_hitl_cancel":    map[string]any{"status": "PASS", "held_again": true, "no_effect_in_store": true},
		// j9 as the CURRENT leg publishes it (isolation-scope, 3b71b6b9: per-resource
		// probes judged by contract code, a durable stranger-grant probe, and an
		// owner-control trap). The pre-3b71b6b9 shape - three refusal booleans - is
		// asserted unproven below, on purpose: those three booleans are exactly the
		// self-attestation this rule exists to stop.
		"j9_two_user_isolation": map[string]any{"status": "PASS", "resources_attempted": 12,
			"denied_resource_count": 10, "contract_codes": []string{"NOT_FOUND", "FORBIDDEN"},
			"probes":          []map[string]any{{"resource": "artifact", "http": 404, "code": "NOT_FOUND"}},
			"artifact_denied": true, "artifact_http": 404, "run_denied": true, "run_http": 404,
			"memory_empty_or_denied": true, "leaked_resources": []string{},
			"foreign_subject_grant_rows": 0, "owner_control_sentinel_ran": true},
		"j11_human_artifact_read": map[string]any{"status": "PASS", "http": 200,
			"received_sha256": "1f7852f46535", "sealed_sha256": "1f7852f46535", "bytes_match_sealed": true},
		"j10_browser_turn_def1_branch": map[string]any{"status": "PASS", "terminal_status": "done",
			"answer_found": true, "answer_marker": "JOURNEY-TURN-OK"},
	}
	c := strictJourney(t, checks)
	if v := overallVerdict(c.completed, c.blocked, c.failed); v != "PASS" {
		t.Fatalf("the historical green journey shape must still PASS, got %s blocked=%v reasons=%v", v, c.blocked, c.blockedReasons)
	}
	if len(c.completed) != 11 {
		t.Fatalf("want 11 completed legs, got %d", len(c.completed))
	}
	if len(c.frameFreeUnproven) != 0 {
		t.Fatalf("no leg of the green journey may be unproven, got %v", c.frameFreeUnproven)
	}
	if c.terminalStates["j10_browser_turn_def1_branch"] != terminalSeenCompleted {
		t.Fatal("the turn leg must stay an OBSERVED completed frame")
	}
	for _, name := range []string{"j1_task_grant_token", "j6_file_sha", "j9_two_user_isolation"} {
		if c.terminalStates[name] != terminalDeclaredNoFrame {
			t.Fatalf("%s must be declared-and-proven, got %q", name, c.terminalStates[name])
		}
	}
	// Turning one real fact off must break it: the rule is load-bearing on the
	// historical shape, not decoration that any blob satisfies.
	delete(checks["j1_task_grant_token"].(map[string]any), "grant_rows")
	if v, _, blocked, _ := journeyVerdict(checks); v == "PASS" {
		t.Fatalf("removing j1's durable grant count must stop the PASS, blocked=%v", blocked)
	}
	checks["j1_task_grant_token"].(map[string]any)["grant_rows"] = 1

	// And the OLD j9 shape is deliberately no longer enough. This is a documented
	// consequence, not a silent retcon of history: the 20261110T043557Z artifact was
	// judged by the rule in force then, and is neither rewritten nor revoked.
	checks["j9_two_user_isolation"] = map[string]any{"status": "PASS", "artifact_denied": true,
		"run_denied": true, "memory_empty_or_denied": true, "codes": []string{"403", "403", "404"}}
	cOld := strictScenario(t, scenarioPersonalDotJourney, checks)
	if got := cOld.terminalStates["j9_two_user_isolation"]; got != terminalDeclaredUnproven {
		t.Fatalf("three refusal booleans must read as declared-unproven, got %q", got)
	}
	if reason := cOld.blockedReasons["j9_two_user_isolation"]; !strings.Contains(reason, "frame_free_without_durable_evidence") {
		t.Fatalf("the blocked reason must name the missing durable evidence, got %q", reason)
	}
	if v := overallVerdict(cOld.completed, cOld.blocked, cOld.failed); v == "PASS" {
		t.Fatal("regression: PASS while the isolation leg proves isolation by asserting it")
	}
}

func TestFrameFreeProofRejectsABreachTheLegItselfRecords(t *testing.T) {
	// RED against "presence proves": a leg that records a non-empty breach list hands
	// the harness the evidence of the failure inside its own PASS blob.
	checks := scenarioChecks(scenarioPersonalDotJourney)
	leg := checks["j9_two_user_isolation"].(map[string]any)
	leg["leaked_resources"] = []string{"tool_result_note"}
	c := strictScenario(t, scenarioPersonalDotJourney, checks)
	if got := c.terminalStates["j9_two_user_isolation"]; got != terminalDeclaredUnproven {
		t.Fatalf("a recorded leak must not classify as proven, got %q", got)
	}
	evidence := c.frameFree["j9_two_user_isolation"]
	contradicted, _ := evidence["contradicted_facts"].([]string)
	if len(contradicted) == 0 || !strings.Contains(contradicted[0], "leaked_resources") {
		t.Fatalf("the artifact must name the contradicted fact, got %v", evidence["contradicted_facts"])
	}
	if v := overallVerdict(c.completed, c.blocked, c.failed); v == "PASS" {
		t.Fatalf("PASS while the leg recorded leaked resources: blocked=%v failed=%v", c.blocked, c.failed)
	}
}

func TestFrameFreeProofRequiresTheDeclaredValueNotJustTheKey(t *testing.T) {
	// Two durable contradictions the presence rule would have waved through: a grant
	// row held by the stranger inside the owner's tenant, and an owner-control trap
	// that never ran (so the refusals say nothing about the owner's own access).
	for _, mutation := range []struct {
		key   string
		value any
		want  string
	}{
		{"foreign_subject_grant_rows", 1, "foreign_subject_grant_rows"},
		{"owner_control_sentinel_ran", false, "owner_control_sentinel_ran"},
	} {
		checks := scenarioChecks(scenarioPersonalDotJourney)
		checks["j9_two_user_isolation"].(map[string]any)[mutation.key] = mutation.value
		c := strictScenario(t, scenarioPersonalDotJourney, checks)
		if got := c.terminalStates["j9_two_user_isolation"]; got != terminalDeclaredUnproven {
			t.Fatalf("%s=%v must not be proven, got %q", mutation.key, mutation.value, got)
		}
		evidence := c.frameFree["j9_two_user_isolation"]
		contradicted, _ := evidence["contradicted_facts"].([]string)
		found := false
		for _, line := range contradicted {
			found = found || strings.Contains(line, mutation.want)
		}
		if !found {
			t.Fatalf("%s=%v must be named in contradicted_facts, got %v", mutation.key, mutation.value, contradicted)
		}
		if v := overallVerdict(c.completed, c.blocked, c.failed); v == "PASS" {
			t.Fatalf("PASS while %s=%v, blocked=%v", mutation.key, mutation.value, c.blocked)
		}
	}
}

// greenFixtureFor returns the synthetic GREEN checks of a scenario - the same maps
// the acceptance-shaped tests build. Kept in one place so the two guards below
// cannot check a different fixture than the tests that publish verdicts.
func greenFixtureFor(t *testing.T, scenario string) map[string]any {
	t.Helper()
	if scenario == scenarioPersonalDotJourney {
		return journeyGreenChecks()
	}
	return scenarioChecks(scenario)
}

// requiredFactsOf lists every key a declaration judges: the observed facts plus the
// value-level requirements (both are load-bearing, both must be in the fixture).
func requiredFactsOf(decl frameFreeDeclaration) []string {
	keys := append([]string{}, decl.Locator.ObservedKeys...)
	keys = append(keys, decl.Locator.MustBeEmpty...)
	for _, want := range decl.Locator.MustEqual {
		keys = append(keys, want.Key)
	}
	return keys
}

func TestGreenFixturesSatisfyEveryFrameFreeDeclaration(t *testing.T) {
	// The structural defect this refuses: a declaration gains a required fact (or a leg
	// starts publishing one more durable key) and only the LIVE leg knows about it, so
	// the synthetic green fixtures drift and a verdict test quietly starts disagreeing
	// with the classifier - which is what happened on 2026-11-10 (five journey legs
	// classified frame_free_unproven, and the drift was found after the merge). Every
	// declared leg of every acceptance scenario must be PROVEN out of its own fixture.
	for _, scenario := range []string{scenarioPersonalDotJourney, scenarioStandaloneGoSDK, scenarioStandalonePythonSDK} {
		checks := greenFixtureFor(t, scenario)
		c := strictScenario(t, scenario, checks)
		for leg, decl := range frameFreeDeclarations[scenario] {
			legMap, ok := checks[leg].(map[string]any)
			if !ok {
				t.Fatalf("%s/%s: the green fixture carries no map for a declared frame-free leg", scenario, leg)
			}
			for _, key := range requiredFactsOf(decl) {
				if _, present := legMap[key]; !present {
					t.Fatalf("%s/%s: the fixture does not carry %q, which the declaration requires; add what the live leg publishes (never delete the requirement)", scenario, leg, key)
				}
			}
			if got := c.terminalStates[leg]; got != terminalDeclaredNoFrame {
				ev := c.frameFree[leg]
				t.Fatalf("%s/%s: fixture must classify proven, got %q (missing=%v contradicted=%v)", scenario, leg, got, ev["missing_facts"], ev["contradicted_facts"])
			}
		}
		if len(c.frameFreeUnproven) != 0 {
			t.Fatalf("%s: no declared leg of a green fixture may be unproven, got %v", scenario, c.frameFreeUnproven)
		}
		if v := overallVerdict(c.completed, c.blocked, c.failed); v != "PASS" {
			t.Fatalf("%s: the green fixture must classify PASS with the rule in force, got %s blocked=%v reasons=%v", scenario, v, c.blocked, c.blockedReasons)
		}
	}
}

func TestEveryDeclaredDurableFactIsLoadBearingInTheGreenFixture(t *testing.T) {
	// The mutation half of the same guarantee: taking a required fact away from a
	// fixture must redden the leg, for EVERY declared fact. A declaration the fixture
	// could satisfy while the fact is missing is decorative, and a decorative gate is
	// the class of defect this lane exists to close.
	for _, scenario := range []string{scenarioPersonalDotJourney, scenarioStandaloneGoSDK, scenarioStandalonePythonSDK} {
		for leg, decl := range frameFreeDeclarations[scenario] {
			for _, key := range requiredFactsOf(decl) {
				checks := greenFixtureFor(t, scenario)
				legMap, ok := checks[leg].(map[string]any)
				if !ok {
					t.Fatalf("%s/%s: fixture has no leg map", scenario, leg)
				}
				if _, present := legMap[key]; !present {
					t.Fatalf("%s/%s: fixture does not carry %q before the mutation, so this mutation proves nothing", scenario, leg, key)
				}
				delete(legMap, key)
				c := strictScenario(t, scenario, checks)
				if got := c.terminalStates[leg]; got != terminalDeclaredUnproven {
					t.Fatalf("%s/%s: deleting the required fact %q left the state %q - the fact is not load-bearing", scenario, leg, key, got)
				}
				if !slicesContains(c.frameFreeUnproven, leg) {
					t.Fatalf("%s/%s: after deleting %q the leg must appear in frame_free_unproven_legs, got %v", scenario, leg, key, c.frameFreeUnproven)
				}
				if v := overallVerdict(c.completed, c.blocked, c.failed); v == "PASS" {
					t.Fatalf("%s/%s: PASS while the required fact %q is absent", scenario, leg, key)
				}
			}
		}
	}
}
