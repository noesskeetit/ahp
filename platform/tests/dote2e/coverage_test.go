package dote2e_test

// Self-declared coverage (director directive 2026-11-10: "the artifact must say
// this itself, not the director's report"). A PASS verdict means one precise
// thing: every leg reached its terminal with its own gates holding. It does NOT
// mean the personal-Dot requirements of the source PDF are fully covered.
//
// The claims below are DERIVED from the checks map at record time: a gate is a
// predicate over what the leg actually recorded, so a leg that stops recording a
// sub-check degrades to partial/not_covered instead of silently reading "done".
// The gaps (not_proven_in_this_run / not_covered_areas) are written here, in the
// lane, so no downstream report has to guess them.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type coverageClaim struct {
	Requirement string   `json:"requirement"`
	Leg         string   `json:"leg"`
	Status      string   `json:"status"` // proven | partial | not_covered
	GatesHeld   []string `json:"gates_held"`
	GatesUnmet  []string `json:"gates_unmet"`
	NotProven   []string `json:"not_proven_in_this_run"`
}

// coverageGate describes one recorded predicate: it returns a human-readable
// outcome and whether the predicate held.
type coverageGate struct {
	describe string
	holds    func(leg map[string]any) (bool, string)
}

func legBoolIs(key string, want bool) func(map[string]any) (bool, string) {
	return func(leg map[string]any) (bool, string) {
		value, present := leg[key].(bool)
		if !present {
			return false, fmt.Sprintf("%s: not recorded", key)
		}
		return value == want, fmt.Sprintf("%s=%v (required %v)", key, value, want)
	}
}

func legIntAtLeast(key string, want int) func(map[string]any) (bool, string) {
	return func(leg map[string]any) (bool, string) {
		switch value := leg[key].(type) {
		case int:
			return value >= want, fmt.Sprintf("%s=%d (required >=%d)", key, value, want)
		case int64:
			return value >= int64(want), fmt.Sprintf("%s=%d (required >=%d)", key, value, want)
		case float64:
			return int(value) >= want, fmt.Sprintf("%s=%v (required >=%d)", key, value, want)
		default:
			return false, fmt.Sprintf("%s: not recorded", key)
		}
	}
}

func legEqualsOther(a, b string) func(map[string]any) (bool, string) {
	return func(leg map[string]any) (bool, string) {
		left, _ := leg[a].(string)
		right, _ := leg[b].(string)
		if left == "" || right == "" {
			return false, fmt.Sprintf("%s/%s: not recorded", a, b)
		}
		return left == right, fmt.Sprintf("%s==%s (%s=%s)", a, b, a, truncDigest(left))
	}
}

// legListEmpty gates on a RECORDED list being empty, in both shapes the same value
// has in-process ([]string) and after a JSON round trip ([]any). A missing key is
// "not recorded", which fails the gate: a claim may not survive the disappearance
// of its own evidence.
func legListEmpty(key string) func(map[string]any) (bool, string) {
	return func(leg map[string]any) (bool, string) {
		value, present := leg[key]
		if !present {
			return false, fmt.Sprintf("%s: not recorded", key)
		}
		list := legStrings(value)
		if list == nil {
			return false, fmt.Sprintf("%s: recorded in a shape this gate cannot read", key)
		}
		return len(list) == 0, fmt.Sprintf("%s empty=%v (%d recorded)", key, len(list) == 0, len(list))
	}
}

// legIntExactly gates on an exact recorded count. Used where "at least" would let a
// sentinel value through: the isolation leg records -1 when the durable grant probe
// itself failed, and missing evidence must never read as "zero rows".
func legIntExactly(key string, want int) func(map[string]any) (bool, string) {
	return func(leg map[string]any) (bool, string) {
		switch value := leg[key].(type) {
		case int:
			return value == want, fmt.Sprintf("%s=%d (required %d)", key, value, want)
		case int64:
			return int(value) == want, fmt.Sprintf("%s=%d (required %d)", key, value, want)
		case float64:
			return int(value) == want, fmt.Sprintf("%s=%v (required %d)", key, value, want)
		default:
			return false, fmt.Sprintf("%s: not recorded", key)
		}
	}
}

// legStringIs gates on a recorded string being exactly the value the mechanism owes.
// It exists for the owner-control trap, where any other answer means a gate was
// removed and the artifact would credit a fabricated refusal.
func legStringIs(key, want string) func(map[string]any) (bool, string) {
	return func(leg map[string]any) (bool, string) {
		value, present := leg[key].(string)
		if !present {
			return false, fmt.Sprintf("%s: not recorded", key)
		}
		return value == want, fmt.Sprintf("%s=%q (required %q)", key, value, want)
	}
}

func legNonEmpty(key string) func(map[string]any) (bool, string) {
	return func(leg map[string]any) (bool, string) {
		value, _ := leg[key].(string)
		return value != "", fmt.Sprintf("%s recorded=%v", key, value != "")
	}
}

func truncDigest(value string) string {
	if len(value) > 12 {
		return value[:12] + "..."
	}
	return value
}

// journeyCoverageSpec is the honest per-leg statement of what the journey proves.
// scopeFullyCovered says whether the gates cover the WHOLE requirement; when it
// is false the claim can only ever be "partial", however green the leg is.
type journeyCoverageSpec struct {
	requirement       string
	leg               string
	gates             []coverageGate
	scopeFullyCovered bool
	notProven         []string
	// notProvenFromLeg names a RECORDED list on the leg whose entries join the gap
	// statement at claim time. It exists so a leg that silently stops attempting a
	// kind of resource cannot widen its own coverage by forgetting to mention it:
	// the gap is derived from what the run actually declared, not from this table.
	notProvenFromLeg string
}

func journeyCoverageSpecs() []journeyCoverageSpec {
	return []journeyCoverageSpec{
		{requirement: "the Dot turn is a task that carries a grant and a token", leg: "j1_task_grant_token",
			gates:             []coverageGate{{"a grant row exists for the task and context", legIntAtLeast("grant_rows", 1)}},
			scopeFullyCovered: false,
			notProven: []string{"the grant's scopes are not compared with the scopes the task asked for",
				"the token's binding to that grant row and its expiry are not asserted"}},
		{requirement: "the Dot runs from the pinned AgentConfig revision", leg: "j2_config_profile_pin",
			gates:             []coverageGate{{"the pin returned a revision", legIntAtLeast("config_revision", 1)}},
			scopeFullyCovered: false,
			notProven: []string{"the pinned revision is not compared with the revision this run published",
				"the runtime profile of the pinned config is not asserted (only the pin call succeeded)"}},
		{requirement: "a real Qwen answer through the Model Gateway", leg: "j3_model_real_qwen",
			gates: []coverageGate{{"a durable COMPLETED generation row exists", legIntAtLeast("durable_rows", 1)},
				{"the answering model is named", legNonEmpty("model")}},
			scopeFullyCovered: false,
			notProven: []string{"the completion text is not compared with the request: a COMPLETED row proves the gateway and the model ran, not that the answer matched",
				"token/usage accounting is not asserted by this leg"}},
		{requirement: "an unapproved tool call is held, and after approval the owner READS the result back", leg: "j4_tool_hitl_result",
			gates: []coverageGate{{"the public GetResult answered 200", legIntAtLeast("get_result_http", 200)},
				{"the result carries this run's note", legBoolIs("result_carries_note", true)}},
			scopeFullyCovered: true,
			notProven:         []string{"the approved result reaching the NEXT model turn is outside this leg (DEF-DOTE2E-1 boundary)"}},
		{requirement: "the personal memory of this Dot records a fact and re-tells it", leg: "j5_memory",
			gates:             []coverageGate{{"the code word came back from the public Search", legBoolIs("code_word_found", true)}},
			scopeFullyCovered: true,
			notProven: []string{"bank-level durable rows (units/chunks/kind) are not asserted by this leg",
				"the recall is proven inside the owner's personal scope; which bank row served it is not proven beyond that scope"}},
		{requirement: "a file uploads, seals and downloads byte-for-byte", leg: "j6_file_sha",
			gates: []coverageGate{{"download answered 200", legIntAtLeast("http", 200)},
				{"downloaded bytes match the expected SHA", legEqualsOther("sha256", "expected")},
				{"the sealed digest matches the expected SHA", legEqualsOther("sealed", "expected")}},
			scopeFullyCovered: true,
			notProven:         nil},
		{requirement: "history reloads on a brand-new session", leg: "j7_history_reload",
			gates:             []coverageGate{{"the same run reloads over a new connection and token", legBoolIs("same_run", true)}},
			scopeFullyCovered: false,
			notProven:         []string{"the reloaded transcript CONTENT is not compared with what was written; run identity reloads, message-level equality is unproven here"}},
		{requirement: "an unsigned HITL effect never executes", leg: "j8_hitl_cancel",
			gates: []coverageGate{{"the call is still held on a second attempt", legBoolIs("held_again", true)},
				{"the store carries no effect", legBoolIs("no_effect_in_store", true)}},
			scopeFullyCovered: false,
			notProven:         []string{"an explicit Cancel call by the approver is not exercised; only the never-executes-without-approval property is proven"}},
		{requirement: "a second user is denied the first user's data", leg: "j9_two_user_isolation",
			gates: []coverageGate{
				{"foreign artifact download refused with a contract code", legBoolIs("artifact_denied", true)},
				{"foreign memory search returns nothing of the owner (refused, or empty where the contract deliberately answers empty)", legBoolIs("memory_empty_or_denied", true)},
				{"foreign GetRun refused with a contract code", legBoolIs("run_denied", true)},
				{"no owner resource reached the second user", legListEmpty("leaked_resources")},
				{"every attempted kind answered with a contract refusal or a contract-empty", legListEmpty("unproven_resources")},
				{"the stranger holds no grant row in the owner's tenant (-1 means the durable probe failed)", legIntExactly("foreign_subject_grant_rows", 0)},
				{"the breadth attempted is breadth the requirement actually asks about", legIntAtLeast("resources_attempted", 12)},
				{"the denials are explicit contract refusals, not merely empty payloads", legIntAtLeast("denied_resource_count", 8)},
				{"the owner-control trap stayed unasked, so no dead route is credited as a denial",
					legStringIs("owner_control_sentinel", "not_attempted")},
				{"the owner-control trap actually ran: a guard whose default is success can be deleted",
					legBoolIs("owner_control_sentinel_ran", true)}},
			scopeFullyCovered: false,
			notProvenFromLeg:  "not_attempted_resources",
			notProven: []string{
				"State run-scoped KV and checkpoints are not probed: their writes require a workload fence lease (ClaimRun is a workload-face route, internal/stateapi/stateapi.go:135) that this lane's task credential cannot hold, so no owner KV/checkpoint object can be created to probe",
				"Space turn objects (GetTurn, run history of a turn) belong to the browser-turn leg j10 and are not probed against the stranger here",
				"Memory Read/Settings/Clear against an owner item are not probed: this lane creates no owner settings or Read fixture",
				"workspace, compute and trigger objects are not exercised by the journey at all (see not_covered_areas)",
				"a stranger refused on the human approval route is recorded as denied_by_authority and NOT counted as isolation: its own principal lacks tools:approve, and the wire cannot separate that from an owner-binding refusal"}},
		{requirement: "the browser Dot turn reaches a completed terminal with a real model answer", leg: "j10_browser_turn_def1_branch",
			gates: []coverageGate{{"the durable terminal frame is done", func(leg map[string]any) (bool, string) {
				value, _ := leg["terminal_status"].(string)
				return value == "done", fmt.Sprintf("terminal_status=%q (required \"done\")", value)
			}}, {"the expected JOURNEY-TURN-OK marker was found in the answer", legBoolIs("answer_found", true)},
				{"the marker text is recorded", legNonEmpty("answer_marker")}},
			scopeFullyCovered: true,
			notProven:         []string{"compute isolation behind the runner is not claimed by this leg (see not_covered_areas)"}},
		{requirement: "the human reads their own artifact through the browser face", leg: "j11_human_artifact_read",
			gates:             []coverageGate{{"the browser face answered 200", legIntAtLeast("http", 200)}},
			scopeFullyCovered: false,
			notProven:         []string{"content equality of the human download is RECORDED (bytes_match_sealed) but not gated by this leg: the human face proving byte-identical content is unproven until the gate is promoted"}},
	}
}

func journeyCoverageClaims(checks map[string]any) []coverageClaim {
	claims := []coverageClaim{}
	failedTags, _ := checks["failed_legs"].([]string)
	for _, spec := range journeyCoverageSpecs() {
		claim := coverageClaim{Requirement: spec.requirement, Leg: spec.leg,
			GatesHeld: []string{}, GatesUnmet: []string{},
			NotProven: append([]string{}, spec.notProven...)}
		leg, present := checks[spec.leg].(map[string]any)
		if !present {
			claim.Status = "not_covered"
			claim.GatesUnmet = []string{"leg not recorded by this run"}
			claims = append(claims, claim)
			continue
		}
		if tag := contradictingFailedTag(spec.leg, failedTags); tag != "" {
			claim.Status = "not_covered"
			claim.GatesUnmet = []string{fmt.Sprintf("failed_legs records %q for this leg: contradictory evidence is never credited as coverage", tag)}
			claims = append(claims, claim)
			continue
		}
		status, _ := leg["status"].(string)
		if status != "PASS" {
			claim.Status = "not_covered"
			reason, _ := leg["failure_reason"].(string)
			if reason == "" {
				reason, _ = leg["note"].(string)
			}
			claim.GatesUnmet = []string{fmt.Sprintf("leg status=%s%s", status,
				func() string {
					if reason != "" {
						return "; " + reason
					}
					return ""
				}())}
			claims = append(claims, claim)
			continue
		}
		allHeld := true
		for _, gate := range spec.gates {
			held, described := gate.holds(leg)
			described = gate.describe + ": " + described
			if held {
				claim.GatesHeld = append(claim.GatesHeld, described)
			} else {
				claim.GatesUnmet = append(claim.GatesUnmet, described)
				allHeld = false
			}
		}
		switch {
		case !allHeld:
			claim.Status = "not_covered"
		case spec.scopeFullyCovered:
			claim.Status = "proven"
		default:
			claim.Status = "partial"
		}
		// Kinds the leg itself declared untried join the gap statement at claim time,
		// so a leg cannot widen its coverage by forgetting to mention them: the gap is
		// derived from what this run recorded.
		if spec.notProvenFromLeg != "" {
			for _, resource := range legStrings(leg[spec.notProvenFromLeg]) {
				claim.NotProven = append(claim.NotProven, fmt.Sprintf(
					"%s: recorded as not attempted by this run, so nothing is claimed about it", resource))
			}
		}
		claims = append(claims, claim)
	}
	return claims
}

// journeyNotCoveredAreas names requirement areas the journey does not exercise at
// all, and where the lane's evidence for them actually lives.
func journeyNotCoveredAreas(scope map[string]any) []map[string]any {
	text := func(key string) string { value, _ := scope[key].(string); return value }
	return []map[string]any{
		{"area": "a2a_delegation",
			"status_by_journey":    "not_covered",
			"why":                  "no journey leg posts to /a2a/public or /a2a/internal; the journey proves the Dot turn, not delegation",
			"stand_fact_recorded":  fmt.Sprintf("the agent process was started and answered /readyz (deployment %q under the root single-consumer rule); process readiness is not delegation behaviour", text("a2a_deployment")),
			"where_evidence_lives": "scenario delegation-perimeter - whose own artifact records delegation_round_trip_inside_the_turn as BLOCKED (DEF-DOTE2E-1), so no artifact in this lane proves delegation end-to-end"},
		{"area": "compute_isolation",
			"status_by_journey":    "not_covered",
			"why":                  fmt.Sprintf("compute_profile=%s: the mock profile is accepted for the prototype by customer decision and proves nothing about isolation", text("compute_profile")),
			"where_evidence_lives": "no artifact in this lane claims it"},
		{"area": "memory_bank_durable_rows",
			"status_by_journey":    "partial",
			"why":                  fmt.Sprintf("j5 proves the public round trip against backend %s; it does not read bank rows (units/chunks)", text("hindsight_url")),
			"where_evidence_lives": "scenario memory-owner-key-scope for scope/key separation; reports/mac-director/dot-j5-root-cause-20261109.md for bank-level diagnosis"},
		{"area": "text_extraction_parser",
			"status_by_journey":    "not_covered",
			"why":                  "this journey does not parse text; the standalone Go scenario independently requires Linux extraction/index readiness, a durable cache row and exact-version text",
			"where_evidence_lives": "scenario custom-agent-standalone-go (text_extraction_gate)"},
		{"area": "restart_fault_injection_and_load",
			"status_by_journey":    "not_covered",
			"why":                  "the journey is a single functional pass; it never restarts a component, injects a fault or runs concurrent load",
			"where_evidence_lives": "scenarios recovery-durable-state and model-fault-isolation; load behaviour belongs to the profiling lane, not to this artifact"},
	}
}

func standComponents(standDir string) map[string]any {
	raw, err := os.ReadFile(filepath.Join(standDir, "processes.json"))
	if err != nil {
		return map[string]any{"status": "unknown", "why": "processes.json not readable at record time"}
	}
	var processes map[string]map[string]any
	if err := json.Unmarshal(raw, &processes); err != nil {
		return map[string]any{"status": "unknown", "why": "processes.json unparsable"}
	}
	roles := []string{}
	for role := range processes {
		roles = append(roles, role)
	}
	return map[string]any{"status": "started_and_ready", "roles": roles,
		"note": "start.py fails unless every role answers its readiness endpoint; readiness is a stand fact, not a behaviour proof"}
}

func journeyCoverage(checks map[string]any, scope map[string]any, standDir string) map[string]any {
	return map[string]any{
		"journey":      "personal-dot-journey",
		"reading_rule": "verdict PASS means every leg reached its terminal with its recorded gates holding. It does NOT mean the source PDF's personal-Dot requirements are covered: read claims[].status, claims[].not_proven_in_this_run and not_covered_areas before making any coverage statement.",
		// The decision the band makes explicitly (director/reviewer directive 2026-11-10,
		// M3): self-declared coverage is ADVISORY. It is a statement the lane makes about
		// its own evidence, computed from what this run recorded, and it is never an input
		// to the verdict - the verdict is derived from the leg gates by overallVerdict and
		// cannot be raised or lowered by a claim. The safety properties are therefore
		// enforced where the verdict IS derived: a leak, a stranger grant row in the
		// owner's tenant, a failed owner-control trap or a missing core refusal fails the
		// leg, and a failed leg fails the verdict. Pinned by
		// TestCoverageClaimsAreAdvisoryAndCannotMoveTheVerdict; a reader must never treat
		// claims[].status as a gate that was enforced.
		"claims_authority":  "advisory: computed from recorded evidence, never read by the verdict (overallVerdict reads leg gates only). Enforcement lives in the leg gates.",
		"claims":            journeyCoverageClaims(checks),
		"not_covered_areas": journeyNotCoveredAreas(scope),
		"stand_components":  standComponents(standDir),
	}
}

// --- pure tests (no Docker, no PG) ---

func journeyGreenChecks() map[string]any {
	checks := journeyChecks(map[string]any{"status": "PASS", "terminal_status": "done",
		"answer_found": true, "answer_marker": "JOURNEY-TURN-OK 010101", "turn": "turn-1"})
	checks["j1_task_grant_token"] = map[string]any{"status": "PASS", "grant_rows": 1}
	checks["j2_config_profile_pin"] = map[string]any{"status": "PASS", "config_revision": 6}
	// The live leg records the identity it read back from the generation row
	// (journey_test.go: "model": generation.Model), and the claim gate only asks that the
	// field be non-empty (legNonEmpty("model")). A green fixture therefore names a fixture
	// label, not the routed identity: stating it here would make this lane an unaccounted
	// copy of an identity that is declared once in infra/model/providers.json and pinned by
	// the ledger in internal/model/routing/config_copies_test.go.
	checks["j3_model_real_qwen"] = map[string]any{"status": "PASS", "durable_rows": 1, "generation": "gen-1", "model": "generation-row model (fixture)"}
	checks["j4_tool_hitl_result"] = map[string]any{"status": "PASS", "get_result_http": 200, "result_carries_note": true}
	checks["j5_memory"] = map[string]any{"status": "PASS", "code_word_found": true,
		"operation_states": []string{"PENDING", "COMPLETED"}}
	checks["j6_file_sha"] = map[string]any{"status": "PASS", "http": 200, "artifact": "art-1",
		"sha256": "aaa111", "sealed": "aaa111", "expected": "aaa111"}
	checks["j7_history_reload"] = map[string]any{"status": "PASS", "same_run": true}
	checks["j8_hitl_cancel"] = map[string]any{"status": "PASS", "held_again": true, "no_effect_in_store": true}
	// A green j9 fixture that mirrors what the leg records when isolation holds: the
	// three original denials now carry contract codes, four kinds answer empty BY
	// CONTRACT (recorded as a fact, never booked as a denial), the human approval
	// route is a refusal of the stranger's OWN authority (not isolation proof), and
	// two kinds could not be attempted at all - which joins not_proven, not the pass.
	checks["j9_two_user_isolation"] = map[string]any{"status": "PASS",
		"artifact_denied": true, "artifact_http": 404, "artifact_outcome": "denied",
		"run_denied": true, "run_http": 404, "run_outcome": "denied",
		"memory_empty_or_denied": true, "memory_outcome": "empty_by_scope",
		"resources_attempted":   14,
		"denied_resource_count": 9,
		"denied_resources": []string{"artifact_bytes", "run_record", "conversation_thread",
			"thread_durable_stream", "tool_operation_history", "tool_result_note",
			"agent_config", "grant_delegate_into_owner_tree", "grant_cancel_owner_tree"},
		"leaked_resources":   []string{},
		"unproven_resources": []string{},
		"empty_by_scope_resources": []string{"memory_personal_search", "run_list_of_owner_thread",
			"chat_list", "hitl_worklist"},
		// The two keys the frame-free declaration requires and the live leg publishes
		// (isolation_resources_test.go:653-654): the refusal is judged by the CONTRACT
		// CODE it answered, and `probes` carries one row per resource in the shape
		// isoProbe.recorded() writes - resource / operation / http / contract_code /
		// outcome. Without them the leg attests isolation by asserting it.
		"contract_codes": []string{"NOT_FOUND", "FORBIDDEN"},
		"probes": []map[string]any{
			{"resource": "artifact_bytes", "operation": "artifacts.Get", "http": 404,
				"contract_code": "NOT_FOUND", "outcome": "denied"},
			{"resource": "run_record", "operation": "state.GetRun", "http": 404,
				"contract_code": "NOT_FOUND", "outcome": "denied"},
			{"resource": "grant_delegate_into_owner_tree", "operation": "grants.Delegate", "http": 403,
				"contract_code": "FORBIDDEN", "outcome": "denied"},
		},
		"authority_refusals":         []string{"hitl_approve_by_stranger"},
		"not_attempted_resources":    []string{"memory_item_invalidate", "agent_profile"},
		"owner_control_sentinel":     "not_attempted",
		"owner_control_sentinel_ran": true,
		"foreign_subject_grant_rows": 0, "foreign_grant_task": "task-fixture-stranger",
		"foreign_subject": "dot-stranger", "foreign_tenant_mismatch": true}
	checks["j11_human_artifact_read"] = map[string]any{"status": "PASS", "http": 200, "bytes_match_sealed": true,
		"sealed_sha256": "aaa111", "received_sha256": "aaa111"}
	return checks
}

func claimByLeg(claims []coverageClaim, leg string) coverageClaim {
	for _, claim := range claims {
		if claim.Leg == leg {
			return claim
		}
	}
	return coverageClaim{Status: "missing"}
}

func TestJourneyCoverageClaimsAreDerivedFromEvidence(t *testing.T) {
	claims := journeyCoverageClaims(journeyGreenChecks())
	if len(claims) != 11 {
		t.Fatalf("one claim per journey leg expected, got %d", len(claims))
	}
	// Legs whose recorded gates ARE the whole requirement.
	for _, leg := range []string{"j4_tool_hitl_result", "j5_memory", "j6_file_sha", "j10_browser_turn_def1_branch"} {
		if got := claimByLeg(claims, leg).Status; got != "proven" {
			t.Fatalf("%s must be proven on a green stand, got %s (unmet=%v)", leg, got, claimByLeg(claims, leg).GatesUnmet)
		}
	}
	// Legs the director named as partially proven "it did not crash" proofs: they may
	// never read as full coverage, however green they are.
	for _, leg := range []string{"j1_task_grant_token", "j2_config_profile_pin", "j3_model_real_qwen",
		"j7_history_reload", "j8_hitl_cancel", "j9_two_user_isolation", "j11_human_artifact_read"} {
		claim := claimByLeg(claims, leg)
		if claim.Status != "partial" {
			t.Fatalf("%s must read partial (gates hold but the requirement is broader), got %s", leg, claim.Status)
		}
		if len(claim.NotProven) == 0 {
			t.Fatalf("%s must name what it does not prove", leg)
		}
	}
	// A gate is a predicate over RECORDED data: drop the recorded sub-check and the
	// claim degrades - it cannot stay "proven" on memory of the last run.
	checks := journeyGreenChecks()
	delete(checks["j6_file_sha"].(map[string]any), "sha256")
	if got := claimByLeg(journeyCoverageClaims(checks), "j6_file_sha").Status; got != "not_covered" {
		t.Fatalf("j6 must degrade when its recorded evidence disappears, got %s", got)
	}
	// A failed or missing leg is not_covered, never partial.
	checks = journeyGreenChecks()
	checks["j5_memory"] = map[string]any{"status": "FAIL", "failure_reason": "code word never returned"}
	claim := claimByLeg(journeyCoverageClaims(checks), "j5_memory")
	if claim.Status != "not_covered" {
		t.Fatalf("a FAIL leg cannot be coverage, got %s", claim.Status)
	}
	if len(claim.GatesUnmet) == 0 || claim.GatesUnmet[0] == "" {
		t.Fatal("a not_covered claim must say why")
	}
	delete(checks, "j8_hitl_cancel")
	if got := claimByLeg(journeyCoverageClaims(checks), "j8_hitl_cancel").Status; got != "not_covered" {
		t.Fatalf("an unrecorded leg is not_covered, got %s", got)
	}
}

// TestJ9IsolationClaimDegradesOnLeakOrMissingEvidence pins the honesty rules of the
// widened isolation leg: a leak is never coverage, an answer that is not a contract
// refusal is never coverage, and evidence that stops being recorded cannot keep the
// claim alive. It also pins the derived gap statement - a kind the leg declared
// untried must reappear in not_proven, never be dropped.
func TestJ9IsolationClaimDegradesOnLeakOrMissingEvidence(t *testing.T) {
	claimOf := func(checks map[string]any) coverageClaim {
		return claimByLeg(journeyCoverageClaims(checks), "j9_two_user_isolation")
	}
	// Owner data reaching the stranger blocks the claim however green the rest is.
	checks := journeyGreenChecks()
	checks["j9_two_user_isolation"].(map[string]any)["leaked_resources"] = []string{"tool_result_note"}
	claim := claimOf(checks)
	if claim.Status != "not_covered" {
		t.Fatalf("a leak cannot be coverage, got %s", claim.Status)
	}
	if !strings.Contains(strings.Join(claim.GatesUnmet, "; "), "leaked_resources") {
		t.Fatalf("the leak gate must name the recorded list, got %v", claim.GatesUnmet)
	}
	// An attempted kind whose answer is neither a contract refusal nor a contract-empty
	// blocks the claim: an odd answer is not evidence of isolation.
	checks = journeyGreenChecks()
	checks["j9_two_user_isolation"].(map[string]any)["unproven_resources"] = []string{"thread_durable_stream"}
	if got := claimOf(checks).Status; got != "not_covered" {
		t.Fatalf("an unclassified answer cannot be coverage, got %s", got)
	}
	// The durable grant sentinel: a failed probe records -1 so that missing evidence
	// cannot read as "the stranger holds no grant row".
	checks = journeyGreenChecks()
	checks["j9_two_user_isolation"].(map[string]any)["foreign_subject_grant_rows"] = -1
	if got := claimOf(checks).Status; got != "not_covered" {
		t.Fatalf("a failed durable grant probe cannot read as zero rows, got %s", got)
	}
	// The owner-control trap: an answer other than "not attempted" means the stranger
	// was asked a route the platform does not mount, and its 404 would be a fabricated
	// denial - the claim degrades instead of crediting it.
	checks = journeyGreenChecks()
	checks["j9_two_user_isolation"].(map[string]any)["owner_control_sentinel"] = "denied"
	if got := claimOf(checks); got.Status != "not_covered" {
		t.Fatalf("a passed owner-control gate cannot be coverage, got %s", got.Status)
	}
	// The trap disappearing, as mutation M4 made it: the probe never ran, so the only
	// honest reading is that the owner control is unproven this run.
	checks = journeyGreenChecks()
	checks["j9_two_user_isolation"].(map[string]any)["owner_control_sentinel"] = "sentinel_did_not_run"
	checks["j9_two_user_isolation"].(map[string]any)["owner_control_sentinel_ran"] = false
	if got := claimOf(checks); got.Status != "not_covered" {
		t.Fatalf("a trap that never ran cannot certify the owner control, got %s", got.Status)
	}
	// Evidence that disappears degrades the claim, exactly like every other leg.
	for _, key := range []string{"resources_attempted", "denied_resource_count", "owner_control_sentinel",
		"owner_control_sentinel_ran",
		"leaked_resources", "unproven_resources", "foreign_subject_grant_rows",
		"artifact_denied", "run_denied", "memory_empty_or_denied"} {
		checks = journeyGreenChecks()
		delete(checks["j9_two_user_isolation"].(map[string]any), key)
		if got := claimOf(checks); got.Status != "not_covered" {
			t.Fatalf("dropping %s must degrade the claim, got %s", key, got.Status)
		}
	}
	// A kind declared untried is never laundered into the pass: it joins the gaps.
	checks = journeyGreenChecks()
	fixtureUntried := legStrings(checks["j9_two_user_isolation"].(map[string]any)["not_attempted_resources"])
	checks["j9_two_user_isolation"].(map[string]any)["not_attempted_resources"] =
		append(append([]string{}, fixtureUntried...), "state_run_kv")
	claim = claimOf(checks)
	if claim.Status != "partial" {
		t.Fatalf("an untried kind degrades the requirement statement, it does not fail the gates: got %s", claim.Status)
	}
	if !strings.Contains(strings.Join(claim.NotProven, "; "), "state_run_kv") {
		t.Fatalf("a declared-untried kind must appear in not_proven, got %v", claim.NotProven)
	}
	// And the fixture's own untried kind is stated even when every gate holds.
	if !strings.Contains(strings.Join(claim.NotProven, "; "), "memory_item_invalidate") {
		t.Fatalf("the leg's recorded not_attempted list must reach the claim, got %v", claim.NotProven)
	}
}

func TestJourneyCoverageDeclaresA2AAndMemoryLimits(t *testing.T) {
	scope := map[string]any{"a2a_deployment": "a2a", "compute_profile": "mock-microvm",
		"hindsight_url": "http://127.0.0.1:38888"}
	coverage := journeyCoverage(journeyGreenChecks(), scope, t.TempDir())
	areas, ok := coverage["not_covered_areas"].([]map[string]any)
	if !ok || len(areas) == 0 {
		t.Fatal("the artifact must name requirement areas the journey does not exercise")
	}
	found := map[string]string{}
	for _, area := range areas {
		name, _ := area["area"].(string)
		status, _ := area["status_by_journey"].(string)
		found[name] = status
	}
	if found["a2a_delegation"] != "not_covered" {
		t.Fatalf("A2A must be declared not_covered by the journey, got %q", found["a2a_delegation"])
	}
	if found["compute_isolation"] != "not_covered" {
		t.Fatalf("mock compute isolation must never read as covered, got %q", found["compute_isolation"])
	}
	if found["memory_bank_durable_rows"] != "partial" {
		t.Fatalf("memory must be declared partial (round trip yes, bank rows no), got %q", found["memory_bank_durable_rows"])
	}
	if rule, _ := coverage["reading_rule"].(string); rule == "" {
		t.Fatal("the coverage block must carry a reading rule so PASS is not read as coverage")
	}
	if components, ok := coverage["stand_components"].(map[string]any); !ok || components["status"] != "unknown" {
		t.Fatal("a stand whose processes.json is unreadable must report unknown, not pretend")
	}
}

func TestContradictoryFailedTagDowngradesPassToBlocked(t *testing.T) {
	// The clobber class: a leg writes PASS while the scenario's fail() recorded a
	// tag for the same leg (historically j1/j2/j3/j5 wrote PASS unconditionally
	// after a FAIL branch). The generator must refuse to count such a leg.
	checks := journeyGreenChecks()
	checks["failed_legs"] = []string{"j1"}
	if st := claimByLeg(journeyCoverageClaims(checks), "j1_task_grant_token").Status; st == "proven" || st == "partial" {
		t.Fatalf("coverage must not credit a leg the scenario marked failed, got %s", st)
	}
	c := classifyLegs("personal-dot-journey", checks)
	for _, name := range c.completed {
		if name == "j1_task_grant_token" {
			t.Fatal("regression: a leg contradicted by failed_legs was counted completed")
		}
	}
	if reason := c.blockedReasons["j1_task_grant_token"]; reason == "" {
		t.Fatal("the contradictory leg must carry a reason")
	}
	if verdict := overallVerdict(c.completed, c.blocked, c.failed); verdict == "PASS" {
		t.Fatal("regression: PASS while failed_legs contradicts a leg")
	}
}

func TestRecordedVerdictCannotBeMoreOptimisticThanLegs(t *testing.T) {
	// record() used to write the caller's verdict verbatim, so a scenario could
	// sign PASS next to a BLOCKED leg. The artifact must carry the worse verdict
	// and keep the requested one visible.
	evidence := t.TempDir()
	stand := &instance{Dir: t.TempDir(), Evidence: evidence, Scope: map[string]any{}}
	checks := map[string]any{
		"some_leg":    map[string]any{"status": "PASS", "terminal_status": "done"},
		"blocked_leg": map[string]any{"status": "BLOCKED", "blocked_by": "DEF-SOMETHING"},
	}
	path := stand.record(t, "coverage-probe-scenario", "PASS", checks)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	var artifact map[string]any
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatalf("parse artifact: %v", err)
	}
	if artifact["verdict"] != "BLOCKED" {
		t.Fatalf("the artifact must carry the verdict its legs prove, got %v", artifact["verdict"])
	}
	if artifact["requested_verdict"] != "PASS" {
		t.Fatalf("the requested verdict must stay visible, got %v", artifact["requested_verdict"])
	}
	if conflict, _ := artifact["verdict_conflict"].(string); conflict == "" {
		t.Fatal("the disagreement must be stated, not silently corrected")
	}
	// A caller may always be MORE pessimistic than the legs (FAIL stays FAIL).
	path = stand.record(t, "coverage-probe-scenario", "FAIL", map[string]any{
		"some_leg": map[string]any{"status": "PASS", "terminal_status": "done"}})
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatalf("parse artifact: %v", err)
	}
	if artifact["verdict"] != "FAIL" {
		t.Fatalf("a declared FAIL must never be softened by the generator, got %v", artifact["verdict"])
	}
}

// TestCoverageClaimsAreAdvisoryAndCannotMoveTheVerdict pins the M3 decision instead of
// leaving it as prose: coverage claims are advisory in BOTH directions - they cannot
// rescue a verdict, and a degraded claim leaves the verdict untouched - while the
// isolation properties stay load-bearing where the verdict is actually computed.
func TestCoverageClaimsAreAdvisoryAndCannotMoveTheVerdict(t *testing.T) {
	green, _, _, _ := journeyVerdict(journeyGreenChecks())
	if green != "PASS" {
		t.Fatalf("the green fixture must classify PASS before any mutation, got %s", green)
	}
	// A degraded claim does not move the verdict: advisory means advisory.
	checks := journeyGreenChecks()
	checks["j9_two_user_isolation"].(map[string]any)["unproven_resources"] = []string{"memory_item_invalidate"}
	checks["j9_two_user_isolation"].(map[string]any)["owner_control_sentinel"] = "denied"
	claim := claimByLeg(journeyCoverageClaims(checks), "j9_two_user_isolation")
	if claim.Status != "not_covered" {
		t.Fatalf("the claim must report the contradiction honestly, got %s", claim.Status)
	}
	after, _, _, _ := journeyVerdict(checks)
	if after != green {
		t.Fatalf("a coverage claim must not move the verdict: %s -> %s", green, after)
	}
	// The same facts, recorded as a failed leg, DO move it: that is where enforcement
	// lives, and it is why calling the claim advisory is not a softening.
	checks = journeyGreenChecks()
	checks["j9_two_user_isolation"] = map[string]any{"status": "FAIL",
		"failure_reason":   "owner data reached the second user (leaked_resources)",
		"leaked_resources": []string{"chat_list"}}
	verdict, _, _, failed := journeyVerdict(checks)
	if verdict == "PASS" {
		t.Fatal("a recorded isolation breach must fail the verdict, not just the claim")
	}
	found := false
	for _, leg := range failed {
		if leg == "j9_two_user_isolation" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the failed leg must be named in the verdict's failure list, got %v", failed)
	}
}
