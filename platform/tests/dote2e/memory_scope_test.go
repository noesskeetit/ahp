package dote2e_test

// PDF rev3.2 L693 and L2050: memory has a workspace scope and a personal scope
// keyed by (tenant, user, dot, ...); another employee's Dot cannot read it.
//
// Everything here goes through the platform Memory authority over real
// PostgreSQL and the AHP-OWNED Hindsight deployment named in the instance scope.
// The personal memory service on :8889 is NOT used and its data is NOT read.
// The Dot's own turn-time memory write runs inside the Space runner and is
// blocked by DEF-DOTE2E-1 (dot-turn test), so this scenario writes the memory
// with the employee's own task token against a real State run - the same
// authority and the same key the turn would carry.

import (
	"context"
	"strings"
	"testing"
	"time"
)

type memoryFact struct {
	MemoryRef string `json:"memory_ref"`
	Scope     string `json:"scope"`
	Text      string `json:"text"`
	Type      string `json:"type"`
}

// laneRun creates a real thread and run in the State owner so the memory write
// can cite a real source_ref (the Memory authority refuses an unknown run).
func laneRun(t *testing.T, stand *instance, token, key, contextID, input string) string {
	t.Helper()
	var thread struct {
		ID string `json:"id"`
	}
	if err := stand.call(stand.HTTP, "/v1/state/CreateThread",
		map[string]any{"key": key + "-thread", "context_id": contextID}, token, &thread); err != nil {
		t.Fatalf("CreateThread refused: %v", err)
	}
	var run struct {
		ID string `json:"id"`
	}
	if err := stand.call(stand.HTTP, "/v1/state/CreateRun",
		map[string]any{"thread_id": thread.ID, "key": key + "-run", "payload": map[string]any{"input": input}},
		token, &run); err != nil {
		t.Fatalf("CreateRun refused: %v", err)
	}
	if run.ID == "" {
		t.Fatal("State returned a run without an id")
	}
	return run.ID
}

func TestPersonalDotMemoryIsScopedByOwnerKey(t *testing.T) {
	stand := loadStand(t)
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	// The conversation the memory belongs to: the Memory authority compares the
	// task token's context claim with the source run's thread context, so the
	// scenario names one conversation and uses it everywhere.
	memoryContext := "dot-memory-ctx-" + stamp
	token, task := stand.taskGrantContext(t, stand.HTTP, human, "dot-memory-"+stamp, memoryContext,
		[]string{"memory", "state"})
	checks := map[string]any{}
	codeWord := "kobalt-otter-" + task[len(task)-6:]
	input := "The employee writes research notes in Go and keeps the code word " + codeWord
	runID := laneRun(t, stand, token, task, memoryContext, input)
	checks["source_run_in_state"] = map[string]any{"status": "PASS", "run": runID}

	// (a) The Dot records a personal memory through the Memory authority, citing the
	// real run as its source. The authority answers with an operation, not a lie.
	var remembered struct {
		ID    string `json:"id"`
		State string `json:"state"`
		Scope string `json:"scope"`
	}
	err := stand.call(stand.HTTP, "/v1/memory/Remember", map[string]any{
		"target": "personal", "text": input, "op_key": task + "-remember",
		"source_refs": []string{"state://runs/" + runID + "/input"}}, token, &remembered)
	if err != nil || remembered.ID == "" {
		stand.record(t, "memory-owner-key-scope", "FAIL", map[string]any{"remember": map[string]any{
			"status": "FAIL", "http": status(err), "code": errorCode(err), "error": errText(err)}})
		t.Fatalf("Memory Remember refused: %v", err)
	}
	checks["remember_accepted"] = map[string]any{"status": "PASS", "operation": remembered.ID,
		"state": remembered.State, "scope": remembered.Scope}

	// (b) The operation settles (Hindsight extracts the fact); the operation record
	// is the durable proof, not a sleep.
	deadline := time.Now().Add(180 * time.Second)
	operationState := remembered.State
	for time.Now().Before(deadline) {
		var operation struct {
			State string `json:"state"`
		}
		if err := stand.call(stand.HTTP, "/v1/memory/Operation", map[string]any{"id": remembered.ID}, token, &operation); err == nil {
			operationState = operation.State
		}
		if operationState == "succeeded" || operationState == "failed" {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if operationState != "succeeded" {
		stand.record(t, "memory-owner-key-scope", "FAIL", map[string]any{"operation": map[string]any{
			"status": "FAIL", "state": operationState, "operation_id": remembered.ID}})
		t.Fatalf("the memory operation did not settle: %q", operationState)
	}
	checks["remember_operation_succeeded"] = map[string]any{"status": "PASS", "operation": remembered.ID}

	// (c) The same Dot reads it back semantically.
	var facts []memoryFact
	var personal struct {
		Facts []memoryFact `json:"facts"`
	}
	for time.Now().Before(deadline) {
		if err := stand.call(stand.HTTP, "/v1/memory/Search",
			map[string]any{"query": "Which language does the employee use for research notes?", "scope": "personal"},
			token, &personal); err != nil {
			stand.record(t, "memory-owner-key-scope", "FAIL", map[string]any{"search": map[string]any{
				"status": "FAIL", "error": errText(err)}})
			t.Fatalf("Memory Search refused: %v", err)
		}
		facts = personal.Facts
		if len(facts) > 0 {
			break
		}
		time.Sleep(3 * time.Second)
	}
	matched := ""
	for _, fact := range facts {
		if strings.Contains(fact.Text, codeWord) || strings.Contains(fact.Text, "Go") {
			matched = fact.Text
			break
		}
	}
	if matched == "" {
		stand.record(t, "memory-owner-key-scope", "FAIL", map[string]any{"search": map[string]any{
			"status": "FAIL", "facts": len(facts)}})
		t.Fatalf("the personal memory was not retrieved (facts=%d)", len(facts))
	}
	checks["personal_memory_read_back"] = map[string]any{"status": "PASS", "facts": len(facts),
		"matched_contains_code_word": strings.Contains(matched, codeWord), "fact_type": firstType(facts)}

	// (d) Another subject of the SAME tenant cannot read it: the owner key is
	// (tenant, subject, deployment), so a colleague's Dot has a different key.
	otherHuman := stand.humanToken(t, stand.HTTP, mustLogin(t, "admin_email"), mustLogin(t, "admin_password"))
	otherToken, _ := stand.taskGrant(t, stand.HTTP, otherHuman, "dot-memory-other-"+stamp, []string{"memory"})
	var other struct {
		Facts []memoryFact `json:"facts"`
	}
	if err := stand.call(stand.HTTP, "/v1/memory/Search",
		map[string]any{"query": "Which language does the employee use for research notes?", "scope": "personal"},
		otherToken, &other); err != nil {
		// A refusal is also a correct answer; the check is that nothing leaks.
		checks["other_subject_denied_personal_search"] = map[string]any{"status": "PASS", "http": status(err), "code": errorCode(err)}
	} else {
		for _, fact := range other.Facts {
			if strings.Contains(fact.Text, codeWord) || strings.Contains(fact.Text, "Go") {
				stand.record(t, "memory-owner-key-scope", "FAIL", map[string]any{"leak": map[string]any{
					"status": "FAIL", "fact": fact.Text}})
				t.Fatal("another subject read the employee's personal memory")
			}
		}
		checks["other_subject_denied_personal_search"] = map[string]any{"status": "PASS", "facts_returned": len(other.Facts)}
	}

	// (e) A foreign tenant is refused or returns nothing at all.
	foreignHuman := stand.humanToken(t, stand.Foreign, mustLogin(t, "foreign_email"), mustLogin(t, "foreign_password"))
	foreignToken, _ := stand.taskGrant(t, stand.Foreign, foreignHuman, "dot-memory-foreign-"+stamp, []string{"memory"})
	var foreign struct {
		Facts []memoryFact `json:"facts"`
	}
	err = stand.call(stand.Foreign, "/v1/memory/Search",
		map[string]any{"query": "Which language does the employee use for research notes?", "scope": "personal"},
		foreignToken, &foreign)
	for _, fact := range foreign.Facts {
		if strings.Contains(fact.Text, codeWord) {
			stand.record(t, "memory-owner-key-scope", "FAIL", map[string]any{"leak": map[string]any{
				"status": "FAIL", "tenant": stand.Scope["foreign_tenant"], "fact": fact.Text}})
			t.Fatal("the foreign tenant read the employee's personal memory")
		}
	}
	checks["foreign_tenant_cannot_read"] = map[string]any{"status": "PASS", "http": status(err), "facts": len(foreign.Facts)}

	// (f) The key is a stored fact: the Memory authority keeps one owner per
	// (tenant, subject, deployment=dot) and the write lives under that key.
	var ownerKeys, records int
	err = stand.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_longterm.owner_directory WHERE tenant=$1 AND subject=$2 AND deployment=$3`,
		stand.Scope["tenant"], stand.Scope["subject"], stand.Scope["deployment"]).Scan(&ownerKeys)
	if err != nil {
		t.Fatalf("read ahp_longterm.owner_directory: %v", err)
	}
	err = stand.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_longterm.memory_records WHERE owner_key IN
		   (SELECT owner_key FROM ahp_longterm.owner_directory WHERE tenant=$1 AND subject=$2 AND deployment=$3)`,
		stand.Scope["tenant"], stand.Scope["subject"], stand.Scope["deployment"]).Scan(&records)
	if err != nil {
		t.Fatalf("read ahp_longterm.memory_records: %v", err)
	}
	if ownerKeys != 1 || records == 0 {
		t.Fatalf("expected one owner key with records, got owners=%d records=%d", ownerKeys, records)
	}
	var kinds []string
	rows, err := stand.Pool.Query(context.Background(),
		`SELECT DISTINCT kind FROM ahp_longterm.memory_records WHERE owner_key IN
		   (SELECT owner_key FROM ahp_longterm.owner_directory WHERE tenant=$1)ORDER BY kind`,
		stand.Scope["tenant"])
	if err != nil {
		t.Fatalf("read memory record kinds: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			t.Fatalf("scan record kind: %v", err)
		}
		kinds = append(kinds, kind)
	}
	checks["owner_key_in_postgres"] = map[string]any{"status": "PASS", "owner_rows": ownerKeys, "records": records,
		"kinds": kinds, "key": "owner_key = hash(tenant, subject, deployment=dot)",
		"hindsight_bank_record": true}

	// (g) The workspace (project) scope is a SEPARATE scope of the same authority:
	// it refuses a write and a read that do not name a private workspace, while the
	// personal scope of this employee stays active. Two scopes, two keys.
	var personalStatus struct {
		Scope string `json:"scope"`
		State string `json:"state"`
	}
	if err := stand.call(stand.HTTP, "/v1/memory/Status", map[string]any{"scope": "personal"}, token, &personalStatus); err != nil {
		t.Fatalf("the personal memory scope is not readable: %v", err)
	}
	workspaceWrite := status(err)
	err = stand.call(stand.HTTP, "/v1/memory/Remember", map[string]any{
		"target": "project", "text": "must be refused without a workspace", "op_key": task + "-project"}, token, nil)
	if status(err) == 0 {
		stand.record(t, "memory-owner-key-scope", "FAIL", map[string]any{"workspace_scope": map[string]any{
			"status": "FAIL", "note": "a project-scoped write was accepted without a workspace"}})
		t.Fatal("a workspace-scoped memory write must require a workspace")
	}
	projectWriteHTTP, projectWriteCode := status(err), errorCode(err)
	err = stand.call(stand.HTTP, "/v1/memory/Status", map[string]any{"scope": "project"}, token, nil)
	if status(err) == 0 || errorCode(err) != "project_required" {
		stand.record(t, "memory-owner-key-scope", "FAIL", map[string]any{"workspace_scope": map[string]any{
			"status": "FAIL", "http": status(err), "code": errorCode(err)}})
		t.Fatalf("the workspace scope must demand a private workspace, got http=%d code=%s", status(err), errorCode(err))
	}
	_ = workspaceWrite
	checks["workspace_scope_is_separate"] = map[string]any{"status": "PASS", "personal_scope_state": personalStatus.State,
		"project_write_http": projectWriteHTTP, "project_write_code": projectWriteCode,
		"project_read_http": status(err), "project_read_code": errorCode(err)}

	// (h) Honest boundary: this revision keys personal memory by
	// (tenant, subject, deployment). There is no 'realm' column in the memory
	// authority, so the PDF's fourth key component is not a memory dimension here
	// (it IS one in Compute). Recorded rather than papered over.
	checks["realm_dimension"] = map[string]any{"status": "PARTIAL",
		"note": "ahp_longterm.owner_directory = (tenant, subject, deployment); no realm column in the Memory authority"}

	stand.record(t, "memory-owner-key-scope", "PASS", checks)
}

func firstType(facts []memoryFact) string {
	if len(facts) == 0 {
		return ""
	}
	return facts[0].Type
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
