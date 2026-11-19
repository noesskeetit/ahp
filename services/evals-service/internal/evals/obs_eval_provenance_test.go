package evals_test

// obs_eval_provenance_test.go -- the two halves of row 24 that were proved only in
// the Go structs, and therefore only as long as the code that filled them.
//
// The row demands that an eval result CARRIES its own provenance: the policy it was
// earned under (id and digest), the timestamp that makes staleness decidable, and
// the revision it is a verdict about. That is a property of the stored row -- the
// artifact a reviewer reads after the process is gone -- so it is asserted here with
// SQL against ahp_evals.verdicts, not by trusting Record's return value. The second
// leg moves "a partial run cannot certify" down to the level it happens at: the
// runner, which executes an allowlisted suite and then asks the plane to record. A
// suite that does not cover every task the policy demands must leave no PASS behind.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestObsVerdictRowCarriesPolicyDigestTimestampAndTargetRevision(t *testing.T) {
	s := store(t)
	ctx := context.Background()

	p1, err := s.SetPolicy(ctx, evals.Policy{
		ID: "obs-prov-p1", TaskSetID: "obs-prov-set", RequiredTaskIDs: []string{"obs-t1"}, MaxAgeSeconds: 3600,
	})
	if err != nil {
		t.Fatalf("SetPolicy p1: %v", err)
	}
	revision := randDigest(t)
	if _, err := s.Record(ctx, evals.RecordInput{
		AgentDigest: revision, TaskSetID: "obs-prov-set", Setup: map[string]any{"obs": "runner-config"},
		Outcome:  evals.OutcomePass,
		Tasks:    []evals.TaskResult{{TaskID: "obs-t1", Passed: true, TerminalState: evals.TerminalCompleted, Cost: 0.01}},
		PolicyID: "obs-prov-p1", TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("Record: %v", err)
	}

	// All four facts, read back from the row itself.
	var policyID, policyDigest, targetKind string
	var fresh bool
	if err := pool(t).QueryRow(ctx, `
SELECT policy_id, policy_digest, target_kind,
       (now() - created_at) BETWEEN interval '0 seconds' AND interval '5 minutes'
  FROM ahp_evals.verdicts WHERE agent_digest = $1`, revision).
		Scan(&policyID, &policyDigest, &targetKind, &fresh); err != nil {
		t.Fatalf("read stored verdict: %v", err)
	}
	if policyID != p1.ID {
		t.Fatalf("stored policy_id = %q, want %q: an unattributed verdict cannot be aged", policyID, p1.ID)
	}
	if policyDigest != p1.Digest || !strings.HasPrefix(policyDigest, "sha256:") {
		t.Fatalf("stored policy_digest = %q, want the declared policy digest %q", policyDigest, p1.Digest)
	}
	if targetKind != string(evals.TargetFixture) {
		t.Fatalf("stored target_kind = %q, want %q", targetKind, evals.TargetFixture)
	}
	if !fresh {
		t.Fatal("created_at is not a fresh timestamp: staleness would be undecidable")
	}
	// The revision the verdict is about is the row key, so it cannot be retargeted.
	if err := s.RequirePassedVerdict(ctx, revision); err != nil {
		t.Fatalf("gate for the evaluated revision = %v, want open", err)
	}

	// A different revision is a different artifact: the verdict that certifies one
	// does not travel to the next, however the request is worded.
	if err := s.RequirePassedVerdict(ctx, randDigest(t)); !strings.Contains(err.Error(), "no evaluation verdict") {
		t.Fatalf("gate for an un-evaluated revision = %v, want the missing-verdict denial", err)
	}

	// The operator renews the policy. History is append-only, so the stored row keeps
	// the policy it was really earned under -- and the gate measures it against the
	// CURRENT one, which is what makes "stale" a stored fact rather than a claim.
	p2, err := s.SetPolicy(ctx, evals.Policy{
		ID: "obs-prov-p2", TaskSetID: "obs-prov-set", RequiredTaskIDs: []string{"obs-t1", "obs-t2"}, MaxAgeSeconds: 3600,
	})
	if err != nil {
		t.Fatalf("SetPolicy p2: %v", err)
	}
	if err := pool(t).QueryRow(ctx,
		`SELECT policy_id, policy_digest FROM ahp_evals.verdicts WHERE agent_digest = $1`, revision).
		Scan(&policyID, &policyDigest); err != nil {
		t.Fatalf("re-read stored verdict: %v", err)
	}
	if policyID != p1.ID || policyDigest != p1.Digest {
		t.Fatalf("a policy renewal rewrote verdict provenance: %s/%s, want %s/%s",
			policyID, policyDigest, p1.ID, p1.Digest)
	}
	if err := s.RequirePassedVerdict(ctx, revision); !strings.Contains(err.Error(), "superseded") {
		t.Fatalf("gate under the renewed policy = %v, want the superseded denial", err)
	}

	// Only re-running under the current policy re-opens the gate, and the new row
	// carries the new policy digest while the old row stays as it was.
	if _, err := s.Record(ctx, evals.RecordInput{
		AgentDigest: revision, TaskSetID: "obs-prov-set", Setup: map[string]any{"obs": "runner-config-2"},
		Outcome: evals.OutcomePass,
		Tasks: []evals.TaskResult{
			{TaskID: "obs-t1", Passed: true, TerminalState: evals.TerminalCompleted, Cost: 0.01},
			{TaskID: "obs-t2", Passed: true, TerminalState: evals.TerminalCompleted, Cost: 0.01},
		},
		PolicyID: "obs-prov-p2", TargetKind: evals.TargetReal,
	}); err != nil {
		t.Fatalf("re-record under the current policy: %v", err)
	}
	if err := s.RequirePassedVerdict(ctx, revision); err != nil {
		t.Fatalf("gate after re-running under the current policy = %v, want open", err)
	}
	var newestPolicy, newestDigest, newestKind string
	if err := pool(t).QueryRow(ctx, `
SELECT policy_id, policy_digest, target_kind FROM ahp_evals.verdicts
 WHERE agent_digest = $1 ORDER BY created_at DESC, id DESC LIMIT 1`, revision).
		Scan(&newestPolicy, &newestDigest, &newestKind); err != nil {
		t.Fatalf("read newest stored verdict: %v", err)
	}
	if newestPolicy != p2.ID || newestDigest != p2.Digest {
		t.Fatalf("newest row carries %s/%s, want the current policy %s/%s", newestPolicy, newestDigest, p2.ID, p2.Digest)
	}
	if newestKind != string(evals.TargetReal) {
		t.Fatalf("newest target_kind = %q, want %q", newestKind, evals.TargetReal)
	}
}

// obsRunner builds an allowlisted suite over the real runner against an httptest
// target, declaring which tasks the operator policy requires and which the suite
// actually runs. required != run is the partial-run case.
func obsRunner(t *testing.T, s *evals.Store, required, run []string) (*evals.Runner, evals.RunRequest, string) {
	t.Helper()
	digest := randDigest(t)
	manifest := json.RawMessage(`{"components":[{"a2a":true,"image":"example/agent@` + digest + `"}]}`)
	imageDigest := digest
	var err error
	digest, err = contracts.Digest(manifest)
	if err != nil {
		t.Fatalf("manifest digest: %v", err)
	}
	target := evals.Target{Manifest: manifest, AgentID: "obs-agent", Deployment: "obs-deployment",
		AgentDigest: digest, Image: "example/agent@" + imageDigest}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "agent_id": target.AgentID,
				"version_digest": digest, "image": target.Image})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "result": map[string]any{
			"id": "task-id", "status": map[string]any{"state": "completed"},
			"artifacts": []any{map[string]any{"parts": []any{map[string]any{"kind": "text", "text": "answer"}}}}}})
	}))
	t.Cleanup(server.Close)
	target.Endpoint = server.URL + "/a2a"
	target.ReadyURL = server.URL + "/readyz"

	suffix := strings.ReplaceAll(randDigest(t)[7:20], ":", "")
	policyID := "obs-runner-policy-" + suffix
	suiteID := "obs-runner-set-" + suffix
	mustPolicy(t, s, policyID, suiteID, required...)
	cases := make([]evals.Case, 0, len(run))
	for _, id := range run {
		cases = append(cases, evals.Case{ID: id, Input: json.RawMessage(`"reference prompt"`),
			ExpectedOutputSHA256: fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("answer")))})
	}
	runner, err := evals.NewRunner(s, []evals.Suite{{ID: suiteID, Tenant: "obs-tenant", Target: target,
		TimeoutSeconds: 5, PolicyID: policyID, Cases: cases}}, server.Client())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	runner.SetTokenIssuer(evalTestIssuer{})
	return runner, evals.RunRequest{Tenant: "obs-tenant", SuiteID: suiteID, AgentDigest: digest}, digest
}

func TestObsRunnerPartialRunCannotRecordPass(t *testing.T) {
	s := store(t)
	ctx := context.Background()

	// The policy demands two reference tasks; the allowlisted suite runs one of them
	// and the agent answers correctly. An honest single-task run is still a partial
	// run: it must not be able to write a PASS for the revision.
	runner, req, revision := obsRunner(t, s, []string{"obs-t1", "obs-t2"}, []string{"obs-t1"})
	if _, err := runner.RunEval(ctx, req); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("partial run = %v, want INVALID (a missing policy-required task cannot certify)", err)
	}
	var rows int
	if err := pool(t).QueryRow(ctx,
		`SELECT count(*) FROM ahp_evals.verdicts WHERE agent_digest = $1`, revision).Scan(&rows); err != nil {
		t.Fatalf("count verdicts: %v", err)
	}
	if rows != 0 {
		t.Fatalf("a partial run left %d verdict rows, want 0", rows)
	}
	if err := s.RequirePassedVerdict(ctx, revision); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("gate after a partial run = %v, want CONFLICT", err)
	}

	// The same policy opens only when the suite covers every required task.
	full, fullReq, fullRevision := obsRunner(t, s, []string{"obs-t1", "obs-t2"}, []string{"obs-t1", "obs-t2"})
	verdict, err := full.RunEval(ctx, fullReq)
	if err != nil {
		t.Fatalf("full run: %v", err)
	}
	if verdict.Outcome != evals.OutcomePass || verdict.PolicyDigest == "" || verdict.CreatedAt.IsZero() {
		t.Fatalf("full run verdict = %+v, want a PASS carrying policy digest and timestamp", verdict)
	}
	if err := s.RequirePassedVerdict(ctx, fullRevision); err != nil {
		t.Fatalf("gate after a covering run = %v, want open", err)
	}
}
