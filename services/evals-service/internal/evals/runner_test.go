package evals_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestLatestFailureClosesPublicationAndRejectsInconsistentOutcome(t *testing.T) {
	s := store(t)
	ctx := context.Background()
	d := randDigest(t)
	mustPolicy(t, s, "runner-test-policy", "reference", "ref-sum", "ref-search")
	for _, bad := range []evals.RecordInput{
		{AgentDigest: d, TaskSetID: "reference", Outcome: evals.OutcomePass, PolicyID: "runner-test-policy", TargetKind: evals.TargetFixture},
		{AgentDigest: d, TaskSetID: "reference", Outcome: evals.OutcomePass, Tasks: tasks(false), PolicyID: "runner-test-policy", TargetKind: evals.TargetFixture},
		{AgentDigest: d, TaskSetID: "reference", Outcome: evals.OutcomeFail, Tasks: tasks(true), PolicyID: "runner-test-policy", TargetKind: evals.TargetFixture},
	} {
		if _, err := s.Record(ctx, bad); contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("inconsistent outcome admitted: %v", err)
		}
	}
	for _, outcome := range []evals.Outcome{evals.OutcomePass, evals.OutcomeFail} {
		if _, err := s.Record(ctx, evals.RecordInput{AgentDigest: d, TaskSetID: "reference", Outcome: outcome, Tasks: tasks(outcome == evals.OutcomePass), PolicyID: "runner-test-policy", TargetKind: evals.TargetFixture}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RequirePassedVerdict(ctx, d); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("historical pass unlocked latest fail: %v", err)
	}
}

// An HTTP protocol test, not image/deployment or provider evidence.
func TestRunnerComputesVerdictFromAllowlistedCompletedTask(t *testing.T) {
	s := store(t)
	digest := randDigest(t)
	var calls atomic.Int64
	var fail atomic.Bool
	manifest := json.RawMessage(`{"components":[{"a2a":true,"image":"example/agent@` + digest + `"}]}`)
	imageDigest := digest
	digest, _ = contracts.Digest(manifest)
	target := evals.Target{Manifest: manifest, AgentID: "eval-agent", Deployment: "eval-deployment", AgentDigest: digest, Image: "example/agent@" + imageDigest}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "agent_id": target.AgentID, "version_digest": digest, "image": target.Image})
			return
		}
		calls.Add(1)
		output := "answer"
		if fail.Load() {
			output = "wrong"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "result": map[string]any{"id": "task-id", "status": map[string]any{"state": "completed"}, "artifacts": []any{map[string]any{"parts": []any{map[string]any{"kind": "text", "text": output}}}}}})
	}))
	defer server.Close()
	target.Endpoint = server.URL + "/a2a"
	target.ReadyURL = server.URL + "/readyz"
	mustPolicy(t, s, "runner-suite-policy", "allowlisted", "case")
	suite := evals.Suite{ID: "allowlisted", Tenant: "tenant-a", Target: target, TimeoutSeconds: 3, PolicyID: "runner-suite-policy", Cases: []evals.Case{{ID: "case", Input: json.RawMessage(`"reference prompt"`), ExpectedOutputSHA256: fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("answer")))}}}
	runner, err := evals.NewRunner(s, []evals.Suite{suite}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	runner.SetTokenIssuer(evalTestIssuer{})
	req := evals.RunRequest{Tenant: "foreign", SuiteID: suite.ID, AgentDigest: digest}
	if _, err := runner.RunEval(context.Background(), req); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("foreign allowed: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("foreign target executed")
	}
	req.Tenant = suite.Tenant
	v, err := runner.RunEval(context.Background(), req)
	if err != nil || v.Outcome != evals.OutcomePass {
		t.Fatalf("pass=%+v %v", v, err)
	}
	fail.Store(true)
	v, err = runner.RunEval(context.Background(), req)
	if err != nil || v.Outcome != evals.OutcomeFail {
		t.Fatalf("fail=%+v %v", v, err)
	}
	if err := s.RequirePassedVerdict(context.Background(), digest); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("runner fail did not close gate: %v", err)
	}
}

type evalTestIssuer struct{}

func (evalTestIssuer) IssueEvalToken(context.Context, evals.RunRequest, evals.Target) (string, error) {
	return "fixture-child-token", nil
}
