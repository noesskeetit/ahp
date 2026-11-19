package evals_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Row 24 addendum (DEF-PROF-1 class): the verdict must report transport errors
// and terminal statuses separately. "0 transport errors" must never be readable
// as "0 failed generations", and a PASS must be impossible when a task never
// reached a completed terminal state or its exchange broke -- the outcome is
// evidence, not a claim.
func TestVerdictSeparatesTransportFromTerminal(t *testing.T) {
	s := store(t)
	ctx := context.Background()
	pol := "tt-policy-" + randID(t)
	set := "tt-set-" + randID(t)
	mustPolicy(t, s, pol, set, "reference-task")
	d1, d2, d3, d4 := randDigest(t), randDigest(t), randDigest(t), randDigest(t)
	d5 := randDigest(t)

	// Honest FAIL evidence of a broken transport: records and reads back.
	if _, err := s.Record(ctx, evals.RecordInput{
		AgentDigest: d1, TaskSetID: set, Outcome: evals.OutcomeFail,
		Setup: map[string]any{"harness": "tt"}, PolicyID: pol, TargetKind: evals.TargetFixture,
		Tasks: []evals.TaskResult{{TaskID: "reference-task", Passed: false, TransportError: true, TerminalState: evals.TerminalUnknown}},
	}); err != nil {
		t.Fatalf("honest transport-failure FAIL must record: %v", err)
	}
	back, err := s.LatestVerdict(ctx, d1)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if len(back.Tasks) != 1 || !back.Tasks[0].TransportError || back.Tasks[0].TerminalState != evals.TerminalUnknown {
		t.Fatalf("transport/terminal evidence lost in storage: %+v", back.Tasks)
	}

	// A PASS claiming completion THROUGH a broken transport is refused.
	if _, err := s.Record(ctx, evals.RecordInput{
		AgentDigest: d2, TaskSetID: set, Outcome: evals.OutcomePass,
		Setup: map[string]any{"harness": "tt"}, PolicyID: pol, TargetKind: evals.TargetFixture,
		Tasks: []evals.TaskResult{{TaskID: "reference-task", Passed: true, TransportError: true, TerminalState: evals.TerminalCompleted}},
	}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("completed-through-broken-transport pass = %v, want INVALID", err)
	}

	// A PASS whose task never reached a terminal state is refused: content that
	// happened to match is not a completed generation (the DEF-PROF-1 mask).
	if _, err := s.Record(ctx, evals.RecordInput{
		AgentDigest: d3, TaskSetID: set, Outcome: evals.OutcomePass,
		Setup: map[string]any{"harness": "tt"}, PolicyID: pol, TargetKind: evals.TargetFixture,
		Tasks: []evals.TaskResult{{TaskID: "reference-task", Passed: true, TerminalState: evals.TerminalUnknown}},
	}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("non-terminal pass = %v, want INVALID", err)
	}

	// A FAIL with an intact exchange and a completed-but-wrong task is the
	// honest terminal shape and must record.
	if _, err := s.Record(ctx, evals.RecordInput{
		AgentDigest: d4, TaskSetID: set, Outcome: evals.OutcomeFail,
		Setup: map[string]any{"harness": "tt"}, PolicyID: pol, TargetKind: evals.TargetFixture,
		Tasks: []evals.TaskResult{{TaskID: "reference-task", Passed: false, TerminalState: evals.TerminalCompleted}},
	}); err != nil {
		t.Fatalf("logical FAIL must record: %v", err)
	}

	// The honest PASS: completed terminal, intact exchange, matching content.
	if _, err := s.Record(ctx, evals.RecordInput{
		AgentDigest: d5, TaskSetID: set, Outcome: evals.OutcomePass,
		Setup: map[string]any{"harness": "tt"}, PolicyID: pol, TargetKind: evals.TargetFixture,
		Tasks: []evals.TaskResult{{TaskID: "reference-task", Passed: true, TerminalState: evals.TerminalCompleted}},
	}); err != nil {
		t.Fatalf("honest PASS must record: %v", err)
	}
	if err := s.RequirePassedVerdict(ctx, d5); err != nil {
		t.Fatalf("honest PASS must open the gate: %v", err)
	}
}

// runnerTarget builds an allowlisted suite against an httptest target whose
// reply is scripted by reply.
func runnerTarget(t *testing.T, s *evals.Store, reply func(w http.ResponseWriter)) (*evals.Runner, evals.RunRequest, string) {
	t.Helper()
	digest := randDigest(t)
	manifest := json.RawMessage(`{"components":[{"a2a":true,"image":"example/agent@` + digest + `"}]}`)
	imageDigest := digest
	digest, err := contracts.Digest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	target := evals.Target{Manifest: manifest, AgentID: "eval-agent", Deployment: "eval-deployment", AgentDigest: digest, Image: "example/agent@" + imageDigest}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "agent_id": target.AgentID, "version_digest": digest, "image": target.Image})
			return
		}
		reply(w)
	}))
	t.Cleanup(server.Close)
	target.Endpoint = server.URL + "/a2a"
	target.ReadyURL = server.URL + "/readyz"
	polID := "tt-runner-policy-" + randID(t)
	suiteID := "tt-suite-" + randID(t)
	mustPolicy(t, s, polID, suiteID, "case")
	suite := evals.Suite{ID: suiteID, Tenant: "tenant-tt", Target: target, TimeoutSeconds: 3, PolicyID: polID,
		Cases: []evals.Case{{ID: "case", Input: json.RawMessage(`"p"`), ExpectedOutputSHA256: fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("answer")))}}}
	runner, err := evals.NewRunner(s, []evals.Suite{suite}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	runner.SetTokenIssuer(evalTestIssuer{})
	return runner, evals.RunRequest{Tenant: suite.Tenant, SuiteID: suiteID, AgentDigest: digest}, digest
}

func TestRunnerTerminalFailedIsNotTransportError(t *testing.T) {
	s := store(t)
	runner, req, digest := runnerTarget(t, s, func(w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "result": map[string]any{"id": "t", "status": map[string]any{"state": "failed"}}})
	})
	v, err := runner.RunEval(context.Background(), req)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v.Outcome != evals.OutcomeFail || len(v.Tasks) != 1 {
		t.Fatalf("verdict: %+v", v)
	}
	if v.Tasks[0].TransportError || v.Tasks[0].TerminalState != evals.TerminalFailed {
		t.Fatalf("terminal-failed must not read as transport error and must not be unknown: %+v", v.Tasks[0])
	}
	if v2, err := s.LatestVerdict(context.Background(), digest); err != nil || v2.Tasks[0].TerminalState != evals.TerminalFailed {
		t.Fatalf("terminal state lost: %+v %v", v2.Tasks, err)
	}
}

func TestRunnerTransportErrorCarriesNoTerminalClaim(t *testing.T) {
	s := store(t)
	runner, req, _ := runnerTarget(t, s, func(w http.ResponseWriter) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	v, err := runner.RunEval(context.Background(), req)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v.Outcome != evals.OutcomeFail || len(v.Tasks) != 1 {
		t.Fatalf("verdict: %+v", v)
	}
	if !v.Tasks[0].TransportError || v.Tasks[0].TerminalState != evals.TerminalUnknown {
		t.Fatalf("HTTP 500 must read as transport error with unknown terminal: %+v", v.Tasks[0])
	}
}
