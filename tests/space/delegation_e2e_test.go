package space_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// claimRunForTask accepts a fresh turn through the State client and claims it,
// returning the identifiers the Core loop needs to fence its writes and record
// tool-call intents (the post-claim TaskToken). This mirrors
// Coordinator.DispatchTurn at the wire level.
func claimRunForTask(t *testing.T, sc *space.StateClient, contextID string) (runID, threadID, taskToken, worker string, epoch int64) {
	t.Helper()
	worker = "worker-e2e"
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	th, err := sc.CreateThread(context.Background(), "token", scope, contextID, contextID+":k")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := sc.CreateRun(context.Background(), "token", th.ID, contextID+":run", json.RawMessage(`{"type":"turn"}`), ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	claim, err := sc.ClaimRun(context.Background(), "token", worker, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if claim.Lease == nil || claim.TaskToken == "" {
		t.Fatal("expected a lease and a task token after claim")
	}
	return claim.Lease.Run.ID, claim.Lease.Thread.ID, claim.TaskToken, worker, claim.Lease.Epoch
}

// TestDelegationArtifactDownloadE2E exercises the full human journey for one
// turn, without a live LLM: a deterministic model decides to call the
// emil.delegate tool, the real Core loop dispatches that call to the Emil
// adapter, the adapter speaks A2A over HTTP to an Emil-shaped server that
// produces a file artifact, the loop records the artifact digest as a tool
// result and the delegation intent in State, and finally the test downloads
// the artifact bytes and verifies their digest matches the one provenance
// recorded. This is the LLM -> tool -> artifact -> download journey in one
// in-process run with verifiable end-to-end state.
func TestDelegationArtifactDownloadE2E(t *testing.T) {
	const known = "REVENUE TABLE\nQ1=100\nQ2=137\nTOTAL=237\n"
	sum := sha256.Sum256([]byte(known))
	wantDigest := hex.EncodeToString(sum[:])

	// State backend (Space's own published wire contract).
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()
	sc := space.NewStateClient(srv.URL, nil)

	// Emil-shaped A2A server: an Agent Card, a message/send that returns a
	// completed task carrying one file artifact, and a download endpoint.
	var emilSrv *httptest.Server
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case emil.AgentCardPath:
			card := emil.AgentCard{
				Name:         "core-agent",
				Description:  "Policy-enforced core agent runtime",
				Version:      "1.0.0",
				Capabilities: emil.Capabilities{Streaming: true},
				SupportedInterfaces: []emil.SupportedInterface{
					{ProtocolBinding: "JSONRPC", ProtocolVersion: "0.3"},
				},
				Skills: []emil.Skill{{ID: "core_artifact_list", Name: "core_artifact_list"}},
			}
			_ = json.NewEncoder(w).Encode(card)
		case "/":
			task := emil.Task{
				ID:        "task-e2e-1",
				ContextID: "ctx-e2e-1",
				Kind:      "task",
				Status:    emil.TaskStatus{State: emil.StateCompleted, Timestamp: "2026-10-29T09:37:40Z"},
				Artifacts: []emil.Artifact{{
					ArtifactID: "art-1",
					Name:       "revenue.txt",
					Parts: []emil.Part{{Kind: "file", File: &emil.FilePart{
						Bytes:    known,
						URI:      emilSrv.URL + "/artifacts/art-1",
						Name:     "revenue.txt",
						MimeType: "text/plain",
					}}},
				}},
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": task})
		case "/artifacts/art-1":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = io.WriteString(w, known)
		default:
			http.NotFound(w, r)
		}
	})
	emilSrv = httptest.NewServer(handler)
	defer emilSrv.Close()

	// Wire the Emil adapter as the Core tool.
	adapter, err := emil.New(emil.Config{BaseURL: emilSrv.URL})
	if err != nil {
		t.Fatalf("emil.New: %v", err)
	}
	reg := core.NewToolRegistry(adapter)
	reg.Register(core.ToolDef{Name: emil.ToolName, Description: "delegate to Emil core-agent"})

	// Deterministic "LLM": one tool call, then a final answer.
	model := &scriptedModel{responses: []core.ModelResponse{
		{
			Content:   json.RawMessage(`"I will ask Emil for the revenue table"`),
			ToolCalls: []core.ToolCall{{ID: "tc1", Name: emil.ToolName, Args: json.RawMessage(`{"prompt":"fetch revenue table"}`)}},
		},
		{Content: json.RawMessage(`"Emil produced the revenue table"`)},
	}}

	rt := core.NewRuntime(core.RuntimeConfig{Model: model, Tools: reg, State: sc})

	runID, threadID, taskToken, worker, epoch := claimRunForTask(t, sc, "e2e-"+contracts.NewID())
	req := &space.CoreRequest{
		TurnID: "turn-" + contracts.NewID(), ThreadID: threadID, RunID: runID,
		Worker: worker, Epoch: epoch, TaskToken: taskToken,
		Input: json.RawMessage(`"give me the revenue table"`), MaxSteps: 4,
	}

	results := make(chan space.StepResult, 64)
	var steps []space.StepResult
	collected := make(chan struct{})
	go func() {
		for s := range results {
			steps = append(steps, s)
		}
		close(collected)
	}()
	runErr := rt.Run(context.Background(), req, results)
	close(results)
	<-collected

	if runErr != nil {
		t.Fatalf("runtime run: %v", runErr)
	}

	var toolStep *space.StepResult
	sawDone := false
	for i := range steps {
		s := steps[i]
		if s.StepType == space.StepTool && s.ToolCall != nil && s.ToolCall.Name == emil.ToolName {
			toolStep = &steps[i]
		}
		if s.StepType == space.StepDone && s.Done {
			sawDone = true
		}
	}
	if toolStep == nil {
		t.Fatalf("no emil.delegate tool step emitted; steps=%d", len(steps))
	}
	if !sawDone {
		t.Fatal("expected a terminal done step")
	}

	// The tool result must carry provenance and the artifact digest.
	content := string(toolStep.Content)
	if !strings.Contains(content, "provenance") {
		t.Fatalf("tool result missing provenance: %s", content)
	}
	if !strings.Contains(content, wantDigest) {
		t.Fatalf("tool result missing artifact digest %s: %s", wantDigest, content)
	}

	// The tool call must be auditable in State (intent recorded for the run).
	ms.mu.Lock()
	intents := len(ms.intents)
	var intentTarget string
	for _, it := range ms.intents {
		if it.Target == emil.ToolName {
			intentTarget = it.Target
		}
	}
	ms.mu.Unlock()
	if intents == 0 || intentTarget != emil.ToolName {
		t.Fatalf("expected a recorded intent for %s; intents=%d target=%q", emil.ToolName, intents, intentTarget)
	}

	// Download journey: fetch the artifact bytes over HTTP and verify the digest
	// matches what provenance recorded (and what we know the Emil produced).
	artifactURI := emilSrv.URL + "/artifacts/art-1"
	dl, err := http.Get(artifactURI)
	if err != nil {
		t.Fatalf("download artifact: %v", err)
	}
	defer dl.Body.Close()
	body, err := io.ReadAll(dl.Body)
	if err != nil {
		t.Fatalf("read artifact body: %v", err)
	}
	gotSum := sha256.Sum256(body)
	gotDigest := hex.EncodeToString(gotSum[:])
	if gotDigest != wantDigest {
		t.Fatalf("downloaded artifact digest %s != provenance digest %s", gotDigest, wantDigest)
	}
	if string(body) != known {
		t.Fatalf("downloaded artifact bytes mismatch: got %q", string(body))
	}

	// Evidence for the acceptance report.
	t.Logf("E2E journey: model->tool(%s)->artifact->download", emil.ToolName)
	t.Logf("  run=%s thread=%s epoch=%d intents=%d", runID, threadID, epoch, intents)
	t.Logf("  artifact bytes=%d sha256=%s (matches provenance)", len(body), gotDigest)
	fmt.Fprintf(io.Discard, "%d", len(steps))
}
