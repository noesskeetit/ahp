//go:build live_llm

package space_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestLiveLLMArtifactDownloadJourney is the real-LLM counterpart of
// TestDelegationArtifactDownloadE2E. It drives the Core model/tool loop with a
// live OpenAI-compatible provider (Qwen via AHP_LLM_BASE_URL/AHP_LLM_API_KEY/
// AHP_LLM_MODEL), lets the model decide to call the emil.delegate tool, sends
// the delegation to an Emil-shaped A2A server that returns a file artifact,
// records the delegation intent in State, and downloads the artifact, verifying
// its digest matches provenance. This is the LLM -> tool -> artifact -> download
// journey with a real model in the loop.
//
// The live_llm build tag is a claim of REAL native-Qwen proof, so this test
// must FAIL (never skip) when the configured provider is not native Qwen: the
// openai_compat client only checks that AHP_LLM_BASE_URL is http(s), which by
// itself would prove OpenAI-COMPAT, not native-Qwen-only. assertNativeQwen
// enforces a host + model allowlist. Run:
//
//	go test -tags live_llm -run TestLiveLLMArtifactDownloadJourney ./tests/space

// assertNativeQwen fails the test unless AHP_LLM_BASE_URL points at a native
// Qwen inference host and AHP_LLM_MODEL is an accepted native Qwen model. It
// returns the verified host and model so the journey can log them as evidence.
func assertNativeQwen(t *testing.T) (host, model string) {
	t.Helper()
	base := os.Getenv(core.EnvOpenAIBaseURL)
	model = os.Getenv(core.EnvOpenAIModel)
	h, err := classifyNativeQwen(base, model)
	if err != nil {
		t.Fatalf("live_llm native-Qwen gate: %v", err)
	}
	return h, model
}

func TestLiveLLMArtifactDownloadJourney(t *testing.T) {
	host, modelID := assertNativeQwen(t)
	t.Logf("live native-Qwen proof: host=%s model=%s", host, modelID)

	const known = "REVENUE TABLE\nQ1=100\nQ2=137\nTOTAL=237\n"
	wantDigest := func() string { s := sha256.Sum256([]byte(known)); return hex.EncodeToString(s[:]) }()

	// State backend (Space's published wire contract, no PG).
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()
	sc := space.NewStateClient(srv.URL, nil)

	// Emil-shaped A2A server returning one completed task with one file artifact.
	var emilSrv *httptest.Server
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case emil.AgentCardPath:
			_ = json.NewEncoder(w).Encode(emil.AgentCard{
				Name:    "core-agent",
				Version: "1.0.0",
				Skills:  []emil.Skill{{ID: emil.ToolName, Name: emil.ToolName}},
			})
		case "/":
			task := emil.Task{
				ID: "task-live-1", ContextID: "ctx-live-1", Kind: "task",
				Status: emil.TaskStatus{State: emil.StateCompleted, Timestamp: "2026-10-29T09:37:40Z"},
				Artifacts: []emil.Artifact{{
					ArtifactID: "art-live-1", Name: "revenue.txt",
					Parts: []emil.Part{{Kind: "file", File: &emil.FilePart{
						Bytes: known, URI: emilSrv.URL + "/artifacts/art-live-1",
						Name: "revenue.txt", MimeType: "text/plain",
					}}},
				}},
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": task})
		case "/artifacts/art-live-1":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = io.WriteString(w, known)
		default:
			http.NotFound(w, r)
		}
	})
	emilSrv = httptest.NewServer(handler)
	defer emilSrv.Close()

	adapter, err := emil.New(emil.Config{BaseURL: emilSrv.URL})
	if err != nil {
		t.Fatalf("emil.New: %v", err)
	}
	reg := core.NewToolRegistry(adapter)
	reg.Register(core.ToolDef{
		Name:        emil.ToolName,
		Description: "Delegate a data-retrieval request to the Emil core agent. It returns generated file artifacts. Always call this tool when the user asks for a report or table.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"prompt":{"type":"string","description":"the request to delegate"}},"required":["prompt"]}`),
	})

	// Real OpenAI-compatible model client.
	model, err := core.NewOpenAICompatFromEnv()
	if err != nil {
		t.Fatalf("openai-compat from env: %v", err)
	}
	t.Logf("live model = %s", model.Model())

	rt := core.NewRuntime(core.RuntimeConfig{Model: model, Tools: reg, State: sc})

	runID, threadID, taskToken, worker, epoch := claimRunForTask(t, sc, "live-"+contracts.NewID())
	req := &space.CoreRequest{
		TurnID: "turn-" + contracts.NewID(), ThreadID: threadID, RunID: runID,
		Worker: worker, Epoch: epoch, TaskToken: taskToken,
		Input:    json.RawMessage(`"Use the ` + emil.ToolName + ` tool now to fetch the revenue table, then reply with the totals."`),
		MaxSteps: 6,
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

	for _, s := range steps {
		switch s.StepType {
		case space.StepModel:
			t.Logf("model step: %s", truncate(string(s.Content), 240))
		case space.StepTool:
			t.Logf("tool step: name=%q content=%s", toolName(s), truncate(string(s.Content), 240))
		case space.StepError:
			t.Logf("error step: %s", truncate(string(s.Content), 240))
		}
	}
	if runErr != nil {
		t.Fatalf("live runtime run: %v", runErr)
	}

	// Require the model to have called the Emil tool and produced a provable artifact.
	var toolStep *space.StepResult
	for i := range steps {
		if steps[i].StepType == space.StepTool && toolName(steps[i]) == emil.ToolName {
			toolStep = &steps[i]
		}
	}
	if toolStep == nil {
		t.Fatalf("live model did not call %s within MaxSteps; steps=%d — retry or strengthen prompt/tool description", emil.ToolName, len(steps))
	}
	content := string(toolStep.Content)
	if !strings.Contains(content, "provenance") {
		t.Fatalf("tool result missing provenance: %s", content)
	}
	if !strings.Contains(content, wantDigest) {
		t.Fatalf("tool result missing artifact digest %s: %s", wantDigest, content)
	}

	// Delegation must be auditable in State.
	ms.mu.Lock()
	var intentTarget string
	for _, it := range ms.intents {
		if it.Target == emil.ToolName {
			intentTarget = it.Target
		}
	}
	ms.mu.Unlock()
	if intentTarget != emil.ToolName {
		t.Fatalf("expected a recorded %s intent; target=%q", emil.ToolName, intentTarget)
	}

	// Download the artifact and verify its digest against provenance.
	dl, err := http.Get(emilSrv.URL + "/artifacts/art-live-1")
	if err != nil {
		t.Fatalf("download artifact: %v", err)
	}
	defer dl.Body.Close()
	body, err := io.ReadAll(dl.Body)
	if err != nil {
		t.Fatalf("read artifact body: %v", err)
	}
	got := sha256.Sum256(body)
	if gotDigest := hex.EncodeToString(got[:]); gotDigest != wantDigest {
		t.Fatalf("downloaded digest %s != provenance %s", gotDigest, wantDigest)
	}
	t.Logf("LIVE journey ok: run=%s thread=%s epoch=%d artifact=%dB sha256=%s",
		runID, threadID, epoch, len(body), wantDigest)
}

func toolName(s space.StepResult) string {
	if s.ToolCall != nil {
		return s.ToolCall.Name
	}
	return ""
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}
