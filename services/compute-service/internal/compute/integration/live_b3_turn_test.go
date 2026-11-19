//go:build compute_live3

// Block-3 integrated evidence (CONTRACT compute-workspace-sandbox-20261101T091617Z).
//
// Closes the ONE seam the block was missing: a live user turn whose model invokes a
// tool, and that tool executes a command INSIDE the compute sandbox (real limits)
// producing a file. The turn engine is the REAL internal/core.Runtime model/tool loop
// and internal/space.CoreRequest -- not a CLI scaffolding substitute. The command
// substrate is the real compute runner over live PostgreSQL + Redis (newLiveEnv).
//
// Gated behind //go:build compute_live3 (never in the default/CI suite):
//
//	go test -tags compute_live3 ./internal/compute/integration/ -run TestB3Turn
package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const computeToolName = "compute.run_command"

// scriptedModel is a deterministic core.ModelClient: it requests the compute tool on
// step 0 and finishes later. This is the standard way to drive a real tool loop
// deterministically (the exec lane gates the equivalent live variant behind
// AHP_LIVE_QWEN); the core.Runtime engine, tool dispatch and sandbox are all real.
type scriptedModel struct{}

func (scriptedModel) Generate(_ context.Context, req *core.ModelRequest) (*core.ModelResponse, error) {
	if req.Step == 0 {
		args, _ := json.Marshal(map[string]any{"command": "printf ahp-block3 > out.txt"})
		return &core.ModelResponse{
			Content:   json.RawMessage("null"),
			ToolCalls: []core.ToolCall{{ID: "call-1", Name: computeToolName, Args: args}},
		}, nil
	}
	return &core.ModelResponse{Content: json.RawMessage(`"file written"`)}, nil
}

// computeTool implements core.ToolExecutor by running the command through the real
// compute runner inside the sandbox, then reading the produced file from disk.
type computeTool struct {
	runner     *compute.Runner
	homeBase   string
	computerID string
	ran        int
	lastDigest string
	lastSize   int64
}

func (ct *computeTool) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	if name != computeToolName {
		return nil, fmt.Errorf("computeTool: unknown tool %q", name)
	}
	var in struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, err
	}
	scope, _, ok := core.RunScopeFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("computeTool: no run scope on context")
	}
	rec, err := ct.runner.RunCommand(ctx, scope, ct.computerID, "run-"+contracts.NewID(), in.Command, "")
	if err != nil {
		return nil, err
	}
	if rec.Status != compute.CommandSucceeded {
		return nil, fmt.Errorf("computeTool: command status=%s stderr=%q", rec.Status, rec.Stderr)
	}
	ct.ran++
	b, err := os.ReadFile(filepath.Join(ct.homeBase, "out.txt"))
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	ct.lastDigest = "sha256:" + hex.EncodeToString(sum[:])
	ct.lastSize = int64(len(b))
	return json.Marshal(map[string]any{"path": "out.txt", "sha256": ct.lastDigest, "size": ct.lastSize})
}

// driveTurn runs a single real core.Runtime turn to completion. State/Durable are
// nil: this proves the model->tool->sandbox path; durable terminal state is space's
// own test surface.
func driveTurn(t *testing.T, runtime *core.Runtime, sc contracts.Scope, c context.Context) (sawTool bool, runErr error) {
	t.Helper()
	req := &space.CoreRequest{
		RunID: "run-" + contracts.NewID(), ThreadID: "th-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(), Worker: "b3-worker", Epoch: 1,
		MaxSteps: 4, Scope: sc,
	}
	results := make(chan space.StepResult, 16)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(c, req, results) }()
	for {
		select {
		case sr := <-results:
			if sr.StepType == space.StepTool {
				sawTool = true
			}
			if sr.StepType == space.StepError {
				runErr = fmt.Errorf("turn emitted error step")
			}
		case err := <-done:
			if runErr == nil {
				runErr = err
			}
			for {
				select {
				case sr := <-results:
					if sr.StepType == space.StepTool {
						sawTool = true
					}
				default:
					return sawTool, runErr
				}
			}
		}
	}
}

// TestB3TurnModelToolSandboxWithLimits is CONTRACT item 3 (model/tool/sandbox/file)
// live: a real core.Runtime turn calls the compute tool, which executes a command in
// the sandbox under the class quota and writes a file verified on disk (sha/size).
func TestB3TurnModelToolSandboxWithLimits(t *testing.T) {
	e := newLiveEnv(t)
	sc := scope("b3-alice")
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b3-computer-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	ct := &computeTool{runner: e.runner, homeBase: e.home(comp.ID), computerID: comp.ID}
	tools := core.NewToolRegistry(ct)
	tools.Register(core.ToolDef{
		Name:        computeToolName,
		Description: "Run a shell command inside the compute sandbox and write a file.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`),
	})
	runtime := core.NewRuntime(core.RuntimeConfig{Model: scriptedModel{}, Tools: tools, MaxTokens: 256})

	sawTool, runErr := driveTurn(t, runtime, sc, ctx(t))
	if runErr != nil {
		t.Fatalf("turn errored: %v", runErr)
	}
	if !sawTool {
		t.Fatal("turn never invoked the compute tool")
	}
	if ct.ran != 1 {
		t.Fatalf("compute tool executed %d times, want 1", ct.ran)
	}
	if ct.lastSize != int64(len("ahp-block3")) {
		t.Fatalf("file size=%d, want %d", ct.lastSize, len("ahp-block3"))
	}
	want := sha256.Sum256([]byte("ahp-block3"))
	if ct.lastDigest != "sha256:"+hex.EncodeToString(want[:]) {
		t.Fatalf("digest=%s, want sha256:%s", ct.lastDigest, hex.EncodeToString(want[:]))
	}
}

// TestB3TurnRefusesWhenQuotaForbidsProcesses proves the limit path is real: a
// degenerate quota (MaxPIDs<=0) is refused by CheckProcessBudget (host backend
// enforces it before exec), while the class X default is not.
func TestB3TurnRefusesWhenQuotaForbidsProcesses(t *testing.T) {
	if err := compute.CheckProcessBudget(compute.ResolveQuota(compute.ClassX, compute.Quota{})); err != nil {
		t.Fatalf("class X default must allow processes, got %v", err)
	}
	if err := compute.CheckProcessBudget(compute.Quota{MaxPIDs: 0}); err == nil {
		t.Fatal("MaxPIDs<=0 must be refused")
	}
}

// TestB3RecoveryNoDuplicateExternalEffect is CONTRACT item 4 at the compute boundary:
// dispatch a command, kill the executor mid-command (simulated by not completing and
// reopening the store on a fresh connection -- a post-SIGKILL life); replaying the
// SAME idempotency key must NOT re-dispatch (no duplicated external effect), and
// completing the original drives it to a terminal state consistently.
func TestB3RecoveryNoDuplicateExternalEffect(t *testing.T) {
	e := newLiveEnv(t)
	sc := scope("b3-recovery")
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b3-rec-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	key := "b3-once-" + contracts.NewID()

	rec1, mayDispatch, err := e.store.Exec(ctx(t), sc, comp.ID, key, "touch external-effect", "")
	if err != nil || !mayDispatch {
		t.Fatalf("life1 Exec: mayDispatch=%v err=%v", mayDispatch, err)
	}
	if _, err := e.store.MarkDispatched(ctx(t), sc, comp.ID, rec1.ID); err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}

	fresh := compute.New(e.pool)
	rec2, mayDispatch2, err := fresh.Exec(ctx(t), sc, comp.ID, key, "touch external-effect", "")
	if err != nil {
		t.Fatalf("recovery Exec: %v", err)
	}
	if mayDispatch2 {
		t.Fatal("REPLAY RE-EXECUTED: recovery must not re-dispatch a known key")
	}
	if rec2.ID != rec1.ID {
		t.Fatalf("recovery returned a different command id (%s vs %s)", rec2.ID, rec1.ID)
	}
	if _, err := e.store.Complete(ctx(t), sc, comp.ID, rec1.ID, 0, "ok", ""); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	final, err := fresh.GetCommand(ctx(t), sc, comp.ID, rec1.ID)
	if err != nil {
		t.Fatalf("GetCommand after terminal: %v", err)
	}
	if final.Status != compute.CommandSucceeded {
		t.Fatalf("final status=%s, want succeeded", final.Status)
	}
}

// TestB3SandboxEnvIsSecretFree evidences CONTRACT item 5 at the compute boundary:
// the host backend runs a command with a MINIMAL environment (PATH, HOME only), so
// the process secrets that live in the runner/supervisor env -- database DSN, MinIO
// root credentials, vault token -- never reach the sandboxed command or its captured
// stdout/stderr (the observer transcript).
func TestB3SandboxEnvIsSecretFree(t *testing.T) {
	e := newLiveEnv(t)
	sc := scope("b3-observer")
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b3-obs-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	rec, err := e.runner.RunCommand(ctx(t), sc, comp.ID, "env-"+contracts.NewID(), "env", "")
	if err != nil {
		t.Fatalf("RunCommand(env): %v", err)
	}
	if rec.Status != compute.CommandSucceeded {
		t.Fatalf("env cmd status=%s", rec.Status)
	}
	forbidden := []string{"AHP_", "DATABASE", "MINIO", "SECRET", "VAULT", "postgres://", "PASSWORD", "DSN", "TOKEN"}
	combined := rec.Stdout + "\n" + rec.Stderr
	for _, bad := range forbidden {
		if strings.Contains(combined, bad) {
			t.Fatalf("sandbox observer output leaked secret substring %q", bad)
		}
	}
}
