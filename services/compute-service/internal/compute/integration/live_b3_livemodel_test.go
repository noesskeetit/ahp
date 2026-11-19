//go:build compute_live3

// The same integrated turn as live_b3_turn_test.go / live_b3_chain_test.go, with the
// scripted model replaced by the REAL provider: the production native-Qwen tool-calling
// client (internal/executor.NewQwenModel) over a scoped Vault secret
// (secretstore.QwenFromEnv), driving the real core.Runtime loop, the real compute runner
// and the real artifacts store. Opt-in like the rest of the fleet's live-provider tests:
//
//	AHP_LIVE_QWEN=1 go test -tags compute_live3 ./internal/compute/integration/ -run TestB3LiveModel
//
// Also here: what the host sandbox ACTUALLY enforces, measured from inside the sandbox
// rather than asserted from the code that sets it, and a command process killed with
// SIGKILL in the middle of a command.
package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// liveQwenModel returns the production Qwen client, skipping when live mode is off or
// the secret store is not configured for this process. Only the provider host and model
// id are logged; the key is never printed and never read from an ambient env var.
func liveQwenModel(t *testing.T) core.ModelClient {
	t.Helper()
	if os.Getenv("AHP_LIVE_QWEN") != "1" {
		t.Skip("AHP_LIVE_QWEN!=1: the live-model chain is disabled")
	}
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sec, err := secretstore.QwenFromEnv(c)
	if err != nil {
		if errors.Is(err, secretstore.ErrNotConfigured) {
			t.Skipf("live model enabled but the secret store is not configured: %v", err)
		}
		t.Fatalf("secretstore.QwenFromEnv: %v", err)
	}
	if err := sec.Validate(); err != nil {
		t.Fatalf("qwen secret: %v", err)
	}
	m, err := executor.NewQwenModel(c, sec)
	if err != nil {
		t.Fatalf("executor.NewQwenModel: %v", err)
	}
	if u, perr := url.Parse(sec.BaseURL); perr == nil {
		t.Logf("live provider host=%s model=%s (key never printed)", u.Host, sec.Model)
	}
	return m
}

// driveTurnPrompt runs one turn with a real user prompt through the real runtime.
func driveTurnPrompt(t *testing.T, runtime *core.Runtime, sc contracts.Scope, prompt string) (sawTool bool, runErr error) {
	t.Helper()
	input, err := json.Marshal(prompt)
	if err != nil {
		t.Fatalf("marshal prompt: %v", err)
	}
	req := &space.CoreRequest{
		RunID: "run-" + contracts.NewID(), ThreadID: "th-" + contracts.NewID(),
		ContextID: ownerRunCtx, Worker: "b3-live", Epoch: 1,
		MaxSteps: 4, Scope: sc, Input: input,
	}
	results := make(chan space.StepResult, 16)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(context.Background(), req, results) }()
	for {
		select {
		case sr := <-results:
			switch sr.StepType {
			case space.StepTool:
				sawTool = true
			case space.StepError:
				runErr = errors.New("turn emitted an error step")
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

// TestB3LiveModelChainToDownload is the integrated chain with a real model in the loop:
// the provider itself decides to call compute.run_command, the command runs in the
// sandbox, the produced file is sealed in the real artifacts store, the owner downloads
// it with a matching digest and size, a foreign task-context gets 403 and a foreign
// subject the parity NOT_FOUND. Nothing about the tool call is scripted.
func TestB3LiveModelChainToDownload(t *testing.T) {
	model := liveQwenModel(t)
	e := newLiveEnv(t)
	astore, ownerScope, foreignScope := newArtifactsStore(t, liveDSN(t))

	comp, err := e.store.CreateComputer(ctx(t), ownerScope, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b3-live-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	token := "AHPLIVE-" + contracts.NewID()
	ct := &chainTool{runner: e.runner, exporter: &storeExporter{st: astore, bindCtx: ownerRunCtx},
		homeBase: e.home(comp.ID), computerID: comp.ID}
	tools := core.NewToolRegistry(ct)
	tools.Register(core.ToolDef{
		Name:        computeToolName,
		Description: "Run one shell command inside the employee's sandboxed Computer and export the file it writes. Use this instead of answering from memory.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"command":{"type":"string","description":"shell command to run in the sandbox"}},"required":["command"]}`),
	})
	runtime := core.NewRuntime(core.RuntimeConfig{Model: model, Tools: tools, MaxTokens: 512})

	prompt := "Use the " + computeToolName + " tool now. Run exactly this shell command in the sandbox: " +
		"printf '" + token + "' > out.txt . Do not answer before the tool has run; afterwards say only: written."
	sawTool, runErr := driveTurnPrompt(t, runtime, ownerScope, prompt)
	if runErr != nil {
		t.Fatalf("live turn errored: %v", runErr)
	}
	if !sawTool || ct.ran != 1 {
		t.Fatalf("the live model did not run the tool exactly once (sawTool=%v ran=%d)", sawTool, ct.ran)
	}
	if ct.ref.ArtifactID == "" {
		t.Fatal("the live turn produced no sealed artifact")
	}

	rc, ref, err := astore.OpenRange(artifacts.WithCallerContext(ctx(t), ownerRunCtx), ownerScope, ct.ref.ArtifactID, 0, 0)
	if err != nil {
		t.Fatalf("owner download: %v", err)
	}
	body, _ := io.ReadAll(rc)
	rc.Close()
	if !strings.Contains(string(body), token) {
		t.Fatalf("the downloaded file does not carry the token the model was asked to write: %q", string(body))
	}
	if sealed := strings.TrimPrefix(ct.ref.Digest, "sha256:"); ref2digest(ref) != "sha256:"+sealed || ref.Size != ct.ref.SizeBytes {
		t.Fatalf("download identity mismatch: store=%s/%d tool=%s/%d",
			ref2digest(ref), ref.Size, ct.ref.Digest, ct.ref.SizeBytes)
	}
	if _, _, err := astore.OpenRange(artifacts.WithCallerContext(ctx(t), "ctx-attacker"), ownerScope, ct.ref.ArtifactID, 0, 0); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("foreign task-context: code=%v err=%v, want FORBIDDEN", contracts.Code(err), err)
	}
	if _, _, err := astore.OpenRange(ctx(t), foreignScope, ct.ref.ArtifactID, 0, 0); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign subject: code=%v err=%v, want NOT_FOUND", contracts.Code(err), err)
	}
}

// TestB3SandboxLimitsAreMeasuredNotClaimed reads the limits back FROM INSIDE the sandbox
// and compares them with the resolved quota, then measures the timeout actually killing a
// command. It also measures what is NOT enforced on this backend (CPU seconds, address
// space), because a limit that is only declared must be reported as declared.
func TestB3SandboxLimitsAreMeasuredNotClaimed(t *testing.T) {
	e := newLiveEnv(t)
	sc := scope("b3-limits")
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Quota: compute.Quota{MaxPIDs: 997, MaxOpenFiles: 313, MaxSessionTTL: 2 * time.Second},
	}, "b3-limits-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	rec, err := e.runner.RunCommand(ctx(t), sc, comp.ID, "limits-"+contracts.NewID(),
		"bash -c 'echo nproc=$(ulimit -u); echo nofile=$(ulimit -n); echo cpu=$(ulimit -t); echo as=$(ulimit -v)'", "")
	if err != nil {
		t.Fatalf("RunCommand: %v", err)
	}
	if rec.Status != compute.CommandSucceeded {
		t.Fatalf("limits probe status=%s stderr=%s", rec.Status, rec.Stderr)
	}
	t.Logf("in-sandbox probe stdout=%q stderr=%q exit=%d", rec.Stdout, rec.Stderr, rec.ExitCode)
	got := map[string]string{}
	for _, line := range strings.Split(rec.Stdout, "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok {
			got[k] = v
		}
	}
	if got["nproc"] != strconv.Itoa(int(comp.Quota.MaxPIDs)) {
		t.Fatalf("RLIMIT_NPROC inside the sandbox = %q, want the quota %d", got["nproc"], comp.Quota.MaxPIDs)
	}
	if got["nofile"] != strconv.Itoa(int(comp.Quota.MaxOpenFiles)) {
		t.Fatalf("RLIMIT_NOFILE inside the sandbox = %q, want the quota %d", got["nofile"], comp.Quota.MaxOpenFiles)
	}
	t.Logf("measured in-sandbox: nproc=%s nofile=%s cpu=%s address-space=%s", got["nproc"], got["nofile"], got["cpu"], got["as"])
	if got["cpu"] != "unlimited" || got["as"] == "0" {
		t.Logf("CPU/address-space limits on this host backend: cpu=%s as=%s (declared, not enforced here)", got["cpu"], got["as"])
	}

	started := time.Now()
	rec2, err := e.runner.RunCommand(ctx(t), sc, comp.ID, "timeout-"+contracts.NewID(), "sleep 30", "")
	if err != nil {
		t.Fatalf("RunCommand(sleep): %v", err)
	}
	elapsed := time.Since(started)
	if rec2.ExitCode != -1 || elapsed > 15*time.Second {
		t.Fatalf("timeout not enforced: exit=%d elapsed=%s status=%s (session TTL was 2s)", rec2.ExitCode, elapsed, rec2.Status)
	}
	t.Logf("timeout enforced: a 30s sleep was killed after %s with exit %d", elapsed.Round(time.Millisecond), rec2.ExitCode)
}

// // TestB3CommandProcessKilledMidCommandLeavesOneEffect kills the SANDBOXED COMMAND PROCESS
// with SIGKILL while it is running, then restarts the executor (a fresh store, as after a
// crash) and measures three things: the effect that had not happened yet did not happen,
// the same idempotency key refuses to re-dispatch, and an explicit new dispatch runs it
// exactly once. No phantom file, no lost work, no duplicate.
func TestB3CommandProcessKilledMidCommandLeavesOneEffect(t *testing.T) {
	e := newLiveEnv(t)
	sc := scope("b3-kill")
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Quota: compute.Quota{MaxSessionTTL: 60 * time.Second},
	}, "b3-kill-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	home := e.home(comp.ID)
	marker := filepath.Join(home, "effect.marker")
	counter := filepath.Join(home, "effect.count")
	key := "b3-kill-" + contracts.NewID()
	cmd := "sleep 20; printf EFFECT > effect.marker; printf x >> effect.count"

	done := make(chan error, 1)
	go func() {
		_, err := e.runner.RunCommand(ctx(t), sc, comp.ID, key, cmd, "")
		done <- err
	}()

	pid, err := waitForSandboxChild(8*time.Second, "sleep 20")
	if err != nil {
		t.Fatalf("the sandbox command process was never observable: %v", err)
	}
	// The backend gives the command its own process group, so this is the whole
	// command tree dying mid-command, which is what an executor crash looks like
	// from the outside.
	if kerr := syscall.Kill(-pid, syscall.SIGKILL); kerr != nil {
		if kerr2 := syscall.Kill(pid, syscall.SIGKILL); kerr2 != nil {
			t.Fatalf("SIGKILL pid %d: %v (group: %v)", pid, kerr2, kerr)
		}
	}
	t.Logf("SIGKILL sent to the sandboxed command process group %d mid-command", pid)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the command never returned after its process was killed")
	}

	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an effect the killed command had not reached appeared anyway: %v", err)
	}
	if _, err := os.Stat(counter); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the killed command produced a counter write it never reached: %v", err)
	}

	fresh := compute.New(e.pool)
	rec, mayDispatch, err := fresh.Exec(ctx(t), sc, comp.ID, key, cmd, "")
	if err != nil {
		t.Fatalf("recovery Exec: %v", err)
	}
	if mayDispatch {
		t.Fatal("recovery re-dispatched a key whose command was already dispatched: the external effect would repeat")
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the recovery dispatch executed the command itself: %v", err)
	}
	t.Logf("recovery refused to re-dispatch key %s (command %s, status %s)", key[:12], rec.ID[:12], rec.Status)

	restartedLeases, err := compute.NewLeaseService(fresh, e.redis)
	if err != nil {
		t.Fatalf("lease service for the restarted executor: %v", err)
	}
	restarted := compute.NewRunner(fresh, compute.NewRegistry(compute.NewHostBackend(e.jailBase)),
		e.jailBase, "test-node-restarted", nil).WithLeaseService(restartedLeases)
	rec2, err := restarted.RunCommand(ctx(t), sc, comp.ID, "b3-after-"+contracts.NewID(), cmd, "")
	if err != nil {
		t.Fatalf("post-recovery run: %v", err)
	}
	if rec2.Status != compute.CommandSucceeded {
		t.Fatalf("post-recovery run status=%s", rec2.Status)
	}
	body, err := os.ReadFile(counter)
	if err != nil {
		t.Fatalf("the work was lost as well as not repeated: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("the command ran %d times after recovery, want exactly 1", len(body))
	}
}

// findSandboxChild returns the pid of a command process this test spawned whose
// command line contains needle. The sandbox runs its own process group, so the
// returned pid is also the process-group id the backend chose for that command.
func findSandboxChild(needle string) (int, bool) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, false
	}
	self := os.Getpid()
	for _, ent := range entries {
		pid, cerr := strconv.Atoi(ent.Name())
		if cerr != nil || pid == self {
			continue
		}
		cmdline, rerr := os.ReadFile(filepath.Join("/proc", ent.Name(), "cmdline"))
		if rerr != nil || !strings.Contains(strings.ReplaceAll(string(cmdline), "\x00", " "), needle) {
			continue
		}
		status, serr := os.ReadFile(filepath.Join("/proc", ent.Name(), "status"))
		if serr != nil {
			continue
		}
		for _, line := range strings.Split(string(status), "\n") {
			if rest, ok := strings.CutPrefix(line, "PPid:"); ok {
				if ppid, aerr := strconv.Atoi(strings.TrimSpace(rest)); aerr == nil && ppid == self {
					return pid, true
				}
			}
		}
	}
	return 0, false
}

// waitForSandboxChild gives the command a chance to actually be running before the
// test kills it; a kill that arrives before the exec would prove nothing.
func waitForSandboxChild(d time.Duration, needle string) (int, error) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pid, ok := findSandboxChild(needle); ok {
			return pid, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return 0, errors.New("no running sandbox command process matching " + needle)
}
