package registry_test

// D-REG-3 (independent verification 20261110, registry-deploy-admission-
// verification-20261110.md): the promote readiness probe was only ever exercised
// through a scripted Go prober (p2_readiness_gate_test.go) and a language-neutral
// /readyz double, so "the probe works against a real runtime" was never measured
// for ANY real runtime -- Go or otherwise.
//
// This file closes the part of that which is provable on this machine: the REAL
// registry.HTTPReadinessProber (the same one the composed product wires) probing
// REAL out-of-process runtimes, driven through the REAL promote gate and the REAL
// auto-rollback supervisor. Nothing here scripts the prober; the fixtures are the
// runtimes being probed, and they are separate OS processes.
//
// Scope honesty: a positive leg on the real Go workload binary needs a State API
// it can claim work from (internal/agentworkload marks itself healthy only after a
// successful ClaimRun), which is a stand mount, not a unit fixture. That leg and
// live OCI push/pull are listed as prerequisites in the lane report; they are NOT
// claimed here.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// liveReadyzScript is a real, separate-language runtime: python3 standard library
// only, no product code, no Go substitute. It re-reads its state file on every
// request so a leg can flip readiness or identity in a LIVE process instead of
// standing up a new double, and it binds an ephemeral port itself (printing the
// port) rather than racing over a pre-picked one.
const liveReadyzScript = `
import json, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
state_file = sys.argv[1]
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            with open(state_file) as fh:
                state = json.load(fh)
        except Exception:
            state = {"ready": False}
        body = json.dumps(state).encode()
        self.send_response(200 if state.get("ready") else 503)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass
server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
sys.stdout.write("PORT %d\n" % server.server_address[1])
sys.stdout.flush()
server.serve_forever()
`

// liveRuntime is one running runtime process plus the file its /readyz answer is
// read from.
type liveRuntime struct {
	t         *testing.T
	stateFile string
	url       string
	cmd       *exec.Cmd
}

// startLiveRuntime launches the python3 runtime and waits for its port banner.
// The wait is bounded: it waits for a process to come up, it never asserts on
// elapsed time.
func startLiveRuntime(t *testing.T) *liveRuntime {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatalf("prerequisite missing: python3 is not on PATH. This leg measures the probe against a real non-Go process and has no Go substitute, so it fails instead of silently passing.")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "readyz_runtime.py")
	if err := os.WriteFile(script, []byte(liveReadyzScript), 0o600); err != nil {
		t.Fatalf("write runtime script: %v", err)
	}
	rt := &liveRuntime{t: t, stateFile: filepath.Join(dir, "state.json")}
	rt.set(map[string]any{"ready": false, "agent_id": "not-yet", "version_digest": "not-yet", "image": "not-yet"})
	cmd := exec.Command(python, script, rt.stateFile)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start live runtime: %v", err)
	}
	rt.cmd = cmd
	t.Cleanup(rt.kill)
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "PORT ") {
		rt.kill()
		t.Fatalf("live runtime never announced its port: %q %v", line, err)
	}
	var port int
	if _, err := fmt.Sscanf(line, "PORT %d", &port); err != nil || port <= 0 {
		rt.kill()
		t.Fatalf("live runtime announced an unusable port: %q", line)
	}
	rt.url = "http://127.0.0.1:" + strconv.Itoa(port) + "/readyz"
	return rt
}

// set replaces the state the live process serves. Written atomically so the
// process never reads a half-written document.
func (r *liveRuntime) set(state map[string]any) {
	r.t.Helper()
	b, err := json.Marshal(state)
	if err != nil {
		r.t.Fatalf("marshal runtime state: %v", err)
	}
	tmp := r.stateFile + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		r.t.Fatalf("write runtime state: %v", err)
	}
	if err := os.Rename(tmp, r.stateFile); err != nil {
		r.t.Fatalf("publish runtime state: %v", err)
	}
}

// identify publishes the exact identity one binding pins, ready or not.
func (r *liveRuntime) identify(agent, digest, image string, ready bool) {
	r.t.Helper()
	r.set(map[string]any{"ready": ready, "agent_id": agent, "version_digest": digest, "image": image})
}

func (r *liveRuntime) kill() {
	if r.cmd != nil && r.cmd.Process != nil {
		_ = r.cmd.Process.Kill()
		_, _ = r.cmd.Process.Wait()
		r.cmd = nil
	}
}

// awaitAnswerReady polls until the live process answers HTTP at all. Used only to
// avoid a probe racing the process's own startup, never to assert a duration.
func (r *liveRuntime) awaitAnswerReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(r.url)
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("live runtime at %s never answered", r.url)
}

// TestPromoteGateProbesRealNonGoRuntime runs the real prober against two real
// python3 runtime processes, through the real promote gate and supervisor:
//
//  1. a live process that says it is not ready must block the switch,
//  2. the same process once ready must allow it (acceptance measured against a
//     genuinely foreign-language runtime, not a Go httptest double),
//  3. a LIVE process that names a different agent than the binding pins is not
//     ready: the supervisor moves traffic back,
//  4. a killed process (unreachable) blocks the switch again.
//
// Traffic position is read back from the store after every step, so "refused"
// means "traffic did not move", not "an error string appeared".
func TestPromoteGateProbesRealNonGoRuntime(t *testing.T) {
	ctx := context.Background()
	prober := registry.NewHTTPReadinessProber(&http.Client{Timeout: 3 * time.Second})
	store, tenant := newGatedStore(t, prober, "stage")

	agent := "agent-" + randSuffix(t)
	a := publish(t, store, agent, manifest(t, "livegate.a"))
	b := publish(t, store, agent, manifest(t, "livegate.b"))

	image := "registry.local/ahp/agent:live@sha256:" + strings.Repeat("b", 64)
	rtA := startLiveRuntime(t)
	rtB := startLiveRuntime(t)
	rtA.awaitAnswerReady(t)
	rtB.awaitAnswerReady(t)
	rtA.identify(agent, a.Digest, image, true)
	rtB.identify(agent, b.Digest, image, false) // live, answering, not ready

	registerLive := func(v registry.Version, rt *liveRuntime) {
		t.Helper()
		binding := registry.RuntimeBinding{
			Tenant: tenant, Deployment: "rt-" + randSuffix(t), Environment: "stage", AgentID: agent,
			Digest: v.Digest, Image: image, Endpoint: strings.TrimSuffix(rt.url, "/readyz") + "/a2a",
			ReadyURL:    rt.url,
			Attestation: `{"payloadType":"application/vnd.in-toto+json","payload":"e30","signatures":[]}`,
			SBOM:        []byte(`{"bomFormat":"CycloneDX","components":[]}`),
		}
		if err := store.RegisterRuntime(ctx, binding); err != nil {
			t.Fatalf("register live runtime for %s: %v", v.ID, err)
		}
	}
	registerLive(a, rtA)
	registerLive(b, rtB)

	inst, err := store.Install(ctx, tenant, agent, a.ID, "stage", "inst-"+randSuffix(t), granted("model/qwen-max"))
	if err != nil {
		t.Fatalf("install A into stage: %v", err)
	}

	// (1) Live but not ready.
	if _, err := store.Promote(ctx, tenant, inst.ID, b.ID); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("promote while the live runtime reports not ready = %v, want UNAVAILABLE", err)
	}
	if got, _ := store.GetInstallation(ctx, tenant, inst.ID); got.VersionID != a.ID {
		t.Fatalf("a refused promote moved traffic to %s, want %s", got.VersionID, a.ID)
	}

	// (2) The very same process becomes ready: acceptance is measured against a
	// real non-Go runtime speaking the contract, through the real prober.
	rtB.identify(agent, b.Digest, image, true)
	if _, err := store.Promote(ctx, tenant, inst.ID, b.ID); err != nil {
		t.Fatalf("promote once the live runtime is ready: %v", err)
	}
	if got, _ := store.GetInstallation(ctx, tenant, inst.ID); got.VersionID != b.ID {
		t.Fatalf("ready live runtime did not get traffic: %s, want %s", got.VersionID, b.ID)
	}

	// (3) Still answering, but naming a different agent: a running process is not
	// the same as the pinned runtime, and the supervisor must switch back.
	rtB.identify("agent-somebody-else", b.Digest, image, true)
	rolled, did, err := store.AutoRollbackIfUnready(ctx, tenant, inst.ID)
	if err != nil {
		t.Fatalf("auto-rollback against a live runtime with foreign identity: %v", err)
	}
	if !did {
		t.Fatal("a live runtime naming another agent was accepted as ready")
	}
	if rolled.VersionID != a.ID {
		t.Fatalf("auto-rollback left traffic on %s, want %s", rolled.VersionID, a.ID)
	}
	// The gate must now also refuse the switch forward for the same reason.
	if _, err := store.Promote(ctx, tenant, inst.ID, b.ID); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("promote onto a live runtime with foreign identity = %v, want UNAVAILABLE", err)
	}

	// (4) Process gone: unreachable is not ready either.
	rtB.kill()
	if _, err := store.Promote(ctx, tenant, inst.ID, b.ID); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("promote onto a dead runtime = %v, want UNAVAILABLE", err)
	}
	if got, _ := store.GetInstallation(ctx, tenant, inst.ID); got.VersionID != a.ID {
		t.Fatalf("a dead runtime got traffic: %s, want %s", got.VersionID, a.ID)
	}
}
