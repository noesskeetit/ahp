// D-REG-3, second half: a readiness-probe leg against the REAL Go workload
// binary, not against a Go double. It boots the product's own State stand
// (cmd/state-stand: real HTTPS+mTLS, real PostgreSQL, product migrations) and the
// product's own agent runtime (cmd/ahp-agent), then asks the production prober
// (registry.NewHTTPReadinessProber, the same construction cmd/ahp/mount_w2a.go:93
// wires) whether the promotion target is serving -- first while the runtime is
// healthy, then after the runtime has been drained by SIGUSR1, which is the
// product's own drain signal.
//
// What is real and what is not, stated exactly:
//   - real: /readyz handler, claim loop, drain signal, TLS server, the prober,
//     the promote gate and the auto-rollback supervisor;
//   - real: PostgreSQL (this lane's own database), the State API and its PKI;
//   - not real: nothing stands in for the runtime. The probe CLIENT, however,
//     verifies the certificate CHAIN against the stand CA but skips name/EKU
//     checking, because the only certificate the State stand mints for a workload
//     carries URI SANs and ClientAuth EKU, while cmd/ahp-agent serves its socket
//     with that same workload certificate. A strict server-auth verification of
//     that leaf cannot succeed, and that mismatch is a deployment question
//     (AHP_RUNTIME_CLIENT_* in cmd/ahp/mount_w2a.go) this leg does not adjudicate.
//
// Harness prerequisites (see the lane report for the exact command; the leg
// SKIPS when they are absent so the package stays runnable, and a skip here is
// disclosed as not-coverage, never reported as a pass):
//
//	AHP_AGENT_BIN         built cmd/ahp-agent binary
//	AHP_STATE_STAND_BIN   built cmd/state-stand binary
//	AHP_LANE_ENV_FILE     0600 lane env file carrying AHP_TEST_DATABASE_URL
package registry_test

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type liveHandshake struct {
	BaseURL    string `json:"base_url"`
	CAFile     string `json:"ca_file"`
	CertFile   string `json:"cert_file"`
	KeyFile    string `json:"key_file"`
	Tenant     string `json:"tenant"`
	Deployment string `json:"deployment"`
	// TaskToken is deliberately NOT read: this leg never prints or forwards it.
}

// requireProductBinaries returns the harness-supplied binaries, or skips the leg
// with the exact prerequisite named.
func requireProductBinaries(t *testing.T) (agent, stand, laneEnv, dbName string) {
	t.Helper()
	agent, stand, laneEnv = os.Getenv("AHP_AGENT_BIN"), os.Getenv("AHP_STATE_STAND_BIN"), os.Getenv("AHP_LANE_ENV_FILE")
	if agent == "" || stand == "" || laneEnv == "" {
		t.Skip("prerequisite not supplied: AHP_AGENT_BIN, AHP_STATE_STAND_BIN and AHP_LANE_ENV_FILE must name the built binaries and the lane env file. This leg is only evidence when it RAN; a skip is not coverage.")
	}
	for _, path := range []string{agent, stand, laneEnv} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("prerequisite unreadable: %v", err)
		}
	}
	raw := os.Getenv("AHP_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("prerequisite not supplied: AHP_TEST_DATABASE_URL")
	}
	u, err := url.Parse(raw)
	if err != nil || strings.Trim(u.Path, "/") == "" {
		t.Fatalf("cannot derive the database name from the lane DSN")
	}
	return agent, stand, laneEnv, strings.Trim(u.Path, "/")
}

// startStateStand boots the real State stand and returns once it prints READY.
// The bound is startup-only (migrations run first); no assertion depends on time.
func startStateStand(t *testing.T, bin, laneEnv, dbName string) liveHandshake {
	t.Helper()
	hsPath := filepath.Join(t.TempDir(), "handshake.json")
	cmd := exec.Command(bin, "-env", laneEnv, "-handshake", hsPath, "-db-allowlist", dbName)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stand stdout: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stand stderr: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start state stand: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	readied := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.TrimSpace(scanner.Text()) == "READY" {
				readied <- nil
				return
			}
		}
		readied <- &standStartupError{detail: "stdout closed before READY"}
	}()
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			t.Logf("state-stand: %s", scanner.Text())
		}
	}()
	select {
	case err := <-readied:
		if err != nil {
			t.Fatalf("state stand never became ready: %v", err)
		}
	case <-time.After(90 * time.Second):
		t.Fatal("state stand did not print READY within 90s")
	}
	raw, err := os.ReadFile(hsPath)
	if err != nil {
		t.Fatalf("read handshake: %v", err)
	}
	var hs liveHandshake
	if err := json.Unmarshal(raw, &hs); err != nil {
		t.Fatalf("parse handshake: %v", err)
	}
	if hs.BaseURL == "" || hs.CAFile == "" || hs.CertFile == "" || hs.KeyFile == "" || hs.Tenant == "" || hs.Deployment == "" {
		t.Fatalf("handshake is missing the fields this leg needs")
	}
	t.Logf("state stand: base_url=%s tenant=%s deployment=%s", hs.BaseURL, hs.Tenant, hs.Deployment)
	return hs
}

type standStartupError struct{ detail string }

func (e *standStartupError) Error() string { return "stand exited before READY: " + e.detail }

// probeTLSClient verifies the chain against the stand CA. Name and EKU checking
// is skipped for the documented reason in this file's header: the stand's
// workload leaf is URI-SAN/ClientAuth-only, and the agent serves with it.
func probeTLSClient(t *testing.T, hs liveHandshake) *http.Client {
	t.Helper()
	pair, err := tls.LoadX509KeyPair(hs.CertFile, hs.KeyFile)
	if err != nil {
		t.Fatalf("load workload keypair: %v", err)
	}
	caPEM, err := os.ReadFile(hs.CAFile)
	if err != nil {
		t.Fatalf("read stand CA: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("stand CA contained no parsable certificate")
	}
	block, _ := pem.Decode(caPEM)
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse stand CA: %v", err)
	}
	pool.AddCert(caCert)
	return &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion:         tls.VersionTLS12,
		RootCAs:            pool,
		Certificates:       []tls.Certificate{pair},
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			for _, raw := range rawCerts {
				leaf, err := x509.ParseCertificate(raw)
				if err != nil {
					return err
				}
				_, err = leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
				if err == nil {
					return nil
				}
			}
			return contracts.NewError(contracts.ErrUnavailable, "runtime certificate does not chain to the stand CA")
		},
	}}}
}

// startAgentRuntime boots the real cmd/ahp-agent against the real stand.
func startAgentRuntime(t *testing.T, bin string, hs liveHandshake, agentID, digest, image string) (string, int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick an agent port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	// Derived from a seed rather than from GenerateKey's return values: this
	// toolchain orders those differently from the familiar (priv, pub), and the
	// agent validates the file by LENGTH (ed25519.PublicKeySize), so the wrong
	// one of the two silently produces a 64-byte "public" key the agent rejects.
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		t.Fatalf("seed a task-token verification key: %v", err)
	}
	pub, ok := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize {
		t.Fatalf("derived a public key of %d bytes, want %d", len(pub), ed25519.PublicKeySize)
	}
	keysFile := filepath.Join(t.TempDir(), "public-keys.json")
	raw, err := json.Marshal(map[string]string{"harness-1": base64.StdEncoding.EncodeToString(pub)})
	if err != nil {
		t.Fatalf("marshal public keys: %v", err)
	}
	if err := os.WriteFile(keysFile, raw, 0o600); err != nil {
		t.Fatalf("write public keys: %v", err)
	}
	cmd := exec.Command(bin)
	// The lane env this test process was started from already carries AHP_AGENT_*
	// and AHP_TLS_* values belonging to other harnesses, and os.Getenv in the child
	// resolves the FIRST match -- so inherited keys are dropped rather than appended
	// over, or the agent silently starts with somebody else's configuration.
	cmd.Env = append(envWithoutPrefixes(os.Environ(),
		"AHP_AGENT_", "AHP_TLS_", "AHP_STATE_URL", "AHP_MODEL_URL", "AHP_LISTEN"),
		"AHP_AGENT_TENANT="+hs.Tenant,
		"AHP_AGENT_DEPLOYMENT="+hs.Deployment,
		"AHP_AGENT_ID="+agentID,
		"AHP_AGENT_DIGEST="+digest,
		"AHP_AGENT_IMAGE="+image,
		"AHP_AGENT_PUBLIC_KEYS_FILE="+keysFile,
		"AHP_AGENT_GATEWAY_WORKLOADS=gateway.workloads.invalid",
		"AHP_STATE_URL="+hs.BaseURL,
		"AHP_MODEL_URL=https://model.invalid",
		"AHP_TLS_CERT="+hs.CertFile,
		"AHP_TLS_KEY="+hs.KeyFile,
		"AHP_TLS_CA="+hs.CAFile,
		"AHP_LISTEN=127.0.0.1:"+strconv.Itoa(port),
	)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("agent stderr: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start agent runtime: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			t.Logf("ahp-agent: %s", scanner.Text())
		}
	}()
	return "https://127.0.0.1:" + strconv.Itoa(port), cmd.Process.Pid
}

// envWithoutPrefixes removes inherited variables that this child must own.
func envWithoutPrefixes(env []string, prefixes ...string) []string {
	out := make([]string, 0, len(env))
	for _, entry := range env {
		drop := false
		for _, prefix := range prefixes {
			if strings.HasPrefix(entry, prefix) {
				drop = true
				break
			}
		}
		if !drop {
			out = append(out, entry)
		}
	}
	return out
}

// TestPromoteGateProbesRealGoRuntime measures the probe contract against the
// product's own runtime process: a healthy agent passes the real prober and gets
// traffic through the promote gate; the same process after SIGUSR1-drain is not
// ready, and the supervisor moves traffic back off it.
func TestPromoteGateProbesRealGoRuntime(t *testing.T) {
	agentBin, standBin, laneEnv, dbName := requireProductBinaries(t)
	ctx := context.Background()

	hs := startStateStand(t, standBin, laneEnv, dbName)

	store, tenant := newGatedStore(t, registry.NewHTTPReadinessProber(probeTLSClient(t, hs)), "stage")
	agent := "agent-" + randSuffix(t)
	a := publish(t, store, agent, manifest(t, "livego.a"))
	b := publish(t, store, agent, manifest(t, "livego.b"))

	image := "registry.local/ahp/agent:livego@sha256:" + strings.Repeat("c", 64)
	baseURL, pid := startAgentRuntime(t, agentBin, hs, agent, b.Digest, image)
	readyURL := baseURL + "/readyz"

	client := probeTLSClient(t, hs)
	if err := store.RegisterRuntime(ctx, registry.RuntimeBinding{
		Tenant: tenant, Deployment: "rt-" + randSuffix(t), Environment: "stage", AgentID: agent,
		Digest: b.Digest, Image: image, Endpoint: baseURL + "/a2a", ReadyURL: readyURL,
		Attestation: `{"payloadType":"application/vnd.in-toto+json","payload":"e30","signatures":[]}`,
		SBOM:        []byte(`{"bomFormat":"CycloneDX","components":[]}`),
	}); err != nil {
		t.Fatalf("register the live Go runtime: %v", err)
	}
	inst, err := store.Install(ctx, tenant, agent, a.ID, "stage", "inst-"+randSuffix(t), granted("model/qwen-max"))
	if err != nil {
		t.Fatalf("install A into stage: %v", err)
	}

	prober := registry.NewHTTPReadinessProber(client)
	bindingOf := func() registry.RuntimeBinding {
		resolved, err := store.ResolveRuntime(ctx, tenant, "stage", agent, b.Digest)
		if err != nil {
			t.Fatalf("resolve the live runtime: %v", err)
		}
		return resolved
	}

	// The agent becomes healthy once its claim loop reaches State. Waiting for
	// that is startup, not an assertion about durations.
	deadline := time.Now().Add(30 * time.Second)
	var lastProbe error
	for time.Now().Before(deadline) {
		if lastProbe = prober.ProbeRuntimeReady(ctx, bindingOf()); lastProbe == nil {
			break
		}
		if err := syscall.Kill(pid, 0); err != nil {
			t.Fatalf("the agent process exited before becoming ready: %v (last probe: %v)", err, lastProbe)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastProbe != nil {
		t.Fatalf("the real Go runtime never passed the probe: %v", lastProbe)
	}

	// A healthy real runtime is accepted by the gate and takes traffic.
	if _, err := store.Promote(ctx, tenant, inst.ID, b.ID); err != nil {
		t.Fatalf("promote onto a healthy real runtime: %v", err)
	}
	if got, _ := store.GetInstallation(ctx, tenant, inst.ID); got.VersionID != b.ID {
		t.Fatalf("healthy real runtime did not get traffic: %s, want %s", got.VersionID, b.ID)
	}

	// Drain the live process with the product's own signal: /readyz must go
	// unready, the gate must refuse, and the supervisor must switch traffic off it.
	if err := syscall.Kill(pid, syscall.SIGUSR1); err != nil {
		t.Fatalf("drain signal: %v", err)
	}
	drained := false
	drainDeadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(drainDeadline) {
		if err := prober.ProbeRuntimeReady(ctx, bindingOf()); err != nil {
			drained = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !drained {
		t.Fatal("a drained real runtime still passed the readiness probe")
	}
	rolled, did, err := store.AutoRollbackIfUnready(ctx, tenant, inst.ID)
	if err != nil {
		t.Fatalf("auto-rollback against a drained real runtime: %v", err)
	}
	if !did {
		t.Fatal("the supervisor kept traffic on a drained real runtime")
	}
	if rolled.VersionID != a.ID {
		t.Fatalf("rollback left traffic on %s, want %s", rolled.VersionID, a.ID)
	}
}
