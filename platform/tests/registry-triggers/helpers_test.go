package registrytriggers_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// requireEnv implements the loop contract of reports/mac-director/pitfalls-product.md
// §2: an env-gated leg must not pass silently. A missing gate is a failure, and
// the message says exactly what to source.
func requireEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is not set: the test loop must source /tmp/ahp-test-env.sh before running (pitfalls §2); refusing to skip silently", name)
	}
	return value
}

// repoRoot resolves the checkout root from this test file's location.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

// deployCADir resolves the stand CA directory. AHP_DEPLOYED_CA_DIR overrides the
// default <repoRoot>/infra/certs/ca, so the harness can run from a worktree that
// did NOT generate the stand's certificates. Default behaviour is unchanged.
func deployCADir(t *testing.T) string {
	t.Helper()
	if v := strings.TrimSpace(os.Getenv("AHP_DEPLOYED_CA_DIR")); v != "" {
		return v
	}
	// Keep the mint directory coherent with the trust file when only the file
	// path is given, so clientAuth certs chain to the CA the client already trusts.
	if v := strings.TrimSpace(os.Getenv("AHP_DEPLOYED_CA")); v != "" {
		// One value for the whole harness: a directory names itself, a file names
		// the directory that holds it -- the same leniency the Node token provider
		// has, so nobody has to guess which spelling this harness expects.
		if st, err := os.Stat(v); err == nil && st.IsDir() {
			return v
		}
		return filepath.Dir(v)
	}
	return filepath.Join(repoRoot(t), "infra", "certs", "ca")
}

// deployCAFile resolves the stand CA certificate the client trusts. AHP_DEPLOYED_CA
// may point at a ca.crt or at the directory holding one; with nothing set it is
// <deployCADir>/ca.crt. Default unchanged.
func deployCAFile(t *testing.T) string {
	t.Helper()
	if v := strings.TrimSpace(os.Getenv("AHP_DEPLOYED_CA")); v != "" {
		if st, err := os.Stat(v); err == nil && st.IsDir() {
			return filepath.Join(v, "ca.crt")
		}
		return v
	}
	return filepath.Join(deployCADir(t), "ca.crt")
}

// devSigner mirrors cmd/ahp devAuth and scripts/tokengen: the dev Ed25519 key is
// derived from AHP_DEV_JWT_SEED and signs under kid "dev-1". The seed is read
// from the environment and never printed.
func devSigner(t *testing.T) *auth.Signer {
	t.Helper()
	raw := requireEnv(t, "AHP_DEV_JWT_SEED")
	seed, err := hex.DecodeString(raw)
	if err != nil {
		t.Fatalf("AHP_DEV_JWT_SEED must be hex: %v", err)
	}
	if len(seed) != ed25519.SeedSize {
		t.Fatal("AHP_DEV_JWT_SEED must decode to 32 bytes")
	}
	signer, err := auth.NewSigner("ahp", "dev-1", ed25519.NewKeyFromSeed(seed))
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	return signer
}

// mintTask mints a task token for one (tenant, deployment, subject, task, ctx).
// Every call yields different bytes, which is what lets a test prove a token was
// re-issued rather than reused.
func mintTask(t *testing.T, signer *auth.Signer, tenant, deployment, subject, task, ctxID string) string {
	t.Helper()
	if strings.TrimSpace(os.Getenv("AHP_DEPLOYED_URL")) != "" {
		// A deployed Identity-backed front checks the live grant, not just the
		// signature. Provision fixture authority through its real owner API.
		ctx := context.Background()
		fixturePool := newPool(t, requireEnv(t, "AHP_TEST_DATABASE_URL"))
		ids := identity.New(fixturePool)
		if err := ids.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		scope := contracts.Scope{Tenant: tenant, Deployment: deployment, Subject: subject}
		// The fixture is an operator for its nonce-bound tenant/deployment. A
		// synthetic deployed front also requires explicit resource rules; a
		// signed grant alone must never bypass that second policy boundary.
		if os.Getenv("AHP_TEST_RESOURCE_IAM") == "synthetic" {
			policy := identity.NewSyntheticResourcePolicy(fixturePool)
			for _, capability := range []string{"execution", "state", "model", "tools", "artifacts", "registry"} {
				for _, resource := range []identity.Resource{{Kind: "tenant", ID: tenant}, {Kind: "deployment", ID: deployment}} {
					rule := identity.ResourceRule{Tenant: tenant, Subject: subject, Resource: resource, Action: capability, Allowed: true}
					if _, err := policy.PutRule(ctx, rule, 0); err != nil {
						if contracts.Code(err) != contracts.ErrConflict {
							t.Fatal(err)
						}
						if err := policy.Require(ctx, identity.ResourceRequest{Scope: scope, Resource: resource, Action: capability}); err != nil {
							t.Fatalf("existing fixture policy refuses capability; will not overwrite revocation: %v", err)
						}
					}
				}
			}
		}
		_, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: task, Context: ctxID,
			Deployment: deployment, TTL: 15 * time.Minute,
			Capabilities: []string{"execution", "state", "model", "tools", "artifacts", "registry"}})
		if err != nil {
			t.Fatalf("live fixture grant: %v", err)
		}
		token, err := ids.MintTask(ctx, scope, task, signer, "ahp", 15*time.Minute)
		if err != nil {
			t.Fatalf("live fixture task token: %v", err)
		}
		return token
	}
	token, err := signer.Sign(auth.Claims{
		Audience:   "ahp",
		Kind:       auth.KindTask,
		Tenant:     tenant,
		Deployment: deployment,
		Subject:    subject,
		Task:       task,
		Root:       task,
		Context:    ctxID,
		Grant:      "grant-" + task,
	}, 900*time.Second)
	if err != nil {
		t.Fatalf("mint task token: %v", err)
	}
	return token
}

// frontClient is a minimal mTLS + Bearer client for the deployed composed front.
type frontClient struct {
	t       *testing.T
	base    string
	port    string
	host    string
	hc      *http.Client
	certDir string
}

// newFrontClient issues a client certificate bound to (tenant, deployment) from
// the stand's dev CA and returns a client that presents it.
func newFrontClient(t *testing.T, base, tenant, deployment string) *frontClient {
	t.Helper()
	root := repoRoot(t)
	certDir := filepath.Join(t.TempDir(), "certs")
	if err := os.MkdirAll(certDir, 0o755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "infra", "gen-client.sh")
	if _, err := os.Stat(script); err != nil {
		t.Fatalf("infra/gen-client.sh missing: %v", err)
	}
	caFile := deployCAFile(t)
	if _, err := os.Stat(caFile); err != nil {
		t.Fatalf("stand CA missing at %s (set AHP_DEPLOYED_CA / AHP_DEPLOYED_CA_DIR): %v", caFile, err)
	}
	// Mint the client certificate under the STAND's CA, not this checkout's, so the
	// leg is reproducible from any worktree that points AHP_DEPLOYED_CA(_DIR) (and
	// optionally AHP_CA_DIR/AHP_CA_FILE) at the stand CA. Explicit env wins; with no
	// env this resolves to the same repoRoot/infra/certs/ca the script already used,
	// so the default path is byte-identical.
	mintEnv := os.Environ()
	if _, ok := os.LookupEnv("AHP_CA_DIR"); !ok {
		mintEnv = append(mintEnv, "AHP_CA_DIR="+deployCADir(t))
	}
	if _, ok := os.LookupEnv("AHP_CA_FILE"); !ok {
		if v := strings.TrimSpace(os.Getenv("AHP_DEPLOYED_CA")); v != "" {
			mintEnv = append(mintEnv, "AHP_CA_FILE="+v)
		}
	}
	cmd := exec.Command("bash", script, tenant, deployment, certDir)
	cmd.Env = mintEnv
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("gen-client failed: %v\n%s", err, redact(string(out)))
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(certDir, "client.crt"), filepath.Join(certDir, "client.key"))
	if err != nil {
		t.Fatalf("load client cert: %v", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("CA did not parse")
	}
	u := strings.TrimSuffix(base, "/")
	parts := strings.SplitN(strings.TrimPrefix(u, "https://"), ":", 2)
	host := parts[0]
	port := "443"
	if len(parts) > 1 {
		port = parts[1]
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, RootCAs: pool, ServerName: "localhost"}
	return &frontClient{
		t: t, base: u, host: host, port: port, certDir: certDir,
		hc: &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}, Timeout: 180 * time.Second},
	}
}

// post sends a JSON POST with an optional bearer token.
func (c *frontClient) post(path, token string, body any, timeout time.Duration) (int, []byte, error) {
	c.t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
	}
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("content-type", "application/json")
	if token != "" {
		req.Header.Set("authorization", "Bearer "+token)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
	return resp.StatusCode, raw, err
}

// a2a performs one JSON-RPC call on the public A2A entry.
func (c *frontClient) a2a(method string, params any, token string, timeout time.Duration) (int, []byte, error) {
	c.t.Helper()
	return c.post("/a2a/public", token, map[string]any{
		"jsonrpc": "2.0", "id": time.Now().UnixNano() % 1e9, "method": method, "params": params,
	}, timeout)
}

var hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)

// harvested is the journey-strength evidence pulled out of an A2A task: the
// fields the canonical journey asserts (state, artifact, steps, close reason).
type harvested struct {
	State        string
	ArtifactID   string
	ArtifactRefs int
	Steps        int
	CloseReason  string
	RecordedSHA  string
	ContextID    string
	TaskID       string
}

// harvest walks the task document the same way scripts/journey-deployed.mjs
// does, so the Go leg asserts exactly the canonical journey fields.
func harvest(task map[string]any) harvested {
	out := harvested{}
	var walk func(any)
	walk = func(x any) {
		switch v := x.(type) {
		case string:
			if hex64.MatchString(v) && out.RecordedSHA == "" {
				out.RecordedSHA = v
			}
		case map[string]any:
			for k, val := range v {
				switch {
				case (k == "artifact_id" || k == "artifactId"):
					if s, ok := val.(string); ok && out.ArtifactID == "" {
						out.ArtifactID = s
					}
				case k == "sha256":
					if s, ok := val.(string); ok && hex64.MatchString(s) && out.RecordedSHA == "" {
						out.RecordedSHA = s
					}
				case k == "artifacts":
					if arr, ok := val.([]any); ok && len(arr) > out.ArtifactRefs {
						out.ArtifactRefs = len(arr)
					}
				case k == "steps_consumed":
					if f, ok := val.(float64); ok && int(f) > out.Steps {
						out.Steps = int(f)
					}
				case k == "close_reason":
					if s, ok := val.(string); ok && s != "" && out.CloseReason == "" {
						out.CloseReason = s
					}
				}
				walk(val)
			}
		case []any:
			for _, item := range v {
				walk(item)
			}
		}
	}
	walk(task)
	if status, ok := task["status"].(map[string]any); ok {
		out.State, _ = status["state"].(string)
	}
	out.TaskID, _ = task["id"].(string)
	out.ContextID, _ = task["contextId"].(string)
	return out
}

func isDone(state string) bool {
	switch strings.ToLower(state) {
	case "completed", "failed", "canceled", "rejected",
		"task_state_completed", "task_state_failed", "task_state_canceled", "task_state_rejected":
		return true
	}
	return false
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// redact removes anything that looks like a secret before it reaches the log.
func redact(s string) string {
	tokens := regexp.MustCompile(`[A-Za-z0-9._+/=-]{24,}`)
	return tokens.ReplaceAllString(s, "<redacted>")
}

// uniqueSuffix gives each run its own identifiers so no residue from a previous
// run can be mistaken for evidence (pitfalls §9).
func uniqueSuffix() string {
	return fmt.Sprintf("%d", time.Now().UnixNano()%1e12)
}

func newPool(t *testing.T, url string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("postgres unreachable: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
