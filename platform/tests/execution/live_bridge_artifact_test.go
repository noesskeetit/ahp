package execution_test

// live_bridge_artifact_test.go -- the LIVE proof of the public A2A path on the
// wiring the stand actually runs (default AHP_EXEC_MODEL_VIA_GATEWAY=1, i.e. the
// Model Gateway bridge). Opt-in with AHP_LIVE_QWEN=1; without it the test FAILS
// with a reason (requireLiveQwen), it never passes silently.
//
// The acceptance criteria are four facts in four different places, and no single
// one of them proves the turn happened:
//
//	1. ahp_model.generations holds a row for THIS task, on the provider model the
//	   bridge reports, with usage_total > 0 and status != FAILED.
//	2. ahp_model.budgets shows the charge attributed to the task's own root
//	   (a2a-task:<task_id>), not an anonymous spend.
//	3. GetTask on the public face reports terminal COMPLETED.
//	4. THAT SAME task carries an artifact whose bytes download from object storage.
//
// Criterion 4 is what this wiring used to miss: the bridge was mounted WITHOUT
// the artifact plane's tool registry, so the model on this face never received a
// tool definition and could not ask for a file, while the Agent Card the same
// process publishes advertises the skill: "COMPLETED with nothing to download"
// was guaranteed, not occasional. (The older claim that this face CANNOT have a
// tool wire is false and is kept here as history only: internal/model reassembles
// streamed tool_calls and gateway_model.go carries definitions both ways -- see
// TestBridgedTurnDispatchesRealToolCall and TestLiveBridgedToolWireSealsModelArtifact.)
// This test pins the FALLBACK door, the model-only mount: the plane exports the
// turn's own answer and the exported bytes are proved to be the answer. The request is
// sent as the RAW v0 wire document a real consumer writes (role "user"), so this
// test also covers the role canonicalization at the ingress.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func TestLiveBridgedSendProducesGenerationBudgetAndArtifact(t *testing.T) {
	requireLiveQwen(t) // also requires the real PostgreSQL and the artifact plane
	ctx, cancel := context.WithTimeout(context.Background(), 330*time.Second)
	defer cancel()

	pool, store, ledger, sfx := gwSetup(t)
	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	nonce := hex.EncodeToString(rnd[:])
	tenant, dep := "t-livebr-"+nonce[:8], "d-livebr"
	scope := contracts.Scope{Tenant: tenant, Deployment: dep, Subject: "a2a-agent"}

	// The same object-storage side the direct executor uses, and the bridge built
	// the way the execution role builds it -- from the environment.
	plane, err := executor.ArtifactPlaneFromEnv(ctx, pool)
	if err != nil {
		t.Fatalf("artifact plane: %v", err)
	}
	bridge, err := execbridge.FromEnvWithArtifacts(ctx, pool, plane)
	if err != nil {
		t.Fatalf("bridge: %v", err)
	}
	if !bridge.ProviderConfigured() {
		t.Fatal("AHP_LIVE_QWEN=1 but the bridge has no provider handle -- no real generation is possible")
	}
	modelName := bridge.Model()

	svc, err := gateway.NewService(store, ledger, bridge, gateway.NullNotifier{}, gateway.Config{
		// The lease must outlive one REAL provider turn even on a saturated node:
		// a lease that expires mid-generation re-claims the task and burns the
		// step's already-dispatched effect key, which would fail the test for the
		// wrong reason. MaxAttempts stays 2 -- retries are still exercised.
		WorkerID: "livebr-" + sfx[:8], Lease: 90 * time.Second, Poll: 20 * time.Millisecond,
		MaxBlock: 5 * time.Second, MaxAttempts: 2, ParkLease: 60 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Drain through the production helper, as the role does.
	cancelWorker := executor.StartWorker(ctx, svc)
	defer cancelWorker()

	verifier, signer := ptVerifierSigner(t)
	ca := ptNewCA(t)
	newFace := func(client tls.Certificate) *httptest.Server {
		authorize := func(context.Context, auth.Claims, string) error { return nil }
		deny := func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "internal disabled", http.StatusForbidden) })
		}
		srv, err := gateway.NewServer(gateway.NewHandler(svc, gateway.HandlerConfig{}), gateway.ServerConfig{
			Public:       gateway.AuthConfig{Middleware: transport.Task(verifier, "ahp", "execution", authorize)},
			Internal:     gateway.AuthConfig{Middleware: deny},
			PublicPath:   "/a2a/public",
			InternalPath: "/a2a/internal",
			HealthPath:   "/a2a/healthz",
		})
		if err != nil {
			t.Fatal(err)
		}
		return ptMTLSServer(t, ca, client, srv)
	}
	mtls := newFace(ca.ptLeaf(t, "wl", workloadURI(tenant, dep), false))
	token := ptTaskToken(t, signer, tenant, dep, "a2a-agent", "task-livebr-"+nonce)

	// Deliberately NO instruction to call a tool: this test pins the model-only
	// mount (FromEnvWithArtifacts, tools=nil), where the artifact can only come
	// from the plane's own export. The nonce inside the answer is what proves the
	// sealed bytes came from THIS turn. The tool-mounted face -- where the model
	// writes the file itself -- is proved by
	// TestLiveBridgedToolWireSealsModelArtifact.
	prompt := "Ответь ровно одним коротким предложением, содержащим токен TOKEN-" + nonce
	taskID, code, refused := vrRawSendV0(t, mtls, token, "livebr-msg-"+nonce, prompt)
	if code != http.StatusOK || refused != "" {
		t.Fatalf("raw v0 SendMessage through the option-A face was refused: code=%d message=%q", code, refused)
	}
	if taskID == "" {
		t.Fatal("send returned no task id")
	}

	got := livePoll(t, mtls, token, taskID)
	if got == nil {
		t.Fatalf("bridged task %s never reached a terminal state", taskID)
	}
	answer := ""
	if got.Status.Message != nil {
		for _, p := range got.Status.Message.Parts {
			answer += p.Text()
		}
	}
	if got.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("bridged task %s ended %s (status message %q)", taskID, got.Status.State, answer)
	}
	if len(got.Artifacts) == 0 {
		t.Fatalf("task %s completed with no artifact: the card advertises a skill this face did not serve", taskID)
	}
	if strings.TrimSpace(answer) == "" {
		t.Fatalf("task %s completed without an answer text to export", taskID)
	}

	// Criterion 4: the artifact of THAT task downloads and matches its seal.
	art := got.Artifacts[0]
	sha, _ := art.Metadata["sha256"].(string)
	rc, ref, err := plane.Store.Open(ctx, scope, string(art.ID))
	if err != nil {
		t.Fatalf("artifact %s is not downloadable: %v", art.ID, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read artifact bytes: %v", err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		t.Fatalf("artifact %s sealed but empty", art.ID)
	}
	if !bytes.Contains(data, []byte("TOKEN-"+nonce)) {
		t.Fatalf("artifact %s does not carry this turn's answer (nonce missing in %d bytes)", art.ID, len(data))
	}
	if sum := sha256.Sum256(data); hex.EncodeToString(sum[:]) != ref.SHA256 || ref.SHA256 != sha {
		t.Fatalf("PUT!=GET bytes=%s ref=%s meta=%s", hex.EncodeToString(sum[:]), ref.SHA256, sha)
	}
	t.Logf("A2A_LIVE task=%s artifact=%s bytes=%d sha256=%.12s contains_token=%v",
		taskID, art.ID, len(data), ref.SHA256, bytes.Contains(data, []byte(nonce)))

	// Criterion 1: a real generation for this task on the provider model.
	var (
		genModel, genStatus string
		usage               int
	)
	if err := pool.QueryRow(ctx, `SELECT model, status, usage_total FROM ahp_model.generations
		WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND task=$4
		ORDER BY created_at DESC LIMIT 1`,
		scope.Tenant, scope.Deployment, scope.Subject, string(taskID)).Scan(&genModel, &genStatus, &usage); err != nil {
		t.Fatalf("no ahp_model.generations row for task %s: %v", taskID, err)
	}
	if genStatus == "FAILED" {
		t.Fatalf("generation for task %s is FAILED", taskID)
	}
	if genModel != modelName {
		t.Fatalf("generation model = %q, want the bridge's provider model %q", genModel, modelName)
	}
	if usage <= 0 {
		t.Fatalf("generation usage_total = %d: a real provider turn must report usage", usage)
	}

	// Criterion 2: the spend is attributed to the task's own budget root.
	var spent int64
	if err := pool.QueryRow(ctx, `SELECT spent FROM ahp_model.budgets
		WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND task=$4 AND key=$5`,
		scope.Tenant, scope.Deployment, scope.Subject, "a2a-task:"+string(taskID), execbridge.RootBudgetKey).
		Scan(&spent); err != nil {
		t.Fatalf("no budget charge under a2a-task:%s: %v", taskID, err)
	}
	if spent <= 0 {
		t.Fatalf("budget root a2a-task:%s spent=%d, want a real charge", taskID, spent)
	}
	t.Logf("A2A_LIVE generation model=%s usage_total=%d budget_root=a2a-task:%s spent=%d",
		genModel, usage, taskID, spent)
	// The contract asks for machine-readable evidence of THIS leg: the http code
	// the face really answered, the task it created, the artifact reference the
	// bytes were verified against, the provider row and the budget charge -- one
	// line a verifier can diff against ahp_model without reading a test log.
	writeEvidence(t, "a2a.live_send_to_terminal", map[string]any{
		"transport":             "mTLS option-A face + task token",
		"wire_shape":            "a2a v0-shaped SendMessage body",
		"send_http_status":      code,
		"task_id":               taskID,
		"terminal_state":        string(got.Status.State),
		"answer_bytes":          len(answer),
		"artifact_id":           string(art.ID),
		"artifact_name":         art.Name,
		"artifact_sha256":       ref.SHA256,
		"artifact_bytes":        len(data),
		"artifact_carries_turn": true,
		"generation_model":      genModel,
		"generation_status":     genStatus,
		"usage_total":           usage,
		"budget_root":           "a2a-task:" + string(taskID),
		"budget_spent":          spent,
	})

	// Negative that must not regress: a foreign scope cannot read this task.
	foreignFace := newFace(ca.ptLeaf(t, "evil", workloadURI(tenant+"-foreign", dep), false))
	foreignTok := ptTaskToken(t, signer, tenant+"-foreign", dep, "eve", "task-livebr-foreign")
	if code := liveGetTaskCode(t, foreignFace, foreignTok, taskID); code != -32001 {
		t.Fatalf("foreign-scope GetTask through the face = %d, want -32001", code)
	}
	writeEvidence(t, "a2a.live_foreign_scope_refused", map[string]any{
		"task_id": taskID, "scope": "foreign tenant/subject", "rpc_code": -32001,
	})
}
