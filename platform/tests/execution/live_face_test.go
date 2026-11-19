package execution_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// TestLiveNativeQwenThroughOptionAFace composes EVERY proof at the LIVE tier in
// one run (opt-in: AHP_LIVE_QWEN=1): the real native-Qwen+tools executor (Model
// nil -> real Qwen from the scoped Vault runtime secret, no fallback) runs behind
// the execution Public face mounted with transport.Task (option A), reached over
// a REAL mTLS connection carrying a scoped KindTask bearer bound to the
// certificate workload. A SendMessage drives the durable worker to a terminal
// COMPLETED with a model-emitted write_artifact; the artifact is a real MinIO
// object readable (PUT==GET, verified sha256) under the owning scope; and a
// foreign-tenant token querying that task id is NOT_FOUND (-32001) THROUGH the
// face. This is the closest author-side analog of the iv authed live journey;
// accepted is still set only by iv/director (author != verifier).
func TestLiveNativeQwenThroughOptionAFace(t *testing.T) {
	// Fail-closed: the whole point of this test is a REAL model turn behind the
	// option-A face, so an unavailable live dependency must FAIL with a reason.
	// Only AHP_ALLOW_SKIP_LIVE_QWEN=1 downgrades it, and then the run prints
	// SKIP_EXPLICIT so acceptance cannot count it as a pass.
	requireLiveQwen(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	pool, store, ledger, sfx := gwSetup(t)

	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	nonce := hex.EncodeToString(rnd[:])

	ex, err := executor.New(ctx, pool, executor.Config{
		ParserCommand: artifacts.ParserCommand{Path: os.Getenv("AHP_ARTIFACT_PARSER")},
		S3Endpoint:    os.Getenv("AHP_S3_ENDPOINT"),
		S3Region:      os.Getenv("AHP_S3_REGION"),
		S3AccessKey:   os.Getenv("AHP_MINIO_ROOT_USER"),
		S3SecretKey:   os.Getenv("AHP_MINIO_ROOT_PASSWORD"),
		Bucket:        "ahp-exec-face-" + nonce,
		MaxSteps:      8,
	})
	if err != nil {
		t.Fatalf("executor.New: %v", err)
	}
	svc, err := gateway.NewService(store, ledger, ex, gateway.NullNotifier{}, gateway.Config{
		WorkerID: "liveface-" + sfx[:8], Lease: 5 * time.Second, Poll: 20 * time.Millisecond,
		MaxBlock: 3 * time.Second, MaxAttempts: 2, ParkLease: 60 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	// DRAIN VIA THE PRODUCTION HELPER: the same executor.StartWorker the execution
	// role uses to launch the drain loop (iv-119 fix). The face alone never
	// advances a task; this proves the mounted guarded face plus the production
	// worker actually drain SUBMITTED -> COMPLETED with the native executor.
	cancelWorker := executor.StartWorker(ctx, svc)
	defer cancelWorker()

	verifier, signer := ptVerifierSigner(t)
	ca := ptNewCA(t)
	tenant := "t-liveface-" + nonce[:8]
	dep := "d-liveface"
	clientOK := ca.ptLeaf(t, "wl", "spiffe://ahp/tenant/"+tenant+"/deployment/"+dep, false)

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
	mtls := newFace(clientOK)

	prompt := "Call the write_artifact tool exactly once. Artifact name answer.txt, " +
		"mime text/plain. The artifact content MUST be exactly this token and nothing else: TOKEN-" + nonce
	tok := ptTaskToken(t, signer, tenant, dep, "svc", "task-liveface-1")

	id, code := liveSend(t, mtls, tok, prompt)
	if code != http.StatusOK {
		t.Fatalf("SendMessage through option-A face rejected: code=%d", code)
	}
	got := livePoll(t, mtls, tok, id)
	if got == nil {
		t.Fatalf("live native-Qwen did not reach COMPLETED through the option-A mTLS face")
	}
	if got.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("state=%q want COMPLETED", got.Status.State)
	}
	if len(got.Artifacts) == 0 {
		t.Fatalf("no model-emitted artifact on the completed task (proof #1/#2 unmet)")
	}
	art := got.Artifacts[0]
	sha, _ := art.Metadata["sha256"].(string)

	scope := contracts.Scope{Tenant: tenant, Deployment: dep, Subject: "svc"}
	data, ref, err := ex.OpenArtifact(ctx, scope, string(art.ID))
	if err != nil {
		t.Fatalf("OpenArtifact(owning): %v", err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != ref.SHA256 || ref.SHA256 != sha {
		t.Fatalf("PUT!=GET sha bytes=%s ref=%s meta=%s", hex.EncodeToString(sum[:]), ref.SHA256, sha)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		t.Fatal("artifact bytes empty")
	}
	t.Logf("live-face COMPLETED task=%s artifacts=%d artifact_sha256=%s bytes=%d contains_token=%v",
		got.ID, len(got.Artifacts), ref.SHA256, len(data), bytes.Contains(data, []byte(nonce)))

	// Foreign-tenant isolation THROUGH the face: a token bound to a different
	// tenant querying the owning task id must be NOT_FOUND (-32001).
	foreignClient := ca.ptLeaf(t, "evil", "spiffe://ahp/tenant/"+tenant+"-foreign/deployment/"+dep, false)
	mtlsForeign := newFace(foreignClient)
	foreignTok := ptTaskToken(t, signer, tenant+"-foreign", dep, "eve", "task-liveface-foreign")
	if code := liveGetTaskCode(t, mtlsForeign, foreignTok, got.ID); code != -32001 {
		t.Fatalf("foreign-tenant GetTask through face: rpc code=%d want -32001 (NOT_FOUND)", code)
	}
}

func livePost(t *testing.T, srv *httptest.Server, token string, body []byte) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/a2a/public", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func liveSend(t *testing.T, srv *httptest.Server, token, text string) (a2a.TaskID, int) {
	t.Helper()
	pb, _ := json.Marshal(&a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(text)),
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	})
	body, _ := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: "SendMessage", Params: pb})
	_, raw := livePost(t, srv, token, body)
	var out struct {
		Result sendResult `json:"result"`
		Error  *rpcErr    `json:"error"`
	}
	_ = json.Unmarshal(raw, &out)
	if out.Error != nil {
		return "", out.Error.Code
	}
	return out.Result.Task.ID, http.StatusOK
}

func livePoll(t *testing.T, srv *httptest.Server, token string, id a2a.TaskID) *a2a.Task {
	t.Helper()
	deadline := time.Now().Add(150 * time.Second)
	for time.Now().Before(deadline) {
		var cur a2a.Task
		if liveDecodeTask(t, srv, token, id, &cur) {
			if cur.Status.State == a2a.TaskStateCompleted ||
				cur.Status.State == a2a.TaskStateFailed ||
				cur.Status.State == a2a.TaskStateCanceled {
				return &cur
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func liveGetTaskCode(t *testing.T, srv *httptest.Server, token string, id a2a.TaskID) int {
	t.Helper()
	pb, _ := json.Marshal(&a2a.GetTaskRequest{ID: id})
	body, _ := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: "GetTask", Params: pb})
	_, raw := livePost(t, srv, token, body)
	var out struct {
		Error *rpcErr `json:"error"`
	}
	_ = json.Unmarshal(raw, &out)
	if out.Error != nil {
		return out.Error.Code
	}
	return http.StatusOK
}

func liveDecodeTask(t *testing.T, srv *httptest.Server, token string, id a2a.TaskID, into *a2a.Task) bool {
	t.Helper()
	pb, _ := json.Marshal(&a2a.GetTaskRequest{ID: id})
	body, _ := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: "GetTask", Params: pb})
	_, raw := livePost(t, srv, token, body)
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcErr         `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.Error != nil {
		return false
	}
	return json.Unmarshal(out.Result, into) == nil
}
