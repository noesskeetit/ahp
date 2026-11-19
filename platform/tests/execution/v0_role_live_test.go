package execution_test

// v0_role_live_test.go -- the LIVE-tier analog of the stand probe
// (tools/a2a-public-send-probe.sh), opt-in with AHP_LIVE_QWEN=1. The accepted
// live face test posted a canonical ROLE_USER message, which is exactly why the
// suite stayed green while every real consumer of this plane -- which spells the
// role "user" -- got TASK_STATE_FAILED "execbridge: no usable text input". This
// test posts the RAW v0 wire document a consumer actually writes, through the
// same option-A mTLS face, into the real native-Qwen executor, and requires a
// terminal COMPLETED carrying a real artifact.
//
// Author-side proof only: accepted is set by director/IV on the relaunched pin.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
	"github.com/ahp-platform/ahp/pkg/transport"
)

// vrRawSendV0 posts a hand-written v0-shaped SendMessage (role "user", parts
// tagged with "kind"). It returns the created task id, the effective code and
// the refusal text. The code is deliberately unambiguous: on success it is the
// HTTP status (200), on refusal the JSON-RPC code (negative) -- and the refusal
// text is carried too, because an assertion that can only print a bare number
// turns a real product refusal into an unreadable test failure.
func vrRawSendV0(t *testing.T, srv *httptest.Server, token, msgID, text string) (a2a.TaskID, int, string) {
	t.Helper()
	params, err := json.Marshal(map[string]any{
		"message": map[string]any{
			"kind":      "message",
			"role":      "user",
			"messageId": msgID,
			"parts":     []any{map[string]any{"kind": "text", "text": text}},
		},
		"configuration": map[string]any{"returnImmediately": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: "SendMessage", Params: params})
	if err != nil {
		t.Fatal(err)
	}
	code, raw := livePost(t, srv, token, body)
	var out struct {
		Result sendResult `json:"result"`
		Error  *rpcErr    `json:"error"`
	}
	_ = json.Unmarshal(raw, &out)
	if out.Error != nil {
		return "", out.Error.Code, out.Error.Message
	}
	return out.Result.Task.ID, code, ""
}

// liveMessageText renders the text of a task's status message. %+v of an
// a2a.Message prints part POINTERS, which is how a live failure read
// "Parts:[0x2e12b52fc480]" and told nobody the reason.
func liveMessageText(m *a2a.Message) string {
	if m == nil {
		return ""
	}
	out := ""
	for _, p := range m.Parts {
		out += p.Text()
	}
	return out
}

func TestLiveV0RoleThroughFaceProducesArtifact(t *testing.T) {
	requireLiveQwen(t)
	requireArtifactPlane(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	pool, store, ledger, sfx := gwSetup(t)
	buf := make([]byte, 6)
	_, _ = rand.Read(buf)
	nonce := hex.EncodeToString(buf)

	ex, err := executor.New(ctx, pool, executor.Config{
		ParserCommand: artifacts.ParserCommand{Path: os.Getenv("AHP_ARTIFACT_PARSER")},
		S3Endpoint:    os.Getenv("AHP_S3_ENDPOINT"),
		S3Region:      os.Getenv("AHP_S3_REGION"),
		S3AccessKey:   os.Getenv("AHP_MINIO_ROOT_USER"),
		S3SecretKey:   os.Getenv("AHP_MINIO_ROOT_PASSWORD"),
		Bucket:        "ahp-exec-v0role-" + nonce,
		MaxSteps:      8,
	})
	if err != nil {
		t.Fatalf("executor.New: %v", err)
	}
	svc, err := gateway.NewService(store, ledger, ex, gateway.NullNotifier{}, gateway.Config{
		WorkerID: "v0rolelive-" + sfx[:8], Lease: 5 * time.Second, Poll: 20 * time.Millisecond,
		MaxBlock: 3 * time.Second, MaxAttempts: 2, ParkLease: 60 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer executor.StartWorker(ctx, svc)()

	verifier, signer := ptVerifierSigner(t)
	ca := ptNewCA(t)
	tenant := "t-v0rolelive-" + nonce[:8]
	dep := "d-v0rolelive"
	client := ca.ptLeaf(t, "wl", "spiffe://ahp/tenant/"+tenant+"/deployment/"+dep, false)
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	deny := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "internal disabled", http.StatusForbidden) })
	}
	router, err := gateway.NewServer(gateway.NewHandler(svc, gateway.HandlerConfig{}), gateway.ServerConfig{
		Public:       gateway.AuthConfig{Middleware: transport.Task(verifier, "ahp", "execution", authorize)},
		Internal:     gateway.AuthConfig{Middleware: deny},
		PublicPath:   "/a2a/public",
		InternalPath: "/a2a/internal",
		HealthPath:   "/a2a/healthz",
	})
	if err != nil {
		t.Fatal(err)
	}
	face := ptMTLSServer(t, ca, client, router)

	token := ptTaskToken(t, signer, tenant, dep, "a2a-agent", "task-v0role-"+nonce)
	prompt := "Call the write_artifact tool exactly once. Artifact name paragraph.txt, " +
		"mime text/plain. The artifact content MUST be exactly this token and nothing else: V0ROLE-" + nonce

	id, code, refused := vrRawSendV0(t, face, token, "v0role-"+nonce, prompt)
	if code != http.StatusOK || refused != "" {
		t.Fatalf("a v0-spelled SendMessage was rejected by the live face: code=%d message=%q", code, refused)
	}
	if id == "" {
		t.Fatal("a v0-spelled SendMessage created no task")
	}
	got := livePoll(t, face, token, id)
	if got == nil {
		t.Fatalf("task %s never reached a terminal state", id)
	}
	if got.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("state = %s, want COMPLETED: the v0 role spelling must not strand the turn; status message=%q",
			got.Status.State, liveMessageText(got.Status.Message))
	}
	if len(got.Artifacts) == 0 {
		t.Fatalf("completed with no artifact: the model turn ran but produced no artifact")
	}
}
