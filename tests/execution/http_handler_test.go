package execution_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// principalInjector simulates the transport layer AFTER mTLS/Ed25519
// verification: it installs a scope-complete Principal on the request context
// through the exported transport.WithPrincipal seam -- the same authenticated-
// principal contract gateway's own accepted integration tests rely on. This lets
// the REAL a2a JSON-RPC handler and the canonical executor run behind an
// authenticated principal without standing up the production PKI (a separate
// IAM/deployment gate owned elsewhere). Scope is taken from request headers so
// one server can serve several authenticated identities for the isolation test.
func principalInjector(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sc := contracts.Scope{
			Tenant:     r.Header.Get("X-Test-Tenant"),
			Deployment: r.Header.Get("X-Test-Deployment"),
			Subject:    r.Header.Get("X-Test-Subject"),
		}
		ctx := transport.WithPrincipal(r.Context(), transport.Principal{
			Claims: auth.Claims{Kind: auth.KindTask, Tenant: sc.Tenant, Deployment: sc.Deployment, Subject: sc.Subject, Task: "task-" + sc.Subject, Context: "ctx-" + sc.Subject, Root: "root-" + sc.Subject, Grant: "grant-" + sc.Subject},
			Scope:  sc,
		})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type rpcReq struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func rpcCall(t *testing.T, url, method string, params any, sc contracts.Scope) (json.RawMessage, *rpcErr) {
	t.Helper()
	pb, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	bb, _ := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: method, Params: pb})
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(bb))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Test-Tenant", sc.Tenant)
	req.Header.Set("X-Test-Deployment", sc.Deployment)
	req.Header.Set("X-Test-Subject", sc.Subject)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcErr         `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode rpc response: %v", err)
	}
	return out.Result, out.Error
}

// sendResult mirrors the a2a SendMessageResult union wire shape {"task":{...}}.
type sendResult struct {
	Task *a2a.Task `json:"task"`
}

func TestCompositionGatewayPublicHTTPHandler(t *testing.T) {
	pool, store, ledger, sfx := gwSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	body := "gateway-http-payload-" + sfx[:8]
	ex := newExec(t, ctx, pool, sfx, compModel{body: body})

	svc, err := gateway.NewService(store, ledger, ex, gateway.NullNotifier{}, gateway.Config{
		WorkerID: "http-" + sfx[:8], Lease: 5 * time.Second, Poll: 10 * time.Millisecond,
		MaxBlock: 3 * time.Second, MaxAttempts: 2, ParkLease: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	go svc.Run(ctx)

	h := gateway.NewHandler(svc, gateway.HandlerConfig{})
	deny := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "denied", http.StatusForbidden) })
	}
	srv, err := gateway.NewServer(h, gateway.ServerConfig{
		Public:       gateway.AuthConfig{Middleware: principalInjector},
		Internal:     gateway.AuthConfig{Middleware: deny},
		PublicPath:   "/a2a/public",
		InternalPath: "/a2a/internal",
		HealthPath:   "/a2a/healthz",
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()
	url := ts.URL + "/a2a/public"

	scope := contracts.Scope{Tenant: "t-" + sfx[:12], Deployment: "dep-http", Subject: "alice"}
	foreign := contracts.Scope{Tenant: "t-foreign-" + sfx[:8], Deployment: "dep-http", Subject: "bob"}

	send := &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("produce the artifact")),
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	}
	res, rpcE := rpcCall(t, url, "SendMessage", send, scope)
	if rpcE != nil {
		t.Fatalf("SendMessage error: %+v", rpcE)
	}
	var created sendResult
	if err := json.Unmarshal(res, &created); err != nil {
		t.Fatalf("decode send result: %v (raw=%s)", err, string(res))
	}
	if created.Task == nil || created.Task.ID == "" {
		t.Fatalf("no task id in send result (raw=%s)", string(res))
	}

	var done a2a.Task
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		r, e := rpcCall(t, url, "GetTask", &a2a.GetTaskRequest{ID: created.Task.ID}, scope)
		if e != nil {
			t.Fatalf("GetTask owner: %+v", e)
		}
		var cur a2a.Task
		if err := json.Unmarshal(r, &cur); err != nil {
			t.Fatalf("decode get result: %v (raw=%s)", err, string(r))
		}
		if cur.Status.State == a2a.TaskStateCompleted {
			done = cur
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if done.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("task not completed over HTTP: %+v", done.Status)
	}
	if len(done.Artifacts) != 1 {
		t.Fatalf("http artifacts=%d want 1", len(done.Artifacts))
	}
	aid := string(done.Artifacts[0].ID)

	// The HTTP-completed task is backed by a real sealed MinIO artifact (PUT==GET).
	data, ref, err := ex.OpenArtifact(ctx, scope, aid)
	if err != nil || string(data) != body || ref.SHA256 == "" {
		t.Fatalf("http task artifact: data=%q err=%v ref=%+v", string(data), err, ref)
	}

	// Tenant isolation over the wire: a foreign authenticated principal gets
	// task-not-found for the same id, never the artifact.
	_, e := rpcCall(t, url, "GetTask", &a2a.GetTaskRequest{ID: created.Task.ID}, foreign)
	if e == nil {
		t.Fatal("foreign GetTask: expected not-found error, got success")
	}
	t.Logf("foreign GetTask rpc error: code=%d message=%q", e.Code, e.Message)
	if !strings.Contains(strings.ToLower(e.Message), "not found") {
		t.Fatalf("foreign GetTask error not not-found: %+v", e)
	}
}
