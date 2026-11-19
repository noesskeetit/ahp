package execution_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// mountPublicFace wires the gateway HTTP handler behind an authenticated
// principal injector (the same transport.Principal contract the production
// transport.Task middleware installs after mTLS+token verification, minus the
// PKI which is a separate IAM/deployment gate). It does NOT start any worker, so
// callers control exactly who drives the drain loop.
func mountPublicFace(t *testing.T, svc *gateway.Service) *httptest.Server {
	t.Helper()
	h := gateway.NewHandler(svc, gateway.HandlerConfig{})
	deny := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "internal disabled", http.StatusForbidden)
		})
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
	t.Cleanup(ts.Close)
	return ts
}

func submitTask(t *testing.T, url string, scope contracts.Scope) a2a.TaskID {
	t.Helper()
	res, rpcE := rpcCall(t, url, "SendMessage", &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("produce the artifact")),
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	}, scope)
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
	return created.Task.ID
}

func getTask(t *testing.T, url string, scope contracts.Scope, id a2a.TaskID) (a2a.Task, *rpcErr) {
	t.Helper()
	r, e := rpcCall(t, url, "GetTask", &a2a.GetTaskRequest{ID: id}, scope)
	var cur a2a.Task
	if e != nil {
		return cur, e
	}
	if err := json.Unmarshal(r, &cur); err != nil {
		t.Fatalf("decode get result: %v (raw=%s)", err, string(r))
	}
	return cur, nil
}

// TestStartWorkerDrainsSubmittedTask is the regression for iv-119: the durable
// gateway accepts a task as SUBMITTED and runs it ONLY when a worker claims it
// from the store. With NO worker running, the accepted task is stranded at a
// non-terminal state (control); once the production executor.StartWorker helper
// is started -- the exact helper the execution role launches -- the same task is
// drained to a terminal COMPLETED with a real sealed MinIO artifact. The model is
// a deterministic double (offline/wiring tier); the native model leg is proven in
// the opt-in live test. Author = execution; accepted is set by iv/director.
func TestStartWorkerDrainsSubmittedTask(t *testing.T) {
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	pool, store, ledger, sfx := gwSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	body := "drain-worker-payload-" + sfx[:8]
	ex := newExec(t, ctx, pool, sfx, compModel{body: body})

	mkSvc := func(tag string) *gateway.Service {
		svc, err := gateway.NewService(store, ledger, ex, gateway.NullNotifier{}, gateway.Config{
			WorkerID: tag + "-" + sfx[:8], Lease: 5 * time.Second, Poll: 10 * time.Millisecond,
			MaxBlock: 2 * time.Second, MaxAttempts: 2, ParkLease: 30 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		return svc
	}

	scope := contracts.Scope{Tenant: "t-" + sfx[:12], Deployment: "dep-drain", Subject: "alice"}
	foreign := contracts.Scope{Tenant: "t-foreign-" + sfx[:8], Deployment: "dep-drain", Subject: "bob"}

	// CONTROL: no worker started -> accepted task stays non-terminal (iv-119 defect).
	{
		svc := mkSvc("noctl")
		ts := mountPublicFace(t, svc)
		id := submitTask(t, ts.URL+"/a2a/public", scope)
		deadline := time.Now().Add(1500 * time.Millisecond)
		for time.Now().Before(deadline) {
			cur, e := getTask(t, ts.URL+"/a2a/public", scope, id)
			if e == nil && (cur.Status.State == a2a.TaskStateCompleted ||
				cur.Status.State == a2a.TaskStateFailed || cur.Status.State == a2a.TaskStateCanceled) {
				t.Fatalf("CONTROL broken: task reached terminal %q with NO worker started (drain leaked from elsewhere)", cur.Status.State)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// FIX: the production drain helper runs the worker -> drains to COMPLETED.
	svc := mkSvc("worker")
	cancelWorker := executor.StartWorker(ctx, svc)
	defer cancelWorker()

	ts := mountPublicFace(t, svc)
	id := submitTask(t, ts.URL+"/a2a/public", scope)

	var done a2a.Task
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		cur, e := getTask(t, ts.URL+"/a2a/public", scope, id)
		if e != nil {
			t.Fatalf("GetTask owner: %+v", e)
		}
		if cur.Status.State == a2a.TaskStateCompleted {
			done = cur
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if done.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("executor.StartWorker did not drain the task to COMPLETED: %+v", done.Status)
	}
	if len(done.Artifacts) != 1 {
		t.Fatalf("worker-drained artifacts=%d want 1", len(done.Artifacts))
	}
	aid := string(done.Artifacts[0].ID)
	data, ref, err := ex.OpenArtifact(ctx, scope, aid)
	if err != nil || string(data) != body || ref.SHA256 == "" {
		t.Fatalf("worker-drained artifact: data=%q err=%v ref=%+v", string(data), err, ref)
	}

	// Tenant isolation is unaffected by the worker: a foreign principal is NOT_FOUND.
	if _, e := getTask(t, ts.URL+"/a2a/public", foreign, id); e == nil {
		t.Fatal("foreign GetTask: expected not-found, got success")
	}
	t.Logf("executor.StartWorker drained task=%s to COMPLETED; artifact_sha256=%s bytes=%d; foreign NOT_FOUND ok",
		id, ref.SHA256, len(data))
}

// TestExecutionRoleWiresDrainWorker pins the iv-119 fix at the injection point:
// the ahp_exec execution-role builder (the only place the execution plane is
// mounted, for both role=execution and composed role=all) must, after wiring a
// real executor through the gateway seam, start the drain worker via
// executor.StartWorker. Reading the source is a read-only wiring contract; a
// regression that drops the worker launch fails this offline with no env.
func TestExecutionRoleWiresDrainWorker(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("cannot resolve source path")
	}
	repo := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	p := filepath.Join(repo, "cmd", "ahp", "role_execution.go")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	s := string(raw)
	for _, want := range []string{
		"executor.NewFromEnv(",
		"gateway.NewService(gstore, ledger, exec",
		"executor.StartWorker(ctx, svc)",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("execution role missing wiring %q (iv-119 regression guard)", want)
		}
	}
	if !strings.HasPrefix(strings.TrimSpace(strings.Split(s, "\n")[0]), "//go:build ahp_exec") {
		t.Fatal("role_execution.go must remain gated behind //go:build ahp_exec")
	}
}
