package sdk_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/sdk"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

// mockStateAPI simulates the State API for SDK integration tests.
//
// The binding rules that matter for the SDK contract mirror internal/stateapi:
// a run's task identity comes from the PRESENTED credential (here: the bearer
// token, which these tests name after the task), a body task_id that disagrees
// is refused, and ClaimRun only ever grants the eligible FIFO head -- of the
// run it names, when it names one. Release can be forced to deny a closing
// write so the executor's conflict semantics are testable without PostgreSQL.
type mockStateAPI struct {
	claimToken  string
	mu          sync.Mutex
	threads     map[string]sdkstate.Thread
	runs        map[string]sdkstate.Run
	order       []string // run keys in creation order: the FIFO the queue respects
	leases      map[string]*sdkstate.Lease
	checkpoints map[string]sdkstate.Checkpoint
	epochSeq    int64
	releaseErr  func(runID string) (int, string)
	handler     http.Handler
}

func newMockStateAPI(t *testing.T) (*mockStateAPI, *httptest.Server) {
	t.Helper()
	m := &mockStateAPI{
		threads:     make(map[string]sdkstate.Thread),
		runs:        make(map[string]sdkstate.Run),
		leases:      make(map[string]*sdkstate.Lease),
		checkpoints: make(map[string]sdkstate.Checkpoint),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/state/CreateThread", m.handleCreateThread)
	mux.HandleFunc("POST /v1/state/CreateRun", m.handleCreateRun)
	mux.HandleFunc("POST /v1/state/GetRun", m.handleGetRun)
	mux.HandleFunc("POST /v1/state/ClaimRun", m.handleClaimRun)
	mux.HandleFunc("POST /v1/state/Heartbeat", m.handleHeartbeat)
	mux.HandleFunc("POST /v1/state/Release", m.handleRelease)
	mux.HandleFunc("POST /v1/state/Suspend", m.handleSuspend)
	mux.HandleFunc("POST /v1/state/PutCheckpoint", m.handlePutCheckpoint)
	mux.HandleFunc("POST /v1/state/GetCheckpoint", m.handleGetCheckpoint)
	mux.HandleFunc("POST /v1/state/RecordIntent", m.handleRecordIntent)
	srv := httptest.NewTLSServer(mux)
	return m, srv
}

func (m *mockStateAPI) handleCreateThread(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ContextID string `json:"context_id"`
		Key       string `json:"key"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	m.mu.Lock()
	defer m.mu.Unlock()
	if th, ok := m.threads[req.ContextID]; ok {
		json.NewEncoder(w).Encode(th)
		return
	}
	th := sdkstate.Thread{ID: "th-" + req.ContextID, ContextID: req.ContextID, Epoch: 0}
	m.threads[req.ContextID] = th
	json.NewEncoder(w).Encode(th)
}

func (m *mockStateAPI) handleCreateRun(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ThreadID string          `json:"thread_id"`
		Key      string          `json:"key"`
		Payload  json.RawMessage `json:"payload"`
		TaskID   string          `json:"task_id"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	presented := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	taskID := req.TaskID
	if taskID == "" {
		taskID = presented
	} else if taskID != presented {
		w.WriteHeader(403)
		json.NewEncoder(w).Encode(map[string]string{"code": "FORBIDDEN", "message": "task_id does not match the presenting task token"})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if run, ok := m.runs[req.Key]; ok {
		json.NewEncoder(w).Encode(run)
		return
	}
	run := sdkstate.Run{ID: "run-" + req.Key, ThreadID: req.ThreadID, TaskID: taskID, Status: sdkstate.RunPending, Payload: req.Payload}
	m.runs[req.Key] = run
	m.order = append(m.order, req.Key)
	json.NewEncoder(w).Encode(run)
}

func (m *mockStateAPI) handleGetRun(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RunID string `json:"run_id"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, run := range m.runs {
		if run.ID == req.RunID {
			json.NewEncoder(w).Encode(run)
			return
		}
	}
	w.WriteHeader(404)
	json.NewEncoder(w).Encode(map[string]string{"code": "NOT_FOUND", "message": "no run"})
}

func (m *mockStateAPI) handleClaimRun(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RunID  string `json:"run_id"`
		Worker string `json:"worker"`
		TTLms  int64  `json:"ttl_ms"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range m.order {
		run := m.runs[key]
		if run.Status != sdkstate.RunPending {
			continue
		}
		if req.RunID != "" {
			// A bound claim never reaches past the FIFO head of the thread: that
			// is what stops a second worker from executing a later run while an
			// earlier one is still unclaimed.
			if run.ID != req.RunID {
				continue
			}
			for _, earlier := range m.order {
				prior := m.runs[earlier]
				if prior.ID == run.ID {
					break
				}
				if prior.ThreadID != run.ThreadID {
					continue
				}
				if prior.Status == sdkstate.RunPending || prior.Status == sdkstate.RunRunning {
					// Something ahead of the named run still owns the thread.
					w.WriteHeader(200)
					json.NewEncoder(w).Encode(map[string]any{"lease": nil, "task_token": ""})
					return
				}
			}
		}
		{
			m.epochSeq++
			epoch := m.epochSeq
			var thread sdkstate.Thread
			for _, candidate := range m.threads {
				if candidate.ID == run.ThreadID {
					thread = candidate
					break
				}
			}
			lease := &sdkstate.Lease{
				Thread:    thread,
				Run:       run,
				Worker:    req.Worker,
				Epoch:     epoch,
				ExpiresAt: time.Now().Add(time.Duration(req.TTLms) * time.Millisecond),
			}
			m.leases[run.ID] = lease
			m.claimToken = "tok-" + run.ID
			run.Status = sdkstate.RunRunning
			for k, stored := range m.runs {
				if stored.ID == run.ID {
					m.runs[k] = run
					break
				}
			}
			resp := map[string]any{
				"lease":      lease,
				"task_token": m.claimToken,
			}
			json.NewEncoder(w).Encode(resp)
			return
		}
	}
	json.NewEncoder(w).Encode(map[string]any{"lease": nil, "task_token": ""})
}

func (m *mockStateAPI) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]any{"expires_at": time.Now().Add(30 * time.Second)})
}

func (m *mockStateAPI) handleRelease(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RunID  string `json:"run_id"`
		Status string `json:"status"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if m.releaseErr != nil {
		if status, code := m.releaseErr(req.RunID); status != 0 {
			w.WriteHeader(status)
			json.NewEncoder(w).Encode(map[string]string{"code": code, "message": "release refused"})
			return
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if lease, ok := m.leases[req.RunID]; ok {
		lease.Run.Status = req.Status
		delete(m.leases, req.RunID)
	}
	for k, run := range m.runs {
		if run.ID == req.RunID {
			run.Status = req.Status
			m.runs[k] = run
			break
		}
	}
	json.NewEncoder(w).Encode(map[string]any{"ok": true})
}

func (m *mockStateAPI) handleSuspend(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]any{"ok": true})
}

func (m *mockStateAPI) handlePutCheckpoint(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Checkpoint sdkstate.Checkpoint `json:"checkpoint"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkpoints[req.Checkpoint.ID] = req.Checkpoint
	// bare response: internal/stateapi answers with the Checkpoint itself, and the
	// canonical consumer reads id/version/parts at the top level.
	json.NewEncoder(w).Encode(req.Checkpoint)
}

func (m *mockStateAPI) handleGetCheckpoint(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ThreadID  string `json:"thread_id"`
		Namespace string `json:"namespace"`
		ID        string `json:"id"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	m.mu.Lock()
	defer m.mu.Unlock()
	if cp, ok := m.checkpoints[req.ID]; ok {
		json.NewEncoder(w).Encode(cp)
		return
	}
	for _, cp := range m.checkpoints {
		json.NewEncoder(w).Encode(cp)
		return
	}
	w.WriteHeader(404)
	json.NewEncoder(w).Encode(map[string]string{"code": "NOT_FOUND", "message": "no checkpoint"})
}

func (m *mockStateAPI) handleRecordIntent(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RunID  string `json:"run_id"`
		Node   string `json:"node"`
		Call   string `json:"call"`
		Kind   string `json:"kind"`
		Target string `json:"target"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	intent := sdkstate.Intent{
		ID:     "int-" + req.RunID + "-" + req.Node + "-" + req.Call,
		RunID:  req.RunID,
		Node:   req.Node,
		Call:   req.Call,
		Kind:   req.Kind,
		Target: req.Target,
		Status: "recorded",
		Digest: "key-" + req.RunID + req.Node + req.Call,
	}
	json.NewEncoder(w).Encode(intent)
}

func newSDKClient(t *testing.T, srv *httptest.Server) *sdkstate.Client {
	t.Helper()
	return newSDKClientToken(t, srv, "test-token")
}

// newSDKClientToken builds a client whose credential NAMES a task: State binds a
// run's task identity to the presented credential, so a test that creates a run
// for task X must present X's credential -- the same rule internal/agentworkload
// enforces on a real agent front.
func newSDKClientToken(t *testing.T, srv *httptest.Server, token string) *sdkstate.Client {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	c, err := sdkstate.New(sdkstate.Config{
		BaseURL:     "https://" + srv.Listener.Addr().String(),
		HTTPClient:  hc,
		TokenSource: func(context.Context) (string, error) { return token, nil },
	})
	if err != nil {
		t.Fatalf("sdkstate.New: %v", err)
	}
	return c
}

func TestAppRunOnceForNamedTask(t *testing.T) {
	mock, srv := newMockStateAPI(t)
	defer srv.Close()
	_ = mock

	client := newSDKClientToken(t, srv, "task-1")
	cfg := &sdk.Config{
		StateClient:       client,
		Worker:            "test-worker",
		LeaseTTL:          10 * time.Second,
		HeartbeatInterval: 2 * time.Second,
	}
	_ = cfg.Validate()

	ctx := context.Background()
	var dummyGraph sdk.GraphFunc = func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
		return nil, nil
	}
	dummyApp, _ := sdk.NewApp(cfg, dummyGraph)
	task, err := dummyApp.EnsureThreadAndRun(ctx, "ctx-1", "task-1", json.RawMessage(`{"msg":"hello"}`))
	if err != nil {
		t.Fatalf("EnsureThreadAndRun: %v", err)
	}
	if task.ThreadID != "th-ctx-1" {
		t.Errorf("ThreadID = %q", task.ThreadID)
	}

	var graphCalled bool
	app, err := sdk.NewApp(cfg, func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
		graphCalled = true
		if rc.Lease == nil {
			t.Error("Lease is nil")
		}
		return json.RawMessage(`{"reply":"hi"}`), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.RunOnceFor(ctx, task)
	if err != nil {
		t.Fatalf("RunOnceFor: %v", err)
	}
	if !graphCalled {
		t.Fatal("graph was not called")
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	if result.Status != sdkstate.RunDone {
		t.Errorf("Status = %s, want done", result.Status)
	}
}

func TestAppInterrupt(t *testing.T) {
	_, srv := newMockStateAPI(t)
	defer srv.Close()

	client := newSDKClientToken(t, srv, "task-i")
	cfg := &sdk.Config{
		StateClient:       client,
		Worker:            "test-worker",
		LeaseTTL:          10 * time.Second,
		HeartbeatInterval: 2 * time.Second,
	}
	_ = cfg.Validate()

	app, err := sdk.NewApp(cfg, func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
		return nil, &sdk.InterruptError{WaitFor: "user-reply"}
	})
	if err != nil {
		t.Fatal(err)
	}
	task, err := app.EnsureThreadAndRun(context.Background(), "ctx-i", "task-i", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.RunOnceFor(context.Background(), task)
	if err != nil {
		t.Fatalf("RunOnceFor: %v", err)
	}
	if result.Status != sdkstate.RunWaiting {
		t.Errorf("Status = %s, want waiting", result.Status)
	}
}

func TestCheckpointSaver(t *testing.T) {
	mock, srv := newMockStateAPI(t)
	defer srv.Close()

	client := newSDKClientToken(t, srv, "task-cp")
	cfg := &sdk.Config{
		StateClient:       client,
		Worker:            "test-worker",
		LeaseTTL:          10 * time.Second,
		HeartbeatInterval: 2 * time.Second,
	}

	ctx := context.Background()
	app, _ := sdk.NewApp(cfg, func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
		cp := sdkstate.Checkpoint{
			ID:        "cp-1",
			Namespace: "graph",
			Version:   1,
			Parts:     []sdkstate.CheckpointPart{{Channel: "state", Version: "v1", Data: []byte(`{"x":1}`)}},
		}
		stored, err := rc.CheckpointSaver.PutCheckpoint(ctx, "graph", cp, "cp-key-1")
		if err != nil {
			t.Errorf("PutCheckpoint: %v", err)
			return nil, err
		}
		if stored.ID != "cp-1" {
			t.Errorf("stored.ID = %q", stored.ID)
		}
		return json.RawMessage(`{"ok":true}`), nil
	})
	task, err := app.EnsureThreadAndRun(ctx, "ctx-cp", "task-cp", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.RunOnceFor(ctx, task); err != nil {
		t.Fatalf("RunOnceFor: %v", err)
	}
	cp := mock.checkpoints["cp-1"]
	if cp.ID != "cp-1" {
		t.Errorf("mock checkpoint not stored: %+v", mock.checkpoints)
	}
}

func TestIntentRecorder(t *testing.T) {
	_, srv := newMockStateAPI(t)
	defer srv.Close()

	client := newSDKClientToken(t, srv, "task-int")
	cfg := &sdk.Config{
		StateClient:       client,
		Worker:            "test-worker",
		LeaseTTL:          10 * time.Second,
		HeartbeatInterval: 2 * time.Second,
	}

	app, _ := sdk.NewApp(cfg, func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
		intent, err := rc.Intents.Record(ctx, "tools", "call-1", "http", "https://example.com/api", json.RawMessage(`{}`))
		if err != nil {
			return nil, err
		}
		if intent.Status != "recorded" {
			t.Errorf("intent.Status = %q", intent.Status)
		}
		if intent.Digest == "" {
			t.Error("intent.Digest is empty")
		}
		return json.RawMessage(`{"key":"` + intent.Digest + `"}`), nil
	})
	task, err := app.EnsureThreadAndRun(context.Background(), "ctx-int", "task-int", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.RunOnceFor(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != sdkstate.RunDone {
		t.Errorf("Status = %s", result.Status)
	}
}

func TestOperationKeyDeterministic(t *testing.T) {
	k1 := sdk.OperationKey("run-1", "node-a", 0)
	k2 := sdk.OperationKey("run-1", "node-a", 0)
	k3 := sdk.OperationKey("run-1", "node-a", 1)
	if k1 != k2 {
		t.Error("same inputs produce different keys")
	}
	if k1 == k3 {
		t.Error("different callIndex produce same key")
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := &sdk.Config{}
	if err := cfg.Validate(); err == nil {
		t.Error("nil StateClient accepted")
	}
	pool := x509.NewCertPool()
	pool.AddCert(&x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true})
	client, _ := sdkstate.New(sdkstate.Config{
		BaseURL:     "https://localhost:1",
		HTTPClient:  &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}},
		TokenSource: func(context.Context) (string, error) { return "x", nil },
	})
	cfg = &sdk.Config{StateClient: client, HeartbeatInterval: 10 * time.Second, LeaseTTL: 5 * time.Second}
	if err := cfg.Validate(); err == nil {
		t.Error("HeartbeatInterval >= LeaseTTL accepted")
	}
	_ = pool
}

// Helper: create a minimal CA + server TLS material for the mock.
func init() {
	_ = ecdsa.GenerateKey
	_ = elliptic.P256
	_ = rand.Reader
	_ = pem.Encode
	_ = pkix.Name{}
}

// An unbound claim returns the FIFO head of ANY thread, which is how an executor
// ends up running a payload it was never asked to run, under that run's fence.
// The SDK therefore has no unbound form at all.
func TestUnboundClaimIsRefused(t *testing.T) {
	_, srv := newMockStateAPI(t)
	defer srv.Close()
	client := newSDKClient(t, srv)
	cfg := &sdk.Config{StateClient: client, Worker: "w", LeaseTTL: 5 * time.Second, HeartbeatInterval: 1 * time.Second}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	lease, err := sdk.ClaimTaskRun(context.Background(), cfg, "")
	if err == nil {
		t.Fatalf("unbound claim returned a lease: %+v", lease)
	}
	if lease != nil || !strings.Contains(err.Error(), "must name the run") {
		t.Errorf("unexpected refusal: %v / %+v", err, lease)
	}
}

func TestRunOnceForRefusesForeignTaskPayloadAndNonHead(t *testing.T) {
	mock, srv := newMockStateAPI(t)
	defer srv.Close()

	head := newSDKClientToken(t, srv, "task-head")
	ctx := context.Background()
	if _, err := head.CreateRun(ctx, func() string {
		th, err := head.CreateThread(ctx, "ctx-two", "two")
		if err != nil {
			t.Fatal(err)
		}
		return th.ID
	}(), "task:task-head", json.RawMessage(`{"i":1}`)); err != nil {
		t.Fatal(err)
	}
	later := head.WithTokenSource(func(context.Context) (string, error) { return "task-later", nil })
	if _, err := later.CreateRun(ctx, "th-ctx-two", "task:task-later", json.RawMessage(`{"i":2}`)); err != nil {
		t.Fatal(err)
	}

	var executed []string
	app, err := sdk.NewApp(&sdk.Config{StateClient: head, Worker: "w-2", LeaseTTL: 5 * time.Second, HeartbeatInterval: 1 * time.Second},
		func(context.Context, *sdk.RunContext) (json.RawMessage, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	graph := func(context.Context, *sdk.RunContext) (json.RawMessage, error) { return nil, nil }
	_ = graph

	// A run belonging to another task, and a payload the run does not carry, are
	// both refused before the lease.
	if _, err := app.RunOnceFor(ctx, sdk.Task{RunID: "run-task:task-head", TaskID: "task-later", ContextID: "ctx-two", Payload: json.RawMessage(`{"i":1}`)}); err == nil {
		t.Error("a run of another task was accepted")
	}
	if _, err := app.RunOnceFor(ctx, sdk.Task{RunID: "run-task:task-head", TaskID: "task-head", ContextID: "ctx-two", Payload: json.RawMessage(`{"i":999}`)}); err == nil {
		t.Error("a payload the run does not carry was accepted")
	}

	// The later run is not this thread's FIFO head: claiming it would steal work
	// from the unclaimed head, so nothing is executed and no status is written.
	stolen, err := app.RunOnceFor(ctx, sdk.Task{RunID: "run-task:task-later", TaskID: "task-later", ContextID: "ctx-two", Payload: json.RawMessage(`{"i":2}`)})
	if err != nil {
		t.Fatalf("non-head claim: %v", err)
	}
	if stolen != nil {
		t.Errorf("claimed past the FIFO head: %+v", stolen)
	}
	if len(executed) != 0 {
		t.Errorf("graph ran on refused attempts: %v", executed)
	}
	mock.mu.Lock()
	defer mock.mu.Unlock()
	for _, key := range []string{"task:task-head", "task:task-later"} {
		if got := mock.runs[key].Status; got != sdkstate.RunPending {
			t.Errorf("run %s moved to %q while every attempt above was refused", key, got)
		}
	}
}

// A rejected closing Release means the run is not the caller's any more: the
// result must not read as done, and the durable run must not be closed by us.
func TestAppRejectedReleaseIsNotCompleted(t *testing.T) {
	mock, srv := newMockStateAPI(t)
	defer srv.Close()
	mock.releaseErr = func(string) (int, string) { return 409, "STALE_EPOCH" }

	client := newSDKClientToken(t, srv, "task-conflict")
	app, err := sdk.NewApp(&sdk.Config{StateClient: client, Worker: "w-3", LeaseTTL: 5 * time.Second, HeartbeatInterval: 1 * time.Second},
		func(context.Context, *sdk.RunContext) (json.RawMessage, error) {
			return json.RawMessage(`{"ok":true}`), nil
		})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	task, err := app.EnsureThreadAndRun(ctx, "ctx-conflict", "task-conflict", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.RunOnceFor(ctx, task)
	if err == nil {
		t.Fatal("a rejected Release was reported as success")
	}
	if result != nil && result.Status == sdkstate.RunDone {
		t.Errorf("result reports %q after a rejected Release", result.Status)
	}
	stored, err := client.GetRun(ctx, task.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status == sdkstate.RunDone {
		t.Errorf("run closed as done although the Release was rejected: %+v", stored)
	}
}

// The loop only serves tasks it was given, and stops when each reached an outcome.
func TestRunTasksServesOnlyNamedTasks(t *testing.T) {
	_, srv := newMockStateAPI(t)
	defer srv.Close()

	client := newSDKClientToken(t, srv, "task-loop-a")
	ctx := context.Background()
	app, err := sdk.NewApp(&sdk.Config{StateClient: client, Worker: "w-loop", LeaseTTL: 5 * time.Second, HeartbeatInterval: 1 * time.Second},
		func(_ context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
			return json.RawMessage(`{"task":` + string(mustJSON(rc.Task.TaskID)) + `}`), nil
		})
	if err != nil {
		t.Fatal(err)
	}
	th, err := client.CreateThread(ctx, "ctx-loop", "loop")
	if err != nil {
		t.Fatal(err)
	}
	a, err := app.EnsureThreadAndRun(ctx, "ctx-loop", "task-loop-a", json.RawMessage(`{"i":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.WithTokenSource(func(context.Context) (string, error) { return "task-loop-b", nil }).CreateRun(ctx, th.ID, "task:task-loop-b", json.RawMessage(`{"i":2}`)); err != nil {
		t.Fatal(err)
	}

	loopCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := app.RunTasks(loopCtx, []sdk.Task{a}, 20*time.Millisecond); err != nil {
		t.Fatalf("RunTasks: %v", err)
	}
	storedA, err := client.GetRun(ctx, a.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if storedA.Status != sdkstate.RunDone {
		t.Errorf("named run status=%q, want done", storedA.Status)
	}
	storedB, err := client.WithTokenSource(func(context.Context) (string, error) { return "task-loop-b", nil }).GetRun(ctx, "run-task:task-loop-b")
	if err != nil {
		t.Fatal(err)
	}
	if storedB.Status != sdkstate.RunPending {
		t.Errorf("an unnamed run was executed: %+v", storedB)
	}
}

func mustJSON(s string) []byte {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return b
}
