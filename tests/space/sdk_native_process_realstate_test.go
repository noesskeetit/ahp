//go:build state_integration

package space_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/sdk"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

// All private keys below are freshly generated test credentials in t.TempDir.
// No user/provider key or native session data is copied into the child process.
type nativeProcessConfig struct {
	StateURL, Tenant, Deployment, Worker, EffectURL, ReadyPath string
	PauseOnAnswer                                              bool
	RecoverOrphans                                             bool
	PublicKey                                                  []byte
	ClientCA, ServerCert, ServerKey, ClientCert, ClientKey     []byte
}

func nativeProcessPair(t *testing.T, certificate tls.Certificate) ([]byte, []byte) {
	t.Helper()
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	var chain []byte
	for _, der := range certificate.Certificate {
		chain = append(chain, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	return chain, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
}

// This helper is only activated in a child test binary. The parent terminates
// it with SIGKILL; it is not a mocked restart or a replacement Go object.
func TestSDKNativeHostProcessHelper(t *testing.T) {
	path := os.Getenv("AHP_NATIVE_PROCESS_FIXTURE")
	if path == "" {
		return
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var c nativeProcessConfig
	if err = json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	root := x509.NewCertPool()
	if !root.AppendCertsFromPEM(c.ServerCert) {
		t.Fatal("invalid server certificate")
	}
	clientCert, err := tls.X509KeyPair(c.ClientCert, c.ClientKey)
	if err != nil {
		t.Fatal(err)
	}
	stateClient, err := sdkstate.New(sdkstate.Config{BaseURL: c.StateURL, HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: root, Certificates: []tls.Certificate{clientCert}}}}, TokenSource: sdk.NativeTaskTokenSource})
	if err != nil {
		t.Fatal(err)
	}
	graph := func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
		cp, err := rc.CheckpointSaver.GetLatest(ctx, "process-hitl")
		if contracts.Code(err) == contracts.ErrNotFound {
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.EffectURL+"/effect", nil)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				return nil, err
			}
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("fixture effect failed")
			}
			_, err = rc.CheckpointSaver.PutCheckpoint(ctx, "process-hitl", sdkstate.Checkpoint{ID: "before-human", Version: 1, Parts: []sdkstate.CheckpointPart{{Channel: "phase", Version: "1", Data: []byte("ticket-created")}}}, "before-human")
			if err != nil {
				return nil, err
			}
			return nil, &sdk.InterruptError{WaitFor: "hitl_response", Payload: json.RawMessage(`{"question":"approve?"}`)}
		}
		if err != nil {
			return nil, err
		}
		if cp.ID != "before-human" {
			return nil, fmt.Errorf("unexpected graph checkpoint")
		}
		items, err := rc.Config.StateClient.ReadInbox(ctx, rc.Task.ThreadID, 0, 10)
		if err != nil {
			return nil, err
		}
		if len(items) != 1 {
			return nil, fmt.Errorf("expected exactly one unacknowledged answer")
		}
		var answer struct {
			RunID, TaskID string
			Input         struct {
				Approved bool `json:"approved"`
			} `json:"input"`
		}
		// Explicit wire tags: run_id/task_id must not be confused with Go names.
		var wire map[string]json.RawMessage
		if json.Unmarshal(items[0].Payload, &wire) != nil {
			return nil, fmt.Errorf("invalid answer")
		}
		_ = json.Unmarshal(wire["run_id"], &answer.RunID)
		_ = json.Unmarshal(wire["task_id"], &answer.TaskID)
		_ = json.Unmarshal(wire["input"], &answer.Input)
		if answer.RunID != rc.Task.RunID || answer.TaskID != rc.Task.TaskID || !answer.Input.Approved {
			return nil, fmt.Errorf("answer binding/contents lost")
		}
		if c.PauseOnAnswer {
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.EffectURL+"/pause", nil)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				return nil, err
			}
			res.Body.Close()
		}
		_, err = rc.CheckpointSaver.PutCheckpoint(ctx, "process-hitl", sdkstate.Checkpoint{ID: "after-human", Parent: cp.ID, Version: 2, Parts: []sdkstate.CheckpointPart{{Channel: "phase", Version: "2", Data: []byte("approved")}}}, "after-human")
		if err != nil {
			return nil, err
		}
		// Host ACKs this answer atomically with its terminal protocol snapshot.
		return json.RawMessage(`{"approved":true}`), nil
	}
	handler, err := sdk.NewDurableA2AHandler(&sdk.Config{StateClient: stateClient, Worker: c.Worker, LeaseTTL: time.Second, HeartbeatInterval: 20 * time.Millisecond}, graph)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(cIssuer, map[string]ed25519.PublicKey{cKID: ed25519.PublicKey(c.PublicKey)})
	ingress, err := sdk.NewNativeIngress(sdk.NativeIngressConfig{Tenant: c.Tenant, Deployment: c.Deployment, GatewayDeployment: "native-gateway", Audience: cAudience, Verifier: verifier, Authorize: func(context.Context, auth.Claims) error { return nil }}, a2asrv.NewJSONRPCHandler(handler))
	if err != nil {
		t.Fatal(err)
	}
	serverCert, err := tls.X509KeyPair(c.ServerCert, c.ServerKey)
	if err != nil {
		t.Fatal(err)
	}
	clientRoots := x509.NewCertPool()
	if !clientRoots.AppendCertsFromPEM(c.ClientCA) {
		t.Fatal("invalid client CA")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: ingress, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientRoots}, ReadHeaderTimeout: 5 * time.Second}
	if c.RecoverOrphans {
		go func() {
			if err := sdk.RunDurableNativeWorker(context.Background(), &sdk.Config{StateClient: stateClient, Worker: c.Worker + "-recovery", LeaseTTL: time.Second, HeartbeatInterval: 20 * time.Millisecond}, graph); err != nil {
				t.Error("background native recovery failed", err)
				_ = server.Close()
			}
		}()
	}
	if err = os.WriteFile(c.ReadyPath, []byte("https://"+listener.Addr().String()), 0600); err != nil {
		t.Fatal(err)
	}
	if err = server.ServeTLS(listener, "", ""); err != nil && err != http.ErrServerClosed {
		t.Fatal(err)
	}
}

type nativeChild struct {
	command *exec.Cmd
	done    chan error
	once    sync.Once
}

func (p *nativeChild) kill() { p.once.Do(func() { _ = p.command.Process.Kill(); <-p.done }) }

func TestSDKNativeA2AHITLAfterProcessKillRealState(t *testing.T) {
	e := cNewEnvProfile(t, true)
	var effects atomic.Int32
	paused := make(chan struct{}, 1)
	effect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/pause" {
			select {
			case paused <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			return
		}
		effects.Add(1)
		_, _ = w.Write([]byte(`{"ticket":"fixture-ticket"}`))
	}))
	defer effect.Close()
	base := nativeProcessConfig{StateURL: e.srv.URL, Tenant: e.tenant, Deployment: e.deployment, EffectURL: effect.URL, PublicKey: e.signer.Public(), ClientCA: e.ca.pem}
	base.ServerCert, base.ServerKey = nativeProcessPair(t, e.srv.TLS.Certificates[0])
	base.ClientCert, base.ClientKey = nativeProcessPair(t, e.srv.Client().Transport.(*http.Transport).TLSClientConfig.Certificates[0])
	root := x509.NewCertPool()
	root.AppendCertsFromPEM(base.ServerCert)
	caller := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: root, Certificates: []tls.Certificate{e.ca.clientCert(t, cWorkloadURI(e.tenant, "native-gateway"))}}}, Timeout: 10 * time.Second}
	defer caller.CloseIdleConnections()
	start := func(worker string, pause bool) (*nativeChild, string) {
		t.Helper()
		dir := t.TempDir()
		c := base
		c.Worker = worker
		c.PauseOnAnswer = pause
		c.RecoverOrphans = worker == "native-process-three"
		c.ReadyPath = filepath.Join(dir, "ready")
		configPath := filepath.Join(dir, "fixture.json")
		b, _ := json.Marshal(c)
		if err := os.WriteFile(configPath, b, 0600); err != nil {
			t.Fatal(err)
		}
		empty := filepath.Join(dir, "empty-workspace")
		if err := os.Mkdir(empty, 0700); err != nil {
			t.Fatal(err)
		}
		command := exec.Command(os.Args[0], "-test.run=^TestSDKNativeHostProcessHelper$", "-test.timeout=60s")
		command.Dir = empty
		command.Env = append(os.Environ(), "AHP_NATIVE_PROCESS_FIXTURE="+configPath)
		log, err := os.OpenFile(filepath.Join(dir, "child.log"), os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		command.Stdout = log
		command.Stderr = log
		if err = command.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		child := &nativeChild{command: command, done: make(chan error, 1)}
		go func() { child.done <- command.Wait(); log.Close() }()
		t.Cleanup(child.kill)
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if b, err := os.ReadFile(c.ReadyPath); err == nil {
				return child, string(b)
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("native child did not publish listening address")
		return nil, ""
	}
	token := e.signTask(t, "native-process-grant")
	client, err := sdkstate.New(sdkstate.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	exchange := func(url, method string, params any, bearer string) (*a2a.Task, error) {
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "test", "method": method, "params": params})
		req, err := http.NewRequestWithContext(e.ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("A2A-Version", "1.0")
		req.Header.Set("Authorization", "Bearer "+bearer)
		response, err := caller.Do(req)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()
		var result struct {
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if err = json.NewDecoder(response.Body).Decode(&result); err != nil {
			return nil, err
		}
		if len(result.Error) > 0 {
			return nil, fmt.Errorf("native protocol refused: %s", result.Error)
		}
		var task a2a.Task
		if method == "SendMessage" {
			var envelope struct {
				Task *a2a.Task `json:"task"`
			}
			err = json.Unmarshal(result.Result, &envelope)
			return envelope.Task, err
		}
		err = json.Unmarshal(result.Result, &task)
		return &task, err
	}
	send := func(url string, msg *a2a.Message) (*a2a.Task, error) {
		return exchange(url, "SendMessage", a2a.SendMessageRequest{Message: msg, Config: &a2a.SendMessageConfig{}}, token)
	}
	firstProcess, url := start("native-process-one", false)
	initial := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("create one fixture ticket"))
	initial.ID = "initial-process"
	initial.ContextID = "ctx-1"
	first, err := send(url, initial)
	if err != nil || first == nil || first.Status.State != a2a.TaskStateInputRequired {
		t.Fatal("initial native question", first, err)
	}
	thread, err := client.GetThreadByContext(e.ctx, "ctx-1")
	if err != nil {
		t.Fatal(err)
	}
	firstEpoch := thread.Epoch
	firstProcess.kill()
	secondProcess, url := start("native-process-two", true)
	loaded, err := exchange(url, "GetTask", a2a.GetTaskRequest{ID: first.ID}, token)
	if err != nil || loaded == nil || loaded.Status.Message == nil || loaded.Status.Message.ID != first.Status.Message.ID {
		t.Fatal("question lost after SIGKILL", loaded, err)
	}
	if other, err := exchange(url, "GetTask", a2a.GetTaskRequest{ID: first.ID}, e.signTask(t, "other-process-grant")); err == nil || other != nil {
		t.Fatal("foreign grant read native task")
	}
	foreignSubject, err := e.signer.Sign(auth.Claims{Audience: cAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment, Subject: "user-bob", Task: "native-process-grant", Root: "run-1", Context: "ctx-1", Grant: "grant-1"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := exchange(url, "GetTask", a2a.GetTaskRequest{ID: first.ID}, foreignSubject); err == nil || other != nil {
		t.Fatal("foreign subject read native task")
	}
	answer := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"approved": true}))
	answer.ID = "answer-process"
	answer.ContextID = "ctx-1"
	answer.TaskID = first.ID
	answerDone := make(chan error, 1)
	go func() { _, err := send(url, answer); answerDone <- err }()
	select {
	case <-paused:
	case err := <-answerDone:
		t.Fatal("answer ended before kill barrier", err)
	case <-time.After(10 * time.Second):
		t.Fatal("answer did not reach execution barrier")
	}
	thread, err = client.GetThread(e.ctx, thread.ID)
	if err != nil || thread.Epoch <= firstEpoch {
		t.Fatal("replacement did not acquire new epoch", err)
	}
	secondEpoch := thread.Epoch
	secondProcess.kill()
	select {
	case err := <-answerDone:
		if err == nil {
			t.Fatal("killed answer falsely succeeded")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("killed connection did not finish")
	}
	// An older pending run of a different runtime must not be consumed by this
	// worker merely because it is in the same deployment's queue.
	unrelatedToken, err := e.signer.Sign(auth.Claims{Audience: cAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject, Task: "unrelated-profile", Root: "run-other", Context: "ctx-unrelated", Grant: "grant-other"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	unrelated := client.WithTokenSource(func(context.Context) (string, error) { return unrelatedToken, nil })
	unrelatedThread, err := unrelated.CreateThread(e.ctx, "ctx-unrelated", "other-thread")
	if err != nil {
		t.Fatal(err)
	}
	unrelatedRun, err := unrelated.CreateRun(e.ctx, unrelatedThread.ID, "other-run", json.RawMessage(`{"type":"other.runtime","text":"never execute here"}`))
	if err != nil {
		t.Fatal(err)
	}
	_, url = start("native-process-three", false)
	// GetTask is read-only. No SendMessage retry is allowed to start recovery.
	var final *a2a.Task
	recoveryDeadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(recoveryDeadline) {
		final, err = exchange(url, "GetTask", a2a.GetTaskRequest{ID: first.ID}, token)
		if err != nil {
			t.Fatal("read during orphan recovery", err)
		}
		if final != nil && final.Status.State == a2a.TaskStateCompleted {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err != nil || final == nil || final.ID != first.ID || final.Status.State != a2a.TaskStateCompleted || len(final.History) != 3 || effects.Load() != 1 {
		t.Fatal("native process recovery failed", final, err, "effects", effects.Load())
	}
	run, err := client.GetRun(e.ctx, string(first.ID))
	if err != nil || run.Status != sdkstate.RunDone {
		t.Fatal("run not completed", run, err)
	}
	thread, err = client.GetThread(e.ctx, thread.ID)
	if err != nil || thread.Epoch <= secondEpoch {
		t.Fatal("orphan was not fenced by replacement", err)
	}
	if _, err := client.PutBatch(e.ctx, sdkstate.Fence{RunID: run.ID, Worker: "native-process-two", Epoch: secondEpoch}, sdkstate.BatchRequest{Key: "stale-process-write", RunStatus: &sdkstate.RunStatusOp{Status: sdkstate.RunRunning}}); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatal("killed worker retained mutation authority", err)
	}
	items, err := client.ReadInbox(e.ctx, thread.ID, 0, 10)
	if err != nil || len(items) != 0 {
		t.Fatal("answer was not acknowledged atomically", items, err)
	}
	for _, msg := range []*a2a.Message{initial, answer} {
		again, err := send(url, msg)
		if err != nil || again == nil || again.ID != first.ID || again.Status.State != a2a.TaskStateCompleted || effects.Load() != 1 {
			t.Fatal("replay repeated effect", err)
		}
	}
	for _, msg := range []*a2a.Message{initial, answer} {
		changed := *msg
		changed.Parts = a2a.ContentParts{a2a.NewTextPart("different content with the same message ID")}
		if result, err := send(url, &changed); err == nil || result != nil {
			t.Fatal("changed message reused an admitted ID")
		}
	}
	other, err := unrelated.GetRun(e.ctx, unrelatedRun.ID)
	if err != nil || other.Status != sdkstate.RunPending {
		t.Fatal("native worker consumed foreign runtime", other, err)
	}
	otherThread, err := unrelated.GetThread(e.ctx, unrelatedThread.ID)
	if err != nil || otherThread.Epoch != 0 {
		t.Fatal("native worker claimed foreign runtime", otherThread, err)
	}
	if effects.Load() != 1 {
		t.Fatal("negative requests repeated external effect")
	}
	t.Log("two actual SIGKILLs, three distinct host processes/empty workspaces, same State task, higher epochs, native mTLS/JWT and State mTLS/PG, one fixture effect; Go graph, not Python LangGraph or live LLM; background orphan recovery without a user resend")
}
