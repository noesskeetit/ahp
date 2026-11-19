//go:build state_integration

package space_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/internal/agentworkload"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/sdk"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Ingress matches the native SDK deployment boundary: verified gateway mTLS,
// then task JWT for this workload. Readiness discloses only artifact identity.
func nativeSDKProtocolServer(t *testing.T, e *cEnv, inner http.Handler) *httptest.Server {
	t.Helper()
	gateway := cWorkloadURI(e.tenant, "native-gateway")
	ingress, err := sdk.NewNativeIngress(sdk.NativeIngressConfig{Tenant: e.tenant, Deployment: e.deployment, GatewayDeployment: "native-gateway", Audience: cAudience, Verifier: e.verifier,
		// Identity is the explicit cEnv authorization fixture, not a live IAM claim.
		Authorize: func(context.Context, auth.Claims) error { return nil },
	}, inner)
	if err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0][0].URIs) != 1 || r.TLS.VerifiedChains[0][0].URIs[0].String() != gateway {
			http.Error(w, "gateway required", http.StatusForbidden)
			return
		}
		if r.URL.Path == "/readyz" {
			inner.ServeHTTP(w, r)
			return
		}
		ingress.ServeHTTP(w, r)
	})
	server := httptest.NewUnstartedServer(handler)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(e.ca.pem) {
		t.Fatal("client CA invalid")
	}
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	t.Cleanup(server.Close)
	tr := server.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{e.ca.clientCert(t, gateway)}
	server.Client().Transport = tr
	return server
}

// This explicit fixture isolates protocol composition from evaluation policy.
// It admits exactly this test digest; it is not an artifact evaluation result.
type nativeProtocolPublicationFixture struct{ digest string }

func (f nativeProtocolPublicationFixture) RequirePassedVerdict(_ context.Context, digest string) error {
	if digest != f.digest {
		return contracts.NewError(contracts.ErrForbidden, "protocol fixture digest mismatch")
	}
	return nil
}

type nativeProtocolIssuerFixture struct{ token string }

func (f nativeProtocolIssuerFixture) IssueRuntimeToken(context.Context, registry.InvokeRequest, registry.RuntimeBinding) (string, error) {
	return f.token, nil
}

// One composed execution through production Registry transport, the actual
// upstream A2A server, Go SDK, mTLS State API, and PostgreSQL. Publication gate,
// supply-chain and Identity issuance are named fixtures. No inference, durable
// native taskstore, restart, sandbox or production admission is claimed.
func TestRegistryNativeA2A10SDKCheckpointRealState(t *testing.T) {
	testRegistryNativeSDKCheckpoint(t, "conversation")
}

func TestRegistryOneShotNativeA2A10SDKCheckpointRealState(t *testing.T) {
	testRegistryNativeSDKCheckpoint(t, "one-shot")
}

// Reference runtime compatibility only: the actual receiver writes its pending
// run to State. Its worker/model are deliberately not started in this leg.
func TestRegistryOneShotReferenceAgentAcceptsInputRealState(t *testing.T) {
	testRegistryNativeSDKCheckpoint(t, "reference")
}

func testRegistryNativeSDKCheckpoint(t *testing.T, mode string) {
	t.Helper()
	oneShot, reference := mode != "conversation", mode == "reference"
	e := cNewEnv(t)
	grant := "native-registry-" + contracts.NewID()
	token := e.signTask(t, grant)
	client, err := sdkstate.New(sdkstate.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	var graphs atomic.Int32
	executor, err := sdk.NewLangGraphExecutor(&sdk.Config{StateClient: client, Worker: "native-registry-worker", LeaseTTL: time.Second, HeartbeatInterval: 20 * time.Millisecond}, func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
		graphs.Add(1)
		if rc.Task.TaskID != grant || rc.Task.ContextID != "ctx-1" {
			t.Error("Registry transport lost the verified AHP grant")
		}
		_, err := rc.CheckpointSaver.PutCheckpoint(ctx, "native-registry", sdkstate.Checkpoint{ID: "native-registry-result", Version: 1, Parts: []sdkstate.CheckpointPart{{Channel: "result", Version: "1", Data: []byte("native-registry-checkpoint")}}}, "native-registry-result")
		return json.RawMessage(`{"native":true}`), err
	})
	if err != nil {
		t.Fatal(err)
	}
	m := registry.Manifest{Kind: registry.KindAgent, System: "native.sdk.protocol", Visibility: registry.VisibilityTenant, Audience: registry.AudienceShared, Class: registry.ClassC,
		Components: []registry.Component{{Name: "agent", Image: "example/native@sha256:" + strings.Repeat("e", 64), A2A: true, MinReplicas: 1, MaxReplicas: 1}},
		State:      registry.StateConfig{Partition: "subject:native.sdk.protocol", Compat: "native-v1"}, Limits: registry.Limits{Depth: 1, ActiveTasks: 2, TaskTimeout: "1m"}}
	digest, err := contracts.Digest(m)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(e.ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := registry.New(pool, registry.WithEvalGate(nativeProtocolPublicationFixture{digest}), registry.WithSyntheticSupplyChain("native protocol composition; no artifact evaluation/admission"))
	if err := store.Migrate(e.ctx); err != nil {
		t.Fatal(err)
	}
	agent := "native-sdk-" + e.tenant
	if _, err := store.RegisterAgent(e.ctx, "native-protocol-publisher", agent); err != nil {
		t.Fatal(err)
	}
	version, err := store.PutVersion(e.ctx, "native-protocol-publisher", agent, digest, m)
	if err != nil {
		t.Fatal(err)
	}
	inst, err := store.Install(e.ctx, e.tenant, agent, version.ID, "dev", "native-sdk-"+e.deployment, m.Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	var wire http.Handler = a2asrv.NewJSONRPCHandler(sdk.NewA2AHandler(executor))
	if reference {
		wire, err = agentworkload.New(agentworkload.Config{Tenant: e.tenant, Deployment: e.deployment, AgentID: agent, Digest: digest, Image: m.Components[0].Image, Profile: "registry-input-compatibility", StateURL: e.srv.URL, ModelURL: e.srv.URL, Worker: "reference-not-started", GatewayWorkloads: []string{cWorkloadURI(e.tenant, "native-gateway")}, Client: e.srv.Client(), Verifier: e.verifier})
		if err != nil {
			t.Fatal(err)
		}
	}
	server := nativeSDKProtocolServer(t, e, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "agent_id": agent, "version_digest": digest, "image": m.Components[0].Image})
			return
		}
		wire.ServeHTTP(w, r)
	}))
	if err := store.RegisterRuntime(e.ctx, registry.RuntimeBinding{Tenant: e.tenant, Deployment: e.deployment, Environment: "dev", AgentID: agent, Digest: digest, Image: m.Components[0].Image, Endpoint: server.URL + "/a2a", ReadyURL: server.URL + "/readyz", Attestation: "synthetic protocol fixture"}); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := registry.NewRoutedA2AInvokeDispatcher(store, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.SetRuntimeTokenIssuer(nativeProtocolIssuerFixture{token})
	req := registry.InvokeRequest{Scope: contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}, Tenant: e.tenant, Environment: "dev", AgentID: agent, VersionID: version.ID, VersionDigest: digest, InstallationID: inst.ID, TaskID: grant, ConversationContext: "ctx-1"}
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("checkpoint through Registry"))
	message.ContextID = req.ConversationContext
	body, _ := json.Marshal(message)
	var raw json.RawMessage
	if oneShot {
		if _, err := store.PinForSubject(e.ctx, req.Scope, inst.ID, grant); err != nil {
			t.Fatal(err)
		}
		req.Input = json.RawMessage(`"checkpoint through Registry"`)
		remote, err := dispatcher.Invoke(e.ctx, req)
		if err != nil {
			t.Fatal("one-shot Registry could not reach the native SDK", err)
		}
		if err := store.AliasTaskPin(e.ctx, e.tenant, inst.ID, grant, remote); err != nil {
			t.Fatal(err)
		}
		req.Observe, req.TaskID = true, remote
		raw, err = dispatcher.Observe(e.ctx, req)
	} else {
		raw, err = dispatcher.SendAgentMessage(e.ctx, req, body)
	}
	if err != nil {
		t.Fatal("Registry could not reach the native SDK", err)
	}
	var task a2a.Task
	if err := json.Unmarshal(raw, &task); err != nil || task.ID == "" {
		t.Fatalf("invalid native task: %s error=%v", raw, err)
	}
	if reference {
		thread, err := client.GetThreadByContext(e.ctx, "ctx-1")
		if err != nil {
			t.Fatal(err)
		}
		runs, err := client.ListRuns(e.ctx, thread.ID)
		if err != nil || len(runs) != 1 || runs[0].TaskID != grant || runs[0].Status != sdkstate.RunPending || graphs.Load() != 0 {
			t.Fatalf("reference receiver did not durably accept the input: runs=%+v graphs=%d error=%v", runs, graphs.Load(), err)
		}
		var input struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(runs[0].Payload, &input) != nil || input.Text != "checkpoint through Registry" {
			t.Fatal("reference receiver lost the actual text input")
		}
		t.Log("actual reference receiver + Registry transport + State mTLS/PG accepted exact input; readiness/Identity/admission fixtures; worker not started, no inference/completion claim")
		return
	}
	req.Observe = true
	deadline := time.Now().Add(5 * time.Second)
	for !task.Status.State.Terminal() && time.Now().Before(deadline) {
		if oneShot {
			raw, err = dispatcher.Observe(e.ctx, req)
		} else {
			raw, err = dispatcher.AgentTask(e.ctx, req, "GetTask", string(task.ID))
		}
		if err != nil {
			t.Fatal("Registry lost native task observation", err)
		}
		if err = json.Unmarshal(raw, &task); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if task.Status.State != a2a.TaskStateCompleted || graphs.Load() != 1 || string(task.ID) == grant || len(task.Artifacts) != 1 || len(task.Artifacts[0].Parts) != 1 {
		t.Fatalf("native SDK did not durably complete once: task=%+v graphs=%d", task, graphs.Load())
	}
	// GraphFunc returns JSON; the SDK intentionally emits a native Data part.
	output, err := json.Marshal(task.Artifacts[0].Parts[0].Data())
	if err != nil || string(output) != `{"native":true}` {
		t.Fatalf("native structured result changed: %s error=%v", output, err)
	}
	thread, err := client.GetThreadByContext(e.ctx, "ctx-1")
	if err != nil {
		t.Fatal(err)
	}
	runs, err := client.ListRuns(e.ctx, thread.ID)
	if err != nil || len(runs) != 1 || runs[0].TaskID != grant || runs[0].Status != sdkstate.RunDone {
		t.Fatalf("State grant/result mismatch: %+v %v", runs, err)
	}
	part, err := client.GetPart(e.ctx, thread.ID, "native-registry", "result", "1")
	if err != nil || string(part) != "native-registry-checkpoint" {
		t.Fatalf("State checkpoint mismatch: bytes=%d error=%v", len(part), err)
	}
	t.Logf("Registry PG routing -> native A2A1.0 over gateway mTLS/JWT -> SDK -> State mTLS/PG: completed, exact checkpoint, opaque task, graph once; one-shot=%t; evaluation/Identity are explicit fixtures", oneShot)
}
