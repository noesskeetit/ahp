package execution_test

// served_surface_test.go -- the published Agent Card may only promise what the
// public A2A face really answers.
//
// Measured on the live pin 1370d01 (state/journey-runs/a2a-exec-20261031T030702Z):
// an honest task token passed the public face, and the call for the v0 name
// "message/send" came back as JSON-RPC -32601 METHOD_NOT_FOUND. The plane speaks
// the A2A v1.0 core methods (a2asrv of a2a-go v2.7.0); the v0 names live in
// a2acompat/a2av0, a separate module that is not part of this build. So the card
// must name the served methods, and this file locks BOTH halves of that truth:
// every re-exported served name really is dispatched, and every legacy v0 name
// really is refused with -32601 (never a 200 pretending success).

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const jsonrpcMethodNotFound = -32601

// rawRPC calls the public face with an explicit Accept header. The streaming
// methods answer SSE unless the client asks for JSON, so a surface probe that
// wants a parseable JSON-RPC error must say so.
func rawRPC(t *testing.T, url, method string, params any, sc contracts.Scope) (int, *rpcErr, string) {
	t.Helper()
	pb, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	bb, _ := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: method, Params: pb})
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(bb))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
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
	body := new(bytes.Buffer)
	_, _ = body.ReadFrom(resp.Body)
	_ = json.Unmarshal(body.Bytes(), &out)
	return resp.StatusCode, out.Error, body.String()
}

// mountPublicFace mounts the real gateway handler behind an authenticated
// principal, exactly as the execution role mounts it (public face guarded,
// internal face refused).
func mountFaceWithCard(t *testing.T, svc *gateway.Service, card *a2a.AgentCard) string {
	t.Helper()
	deny := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "internal disabled", http.StatusForbidden)
		})
	}
	srv, err := gateway.NewServer(gateway.NewHandler(svc, gateway.HandlerConfig{ExtendedCard: card}), gateway.ServerConfig{
		AgentCard:    card,
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
	return ts.URL + "/a2a/public"
}

func newServiceWithStub(t *testing.T) *gateway.Service {
	t.Helper()
	pool, store, ledger, sfx := gwSetup(t)
	_ = pool
	stub := gateway.ExecutorFunc(func(context.Context, gateway.ExecInput) (gateway.ExecResult, error) {
		return gateway.ExecResult{State: a2a.TaskStateCompleted}, nil
	})
	svc, err := gateway.NewService(store, ledger, stub, gateway.NullNotifier{}, gateway.Config{
		WorkerID: "surface-" + sfx[:8], Lease: 5 * time.Second, Poll: 10 * time.Millisecond,
		MaxBlock: 200 * time.Millisecond, MaxAttempts: 1, ParkLease: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

// TestServedMethodSurfaceMatchesHandler proves gateway.ServedMethods is the real
// dispatch table (no name that is not served, no served name left out) and that
// every A2A v0 name is refused with METHOD_NOT_FOUND. A card is validated
// against this list, so this test is what keeps a published card honest.
func TestServedMethodSurfaceMatchesHandler(t *testing.T) {
	svc := newServiceWithStub(t)
	url := mountFaceWithCard(t, svc, nil)
	scope := contracts.Scope{Tenant: "t-surface", Deployment: "d-surface", Subject: "svc"}

	for _, m := range gateway.ServedMethods {
		if !gateway.MethodIsServed(m) {
			t.Fatalf("ServedMethods contains %q but MethodIsServed denies it", m)
		}
		code, e, raw := rawRPC(t, url, m, map[string]any{}, scope)
		if e != nil && e.Code == jsonrpcMethodNotFound {
			t.Fatalf("served method %q answered METHOD_NOT_FOUND (list drifted from the handler): %s", m, raw)
		}
		t.Logf("served %-38s http=%d rpc_error=%s", m, code, formatRPCErr(e))
	}
	for _, m := range gateway.UnservedLegacyMethods {
		if gateway.MethodIsServed(m) {
			t.Fatalf("legacy v0 method %q must not be listed as served", m)
		}
		code, e, raw := rawRPC(t, url, m, map[string]any{}, scope)
		if e == nil || e.Code != jsonrpcMethodNotFound {
			t.Fatalf("legacy v0 method %q must be refused with %d, got http=%d error=%s body=%s",
				m, jsonrpcMethodNotFound, code, formatRPCErr(e), raw)
		}
		t.Logf("unserved %-38s http=%d rpc_error=%d %q", m, code, e.Code, e.Message)
	}
}

func formatRPCErr(e *rpcErr) string {
	if e == nil {
		return "none"
	}
	return fmt.Sprintf("%d %s", e.Code, e.Message)
}

// TestCardClaimsAreServedByPublicFace is the BLOCK-3 claim: the card's machine
// readable protocol claim and the operations its skill names must be answered by
// the face that publishes the card. It drives a real task through the public face
// (send -> get -> terminal COMPLETED with a real sealed artifact) and refuses a
// foreign scope, so a card that advertises a skill the face cannot serve fails
// here instead of misleading a client.
func TestCardClaimsAreServedByPublicFace(t *testing.T) {
	pool, store, ledger, sfx := gwSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	nonce := hex.EncodeToString(rnd[:])
	body := "card-claims-payload-" + nonce[:8]
	ex := newExec(t, ctx, pool, sfx, compModel{body: body})
	svc, err := gateway.NewService(store, ledger, ex, gateway.NullNotifier{}, gateway.Config{
		WorkerID: "claims-" + sfx[:8], Lease: 5 * time.Second, Poll: 10 * time.Millisecond,
		MaxBlock: 2 * time.Second, MaxAttempts: 2, ParkLease: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	go svc.Run(ctx)

	card := platformCardForFace(body)
	url := mountFaceWithCard(t, svc, card)
	owner := contracts.Scope{Tenant: "t-claims-" + nonce[:8], Deployment: "d-claims", Subject: "owner"}
	foreign := contracts.Scope{Tenant: "t-claims-foreign-" + nonce[:8], Deployment: "d-claims", Subject: "intruder"}

	send := &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("produce the artifact")),
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	}
	res, e := rpcCall(t, url, gateway.MethodSendMessage, send, owner)
	if e != nil {
		t.Fatalf("%s through the public face failed: %+v", gateway.MethodSendMessage, e)
	}
	var sr struct {
		Task *a2a.Task `json:"task"`
	}
	if err := json.Unmarshal(res, &sr); err != nil || sr.Task == nil {
		t.Fatalf("send result shape: %s err=%v", string(res), err)
	}
	id := string(sr.Task.ID)
	t.Logf("%s accepted task=%s state=%s", gateway.MethodSendMessage, id, sr.Task.Status.State)

	var got *a2a.Task
	for i := 0; i < 200; i++ {
		raw, e := rpcCall(t, url, gateway.MethodGetTask, &a2a.GetTaskRequest{ID: sr.Task.ID}, owner)
		if e != nil {
			t.Fatalf("%s: %+v", gateway.MethodGetTask, e)
		}
		var tr a2a.Task
		if err := json.Unmarshal(raw, &tr); err != nil {
			t.Fatalf("GetTask shape: %s", string(raw))
		}
		got = &tr
		if tr.Status.State.Terminal() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got == nil || got.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("task %s never reached COMPLETED through the public face (state=%v)", id, stateOf(got))
	}
	if len(got.Artifacts) == 0 {
		t.Fatalf("the skill the card advertises produced no artifact on a COMPLETED task")
	}
	t.Logf("%s terminal state=%s artifacts=%d", gateway.MethodGetTask, got.Status.State, len(got.Artifacts))
	writeEvidence(t, "a2a.card_skill_served", map[string]any{
		"card_version":   card.Version,
		"skill_id":       card.Skills[0].ID,
		"wire_shape":     "canonical a2a v1 SendMessageRequest",
		"task_id":        id,
		"send_state":     string(sr.Task.Status.State),
		"terminal_state": string(got.Status.State),
		"artifacts":      len(got.Artifacts),
		"artifact_refs":  artifactNames(got.Artifacts),
	})

	// The card promises data isolation for the task it serves: a foreign scope
	// must not read it. This is the honest refusal, not a 200 with empty data.
	if _, e := rpcCall(t, url, gateway.MethodGetTask, &a2a.GetTaskRequest{ID: sr.Task.ID}, foreign); e == nil {
		t.Fatalf("foreign scope read task %s through the public face", id)
	} else {
		t.Logf("foreign %s refused: code=%d %s", gateway.MethodGetTask, e.Code, e.Message)
		writeEvidence(t, "a2a.card_scope_refused", map[string]any{
			"task_id": id, "rpc_code": e.Code, "rpc_message": e.Message, "scope": "foreign tenant",
		})
	}

	// Cancel is part of what the card's skill text claims; a COMPLETED task must
	// be refused honestly and the method must still be dispatched (not -32601).
	code, e, raw := rawRPC(t, url, gateway.MethodCancelTask, map[string]any{"id": id}, owner)
	if e != nil && e.Code == jsonrpcMethodNotFound {
		t.Fatalf("%s is not dispatched through the public face: %s", gateway.MethodCancelTask, raw)
	}
	t.Logf("%s on a terminal task: http=%d error=%s", gateway.MethodCancelTask, code, formatRPCErr(e))

	// The card's claim is about a *client*, not about a Go struct: the real
	// consumers of this plane speak the v0.3 wire shape (role "user", parts
	// tagged with "kind" -- tests/e2e/emil-tool-hop.mjs:54). A skill that only
	// works for a hand-built canonical request is still a card that lies, so the
	// advertised skill is driven once more through the same face with a raw v0
	// body and must reach COMPLETED with a real artifact.
	v0Params := map[string]any{"message": map[string]any{
		"kind":      "message",
		"role":      "user",
		"messageId": "skill-v0-" + nonce,
		"parts":     []any{map[string]any{"kind": "text", "text": "produce the artifact"}},
	}}
	_, v0Err, v0Raw := rawRPC(t, url, gateway.MethodSendMessage, v0Params, owner)
	if v0Err != nil {
		t.Fatalf("the advertised skill is not served for the wire shape real clients write: %+v (%s)", v0Err, v0Raw)
	}
	var v0Body struct {
		Result struct {
			Task *a2a.Task `json:"task"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(v0Raw), &v0Body); err != nil || v0Body.Result.Task == nil {
		t.Fatalf("raw v0 send result shape: %s err=%v", v0Raw, err)
	}
	v0ID := v0Body.Result.Task.ID
	var v0Got *a2a.Task
	for i := 0; i < 200; i++ {
		raw, e := rpcCall(t, url, gateway.MethodGetTask, &a2a.GetTaskRequest{ID: v0ID}, owner)
		if e != nil {
			t.Fatalf("GetTask on the v0-shaped skill call: %+v", e)
		}
		var tr a2a.Task
		if err := json.Unmarshal(raw, &tr); err != nil {
			t.Fatalf("GetTask shape: %s", string(raw))
		}
		v0Got = &tr
		if tr.Status.State.Terminal() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if v0Got == nil || v0Got.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("skill %q served to a v0-shaped client never completed (state=%v)",
			platformCardForFace(body).Skills[0].ID, stateOf(v0Got))
	}
	if len(v0Got.Artifacts) == 0 {
		t.Fatalf("the skill the card advertises produced no artifact for a v0-shaped client")
	}
	t.Logf("v0-shaped client: task=%s state=%s artifacts=%d", v0ID, v0Got.Status.State, len(v0Got.Artifacts))
	writeEvidence(t, "a2a.card_skill_served", map[string]any{
		"card_version":   card.Version,
		"skill_id":       card.Skills[0].ID,
		"wire_shape":     "a2a v0-shaped body (role user, parts kind=text)",
		"task_id":        string(v0ID),
		"terminal_state": string(v0Got.Status.State),
		"artifacts":      len(v0Got.Artifacts),
		"artifact_refs":  artifactNames(v0Got.Artifacts),
	})
}

func stateOf(task *a2a.Task) string {
	if task == nil {
		return "<nil>"
	}
	return string(task.Status.State)
}

// platformCardForFace is a card in the shape the operator publishes: the public
// interface declares the binding/version this plane serves, the AHP extension is
// required, and the skill text names the CANONICAL methods it really serves.
func platformCardForFace(body string) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "AHP Execution Plane (claims test)",
		Version:     "test-" + body,
		Description: "Executes a durable A2A task; the task is accepted by " + gateway.MethodSendMessage + ", observed with " + gateway.MethodGetTask + " and cancelled with " + gateway.MethodCancelTask + ".",
		SupportedInterfaces: []*a2a.AgentInterface{{
			URL:             "https://exec.example.com/a2a/public",
			ProtocolBinding: a2a.TransportProtocolJSONRPC,
			ProtocolVersion: a2a.Version,
		}},
		Capabilities: a2a.AgentCapabilities{
			Streaming:         true,
			ExtendedAgentCard: true,
			PushNotifications: false,
			Extensions: []a2a.AgentExtension{{
				URI:      gateway.AgentExtensionURI,
				Required: true,
				Params:   map[string]any{"history": "owned", "steering": "interrupt", "run_timeout_ms": float64(300000)},
			}},
		},
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"text/plain", "application/json"},
		Skills: []a2a.AgentSkill{{
			ID:          "agents-space-turn",
			Name:        "Agents Space turn execution",
			Description: "Accepts a task with " + gateway.MethodSendMessage + " and runs the agent turn; status and history come from " + gateway.MethodGetTask + ", streaming from " + gateway.MethodSendStreamingMessage + ", cancellation from " + gateway.MethodCancelTask + ".",
			Tags:        []string{"a2a", "agents-space", "turn"},
		}},
	}
}
