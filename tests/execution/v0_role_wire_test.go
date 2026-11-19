package execution_test

// v0_role_wire_test.go -- the defect the live stand measured, kept as a
// regression the suite can never lose again.
//
// Evidence state/journey-runs/a2a-send-20261031T034648Z: SendMessage returned
// 200 and created a task whose own history[0] carried the user's text, yet the
// task drained to TASK_STATE_FAILED with "execbridge: no usable text input".
// The text was never lost on the wire. A2A 1.0 spells a role ROLE_USER; the
// v0.3 spelling this repo's real consumers emit (tools/a2a-public-send-probe.sh,
// tests/e2e/emil-tool-hop.mjs:54, adapters/emil) is "user", and a2a-go v2.7.0
// stores it verbatim. The drain selected role=="ROLE_USER" only, so
// gateway.ExecInput.Input arrived nil and the bridge reported an empty turn.
//
// Every pre-existing test built its message with
// a2a.NewMessage(a2a.MessageRoleUser, ...), i.e. the canonical spelling, which
// is why the suite stayed green over a broken public interface. These tests
// therefore post the RAW v0 JSON body -- no library types on the request path.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// vrRecorder is an executor that records what the gateway handed it. The point
// of the test is the handoff, so the executor must report what it actually saw.
type vrRecorder struct {
	mu        sync.Mutex
	sawNil    int
	roles     []a2a.MessageRole
	texts     []string
	completed int
}

func (r *vrRecorder) Execute(_ context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if in.Input == nil {
		r.sawNil++
	} else {
		r.roles = append(r.roles, in.Input.Role)
		text := ""
		for _, p := range in.Input.Parts {
			if t := p.Text(); t != "" {
				text += t
			}
		}
		r.texts = append(r.texts, text)
	}
	r.completed++
	return gateway.ExecResult{
		State:   a2a.TaskStateCompleted,
		Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("turn answered")),
	}, nil
}

func (r *vrRecorder) snapshot() (int, []a2a.MessageRole, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sawNil, append([]a2a.MessageRole(nil), r.roles...), append([]string(nil), r.texts...)
}

// vrFace wires a durable gateway service with the recording executor behind the
// same mTLS + scoped-task-token Public face the execution role mounts.
func vrFace(t *testing.T, exec gateway.Executor) (*httptest.Server, *auth.Signer, string, string) {
	t.Helper()
	return vrFaceCeil(t, exec, 0)
}

// vrFaceCeil is vrFace with an explicit operator step-budget ceiling, so a test
// can state the deployment bound the way role_execution does (from
// AHP_EXEC_MAX_STEPS) instead of baking it into the helper.
func vrFaceCeil(t *testing.T, exec gateway.Executor, ceiling int) (*httptest.Server, *auth.Signer, string, string) {
	t.Helper()
	_, store, ledger, sfx := gwSetup(t)
	verifier, signer := ptVerifierSigner(t)
	ca := ptNewCA(t)
	tenant := "t-v0role-" + randSuffix()
	dep := "d-v0role"
	client := ca.ptLeaf(t, "wl", workloadURI(tenant, dep), false)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	svc, err := gateway.NewService(store, ledger, exec, gateway.NullNotifier{}, gateway.Config{
		WorkerID: "v0role-" + sfx[:8], Lease: 5 * time.Second, Poll: 10 * time.Millisecond,
		MaxBlock: 10 * time.Second, MaxAttempts: 2, ParkLease: 30 * time.Second,
		StepBudgetCeiling: ceiling,
	})
	if err != nil {
		t.Fatal(err)
	}
	go svc.Run(ctx)
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	deny := func(http.Handler) http.Handler {
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
	return ptMTLSServer(t, ca, client, srv), signer, tenant, dep
}

// vrRawSend posts a hand-built JSON-RPC body. The params are assembled as a map
// on purpose: the request must be the wire document a v0 client writes, not
// something a2a-go produced.
// vrRawSend posts a hand-built v0-shaped JSON-RPC body and reports the status,
// the task it created (nil when the face refused), the refusal detail, the
// JSON-RPC code and the raw response body.
func vrRawSend(t *testing.T, srv *httptest.Server, token, role, msgID, text string) (int, *a2a.Task, string, int, []byte) {
	t.Helper()
	params, err := json.Marshal(map[string]any{"message": vrRawMessage(role, msgID, text)})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: "SendMessage", Params: params})
	if err != nil {
		t.Fatal(err)
	}
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
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Reason  string `json:"reason"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("body is not a JSON-RPC response: %v (%s)", err, raw)
	}
	if out.Error != nil {
		return resp.StatusCode, nil, out.Error.Message, out.Error.Code, raw
	}
	var sr sendResult
	_ = json.Unmarshal(out.Result, &sr)
	return resp.StatusCode, sr.Task, "", 0, raw
}

// vrRawMessage builds the message object exactly as a v0.3 client writes it:
// kind discriminators on the envelope and on every part.
func vrRawMessage(role, msgID, text string) map[string]any {
	return map[string]any{
		"kind":      "message",
		"role":      role,
		"messageId": msgID,
		"parts":     []any{map[string]any{"kind": "text", "text": text}},
	}
}

// vrRawSendParts is the same request with a caller-supplied parts array, for
// content the face must refuse before a task exists.
func vrRawSendParts(t *testing.T, srv *httptest.Server, token, msgID string, parts []any) (int, *a2a.Task, string, int) {
	t.Helper()
	params, err := json.Marshal(map[string]any{"message": map[string]any{
		"kind": "message", "role": "user", "messageId": msgID, "parts": parts,
	}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: "SendMessage", Params: params})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/a2a/public", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcErr         `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("body is not a JSON-RPC response: %v (%s)", err, raw)
	}
	if out.Error != nil {
		return resp.StatusCode, nil, out.Error.Message, out.Error.Code
	}
	var sr sendResult
	_ = json.Unmarshal(out.Result, &sr)
	return resp.StatusCode, sr.Task, "", 0
}

// TestV0RoleSpellingReachesExecutor is the regression: a client speaking the
// v0.3 role spelling must have its text reach the executor, the task must
// complete, and the history the agent serves must carry the canonical role.
func TestV0RoleSpellingReachesExecutor(t *testing.T) {
	rec := &vrRecorder{}
	srv, signer, tenant, dep := vrFace(t, rec)
	token := ptTaskToken(t, signer, tenant, dep, "a2a-agent", "vr-legacy-role")
	const want = "Составь один короткий абзац про снимки состояния."

	status, task, detail, code, raw := vrRawSend(t, srv, token, "user", "vr-msg-user-"+randSuffix(), want)
	if detail != "" {
		t.Fatalf("a v0-spelled user message was refused: detail=%q code=%d", detail, code)
	}
	if status != http.StatusOK || task == nil {
		t.Fatalf("send must complete 200 with a task, got status=%d task=%v", status, task)
	}
	if task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("task state = %s, want COMPLETED (status=%+v)", task.Status.State, task.Status.Message)
	}

	sawNil, roles, texts := rec.snapshot()
	if sawNil != 0 {
		t.Fatalf("the executor received a nil input %d time(s): the inbound text did not survive the handoff", sawNil)
	}
	if len(texts) != 1 || texts[0] != want {
		t.Fatalf("executor saw %q, want exactly %q", texts, want)
	}
	if len(roles) != 1 || roles[0] != a2a.MessageRoleUser {
		t.Fatalf("executor saw roles %v, want the canonical %q", roles, a2a.MessageRoleUser)
	}

	// What the plane serves must be what the protocol names: no v0 spelling
	// leaking back out of a task this gateway journalled.
	if len(task.History) != 1 {
		t.Fatalf("history = %d messages, want 1", len(task.History))
	}
	if task.History[0].Role != a2a.MessageRoleUser {
		t.Fatalf("served history role = %q, want %q", task.History[0].Role, a2a.MessageRoleUser)
	}
	if got := task.History[0].Parts[0].Text(); got != want {
		t.Fatalf("served history text = %q, want %q", got, want)
	}

	// The served part must be canonical A2A 1.0. v1.0 has no "kind"
	// discriminator on a part (the library flattens a part to {"text": ...}),
	// and this card declares protocolVersion 1.0 -- echoing a v0 discriminator
	// back would advertise a protocol this face does not serve. The v0 client's
	// "kind" is accepted on the way in and its text survives; that is the whole
	// of the compatibility, and it is what this pins.
	var wire struct {
		Result struct {
			Task struct {
				History []json.RawMessage `json:"history"`
			} `json:"task"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("response is not the documented shape: %v (%s)", err, raw)
	}
	if len(wire.Result.Task.History) != 1 {
		t.Fatalf("served history = %d messages, want 1", len(wire.Result.Task.History))
	}
	var partWire map[string]json.RawMessage
	if err := json.Unmarshal(wire.Result.Task.History[0], &partWire); err != nil {
		t.Fatalf("history[0] is not a message object: %v", err)
	}
	var parts []map[string]json.RawMessage
	if err := json.Unmarshal(partWire["parts"], &parts); err != nil || len(parts) != 1 {
		t.Fatalf("history[0].parts must hold one part: %v (%s)", err, partWire["parts"])
	}
	if _, hasKind := parts[0]["kind"]; hasKind {
		t.Fatalf("served part carries a v0 \"kind\" discriminator while the card declares protocolVersion %s: %s",
			gateway.ServedProtocolVersion, partWire["parts"])
	}
	if _, hasText := parts[0]["text"]; !hasText {
		t.Fatalf("served part lost its text through the round trip: %s", partWire["parts"])
	}
}

// TestEmptyContentRefusedAtIngress is the second half of the ingress rule the
// stand asked for: a request whose parts carry nothing must be refused with
// -32602 before a task id exists, not accepted and then failed inside the
// worker (which reads to an operator as a broken executor).
func TestEmptyContentRefusedAtIngress(t *testing.T) {
	rec := &vrRecorder{}
	srv, signer, tenant, dep := vrFace(t, rec)
	token := ptTaskToken(t, signer, tenant, dep, "a2a-agent", "vr-empty-content")

	for _, tc := range []struct {
		name  string
		parts []any
	}{
		{"blank-text", []any{map[string]any{"kind": "text", "text": "   \n"}}},
		{"empty-text", []any{map[string]any{"kind": "text", "text": ""}}},
		{"no-parts", []any{}},
		{"blank-plus-empty", []any{
			map[string]any{"kind": "text", "text": ""},
			map[string]any{"kind": "text", "text": " "},
		}},
	} {
		status, task, detail, code := vrRawSendParts(t, srv, token, "vr-empty-"+tc.name+"-"+randSuffix(), tc.parts)
		if task != nil {
			t.Fatalf("%s: a task was created for content-free parts (%s): %s", tc.name, tc.parts, task.ID)
		}
		if code != -32602 {
			t.Fatalf("%s: content-free parts must be refused as -32602, got code=%d detail=%q status=%d",
				tc.name, code, detail, status)
		}
		if detail == "" {
			t.Fatalf("%s: the refusal must tell the client what to fix", tc.name)
		}
	}
	if _, _, texts := rec.snapshot(); len(texts) != 0 {
		t.Fatalf("a content-free request must never reach the executor, saw %q", texts)
	}
}

// TestNonUserInboundRoleFailsClosed is the other half of the boundary: a role
// that cannot name a user turn is a client error (-32602), not a task that is
// journalled and then silently never drained.
func TestNonUserInboundRoleFailsClosed(t *testing.T) {
	rec := &vrRecorder{}
	srv, signer, tenant, dep := vrFace(t, rec)
	token := ptTaskToken(t, signer, tenant, dep, "a2a-agent", "vr-negative-role")

	for _, tc := range []struct{ name, role string }{
		{"agent-role", "agent"},
		{"canonical-agent-role", string(a2a.MessageRoleAgent)},
		{"unspecified-role", string(a2a.MessageRoleUnspecified)},
		{"garbage-role", "rooter"},
	} {
		status, task, detail, code, _ := vrRawSend(t, srv, token, tc.role, "vr-msg-"+tc.name+"-"+randSuffix(), "должна быть отклонена")
		if task != nil {
			t.Fatalf("%s: a task was created for role %q: %+v", tc.name, tc.role, task.ID)
		}
		if code != -32602 {
			t.Fatalf("%s: role %q must be rejected as JSON-RPC -32602 invalid params, got code=%d detail=%q status=%d",
				tc.name, tc.role, code, detail, status)
		}
		if detail == "" {
			t.Fatalf("%s: a refusal must carry a reason the client can act on, got an empty error detail", tc.name)
		}
	}
	if _, _, texts := rec.snapshot(); len(texts) != 0 {
		t.Fatalf("a rejected message must never reach the executor, saw %q", texts)
	}
}
