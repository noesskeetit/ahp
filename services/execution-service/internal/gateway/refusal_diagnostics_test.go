package gateway_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// syncBuffer collects what the operator journal received.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// refusalFront mounts the real server on a real PostgreSQL-backed service, with
// the operator journal captured and the test principal injected from a header
// so one mount can be addressed by more than one caller.
func refusalFront(t *testing.T, svc *gateway.Service, scope contracts.Scope, caller string) (*httptest.Server, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	h := gateway.NewHandler(svc, gateway.HandlerConfig{
		Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	cfg := baseCfg()
	cfg.Public = gateway.AuthConfig{Middleware: func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sub := r.Header.Get("X-Test-Caller")
			if sub == "" {
				sub = caller
			}
			principal := transport.Principal{
				Scope:  scope,
				Claims: auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject, Task: sub, Root: "root-" + sub, Context: "grant-context-" + sub, Grant: "grant-" + sub},
			}
			next.ServeHTTP(w, r.WithContext(transport.WithPrincipal(r.Context(), principal)))
		})
	}}
	srv, err := gateway.NewServer(h, cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, logs
}

// rpcError is the JSON-RPC error object as it reaches the wire.
type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type rpcReply struct {
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
	RawBody string          `json:"-"`
}

// rpc calls one JSON-RPC method on the mounted face as one caller.
func rpc(t *testing.T, ts *httptest.Server, method string, params any, caller string, id int) rpcReply {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id, "method": method, "params": params,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/a2a/public", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if caller != "" {
		req.Header.Set("X-Test-Caller", caller)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var out rpcReply
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("reply is not JSON-RPC: %v (body=%s)", err, raw)
	}
	out.RawBody = string(raw)
	return out
}

// sendParams builds a send request whose text carries a unique marker, so any
// leak of request content into the journal is detectable by that marker alone.
func sendParams(t *testing.T, msgID, marker, contextID string) *a2a.SendMessageRequest {
	t.Helper()
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("please act on "+marker))
	msg.ID = msgID
	if contextID != "" {
		msg.ContextID = contextID
	}
	return &a2a.SendMessageRequest{
		Message: msg,
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	}
}

// emptySendParams builds a syntactically valid send with no usable content.
func emptySendParams(t *testing.T, msgID string) *a2a.SendMessageRequest {
	t.Helper()
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("   "))
	msg.ID = msgID
	return &a2a.SendMessageRequest{
		Message: msg,
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	}
}

// refusalRecords parses the journal entries the gateway wrote for refusals.
func refusalRecords(t *testing.T, logs *syncBuffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(logs.String(), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("journal line is not structured: %v (%s)", err, line)
		}
		if rec["msg"] == "gateway: request refused" {
			out = append(out, rec)
		}
	}
	return out
}

func findByReason(records []map[string]any, reason string) []map[string]any {
	var out []map[string]any
	for _, r := range records {
		if r["reason"] == reason {
			out = append(out, r)
		}
	}
	return out
}

// TestRefusalJournalDistinguishesRefusalsThatLookIdenticalToTheClient is the
// core leg of DEF-H25-1: refusals that collapse onto one protocol answer must
// still be separable by an operator, from the accepting side's own journal,
// without the client answer changing.
func TestRefusalJournalDistinguishesRefusalsThatLookIdenticalToTheClient(t *testing.T) {
	env := newEnv(t)
	svc := env.service(t, echoExecutor(), nil)
	ts, logs := refusalFront(t, svc, env.scope, env.caller)

	promptMarker := "PROMPT-MARKER-" + env.sfx
	sharedID := "msg-shared-" + env.sfx

	// Baseline: a well-formed send is accepted, so the refusals below are known
	// to be caused by their own defect and not by a broken mount.
	if ok := rpc(t, ts, gateway.MethodSendMessage, sendParams(t, "msg-ok-"+env.sfx, promptMarker, ""), env.caller, 1); ok.Error != nil {
		t.Fatalf("baseline send refused: %+v", ok.Error)
	}

	// (1) INVALID_ARGUMENT on the protocol: empty content.
	emptyID := "msg-empty-" + env.sfx
	empty := rpc(t, ts, gateway.MethodSendMessage, emptySendParams(t, emptyID), env.caller, 2)
	if empty.Error == nil {
		t.Fatal("empty-content send was accepted, want refusal")
	}

	// (2) The same idempotency key with a different payload: KEY_REUSE_MISMATCH,
	// which the transport also lives on 400/-32602.
	if first := rpc(t, ts, gateway.MethodSendMessage, sendParams(t, sharedID, "FIRST-PAYLOAD-"+env.sfx, ""), env.caller, 3); first.Error != nil {
		t.Fatalf("first use of the key refused: %+v", first.Error)
	}
	reuse := rpc(t, ts, gateway.MethodSendMessage, sendParams(t, sharedID, "SECOND-PAYLOAD-"+env.sfx, ""), env.caller, 4)
	if reuse.Error == nil {
		t.Fatal("key reuse was accepted, want refusal")
	}
	// The same key with the payload it was claimed with is a replay, not a
	// reuse: it must be accepted idempotently and must not log a refusal.
	if replay := rpc(t, ts, gateway.MethodSendMessage, sendParams(t, sharedID, "FIRST-PAYLOAD-"+env.sfx, ""), env.caller, 5); replay.Error != nil {
		t.Fatalf("idempotent replay refused: %+v\nJOURNAL:\n%s", replay.Error, logs.String())
	}

	// (3) A context owned by another caller: FORBIDDEN.
	sharedCtx := "ctx-" + env.sfx
	if got := rpc(t, ts, gateway.MethodSendMessage, sendParams(t, "msg-ctx-a-"+env.sfx, promptMarker, sharedCtx), env.caller, 6); got.Error != nil {
		t.Fatalf("owner send refused: %+v", got.Error)
	}
	foreign := rpc(t, ts, gateway.MethodSendMessage, sendParams(t, "msg-ctx-b-"+env.sfx, promptMarker, sharedCtx), "intruder-"+env.sfx, 7)
	if foreign.Error == nil {
		t.Fatal("cross-caller context reuse was accepted, want refusal")
	}

	// (4) A probe for a task that does not exist here.
	probe := rpc(t, ts, gateway.MethodGetTask, map[string]any{"id": "no-such-task-" + env.sfx}, env.caller, 8)
	if probe.Error == nil {
		t.Fatal("unknown task probe was accepted, want refusal")
	}

	// (5) A malformed probe of the same method: same method, different guard.
	noID := rpc(t, ts, gateway.MethodGetTask, map[string]any{}, env.caller, 9)
	if noID.Error == nil {
		t.Fatal("GetTask without an id was accepted, want refusal")
	}

	records := refusalRecords(t, logs)

	// Every refusal class must be present exactly once with its own reason AND
	// its own code: two refusals sharing one client answer stay separable.
	wants := []struct {
		reason string
		code   string
		op     string
	}{
		{gateway.ReasonContentEmpty, contracts.ErrInvalid, gateway.MethodSendMessage},
		{gateway.ReasonKeyReuseMismatch, contracts.ErrKeyReuse, gateway.MethodSendMessage},
		{gateway.ReasonContextNotOwned, contracts.ErrForbidden, gateway.MethodSendMessage},
		{"task_read", "TASK_NOT_FOUND", gateway.MethodGetTask},
		{gateway.ReasonTaskIDMissing, "INVALID_PARAMS", gateway.MethodGetTask},
	}
	seen := map[string]string{}
	for _, w := range wants {
		hits := findByReason(records, w.reason)
		if len(hits) != 1 {
			t.Fatalf("journal has %d records for reason %q, want exactly 1 (all: %v)", len(hits), w.reason, reasonsOf(records))
		}
		rec := hits[0]
		if rec["code"] != w.code {
			t.Fatalf("reason %q logged code %v, want %q", w.reason, rec["code"], w.code)
		}
		if rec["op"] != w.op {
			t.Fatalf("reason %q logged op %v, want %q", w.reason, rec["op"], w.op)
		}
		seen[w.reason] = w.code
	}
	if len(seen) != len(wants) {
		t.Fatalf("refusals are not mutually distinguishable: %v", seen)
	}

	// The journal must carry the operation identifiers of the refusal.
	reuseRec := findByReason(records, gateway.ReasonKeyReuseMismatch)[0]
	for field, want := range map[string]string{
		"tenant": env.scope.Tenant, "deployment": env.scope.Deployment,
		"subject": env.scope.Subject, "caller": env.caller, "message_id": sharedID,
	} {
		if got, _ := reuseRec[field].(string); got != want {
			t.Fatalf("key-reuse record %s = %v want %q", field, reuseRec[field], want)
		}
	}
	probeRec := findByReason(records, "task_read")[0]
	if got, _ := probeRec["task_id"].(string); got != "no-such-task-"+env.sfx {
		t.Fatalf("probe record task_id = %v, want the probed id", probeRec["task_id"])
	}
	foreignRec := findByReason(records, gateway.ReasonContextNotOwned)[0]
	if got, _ := foreignRec["caller"].(string); got != "intruder-"+env.sfx {
		t.Fatalf("foreign record caller = %v, want the presenting caller", foreignRec["caller"])
	}

	// No request content travels: neither the accepted prompt nor any refused
	// payload may appear anywhere in the journal.
	journal := logs.String()
	for _, marker := range []string{promptMarker, "SECOND-PAYLOAD-", "FIRST-PAYLOAD-", "please act on"} {
		if strings.Contains(journal, marker) {
			t.Fatalf("journal leaked request content %q:\n%s", marker, journal)
		}
	}
	if strings.Contains(journal, "PROMPT-MARKER") {
		t.Fatalf("journal leaked the prompt marker:\n%s", journal)
	}
}

func reasonsOf(records []map[string]any) []string {
	out := make([]string, 0, len(records))
	for _, r := range records {
		out = append(out, stringOf(r["reason"])+"/"+stringOf(r["code"]))
	}
	return out
}

func stringOf(v any) string {
	s, _ := v.(string)
	return s
}

// TestRefusalClientAnswerIsByteIdentical pins the other half of the fix: the
// answer on the wire did not gain the guard name, the reason or the cause line.
// The literals below are the canon strings this revision already answered with;
// if this test ever fails, the refusal became an EXIF dump.
func TestRefusalClientAnswerIsByteIdentical(t *testing.T) {
	env := newEnv(t)
	svc := env.service(t, echoExecutor(), nil)
	ts, logs := refusalFront(t, svc, env.scope, env.caller)

	const rejectedMessage = "gateway: request rejected"

	empty := rpc(t, ts, gateway.MethodSendMessage, emptySendParams(t, "msg-empty-"+env.sfx), env.caller, 1)
	shared := "msg-shared-" + env.sfx
	if first := rpc(t, ts, gateway.MethodSendMessage, sendParams(t, shared, "PAYLOAD-A-"+env.sfx, ""), env.caller, 2); first.Error != nil {
		t.Fatalf("baseline send refused: %+v", first.Error)
	}
	reuse := rpc(t, ts, gateway.MethodSendMessage, sendParams(t, shared, "PAYLOAD-B-"+env.sfx, ""), env.caller, 3)

	for name, reply := range map[string]rpcReply{"empty_content": empty, "key_reuse": reuse} {
		if reply.Error == nil {
			t.Fatalf("%s: want a refusal", name)
		}
		if reply.Error.Code != -32602 {
			t.Fatalf("%s: jsonrpc code = %d want -32602 (body=%s)", name, reply.Error.Code, reply.RawBody)
		}
		if reply.Error.Message != rejectedMessage {
			t.Fatalf("%s: client message = %q want the unchanged %q", name, reply.Error.Message, rejectedMessage)
		}
		for _, leak := range []string{gateway.ReasonContentEmpty, gateway.ReasonKeyReuseMismatch, contracts.ErrKeyReuse, "detail", "reason="} {
			if strings.Contains(reply.RawBody, leak) {
				t.Fatalf("%s: client answer enriched with %q: %s", name, leak, reply.RawBody)
			}
		}
	}

	// The two refusals are indistinguishable to the client...
	if empty.Error.Message != reuse.Error.Message || empty.Error.Code != reuse.Error.Code {
		t.Fatalf("client answers drifted apart: %+v vs %+v", empty.Error, reuse.Error)
	}
	// ...and distinguishable to the operator.
	records := refusalRecords(t, logs)
	if len(findByReason(records, gateway.ReasonContentEmpty)) != 1 || len(findByReason(records, gateway.ReasonKeyReuseMismatch)) != 1 {
		t.Fatalf("operator cannot separate the two refusals: %v", reasonsOf(records))
	}
}
