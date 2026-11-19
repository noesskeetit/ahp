package triggers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestFireMessageIDIsDeterministic proves the dedup precondition of PDF rev
// 3.2, c. 30 п.1: "Повтор message/send с тем же messageId не создаёт вторую
// задачу". The messageId must be a pure function of (trigger, sequence), so a
// replay of the same fire cannot create a second task.
func TestFireMessageIDIsDeterministic(t *testing.T) {
	a := FireMessageID("trigger-1", 7)
	b := FireMessageID("trigger-1", 7)
	if a != b {
		t.Fatalf("messageId is not deterministic: %s vs %s", a, b)
	}
	if a == FireMessageID("trigger-1", 8) || a == FireMessageID("trigger-2", 7) {
		t.Fatal("messageId collides across fires")
	}
	if !strings.HasPrefix(a, "trg-") || len(a) > 64 {
		t.Fatalf("messageId shape: %q", a)
	}
}

// TestA2AFireSendsOneMessageWithFreshToken proves the platform rule "a trigger
// fire is an A2A message to the agent" (rev 3.2, c. 28) carried under the
// token re-issued at fire time (c. 48).
func TestA2AFireSendsOneMessageWithFreshToken(t *testing.T) {
	var gotAuth, gotMethod, gotMessageID, gotText string
	var gotBlocking any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Method string `json:"method"`
			Params struct {
				Message struct {
					MessageID string `json:"messageId"`
					Parts     []struct {
						Kind string `json:"kind"`
						Text string `json:"text"`
					} `json:"parts"`
				} `json:"message"`
				Configuration map[string]any `json:"configuration"`
			} `json:"params"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("gateway could not parse the fire message: %v", err)
		}
		gotAuth = r.Header.Get("authorization")
		gotMethod = req.Method
		gotMessageID = req.Params.Message.MessageID
		if len(req.Params.Message.Parts) > 0 {
			gotText = req.Params.Message.Parts[0].Text
		}
		gotBlocking = req.Params.Configuration["blocking"]
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"task":{"id":"task-77","contextId":"ctx-1","status":{"state":"TASK_STATE_WORKING"}}}}`))
	}))
	defer srv.Close()

	d, err := NewA2ADispatcher(A2AConfig{URL: srv.URL})
	if err != nil {
		t.Fatalf("NewA2ADispatcher: %v", err)
	}
	res, err := d.Fire(context.Background(), contracts.Scope{}, FireRequest{
		TriggerID: "trg-abc", Sequence: 3, Token: "freshly-re-issued-token",
		Target: "agent:hr", Kind: KindSchedule, Payload: json.RawMessage(`{"text":"write the weekly report"}`),
	})
	if err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if res.TaskID != "task-77" {
		t.Fatalf("dispatched task id = %q, want task-77 (the A2A task the platform started)", res.TaskID)
	}
	if gotMethod != "SendMessage" {
		t.Fatalf("A2A method = %q", gotMethod)
	}
	if want := FireMessageID("trg-abc", 3); gotMessageID != want {
		t.Fatalf("messageId = %q, want the deterministic %q", gotMessageID, want)
	}
	if gotAuth != "Bearer freshly-re-issued-token" {
		t.Fatal("the fire must carry the freshly re-issued task token")
	}
	if gotText != "write the weekly report" {
		t.Fatalf("message text = %q", gotText)
	}
	if gotBlocking != false {
		t.Fatalf("a trigger fire must not block a worker: blocking=%v", gotBlocking)
	}
}

// TestA2ARefusalIsProvablyPreEffect separates a refusal from an ambiguous
// outcome (rev 3.2, c. 13/c. 54) and proves the token never reaches an error.
func TestA2ARefusalIsProvablyPreEffect(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		body     string
		refused  bool
		wantCode string
	}{
		{"forbidden before any task exists", http.StatusForbidden, `{"error":"nope"}`, true, "FORBIDDEN"},
		{"unknown agent route", http.StatusNotFound, `{"error":"nope"}`, true, "NOT_FOUND"},
		{"gateway json-rpc verdict", http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"no such method"}}`, true, "A2A_ERROR"},
		{"gateway overload is ambiguous", http.StatusTooManyRequests, `{"error":"busy"}`, false, "RESOURCE_EXHAUSTED"},
		{"gateway failure is ambiguous", http.StatusBadGateway, `{"error":"down"}`, false, "UNAVAILABLE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			d, err := NewA2ADispatcher(A2AConfig{URL: srv.URL})
			if err != nil {
				t.Fatalf("dispatcher: %v", err)
			}
			_, err = d.Fire(context.Background(), contracts.Scope{}, FireRequest{
				TriggerID: "trg-x", Sequence: 1, Token: "super-secret-token-value", Target: "agent:x", Kind: KindSchedule,
			})
			if err == nil {
				t.Fatalf("expected an error for status %d", tc.status)
			}
			if got := errors.Is(err, ErrDispatchRefused); got != tc.refused {
				t.Fatalf("refused = %v, want %v (err=%v)", got, tc.refused, err)
			}
			var a2aErr *A2AError
			if !errors.As(err, &a2aErr) || a2aErr.Code != tc.wantCode {
				t.Fatalf("error code = %v (%v), want %s", err, errors.As(err, &a2aErr), tc.wantCode)
			}
			if strings.Contains(err.Error(), "super-secret-token-value") {
				t.Fatal("a dispatcher error leaked the bearer token")
			}
		})
	}
}

// TestA2AConfigFailsClosed guards the transport assumptions of a fire: a
// plaintext endpoint that is not loopback is refused, and a fire without the
// re-issued token is refused.
func TestA2AConfigFailsClosed(t *testing.T) {
	if _, err := NewA2ADispatcher(A2AConfig{}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("missing URL code = %q", contracts.Code(err))
	}
	if _, err := NewA2ADispatcher(A2AConfig{URL: "http://gateway.internal:8080/a2a"}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("cleartext remote URL code = %q, want a refusal", contracts.Code(err))
	}
	d, err := NewA2ADispatcher(A2AConfig{URL: "https://gateway.example/a2a/public", Timeout: time.Second})
	if err != nil {
		t.Fatalf("https must be accepted: %v", err)
	}
	if _, err := d.Fire(context.Background(), contracts.Scope{}, FireRequest{TriggerID: "t", Sequence: 1}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("fire without a token code = %q", contracts.Code(err))
	}
}
