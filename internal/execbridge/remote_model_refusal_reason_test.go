package execbridge

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DEF-EXECBRIDGE-REFUSAL-REASON-1. A refusal from the Model Gateway carries the
// contract code and the message that names the refusing check. Dropping that
// body left the durable transcript with "model stream refused: HTTP 403", which
// a reader in another process cannot tell apart from any other 403. These legs
// pin the client side: status semantics stay, authorization stays refused,
// only the lost reason comes back.

func refusalRequest() *core.ModelRequest {
	return &core.ModelRequest{
		Messages:  []core.ChatMessage{{Role: "user", Content: json.RawMessage(`"cause a refusal"`)}},
		MaxTokens: 16,
	}
}

func streamRefusalError(t *testing.T, status int, body string) (error, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer current-run-token" {
			t.Error("refusal path did not use the active run credential")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	client, err := NewRemoteModel(srv.URL, "default-chat", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GenerateStream(remoteTestContext(), refusalRequest(), func(core.ModelDelta) error { return nil })
	return err, &calls
}

func TestRemoteModelStreamRefusalCarriesContractCodeAndRefusingCheck(t *testing.T) {
	err, calls := streamRefusalError(t, http.StatusForbidden,
		`{"code":"FORBIDDEN","message":"FORBIDDEN: child must share tenant and subject"}`)
	if err == nil {
		t.Fatal("gateway refusal was reported as success")
	}
	if got := contracts.Code(err); got != contracts.ErrForbidden {
		t.Fatalf("contract code lost from the refusal: code=%q err=%q", got, err)
	}
	if !strings.Contains(err.Error(), "child must share tenant and subject") {
		t.Fatalf("refusing check lost, reader cannot tell which guard fired: %q", err)
	}
	// The historical text stays a substring: the Dot harness and existing
	// operator habits match on it, and the status must not be remapped.
	if !strings.Contains(err.Error(), "model stream refused: HTTP 403") {
		t.Fatalf("refusal status/name lost: %q", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("an authorization refusal was retried %d times", calls.Load())
	}
}

func TestRemoteModelStreamRefusalCarriesStableReason(t *testing.T) {
	err, _ := streamRefusalError(t, http.StatusBadRequest,
		`{"code":"INVALID_ARGUMENT","message":"INVALID_ARGUMENT: max_tokens 8192 exceeds this deployment's completion cap 4096 (AHP_EXEC_MAX_TOKENS)","reason":"`+ReasonCompletionCapExceeded+`"}`)
	if err == nil {
		t.Fatal("gateway refusal was reported as success")
	}
	if got := contracts.Code(err); got != contracts.ErrInvalid {
		t.Fatalf("code=%q err=%q", got, err)
	}
	if got := contracts.ReasonOf(err); got != ReasonCompletionCapExceeded {
		t.Fatalf("stable reason lost: reason=%q err=%q", got, err)
	}
	if !strings.Contains(err.Error(), "completion cap 4096") {
		t.Fatalf("the fixable number never reached the caller: %q", err)
	}
}

// The refusal text is also the close-reason dictionary's input
// (internal/gateway/budget.go ClassifyCloseReason). Naming the check must not
// move a governance refusal into the transport or step-limit class.
func TestRemoteModelStreamRefusalKeepsCloseReasonClassification(t *testing.T) {
	for _, message := range []string{
		"FORBIDDEN: child must share tenant and subject",
		"FORBIDDEN: task ancestry changes owner",
	} {
		err, _ := streamRefusalError(t, http.StatusForbidden, `{"code":"FORBIDDEN","message":"`+message+`"}`)
		if err == nil {
			t.Fatalf("%q reported as success", message)
		}
		if gateway.IsTransportFailure(err.Error()) {
			t.Fatalf("governance refusal reads as a transport failure: %q", err)
		}
		if got := gateway.ClassifyCloseReason(nil, err.Error(), true, "", 1, 4); got != gateway.CloseReasonError {
			t.Fatalf("close reason for %q = %q, want %q (err=%q)", message, got, gateway.CloseReasonError, err)
		}
	}
}

func TestRemoteModelUnaryRefusalCarriesRefusingCheck(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"FORBIDDEN","message":"FORBIDDEN: task ancestry changes owner"}`))
	}))
	defer srv.Close()
	client, err := NewRemoteModel(srv.URL, "default-chat", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Generate(remoteTestContext(), refusalRequest())
	if err == nil {
		t.Fatal("gateway refusal was reported as success")
	}
	if got := contracts.Code(err); got != contracts.ErrForbidden {
		t.Fatalf("code=%q err=%q", got, err)
	}
	if !strings.Contains(err.Error(), "task ancestry changes owner") {
		t.Fatalf("unary path still drops the message field: %q", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("refusal retried %d times", calls.Load())
	}
}

// A dependency failure keeps its own class: 503 stays 503 and UNAVAILABLE
// stays readable, so an operator is not sent looking for a broken caller.
func TestRemoteModelStreamRefusalKeepsUnavailableClass(t *testing.T) {
	err, _ := streamRefusalError(t, http.StatusServiceUnavailable,
		`{"code":"UNAVAILABLE","message":"UNAVAILABLE: model gateway dependency is not answering"}`)
	if err == nil {
		t.Fatal("gateway refusal was reported as success")
	}
	if got := contracts.Code(err); got != contracts.ErrUnavailable {
		t.Fatalf("code=%q err=%q", got, err)
	}
	if !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("status remapped: %q", err)
	}
}

// A non-contract body (proxy error page, truncated write) must stay bounded and
// must not be echoed wholesale: the client never forwards an unbounded upstream
// blob into a durable frame.
func TestRemoteModelRefusalWithUndecodableBodyIsBounded(t *testing.T) {
	err, _ := streamRefusalError(t, http.StatusBadGateway,
		"<html><body>"+strings.Repeat("gateway-proxy-detail ", 400)+"</body></html>")
	if err == nil {
		t.Fatal("gateway failure was reported as success")
	}
	if !strings.Contains(err.Error(), "model stream refused: HTTP 502") {
		t.Fatalf("status/name lost: %q", err)
	}
	if got := contracts.Code(err); got != "" {
		t.Fatalf("invented a contract code %q from a non-contract body", got)
	}
	if len(err.Error()) > 512 {
		t.Fatalf("upstream body echoed without a bound: %d bytes", len(err.Error()))
	}
}
