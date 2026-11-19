package space

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// These calls use the production HTTP client and error renderer. Removing the
// 409 mapping must fail every seam; widening it to all 4xx must fail the denial
// controls. A refusal never returns a credential or silently retries.
func TestIdentityConflictClassRequiresBothStatusAndEnvelope(t *testing.T) {
	for _, seam := range []struct {
		name string
		call func(*IdentityDelegateIssuer) (string, error)
	}{
		{"delegate", func(i *IdentityDelegateIssuer) (string, error) {
			return i.Delegate(context.Background(), stubParentToken, "parent-task", stubSpec())
		}},
		{"cancel", func(i *IdentityDelegateIssuer) (string, error) {
			return "", i.Cancel(context.Background(), stubParentToken, "child-task")
		}},
		{"mint", func(i *IdentityDelegateIssuer) (string, error) {
			return i.ChildToken(context.Background(), stubParentToken, "child-task")
		}},
		{"child-cancel", func(i *IdentityDelegateIssuer) (string, error) {
			return "", i.CancelChild(context.Background(), stubParentToken, "parent-task", "child-task")
		}},
	} {
		for _, tc := range []struct {
			name       string
			status     int
			code       string
			wantCode   string
			wantStatus int
		}{
			{"conflict", 409, contracts.ErrConflict, contracts.ErrConflict, 409},
			{"forbidden", 403, contracts.ErrForbidden, contracts.ErrForbidden, 403},
			{"unauthorized", 401, contracts.ErrForbidden, contracts.ErrForbidden, 403},
			{"denial-with-conflict-body", 403, contracts.ErrConflict, contracts.ErrForbidden, 403},
			{"conflict-with-denial-body", 409, contracts.ErrForbidden, contracts.ErrForbidden, 403},
			{"conflict-without-envelope", 409, "", contracts.ErrForbidden, 403},
			{"outage-with-conflict-body", 503, contracts.ErrConflict, contracts.ErrUnavailable, 503},
		} {
			t.Run(seam.name+"/"+tc.name, func(t *testing.T) {
				body := "not a contract envelope"
				if tc.code != "" {
					body = contractsBody(t, tc.code, "identity: concurrent write; replay unchanged")
				}
				stub := &delegateStub{t: t, status: tc.status, body: body}
				token, err := seam.call(delegateIssuer(t, stub))
				if err == nil || token != "" {
					t.Fatal("a refusal must return no credential")
				}
				var typed *contracts.Error
				if !errors.As(err, &typed) || typed.Code != tc.wantCode {
					t.Fatalf("typed class = %q, want %q", contracts.Code(err), tc.wantCode)
				}
				rec := httptest.NewRecorder()
				// Exercise the same renderer used by AcceptTurn.
				transport.WriteError(rec, httptest.NewRequest(http.MethodPost, "/", nil), err)
				if rec.Code != tc.wantStatus {
					t.Fatalf("HTTP = %d, want %d", rec.Code, tc.wantStatus)
				}
				if stub.calls != 1 {
					t.Fatalf("client silently retried a refusal: calls=%d", stub.calls)
				}
			})
		}
	}
}

func TestAcceptTurnConflictKeepsCauseAndRequestCorrelation(t *testing.T) {
	const requestID = "admission-conflict-7c1"
	const reason = "identity:delegate:http-409:conflict"
	const cause = "identity: the database aborted this write 14 times as concurrent within a replay budget of 5s (SQLSTATE 40001); replay the request unchanged"
	logs := captureSpaceLogs(t)
	stub := &delegateStub{t: t, status: 409, body: contractsBody(t, contracts.ErrConflict, cause)}
	c := NewCoordinator(CoordinatorConfig{
		StateClient: correlationStateStand(t), WorkerID: "conflict-test",
		RunGrantIssuer: delegateIssuer(t, stub), RequireRunGrant: true,
	})
	body, err := json.Marshal(acceptReq("corr-ctx-7"))
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/space/AcceptTurn", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(correlationHeader, requestID)
	rec := httptest.NewRecorder()
	NewHandler(HandlerConfig{Coordinator: c, Auth: correlationAuth}).ServeHTTP(rec, req)
	if rec.Code != 409 {
		t.Fatalf("AcceptTurn conflict = %d, want 409", rec.Code)
	}
	var refusal struct {
		Code, Reason, Message string
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &refusal); err != nil {
		t.Fatal(err)
	}
	if refusal.Code != contracts.ErrConflict || refusal.Reason != reason || !strings.Contains(refusal.Message, "SQLSTATE 40001") {
		t.Fatalf("lost conflict class/cause: %+v", refusal)
	}
	if rec.Header().Get(correlationHeader) != requestID {
		t.Fatal("reply lost its request correlation")
	}
	for _, want := range []string{"request_id=" + requestID, "contract_code=CONFLICT", reason, "SQLSTATE 40001"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("refusal log lost %q", want)
		}
	}
	for _, secret := range []string{stubParentToken, scopedTokenNeverLogged, "Bearer "} {
		if strings.Contains(rec.Body.String()+logs.String(), secret) {
			t.Fatal("credential escaped through the refusal")
		}
	}
	if stub.calls != 1 {
		t.Fatalf("AcceptTurn must not retry the dependency implicitly: %d calls", stub.calls)
	}
}
