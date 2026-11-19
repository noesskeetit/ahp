package space

import (
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

// refusalFixture answers Registry's owner API with one refusal and records that it was called.
type refusalFixture struct {
	status  int
	body    string
	called  bool
	path    string
	bearer  string
	content string
}

func (f *refusalFixture) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.called = true
		f.path = r.URL.Path
		f.bearer = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", f.contentType())
		w.WriteHeader(f.status)
		_, _ = w.Write([]byte(f.body))
	})
}

func (f *refusalFixture) contentType() string {
	if f.content != "" {
		return f.content
	}
	return "application/json"
}

// TestProfilePinRefusalKeepsItsClass states the contract this lane was opened for: Registry's
// refusal of PinDotConfig must reach AcceptTurn's caller in the class Registry chose.
//
// Measured on the live density stand (2026-11-09T21:53Z..22:12Z): 714 AcceptTurn calls answered
// `500 INTERNAL / internal server error` in 15-49 ms while Registry had answered the pin with
// `503 {"code":"UNAVAILABLE",...}`. Nothing named the dependency, and a caller could not tell a
// transient Registry outage from its own bad request. `artifactsRefusal` already keeps canonical
// codes for Artifacts, but `canonicalContractCode` mirrored an older `transport.WriteError`
// switch and did not know `ErrUnavailable`, so an UNAVAILABLE envelope became *upstreamRefusal,
// whose code no renderer on the AcceptTurn path knows -- and WriteError's default branch is 500.
//
// A refusal is never turned into success here, and no authorization is consulted or widened:
// every case below asserts a refusal, only its class and its cause are under test.
func TestProfilePinRefusalKeepsItsClass(t *testing.T) {
	for _, tc := range []struct {
		name               string
		status             int
		body               string
		wantStatus         int
		wantCode           string
		wantReason         string
		wantMessageHas     string
		wantUpstreamStatus int
	}{
		{
			name:           "registry unavailable envelope stays 503 with its cause",
			status:         http.StatusServiceUnavailable,
			body:           `{"code":"UNAVAILABLE","message":"registry: model route is not ready","reason":"dependency_unavailable"}`,
			wantStatus:     http.StatusServiceUnavailable,
			wantCode:       contracts.ErrUnavailable,
			wantReason:     "dependency_unavailable",
			wantMessageHas: "model route is not ready",
		},
		{
			name:           "registry unavailable envelope without a reason keeps its cause",
			status:         http.StatusServiceUnavailable,
			body:           `{"code":"UNAVAILABLE","message":"registry: admission cold start"}`,
			wantStatus:     http.StatusServiceUnavailable,
			wantCode:       contracts.ErrUnavailable,
			wantMessageHas: "admission cold start",
		},
		{
			name:       "dependency outage stated with no envelope at all is still a dependency failure",
			status:     http.StatusServiceUnavailable,
			body:       `<html>upstream temporarily down</html>`,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   contracts.ErrUnavailable,
		},
		{
			name:           "bad gateway from the dependency is a dependency failure too",
			status:         http.StatusBadGateway,
			body:           `{"code":"UNAVAILABLE","message":"registry: gateway refused"}`,
			wantStatus:     http.StatusServiceUnavailable,
			wantCode:       contracts.ErrUnavailable,
			wantMessageHas: "gateway refused",
		},
		{
			name:           "forbidden refusal keeps 403 and its own sentence",
			status:         http.StatusForbidden,
			body:           `{"code":"FORBIDDEN","message":"registry: capability registry:admin required"}`,
			wantStatus:     http.StatusForbidden,
			wantCode:       contracts.ErrForbidden,
			wantMessageHas: "registry:admin required",
		},
		{
			name:           "conflict refusal keeps its conflict status",
			status:         http.StatusConflict,
			body:           `{"code":"CONFLICT","message":"registry: revision moved"}`,
			wantStatus:     http.StatusConflict,
			wantCode:       contracts.ErrConflict,
			wantMessageHas: "revision moved",
		},
		{
			name:           "stale epoch refusal keeps 409 with the domain code as reason",
			status:         http.StatusConflict,
			body:           `{"code":"STALE_EPOCH","message":"registry: pinned epoch is gone"}`,
			wantStatus:     http.StatusConflict,
			wantCode:       contracts.ErrStaleEpoch,
			wantMessageHas: "pinned epoch is gone",
		},
		{
			// Documented remainder, asserted rather than hidden: a code outside the contracts
			// vocabulary has no renderer on this seam, so the HTTP answer is still the generic
			// one. The class survives on the error value (asserted below), which is what a
			// caller that inspects the error can act on. Closing the rendering gap means
			// answering AcceptTurn through `writeRefusal` the way the download proxy already
			// does -- deliberately out of this lane's scope.
			name:               "a code outside the contracts vocabulary survives on the error, not yet on the wire",
			status:             http.StatusRequestedRangeNotSatisfiable,
			body:               `{"code":"range_not_satisfiable","message":"registry: no such bytes"}`,
			wantStatus:         http.StatusInternalServerError,
			wantCode:           "INTERNAL",
			wantUpstreamStatus: http.StatusRequestedRangeNotSatisfiable,
		},
		{
			name:       "an internal upstream bug is not relabelled as a dependency outage",
			status:     http.StatusInternalServerError,
			body:       `{"code":"INTERNAL","message":"registry: nil map"}`,
			wantStatus: http.StatusInternalServerError,
			wantCode:   "INTERNAL", // the refusal's own code travels, un-invented
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := &refusalFixture{status: tc.status, body: tc.body}
			if strings.HasPrefix(fixture.body, "<") {
				fixture.content = "text/html; charset=utf-8"
			}
			server := httptest.NewUnstartedServer(fixture.handler())
			server.EnableHTTP2 = false
			server.StartTLS()
			defer server.Close()

			pinner, err := NewProfileClient(server.URL, server.Client())
			if err != nil {
				t.Fatalf("NewProfileClient(%s): %v", server.URL, err)
			}
			_, pinErr := pinner.Pin(context.Background(), "lane-task-token", "pin-key", "pin-context")
			if pinErr == nil {
				t.Fatal("Pin accepted a refusal: the fixture answered a non-2xx status")
			}
			if !fixture.called || fixture.path != "/v1/registry/PinDotConfig" {
				t.Fatalf("fixture was not exercised as Registry's owner API: called=%v path=%q", fixture.called, fixture.path)
			}
			if !strings.HasPrefix(fixture.bearer, "Bearer ") {
				t.Fatalf("the owner API call lost its bearer token (Authorization header present=%v)", fixture.bearer != "")
			}

			recorder := httptest.NewRecorder()
			transport.WriteError(recorder, httptest.NewRequest(http.MethodPost, "/v1/space/AcceptTurn", nil), pinErr)
			response := recorder.Result()
			defer response.Body.Close()
			if response.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d; refusal body was %q, surfaced as %s",
					response.StatusCode, tc.wantStatus, tc.body, recorder.Body.String())
			}
			var envelope struct {
				Code    string `json:"code"`
				Reason  string `json:"reason"`
				Message string `json:"message"`
			}
			if err = json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("answer is not an error envelope: %v (%s)", err, recorder.Body.String())
			}
			if envelope.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q (pin error: %v)", envelope.Code, tc.wantCode, pinErr)
			}
			if contracts.Code(pinErr) != codeForWant(tc.wantCode) {
				t.Fatalf("contracts.Code(Pin error) = %q, want %q -- the caller cannot classify the refusal without it",
					contracts.Code(pinErr), codeForWant(tc.wantCode))
			}
			if tc.wantReason != "" && contracts.ReasonOf(pinErr) != tc.wantReason {
				t.Fatalf("reason = %q, want %q -- the dependency's own cause must survive", contracts.ReasonOf(pinErr), tc.wantReason)
			}
			if tc.wantMessageHas != "" && !strings.Contains(envelope.Message, tc.wantMessageHas) {
				t.Fatalf("message = %q, want it to carry %q", envelope.Message, tc.wantMessageHas)
			}
			if tc.wantUpstreamStatus != 0 {
				var stated *upstreamRefusal
				if !errors.As(pinErr, &stated) {
					t.Fatalf("a refusal outside the vocabulary must stay an *upstreamRefusal, got %T (%v)", pinErr, pinErr)
				}
				if stated.status != tc.wantUpstreamStatus {
					t.Fatalf("carried upstream status = %d, want %d", stated.status, tc.wantUpstreamStatus)
				}
				if contracts.Code(pinErr) != "" {
					t.Fatalf("a contracts code was invented for %q: %q", tc.body, contracts.Code(pinErr))
				}
			}
			if envelope.Code == "INTERNAL" && tc.wantCode != "INTERNAL" {
				t.Fatalf("a stated refusal collapsed into the generic internal error")
			}
		})
	}
}

// codeForWant names the contracts code the CALLER must see on the error value. A refusal the
// contracts vocabulary cannot express carries none -- inventing one would be the lie this
// lane's defect was about, in the other direction.
func codeForWant(surfaced string) string {
	switch surfaced {
	case "":
		return ""
	case "INTERNAL":
		return ""
	default:
		return surfaced
	}
}

// TestProfilePinTransportFailureIsAlreadyTyped pins the neighbouring case so a future edit to
// the refusal path cannot quietly move the transport failure (no answer at all) away from
// UNAVAILABLE, which is the only reason the 503-with-no-envelope case above was surprising.
func TestProfilePinTransportFailureIsAlreadyTyped(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.StartTLS()
	address := server.URL
	server.Close() // nothing is listening any more: the call must fail at the transport

	pinner, err := NewProfileClient(address, (&http.Client{}))
	if err != nil {
		t.Fatalf("NewProfileClient: %v", err)
	}
	_, pinErr := pinner.Pin(context.Background(), "lane-task-token", "pin-key", "pin-context")
	if pinErr == nil {
		t.Fatal("Pin reported success against a closed listener")
	}
	if code := contracts.Code(pinErr); code != contracts.ErrUnavailable {
		t.Fatalf("code = %q, want %q", code, contracts.ErrUnavailable)
	}
	recorder := httptest.NewRecorder()
	transport.WriteError(recorder, httptest.NewRequest(http.MethodPost, "/v1/space/AcceptTurn", nil), pinErr)
	if recorder.Result().StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Result().StatusCode)
	}
}
