package space

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Measured operational fact these legs pin. Root reads refusals off the stand by REQUEST ID
// (reports/root-service-audit-20261109/ROOT-C100-403-READONLY-20261110T0112Z.md was built exactly
// that way), and directive 2026-11-10 п.3 requires the durable record of a run-grant refusal to carry
// an identifier that is available WITHOUT a span: a span exists only when the OTel middleware wrapped
// the call, so trace_id is not a reliable join key, and the refused run has no run row to join on
// either. So the correlation id must be a request-scoped identifier adopted from the caller when the
// caller sent one, generated at the entry when it did not, echoed on the reply so the caller knows
// what to quote, and written into the refusal record.
//
// The adoption rule is a trust boundary, not a convenience: the inbound header is attacker-controlled
// text going into a structured log line and into a response header, so it is adopted only while it is
// short and made of characters that cannot forge a log field or a header line. Anything else is
// discarded for a freshly generated id. Nothing here logs a bearer, a body or a subject.

const correlationHeader = "X-Request-Id"

const scopedTokenNeverLogged = "scoped-task-token-must-never-be-logged-4f2a"

// maxAdopted is the length the legs expect an adopted id to survive; longer inbound values must be
// replaced rather than clipped, because a clipped id silently belongs to a different request.
const maxAdopted = 64

// Leg 1: every reply from the Space edge names its request, even one rejected before any handler ran.
// A correlation id that only exists on successful calls cannot answer "why did this call fail".
func TestSpaceRepliesAlwaysCarryACorrelationID(t *testing.T) {
	h := NewHandler(HandlerConfig{Coordinator: NewCoordinator(CoordinatorConfig{WorkerID: "w-corr"})})
	for _, route := range []string{"/v1/space/NoSuchOperation", "/not/space"} {
		req := httptest.NewRequest(http.MethodPost, route, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		got := rec.Header().Get(correlationHeader)
		if got == "" {
			t.Fatalf("%s: every reply must name its request, otherwise the caller cannot quote an id for a failure "+
				"it was refused; got no %s header (status %d)", route, correlationHeader, rec.Code)
		}
		if utf8.RuneCountInString(got) > maxAdopted {
			t.Fatalf("a generated id must stay inside the bounded header value, got %q", got)
		}
	}
}

// Leg 2: the caller's id is the one that comes back, as long as it is safe to carry; an unsafe or
// oversized one is replaced, never echoed and never clipped.
func TestCallerCorrelationIDAdoptedOnlyWhenSafe(t *testing.T) {
	h := NewHandler(HandlerConfig{Coordinator: NewCoordinator(CoordinatorConfig{WorkerID: "w-corr"})})

	safe := []string{"corr-1a2b", "req_20261110T030000Z_9f8e7d6c5b4a3f2e1d0c", "a.b:c-d_1"}
	for _, inbound := range safe {
		req := httptest.NewRequest(http.MethodPost, "/v1/space/NoSuchOperation", nil)
		req.Header.Set(correlationHeader, inbound)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if got := rec.Header().Get(correlationHeader); got != inbound {
			t.Fatalf("a safe caller id must be adopted verbatim so the caller and the log name the same request: sent %q, got %q", inbound, got)
		}
	}

	unsafe := []string{
		strings.Repeat("x", maxAdopted+40),    // oversized: clipping would silently rename the request
		"spoofed id with spaces",              // spaces and separators forge a log field
		"corr-1\r\nX-Forwarded-For: 10.0.0.1", // header injection must not travel
		"corr-1 \"quoted\"",                   // a quote forces a structured-log escape, so it is not adopted verbatim
	}
	for _, inbound := range unsafe {
		req := httptest.NewRequest(http.MethodPost, "/v1/space/NoSuchOperation", nil)
		req.Header.Set(correlationHeader, inbound)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		got := rec.Header().Get(correlationHeader)
		if got == "" {
			t.Fatalf("an unsafe caller id must be replaced by a generated one, not dropped: the reply still has to name its request")
		}
		if got == inbound {
			t.Fatalf("an unsafe caller id must never be echoed back (%q): it would put caller text into a header and a log verbatim", inbound)
		}
		if utf8.RuneCountInString(got) > maxAdopted {
			t.Fatalf("the replacement id must be bounded, got %q", got)
		}
	}
}

// correlationStateStand answers the ONE State call AcceptTurn makes before it asks Identity for a run
// grant, so a leg can reach the refusal over the real HTTP edge instead of calling the helper.
func correlationStateStand(t *testing.T) *StateClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/CreateThread") {
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "thread-corr-1", "context_id": "corr-ctx-7", "epoch": 1})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": "INTERNAL", "message": "stand: not needed by this leg"})
	}))
	t.Cleanup(srv.Close)
	return NewStateClient(srv.URL, srv.Client())
}

// correlationAuth stands in for the auth chain this seam sits behind: it injects the verified principal,
// the scoped task token and the Tier-1 ingress authority task, exactly as production's middleware does,
// so nothing below the edge needs the raw Authorization bearer.
func correlationAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := transport.WithPrincipal(r.Context(), transport.Principal{
			Scope: contracts.Scope{Tenant: "t-corr", Deployment: "space", Subject: "u-corr"},
		})
		ctx = WithScopedTask(ctx, scopedTokenNeverLogged)
		ctx = WithAuthorityTask(ctx, "parent-authority-task")
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Leg 3 (the requirement itself): the refusal record names the request that was refused, with no span
// anywhere on the path, and the caller sees the same id on the reply it was refused with.
func TestRunGrantRefusalRecordNamesTheRequestThatWasRefused(t *testing.T) {
	const wantID = "corr-delegate-7f3a"
	logs := captureSpaceLogs(t)
	stub := &delegateStub{t: t, status: http.StatusInternalServerError,
		body: contractsBody(t, "INTERNAL", "internal server error")}
	c := NewCoordinator(CoordinatorConfig{
		StateClient:     correlationStateStand(t),
		WorkerID:        "w-corr",
		RunGrantIssuer:  delegateIssuer(t, stub),
		RequireRunGrant: true,
	})
	h := NewHandler(HandlerConfig{Coordinator: c, Auth: correlationAuth})

	body, err := json.Marshal(acceptReq("corr-ctx-7"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/space/AcceptTurn", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(correlationHeader, wantID)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("an Identity outage on the run grant must answer 503, got %d (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(correlationHeader); got != wantID {
		t.Fatalf("the refusal reply must carry the request id the caller will quote, got %q", got)
	}

	line := logs.String()
	if !strings.Contains(line, "run grant refused") {
		t.Fatalf("the refusal must leave a server-side record, got: %q", line)
	}
	if !strings.Contains(line, "request_id="+wantID) {
		t.Fatalf("the record must join to the caller's request WITHOUT a span - trace ids only exist when the OTel "+
			"middleware ran, and root reads refusals by request id; got: %s", line)
	}
	if !strings.Contains(line, "identity:delegate:http-500") {
		t.Fatalf("the record must keep the upstream cause, got: %s", line)
	}
	for _, forbidden := range []string{scopedTokenNeverLogged, "Bearer "} {
		if strings.Contains(line, forbidden) {
			t.Fatalf("the record must never carry %q: %s", forbidden, line)
		}
	}
}
