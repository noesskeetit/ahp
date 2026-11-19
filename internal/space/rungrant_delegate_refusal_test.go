package space

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Measured fact this test pins (density stand inst-1109204501f96ab5 / root's canonical repeat
// w1-repeats-20261110T025521Z, and reports/mac-director/DELEGATE-500-and-runner-provenance-20261110T0308Z.md):
// every one of the 24 refusals was an HTTP 500 from Identity on POST /v1/identity/roots/{task}/delegate
// (backend.log per-minute split 1/11/12 for runs 1/2/4; the route logged 3626x201 and 75x500, and ZERO
// 403). Not one quota branch (max_active / max_fanout / depth / expired / revoked / root-not-active)
// fired - all of those answer 403. Yet the caller was told FORBIDDEN and the harness honestly filed
// failure_class=forbidden_entitlement, because rungrant.go threw Identity's status and body away.
//
// The legs below say: an Identity 500 is never an entitlement refusal; a stated Identity 409/CONFLICT
// keeps its retryable contract class, genuine denials stay FORBIDDEN, and 201 behaves exactly as
// before. Nothing here authorizes anything: every non-201 still fails closed and returns no token.

type delegateStub struct {
	t       *testing.T
	status  int
	body    string
	header  http.Header
	sawAuth string
	sawPath string
	sawBody string
	calls   int
}

func (s *delegateStub) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.calls++
		s.sawAuth = r.Header.Get("Authorization")
		s.sawPath = r.URL.Path
		buf := make([]byte, 512)
		n, _ := r.Body.Read(buf)
		s.sawBody = string(buf[:n])
		for k, v := range s.header {
			for _, one := range v {
				w.Header().Add(k, one)
			}
		}
		w.WriteHeader(s.status)
		_, _ = w.Write([]byte(s.body))
	}))
}

const stubParentToken = "parent-task-token-must-never-be-echoed"

func stubSpec() RunGrantSpec {
	return RunGrantSpec{Task: "run-1-child-task", Context: "ctx-1", Capabilities: []string{"model.run"}}
}

// delegateIssuer points a real issuer at the stub; the server lives until the test ends, so a leg can
// also drive the coordinator seam that holds an issuer.
func delegateIssuer(t *testing.T, stub *delegateStub) *IdentityDelegateIssuer {
	t.Helper()
	srv := stub.server()
	t.Cleanup(srv.Close)
	issuer, err := NewIdentityDelegateIssuer(srv.URL, srv.Client())
	if err != nil {
		t.Fatalf("issuer: %v", err)
	}
	return issuer
}

func delegateAgainst(t *testing.T, stub *delegateStub) (string, error) {
	t.Helper()
	return delegateIssuer(t, stub).Delegate(context.Background(), stubParentToken, "parent-attenuated-task", stubSpec())
}

// captureSpaceLogs redirects the package logger, the way the execbridge legs do, so a leg can assert a
// structured record EXISTS instead of a human eyeballing test output.
func captureSpaceLogs(t *testing.T) *strings.Builder {
	t.Helper()
	var buf strings.Builder
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

// contractsBody renders the envelope transport.WriteError actually writes for a contracts error, so
// these legs speak Identity's real wire shape rather than an invented one.
func contractsBody(t *testing.T, code, message string) string {
	t.Helper()
	raw, err := json.Marshal(&contracts.Error{Code: code, Message: message})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}

// Leg (a): an Identity 500 must never reach the caller as an entitlement refusal.
func TestIdentityInternalErrorIsNotAnEntitlementRefusal(t *testing.T) {
	body := contractsBody(t, "INTERNAL", "internal server error") // transport.WriteError's default branch
	token, err := delegateAgainst(t, &delegateStub{t: t, status: http.StatusInternalServerError, body: body})
	if err == nil {
		t.Fatalf("an Identity 500 must never yield a token, got %q", token)
	}
	if token != "" {
		t.Fatal("a failed delegate must return an empty token")
	}
	if code := contracts.Code(err); code != contracts.ErrUnavailable {
		t.Fatalf("Identity's 500 must be UNAVAILABLE, got %q (%v) - this is the forbidden_entitlement mislabel", code, err)
	}
	if reason := contracts.ReasonOf(err); !strings.Contains(reason, "http-500") {
		t.Fatalf("the refusal must name the upstream status it saw: %v", err)
	}
	if strings.Contains(err.Error(), stubParentToken) {
		t.Fatalf("a refusal must never echo the bearer token: %v", err)
	}
}

// Identity's 500 arrives unclassified (delegate.go returns an untyped fmt.Errorf, so the transport
// says "internal server error"), and a gateway in front can answer 5xx without any envelope at all.
func TestIdentityServerErrorWithoutEnvelopeIsStillUnavailable(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		_, err := delegateAgainst(t, &delegateStub{t: t, status: status, body: "<html>bad gateway</html>"})
		if err == nil {
			t.Fatalf("HTTP %d must fail closed", status)
		}
		if code := contracts.Code(err); code != contracts.ErrUnavailable {
			t.Fatalf("HTTP %d must be UNAVAILABLE, got %q (%v)", status, code, err)
		}
	}
}

// Retry-After is the only thing an upstream outage said that the caller can act on. Space does not
// render response headers from a contracts error on this seam, so the value is carried in the text.
func TestIdentityRetryHintSurvivesTheMapping(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "2")
	_, err := delegateAgainst(t, &delegateStub{t: t,
		status: http.StatusServiceUnavailable,
		body:   contractsBody(t, contracts.ErrUnavailable, "identity: delegate retries exhausted"),
		header: h})
	if err == nil {
		t.Fatal("unavailable must fail closed")
	}
	if !strings.Contains(err.Error(), "retry after 2") {
		t.Fatalf("the upstream retry hint must reach the caller: %v", err)
	}
}

// Leg (b): a real entitlement refusal keeps its FORBIDDEN class, but Identity's stated code and
// message must arrive instead of the anonymous "space: run delegate rejected".
func TestIdentityForbiddenKeepsClassAndCarriesIdentitysCode(t *testing.T) {
	// The shape every quota branch produces (delegate.go:323-347).
	token, err := delegateAgainst(t, &delegateStub{t: t, status: http.StatusForbidden,
		body: contractsBody(t, contracts.ErrForbidden, "identity: grant may only narrow the parent ceiling")})
	if err == nil {
		t.Fatal("a 403 must fail closed")
	}
	if token != "" {
		t.Fatalf("a refused delegate must hand back no token, got %q", token)
	}
	if code := contracts.Code(err); code != contracts.ErrForbidden {
		t.Fatalf("a 403 must stay FORBIDDEN (auth is not widened here), got %q", code)
	}
	if msg := err.Error(); !strings.Contains(msg, "may only narrow the parent ceiling") {
		t.Fatalf("Identity's stated reason must reach the caller, got: %v", msg)
	}
	if reason := contracts.ReasonOf(err); !strings.Contains(reason, "forbidden") || !strings.Contains(reason, "http-403") {
		t.Fatalf("the refusal must carry Identity's code and status as a stable reason: %v", reason)
	}
	if strings.Contains(err.Error(), stubParentToken) {
		t.Fatalf("a refusal must never echo the bearer token: %v", err)
	}
}

// A stated conflict remains a refusal, but callers must receive its CONFLICT class rather than
// mistake a retryable database abort for an entitlement denial.
func TestIdentityConflictStaysVisibleInsideTheRefusal(t *testing.T) {
	token, err := delegateAgainst(t, &delegateStub{t: t, status: http.StatusConflict,
		body: contractsBody(t, contracts.ErrConflict, "identity: max active child grants reached")})
	if err == nil {
		t.Fatal("a 409 must fail closed")
	}
	if token != "" {
		t.Fatalf("a refused delegate must hand back no token, got %q", token)
	}
	if code := contracts.Code(err); code != contracts.ErrConflict {
		t.Fatalf("Identity 409/CONFLICT must stay CONFLICT, got %q", code)
	}
	if reason := contracts.ReasonOf(err); !strings.Contains(reason, "conflict") {
		t.Fatalf("Identity's CONFLICT code must be preserved in the reason: %v", reason)
	}
	if !strings.Contains(err.Error(), "max active child grants reached") {
		t.Fatalf("Identity's message must be carried: %v", err)
	}
}

// A transport-level failure is not an entitlement denial either: it is the dependency not answering.
func TestDelegateTransportFailureIsUnavailableNotForbidden(t *testing.T) {
	stub := &delegateStub{t: t, status: http.StatusCreated, body: `{"grant":{"task":"run-1-child-task"},"token":"child-token"}`}
	srv := stub.server()
	url := srv.URL
	srv.Close() // nothing listens any more: the dependency is gone
	issuer, err := NewIdentityDelegateIssuer(url, &http.Client{})
	if err != nil {
		t.Fatalf("issuer: %v", err)
	}
	token, err := issuer.Delegate(context.Background(), stubParentToken, "parent-attenuated-task", stubSpec())
	if err == nil || token != "" {
		t.Fatalf("a dead Identity must fail closed, got token=%q err=%v", token, err)
	}
	if code := contracts.Code(err); code != contracts.ErrUnavailable {
		t.Fatalf("an unreachable Identity must be UNAVAILABLE, not %q (%v)", code, err)
	}
}

// Leg (c): positive control. 201 behaves exactly as it did before this change.
func TestDelegateCreatedStillReturnsTheChildToken(t *testing.T) {
	stub := &delegateStub{t: t, status: http.StatusCreated,
		body: `{"grant":{"task":"run-1-child-task"},"token":"child-task-token"}`}
	token, err := delegateAgainst(t, stub)
	if err != nil {
		t.Fatalf("201 must succeed: %v", err)
	}
	if token != "child-task-token" {
		t.Fatalf("the minted child token must be returned, got %q", token)
	}
	if stub.calls != 1 || stub.sawPath != "/v1/identity/roots/parent-attenuated-task/delegate" {
		t.Fatalf("the call shape must not change: path=%q calls=%d", stub.sawPath, stub.calls)
	}
	if stub.sawAuth != "Bearer "+stubParentToken {
		t.Fatal("the parent task token must still be presented as bearer")
	}
	var sent delegateBody
	if err := json.Unmarshal([]byte(stub.sawBody), &sent); err != nil {
		t.Fatalf("request body must stay the same contract: %v (%s)", err, stub.sawBody)
	}
	if sent.Task != "run-1-child-task" || sent.Context != "ctx-1" || sent.TTLOut == "" {
		t.Fatalf("the delegate request must be untouched: %+v", sent)
	}
}

// Fail-closed properties are unchanged: a bare 200 without the atomic token is still refused (the
// frozen contract forbids a parent-bearer mint-of-child fallback), and a child task that diverges
// from the server-owned id is still refused.
func TestDelegateStillRefusesNonCreatedAndForeignChild(t *testing.T) {
	token, err := delegateAgainst(t, &delegateStub{t: t, status: http.StatusOK,
		body: `{"grant":{"task":"run-1-child-task"}}`})
	if err == nil || token != "" {
		t.Fatalf("a bare 200 Grant must stay refused, token=%q err=%v", token, err)
	}
	if code := contracts.Code(err); code != contracts.ErrForbidden {
		t.Fatalf("a non-atomic 200 is a contract violation, not an outage: %q (%v)", code, err)
	}
	if reason := contracts.ReasonOf(err); !strings.Contains(reason, "http-200") {
		t.Fatalf("the refusal should name what it saw: %v", reason)
	}
	token, err = delegateAgainst(t, &delegateStub{t: t, status: http.StatusCreated,
		body: `{"grant":{"task":"some-foreign-task"},"token":"child-task-token"}`})
	if err == nil || token != "" {
		t.Fatalf("a foreign child task must stay refused, token=%q err=%v", token, err)
	}
	if code := contracts.Code(err); code != contracts.ErrForbidden {
		t.Fatalf("the child-task check must stay FORBIDDEN: %q", code)
	}
}

// refusalProseBoundRunes bounds what a carried refusal may cost a reply: the clip keeps 240 runes
// and adds a three-rune ellipsis. Neither a gateway page nor a verbose upstream may exceed it.
const refusalProseBoundRunes = 243

// Leg (d): the class has to survive the WIRE, not only the error value. An assertion on the returned
// error cannot catch a render regression, and this repo has the precedent - ProfileClient.Pin answered
// 503 as 500 because the renderer did not know the class. So one leg pushes the delegate refusal
// through transport.WriteError and reads the response back.
func TestDelegateRefusalRendersAs503WithReasonOnTheWire(t *testing.T) {
	_, err := delegateAgainst(t, &delegateStub{t: t,
		status: http.StatusInternalServerError,
		body:   contractsBody(t, "INTERNAL", "internal server error")})
	if err == nil {
		t.Fatal("a 500 from Identity must fail closed")
	}
	rec := httptest.NewRecorder()
	transport.WriteError(rec, httptest.NewRequest(http.MethodPost, "/v1/space/threads/turns/accept", nil), err)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("an Identity outage must render 503 on this seam, got %d (%s)", rec.Code, rec.Body.String())
	}
	var wire struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Reason  string `json:"reason"`
	}
	if decErr := json.Unmarshal(rec.Body.Bytes(), &wire); decErr != nil {
		t.Fatalf("the refusal must be the contracts envelope: %v (%s)", decErr, rec.Body.String())
	}
	if wire.Code != contracts.ErrUnavailable {
		t.Fatalf("the wire must say UNAVAILABLE, got %q", wire.Code)
	}
	if !strings.Contains(wire.Reason, "identity:delegate:http-500") {
		t.Fatalf("the wire must carry Identity's stated cause as a reason, got %q", wire.Reason)
	}
	if strings.Contains(rec.Body.String(), stubParentToken) {
		t.Fatal("the bearer must not reach the wire")
	}
}

// The refusal text is carried into Space's own reply, so a gateway page must not be pasted into it: a
// proxy in front of Identity answers 5xx with HTML naming its upstream, which can repeat request data.
// When the upstream DID state prose through a contracts envelope the prose is carried, so there the
// bound is what has to hold.
func TestDelegateRefusalCarriesNoUpstreamMarkupAndStaysBounded(t *testing.T) {
	page := "<html><body><h1>502 Bad Gateway</h1><p>while connecting to upstream " +
		strings.Repeat("proxy-prose-", 400) + " secret-marker-9b1c</p></body></html>"
	_, err := delegateAgainst(t, &delegateStub{t: t, status: http.StatusBadGateway, body: page})
	if err == nil {
		t.Fatal("a gateway 502 must fail closed")
	}
	for _, marker := range []string{"secret-marker-9b1c", "<html>", "proxy-prose-"} {
		if strings.Contains(err.Error(), marker) {
			t.Fatalf("upstream markup must not be pasted into a contract reply (matched %q): %v", marker, err)
		}
	}
	if got := len([]rune(err.Error())); got > refusalProseBoundRunes {
		t.Fatalf("an unparsable upstream page must not be carried at all, got %d runes", got)
	}

	verbose := contractsBody(t, contracts.ErrUnavailable, strings.Repeat("identity retry window exhausted ", 600))
	_, verr := delegateAgainst(t, &delegateStub{t: t, status: http.StatusServiceUnavailable, body: verbose})
	if verr == nil {
		t.Fatal("a 503 from Identity must fail closed")
	}
	if code := contracts.Code(verr); code != contracts.ErrUnavailable {
		t.Fatalf("verbose prose must not change the class, got %q", code)
	}
	if got := len([]rune(verr.Error())); got > refusalProseBoundRunes {
		t.Fatalf("verbose upstream prose must be clipped before it is carried, got %d runes", got)
	}
}

// R1 (RED at this commit): the clip is allowed to shorten an upstream sentence, not to tear one.
// Byte-wise cutting leaves half of a multi-byte character behind, which a reader then sees as
// replacement glyphs once the reply is rendered as JSON.
func TestRefusalClipDoesNotTearMultiByteText(t *testing.T) {
	// Byte 240 of this input is the FIRST byte of a two-byte rune, so a byte cut at 240 is torn.
	torn := truncateOneLine(strings.Repeat("a", 239)+strings.Repeat("ё", 10), 240)
	if !utf8.ValidString(torn) {
		t.Fatalf("truncateOneLine must cut on a rune boundary, got invalid UTF-8: %q", torn)
	}
	if got := len([]rune(torn)); got != 243 {
		t.Fatalf("the clip must keep 240 runes plus the ellipsis, got %d runes", got)
	}
	if !strings.HasSuffix(torn, "...") {
		t.Fatalf("a clipped sentence must say it was clipped: %q", torn)
	}
	// The same thing through the mapping, for every alignment the prose can arrive at.
	for _, pad := range []string{"", "x", "xx"} {
		verbose := contractsBody(t, contracts.ErrUnavailable, pad+strings.Repeat("ё", 400))
		_, err := delegateAgainst(t, &delegateStub{t: t, status: http.StatusServiceUnavailable, body: verbose})
		if err == nil {
			t.Fatal("a 503 from Identity must fail closed")
		}
		if !utf8.ValidString(err.Error()) {
			t.Fatalf("padding %d: a carried refusal must stay valid UTF-8, got %q", len(pad), err)
		}
	}
}

// R2 (RED at this commit): the retry hint is the actionable half of an outage answer. Clipping the
// assembled message means a verbose upstream silently deletes it, while a human still has to be able
// to read it - the machine reason keeps ":retry-after" either way.
func TestRetryHintSurvivesAVerboseUpstreamMessage(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "7")
	verbose := contractsBody(t, contracts.ErrUnavailable, strings.Repeat("отказ ", 600))
	_, err := delegateAgainst(t, &delegateStub{t: t,
		status: http.StatusServiceUnavailable, body: verbose, header: h})
	if err == nil {
		t.Fatal("an outage must fail closed")
	}
	if !strings.Contains(err.Error(), "retry after 7") {
		t.Fatalf("a retry hint must not be the part a length bound cuts away: %v", err)
	}
	if got := len([]rune(err.Error())); got > refusalProseBoundRunes {
		t.Fatalf("keeping the hint must not lift the reply bound, got %d runes", got)
	}
}

// The refusal happens BEFORE State creates a run row (coordinator.go:494 returns ahead of CreateRun), so
// there is no durable carrier for its cause: a later reader of the stand finds a missing run and no
// reason, the same shape as the "terminal failed without a cause" class root keeps asking about. This
// leg pins a server-side record - and pins what it must NOT contain.
func TestDelegateRefusalIsRecordedServerSideWithoutTheCredential(t *testing.T) {
	logs := captureSpaceLogs(t)
	c := &Coordinator{runGrants: delegateIssuer(t, &delegateStub{t: t,
		status: http.StatusInternalServerError,
		body:   contractsBody(t, "INTERNAL", "internal server error")})}
	token, err := c.childGrant(context.Background(), stubParentToken, "parent-authority-task",
		"rtask-subject-1:key-1", "context-1", "subject-1")
	if err == nil || token != "" {
		t.Fatalf("a refused delegate must fail closed, token=%q err=%v", token, err)
	}
	line := logs.String()
	if !strings.Contains(line, "run grant refused") {
		t.Fatalf("the refusal must leave a server-side record - the run row does not exist yet, so the log is the only place its cause can be found later; got: %q", line)
	}
	for _, want := range []string{"contract_code=UNAVAILABLE", "identity:delegate:http-500", "context-1"} {
		if !strings.Contains(line, want) {
			t.Fatalf("the record must carry %q, got: %s", want, line)
		}
	}
	for _, forbidden := range []string{stubParentToken, "rtask-subject-1:key-1", "subject-1"} {
		if strings.Contains(line, forbidden) {
			t.Fatalf("the record must never carry %q: %s", forbidden, line)
		}
	}
}
