package space

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The mislabel that rungrant_delegate_refusal_test.go pins on the Delegate seam is not unique to it. The
// same client hides a dependency's answer on three more seams:
//
//   - Cancel: a transport failure answers FORBIDDEN ("run cancel transport failed"), and an Identity 5xx
//     answers FORBIDDEN with the status folded into prose.
//   - ChildToken: a transport failure returns an UNTYPED error, so pkg/transport renders it as
//     INTERNAL/500 - the very anti-pattern fixed for the Registry (354d27f7) and for Delegate.
//   - CancelChild: the same shape as ChildToken.
//
// Each outage leg below is expected RED at this commit: it asserts the class a dependency's silence or
// 5xx deserves. Each refusal leg is also RED: it asserts Identity's own code reaches the caller instead
// of an anonymous "rejected". The success leg is expected GREEN - it is the control that keeps the other
// legs from being greened by a fixture that never talks to anything.
//
// Nothing here widens authorization: every non-success status still fails closed with no token, and the
// three call sites in coordinator.go (1676, 1715, 1722) return these errors unchanged.

func deadIssuer(t *testing.T) *IdentityDelegateIssuer {
	t.Helper()
	stub := &delegateStub{t: t, status: http.StatusNoContent}
	srv := stub.server()
	url := srv.URL
	srv.Close() // the dependency is gone; nothing answers
	issuer, err := NewIdentityDelegateIssuer(url, &http.Client{})
	if err != nil {
		t.Fatalf("issuer: %v", err)
	}
	return issuer
}

func TestRunCancelTransportFailureIsUnavailableNotForbidden(t *testing.T) {
	err := deadIssuer(t).Cancel(context.Background(), "child-token", "rtask-subject:key")
	if err == nil {
		t.Fatal("a cancel that could not reach Identity must not report success")
	}
	if code := contracts.Code(err); code != contracts.ErrUnavailable {
		t.Fatalf("Identity not answering is UNAVAILABLE, not %q (%v) - an outage is not an entitlement statement", code, err)
	}
	if strings.Contains(err.Error(), "child-token") {
		t.Fatalf("a refusal must never echo the bearer token: %v", err)
	}
}

func TestRunCancelReportsAnIdentityOutageAsOutage(t *testing.T) {
	stub := &delegateStub{t: t, status: http.StatusBadGateway,
		body: contractsBody(t, "INTERNAL", "identity journal writer is not reachable")}
	err := delegateIssuer(t, stub).Cancel(context.Background(), "child-token", "rtask-subject:key")
	if err == nil {
		t.Fatal("a gateway failure must not report a revoked grant")
	}
	if code := contracts.Code(err); code != contracts.ErrUnavailable {
		t.Fatalf("an Identity 5xx during cancel must be UNAVAILABLE, got %q (%v)", code, err)
	}
	if reason := contracts.ReasonOf(err); !strings.Contains(reason, "identity:cancel:http-502") {
		t.Fatalf("the reason must name the seam and the status it saw: %v", reason)
	}
}

func TestRunCancelKeepsIdentitysCodeOnARefusal(t *testing.T) {
	stub := &delegateStub{t: t, status: http.StatusForbidden,
		body: contractsBody(t, contracts.ErrForbidden, "identity: bearer grant is already revoked")}
	err := delegateIssuer(t, stub).Cancel(context.Background(), "child-token", "rtask-subject:key")
	if err == nil {
		t.Fatal("a 403 must fail closed")
	}
	if code := contracts.Code(err); code != contracts.ErrForbidden {
		t.Fatalf("a 403 stays FORBIDDEN, got %q", code)
	}
	if reason := contracts.ReasonOf(err); !strings.Contains(reason, "identity:cancel:http-403:forbidden") {
		t.Fatalf("Identity's code must reach the caller as a reason, got %q", reason)
	}
	if !strings.Contains(err.Error(), "already revoked") {
		t.Fatalf("Identity's sentence must be carried, got: %v", err)
	}
}

func TestDescendantMintTransportFailureIsNotInternal(t *testing.T) {
	token, err := deadIssuer(t).ChildToken(context.Background(), "parent-token", "rtask-subject:key")
	if err == nil || token != "" {
		t.Fatalf("a mint that could not reach Identity must fail closed, token=%q err=%v", token, err)
	}
	if code := contracts.Code(err); code != contracts.ErrUnavailable {
		t.Fatalf("an untyped transport error renders as INTERNAL/500 and hides the outage; got code %q (%v)", code, err)
	}
}

func TestDescendantMintReportsAnIdentityOutageAsOutage(t *testing.T) {
	stub := &delegateStub{t: t, status: http.StatusInternalServerError,
		body: contractsBody(t, "INTERNAL", "internal server error")}
	_, err := delegateIssuer(t, stub).ChildToken(context.Background(), "parent-token", "rtask-subject:key")
	if err == nil {
		t.Fatal("an Identity 500 must not mint a credential")
	}
	if code := contracts.Code(err); code != contracts.ErrUnavailable {
		t.Fatalf("Identity's 500 must be UNAVAILABLE, got %q (%v)", code, err)
	}
	if reason := contracts.ReasonOf(err); !strings.Contains(reason, "identity:mint:http-500") {
		t.Fatalf("the reason must name the seam and the status: %v", reason)
	}
}

func TestDescendantMintKeepsIdentitysCodeOnARefusal(t *testing.T) {
	stub := &delegateStub{t: t, status: http.StatusForbidden,
		body: contractsBody(t, contracts.ErrForbidden, "identity: grant is not an ancestor of the requested task")}
	token, err := delegateIssuer(t, stub).ChildToken(context.Background(), "parent-token", "rtask-subject:key")
	if err == nil || token != "" {
		t.Fatalf("a refused mint must yield no token, token=%q err=%v", token, err)
	}
	if code := contracts.Code(err); code != contracts.ErrForbidden {
		t.Fatalf("a 403 stays FORBIDDEN, got %q", code)
	}
	if reason := contracts.ReasonOf(err); !strings.Contains(reason, "identity:mint:http-403:forbidden") {
		t.Fatalf("Identity's code must reach the caller as a reason, got %q", reason)
	}
}

func TestDescendantCancelDistinguishesOutageFromRefusal(t *testing.T) {
	ctx := context.Background()
	if err := deadIssuer(t).CancelChild(ctx, "parent-token", "parent-task", "rtask-subject:key"); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("an unacknowledged cancel must be UNAVAILABLE, got %q (%v)", contracts.Code(err), err)
	}
	outage := &delegateStub{t: t, status: http.StatusServiceUnavailable,
		body: contractsBody(t, contracts.ErrUnavailable, "identity: journal unavailable")}
	if err := delegateIssuer(t, outage).CancelChild(ctx, "parent-token", "parent-task", "rtask-subject:key"); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("an Identity 503 during cancel must be UNAVAILABLE, got %q (%v)", contracts.Code(err), err)
	}
	refusal := &delegateStub{t: t, status: http.StatusForbidden,
		body: contractsBody(t, contracts.ErrForbidden, "identity: caller is not the grant parent")}
	err := delegateIssuer(t, refusal).CancelChild(ctx, "parent-token", "parent-task", "rtask-subject:key")
	if code := contracts.Code(err); code != contracts.ErrForbidden {
		t.Fatalf("a 403 stays FORBIDDEN, got %q (%v)", code, err)
	}
	if reason := contracts.ReasonOf(err); !strings.Contains(reason, "identity:child-cancel:http-403:forbidden") {
		t.Fatalf("Identity's code must reach the caller as a reason, got %q", reason)
	}
}

// Success legs (GREEN today, kept as the control so the outage legs cannot be greened by a fixture that
// never reaches anything): 204 revokes, 200 mints, and a credential never travels in a URL - on either
// seam it is the bearer only. The client's documented "any 2xx on cancel means accepted" is left exactly
// as it is: tightening a success contract is not what this lane is for.
func TestCancelAndMintStillHonourTheirSuccessCodes(t *testing.T) {
	ctx := context.Background()
	ok204 := &delegateStub{t: t, status: http.StatusNoContent}
	if err := delegateIssuer(t, ok204).Cancel(ctx, "child-token", "rtask-subject:key"); err != nil {
		t.Fatalf("204 is a revocation, got %v", err)
	}
	ok200 := &delegateStub{t: t, status: http.StatusOK, body: `{"token":"child-task-token"}`}
	tok, err := delegateIssuer(t, ok200).ChildToken(ctx, "parent-token", "rtask-subject:key")
	if err != nil || tok != "child-task-token" {
		t.Fatalf("200 must hand back the minted credential, token=%q err=%v", tok, err)
	}
	for _, saw := range []struct{ name, path string }{{"cancel", ok204.sawPath}, {"mint", ok200.sawPath}} {
		if strings.Contains(saw.path, "child-token") || strings.Contains(saw.path, "parent-token") {
			t.Fatalf("%s: a credential must never travel in a path: %q", saw.name, saw.path)
		}
	}
}
