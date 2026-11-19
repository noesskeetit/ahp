package space_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// authPerSubject mimics the real auth middleware a Space deployment mounts: it
// derives a *verified* principal from the presented credential, here keyed by
// the X-Test-Subject header so a single Space instance can be driven as two
// different browser subjects. A request with no subject is rejected with no
// principal (the handler then answers 403, never leaking any turn).
func authPerSubject(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := r.Header.Get("X-Test-Subject")
		if s == "" {
			next.ServeHTTP(w, r) // no principal -> handler returns 403
			return
		}
		ctx := transport.WithPrincipal(r.Context(), transport.Principal{
			Scope: contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: s},
		})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// postSpaceAs issues a POST /v1/space/{op} as the given browser subject.
func postSpaceAs(t *testing.T, base, op string, body any, subject string) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal %s: %v", op, err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(http.MethodPost, base+"/v1/space/"+op, rdr)
	if err != nil {
		t.Fatalf("newreq %s: %v", op, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer tok-"+subject)
	if subject != "" {
		req.Header.Set("X-Test-Subject", subject)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post %s as %s: %v", op, subject, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

// runTurnAcceptDispatchStream drives one subject through the full browser path:
// AcceptTurn -> DispatchTurn -> poll ReadStream to a terminal status. It returns
// the (turnID, threadID) pair the subject received.
func runTurnAcceptDispatchStream(t *testing.T, base, subject, content string) (string, string) {
	t.Helper()
	accBody := map[string]any{
		"key":        "browser-" + subject + "-" + content,
		"context_id": "ctx-" + subject,
		"input":      map[string]any{"role": "user", "content": content},
		"max_steps":  3,
	}
	code, body := postSpaceAs(t, base, "AcceptTurn", accBody, subject)
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn(%s) = %d, want 200 (body=%s)", subject, code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	threadID, _ := acc["thread_id"].(string)
	if turnID == "" || threadID == "" {
		t.Fatalf("AcceptTurn(%s) missing ids: %s", subject, body)
	}
	if c, b := postSpaceAs(t, base, "DispatchTurn", nil, subject); c != http.StatusOK {
		t.Fatalf("DispatchTurn(%s) = %d, want 200 (body=%s)", subject, c, b)
	}
	// Pump until the turn reaches a terminal status (max ~3s).
	deadline := time.Now().Add(3 * time.Second)
	var status string
	for time.Now().Before(deadline) {
		c, b := postSpaceAs(t, base, "GetTurn", map[string]any{"turn_id": turnID}, subject)
		if c != http.StatusOK {
			t.Fatalf("GetTurn(%s) poll = %d (body=%s)", subject, c, b)
		}
		status, _ = decodeMap(t, b)["status"].(string)
		if status == string(space.TurnDone) || status == string(space.TurnFailed) || status == string(space.TurnCanceled) {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if status != string(space.TurnDone) {
		t.Fatalf("turn(%s) did not complete via browser path, status=%s", subject, status)
	}
	return turnID, threadID
}

// TestSpaceTwoSubjectBrowserFlow proves the user-facing two-subject browser flow
// against the real Space HTTP handler: two distinct subjects, each with their own
// verified principal, run independent turns, perform a HITL round-trip, and can
// never read or mutate one another's turns/threads (private-context isolation).
func TestSpaceTwoSubjectBrowserFlow(t *testing.T) {
	srv := newHTTPSpace(t, &stubCore{}, authPerSubject)
	base := srv.URL

	// Subject A: full chat path to done.
	aTurn, aThread := runTurnAcceptDispatchStream(t, base, "alice", "hello from A")
	// Subject B: full chat path to done, independent ids.
	bTurn, bThread := runTurnAcceptDispatchStream(t, base, "bob", "hello from B")

	if aTurn == bTurn || aThread == bThread {
		t.Fatalf("subjects must not share ids: A=(%s,%s) B=(%s,%s)", aTurn, aThread, bTurn, bThread)
	}

	// A's ReadStream returns at least one event (its own activity is visible to A).
	if c, b := postSpaceAs(t, base, "ReadStream", map[string]any{"thread_id": aThread, "after": 0, "limit": 100}, "alice"); c != http.StatusOK {
		t.Fatalf("ReadStream(A as alice) = %d, want 200 (body=%s)", c, b)
	} else if evs, _ := decodeMap(t, b)["events"].([]any); len(evs) == 0 {
		t.Fatalf("ReadStream(A as alice) returned no events: %s", b)
	}

	// PRIVATE CONTEXT: B reading A's thread must be a 404 (not-found), never A's data.
	if c, b := postSpaceAs(t, base, "ReadStream", map[string]any{"thread_id": aThread, "after": 0, "limit": 100}, "bob"); c != http.StatusNotFound {
		t.Fatalf("ReadStream(A as bob) = %d, want 404 (leak!) body=%s", c, b)
	}
	// And A reading B's thread is likewise blocked.
	if c, _ := postSpaceAs(t, base, "ReadStream", map[string]any{"thread_id": bThread, "after": 0, "limit": 100}, "alice"); c != http.StatusNotFound {
		t.Fatalf("ReadStream(B as alice) = %d, want 404 (leak!)", c)
	}

	// GetTurn cross-scope must be 404 (no existence leak).
	if c, b := postSpaceAs(t, base, "GetTurn", map[string]any{"turn_id": aTurn}, "bob"); c != http.StatusNotFound {
		t.Fatalf("GetTurn(A as bob) = %d, want 404 (leak!) body=%s", c, b)
	}
	// Same-subject GetTurn succeeds.
	if c, _ := postSpaceAs(t, base, "GetTurn", map[string]any{"turn_id": aTurn}, "alice"); c != http.StatusOK {
		t.Fatalf("GetTurn(A as alice) = %d, want 200", c)
	}

	// ListTurns must be subject-scoped: A sees exactly its turn, not B's.
	c, b := postSpaceAs(t, base, "ListTurns", map[string]any{}, "alice")
	if c != http.StatusOK {
		t.Fatalf("ListTurns(alice) = %d, want 200 (body=%s)", c, b)
	}
	la := decodeMap(t, b)
	if !listContainsTurn(la, aTurn) {
		t.Fatalf("ListTurns(alice) missing own turn %s: %s", aTurn, b)
	}
	if listContainsTurn(la, bTurn) {
		t.Fatalf("ListTurns(alice) LEAKED bob's turn %s: %s", bTurn, b)
	}
	// Symmetric for bob.
	c, b = postSpaceAs(t, base, "ListTurns", map[string]any{}, "bob")
	if c != http.StatusOK {
		t.Fatalf("ListTurns(bob) = %d, want 200 (body=%s)", c, b)
	}
	lb := decodeMap(t, b)
	if !listContainsTurn(lb, bTurn) {
		t.Fatalf("ListTurns(bob) missing own turn %s: %s", bTurn, b)
	}
	if listContainsTurn(lb, aTurn) {
		t.Fatalf("ListTurns(bob) LEAKED alice's turn %s: %s", aTurn, b)
	}

	// HITL round-trip within one subject: submit under A, respond under A.
	c, b = postSpaceAs(t, base, "SubmitHITL", map[string]any{
		"turn_id": aTurn, "kind": "approval", "prompt": "approve?",
	}, "alice")
	if c != http.StatusOK {
		t.Fatalf("SubmitHITL(A as alice) = %d, want 200 (body=%s)", c, b)
	}
	hitlID, _ := decodeMap(t, b)["id"].(string)
	if hitlID == "" {
		t.Fatalf("SubmitHITL(A) missing id: %s", b)
	}
	if c, b = postSpaceAs(t, base, "RespondHITL", map[string]any{
		"hitl_id": hitlID, "approved": true, "thread_id": aThread,
	}, "alice"); c != http.StatusOK {
		t.Fatalf("RespondHITL(A as alice) = %d, want 200 (body=%s)", c, b)
	}
	// Cross-subject mutation must be blocked at the boundary (404), regardless of
	// knowing the ids: bob cannot steer, cancel, or respond on alice's turn/thread.
	if c, b := postSpaceAs(t, base, "CancelTurn", map[string]any{"turn_id": aTurn}, "bob"); c != http.StatusNotFound {
		t.Fatalf("CancelTurn(A as bob) = %d, want 404 (cross-subject mutation!) body=%s", c, b)
	}
	if c, b := postSpaceAs(t, base, "Steer", map[string]any{"turn_id": aTurn, "instruction": "leak"}, "bob"); c != http.StatusNotFound {
		t.Fatalf("Steer(A as bob) = %d, want 404 (cross-subject mutation!) body=%s", c, b)
	}
	if c, b := postSpaceAs(t, base, "RespondHITL", map[string]any{
		"hitl_id": hitlID, "approved": true, "thread_id": aThread,
	}, "bob"); c != http.StatusNotFound {
		t.Fatalf("RespondHITL(A as bob) = %d, want 404 (cross-subject mutation!) body=%s", c, b)
	}
}

// TestSpaceAnonymousRejected proves no principal means no data: anonymous
// GetTurn/ListTurns/ReadStream are refused (403), so an unauthenticated browser
// cannot enumerate or read any subject's activity.
func TestSpaceAnonymousRejected(t *testing.T) {
	srv := newHTTPSpace(t, &stubCore{}, authPerSubject)
	base := srv.URL
	// Create a turn as alice so there is real state to (fail to) read.
	aTurn, aThread := runTurnAcceptDispatchStream(t, base, "alice", "secret topic")
	_ = aTurn
	for _, tc := range []struct {
		op   string
		body map[string]any
	}{
		{"ListTurns", map[string]any{}},
		{"GetTurn", map[string]any{"turn_id": aTurn}},
		{"ReadStream", map[string]any{"thread_id": aThread, "after": 0, "limit": 10}},
	} {
		c, b := postSpaceAs(t, base, tc.op, tc.body, "")
		if c != http.StatusForbidden {
			t.Fatalf("anonymous %s = %d, want 403 (body=%s)", tc.op, c, b)
		}
	}
}
