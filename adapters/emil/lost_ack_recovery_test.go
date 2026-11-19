package emil

// R3, the half that was never measured: the child task WAS created and the
// acknowledgement never arrived. The adapter must (a) reconcile by presenting the
// SAME logical message identity, so the platform's own deduplication resolves the
// retry to the child that already exists instead of a second one, (b) hold the
// budget reserve until the child's own report settles it -- never refund on an
// unknown outcome, and (c) be able to cancel the child it recovered, because a
// subtree cancel that cannot find the child leaves a foreign deployment running
// work the parent no longer owns.
//
// The only fiction in this file is the peer: a child that accepts SendMessage and
// then dies mid-response. Deduplication is not faked here -- the server answers a
// repeated messageId with the task it already created, which is exactly the rule
// internal/gateway applies to SendMessage; the assertion that matters is that the
// adapter PUTS THE SAME IDENTITY ON THE WIRE for the retry, which is what makes
// that rule apply. Budget behaviour is measured against fakeBudget here and against
// real PostgreSQL in internal/treedelegate.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"
)

type lostAckChild struct {
	mu          sync.Mutex
	created     int
	messageIDs  []string
	sends       []map[string]any // full SendMessage params, in arrival order
	byMessage   map[string]string
	cancels     map[string]int
	polls       int
	steps       int
	neverFinish bool
}

func newLostAckChild(steps int, neverFinish bool) *lostAckChild {
	return &lostAckChild{byMessage: map[string]string{}, cancels: map[string]int{}, steps: steps, neverFinish: neverFinish}
}

func (c *lostAckChild) serve(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == AgentCardPath {
			_, _ = w.Write([]byte(canonicalCard(r)))
			return
		}
		var req struct {
			ID     int64          `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "SendMessage":
			msg, _ := req.Params["message"].(map[string]any)
			id, _ := msg["messageId"].(string)
			c.mu.Lock()
			c.messageIDs = append(c.messageIDs, id)
			c.sends = append(c.sends, req.Params)
			existing, duplicate := c.byMessage[id]
			if !duplicate {
				c.created++
				existing = "child-recovered-1"
				c.byMessage[id] = existing
			}
			first := !duplicate
			c.mu.Unlock()
			if first {
				// The task exists on the child now. Kill the response, so the parent
				// cannot know that -- the exact production shape of a lost ACK.
				conn, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = conn.Close()
				}
				return
			}
			// The canonical v2 SendMessage answers inside a {"task": ...} envelope;
			// the reconcile answer must be indistinguishable from a normal one.
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"task":{"id":"` + existing + `","contextId":"c1","status":{"state":"TASK_STATE_WORKING"}}}}`))
		case "GetTask":
			taskID, _ := req.Params["id"].(string)
			c.mu.Lock()
			c.polls++
			never := c.neverFinish
			steps := c.steps
			c.mu.Unlock()
			if never {
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"id":"` + taskID + `","contextId":"c1","status":{"state":"TASK_STATE_WORKING"}}}`))
				return
			}
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"id":"` + taskID + `","contextId":"c1","metadata":{"steps_consumed":` + itoa(int64(steps)) + `,"close_reason":"model_finished"},"status":{"state":"TASK_STATE_COMPLETED","message":{"role":"ROLE_AGENT","parts":[{"text":"done"}]}}}}`))
		case "CancelTask":
			taskID, _ := req.Params["id"].(string)
			c.mu.Lock()
			c.cancels[taskID]++
			c.mu.Unlock()
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"id":"` + taskID + `","contextId":"c1","status":{"state":"TASK_STATE_CANCELED"}}}`))
		default:
			t.Errorf("unexpected method %q", req.Method)
		}
	}))
}

func (c *lostAckChild) snapshot() (created int, ids []string, sends []map[string]any, cancels map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cancels = make(map[string]int, len(c.cancels))
	for k, v := range c.cancels {
		cancels[k] = v
	}
	return c.created, append([]string(nil), c.messageIDs...), append([]map[string]any(nil), c.sends...), cancels
}

// The recovery leg: one child is born, the ACK is lost, the retry carries the same
// identity, the handle comes back, and the tree is converged exactly once with the
// child's own report -- not refunded into thin air.
func TestLostAckReconcilesToTheOneChildAndSettlesOnce(t *testing.T) {
	child := newLostAckChild(2, false)
	srv := child.serve(t)
	defer srv.Close()

	budget := &fakeBudget{granted: 2}
	a, err := New(Config{BaseURL: srv.URL, RunAuthorityFor: runAuthority(budget, 3)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, _, err := a.Delegate(ctx, "do work"); err != nil {
		t.Fatalf("a reconciled delegation reported failure: %v", err)
	}

	created, ids, sends, _ := child.snapshot()
	if created != 1 {
		t.Errorf("child tasks physically created: %d, want 1 -- the retry must not duplicate the effect", created)
	}
	if len(ids) < 2 {
		t.Fatalf("expected the original send plus a reconcile, saw %v", ids)
	}
	if ids[0] == "" || ids[0] != ids[1] {
		t.Errorf("reconcile presented message ids %v: the retry must carry the SAME logical identity, which is what makes the platform deduplicate it", ids)
	}
	// The identity is only half of a deduplication key: the request must otherwise
	// be the same request, or the platform is right to treat it as new work.
	if len(sends) >= 2 && !reflect.DeepEqual(sends[0], sends[1]) {
		t.Errorf("the retry is not the same request, so deduplication cannot apply:\nfirst:  %v\nsecond: %v", sends[0], sends[1])
	}

	outcomes, reserves := budget.snapshot()
	if reserves != 1 {
		t.Errorf("reserve asks: %d, want 1", reserves)
	}
	if len(outcomes) != 1 {
		t.Fatalf("tree converged %d times, want exactly 1 (no double spend, no double refund)", len(outcomes))
	}
	got := outcomes[0]
	if !got.ReachedChild || got.ChildTaskID != "child-recovered-1" {
		t.Errorf("the settled outcome must name the recovered child, got %+v", got)
	}
	if !got.Reported || got.StepsConsumed != 2 {
		t.Errorf("the settled outcome must carry the child's own report, got %+v", got)
	}
}

// The cancel leg: a delegation whose child was only recovered through reconcile is
// still cancellable. A cancel that finds nothing (CancelTask never sent) leaves the
// child running in another deployment while the parent's budget tree closes.
func TestSubtreeCancelReachesTheReconciledChild(t *testing.T) {
	child := newLostAckChild(0, true)
	srv := child.serve(t)
	defer srv.Close()

	budget := &fakeBudget{granted: 2}
	a, err := New(Config{BaseURL: srv.URL, RunAuthorityFor: runAuthority(budget, 3)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := a.Delegate(ctx, "do work")
		done <- err
	}()
	// Let the submission, the reconcile and at least one poll happen, then cancel
	// the parent's turn the way a subtree cancel does.
	time.Sleep(1500 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Error("a canceled turn reported success")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Delegate ignored the cancellation")
	}

	_, _, _, cancels := child.snapshot()
	if got := cancels["child-recovered-1"]; got != 1 {
		t.Errorf("CancelTask for the recovered child: %d calls, want exactly 1 (cancel_calls=0 while the child runs is the defect)", got)
	}
	outcomes, _ := budget.snapshot()
	if len(outcomes) != 1 {
		t.Fatalf("tree converged %d times, want 1", len(outcomes))
	}
	got := outcomes[0]
	if !got.ReachedChild || got.ChildTaskID != "child-recovered-1" {
		t.Errorf("canceled delegation must settle against the child it canceled, got %+v", got)
	}
	// The child never reported its consumption, so the tree must NOT be told the
	// steps are free: Reported=false is what keeps the grant charged in
	// internal/treedelegate instead of refunding work that may have happened.
	if got.Reported {
		t.Errorf("a child that never reported consumption must not produce a report: %+v", got)
	}
}
