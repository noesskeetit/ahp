package emil

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// fakeBudget stands in for the host's budget tree (internal/treedelegate). It is
// deliberately not the real ledger: what is under test here is the ADAPTER's
// behaviour at the delegation seam -- whose number goes on the wire, and what the
// adapter does when the tree says no -- with the ledger's own behaviour covered
// against real Postgres in internal/treedelegate.

type fakeBudget struct {
	mu       sync.Mutex
	granted  int
	refuse   error
	reserves []struct {
		ref       string
		requested int
	}
	settled []DelegationOutcome
}

func (f *fakeBudget) Reserve(_ context.Context, ref string, requested int) (DelegationGrant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// The ask is recorded whether or not the tree grants it: "one refusal is one
	// ask" is exactly the thing worth asserting, and a fake that only remembered
	// its successes could not say it.
	f.reserves = append(f.reserves, struct {
		ref       string
		requested int
	}{ref, requested})
	if f.refuse != nil {
		return DelegationGrant{}, f.refuse
	}
	return DelegationGrant{Ref: ref, Granted: f.granted, Handle: "handle-" + ref}, nil
}

func (f *fakeBudget) Settle(_ context.Context, grant DelegationGrant, out DelegationOutcome) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if grant.Handle != "handle-"+grant.Ref {
		return errors.New("fakeBudget: the adapter mangled the budget handle it was given")
	}
	f.settled = append(f.settled, out)
	return nil
}

func (f *fakeBudget) snapshot() ([]DelegationOutcome, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]DelegationOutcome(nil), f.settled...), len(f.reserves)
}

// budgetChild is a canonical child face that journals the SendMessage it received
// and completes with its OWN consumption report, the way the AHP execution plane
// does (internal/gateway/budget.go metadata steps_consumed).
func budgetChild(t *testing.T, captured map[string]any, guard *sync.Mutex, terminal string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			guard.Lock()
			for k, v := range req.Params {
				captured[k] = v
			}
			guard.Unlock()
			if terminal == "send-fails" {
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"error":{"code":-32603,"message":"child unavailable"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"task":{"id":"child-task-77","contextId":"c1","status":{"state":"TASK_STATE_SUBMITTED"}}}}`))
		default:
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + itoa(req.ID) + `,"result":{"id":"child-task-77","contextId":"c1","metadata":{"steps_consumed":1,"close_reason":"model_finished"},"status":{"state":"TASK_STATE_COMPLETED","message":{"role":"ROLE_AGENT","parts":[{"text":"done"}]}}}}`))
		}
	}))
	return srv
}

func runAuthority(budget DelegationBudget, bound int) func(context.Context) *RunAuthority {
	return func(context.Context) *RunAuthority {
		return &RunAuthority{
			Bearer:       "run-own-task-token",
			Scope:        contracts.Scope{Tenant: "a2a", Deployment: "a2a-parent", Subject: "alice"},
			TaskID:       "run-task-1",
			OperationKey: "core:0",
			ContextID:    "thread-1",
			Labels:       map[string]any{MetaParentTask: "run-task-1"},
			MaxSteps:     bound,
			Budget:       budget,
		}
	}
}

// TestDelegatedBoundComesFromTheRemainderNotTheParent is the defect in one test: a
// run admitted with 3 steps that already spent one must not hand its child 3 more.
// The bound the child is admitted with is the tree's grant, and the tree's
// reference travels with the task so the two are joinable after the fact.
func TestDelegatedBoundComesFromTheRemainderNotTheParent(t *testing.T) {
	var mu sync.Mutex
	captured := map[string]any{}
	fb := &fakeBudget{granted: 2}
	srv := budgetChild(t, captured, &mu, "")
	defer srv.Close()

	delegateWith(t, Config{BaseURL: srv.URL, RunAuthorityFor: runAuthority(fb, 3)}, context.Background())

	mu.Lock()
	defer mu.Unlock()
	reqMD, msgMD := metadataOf(t, captured)
	for name, md := range map[string]map[string]any{"request": reqMD, "message": msgMD} {
		if got := md[MetaStepBudget]; got != float64(2) {
			t.Fatalf("%s metadata: child admitted with %v, want the tree's grant of 2 (the parent's 3 is already spent)", name, got)
		}
		if md[MetaParentTask] != "run-task-1" {
			t.Fatalf("%s metadata: the tree label must travel with the budget, got %v", name, md)
		}
		if md[MetaDelegationRef] == nil || md[MetaDelegationRef] == "" {
			t.Fatalf("%s metadata: the delegation reference is missing, so the child task cannot be joined to the budget node that paid for it", name)
		}
	}

	outcomes, reserves := fb.snapshot()
	if reserves != 1 {
		t.Fatalf("budget was asked to reserve %d times, want 1", reserves)
	}
	if len(outcomes) != 1 {
		t.Fatalf("the tree was converged %d times, want 1", len(outcomes))
	}
	got := outcomes[0]
	if !got.ReachedChild || got.ChildTaskID != "child-task-77" {
		t.Fatalf("convergence must name the child task that ran, got %+v", got)
	}
	if !got.Reported || got.StepsConsumed != 1 {
		t.Fatalf("convergence must carry the child's own report (steps_consumed=1), got %+v", got)
	}
}

// TestDelegationRefusedByTreeCreatesNoChild: "исчерпанный резерв — отказ". The
// refusal happens BEFORE the submission, so no child task is ever born holding a
// budget nobody granted.
func TestDelegationRefusedByTreeCreatesNoChild(t *testing.T) {
	var mu sync.Mutex
	captured := map[string]any{}
	fb := &fakeBudget{refuse: errors.New("no budget remainder left")}
	srv := budgetChild(t, captured, &mu, "")
	defer srv.Close()

	a, err := New(Config{BaseURL: srv.URL, RunAuthorityFor: runAuthority(fb, 3)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := a.Delegate(ctx, "do work"); err == nil {
		t.Fatal("a delegation refused by the budget tree reported success")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(captured) != 0 {
		t.Fatalf("a refused delegation still submitted a message to the child: %v", captured)
	}
	if _, reserves := fb.snapshot(); reserves != 1 {
		t.Fatalf("the tree was asked %d times, want one refusal to be one ask", reserves)
	}
}

// A submission that never landed spent nothing: the whole reserve goes back, or the
// run would be charged for a child that never existed.
func TestUnconfirmedDelegationKeepsTheReserve(t *testing.T) {
	var mu sync.Mutex
	captured := map[string]any{}
	fb := &fakeBudget{granted: 2}
	srv := budgetChild(t, captured, &mu, "send-fails")
	defer srv.Close()

	a, err := New(Config{BaseURL: srv.URL, RunAuthorityFor: runAuthority(fb, 3)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := a.Delegate(ctx, "do work"); err == nil {
		t.Fatal("a failed submission reported success")
	}

	outcomes, _ := fb.snapshot()
	if len(outcomes) != 0 {
		t.Fatalf("unconfirmed SendMessage settled %d reservations", len(outcomes))
	}

}

// The only fake here is a peer that drops the response after accepting bytes.
func TestLostSendAcknowledgementRetainsGrantAndStableMessageID(t *testing.T) {
	var idsMu sync.Mutex
	var ids []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == AgentCardPath {
			w.Write([]byte(canonicalCard(r)))
			return
		}
		var req struct {
			Params struct {
				Message struct {
					ID string `json:"messageId"`
				} `json:"message"`
			} `json:"params"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		idsMu.Lock()
		ids = append(ids, req.Params.Message.ID)
		idsMu.Unlock()
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close()
	}))
	defer srv.Close()
	b := &fakeBudget{granted: 2}
	for _, key := range []string{"run:node:0", "run:node:0", "run:node:1"} {
		a, err := New(Config{BaseURL: srv.URL, RunAuthorityFor: func(ctx context.Context) *RunAuthority {
			ra := runAuthority(b, 3)(ctx)
			ra.OperationKey = key
			return ra
		}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := a.Delegate(context.Background(), "work"); err == nil {
			t.Fatal("lost ACK reported success")
		}
	}
	outcomes, _ := b.snapshot()
	if len(outcomes) != 0 {
		t.Fatalf("unknown delivery refunded %d reservations", len(outcomes))
	}
	idsMu.Lock()
	got := append([]string(nil), ids...)
	idsMu.Unlock()
	if len(got) < 3 || got[0] != got[1] || got[0] == got[len(got)-1] {
		t.Fatalf("message ids not tied to durable operation: %v", got)
	}
}
