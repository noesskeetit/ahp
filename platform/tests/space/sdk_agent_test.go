package space_test

// sdk_agent_test.go — exercises the SHIPPED Go SDK (sdk/go/agent) against the
// PRODUCTION Space HTTP handler (space.Handler + Coordinator + StateClient), proving
// acceptance #4 "two custom deployments via SDK + agent-to-agent delegation" at the
// Space API contract level: accept a turn on deployment A, run it, follow its stream,
// then DELEGATE a follow-up turn from A to deployment B and run that too — all through
// the actual agent.NewClient, not an inline duplicate.
//
// Honesty: like handler_http_test.go this uses the newHTTPSpace harness whose State
// PEER is the test double (production handler/coordinator/StateClient/SDK throughout).
// This is an SDK + Space-API CONTRACT proof, NOT live/real-State or real-LLM proof
// (real State is the *_realstate suites; the real LLM+tool+artifact journey is the
// live_real suite). Nothing here is presented as a live deployment proof.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/sdk/go/agent"
)

// sdkWaitDone polls GetTurn through the SDK until the turn reaches a terminal status.
func sdkWaitDone(t *testing.T, c *agent.Client, turnID string) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := c.GetTurn(context.Background(), turnID)
		if err != nil {
			t.Fatalf("SDK GetTurn during poll: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("SDK GetTurn parse: %v (raw=%s)", err, raw)
		}
		status, _ := m["status"].(string)
		switch status {
		case string(space.TurnDone), string(space.TurnFailed), string(space.TurnCanceled):
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("SDK turn %s did not reach a terminal status within deadline", turnID)
	return ""
}

func TestSDKTwoDeploymentsDelegate(t *testing.T) {
	srvA := newHTTPSpace(t, &stubCore{}, authWithPrincipal)
	srvB := newHTTPSpace(t, &stubCore{}, authWithPrincipal)
	ctx := context.Background()

	a := agent.NewClient(agent.Config{BaseURL: srvA.URL, Token: "tok-a"})
	b := agent.NewClient(agent.Config{BaseURL: srvB.URL, Token: "tok-b"})

	// 1. Accept + run a turn on deployment A through the SDK.
	outA, err := a.AcceptTurn(ctx, agent.TurnInput{
		Key: "sdk-a-" + contracts.NewID(), ContextID: "sdk-test",
		Input: json.RawMessage(`"hello a"`), MaxSteps: 2,
	})
	if err != nil {
		t.Fatalf("SDK AcceptTurn(A): %v", err)
	}
	if outA.TurnID == "" || outA.ThreadID == "" {
		t.Fatalf("SDK AcceptTurn(A) missing ids: %+v", outA)
	}
	if err := a.Dispatch(ctx); err != nil {
		t.Fatalf("SDK Dispatch(A): %v", err)
	}
	if st := sdkWaitDone(t, a, outA.TurnID); st != string(space.TurnDone) {
		t.Fatalf("deployment A turn not done via SDK: status=%s", st)
	}

	// 2. Follow A's stream through the SDK -> the OBJECT {"events":[...]} envelope.
	raw, err := a.ReadStream(ctx, outA.ThreadID, 0, 100)
	if err != nil {
		t.Fatalf("SDK ReadStream(A): %v", err)
	}
	var env struct {
		Events []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("SDK ReadStream parse: %v (raw=%s)", err, raw)
	}
	if len(env.Events) == 0 {
		t.Fatalf("SDK ReadStream(A) returned no events: %s", raw)
	}

	// 3. Agent-to-agent delegation: A creates a follow-up turn on deployment B.
	outB, err := a.Delegate(ctx, b, agent.TurnInput{
		Key: "sdk-b-" + contracts.NewID(), ContextID: "sdk-test.delegated",
		Input: json.RawMessage(`"delegated by A"`), MaxSteps: 2,
	})
	if err != nil {
		t.Fatalf("SDK Delegate(A->B): %v", err)
	}
	if outB.TurnID == "" {
		t.Fatalf("SDK delegation returned no turn on B: %+v", outB)
	}

	// 4. Run the delegated turn on deployment B through the SDK.
	if err := b.Dispatch(ctx); err != nil {
		t.Fatalf("SDK Dispatch(B): %v", err)
	}
	if st := sdkWaitDone(t, b, outB.TurnID); st != string(space.TurnDone) {
		t.Fatalf("delegated B turn not done via SDK: status=%s", st)
	}

	// 5. SDK ListTurns on B surfaces the delegated turn.
	listRaw, err := b.ListTurns(ctx)
	if err != nil {
		t.Fatalf("SDK ListTurns(B): %v", err)
	}
	if !containsTurnID(listRaw, outB.TurnID) {
		t.Fatalf("SDK ListTurns(B) missing delegated turn %s: %s", outB.TurnID, listRaw)
	}

	t.Logf("SDK two-deployment + delegation OK: A turn=%s done, delegated B turn=%s done (stream events=%d)", outA.TurnID, outB.TurnID, len(env.Events))
}

func containsTurnID(raw []byte, turnID string) bool {
	var m struct {
		Turns []struct {
			ID string `json:"id"`
		} `json:"turns"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return false
	}
	for _, tn := range m.Turns {
		if tn.ID == turnID {
			return true
		}
	}
	return false
}
