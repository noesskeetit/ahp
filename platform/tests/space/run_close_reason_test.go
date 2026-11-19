package space_test

// run_close_reason_test.go -- DEF-A6: the run plane states WHY it ended, in the
// execution plane's own dictionary, so a verifier comparing the two ends of an
// A->B delegation compares the same word against the same word instead of
// comparing a task's metadata.close_reason against a run's bare status column.
//
// The dictionary is not restated here: internal/space calls
// gateway.ClassifyCloseReason, the single classifier this product has, and these
// tests assert the value that reaches State.

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// spokenCore is a Core double that behaves like a real turn: one model frame carrying
// the model's own words, then the done frame, then a clean return. A double that only
// emits StepDone would prove nothing about the reason, because the classifier's
// "the model said nothing" rule would (correctly) refuse to call that model_finished.
type spokenCore struct {
	words string
	block chan struct{}
	calls atomic.Int64
}

func (c *spokenCore) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	c.calls.Add(1)
	if c.block != nil {
		select {
		case <-c.block:
		case <-ctx.Done():
			results <- space.StepResult{StepType: space.StepError, Done: true}
			return ctx.Err()
		}
	}
	if c.words != "" {
		payload, _ := json.Marshal(map[string]string{"text": c.words})
		results <- space.StepResult{StepType: space.StepModel, Content: payload}
	}
	results <- space.StepResult{StepType: space.StepDone, Content: json.RawMessage(`{"text":"done"}`), Done: true}
	return nil
}

// collectReleases drains what the State double recorded, up to d.
func collectReleases(t *testing.T, f *erunState, d time.Duration) []erunRelease {
	t.Helper()
	deadline := time.After(d)
	var out []erunRelease
	for {
		select {
		case r := <-f.releaseReasonCh:
			out = append(out, r)
		case <-deadline:
			return out
		}
	}
}

// TestFinishedRunReleasesWithModelFinished is requirement (a): a turn that ended with
// the model declaring itself finished hands State model_finished, not a bare "done".
func TestFinishedRunReleasesWithModelFinished(t *testing.T) {
	f := newERunState(t, erunChildTask)
	issuer := &grantRec{childToken: "MINTED-CHILD-TOK"}
	core := &spokenCore{words: "the marker is written"}
	c := newERunCoordinator(f, core, issuer)

	owner := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "owner-a"}
	ctx := space.WithAuthorityTask(context.Background(), erunAuthTask)
	if _, err := c.AcceptTurn(ctx, erunIngressTok, owner, space.AcceptTurnRequest{
		Key: "k-cr-done", ContextID: "ctx-cr-done", Input: json.RawMessage(`"write the marker"`), MaxSteps: 3,
	}); err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	f.reset()
	if err := c.DispatchTurn(context.Background(), erunIngressTok, owner); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}

	releases := collectReleases(t, f, 10*time.Second)
	if len(releases) == 0 {
		t.Fatal("the run never reached State with a release at all")
	}
	last := releases[len(releases)-1]
	if last.Status != "done" {
		t.Fatalf("status = %q, want done (releases: %+v)", last.Status, releases)
	}
	if last.CloseReason != gateway.CloseReasonModelFinished {
		t.Fatalf("close_reason = %q, want %q -- the value must come from the shared classifier",
			last.CloseReason, gateway.CloseReasonModelFinished)
	}
}

// TestCanceledRunDoesNotClaimModelFinished is requirement (b): a run the user stopped
// must never read as though the model finished, and must state the stop instead.
func TestCanceledRunDoesNotClaimModelFinished(t *testing.T) {
	f := newERunState(t, erunChildTask)
	issuer := &grantRec{childToken: "MINTED-CHILD-TOK"}
	core := &spokenCore{words: "the marker is written", block: make(chan struct{})}
	c := newERunCoordinator(f, core, issuer)

	owner := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "owner-a"}
	ctx := space.WithAuthorityTask(context.Background(), erunAuthTask)
	acc, err := c.AcceptTurn(ctx, erunIngressTok, owner, space.AcceptTurnRequest{
		Key: "k-cr-cancel", ContextID: "ctx-cr-cancel", Input: json.RawMessage(`"work until canceled"`), MaxSteps: 5,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := c.DispatchTurn(context.Background(), erunIngressTok, owner); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && core.calls.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if core.calls.Load() == 0 {
		t.Fatal("core did not start before cancellation")
	}
	drainReleases(f.releaseCh)
	f.reset()

	if err := c.CancelTurn(context.Background(), acc.TurnID, erunCallerTok); err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}
	close(core.block)

	releases := collectReleases(t, f, 10*time.Second)
	var canceled []erunRelease
	for _, r := range releases {
		if r.Status == "canceled" {
			canceled = append(canceled, r)
		}
		if r.CloseReason == gateway.CloseReasonModelFinished {
			t.Fatalf("a canceled run stated %q: %q -- a user stop is not a finished model (releases: %+v)",
				gateway.CloseReasonModelFinished, r.Status, releases)
		}
	}
	if len(canceled) == 0 {
		t.Fatalf("no canceled release reached State: %+v", releases)
	}
	for _, r := range canceled {
		if r.CloseReason != space.CloseReasonCanceled {
			t.Fatalf("canceled release carries close_reason=%q, want %q",
				r.CloseReason, space.CloseReasonCanceled)
		}
	}
}
