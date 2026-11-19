//go:build state_integration

package space_test

// close_reason_carriers_realstate_test.go — every carrier that ends a run must be
// able to say WHY it ended, and the reason must be visible in the run's terminal
// row read out of band.
//
// The defect this pins is composition-specific: the State face has accepted
// close_reason since DEF-A6, and the space coordinator sends it, but two other
// carriers of the same operation could not: the public Go SDK (sdk/go/state) and
// the as-runner seam (adapters/emil.RunStore). A chain that closed its run through
// either of them reached a terminal status with no terminal reason at all, while a
// test that drove the space client directly stayed green. That is the difference
// between "the field exists" and "the platform states a reason".
//
// Both carriers are exercised here against the REAL State handler on PostgreSQL,
// and read back with direct SQL, because the client that sent the reason is not a
// qualified witness that the reason arrived.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	statesdk "github.com/ahp-platform/ahp/sdk/go/state"
)

// cNewRun claims one run and returns its lease; every carrier test needs the same
// three steps and must not diverge in how it set them up.
func cNewRun(t *testing.T, e *cEnv, label, worker string) (runID, token string, epoch int64) {
	t.Helper()
	human := e.signTask(t, "task-"+label+"-"+contracts.NewID())
	th, err := e.client.CreateThread(e.ctx, human, contracts.Scope{}, "ctx-"+label+"-"+contracts.NewID(), label+"-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := e.client.CreateRun(e.ctx, human, th.ID, label+"-run", json.RawMessage(`{"type":"turn","max_steps":3}`), "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	claim, err := e.client.ClaimRun(e.ctx, "", worker, 4*time.Minute)
	if err != nil || claim == nil || claim.Lease == nil || claim.Lease.Run.ID != run.ID {
		t.Fatalf("ClaimRun: err=%v claim=%+v", err, claim)
	}
	return run.ID, claim.TaskToken, claim.Lease.Epoch
}

func TestCloseReasonCarriers_SDKReleasesWithReason_RealState(t *testing.T) {
	e := cNewEnv(t)
	runID, token, epoch := cNewRun(t, e, "sdk-carrier", "w-sdk")

	sdk, err := statesdk.New(statesdk.Config{
		BaseURL:    e.srv.URL,
		HTTPClient: e.srv.Client(),
		TokenSource: func(context.Context) (string, error) {
			return token, nil
		},
	})
	if err != nil {
		t.Fatalf("state sdk: %v", err)
	}
	if err := sdk.ReleaseWithReason(e.ctx, statesdk.Fence{RunID: runID, Worker: "w-sdk", Epoch: epoch},
		"done", "model_finished"); err != nil {
		t.Fatalf("the public SDK must be able to state a terminal reason: %v", err)
	}
	status, reason := cRunRow(t, runID)
	if status != "done" || reason != "model_finished" {
		t.Fatalf("SDK carrier lost the reason: status=%q reason=%q, want done/model_finished", status, reason)
	}
}

func TestCloseReasonCarriers_SDKReleaseSendsNoReasonKey_RealState(t *testing.T) {
	e := cNewEnv(t)
	runID, token, epoch := cNewRun(t, e, "sdk-plain", "w-sdk-plain")

	sdk, err := statesdk.New(statesdk.Config{
		BaseURL:    e.srv.URL,
		HTTPClient: e.srv.Client(),
		TokenSource: func(context.Context) (string, error) {
			return token, nil
		},
	})
	if err != nil {
		t.Fatalf("state sdk: %v", err)
	}
	if err := sdk.Release(e.ctx, statesdk.Fence{RunID: runID, Worker: "w-sdk-plain", Epoch: epoch}, "done"); err != nil {
		t.Fatalf("plain Release: %v", err)
	}
	status, reason := cRunRow(t, runID)
	if status != "done" {
		t.Fatalf("plain Release must still close the run: status=%q", status)
	}
	if reason != "" {
		t.Errorf("an unstated reason must stay absent, not be invented: reason=%q", reason)
	}
}

func TestCloseReasonCarriers_EmilSeamReleasesWithReason_RealState(t *testing.T) {
	e := cNewEnv(t)
	runID, token, epoch := cNewRun(t, e, "emil-seam", "w-emil")

	var store interface{} = emil.NewStateRunStore(space.NewStateClient(e.srv.URL, e.srv.Client()))
	rr, ok := store.(emil.ReasonReleaser)
	if !ok {
		t.Fatalf("the as-runner seam must carry a terminal reason; %T does not implement emil.ReasonReleaser", store)
	}
	lease := emil.Lease{RunID: runID, TaskToken: token, Worker: "w-emil", Epoch: epoch}
	if err := rr.ReleaseWithReason(e.ctx, lease, "failed", "step_limit"); err != nil {
		t.Fatalf("the seam must be able to state a terminal reason: %v", err)
	}
	status, reason := cRunRow(t, runID)
	if status != "failed" || reason != "step_limit" {
		t.Fatalf("seam carrier lost the reason: status=%q reason=%q, want failed/step_limit", status, reason)
	}
}

// TestCloseReasonCarriers_CanceledRunDoesNotReportModelFinished_RealState is the
// differential leg: the reason must describe the outcome, not be a constant. A run
// the user stopped ends as canceled, and a journal that answers model_finished for
// it is a lie about the outcome — the same word for two different verdicts is worse
// than no word at all, because an auditor cannot tell a finished turn from a
// stopped one. Nothing here defaults a reason: the carrier states it and the row
// must hold exactly that word.
func TestCloseReasonCarriers_CanceledRunDoesNotReportModelFinished_RealState(t *testing.T) {
	e := cNewEnv(t)
	runID, token, epoch := cNewRun(t, e, "canceled-diff", "w-cancel")

	sdk, err := statesdk.New(statesdk.Config{
		BaseURL:    e.srv.URL,
		HTTPClient: e.srv.Client(),
		TokenSource: func(context.Context) (string, error) {
			return token, nil
		},
	})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}
	if err := sdk.ReleaseWithReason(e.ctx, statesdk.Fence{RunID: runID, Worker: "w-cancel", Epoch: epoch},
		"canceled", "canceled"); err != nil {
		t.Fatalf("release a canceled run with its reason: %v", err)
	}
	status, reason := cRunRow(t, runID)
	if status != "canceled" {
		t.Fatalf("a canceled release must land a terminal canceled status, got %q", status)
	}
	if reason == "model_finished" {
		t.Fatalf("a stopped run reported %q: the reason must describe the outcome, not a constant", reason)
	}
	if reason != "canceled" {
		t.Fatalf("canceled run carries reason %q, want canceled", reason)
	}
}
