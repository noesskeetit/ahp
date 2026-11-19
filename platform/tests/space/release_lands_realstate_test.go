//go:build state_integration

package space_test

// release_lands_realstate_test.go — DEF-H22-3 on real PostgreSQL.
//
// The failure this pins was measured, not imagined: on a canonical A->B stand the runner's
// Release was refused by a State peer that does not know the optional close_reason field
// ("transport: request body has an unknown field \"close_reason\""), so the run never became
// terminal, its lease merely expired, and the orphan sweep re-claimed and re-executed a
// finished turn once per tick (epoch 1..26, 33 model steps under a declared max_steps=3).
//
// Here the authoritative State service really runs on PostgreSQL, and a deliberately skewed
// peer in front of it refuses close_reason by name. The assertions are read back with direct
// SQL, out of band, because the client under test must not be the witness of its own success:
//   - the run reaches a TERMINAL status within one release attempt, not "eventually";
//   - the sweep can no longer claim it.
//
// Re-scoped under DEF-RELEASE-REASON-1 (not loosened, and the reason rule is asserted, not
// removed): a terminal failed/canceled cannot be ended without an explanation, so a peer
// refusing the close_reason key can no longer be worked around by dropping it - against a
// State that mandates the reason that retry is refused too, and the client surfaces it. The
// skew this file models in one fixture (a peer refusing close_reason forwarding to a State
// that requires it) is not a deployment that can exist: refusing the field is the pre-DEF-A6
// build, which has no mandate to enforce. So the DEF-H22-3 hazard - a terminal that must
// land in one attempt so the sweep cannot re-claim it - stays pinned for done, the status
// whose contract still permits an unstated terminal, and Part A pins the new rule at the
// HTTP door: an unexplained failed close is refused and moves nothing.
// The control half of the test shows the accepted contract is intact: against the real peer,
// the reason IS journaled in the run payload.
//
// Missing real PG => t.Skip (state_integration harness). Nothing here touches a shared stand.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// cSkewedPeer answers like a State deployment that predates DEF-A6's optional field: any
// body carrying close_reason is refused outright, everything else is forwarded verbatim to
// the real (mTLS) State server with the caller's own task token.
func cSkewedPeer(t *testing.T, e *cEnv) *space.StateClient {
	t.Helper()
	var refused int
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("proxy read: %v", err)
		}
		if bytes.Contains(body, []byte(`"close_reason"`)) {
			refused++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":"INVALID_ARGUMENT","message":"INVALID_ARGUMENT: transport: request body has an unknown field \"close_reason\""}`))
			return
		}
		req, err := http.NewRequestWithContext(e.ctx, http.MethodPost, e.srv.URL+r.URL.Path, bytes.NewReader(body))
		if err != nil {
			t.Errorf("proxy request: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if tok := r.Header.Get("Authorization"); tok != "" {
			req.Header.Set("Authorization", tok)
		}
		resp, err := e.srv.Client().Do(req)
		if err != nil {
			t.Errorf("proxy forward: %v", err)
			return
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(out)
	}))
	t.Cleanup(func() {
		srv.Close()
		if refused == 0 {
			t.Errorf("the skewed peer never refused anything: the test proves nothing")
		}
	})
	return space.NewStateClient(srv.URL, srv.Client())
}

// cRunRow reads the run straight from PostgreSQL: what the durable plane says, not what the
// caller was told.
func cRunRow(t *testing.T, runID string) (status, reason string) {
	t.Helper()
	dsn := cDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("out-of-band connect: %v", err)
	}
	defer conn.Close(ctx)
	var reasonVal *string
	if err := conn.QueryRow(ctx,
		`SELECT status, payload->>'close_reason' FROM ahp_state.runs WHERE id=$1`, runID).Scan(&status, &reasonVal); err != nil {
		t.Fatalf("out-of-band read of run %s: %v", runID, err)
	}
	if reasonVal != nil {
		reason = *reasonVal
	}
	return status, reason
}

func cDatabaseURL(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("set AHP_TEST_DATABASE_URL or DATABASE_URL")
	}
	return dsn
}

func TestReleaseLands_WhenPeerRefusesTheReason_RealState(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())

	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "h22-skew-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := e.client.CreateRun(ctx, human, th.ID, "h22-skew-run", json.RawMessage(`{"type":"turn","max_steps":3}`), "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	claim, err := e.client.ClaimRun(ctx, "", "w-h22-skew", 4*time.Minute)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if claim == nil || claim.Lease == nil || claim.Lease.Run.ID != run.ID {
		t.Fatalf("the real State must hand out the run we just created, got %+v", claim)
	}
	lease := claim.Lease

	skewed := cSkewedPeer(t, e)

	// PART A - what DEF-RELEASE-REASON-1 changed about this shape. A failed run cannot
	// reach its end through the State door without an explanation, so a client whose
	// reason key was refused by an intermediate peer cannot land it unexplained either:
	// the honest retry is refused too, and the client has to surface that instead of
	// pretending the run closed. The row proves it - still running, no reason written -
	// which is also what keeps a refused close from becoming an ANONYMOUS terminal.
	releaseErr := skewed.ReleaseWithReason(ctx, claim.TaskToken, lease.Run.ID, lease.Worker, lease.Epoch, "failed", "step_limit")
	if releaseErr == nil {
		t.Fatalf("an unexplained failed close was accepted through the real State: the run became " +
			"terminal with no reason, which is the defect DEF-RELEASE-REASON-1 exists to refuse")
	}
	if status, reason := cRunRow(t, lease.Run.ID); status != "running" || reason != "" {
		t.Fatalf("the refused close left the run at (%q,%q), want it untouched at (running,\"\"); "+
			"a refusal must not move a run or write a reason it never accepted", status, reason)
	}

	// PART B - what DEF-H22-3 actually measured, pinned for the status whose contract
	// still permits an unstated terminal: done. The same skewed peer refuses close_reason
	// by name, the client drops that one key and retries, and the run has to reach its
	// end in that single attempt - because a run left non-terminal is a run the orphan
	// sweep re-claims and re-executes (epoch 1..26 with 33 model events under a declared
	// max_steps=3). That hazard is independent of the reason rule and stays covered.
	// Its own conversation: the run above is deliberately still running with a live lease,
	// and a thread queues behind its own unfinished run.
	th2, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "h22-skew-done-thread-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread(run2): %v", err)
	}
	run2, err := e.client.CreateRun(ctx, human, th2.ID, "h22-skew-done-"+contracts.NewID(), json.RawMessage(`{"type":"turn","max_steps":3}`), "")
	if err != nil {
		t.Fatalf("CreateRun(run2): %v", err)
	}
	claim2, err := e.client.ClaimRun(ctx, "", "w-h22-skew-done", 4*time.Minute)
	if err != nil || claim2 == nil || claim2.Lease == nil || claim2.Lease.Run.ID != run2.ID {
		t.Fatalf("ClaimRun for run2: err=%v claim=%+v", err, claim2)
	}
	if err := skewed.ReleaseWithReason(ctx, claim2.TaskToken, claim2.Lease.Run.ID,
		claim2.Lease.Worker, claim2.Lease.Epoch, "done", "model_finished"); err != nil {
		t.Fatalf("a refusal of the optional reason must not cost a done run its terminal status: %v", err)
	}
	status, reason := cRunRow(t, run2.ID)
	if status != "done" {
		t.Fatalf("run left non-terminal after a refused reason: status=%q (this is DEF-H22-3)", status)
	}
	if reason != "" {
		t.Errorf("the skewed peer cannot store a reason; journaled reason=%q want empty", reason)
	}

	// And the sweep must no longer be able to claim the closed run.
	again, err := e.client.ClaimRun(ctx, "", "w-h22-sweep", 4*time.Minute)
	if err != nil {
		t.Fatalf("sweep ClaimRun: %v", err)
	}
	if again != nil && again.Lease != nil && again.Lease.Run.ID == run2.ID {
		t.Fatalf("the orphan sweep re-claimed a TERMINAL run (this is the re-execution path of DEF-H22-3)")
	}
}

func TestReleaseJournalsTheReason_AgainstTheRealPeer_RealState(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "h22-control-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := e.client.CreateRun(ctx, human, th.ID, "h22-control-run", json.RawMessage(`{"type":"turn","max_steps":3}`), "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	claim, err := e.client.ClaimRun(ctx, "", "w-h22-ctl", 4*time.Minute)
	if err != nil || claim == nil || claim.Lease == nil || claim.Lease.Run.ID != run.ID {
		t.Fatalf("ClaimRun: err=%v claim=%+v", err, claim)
	}
	lease := claim.Lease
	if err := e.client.ReleaseWithReason(ctx, claim.TaskToken, lease.Run.ID, lease.Worker, lease.Epoch, "done", "model_finished"); err != nil {
		t.Fatalf("release against the real peer: %v", err)
	}
	status, reason := cRunRow(t, lease.Run.ID)
	if status != "done" || reason != "model_finished" {
		t.Fatalf("accepted contract regressed: status=%q reason=%q, want done/model_finished", status, reason)
	}
}
