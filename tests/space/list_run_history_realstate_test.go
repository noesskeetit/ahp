//go:build state_integration

package space_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestListRunHistoryRealState is the durable run-history proof on the REAL State
// server over real PostgreSQL (no mocks): two turns on one thread are persisted as
// runs, ListRuns returns BOTH oldest-first, a brand-new StateClient (no in-memory
// coordinator state — the offline analogue of a process restart) reads them back,
// and a foreign-scope token cannot see the thread (server-side isolation).
func TestListRunHistoryRealState(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())

	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
	th, err := e.client.CreateThread(ctx, human, scope, "ctx-rh-"+contracts.NewID(), "rh-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}

	var seqs []int64
	for i := 0; i < 2; i++ {
		// taskID empty: the server derives it from the presenting token's Task claim
		// (matches how the Coordinator's AcceptTurn creates runs).
		rn, err := e.client.CreateRun(ctx, human, th.ID, "rh-run-"+contracts.NewID(), json.RawMessage(`{"type":"turn","i":`+string(rune('0'+i))+`}`), "")
		if err != nil {
			t.Fatalf("CreateRun: %v", err)
		}
		seqs = append(seqs, rn.Sequence)
	}

	// Owner reads both runs, FIFO by sequence.
	runs, err := e.client.ListRuns(ctx, human, th.ID)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("ListRuns = %d runs, want 2", len(runs))
	}
	for i := 1; i < len(runs); i++ {
		if runs[i].Sequence <= runs[i-1].Sequence {
			t.Fatalf("runs not strictly FIFO: %v after %v", runs[i].Sequence, runs[i-1].Sequence)
		}
		if runs[i].ThreadID != th.ID {
			t.Fatalf("run thread_id=%q, want %q", runs[i].ThreadID, th.ID)
		}
	}
	if runs[0].Sequence != seqs[0] || runs[1].Sequence != seqs[1] {
		t.Fatalf("ListRuns sequence mismatch: got %v/%v want %v/%v", runs[0].Sequence, runs[1].Sequence, seqs[0], seqs[1])
	}

	// Restart analogue: a fresh client (same State, no in-memory turns) reads them.
	fresh := space.NewStateClient(e.srv.URL, e.srv.Client())
	rerun, err := fresh.ListRuns(ctx, human, th.ID)
	if err != nil {
		t.Fatalf("ListRuns (fresh client over PG): %v", err)
	}
	if len(rerun) != 2 {
		t.Fatalf("fresh client did not read durable runs: %d runs, want 2", len(rerun))
	}
	if rerun[0].Sequence != seqs[0] {
		t.Fatalf("fresh client first seq=%v, want %v", rerun[0].Sequence, seqs[0])
	}

	// Foreign-scope token (different subject) must NOT see the thread.
	intruder, err := e.signer.Sign(auth.Claims{
		Audience: cAudience, Kind: auth.KindTask,
		Tenant: e.tenant, Deployment: e.deployment,
		Subject: "intruder-" + contracts.NewID(), Task: "task-intruder", Root: "run-i",
		Context: "ctx-i", Grant: "grant-i", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatalf("sign intruder: %v", err)
	}
	if _, err := e.client.ListRuns(ctx, intruder, th.ID); err == nil {
		t.Fatalf("ListRuns with foreign-scope token leaked thread (err = nil)")
	}
}
