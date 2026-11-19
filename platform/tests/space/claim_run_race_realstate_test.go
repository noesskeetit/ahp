//go:build state_integration

package space_test

// Block 8 (PDF rev 3.2): конкурентные ClaimRun / эпохи lease / старый писатель.
// Everything here runs against the REAL State on PostgreSQL through the production
// State API handler (cNewEnv: mTLS workload identity, real leases, real fences) -
// the property "many workers race for one run" cannot be proved on an in-process
// double whose claim is serialized by a mutex.

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestConcurrentClaimRunHasOneOwner_RealState: N workers claim at the same instant.
// Exactly one may receive a lease for the run, and while that lease is live nobody
// else may receive it either - two owners of one run means the same external effect
// executed twice, which the PDF forbids outright.
func TestConcurrentClaimRunHasOneOwner_RealState(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx

	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-claim-race-"+contracts.NewID(), "claim-race-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := e.client.CreateRun(ctx, human, th.ID, "claim-race-run", json.RawMessage(`{"type":"turn","n":1}`), "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	const racers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	type outcome struct {
		worker string
		epoch  int64
	}
	wins := make(chan outcome, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			worker := fmt.Sprintf("race-worker-%02d", i)
			<-start
			claim, err := e.client.ClaimRun(ctx, "", worker, 120*time.Second)
			if err != nil {
				t.Errorf("ClaimRun(%s): %v", worker, err)
				return
			}
			if claim.Lease == nil {
				return
			}
			if claim.Lease.Run.ID != run.ID {
				t.Errorf("%s claimed an unrelated run %s", worker, claim.Lease.Run.ID)
				return
			}
			wins <- outcome{worker: worker, epoch: claim.Lease.Epoch}
		}(i)
	}
	close(start)
	wg.Wait()
	close(wins)

	var owners []outcome
	for o := range wins {
		owners = append(owners, o)
	}
	if len(owners) != 1 {
		t.Fatalf("concurrent ClaimRun gave %d owners of one run %v, want exactly 1 (double owner = double external effect)", len(owners), owners)
	}
	if owners[0].epoch < 1 {
		t.Fatalf("winner %q holds no real epoch: %d", owners[0].worker, owners[0].epoch)
	}

	// While that lease is live the run must not be handed out again.
	late, err := e.client.ClaimRun(ctx, "", "race-worker-late", 120*time.Second)
	if err != nil {
		t.Fatalf("late ClaimRun: %v", err)
	}
	if late.Lease != nil && late.Lease.Run.ID == run.ID {
		t.Fatalf("run %s was claimed twice: owner %q epoch %d, late %q epoch %d",
			run.ID, owners[0].worker, owners[0].epoch, late.Lease.Worker, late.Lease.Epoch)
	}
}

// TestStaleWriterIsFencedAfterTakeover_RealState: the run steps down (Release ->
// pending), another worker claims it at a HIGHER epoch, and the old writer keeps
// trying to write. Its writes must be refused AND must not appear in the durable
// thread stream - a rejected-but-written frame is the same bug in a quieter shirt.
func TestStaleWriterIsFencedAfterTakeover_RealState(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx

	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-stale-writer-"+contracts.NewID(), "stale-writer-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := e.client.CreateRun(ctx, human, th.ID, "stale-writer-run", json.RawMessage(`{"type":"turn","n":1}`), "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	first, err := e.client.ClaimRun(ctx, "", "writer-old", 120*time.Second)
	if err != nil || first.Lease == nil {
		t.Fatalf("old worker ClaimRun: %v", err)
	}
	oldTok, oldEpoch := first.TaskToken, first.Lease.Epoch
	oldFence := space.Fence{RunID: run.ID, Worker: "writer-old", Epoch: oldEpoch}

	if err := e.client.Release(ctx, oldTok, run.ID, oldFence.Worker, oldEpoch, "pending"); err != nil {
		t.Fatalf("old worker Release(pending): %v", err)
	}
	second, err := e.client.ClaimRun(ctx, "", "writer-new", 120*time.Second)
	if err != nil || second.Lease == nil {
		t.Fatalf("new worker ClaimRun: %v", err)
	}
	if second.Lease.Epoch <= oldEpoch {
		t.Fatalf("takeover did not raise the epoch: old %d, new %d", oldEpoch, second.Lease.Epoch)
	}

	// The old writer is still alive and writes. It must be refused.
	staleBody := json.RawMessage(`{"writer":"old","marker":"MUST-NEVER-BE-DURABLE"}`)
	_, err = e.client.AppendStream(ctx, oldTok, th.ID, run.ID, oldFence.Worker, oldEpoch,
		space.StepKey(run.ID, 0), "tool", staleBody)
	if err == nil {
		t.Fatalf("stale writer (epoch %d) appended after a takeover to epoch %d: fencing is not enforced on the real State", oldEpoch, second.Lease.Epoch)
	}
	if code := contracts.Code(err); code != contracts.ErrConflict && code != contracts.ErrForbidden {
		t.Logf("stale append refused with %v (accepted as a refusal)", err)
	}

	// The new owner writes the same logical position.
	newBody := json.RawMessage(`{"writer":"new","marker":"OWNER"}`)
	if _, err := e.client.AppendStream(ctx, second.TaskToken, th.ID, run.ID, second.Lease.Worker, second.Lease.Epoch,
		space.StepKey(run.ID, 0), "tool", newBody); err != nil {
		t.Fatalf("current owner AppendStream: %v", err)
	}

	events, err := e.client.ReadStream(ctx, human, th.ID, 0, 200)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	for _, ev := range events {
		if got := string(ev.Payload); strings.Contains(got, "MUST-NEVER-BE-DURABLE") {
			t.Fatalf("the durable stream of a taken-over run carries the STALE writer's frame: seq=%d payload=%s", ev.Sequence, got)
		}
	}
	if len(events) != 1 {
		t.Fatalf("durable stream has %d events, want exactly the owner's 1", len(events))
	}
}
