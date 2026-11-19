//go:build state_integration

package space_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestRuntimeSnapshotWriterOnRealState is acceptance leg (1) of the snapshot-writer
// block, against the REAL State server on PostgreSQL: the process that EXECUTES the
// run - the one State granted the lease to - leaves durable snapshots at its step
// boundaries without anyone ever calling the HTTP SaveCheckpoint face.
//
// The composed stand is exactly this topology: the runner holds the lease and has no
// SaveCheckpoint face at all, so before this writer a fully executed journey left
// ListCheckpoints empty. The test pins three things:
//
//  1. a completed turn leaves one snapshot PER durable step boundary in namespace
//     space/core - and on the real server, which enforces checkpoint idempotency keys
//     (a second, different body under a used key is KEY_REUSE_MISMATCH), so a writer
//     that reused one key would fail every boundary after the first here;
//  2. every one of them was accepted under the lease State granted to THIS worker,
//     proven by a foreign-fence write to the same run being refused STALE_EPOCH and
//     leaving the durable list untouched;
//  3. the snapshot actually carries the conversation, so it is resumable state and not
//     an empty shell.
func TestRuntimeSnapshotWriterOnRealState(t *testing.T) {
	e := cNewEnv(t)

	const steps = 2 // model steps before the terminal done -> 3 snapshot boundaries

	// Ingress face: accepts the turn, never executes it, therefore holds no lease.
	coordIngress := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{}, WorkerID: "w-rtsnap-ingress",
	})
	srvIngress := httptest.NewServer(space.NewHandler(space.HandlerConfig{
		Coordinator:       coordIngress,
		Auth:              space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment)),
		RequireScopedTask: true,
	}))
	t.Cleanup(srvIngress.Close)

	// Execution face: this is the process State will grant the lease to, the analogue of
	// as-runner in the composed stand.
	coordExec := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{stepsBeforeDone: steps}, WorkerID: "w-rtsnap-exec",
	})
	srvExec := httptest.NewServer(space.NewHandler(space.HandlerConfig{
		Coordinator:       coordExec,
		Auth:              space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment)),
		RequireScopedTask: true,
	}))
	t.Cleanup(srvExec.Close)

	const input = "write the runtime snapshot on real state"
	key := "rtsnap-" + contracts.NewID()
	code, body := postSpaceAs(t, srvIngress.URL, "AcceptTurn", map[string]any{
		"key": key, "context_id": "ctx-" + key,
		"input": map[string]any{"role": "user", "content": input}, "max_steps": 4,
	}, "ada")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn(ada) = %d, want 200 (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	threadID, _ := acc["thread_id"].(string)
	if turnID == "" || threadID == "" {
		t.Fatalf("AcceptTurn missing ids: %s", body)
	}
	t.Cleanup(func() {
		_, _ = postSpaceAs(t, srvExec.URL, "CancelTurn", map[string]any{"turn_id": turnID}, "ada")
	})

	// Acceptance alone must not have produced a snapshot: nothing has executed.
	ownerTok, _, err := realTaskExchanger{e}.Exchange(context.Background(), space.HumanPrincipal{Subject: "ada"}, "")
	if err != nil {
		t.Fatalf("mint owner task token: %v", err)
	}
	if cps, err := e.client.ListCheckpoints(e.ctx, ownerTok, threadID, space.CheckpointNamespace, 10); err != nil {
		t.Fatalf("ListCheckpoints before execution: %v", err)
	} else if len(cps) != 0 {
		t.Fatalf("checkpoints before execution = %d, want 0", len(cps))
	}

	// The executor takes the run over; State issues ITS worker+epoch.
	if c, b := postSpaceAs(t, srvExec.URL, "DispatchTurn", nil, "ada"); c != http.StatusOK {
		t.Fatalf("DispatchTurn(exec) = %d, want 200 (body=%s)", c, b)
	}
	held := waitLeaseHeld(t, coordExec, turnID, "w-rtsnap-exec")
	if held.LeaseEpoch <= 0 {
		t.Fatalf("executor recorded a zero epoch: %+v", held)
	}

	// One snapshot per durable boundary (steps model steps + the terminal done), awaited
	// through a durable read rather than a wall-clock sleep.
	want := steps + 1
	cps := awaitCheckpoints(t, e.client, ownerTok, threadID, want)
	if len(cps) != want {
		t.Fatalf("runtime snapshots = %d, want %d (one per step boundary): %+v", len(cps), want, cps)
	}
	for _, cp := range cps {
		if cp.Namespace != space.CheckpointNamespace {
			t.Fatalf("snapshot %s namespace = %q, want %q", cp.ID, cp.Namespace, space.CheckpointNamespace)
		}
		if !strings.Contains(string(cp.Metadata), input) {
			t.Fatalf("snapshot %s does not carry the turn input: %s", cp.ID, cp.Metadata)
		}
	}
	// Distinct boundaries produced distinct durable rows, and the newest one is the head
	// of ListCheckpoints - which is exactly what a cold takeover resumes from.
	if cps[0].Version != int64(want) {
		t.Fatalf("newest snapshot version = %d, want %d", cps[0].Version, want)
	}

	// The fence that wrote those rows is the GRANTED lease, not merely "some" fence: a
	// write for the same run under a different worker is refused by the real server.
	forged := json.RawMessage(`{"messages":[{"role":"user","content":"forged"}]}`)
	_, err = e.client.PutCheckpoint(e.ctx, ownerTok,
		space.Fence{RunID: turnID, Worker: "w-not-the-holder", Epoch: held.LeaseEpoch},
		space.CheckpointNamespace, "rtsnap-forged", "", "rtsnap-forged", 99, forged, nil)
	if contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("foreign-worker PutCheckpoint err = %v, want %q", err, contracts.ErrStaleEpoch)
	}
	after, err := e.client.ListCheckpoints(e.ctx, ownerTok, threadID, space.CheckpointNamespace, 10)
	if err != nil {
		t.Fatalf("ListCheckpoints after the refused write: %v", err)
	}
	if len(after) != len(cps) {
		t.Fatalf("a refused foreign write changed durable state: before=%d after=%d", len(cps), len(after))
	}
}

// TestRuntimeSnapshotColdResumeOnRealState is acceptance leg (3) on the real server: the
// first executor writes a snapshot at a real step boundary and then loses its lease the
// way a dead process does (State suspends the lease past its deadline and requeues the
// run); the takeover, at a HIGHER epoch, resumes from THAT snapshot - Core receives the
// checkpoint and its messages - instead of replaying the turn from the raw input.
func TestRuntimeSnapshotColdResumeOnRealState(t *testing.T) {
	e := cNewEnv(t)

	const input = "resume me from a real durable snapshot"
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "ada"}

	// Worker 1 executes the run and hangs after its first step boundary: the abandoned
	// runner of the stand's restart gate.
	hung := make(chan struct{})
	t.Cleanup(func() { close(hung) })
	coord1 := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stepThenBlockCore{steps: 1, release: hung}, WorkerID: "w-rsresume-1",
	})
	ownerTok, _, err := realTaskExchanger{e}.Exchange(context.Background(), space.HumanPrincipal{Subject: "ada"}, "")
	if err != nil {
		t.Fatalf("mint owner task token: %v", err)
	}
	accept, err := coord1.AcceptTurn(e.ctx, ownerTok, scope, space.AcceptTurnRequest{
		Key: "rsresume-" + contracts.NewID(), ContextID: "ctx-rsresume-" + contracts.NewID(),
		Input: json.RawMessage(`"` + input + `"`), MaxSteps: 4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord1.DispatchTurn(e.ctx, ownerTok, scope); err != nil {
		t.Fatalf("DispatchTurn(worker 1): %v", err)
	}
	held := waitLeaseHeld(t, coord1, accept.TurnID, "w-rsresume-1")
	// The runtime snapshot exists BEFORE the crash - that is what the takeover resumes from.
	cps := awaitCheckpoints(t, e.client, ownerTok, accept.ThreadID, 1)
	if !strings.Contains(string(cps[0].Metadata), input) {
		t.Fatalf("snapshot does not carry the turn input: %s", cps[0].Metadata)
	}

	// Worker 1 dies: State suspends its lease past its deadline, which requeues the SAME
	// run, so the next claim carries a higher epoch and worker 1's fence is stale.
	past := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	if err := e.client.Suspend(e.ctx, ownerTok, accept.TurnID, "w-rsresume-1", held.LeaseEpoch, "crashed", &past); err != nil {
		t.Fatalf("Suspend(worker 1 lease): %v", err)
	}

	captured := make(chan *space.CoreRequest, 1)
	coord2 := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{capture: captured}, WorkerID: "w-rsresume-2",
	})
	takeoverTok, _, err := realTaskExchanger{e}.Exchange(context.Background(), space.HumanPrincipal{Subject: "ada"}, "")
	if err != nil {
		t.Fatalf("mint takeover task token: %v", err)
	}
	if err := coord2.DispatchTurn(e.ctx, takeoverTok, scope); err != nil {
		t.Fatalf("cold DispatchTurn: %v", err)
	}
	var req *space.CoreRequest
	select {
	case req = <-captured:
	case <-time.After(60 * time.Second):
		t.Fatal("cold dispatch on real State never reached Core")
	}
	if req.Checkpoint == nil {
		t.Fatal("takeover did not resume from the runtime snapshot (req.Checkpoint is nil)")
	}
	if !strings.Contains(string(req.Messages), input) {
		t.Fatalf("resumed messages did not come from the runtime snapshot: %s", req.Messages)
	}
	if req.Epoch <= held.LeaseEpoch {
		t.Fatalf("takeover epoch = %d, want higher than the crashed worker's %d", req.Epoch, held.LeaseEpoch)
	}
	t.Logf("resumed from checkpoint: run=%s crashed_epoch=%d takeover_epoch=%d checkpoint=%s",
		accept.TurnID, held.LeaseEpoch, req.Epoch, req.Checkpoint.ID)
}
