package statejournal

import (
	"bufio"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestQueuedTurnLeaseRecoveryPostgres(t *testing.T) {
	ctx, _, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "langgraph", Subject: "user"}
	req := EnqueueRequest{
		SessionID: "turn-session", Harness: "langgraph", FormatVersion: 1, Key: "client-message",
		Input: Payload{Inline: []byte(`{"message":"hello"}`)},
	}
	receipt, err := store.EnqueueTurn(ctx, scope, req)
	if err != nil || receipt.HeadSeq != 1 {
		t.Fatalf("admit: %+v (%v)", receipt, err)
	}
	if replay, err := store.EnqueueTurn(ctx, scope, req); err != nil || replay != receipt {
		t.Fatalf("replay admission: %+v (%v)", replay, err)
	}
	changed := req
	changed.Input = Payload{Inline: []byte(`{"message":"different"}`)}
	if _, err := store.EnqueueTurn(ctx, scope, changed); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("accepted changed input with original key: %v", err)
	}
	changed.Key = "second-message"
	if _, err := store.EnqueueTurn(ctx, scope, changed); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("accepted another input while queued: %v", err)
	}
	if _, err := store.AcquireSession(ctx, scope, req.SessionID, "old-route", time.Minute); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("ordinary lease acquisition bypassed queue: %v", err)
	}
	foreign := scope
	foreign.Subject = "another-user"
	if _, err := store.ClaimTurn(ctx, foreign, "", "foreign-worker", time.Minute); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign user reached a turn: %v", err)
	}
	first, err := store.ClaimTurn(ctx, scope, "", "worker-one", 350*time.Millisecond)
	if err != nil || first.Epoch != 1 || first.InputSeq != 1 || string(first.Input.Inline) != string(req.Input.Inline) {
		t.Fatalf("first claim: %+v (%v)", first, err)
	}
	if _, err := store.ClaimTurn(ctx, scope, "", "worker-two", time.Minute); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("a live lease was taken over: %v", err)
	}
	if err := store.ReleaseLease(ctx, scope, req.SessionID, first.Lease); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("the ordinary release bypassed the terminal write: %v", err)
	}
	timer := time.NewTimer(time.Until(first.ExpiresAt) + 20*time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if count, err := store.RequeueExpiredTurns(ctx, scope.Tenant, 64); err != nil || count != 1 {
		t.Fatalf("sweep expired claim = %d, error = %v", count, err)
	}
	if status, err := store.GetTurnStatus(ctx, scope, req.SessionID); err != nil ||
		status.Status != SessionQueued || status.InputSeq == nil || *status.InputSeq != first.InputSeq {
		t.Fatalf("recovery lost the queued input: %+v, error = %v", status, err)
	}
	next, err := store.ClaimTurn(ctx, scope, req.SessionID, "worker-two", time.Minute)
	if err != nil || next.Epoch != 2 || next.InputSeq != first.InputSeq || string(next.Input.Inline) != string(first.Input.Inline) {
		t.Fatalf("reclaim: %+v (%v)", next, err)
	}
	finish := AppendRequest{
		Key: "final-op", ExpectedSeq: next.HeadSeq, FinishTurn: SessionWaitingInput,
		Events: []EventInput{{ID: "final-message", Type: "turn.waiting", Role: "assistant", Codec: "json", Payload: Payload{Inline: []byte(`{"question":"Proceed?"}`)}}},
	}
	if _, err := store.AppendEvents(ctx, scope, req.SessionID, first.Lease, false, finish); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("stale lease finished the current turn: %v", err)
	}
	finished, err := store.AppendEvents(ctx, scope, req.SessionID, next.Lease, false, finish)
	if err != nil || finished.HeadSeq != 2 {
		t.Fatalf("finish: %+v (%v)", finished, err)
	}
	if replay, err := store.AppendEvents(ctx, scope, req.SessionID, next.Lease, false, finish); err != nil || replay != finished {
		t.Fatalf("lost terminal ACK: %+v (%v)", replay, err)
	}
	if status, err := store.GetTurnStatus(ctx, scope, req.SessionID); err != nil || status.Status != SessionWaitingInput || status.InputSeq != nil || status.HeadSeq != 2 {
		t.Fatalf("unexpected terminal status: %+v (%v)", status, err)
	}
	if followup, err := store.EnqueueTurn(ctx, scope, changed); err != nil || followup.HeadSeq != 3 {
		t.Fatalf("admit response to interrupt: %+v (%v)", followup, err)
	}
	if replay, err := store.EnqueueTurn(ctx, scope, req); err != nil || replay != receipt {
		t.Fatalf("old input replay after a new input: %+v (%v)", replay, err)
	}
	if status, err := store.GetTurnStatus(ctx, scope, req.SessionID); err != nil || status.Status != SessionQueued || status.InputSeq == nil || *status.InputSeq != 3 {
		t.Fatalf("an old retry changed the queued input: %+v (%v)", status, err)
	}
}

func TestSweepTurnAfterIngressSIGKILL(t *testing.T) {
	ctx, _, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "langgraph", Subject: "user"}
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQueueAdmissionChild$")
	cmd.Env = append(os.Environ(), "AHP_CRASH_TEST_TENANT="+scope.Tenant)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	lines := bufio.NewScanner(stdout)
	if !lines.Scan() || lines.Text() != "ENQUEUE_COMMITTED" {
		t.Fatalf("ingress did not confirm commit: %q (%v)", lines.Text(), lines.Err())
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil || !cmd.ProcessState.Sys().(syscall.WaitStatus).Signaled() {
		t.Fatalf("expected ingress SIGKILL: %v", err)
	}
	recoveryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	err = store.SweepTurns(recoveryCtx, scope, "sweeper", time.Minute, time.Second, func(ctx context.Context, turn TurnLease) error {
		if turn.Epoch != 1 || turn.SessionID != "crash" || string(turn.Input.Inline) != `{"steps":["input"]}` {
			t.Fatalf("unexpected recovered turn: %+v", turn)
		}
		finish := AppendRequest{
			Key: "recovered", ExpectedSeq: turn.HeadSeq, FinishTurn: SessionIdle,
			Events: []EventInput{{ID: "result", Type: "turn.completed", Role: "assistant", Codec: "json", Payload: turn.Input}},
		}
		_, err := store.AppendEvents(ctx, scope, turn.SessionID, turn.Lease, false, finish)
		cancel()
		return err
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("recovery: %v", err)
	}
	status, err := store.GetTurnStatus(ctx, scope, "crash")
	if err != nil || status.Status != SessionIdle || status.HeadSeq != 2 || status.InputSeq != nil {
		t.Fatalf("terminal status after ingress death: %+v (%v)", status, err)
	}
	t.Logf("ingress SIGKILL after COMMIT, no worker notification, sweeper restored input at epoch %d and committed IDLE", status.Epoch)
}

func TestQueueAdmissionChild(t *testing.T) {
	tenant := os.Getenv("AHP_CRASH_TEST_TENANT")
	if tenant == "" {
		t.Skip("used by the ingress SIGKILL test")
	}
	ctx, _, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: tenant, Deployment: "langgraph", Subject: "user"}
	_, err := store.EnqueueTurn(ctx, scope, EnqueueRequest{
		SessionID: "crash", Harness: "langgraph", FormatVersion: 1, Key: "crash-input",
		Input: Payload{Inline: []byte(`{"steps":["input"]}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("ENQUEUE_COMMITTED")
	<-ctx.Done()
}
