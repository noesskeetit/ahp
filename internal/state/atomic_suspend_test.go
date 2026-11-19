package state

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Decode the desired wire fields so the old implementation fails by refusing
// waiting, rather than by a missing Go field at compile time.
func atomicWaitStatus(t *testing.T, wait string, deadline time.Time) *RunStatusOp {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"status": RunWaiting, "wait_for": wait, "deadline": deadline})
	if err != nil {
		t.Fatal(err)
	}
	var op RunStatusOp
	if err := json.Unmarshal(raw, &op); err != nil {
		t.Fatal(err)
	}
	return &op
}

func TestAtomicSuspendPublishesQuestionAndPreservesWakeup(t *testing.T) {
	for _, common := range []bool{false, true} {
		for _, mode := range []string{"waiting", "early-response", "response-acked-in-cut", "unrelated-response"} {
			t.Run(fmt.Sprintf("common=%t/%s", common, mode), func(t *testing.T) {
				x := rev27Fixture(t)
				deadline := time.Now().UTC().Add(2 * time.Minute).Truncate(time.Microsecond)
				var ack []string
				if mode != "waiting" {
					kind := "hitl_response"
					if mode == "unrelated-response" {
						kind = "other-event"
					}
					ev, err := x.s.Enqueue(x.ctx, x.a, x.ta, "early-answer", kind, json.RawMessage(`{"approved":true}`))
					if err != nil {
						t.Fatal(err)
					}
					if mode == "response-acked-in-cut" {
						ack = []string{ev.ID}
					}
				}
				op := atomicWaitStatus(t, "hitl_response", deadline)
				stream := &StreamOp{Key: "native-question", Kind: "native-a2a", Payload: json.RawMessage(`{"question_id":"question-1","question":"continue?"}`)}
				if common {
					req := rev27Request(t, "atomic-question", 0, nil)
					req.RunStatus, req.Stream, req.AckInbox = op, stream, ack
					receipt, err := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
					if err != nil {
						t.Fatalf("common frame and suspend must commit together: %v", err)
					}
					read, err := x.s.ReadRecords(x.ctx, x.a, x.ta, req.Namespace, 0)
					if err != nil || read.Receipt.CutID != receipt.CutID {
						t.Fatal("question cut missing", err)
					}
					replay, err := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
					if err != nil || replay.CutID != receipt.CutID {
						t.Fatal("suspend ACK replay changed the cut", err)
					}
					changed := *req.RunStatus
					changed.WaitFor = "different-answer"
					mismatch := req
					mismatch.RunStatus = &changed
					if _, err := x.s.CommitRecords(x.ctx, x.a, x.fa, mismatch); contracts.Code(err) != contracts.ErrKeyReuse {
						t.Fatal("reused receipt key accepted another wait target", err)
					}
					if _, err := x.s.CommitRecords(x.ctx, x.a, x.fa, rev27Request(t, "old-writer", receipt.Revision, nil)); contracts.Code(err) != contracts.ErrStaleEpoch {
						t.Fatal("released epoch wrote another cut", err)
					}
				} else {
					if len(ack) > 0 {
						// Legacy PutBatch has no inbox-ACK member. Its early-response
						// case still exercises the same readiness predicate.
						if err := x.s.AckInbox(x.ctx, x.a, x.fa.RunID, x.fa.Worker, x.fa.Epoch, ack); err != nil {
							t.Fatal(err)
						}
					}
					cp := &Checkpoint{ID: "native-question", ThreadID: x.ta, Namespace: "native/a2a", Version: 1, Metadata: json.RawMessage(`{}`), Parts: []Part{{Channel: "question", Version: "1", Data: []byte("continue?")}}}
					out, err := x.s.PutBatch(x.ctx, x.a, x.fa, BatchRequest{Key: "atomic-question", Checkpoint: cp, Stream: stream, RunStatus: op})
					if err != nil || !out.OK {
						t.Fatalf("checkpoint and suspend must commit together: %v", err)
					}
					loaded, err := x.s.GetCheckpoint(x.ctx, x.a, x.ta, cp.Namespace, cp.ID)
					if err != nil || len(loaded.Parts) != 1 || loaded.Parts[0].Channel != "question" {
						t.Fatal("question checkpoint missing after suspend", err)
					}
					data, err := x.s.GetPart(x.ctx, x.a, x.ta, cp.Namespace, loaded.Parts[0].Channel, loaded.Parts[0].Version)
					if err != nil || string(data) != "continue?" {
						t.Fatal("question checkpoint payload missing after suspend", err)
					}
					if _, err := x.s.PutBatch(x.ctx, x.a, x.fa, BatchRequest{Key: "old-writer", RunStatus: &RunStatusOp{Status: RunRunning}}); contracts.Code(err) != contracts.ErrStaleEpoch {
						t.Fatal("released epoch updated the run", err)
					}
				}
				events, err := x.s.ReadStream(x.ctx, x.a, x.ta, 0, 10)
				if err != nil || len(events) != 1 || !jsonEqualTrace(events[0].Payload, stream.Payload) {
					t.Fatal("question stream not durably published exactly once", events, err)
				}
				run, err := x.s.GetRun(x.ctx, x.a, x.fa.RunID)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "early-response" {
					if run.Status != RunPending || run.WaitFor != "" || run.Deadline != nil {
						t.Fatalf("early response lost: %+v", run)
					}
				} else if run.Status != RunWaiting || run.WaitFor != "hitl_response" || run.Deadline == nil || !run.Deadline.Equal(deadline) {
					t.Fatalf("suspend fields were not preserved: %+v", run)
				}
				if mode != "early-response" {
					if _, err := x.s.Enqueue(x.ctx, x.a, x.ta, "next-answer", "hitl_response", json.RawMessage(`{"approved":true}`)); err != nil {
						t.Fatal(err)
					}
				}
				lease, err := x.s.ClaimRunFor(x.ctx, x.a.Tenant, x.a.Deployment, "replacement", x.fa.RunID, time.Minute)
				if err != nil || lease == nil || lease.Epoch <= x.fa.Epoch {
					t.Fatalf("replacement could not claim after atomic suspend: lease=%+v error=%v", lease, err)
				}
			})
		}
	}
}

func TestAtomicSuspendConcurrentAnswerCannotLoseWakeup(t *testing.T) {
	for _, common := range []bool{false, true} {
		t.Run(fmt.Sprintf("common=%t", common), func(t *testing.T) {
			x := rev27Fixture(t)
			op := atomicWaitStatus(t, "hitl_response", time.Now().UTC().Add(time.Minute))
			req := rev27Request(t, "racing-question", 0, nil)
			req.RunStatus = op
			start := make(chan struct{})
			errs := make(chan error, 2)
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				_, err := x.s.Enqueue(x.ctx, x.a, x.ta, "racing-answer", "hitl_response", json.RawMessage(`{"approved":true}`))
				errs <- err
			}()
			go func() {
				defer wg.Done()
				<-start
				var err error
				if common {
					_, err = x.s.CommitRecords(x.ctx, x.a, x.fa, req)
				} else {
					_, err = x.s.PutBatch(x.ctx, x.a, x.fa, BatchRequest{Key: "racing-question", Checkpoint: atomicQuestionCheckpoint(x.ta), RunStatus: op})
				}
				errs <- err
			}()
			close(start)
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			run, err := x.s.GetRun(x.ctx, x.a, x.fa.RunID)
			if err != nil || run.Status != RunPending || run.WaitFor != "" || run.Deadline != nil {
				t.Fatalf("matching concurrent input left run asleep: %+v error=%v", run, err)
			}
			lease, err := x.s.ClaimRunFor(x.ctx, x.a.Tenant, x.a.Deployment, "replacement", x.fa.RunID, time.Minute)
			if err != nil || lease == nil || lease.Epoch <= x.fa.Epoch {
				t.Fatalf("concurrent answer did not make run claimable: %+v error=%v", lease, err)
			}
		})
	}
}

func TestAtomicSuspendRejectsInvalidWaitWithoutPartialWrites(t *testing.T) {
	deadline := time.Now().UTC().Add(time.Minute)
	cases := []RunStatusOp{
		{Status: RunWaiting},
		{Status: RunWaiting, WaitFor: "unsafe target"},
		{Status: RunPending, WaitFor: "hitl_response"},
		{Status: RunRunning, Deadline: &deadline},
		{Status: RunWaiting, WaitFor: "hitl_response", CloseReason: "completed"},
		{Status: "unknown"},
	}
	for _, common := range []bool{false, true} {
		t.Run(fmt.Sprintf("common=%t", common), func(t *testing.T) {
			x := rev27Fixture(t)
			for i, op := range cases {
				var err error
				if common {
					req := rev27Request(t, fmt.Sprintf("invalid-%d", i), 0, nil)
					req.RunStatus = &op
					_, err = x.s.CommitRecords(x.ctx, x.a, x.fa, req)
				} else {
					_, err = x.s.PutBatch(x.ctx, x.a, x.fa, BatchRequest{Key: fmt.Sprintf("invalid-%d", i), Checkpoint: atomicQuestionCheckpoint(x.ta), RunStatus: &op})
				}
				if contracts.Code(err) != contracts.ErrInvalid {
					t.Fatalf("invalid status %+v accepted: %v", op, err)
				}
			}
			assertAtomicSuspendRolledBack(t, x)
		})
	}
}

func atomicQuestionCheckpoint(thread string) *Checkpoint {
	return &Checkpoint{ID: "native-question", ThreadID: thread, Namespace: "native/a2a", Version: 1, Metadata: json.RawMessage(`{}`), Parts: []Part{{Channel: "question", Version: "1", Data: []byte("continue?")}}}
}

func assertAtomicSuspendRolledBack(t *testing.T, x *rev7Owners) {
	t.Helper()
	var persisted int
	err := x.s.pool.QueryRow(x.ctx, `SELECT
 (SELECT count(*) FROM ahp_state.checkpoints WHERE thread_id=$1) +
 (SELECT count(*) FROM ahp_state.common_cuts WHERE thread_id=$1)`, x.ta).Scan(&persisted)
	if err != nil || persisted != 0 {
		t.Fatalf("failed suspend published a partial snapshot: count=%d error=%v", persisted, err)
	}
	run, err := x.s.GetRun(x.ctx, x.a, x.fa.RunID)
	if err != nil || run.Status != RunRunning || run.WaitFor != "" || run.Deadline != nil {
		t.Fatal("failed commit changed run status", run, err)
	}
	if _, err := x.s.PutBatch(x.ctx, x.a, x.fa, BatchRequest{Key: "lease-still-current", RunStatus: &RunStatusOp{Status: RunRunning}}); err != nil {
		t.Fatal("failed commit released the lease", err)
	}
}

func TestAtomicSuspendStreamConflictRollsBackSnapshotAndInboxAck(t *testing.T) {
	for _, common := range []bool{false, true} {
		t.Run(fmt.Sprintf("common=%t", common), func(t *testing.T) {
			x := rev27Fixture(t)
			event, err := x.s.Enqueue(x.ctx, x.a, x.ta, "answer", "hitl_response", json.RawMessage(`{"approved":true}`))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := x.s.AppendStream(x.ctx, x.a, x.fa.RunID, x.fa.Worker, x.fa.Epoch, "conflict", "native-a2a", json.RawMessage(`{"old":true}`)); err != nil {
				t.Fatal(err)
			}
			op := atomicWaitStatus(t, "hitl_response", time.Now().UTC().Add(time.Minute))
			stream := &StreamOp{Key: "conflict", Kind: "native-a2a", Payload: json.RawMessage(`{"new":true}`)}
			if common {
				req := rev27Request(t, "conflicting-question", 0, nil)
				req.RunStatus, req.Stream, req.AckInbox = op, stream, []string{event.ID}
				_, err = x.s.CommitRecords(x.ctx, x.a, x.fa, req)
			} else {
				_, err = x.s.PutBatch(x.ctx, x.a, x.fa, BatchRequest{Key: "conflicting-question", Checkpoint: atomicQuestionCheckpoint(x.ta), Stream: stream, RunStatus: op})
			}
			if contracts.Code(err) != contracts.ErrKeyReuse {
				t.Fatal("conflicting stream must reject entire checkpoint/suspend", err)
			}
			assertAtomicSuspendRolledBack(t, x)
			var acked bool
			if err := x.s.pool.QueryRow(x.ctx, `SELECT acked FROM ahp_state.inbox_events WHERE id=$1 AND thread_id=$2`, event.ID, x.ta).Scan(&acked); err != nil || acked {
				t.Fatal("failed question commit acknowledged the answer", acked, err)
			}
		})
	}
}
