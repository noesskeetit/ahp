package state

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func suspendInboxFixture(t *testing.T) (*Store, contracts.Scope, *Lease) {
	t.Helper()
	s := newEventStore(t)
	scope := eventScope("hitl-" + contracts.NewID())
	ctx := context.Background()
	th, err := s.CreateThread(ctx, scope, "ctx-"+contracts.NewID(), "thread-"+contracts.NewID())
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.CreateRun(ctx, scope, th.ID, "task-1", "run-"+contracts.NewID(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "source", time.Minute)
	if err != nil || lease == nil || lease.Run.ID != run.ID {
		t.Fatalf("fresh claim: %v", err)
	}
	return s, scope, lease
}

// A response already durable in Inbox must not need a second user message to
// wake its run. Unrelated and acknowledged inputs still permit a genuine wait.
func TestSuspendAvailableInbox(t *testing.T) {
	for _, tc := range []struct {
		name, kind, wait, status string
		ack                      bool
	}{
		{"matching", "approval", "approval", RunPending, false},
		{"unrelated", "steering", "approval", RunWaiting, false},
		{"acknowledged", "approval", "approval", RunWaiting, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, scope, l := suspendInboxFixture(t)
			ctx := context.Background()
			ev, err := s.Enqueue(ctx, scope, l.Thread.ID, "input", tc.kind, json.RawMessage(`{"confirmed":true}`))
			if err != nil {
				t.Fatal(err)
			}
			if tc.ack {
				if err := s.AckInbox(ctx, scope, l.Run.ID, l.Worker, l.Epoch, []string{ev.ID}); err != nil {
					t.Fatal(err)
				}
			}
			deadline := time.Now().Add(time.Hour)
			if err := s.Suspend(ctx, scope, l.Run.ID, l.Worker, l.Epoch, tc.wait, &deadline); err != nil {
				t.Fatal(err)
			}
			status, wait := runState(t, s, l.Run.ID)
			if status != tc.status || (status == RunPending && wait != "") || (status == RunWaiting && wait != tc.wait) {
				t.Fatalf("available inbox lost at Suspend: status=%s wait=%s want=%s", status, wait, tc.status)
			}
			next, err := s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "new-worker", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if tc.status == RunPending && (next == nil || next.Run.ID != l.Run.ID || next.Epoch <= l.Epoch || next.Run.Deadline != nil) {
				t.Fatal("ready original run did not release lease/clear deadline for a higher epoch")
			}
			if tc.status == RunWaiting && next != nil {
				t.Fatal("unrelated/acked input incorrectly woke a waiting run")
			}
		})
	}
	t.Run("unsupported-wildcard-is-refused-without-ending-lease", func(t *testing.T) {
		s, scope, l := suspendInboxFixture(t)
		err := s.Suspend(context.Background(), scope, l.Run.ID, l.Worker, l.Epoch, "*", nil)
		if contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("wildcard is not a supported public wait target: %v", err)
		}
		status, wait := runState(t, s, l.Run.ID)
		if status != RunRunning || wait != "" {
			t.Fatal("invalid wait target changed execution state")
		}
	})
}

func TestSuspendConcurrentInboxWake(t *testing.T) {
	for attempt := 0; attempt < 12; attempt++ {
		s, scope, l := suspendInboxFixture(t)
		ctx := context.Background()
		start := make(chan struct{})
		done := make(chan error, 2)
		go func() {
			<-start
			_, err := s.Enqueue(ctx, scope, l.Thread.ID, "approval", "approval", json.RawMessage(`{"confirmed":true}`))
			done <- err
		}()
		go func() { <-start; done <- s.Suspend(ctx, scope, l.Run.ID, l.Worker, l.Epoch, "approval", nil) }()
		close(start)
		for i := 0; i < 2; i++ {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		status, wait := runState(t, s, l.Run.ID)
		if status != RunPending || wait != "" {
			t.Fatalf("concurrent response stranded: status=%s wait=%s attempt=%d", status, wait, attempt)
		}
	}
}
