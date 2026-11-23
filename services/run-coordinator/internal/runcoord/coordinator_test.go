package runcoord

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

type recoveryState struct {
	State
	pending []journal.VMSession
	actions *[]string
	events  []journal.Event
}

func (s recoveryState) ListRecoverableVMSessions(context.Context) ([]journal.VMSession, error) {
	return s.pending, nil
}

func (s recoveryState) RequeueFencedVMTurn(_ context.Context, _ string, _ journal.FenceRequeueRequest) error {
	*s.actions = append(*s.actions, "requeue")
	return nil
}

func (s recoveryState) GetEvents(_ context.Context, _ string, after int64, _ int, _ string) (journal.EventPage, error) {
	var events []journal.Event
	for _, event := range s.events {
		if event.Seq > after {
			events = append(events, event)
		}
	}
	return journal.EventPage{Events: events, HeadSeq: 4}, nil
}

type dispatcherProbe struct {
	actions   *[]string
	failFence bool
}

func (d dispatcherProbe) Fence(_ context.Context, spec Execution) error {
	*d.actions = append(*d.actions, "fence:"+spec.ExecutionID)
	if d.failFence {
		return errors.New("still live")
	}
	return nil
}

func (d dispatcherProbe) EnsureRunning(_ context.Context, spec Execution) error {
	*d.actions = append(*d.actions, "ensure:"+spec.ExecutionID)
	return nil
}

func TestQueuedCrashBeforeEnsureRunningAndBootFailureRecovery(t *testing.T) {
	var calls []string
	scope := contracts.Scope{Tenant: "tenant", Deployment: "agent", Subject: "user"}
	state := recoveryState{pending: []journal.VMSession{{Scope: scope, SessionID: "s", TurnSeq: 2, Status: "QUEUED"}}}
	service := Coordinator{Scope: scope, State: state, Dispatcher: dispatcherProbe{actions: &calls}}
	for range 2 {
		if err := service.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	want := "ensure:" + journal.DeterministicExecutionID(scope, "s", 2, 1)
	if len(calls) != 2 || calls[0] != want || calls[1] != want {
		t.Fatalf("expected repeated attempts with the same execution ID, got %v", calls)
	}
}

func TestWorkerRecoveryFencesPhysicalOwnerBeforeRequeue(t *testing.T) {
	scope := contracts.Scope{Tenant: "tenant", Deployment: "agent", Subject: "user"}
	var calls []string
	// A warm followup has a different turn ID, but the old VM's ID retains the first turn.
	owner := journal.DeterministicExecutionID(scope, "s", 1, 1)
	state := recoveryState{actions: &calls, pending: []journal.VMSession{{
		Scope: scope, SessionID: "s", TurnSeq: 3, Epoch: 1, ExecutionID: owner, Status: "ACTIVE",
	}}}
	service := Coordinator{Scope: scope, State: state, Dispatcher: dispatcherProbe{actions: &calls, failFence: true}}
	if err := service.Reconcile(context.Background()); err == nil || len(calls) != 1 {
		t.Fatalf("recovery proceeded while the old process was live: %v %v", calls, err)
	}
	calls = nil
	service.Dispatcher = dispatcherProbe{actions: &calls}
	if err := service.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"fence:" + owner, "requeue", "ensure:" + journal.DeterministicExecutionID(scope, "s", 3, 2)}
	if strings.Join(calls, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v, want %v", calls, want)
	}
}

func TestTurnScopedSSEWithInterleavedEventsAndIncompleteTail(t *testing.T) {
	complete := journal.Event{Seq: 4, EventInput: journal.EventInput{TurnSeq: 1, Type: "turn.completed",
		Payload: journal.Payload{Inline: []byte(`{"turn_seq":1,"end_seq":4,"outcome":"IDLE"}`)}}}
	reply := journal.Event{Seq: 3, EventInput: journal.EventInput{TurnSeq: 1, Type: "answer"}}
	initial := journal.Event{Seq: 1, EventInput: journal.EventInput{TurnSeq: 1, Type: "turn.input"}}
	inserted := journal.Event{Seq: 2, EventInput: journal.EventInput{Type: "turn.followup"}}
	for _, test := range []struct {
		name    string
		events  []journal.Event
		want    string
		exclude string
	}{
		{"interleaved", []journal.Event{initial, inserted, reply, complete}, "id: 4\nevent: journal", "turn.followup"},
		{"missing", []journal.Event{initial, reply, complete}, "INCOMPLETE_TAIL", "event: end"},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := Coordinator{State: recoveryState{events: test.events}}
			r := httptest.NewRequest(http.MethodGet, "/stream?turn_seq=1", nil)
			r.Header.Set("Last-Event-ID", "1")
			w := httptest.NewRecorder()
			service.stream(w, r, "s")
			body := w.Body.String()
			if !strings.Contains(body, test.want) || strings.Contains(body, test.exclude) {
				t.Fatal(body)
			}
			if test.name == "interleaved" && !strings.Contains(body, "event: end") {
				t.Fatal(body)
			}
		})
	}
}
