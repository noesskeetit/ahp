//go:build state_integration

package space_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// D16: a run's DURABLE SNAPSHOT and its DURABLE TRANSCRIPT are two different artifacts, and the
// cold takeover reads the wrong one when they disagree.
//
// The snapshot is written at step boundaries and is deliberately treated as expendable: a refused
// PutCheckpoint is only logged ("space: runtime snapshot write failed", internal/space/
// coordinator.go:784) and the 4 MiB checkpoint-batch bound stops the writer for the rest of the run
// (:762). The transcript is the opposite: a failed AppendStream stops the run, because a hole in it
// is a replay hazard (:727). So by design the transcript may run ahead of the snapshot - and the
// takeover hands Core `resumeCheckpoint.Messages` (:705), i.e. the EXPENDABLE artifact, as the
// model's conversation history, while stream keys continue from the transcript's high-water (:661).
// Every boundary the snapshot missed is therefore generated a SECOND time, under fresh keys, and a
// reader of the thread sees the same work twice - the OG-2 hazard the high-water seeding only
// half-closed.
//
// This is not a hypothetical shape. It was measured on the live acceptance window at
// 2026-11-01T15:52:38+03:00: `space: runtime snapshot write failed ... err=stateclient
// PutCheckpoint: Post "https://127.0.0.1:18380/v1/state/PutCheckpoint": context deadline exceeded`
// on a run that carried on executing (evidence 132, run 20261101T155201.371742635Z).
//
// The fix must not be "make snapshot writes fatal": the snapshot is a recovery aid, and turning a
// State hiccup into a failed run would be a worse trade. The transcript is the source of truth, so
// a takeover resumes from the transcript whenever it is ahead of the snapshot.

// laggingState fronts the REAL State server and refuses every PutCheckpoint after the first one it
// accepted. One accepted boundary plus refused later ones is exactly the durable shape both the
// measured deadline failure and the batch bound leave behind: transcript ahead, snapshot frozen.
type laggingState struct {
	mu       sync.Mutex
	received int
	refused  int
}

func (l *laggingState) refusedCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.refused
}

func (l *laggingState) front(t *testing.T, upstream string, transport http.RoundTripper) *httptest.Server {
	t.Helper()
	u, err := url.Parse(upstream)
	if err != nil {
		t.Fatalf("parse upstream %q: %v", upstream, err)
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.Transport = transport // the workload certificate the real State requires lives there
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/PutCheckpoint") {
			l.mu.Lock()
			l.received++
			refuse := l.received > 1
			if refuse {
				l.refused++
			}
			l.mu.Unlock()
			if refuse {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":{"code":"UNAVAILABLE","message":"snapshot write refused by the test front"}}`))
				return
			}
		}
		proxy.ServeHTTP(w, r)
	}))
}

// durableStepFrames reads back the boundaries this turn's transcript already carries, in the same
// shape the snapshot writer pairs them (model/tool/done, internal/space/coordinator.go:815).
func durableStepFrames(t *testing.T, sc *space.StateClient, tok, threadID, turnID string) []string {
	t.Helper()
	var kinds []string
	after := int64(0)
	for {
		evs, err := sc.ReadStream(context.Background(), tok, threadID, after, 500)
		if err != nil {
			t.Fatalf("ReadStream: %v", err)
		}
		for _, e := range evs {
			after = e.Sequence
			if !strings.HasPrefix(e.Key, turnID+":") {
				continue
			}
			switch e.Kind {
			case "model", "tool", "done":
				kinds = append(kinds, e.Kind)
			}
		}
		if len(evs) < 500 {
			return kinds
		}
	}
}

// awaitDurableStepFrames waits until the transcript carries `want` boundaries for the turn.
func awaitDurableStepFrames(t *testing.T, sc *space.StateClient, tok, threadID, turnID string, want int) []string {
	t.Helper()
	deadline := time.Now().Add(40 * time.Second)
	for {
		got := durableStepFrames(t, sc, tok, threadID, turnID)
		if len(got) >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("durable transcript carries %d step boundaries, want %d (thread=%s)", len(got), want, threadID)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// resumedStepEntries counts the step entries the takeover actually handed Core as history, in the
// field names the snapshot writer itself uses ("role":"assistant","step_type":...).
func resumedStepEntries(t *testing.T, messages json.RawMessage) int {
	t.Helper()
	if len(messages) == 0 {
		return 0
	}
	var entries []map[string]any
	if err := json.Unmarshal(messages, &entries); err != nil {
		t.Fatalf("resumed messages are not the documented entry list: %v (%s)", err, messages)
	}
	n := 0
	for _, e := range entries {
		if st, _ := e["step_type"].(string); st != "" {
			n++
		}
	}
	return n
}

// TestColdResumeResumesFromTheTranscriptWhenTheCheckpointLags is the RED: three boundaries are
// durable in the thread transcript, the durable snapshot froze at the first one, and the takeover
// must not hand Core a history that is behind what the transcript already proves.
func TestColdResumeResumesFromTheTranscriptWhenTheCheckpointLags(t *testing.T) {
	e := cNewEnv(t)
	lag := &laggingState{}
	front := lag.front(t, e.srv.URL, e.srv.Client().Transport)
	t.Cleanup(front.Close)
	frontClient := space.NewStateClient(front.URL, nil)

	const input = "quote the measured numbers, then quote them again"
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "ada"}

	hung := make(chan struct{})
	t.Cleanup(func() { close(hung) })
	coord1 := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: frontClient, Core: &stepThenBlockCore{steps: 3, release: hung}, WorkerID: "w-lag-1",
	})
	ownerTok, _, err := realTaskExchanger{e}.Exchange(context.Background(), space.HumanPrincipal{Subject: "ada"}, "")
	if err != nil {
		t.Fatalf("mint owner task token: %v", err)
	}
	accept, err := coord1.AcceptTurn(e.ctx, ownerTok, scope, space.AcceptTurnRequest{
		Key: "lagrun-" + contracts.NewID(), ContextID: "ctx-lag-" + contracts.NewID(),
		Input: json.RawMessage(`"` + input + `"`), MaxSteps: 8,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord1.DispatchTurn(e.ctx, ownerTok, scope); err != nil {
		t.Fatalf("DispatchTurn(worker 1): %v", err)
	}
	held := waitLeaseHeld(t, coord1, accept.TurnID, "w-lag-1")

	// The durable shape the defect needs: THREE boundaries in the transcript, ONE in the snapshot.
	frames := awaitDurableStepFrames(t, frontClient, ownerTok, accept.ThreadID, accept.TurnID, 3)
	awaitCheckpoints(t, frontClient, ownerTok, accept.ThreadID, 1)
	if lag.refusedCount() == 0 {
		t.Fatal("the test front refused no snapshot write, so this leg measured nothing")
	}
	if cps, err := frontClient.ListCheckpoints(context.Background(), ownerTok, accept.ThreadID, space.CheckpointNamespace, 10); err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	} else if len(cps) != 1 {
		t.Fatalf("durable snapshots = %d, want exactly 1: the leg needs the snapshot frozen BEHIND the transcript (%d transcript boundaries)", len(cps), len(frames))
	}

	// Worker 1 dies: its lease is suspended past its deadline, which requeues the SAME run.
	past := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	if err := e.client.Suspend(e.ctx, ownerTok, accept.TurnID, "w-lag-1", held.LeaseEpoch, "crashed", &past); err != nil {
		t.Fatalf("Suspend(worker 1 lease): %v", err)
	}

	captured := make(chan *space.CoreRequest, 1)
	coord2 := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{capture: captured}, WorkerID: "w-lag-2",
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

	got := resumedStepEntries(t, req.Messages)
	if got < len(frames) {
		t.Fatalf("cold takeover handed Core %d history entries while the durable transcript already carries %d step boundaries (%s): every boundary the frozen snapshot missed is generated a SECOND time under a fresh step key, so the thread shows the same work twice. The transcript is the source of truth - a takeover must resume from it whenever the snapshot is behind.",
			got, len(frames), strings.Join(frames, ","))
	}
	t.Logf("resumed with %d history entries for %d durable boundaries (run=%s crashed_epoch=%d)", got, len(frames), accept.TurnID, held.LeaseEpoch)
}

// TestColdResumeWithACurrentCheckpointStillUsesTheCheckpoint is the control: when nothing was lost,
// the takeover keeps resuming from the durable snapshot exactly as before. Without this the RED
// could be "fixed" by abandoning snapshots, which would throw away the resumable state the writer
// exists to produce.
func TestColdResumeWithACurrentCheckpointStillUsesTheCheckpoint(t *testing.T) {
	e := cNewEnv(t)
	const input = "resume me from a snapshot that is current"
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "ada"}

	hung := make(chan struct{})
	t.Cleanup(func() { close(hung) })
	coord1 := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stepThenBlockCore{steps: 2, release: hung}, WorkerID: "w-ok-1",
	})
	ownerTok, _, err := realTaskExchanger{e}.Exchange(context.Background(), space.HumanPrincipal{Subject: "ada"}, "")
	if err != nil {
		t.Fatalf("mint owner task token: %v", err)
	}
	accept, err := coord1.AcceptTurn(e.ctx, ownerTok, scope, space.AcceptTurnRequest{
		Key: "okrun-" + contracts.NewID(), ContextID: "ctx-ok-" + contracts.NewID(),
		Input: json.RawMessage(`"` + input + `"`), MaxSteps: 8,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord1.DispatchTurn(e.ctx, ownerTok, scope); err != nil {
		t.Fatalf("DispatchTurn(worker 1): %v", err)
	}
	held := waitLeaseHeld(t, coord1, accept.TurnID, "w-ok-1")
	frames := awaitDurableStepFrames(t, e.client, ownerTok, accept.ThreadID, accept.TurnID, 2)
	awaitCheckpoints(t, e.client, ownerTok, accept.ThreadID, 2)

	past := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	if err := e.client.Suspend(e.ctx, ownerTok, accept.TurnID, "w-ok-1", held.LeaseEpoch, "crashed", &past); err != nil {
		t.Fatalf("Suspend(worker 1 lease): %v", err)
	}

	captured := make(chan *space.CoreRequest, 1)
	coord2 := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{capture: captured}, WorkerID: "w-ok-2",
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
		t.Fatal("takeover stopped resuming from the durable snapshot even though it is current")
	}
	if !strings.Contains(string(req.Messages), input) {
		t.Fatalf("resumed history lost the turn input: %s", req.Messages)
	}
	if got := resumedStepEntries(t, req.Messages); got < len(frames) {
		t.Fatalf("healthy takeover resumed with %d history entries for %d durable boundaries", got, len(frames))
	}
}
