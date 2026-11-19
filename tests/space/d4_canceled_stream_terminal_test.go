package space_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// D4 (measured on the prod stand, evidence
// reports/deputy/evidence/d4-canceled-thread-stream-silence-20261101.json):
//
//	GET /v1/space/stream/<thread_id> for a thread whose ONLY turn is status=canceled
//	returned ZERO frames in a 12 s window and never closed. GetTurn answered canceled.
//
// Two independent causes, proven separately:
//
//	(a) the CANCEL path never wrote a terminal frame into the durable thread stream. Cancel
//	   revokes the run's own grant FIRST (correct: that is the security point of cancel) and
//	   closes the run durably, so by the time the worker notices, every write it attempts -
//	   including its terminal status frame - is refused. A canceled turn therefore ends with
//	   no durable terminal state, while the in-memory snapshot happily says canceled.
//	(b) the SSE tail never terminates: it polls ReadStream forever, so a thread whose runs
//	   are all terminal keeps an opened-but-silent connection ("connection accepted and
//	   silent"). Threads canceled before (a) stay in that shape forever, so the tail itself
//	   has to close them.

// silentCore parks every execution until its context is canceled and emits NO step - the
// exact shape of the second prod example (turn canceled, steps=0, stream_frames=0).
type silentCore struct {
	mu      sync.Mutex
	parked  int
	started chan struct{}
	once    sync.Once
}

func newSilentCore() *silentCore {
	return &silentCore{started: make(chan struct{})}
}

func (c *silentCore) Run(ctx context.Context, _ *space.CoreRequest, _ chan<- space.StepResult) error {
	c.mu.Lock()
	c.parked++
	c.mu.Unlock()
	c.once.Do(func() { close(c.started) })
	<-ctx.Done()
	return ctx.Err()
}

func (c *silentCore) parkedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.parked
}

// d4SpaceHTTP is the real Space HTTP edge (handler + coordinator + StateClient) over the
// State double, per-subject like a browser. It mirrors newHTTPSpace but also hands back the
// double so a test can state durable facts directly.
func d4SpaceHTTP(t *testing.T, core space.CoreRunner) (*httptest.Server, *mockState) {
	t.Helper()
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(srv.URL, nil),
		Core:        core,
		WorkerID:    "w-d4",
	})
	h := space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: authPerSubject})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)
	return hs, ms
}

// readSSEUntilEnd opens the browser SSE tail and reads until the SERVER closes the body
// (io.EOF) or the deadline expires. closed=false is the D4 symptom: a connection that is
// accepted, silent, and never finished.
func readSSEUntilEnd(t *testing.T, url, subject string, deadline time.Duration) (frames []sseFrame, closed bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("newreq: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer tok-"+subject)
	req.Header.Set("X-Test-Subject", subject)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET sse: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SSE status = %d, want 200 (body=%q)", resp.StatusCode, readAll(resp.Body))
	}
	br := bufio.NewReader(resp.Body)
	var evLine, data string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				closed = true
			}
			return frames, closed
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			if data == "" {
				continue
			}
			var env map[string]any
			_ = json.Unmarshal([]byte(data), &env)
			frames = append(frames, sseFrame{event: strings.TrimSpace(strings.TrimPrefix(evLine, "event:")), envel: env, raw: data})
			evLine, data = "", ""
		case strings.HasPrefix(line, "id:"):
			// the durable cursor; this test asserts terminality, not cursor order
		case strings.HasPrefix(line, "event:"):
			evLine = line
		case strings.HasPrefix(line, "data:"):
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
}

// terminalStatus returns the last terminal status a frame reported, and whether any did.
// Both a durable status event (payload {"status":...}) and an explicit end-of-stream frame
// count: what the browser needs is the terminal state of the canceled turn, however the
// edge chooses to carry it.
func terminalStatus(frames []sseFrame) (string, bool) {
	status := ""
	found := false
	for _, f := range frames {
		var s string
		if p, ok := f.envel["payload"].(map[string]any); ok {
			s, _ = p["status"].(string)
		}
		if s == "" {
			s, _ = f.envel["status"].(string)
		}
		switch s {
		case "done", "failed", "canceled":
			status, found = s, true
		}
	}
	return status, found
}

// TestD4CanceledThreadStreamTerminates is the live acceptance shape of D4: open a chat,
// cancel its only turn, then subscribe the way the browser does. The stream MUST deliver
// the terminal state of the canceled turn and MUST NOT hold an empty connection open.
// Nothing is tuned down: the same 12 s window the prod probe used.
func TestD4CanceledThreadStreamTerminates(t *testing.T) {
	core := newSilentCore()
	srv, ms := d4SpaceHTTP(t, core)
	const subject = "alice"

	code, body := postSpaceAs(t, srv.URL, "AcceptTurn", map[string]any{
		"key": "k-d4-stream", "context_id": "ctx-d4-stream",
		"input": map[string]any{"role": "user", "content": "go"}, "max_steps": 4,
	}, subject)
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn = %d (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	threadID, _ := acc["thread_id"].(string)
	turnID, _ := acc["turn_id"].(string)
	if threadID == "" || turnID == "" {
		t.Fatalf("accept returned no thread/turn: %s", body)
	}
	if c, b := postSpaceAs(t, srv.URL, "DispatchTurn", nil, subject); c != http.StatusOK {
		t.Fatalf("DispatchTurn = %d (body=%s)", c, b)
	}
	select {
	case <-core.started:
	case <-time.After(5 * time.Second):
		t.Fatal("core never started")
	}

	if c, b := postSpaceAs(t, srv.URL, "CancelTurn", map[string]any{"turn_id": turnID}, subject); c != http.StatusOK {
		t.Fatalf("CancelTurn = %d (body=%s)", c, b)
	}
	// The turn is durably canceled before anybody subscribes - the prod shape, where the UI
	// opens an already-canceled chat long after the cancel.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := ms.durableStatusOfThread(threadID)
		if st == "canceled" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, durableFrames := ms.durableStatusOfThread(threadID)
	if status != "canceled" {
		t.Fatalf("the canceled run is not durably closed (status=%q): cancel must close the run", status)
	}
	// `frames` is whatever the cancel path left behind (one terminal frame once cancel writes
	// it durably). The zero-frame shape - a thread canceled before any terminal frame was
	// ever written - is the separate leg below, because no cancel-path fix can rewrite it.
	t.Logf("durable state after cancel: run=%s stream_frames=%d", status, durableFrames)
	if c, b := postSpaceAs(t, srv.URL, "GetTurn", map[string]any{"turn_id": turnID}, subject); c == http.StatusOK {
		if s, _ := decodeMap(t, b)["status"].(string); s != "canceled" {
			t.Fatalf("GetTurn status = %q, want canceled (the prod evidence says GetTurn is honest)", s)
		}
	}

	got, closed := readSSEUntilEnd(t, srv.URL+"/v1/space/stream/"+threadID, subject, 12*time.Second)
	term, ok := terminalStatus(got)
	if !ok {
		t.Fatalf("D4: subscribing to a canceled thread delivered %d frames in 12s and closed=%v (frames=%q); the stream must deliver the terminal state of the canceled turn",
			len(got), closed, firstFrames(got, 5))
	}
	if term != "canceled" {
		t.Fatalf("canceled thread stream reported terminal status %q, want canceled", term)
	}
	if !closed {
		t.Fatalf("D4: the stream delivered %q but never closed the connection", term)
	}
}

// TestD4StreamTerminatesForThreadCanceledBeforeTheTerminalFrameExists is the other half of
// the prod measurement: the two named threads are durably canceled and their stream holds
// ZERO events. Rewriting the cancel path cannot help a row that is already written, so the
// SSE edge itself has to stop promising events a finished thread will never produce.
func TestD4StreamTerminatesForThreadCanceledBeforeTheTerminalFrameExists(t *testing.T) {
	core := newSilentCore()
	srv, ms := d4SpaceHTTP(t, core)
	const subject = "carol"

	threadID := ms.seedClosedThread("ctx-d4-historical", "Bearer tok-"+subject, "canceled")
	if status, frames := ms.durableStatusOfThread(threadID); status != "canceled" || frames != 0 {
		t.Fatalf("setup: thread is status=%q frames=%d, want canceled/0 (the prod shape)", status, frames)
	}

	got, closed := readSSEUntilEnd(t, srv.URL+"/v1/space/stream/"+threadID, subject, 12*time.Second)
	term, ok := terminalStatus(got)
	if !ok {
		t.Fatalf("D4: a historically canceled thread delivered %d frames in 12s and closed=%v (frames=%q); the tail must state the terminal status and close",
			len(got), closed, firstFrames(got, 5))
	}
	if term != "canceled" {
		t.Fatalf("historically canceled thread reported %q, want canceled", term)
	}
	if !closed {
		t.Fatalf("D4: the tail of a finished thread never closed the connection")
	}
}

// TestD4CancelWritesTheTerminalFrameDurably pins cause (a) at the coordinator: the cancel
// itself must put the terminal status frame into the durable thread stream, presented with
// the CALLER's still-live credential and BEFORE the run is closed. Past that point the
// worker's credential is revoked by design and it cannot write anything - which is exactly
// why prod had no terminal frame at all.
func TestD4CancelWritesTheTerminalFrameDurably(t *testing.T) {
	f := newERunState(t, erunChildTask)
	issuer := &grantRec{childToken: "MINTED-CHILD-TOK"}
	core := newSilentCore()
	c := newERunCoordinator(f, core, issuer)

	owner := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "owner-a"}
	ctx := space.WithAuthorityTask(context.Background(), erunAuthTask)
	acc, err := c.AcceptTurn(ctx, erunIngressTok, owner, space.AcceptTurnRequest{
		Key: "k-d4-durable", ContextID: "ctx-d4-durable",
		Input: json.RawMessage(`"talk until canceled"`), MaxSteps: 5,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := c.DispatchTurn(context.Background(), erunIngressTok, owner); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	select {
	case <-core.started:
	case <-time.After(5 * time.Second):
		t.Fatal("core never started")
	}
	drainReleases(f.releaseCh)
	f.reset()

	if err := c.CancelTurn(context.Background(), acc.TurnID, erunCallerTok); err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}

	var terminal []erunOp
	for _, o := range f.streamFrames() {
		if o.Kind == "status" && strings.Contains(o.Payload, "canceled") {
			terminal = append(terminal, o)
		}
	}
	if len(terminal) == 0 {
		t.Fatalf("D4: cancel left no terminal frame in the durable stream; appends=%+v", f.streamFrames())
	}
	if len(terminal) > 1 {
		t.Fatalf("a cancel must leave exactly one terminal frame, got %d: %+v", len(terminal), terminal)
	}
	if terminal[0].Bearer != "Bearer "+erunCallerTok {
		t.Fatalf("the terminal frame of a canceled run must be written with the caller's live credential, got %q", terminal[0].Bearer)
	}
	if rel := f.opPosition("Release"); rel >= 0 && terminal[0].Index > rel {
		t.Fatalf("the terminal frame must reach State BEFORE the run is closed (frame@%d, Release@%d)", terminal[0].Index, rel)
	}

	// The worker now discovers that its credential is dead: every write with it is refused,
	// exactly as real State refuses a revoked grant. That must not rewrite the user's
	// cancel as a failure, and must not scatter a second terminal frame.
	f.forbidAppends()
	select {
	case st := <-f.releaseCh:
		if st == "failed" {
			t.Fatalf("a cancel must never be rewritten as a failure: State saw %q", st)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("the worker never finished closing after its credential died")
	}
	after := 0
	for _, o := range f.streamFrames() {
		if o.Kind == "status" && strings.Contains(o.Payload, "canceled") {
			after++
		}
	}
	if after != 1 {
		t.Fatalf("exactly one durable canceled frame must survive the dying worker, got %d", after)
	}
	if st, ok := c.TurnSnapshot(acc.TurnID); !ok || st.Status != space.TurnCanceled {
		t.Fatalf("turn after the worker gave up = %v, want canceled", st.Status)
	}
}

func firstFrames(frames []sseFrame, n int) string {
	var b strings.Builder
	for i, f := range frames {
		if i >= n {
			break
		}
		b.WriteString(f.raw)
		b.WriteString(" | ")
	}
	return b.String()
}
