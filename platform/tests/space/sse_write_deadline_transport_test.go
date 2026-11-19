package space_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	aot "github.com/ahp-platform/ahp/pkg/otel"
)

// This fixture substitutes only State's HTTP peer. The HTTP server, Space
// handler, scoped-task boundary and production OTel wrapper are real. No core,
// model, SDK, database or native engine is started.
type sseDeadlineFixture struct {
	state         *mockState
	handler       http.Handler
	readStarted   chan struct{}
	readCancelled chan struct{}
	release       chan struct{}
	releaseOnce   sync.Once
}

func newSSEDeadlineFixture(t *testing.T) *sseDeadlineFixture {
	t.Helper()
	f := &sseDeadlineFixture{
		state: newMockState(), readStarted: make(chan struct{}),
		readCancelled: make(chan struct{}), release: make(chan struct{}),
	}
	f.state.threads["deadline-thread"] = &threadRecord{
		ID: "deadline-thread", ContextID: "deadline-context", Owner: "Bearer tok-alice",
	}
	f.state.runs["deadline-run"] = &runRecord{
		ID: "deadline-run", ThreadID: "deadline-thread", Status: "running", Sequence: 1,
	}
	peer := f.state.Handler()
	var firstRead sync.Once
	var firstCancel sync.Once
	stateServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/state/ReadStream" {
			// Actual StateAPI transport.ReadJSON consumes the complete body before
			// entering store.ReadStream. HTTP/1 starts its disconnect reader only
			// after body EOF; blocking before it would test a different boundary.
			body, readErr := io.ReadAll(io.LimitReader(r.Body, (64<<10)+1))
			closeErr := r.Body.Close()
			if readErr != nil || closeErr != nil || len(body) > 64<<10 {
				http.Error(w, "State fixture request body rejected", http.StatusBadRequest)
				return
			}
			r = r.Clone(r.Context())
			r.Body = io.NopCloser(bytes.NewReader(body))
			firstRead.Do(func() { close(f.readStarted) })
			select {
			case <-f.release:
			case <-r.Context().Done():
				firstCancel.Do(func() { close(f.readCancelled) })
				return
			}
		}
		peer.ServeHTTP(w, r)
	}))
	t.Cleanup(stateServer.Close)
	t.Cleanup(f.allowRead)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(stateServer.URL, stateServer.Client()),
	})
	auth := func(next http.Handler) http.Handler {
		return authPerSubject(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			subject := r.Header.Get("X-Test-Subject")
			if subject != "" {
				r = r.WithContext(space.WithScopedTask(r.Context(), "tok-"+subject))
			}
			next.ServeHTTP(w, r)
		}))
	}
	f.handler = aot.HTTPMiddleware("space")(space.NewHandler(space.HandlerConfig{
		Coordinator: coord, Auth: auth, RequireScopedTask: true,
	}))
	return f
}

func (f *sseDeadlineFixture) allowRead() {
	f.releaseOnce.Do(func() { close(f.release) })
}

func (f *sseDeadlineFixture) finish() {
	f.state.mu.Lock()
	f.state.streams = []streamItem{{
		ID: "late-event", ThreadID: "deadline-thread", Key: "late-key",
		Kind: "model_delta", Sequence: 8, Payload: json.RawMessage(`{"text":"late-payload"}`),
	}}
	f.state.runs["deadline-run"].Status = "done"
	f.state.mu.Unlock()
	f.allowRead()
}

func sseDeadlineServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	s := httptest.NewUnstartedServer(handler)
	s.Config.WriteTimeout = 100 * time.Millisecond
	s.Start()
	t.Cleanup(s.Close)
	return s
}

func sseDeadlineRequest(t *testing.T, ctx context.Context, method, url string, body io.Reader) *http.Request {
	t.Helper()
	r, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Test-Subject", "alice")
	r.Header.Set("Authorization", "Bearer browser-token-not-a-state-token")
	return r
}

func awaitSSEDeadlineSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// RED on unchanged f739: headers are flushed promptly, but a durable event
// becoming available beyond the server's absolute WriteTimeout cannot be sent.
// Success requires the exact late event, a native Space end frame and clean EOF.
func TestSSEWriteDeadlineLateFrameSurvivesServerTimeout(t *testing.T) {
	f := newSSEDeadlineFixture(t)
	s := sseDeadlineServer(t, f.handler)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := sseDeadlineRequest(t, ctx, http.MethodGet, s.URL+"/v1/space/stream/deadline-thread", nil)
	resp, err := s.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("SSE status=%d", resp.StatusCode)
	}
	awaitSSEDeadlineSignal(t, f.readStarted, "State ReadStream")
	// The delay is deliberately greater than WriteTimeout; it is NOT a request
	// or model timeout. The channel makes event availability deterministic.
	time.Sleep(250 * time.Millisecond)
	f.finish()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil || !bytes.Contains(body, []byte("late-payload")) ||
		!bytes.Contains(body, []byte("id: 8\nevent: model_delta\n")) ||
		!bytes.Contains(body, []byte("event: end\n")) ||
		!bytes.Contains(body, []byte(`"status":"done"`)) {
		t.Fatalf("SSE late frame missing after server WriteTimeout: clean_eof=%t late=%t end=%t",
			readErr == nil, bytes.Contains(body, []byte("late-payload")), bytes.Contains(body, []byte("event: end\n")))
	}
}

func TestSSEWriteDeadlineOtherRouteRetainsServerTimeout(t *testing.T) {
	f := newSSEDeadlineFixture(t)
	s := sseDeadlineServer(t, f.handler)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := sseDeadlineRequest(t, ctx, http.MethodPost, s.URL+"/v1/space/ReadStream",
		strings.NewReader(`{"thread_id":"deadline-thread","after":0,"limit":2}`))
	r.Header.Set("Content-Type", "application/json")
	type answer struct{ delivered bool }
	result := make(chan answer, 1)
	go func() {
		resp, err := s.Client().Do(r)
		if err != nil {
			result <- answer{}
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		result <- answer{err == nil && resp.StatusCode == 200 && bytes.Contains(body, []byte("late-payload"))}
	}()
	awaitSSEDeadlineSignal(t, f.readStarted, "ordinary ReadStream")
	time.Sleep(250 * time.Millisecond)
	f.finish()
	select {
	case got := <-result:
		if got.delivered {
			t.Fatal("non-SSE route bypassed server WriteTimeout")
		}
	case <-ctx.Done():
		t.Fatal("ordinary route did not terminate")
	}
}

func TestSSEWriteDeadlineDisconnectCancelsStateRead(t *testing.T) {
	f := newSSEDeadlineFixture(t)
	done := make(chan struct{})
	s := sseDeadlineServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		f.handler.ServeHTTP(w, r)
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := sseDeadlineRequest(t, ctx, http.MethodGet, s.URL+"/v1/space/stream/deadline-thread", nil)
	resp, err := s.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	awaitSSEDeadlineSignal(t, f.readStarted, "State ReadStream")
	cancel()
	resp.Body.Close()
	awaitSSEDeadlineSignal(t, f.readCancelled, "upstream State cancellation")
	awaitSSEDeadlineSignal(t, done, "Space handler return")
}

// A socket write budget applies only while bytes are being written. Waiting
// for the next State page must have no pending deadline, including after the
// initial headers were flushed through the production OTel wrapper.
func TestSSEWriteDeadlineIdleTailClearsSocketDeadline(t *testing.T) {
	f := newSSEDeadlineFixture(t)
	w := &sseDeadlineObserver{ResponseRecorder: httptest.NewRecorder()}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := sseDeadlineRequest(t, ctx, http.MethodGet,
		"http://space.invalid/v1/space/stream/deadline-thread", nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.handler.ServeHTTP(w, r)
	}()
	awaitSSEDeadlineSignal(t, f.readStarted, "idle State ReadStream")
	w.mu.Lock()
	idleClear := w.deadline.IsZero() && w.deadlineCalls > 0
	w.mu.Unlock()
	// Always release the handler before an assertion can stop the test.
	f.finish()
	awaitSSEDeadlineSignal(t, done, "finished SSE handler")
	w.mu.Lock()
	bounded := w.writes > 0 && w.allWritesBounded
	finalClear := w.deadline.IsZero()
	w.mu.Unlock()
	if !idleClear || !bounded || !finalClear {
		t.Fatalf("SSE deadline must be idle-clear and write-bounded: idle=%t bounded=%t final=%t", idleClear, bounded, finalClear)
	}
}

type sseDeadlineObserver struct {
	*httptest.ResponseRecorder
	mu               sync.Mutex
	deadline         time.Time
	deadlineCalls    int
	writes           int
	allWritesBounded bool
}

func (w *sseDeadlineObserver) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	w.deadline = deadline
	w.deadlineCalls++
	w.mu.Unlock()
	return nil
}

func (w *sseDeadlineObserver) Write(p []byte) (int, error) {
	w.mu.Lock()
	remaining := time.Until(w.deadline)
	bounded := !w.deadline.IsZero() && remaining > 0 && remaining <= 11*time.Second
	if w.writes == 0 {
		w.allWritesBounded = bounded
	} else {
		w.allWritesBounded = w.allWritesBounded && bounded
	}
	w.writes++
	w.mu.Unlock()
	return w.ResponseRecorder.Write(p)
}

// deadlineFailureWriter models a writer that expires a bounded socket write.
// It never waits for ten seconds and never impersonates a benchmark runtime.
type deadlineFailureWriter struct {
	*httptest.ResponseRecorder
	deadline      time.Time
	deadlineCalls int
	writes        int
	boundedWrite  bool
}

func (w *deadlineFailureWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	w.deadlineCalls++
	return nil
}

func (w *deadlineFailureWriter) Write(p []byte) (int, error) {
	w.writes++
	remaining := time.Until(w.deadline)
	w.boundedWrite = !w.deadline.IsZero() && remaining > 0 && remaining <= 11*time.Second
	return 0, os.ErrDeadlineExceeded
}

func TestSSEWriteDeadlineSlowConsumerStopsAfterWriteFailure(t *testing.T) {
	f := newSSEDeadlineFixture(t)
	f.finish()
	w := &deadlineFailureWriter{ResponseRecorder: httptest.NewRecorder()}
	r := sseDeadlineRequest(t, context.Background(), http.MethodGet,
		"http://space.invalid/v1/space/stream/deadline-thread", nil)
	f.handler.ServeHTTP(w, r)
	if !w.boundedWrite || w.writes != 1 {
		t.Fatalf("SSE must bound and stop a failed client write: bounded=%t writes=%d", w.boundedWrite, w.writes)
	}
}

func TestSSEWriteDeadlineRefusalNeverChangesDeadline(t *testing.T) {
	f := newSSEDeadlineFixture(t)
	for _, tc := range []struct {
		name, subject, suffix string
		status                int
	}{
		{"foreign", "bob", "", http.StatusNotFound},
		{"invalid-cursor", "alice", "?after=-1", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &deadlineFailureWriter{ResponseRecorder: httptest.NewRecorder()}
			r := sseDeadlineRequest(t, context.Background(), http.MethodGet,
				"http://space.invalid/v1/space/stream/deadline-thread"+tc.suffix, nil)
			r.Header.Set("X-Test-Subject", tc.subject)
			// Record refusal bodies normally; they are not SSE client writes.
			next := &sseRefusalWriter{deadlineFailureWriter: w}
			f.handler.ServeHTTP(next, r)
			if w.Code != tc.status || w.deadlineCalls != 0 || strings.Contains(w.Header().Get("Content-Type"), "text/event-stream") {
				t.Fatalf("refusal changed SSE deadline or headers: status=%d calls=%d", w.Code, w.deadlineCalls)
			}
		})
	}
}

type sseRefusalWriter struct{ *deadlineFailureWriter }

func (w *sseRefusalWriter) Write(p []byte) (int, error) {
	return w.ResponseRecorder.Write(p)
}
