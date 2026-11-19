package otel

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// memExporter captures finished spans in-process so the middleware's span
// emission is verified without an external OTLP collector (L2 local proof).
type memExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (m *memExporter) ExportSpans(_ context.Context, ss []sdktrace.ReadOnlySpan) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.spans = append(m.spans, ss...)
	return nil
}
func (m *memExporter) Shutdown(context.Context) error { return nil }

func spanAttrs(s sdktrace.ReadOnlySpan) map[string]attribute.Value {
	out := map[string]attribute.Value{}
	for _, kv := range s.Attributes() {
		out[string(kv.Key)] = kv.Value
	}
	return out
}

// TestHTTPMiddlewareEmitsServerSpan proves the tracing middleware creates one
// server span per request, names it with a cardinality-bounded path, and
// records method/target/service/status attributes.
func TestHTTPMiddlewareEmitsServerSpan(t *testing.T) {
	exp := &memExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)

	mw := HTTPMiddleware("state")
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/state/kv/put", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("handler status = %d, want 201", rec.Code)
	}

	exp.mu.Lock()
	defer exp.mu.Unlock()
	if len(exp.spans) != 1 {
		t.Fatalf("captured %d spans, want exactly 1", len(exp.spans))
	}
	s := exp.spans[0]
	if s.SpanKind() != trace.SpanKindServer {
		t.Errorf("span kind = %v, want server", s.SpanKind())
	}
	if want := "POST /v1/state/*"; s.Name() != want {
		t.Errorf("span name = %q, want %q (high-cardinality path must be normalised)", s.Name(), want)
	}
	a := spanAttrs(s)
	if got := a["http.method"].AsString(); got != http.MethodPost {
		t.Errorf("http.method = %q, want POST", got)
	}
	if got := a["http.target"].AsString(); got != "/v1/state/kv/put" {
		t.Errorf("http.target = %q, want full path", got)
	}
	if got := a["service"].AsString(); got != "state" {
		t.Errorf("service = %q, want state", got)
	}
	if got := a["http.status_code"].AsInt64(); got != int64(http.StatusCreated) {
		t.Errorf("http.status_code = %d, want 201", got)
	}
}

// TestSetupInstallsProvider proves Setup wires a global tracer provider that
// yields usable tracers and returns a working shutdown (stdout mode, no
// collector required).
func TestSetupInstallsProvider(t *testing.T) {
	shutdown, err := Setup(context.Background(), Config{ServiceName: "ahp-test"})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	tr := Tracer("ahp-test")
	if tr == nil {
		t.Fatal("Tracer returned nil after Setup")
	}
	_, span := tr.Start(context.Background(), "probe")
	if !span.SpanContext().IsValid() {
		t.Error("span from tracer has an invalid SpanContext")
	}
	span.End()
}

// Recording a status code must not cost the handler its view of the
// connection.
//
// Measured on the live stand: with this middleware in front of the mux, the
// single-origin human edge could not flush a turn's text/event-stream - the
// reverse proxy asks for a flush through http.NewResponseController, which
// walks Unwrap and otherwise answers ErrNotSupported - so a browser watching a
// live turn got 0 bytes until the turn ended and read the origin as dead. The
// proxy was not at fault: the recorder was the only ResponseWriter the handler
// could see, and it named neither Flush nor Unwrap.
func TestHTTPMiddlewareKeepsWriterCapabilities(t *testing.T) {
	var assertErr, rcErr error
	handler := HTTPMiddleware("test")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := w.(http.Flusher); !ok {
			assertErr = errors.New("handler cannot see http.Flusher through the recorder")
		}
		w.WriteHeader(http.StatusCreated)
		// Flushing is what the reverse proxy does once the upstream headers are
		// known. It goes after WriteHeader on purpose: flushing BEFORE it would
		// legitimately commit status 200 here, and this test is about the
		// capability, not about that (correct) net/http rule.
		if err := http.NewResponseController(w).Flush(); err != nil {
			rcErr = err
		}
		_, _ = io.WriteString(w, "ok")
	}))
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/space/stream/thread-1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusCreated || string(body) != "ok" {
		t.Fatalf("response = %d %q, want 201 ok (recording must not change the answer)", resp.StatusCode, body)
	}
	if assertErr != nil {
		t.Error(assertErr)
	}
	if rcErr != nil {
		t.Errorf("http.NewResponseController(w).Flush through the recorder: %v", rcErr)
	}
}

// The same contract where it hurts: bytes must reach the client while the
// handler is still writing, not when it returns.
func TestHTTPMiddlewareStreamsBeforeTheHandlerReturns(t *testing.T) {
	const firstChunk = "data: one\n\n"
	const handlerTail = 500 * time.Millisecond

	handler := HTTPMiddleware("test")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, firstChunk)
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush: %v", err)
		}
		time.Sleep(handlerTail)
		_, _ = io.WriteString(w, "data: two\n\n")
	}))
	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/space/stream/thread-1", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	sentAt := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	buf := make([]byte, len(firstChunk))
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		t.Fatalf("read first chunk: %v", err)
	}
	if string(buf) != firstChunk {
		t.Fatalf("first chunk = %q, want %q", buf, firstChunk)
	}
	if elapsed := time.Since(sentAt); elapsed > handlerTail {
		t.Errorf("first chunk took %s, at least the %s the handler keeps running: the middleware buffered the stream", elapsed, handlerTail)
	}
}

// The recorder must pass the answer through unchanged and must not hide the
// upgrade path either.
func TestHTTPMiddlewarePassesStatusAndHijackThrough(t *testing.T) {
	var sawFlusher, sawHijacker bool
	handler := HTTPMiddleware("test")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, sawFlusher = w.(http.Flusher)
		_, sawHijacker = w.(http.Hijacker)
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, "teapot")
	}))
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/space/CancelTurn")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot || string(body) != "teapot" {
		t.Errorf("response = %d %q, want 418 teapot: recording must not substitute the answer", resp.StatusCode, body)
	}
	if !sawFlusher {
		t.Error("handler cannot see http.Flusher through the recorder")
	}
	if !sawHijacker {
		t.Error("handler cannot see http.Hijacker through the recorder")
	}

	// And when the writer underneath genuinely cannot hijack, the delegate says
	// so instead of panicking or pretending.
	rec := &statusRecorder{ResponseWriter: httptest.NewRecorder(), status: 200}
	if _, _, err := rec.Hijack(); err == nil {
		t.Error("Hijack over a non-hijackable writer returned no error")
	}
}
