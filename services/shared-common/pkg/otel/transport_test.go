package otel

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// TestTransportContinuesTraceAcrossProcess pins both halves of cross-process
// continuity with the real propagator: a server span, an outgoing call made
// inside that span, and a wrapped server that must join -- not restart -- the
// trace. Comparing trace ids (not mere header presence) is what separates
// propagation from decoration: a header carrying another trace id would satisfy
// a presence check and still leave two unrelated traces.
func TestTransportContinuesTraceAcrossProcess(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(t.Context()) }()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	var upstreamTraceID string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamTraceID = traceParentTraceID(r.Header.Get("traceparent"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	var handlerTraceID string
	served := HTTPMiddleware("under-test")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		client := &http.Client{Transport: Transport(http.DefaultTransport)}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream.URL, nil)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("outgoing call: %v", err)
		}
		_ = resp.Body.Close()
		handlerTraceID = trace.SpanContextFromContext(ctx).TraceID().String()
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	served.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/space/x", nil))

	if handlerTraceID == "" {
		t.Fatal("no span context reached the handler: the middleware is not wired")
	}
	if upstreamTraceID == "" {
		t.Fatal("the outgoing request carried no traceparent: injection is not wired")
	}
	if upstreamTraceID != handlerTraceID {
		t.Fatalf("the hop broke the trace: upstream sees %s, handler span is %s", upstreamTraceID, handlerTraceID)
	}
}

// traceParentTraceID returns the 32-hex trace id of a w3c traceparent value.
func traceParentTraceID(v string) string {
	if len(v) < 55 {
		return ""
	}
	return v[3:35]
}
