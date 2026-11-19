package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// TestTracedMuxJoinsTheIncomingTrace pins the property the platform front already
// has and the runner lacked: the user-facing plane joins the caller's trace
// instead of starting its own, so a person's request can be followed across roles.
func TestTracedMuxJoinsTheIncomingTrace(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(t.Context()) }()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	var inner string
	h := tracedMux(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inner = trace.SpanContextFromContext(r.Context()).TraceID().String()
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/u-ahp/v1/space/SendMessage", nil)
	// A traceparent a previous AHP role would have produced for the same request.
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if inner != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("the runner restarted the trace instead of joining it: span trace id = %q", inner)
	}
}

// TestTracedClientInjectsActiveSpanContext is the outgoing half of the same leg.
func TestTracedClientInjectsActiveSpanContext(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(t.Context()) }()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	var got string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	client := tracedClient(upstream.Client())
	ctx, span := tp.Tracer("test").Start(t.Context(), "space-hop")
	defer span.End()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream.URL, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	_ = resp.Body.Close()

	want := span.SpanContext().TraceID().String()
	if got == "" {
		t.Fatal("no traceparent on the outgoing platform call: hops would split the trace")
	}
	if len(got) < 35 || got[3:35] != want {
		t.Fatalf("outgoing traceparent %q does not continue span trace %s", got, want)
	}
}
