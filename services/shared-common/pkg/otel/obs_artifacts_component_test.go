package otel

// obs_artifacts_component_test.go -- the trace must name the leg it is on.
//
// boundedComponent is the only place a span says which plane served the request,
// so an allowlist that does not know /v1/artifacts/ makes the artifact write --
// the step that produced the file a person is looking at -- appear as whichever
// role happens to serve it (data/all). That is not a cosmetic gap: an operator
// reading the trace cannot tell the artifact hop from an unrelated call on the
// same role, and the row that demands a trace through State/Artifacts cannot be
// closed by a span whose component attribute is wrong.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestObsArtifactsLegIsNamedInTheTrace(t *testing.T) {
	exp := &memExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)

	const secretish = "artifact-tenant-acme-conv-01J9XYZ-content-handle"
	mw := HTTPMiddleware("data")
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	// A real artifact op path, with a high-cardinality handle in the query: the
	// handle is an identifier, not content, and even it must not become an
	// attribute -- the middleware records the path without the query.
	req := httptest.NewRequest(http.MethodPost, "/v1/artifacts/Put?artifact_id="+secretish, nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	exp.mu.Lock()
	defer exp.mu.Unlock()
	if len(exp.spans) != 1 {
		t.Fatalf("captured %d spans, want exactly 1", len(exp.spans))
	}
	attrs := spanAttrs(exp.spans[0])
	if got := attrs["ahp.component"].AsString(); got != "artifacts" {
		t.Fatalf("ahp.component = %q, want artifacts: the artifact hop is unidentifiable in the trace (name = %q)",
			got, exp.spans[0].Name())
	}
	if got := attrs["ahp.operation"].AsString(); got != "request" {
		t.Fatalf("ahp.operation = %q, want the bounded request: op names are a fixed allowlist", got)
	}
	if got := exp.spans[0].Name(); got != "POST /v1/artifacts/*" {
		t.Fatalf("span name = %q, want POST /v1/artifacts/*", got)
	}
	for _, kv := range exp.spans[0].Attributes() {
		if strings.Contains(kv.Value.AsString(), secretish) {
			t.Fatalf("attribute %q carried the request identifier %q", kv.Key, secretish)
		}
	}
}
