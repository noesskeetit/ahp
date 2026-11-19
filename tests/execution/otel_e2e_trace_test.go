//go:build !windows

package execution_test

// otel_e2e_trace_test.go -- matrix row 25 (PDF rev3.2 :276 + observability):
//
//	"W3C traceparent сквозь gateway -> as-runner -> tools/model в одном дереве
//	 трасс задачи. Атрибуты только идентификаторные; коллектор/трейсы не содержат
//	 промптов, аргументов инструментов и вывода; захват содержимого выключен;
//	 метрики конвейера отдельно."
//
// Why a REAL collector and not an in-memory spy (the directive is explicit): an
// in-memory SpanRecorder proves only that the Go code held the right objects. The
// guarantee operators deploy against is what LEAVES the process on the wire, so
// this test exports through the product exporter (aot.Setup -> otlptracegrpc) to a
// real OTLP TraceService server on a loopback ephemeral port -- the SAME OTLP
// protobuf types the product client depends on -- and byte-level asserts on the
// serialized Export request the collector actually received. No prompt, no tool
// argument, no model output may be a substring of those bytes; the identifiers the
// contract wants (task/tenant/deployment) must be.
//
// Propagation and joins use the real production surfaces only: HTTPMiddleware
// (extract + cardinality-bounded server span, Error status on 5xx), Transport
// (inject, dropping baggage/tracestate), the durable TraceParent/WithTraceParent
// ancestry the Space runner uses on cold resume, and the SAME RunTraceAttributes
// production stamps on space.run. Hermetic and Mac-safe (no docker, no egress);
// PostgreSQL-free, so it runs natively and inside the Linux gate unchanged.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	aot "github.com/ahp-platform/ahp/pkg/otel"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	tracespb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	otelE2EPromptCanary  = "CANARY-PROMPT-must-never-reach-collector-7f3a91"
	otelE2EToolArgCanary = "CANARY-TOOLARG-secret-argument-value-b21cd0"
	otelE2EOutputCanary  = "CANARY-OUTPUT-model-completion-9de442"

	otelE2ETenant = "tenant-acme"
	otelE2EDeploy = "deploy-emil-prod"
	otelE2ETask   = "task-01RUNCHILD000000000000"
	otelE2ERun    = "run-01RUNRECORD00000000000"
	otelE2EThread = "thread-01CONV0000000000000"

	// The W3C traceparent the plane in front of the gateway produced; every hop
	// below must join this trace rather than start its own.
	otelE2ETraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	otelE2EInbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
)

// otlpE2ECollector is a real OTLP TraceService receiver: it accepts Export RPCs
// over gRPC and retains the serialized request bytes plus a flattened view of the
// spans and resource attributes it actually received.
type otlpE2ECollector struct {
	tracespb.UnimplementedTraceServiceServer

	mu       sync.Mutex
	raw      [][]byte
	spans    []otlpE2ESpan
	resource []string
	calls    int
}

type otlpE2ESpan struct {
	name     string
	traceID  string
	spanID   string
	parentID string
	status   tracepb.Status_StatusCode
	attrs    map[string]string
	duration time.Duration
}

func (c *otlpE2ECollector) Export(_ context.Context, req *tracespb.ExportTraceServiceRequest) (*tracespb.ExportTraceServiceResponse, error) {
	raw, err := proto.Marshal(req) // the exact serialized export on the wire
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.raw = append(c.raw, raw)
	for _, rs := range req.GetResourceSpans() {
		for _, kv := range rs.GetResource().GetAttributes() {
			c.resource = append(c.resource, kv.GetKey()+"="+kv.GetValue().GetStringValue())
		}
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				rec := otlpE2ESpan{
					name:     sp.GetName(),
					traceID:  fmt.Sprintf("%x", sp.GetTraceId()),
					spanID:   fmt.Sprintf("%x", sp.GetSpanId()),
					parentID: fmt.Sprintf("%x", sp.GetParentSpanId()),
					status:   sp.GetStatus().GetCode(),
					attrs:    map[string]string{},
					duration: time.Duration(sp.GetEndTimeUnixNano()-sp.GetStartTimeUnixNano()) * time.Nanosecond,
				}
				for _, kv := range sp.GetAttributes() {
					rec.attrs[kv.GetKey()] = kv.GetValue().GetStringValue()
				}
				c.spans = append(c.spans, rec)
			}
		}
	}
	return &tracespb.ExportTraceServiceResponse{}, nil
}

func (c *otlpE2ECollector) snapshot() ([][]byte, []otlpE2ESpan, []string, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.raw...), append([]otlpE2ESpan(nil), c.spans...), append([]string(nil), c.resource...), c.calls
}

func TestOtelEndToEndTraceIsContentFree(t *testing.T) {
	// 1) A real OTLP collector on loopback (the product's own otlpwire contract).
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	coll := &otlpE2ECollector{}
	grpcSrv := grpc.NewServer()
	tracespb.RegisterTraceServiceServer(grpcSrv, coll)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	// 2) Point the PRODUCT exporter at it (batch processor; flush via Shutdown).
	ctx := context.Background()
	shutdown, err := aot.Setup(ctx, aot.Config{
		ServiceName: "otel-e2e-row25",
		Endpoint:    lis.Addr().String(),
		Insecure:    true,
		SampleRatio: 1.0,
	})
	if err != nil {
		t.Fatalf("aot.Setup: %v", err)
	}

	var wireMu sync.Mutex
	var wire bytes.Buffer
	note := func(b []byte) { wireMu.Lock(); wire.Write(b); wire.WriteByte('\n'); wireMu.Unlock() }

	// 3) hop tools/model. Success path returns model output; /StreamModel is the
	//    error leg that must surface as an Error-status span (pipeline errors).
	modelSrv := httptest.NewServer(aot.HTTPMiddleware("model")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		note(body)
		trace.SpanFromContext(r.Context()).SetAttributes(identAttrs()...)
		if strings.HasSuffix(r.URL.Path, "/StreamModel") {
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
			return
		}
		_, _ = io.WriteString(w, otelE2EOutputCanary)
	})))
	defer modelSrv.Close()

	// 4) hop as-runner (the Space plane: tracedMux = HTTPMiddleware("space")). It
	//    opens the durable space.run child carrying identifier attributes -- the
	//    exact production stamp -- and drives the model hop on the same trace.
	var runnerTraceParent string
	runnerSrv := httptest.NewServer(aot.HTTPMiddleware("space")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		note(body)
		note([]byte(otelE2EToolArgCanary)) // a tool argument the runner forwards to the model
		runnerTraceParent = aot.TraceParent(r.Context())

		// Durable re-join (cold resume): background context, restored ancestry.
		_, runSpan := aot.Tracer("ahp.space").Start(
			aot.WithTraceParent(context.Background(), runnerTraceParent),
			"space.run", trace.WithAttributes(
				space.RunTraceAttributes(otelE2ETenant, otelE2EDeploy, otelE2ETask, otelE2ERun, otelE2EThread)...))

		client := &http.Client{Transport: aot.Transport(http.DefaultTransport)}
		out := mustCall(t, client, r.Context(), modelSrv.URL+"/v1/model/SubmitModel", otelE2EPromptCanary+" "+otelE2EToolArgCanary, 200)
		note([]byte(out))
		// Error leg: a model hop that fails must be an observable Error span, not a silent success.
		mustCallStatus(t, client, r.Context(), modelSrv.URL+"/v1/model/StreamModel", otelE2EPromptCanary, http.StatusBadGateway)

		runSpan.End()
		_, _ = io.WriteString(w, out)
	})))
	defer runnerSrv.Close()

	// 5) hop gateway (:29612-like front). Extracts the inbound traceparent, then
	//    reaches the runner over the injecting Transport (child, same trace).
	gatewaySrv := httptest.NewServer(aot.HTTPMiddleware("gateway")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		note(body)
		client := &http.Client{Transport: aot.Transport(http.DefaultTransport)}
		out := mustCall(t, client, r.Context(), runnerSrv.URL+"/v1/space/AcceptTurn", otelE2EPromptCanary, 200)
		_, _ = io.WriteString(w, out)
	})))
	defer gatewaySrv.Close()

	// 6) Inbound call carrying the W3C traceparent the caller produced.
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, gatewaySrv.URL+"/v1/space/AcceptTurn", strings.NewReader(otelE2EPromptCanary))
	req.Header.Set("traceparent", otelE2EInbound)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("front -> gateway: %v", err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !bytes.Contains(respBody, []byte(otelE2EOutputCanary)) {
		t.Fatalf("scenario: model output never returned through the chain: %q", respBody)
	}

	// 7) Flush the batch processor so every span is on the wire before asserting.
	flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := shutdown(flushCtx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// ===================== ASSERTIONS (on the received OTLP export) ============
	raws, spans, resource, calls := coll.snapshot()
	if calls == 0 {
		t.Fatal("collector received ZERO Export RPCs -- live OTLP delivery not proven (this would be a fixture, not a trace)")
	}
	if len(spans) < 4 {
		t.Fatalf("expected >=4 spans (gateway/runner/model x2 + space.run); collector got %d", len(spans))
	}

	// (A) BYTE-LEVEL content-free: no canary may appear in any serialized export.
	for i, raw := range raws {
		for _, canary := range []string{otelE2EPromptCanary, otelE2EToolArgCanary, otelE2EOutputCanary} {
			if bytes.Contains(raw, []byte(canary)) {
				t.Fatalf("LEAK: canary %q is present in serialized OTLP export #%d (%d bytes)", canary, i, len(raw))
			}
		}
	}
	// Positive control: the canaries really travelled (HTTP bodies), so (A) is not vacuous.
	wireMu.Lock()
	wireStr := wire.String()
	wireMu.Unlock()
	for _, canary := range []string{otelE2EPromptCanary, otelE2EToolArgCanary, otelE2EOutputCanary} {
		if !strings.Contains(wireStr, canary) {
			t.Fatalf("scenario did not put %q on the wire -- content-free assert is vacuous", canary)
		}
	}

	// (B) One trace tree: every received span carries the inbound trace id.
	byName := map[string]otlpE2ESpan{}
	for _, s := range spans {
		byName[s.name] = s
		if s.traceID != otelE2ETraceID {
			t.Errorf("span %q is trace %s, not the inbound tree %s -- gateway did not join the caller's trace", s.name, s.traceID, otelE2ETraceID)
		}
	}
	for _, want := range []string{"POST /v1/space/*", "POST /v1/model/*", "space.run"} {
		if _, ok := byName[want]; !ok {
			t.Fatalf("collector did not receive span %q; got %v", want, otlpNames(spans))
		}
	}

	// (C) child span per hop: the model span descends from a parent, and the
	//     durable space.run child carries the identifier attributes production stamps.
	if byName["POST /v1/model/*"].parentID == "0000000000000000" || byName["POST /v1/model/*"].parentID == "" {
		t.Errorf("model span has no parent -- Transport did not inject the active span context")
	}
	run := byName["space.run"]
	for key, want := range map[string]string{
		"ahp.tenant": otelE2ETenant, "ahp.deployment": otelE2EDeploy,
		"ahp.task": otelE2ETask, "ahp.run": otelE2ERun, "ahp.thread": otelE2EThread,
		"ahp.component": "space", "ahp.operation": "run",
	} {
		if got := run.attrs[key]; got != want {
			t.Errorf("space.run attribute %q = %q, want %q", key, got, want)
		}
	}

	// (D) pipeline measures observable in the export: per-hop durations and at
	//     least one Error status (the failed model leg), never a silent success.
	var anyDuration bool
	var sawError bool
	for _, s := range spans {
		if s.duration > 0 {
			anyDuration = true
		}
		if s.status == tracepb.Status_STATUS_CODE_ERROR {
			sawError = true
		}
	}
	if !anyDuration {
		t.Errorf("no span carried a positive duration -- pipeline timing missing from the export")
	}
	if !sawError {
		t.Errorf("the failed model hop produced no Error-status span -- pipeline errors not observable in the trace")
	}

	// (E) resource attributes carry no credentials/tokens (defense: the resource is
	//     attached to every span, so a secret there leaks with every one).
	for _, kv := range resource {
		low := strings.ToLower(kv)
		for _, bad := range []string{"token", "secret", "bearer", "password", "authorization", "api-key", "apikey", otelE2EPromptCanary, otelE2EToolArgCanary, otelE2EOutputCanary} {
			if strings.Contains(low, bad) {
				t.Fatalf("resource attribute leaks %q: %q", bad, kv)
			}
		}
	}
}

func identAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("ahp.tenant", otelE2ETenant),
		attribute.String("ahp.deployment", otelE2EDeploy),
		attribute.String("ahp.task", otelE2ETask),
		attribute.String("ahp.step", "1"),
	}
}

func mustCall(t *testing.T, client *http.Client, ctx context.Context, url, body string, want int) string {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("call %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		t.Fatalf("call %s: status %d, want %d", url, resp.StatusCode, want)
	}
	return string(b)
}

func mustCallStatus(t *testing.T, client *http.Client, ctx context.Context, url, body string, want int) {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("call %s: %v", url, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != want {
		t.Fatalf("call %s: status %d, want %d", url, resp.StatusCode, want)
	}
}

func otlpNames(spans []otlpE2ESpan) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.name)
	}
	return out
}
