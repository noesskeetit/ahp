//go:build !windows

// Package otlptest proves the OTLP COLLECTOR TARGET leg of the OTel acceptance:
// the product's pkg/otel exporter (pkg/otel/setup.go) delivers finished spans
// over a REAL OTLP/gRPC wire to a REAL OTLP collector endpoint. This is beyond
// the offline/exporter-only proof in tests/e2e/otel-trace.mjs: the spans travel
// the actual otlptracegrpc protocol to a server that speaks the OTLP
// TraceService contract, then are asserted on the receiving side.
//
// It is deliberately Mac-safe and hermetic: the collector is an in-repo
// OTLP-compliant gRPC server bound to a 127.0.0.1 ephemeral port (no docker, no
// egress, no external collector image). The types it uses (proto/otlp
// collector/trace/v1 TraceServiceServer, trace/v1 ResourceSpans) are the SAME
// ones the otlptracegrpc client in the product depends on, so no new module is
// introduced. Delivery owns tests/ and pkg/otel; this test only exercises the
// existing exported API (aot.Setup, aot.HTTPMiddleware), so the compiled
// product stays byte-identical.
package otlptest

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	aot "github.com/ahp-platform/ahp/pkg/otel"
	tracespb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

// fakeCollector is a minimal but REAL OTLP TraceService receiver. It accepts
// Export RPCs and records the span names it actually received over the wire.
type fakeCollector struct {
	tracespb.UnimplementedTraceServiceServer

	mu    sync.Mutex
	names []string
	count int
	calls int
}

func (f *fakeCollector) Export(_ context.Context, req *tracespb.ExportTraceServiceRequest) (*tracespb.ExportTraceServiceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				f.count++
				f.names = append(f.names, sp.GetName())
			}
		}
	}
	return &tracespb.ExportTraceServiceResponse{}, nil
}

func (f *fakeCollector) snapshot() (int, []string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := append([]string(nil), f.names...)
	return f.count, cp, f.calls
}

// TestOTLPDeliversServerSpanToCollector drives a real request through the
// tracing middleware, flushes via Shutdown, and asserts the collector received
// the cardinality-bounded server span over the OTLP wire.
func TestOTLPDeliversServerSpanToCollector(t *testing.T) {
	// 1) REAL OTLP collector on a loopback ephemeral port.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	coll := &fakeCollector{}
	grpcSrv := grpc.NewServer()
	tracespb.RegisterTraceServiceServer(grpcSrv, coll)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	// 2) Point the PRODUCT exporter at the real collector endpoint.
	ctx := context.Background()
	shutdown, err := aot.Setup(ctx, aot.Config{
		ServiceName: "delivery-otlp-target-test",
		Endpoint:    lis.Addr().String(),
		Insecure:    true,
		SampleRatio: 1.0,
	})
	if err != nil {
		t.Fatalf("aot.Setup: %v", err)
	}

	// 3) Emit a real server span through the product middleware.
	mw := aot.HTTPMiddleware("state")
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/state/kv/put", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	// 4) Flush the batch processor (Shutdown exports pending spans).
	flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := shutdown(flushCtx); err != nil {
		t.Fatalf("shutdown/flush: %v", err)
	}

	// 5) Assert the collector actually received the span over OTLP.
	count, names, calls := coll.snapshot()
	if calls == 0 {
		t.Fatalf("collector received ZERO Export RPCs -- OTLP wire delivery not proven (spans=%d)", count)
	}
	if count < 1 {
		t.Fatalf("collector received 0 spans over OTLP (calls=%d) -- delivery not proven", calls)
	}
	const want = "POST /v1/state/*"
	found := false
	for _, n := range names {
		if n == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("collector spans %v do not include cardinality-bounded server span %q", names, want)
	}
	t.Logf("OTLP collector target PROVEN: %d span(s) delivered over real OTLP/gRPC in %d Export RPC(s); names=%v", count, calls, names)
}
