// Command otlp-receiver is a loopback OTLP/gRPC TraceService receiver used by
// acceptance runs: it proves the RUNNING stand exports real spans, and names the
// trace each span belongs to, without a docker collector image and without any
// egress (bound to 127.0.0.1).
//
// It prints one JSON line per received span (service, name, trace/span/parent ids,
// attributes) so an operator can grep whether one user action produced ONE trace
// across roles. It deliberately prints attributes only: AHP spans carry method,
// path, status and duration -- never payload content -- and this tool keeps that
// property visible instead of hiding it.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc"
)

type receiver struct {
	coltracepb.UnimplementedTraceServiceServer
	out *json.Encoder
}

func (r *receiver) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	spans := 0
	for _, rs := range req.GetResourceSpans() {
		service := ""
		for _, kv := range rs.GetResource().GetAttributes() {
			if kv.GetKey() == "service.name" {
				service = attrString(kv.GetValue())
			}
		}
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				attrs := map[string]any{}
				for _, kv := range sp.GetAttributes() {
					attrs[kv.GetKey()] = attrJSON(kv.GetValue())
				}
				_ = r.out.Encode(map[string]any{
					"service":   service,
					"name":      sp.GetName(),
					"trace_id":  hex.EncodeToString(sp.GetTraceId()),
					"span_id":   hex.EncodeToString(sp.GetSpanId()),
					"parent_id": hex.EncodeToString(sp.GetParentSpanId()),
					"kind":      sp.GetKind().String(),
					"attrs":     attrs,
				})
				spans++
			}
		}
	}
	fmt.Fprintf(os.Stderr, "[otlp-receiver] export spans=%d\n", spans)
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func attrString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	if s := v.GetStringValue(); s != "" {
		return s
	}
	return fmt.Sprintf("%v", v.GetValue())
}

func attrJSON(v *commonpb.AnyValue) any {
	if v == nil {
		return nil
	}
	switch {
	case v.GetStringValue() != "":
		return v.GetStringValue()
	case v.GetIntValue() != 0:
		return v.GetIntValue()
	case v.GetBoolValue():
		return true
	}
	return fmt.Sprintf("%v", v.GetValue())
}

func main() {
	addr := flag.String("addr", "127.0.0.1:4317", "loopback OTLP/gRPC listen address")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "otlp-receiver: listen %s: %v\n", *addr, err)
		os.Exit(1)
	}
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, &receiver{out: json.NewEncoder(os.Stdout)})
	fmt.Fprintf(os.Stderr, "[otlp-receiver] listening on %s\n", *addr)
	if err := srv.Serve(lis); err != nil {
		fmt.Fprintf(os.Stderr, "otlp-receiver: serve: %v\n", err)
		os.Exit(1)
	}
}
