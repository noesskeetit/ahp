package otel

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// jsonExporter is a dependency-free sdktrace.SpanExporter that writes one JSON
// object per span to w. It is used for local/offline development when no OTLP
// collector is reachable (no endpoint configured, or no network egress): it
// preserves verifiable trace evidence without pulling in an external exporter
// module. It is safe for concurrent use by the batch processor.
type jsonExporter struct {
	mu sync.Mutex
	enc *json.Encoder
}

func newJSONExporter(w io.Writer) *jsonExporter {
	return &jsonExporter{enc: json.NewEncoder(w)}
}

// ExportSpans implements sdktrace.SpanExporter.
func (e *jsonExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range spans {
		if err := e.enc.Encode(spanToMap(s)); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown implements sdktrace.SpanExporter. The writer is not owned here.
func (e *jsonExporter) Shutdown(context.Context) error { return nil }

// spanToMap projects a ReadOnlySpan into a small JSON-friendly map.
func spanToMap(s sdktrace.ReadOnlySpan) map[string]any {
	sc := s.SpanContext()
	out := map[string]any{
		"name":       s.Name(),
		"trace_id":   sc.TraceID().String(),
		"span_id":    sc.SpanID().String(),
		"kind":       s.SpanKind().String(),
		"start":      s.StartTime(),
		"end":        s.EndTime(),
		"status":     s.Status().Code.String(),
		"status_msg": s.Status().Description,
	}
	if p := s.Parent(); p.IsValid() {
		out["parent_span_id"] = p.SpanID().String()
	}
	attrs := s.Attributes()
	if len(attrs) > 0 {
		m := make(map[string]any, len(attrs))
		for _, kv := range attrs {
			m[string(kv.Key)] = attrValue(kv.Value)
		}
		out["attributes"] = m
	}
	return out
}

func attrValue(v attribute.Value) any {
	switch v.Type() {
	case attribute.BOOL, attribute.INT64, attribute.FLOAT64, attribute.STRING:
		return v.AsInterface()
	case attribute.BOOLSLICE, attribute.INT64SLICE, attribute.FLOAT64SLICE, attribute.STRINGSLICE:
		return v.Emit()
	default:
		return v.Emit()
	}
}
