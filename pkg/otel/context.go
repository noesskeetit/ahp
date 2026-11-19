package otel

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
)

// TraceParent serializes only the validated W3C parent, never baggage or trace state.
func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// WithTraceParent restores durable execution ancestry. An invalid or absent
// value leaves ctx unchanged, so legacy runs start a new trace naturally.
func WithTraceParent(ctx context.Context, parent string) context.Context {
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": parent})
}
