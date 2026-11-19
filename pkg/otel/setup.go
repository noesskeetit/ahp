// Package otel configures OpenTelemetry tracing for AHP services.
package otel

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Config holds OTel initialization parameters.
type Config struct {
	// ServiceName identifies this service in traces (e.g. "state", "identity").
	ServiceName string
	// Endpoint is the OTLP gRPC collector endpoint. When empty, spans are
	// exported as JSON to stderr (development/offline mode) via an in-house
	// dependency-free exporter (no external stdout-exporter module is required).
	Endpoint string
	// Insecure disables TLS on the OTLP connection.
	Insecure bool
	// SampleRatio controls head sampling (0.0–1.0). Default 1.0.
	SampleRatio float64
}

// Shutdown flushes pending spans and stops the exporter.
type Shutdown func(ctx context.Context) error

// Setup configures the global tracer provider and propagators. Returns a
// shutdown function that must be called on process exit.
func Setup(ctx context.Context, cfg Config) (Shutdown, error) {
	if cfg.SampleRatio <= 0 {
		cfg.SampleRatio = 1.0
	}

	// Tag our resource with an empty schema URL: the SDK default resource
	// already carries its own semconv schema URL, and resource.Merge rejects a
	// mismatch between two non-empty, different URLs. service.name is stable
	// across semconv versions, so passing "" keeps the merge valid regardless of
	// the exact SDK/semconv pair in go.mod.
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			"",
			semconv.ServiceName(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: resource: %w", err)
	}

	var exporter sdktrace.SpanExporter
	if cfg.Endpoint != "" {
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint)}
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exporter, err = otlptracegrpc.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("otel: otlp exporter: %w", err)
		}
	} else {
		exporter = newJSONExporter(os.Stderr)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}

// Tracer returns a named tracer from the global provider.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}
