package otel

import (
	"net/http"

	"go.opentelemetry.io/otel/propagation"
)

// Transport wraps a RoundTripper so outgoing requests carry the active span
// context. Without it a call that leaves this process starts a NEW trace on the
// far side: HTTPMiddleware already Extracts an incoming traceparent, so the
// missing half was injection. That is the difference between per-service spans
// and the end-to-end trace the platform requires.
//
// The request is cloned rather than mutated: RoundTrip must not modify the
// request it was handed, and the caller may reuse it.
func Transport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &injectingRoundTripper{base: base}
}

type injectingRoundTripper struct{ base http.RoundTripper }

func (t *injectingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header.Del("baggage")
	clone.Header.Del("tracestate")
	propagation.TraceContext{}.Inject(clone.Context(), propagation.HeaderCarrier(clone.Header))
	return t.base.RoundTrip(clone)
}
