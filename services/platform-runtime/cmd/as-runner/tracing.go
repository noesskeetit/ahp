package main

import (
	"net/http"
	"time"

	aot "github.com/ahp-platform/ahp/pkg/otel"
)

// tracedMux wraps the runner's handler in the same tracing middleware every
// other AHP role uses. It exists as a named function because the alternative --
// inlining the wrapper in the http.Server literal -- leaves the single most
// important property of the user-facing plane (it participates in the trace)
// untestable without booting a process.
func tracedMux(h http.Handler) http.Handler {
	return aot.HTTPMiddleware("space")(h)
}

// tracedClient makes every Space -> platform hop carry the active span context,
// so the browser-facing request, the Space run and the machine-plane call behind
// it land in ONE trace instead of three.
func tracedClient(c *http.Client) *http.Client {
	if c == nil {
		return nil
	}
	c.Transport = aot.Transport(c.Transport)
	return c
}

// artifactTraceDecorator is the Artifacts half of the run trace. Every other
// platform hop propagates by wrapping the http.Transport (tracedClient); the
// Artifacts SDK cannot accept that wrapper, because it inspects the transport
// itself to prove verified mTLS and a wrapper hides the material it must verify
// (sdk/go/artifacts/client.go). So the SDK takes a request decorator, and this is
// the product implementation of it: the validated W3C parent and nothing else.
// No baggage, no tracestate, and never a byte of artifact or prompt content.
func artifactTraceDecorator(r *http.Request) {
	if r == nil {
		return
	}
	if tp := aot.TraceParent(r.Context()); tp != "" {
		r.Header.Set("traceparent", tp)
	}
}

// tracedStateClient keeps the Space -> State hop inside the run trace for a
// deployment that configured no workload certificate bundle. Credentials do not
// change: Space would have handed State an ordinary client either way (see the
// default inside space.NewStateClient), and the only header added here is the W3C
// parent. A client that already carries workload mTLS is already traced by
// buildStateClient and is returned untouched.
func tracedStateClient(c *http.Client) *http.Client {
	if c != nil {
		return c
	}
	return tracedClient(&http.Client{Timeout: 30 * time.Second})
}
