package main

// obs_trace_hops_test.go -- the two hops that used to fall out of the run trace.
//
// tracedMux and tracedClient already join the caller trace and inject on the
// State, model, emil, download and cost hops. The Artifacts hop could not use
// tracedClient: the Artifacts SDK refuses a wrapped transport because it verifies
// the mTLS material on the transport itself, so that client went out with no W3C
// parent and the artifact write started a trace of its own. The certless State hop
// had the same shape by omission (buildStateClient returns nil, and
// space.NewStateClient then builds its own plain client). Both are closed here,
// and the wiring itself is pinned so the next refactor cannot quietly drop it.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const obsCanary = "CANARY-USER-PROMPT-must-not-reach-trace-headers-9b2e"

// obsTracedContext installs a real provider and returns a live span context, so
// what gets propagated is what a running turn would carry.
func obsTracedContext(t *testing.T) (context.Context, string) {
	t.Helper()
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	ctx, span := tp.Tracer("ahp.space").Start(context.Background(), "space.run")
	// span.End takes options, so its type is not func() and it cannot be handed to
	// t.Cleanup unwrapped: unwrapped, this file did not compile, which is why the
	// ROW25 negative legs had never actually run.
	t.Cleanup(func() { span.End() })
	return ctx, span.SpanContext().TraceID().String()
}

// obsUpstream records the trace headers it was handed.
func obsUpstream(t *testing.T, seen *map[string]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		(*seen)["traceparent"] = r.Header.Get("traceparent")
		(*seen)["baggage"] = r.Header.Get("baggage")
		(*seen)["tracestate"] = r.Header.Get("tracestate")
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func obsCall(t *testing.T, client *http.Client, ctx context.Context, url string) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(obsCanary))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	_ = resp.Body.Close()
}

func TestObsArtifactTraceDecoratorContinuesRunTrace(t *testing.T) {
	ctx, traceID := obsTracedContext(t)
	seen := map[string]string{}
	srv := obsUpstream(t, &seen)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/artifacts/Put", strings.NewReader(obsCanary))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	artifactTraceDecorator(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	_ = resp.Body.Close()

	got := seen["traceparent"]
	if got == "" {
		t.Fatal("the artifact hop left without a traceparent: the artifact write starts a new trace")
	}
	if !strings.HasPrefix(got, "00-"+traceID+"-") {
		t.Fatalf("traceparent = %q, want it to continue run trace %s", got, traceID)
	}
	if seen["baggage"] != "" || seen["tracestate"] != "" {
		t.Fatalf("only the validated W3C parent may leave the process, got baggage=%q tracestate=%q",
			seen["baggage"], seen["tracestate"])
	}
	for key, value := range seen {
		if strings.Contains(value, obsCanary) {
			t.Fatalf("request content reached the %q header", key)
		}
	}
}

func TestObsStateClientWithoutWorkloadCertsStillTraces(t *testing.T) {
	ctx, traceID := obsTracedContext(t)
	seen := map[string]string{}
	srv := obsUpstream(t, &seen)

	client := tracedStateClient(nil)
	if client == nil {
		t.Fatal("a certless deployment got no State client: Space would have built an untraced one")
	}
	// The defaults Space itself would have used stay in place: this only adds a
	// header, it does not re-shape the hop.
	if client.Timeout != 30*time.Second {
		t.Fatalf("State client timeout = %v, want the 30s space.NewStateClient default", client.Timeout)
	}
	obsCall(t, client, ctx, srv.URL+"/v1/state/CreateRun")
	got := seen["traceparent"]
	if !strings.HasPrefix(got, "00-"+traceID+"-") {
		t.Fatalf("State hop traceparent = %q, want it to continue run trace %s", got, traceID)
	}

	// An mTLS State client (already traced by buildStateClient) is returned as is:
	// wrapping it again would rebuild a transport that carries workload identity.
	already := &http.Client{Timeout: 12 * time.Second}
	if got := tracedStateClient(already); got != already {
		t.Fatal("tracedStateClient replaced a client that already carries workload mTLS")
	}
}

// obsPeer is a machine plane that records the WHOLE header set of every request it
// serves and counts the requests themselves. Counting is not a convenience: a leg
// that inspects only the hops it chose to record cannot fail on a hop nobody
// recorded, so completeness of observation is asserted, not assumed.
type obsPeer struct {
	mu       sync.Mutex
	headers  http.Header
	served   int
	pathSeen map[string]int
}

func (p *obsPeer) handlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		p.served++
		if p.pathSeen == nil {
			p.pathSeen = map[string]int{}
		}
		p.pathSeen[r.URL.Path]++
		p.headers = r.Header.Clone()
		p.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"artifact","version":"v1","sha256":"` + strings.Repeat("0", 64) + `","size":1}`))
	}
}

func (p *obsPeer) observed() (http.Header, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.headers == nil {
		return http.Header{}, p.served
	}
	return p.headers.Clone(), p.served
}

// obsTransportHeaderKeys are the headers net/http itself adds; they say nothing about
// what this binary put on the request.
var obsTransportHeaderKeys = map[string]bool{
	"Accept-Encoding": true, "User-Agent": true, "Content-Length": true, "Host": true,
	"Transfer-Encoding": true, "Te": true, "Connection": true, "Keep-Alive": true,
	"Proxy-Connection": true, "Upgrade": true, "Expect": true, "Trailer": true,
}

// obsAssertHeaderSet compares the complete set of headers the peer saw against the set
// this leg authorises. A decorator or a client that starts adding a header -- a second
// trace header, a credential, anything -- fails here instead of passing unnoticed.
func obsAssertHeaderSet(t *testing.T, saw http.Header, want ...string) {
	t.Helper()
	got := map[string]bool{}
	for key := range saw {
		if !obsTransportHeaderKeys[key] {
			got[key] = true
		}
	}
	expected := map[string]bool{}
	for _, key := range want {
		expected[key] = true
	}
	for key := range got {
		if !expected[key] {
			t.Fatalf("the peer saw %q, which this leg never authorised (observed %v): a header was added", key, saw)
		}
	}
	for _, key := range want {
		if !got[key] {
			t.Fatalf("the peer never saw %q: the hop stopped carrying it", key)
		}
	}
}

// TestObsArtifactClientCarriesTheTraceThroughTheProductionConstructor is the
// behavioural answer to "is the decorator still wired?". It does not read source
// text: it builds the Artifacts client with the production constructor, from the same
// environment variables a deployment sets, and watches what a real mTLS round trip
// puts on the wire. Move, rename or drop the decorator and the constructor stops
// producing a client that continues the run trace -- a pin on a string in wiring.go
// could not tell the difference while the string survived.
func TestObsArtifactClientCarriesTheTraceThroughTheProductionConstructor(t *testing.T) {
	ctx, traceID := obsTracedContext(t)
	peer := &obsPeer{}
	ca := newOwnerCA(t)
	workload := ca.clientCert(t, "spiffe://ahp/tenant/obs/deployment/obs")
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(peer.handlerFunc())
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	key, err := x509.MarshalPKCS8PrivateKey(workload.PrivateKey)
	if err != nil {
		t.Fatalf("workload key: %v", err)
	}
	dir := t.TempDir()
	for name, data := range map[string][]byte{
		"cert": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: workload.Certificate[0]}),
		"key":  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}),
		"ca":   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}),
	} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatalf("write workload %s: %v", name, err)
		}
	}
	t.Setenv("AS_ARTIFACTS_URL", srv.URL)
	t.Setenv("AS_WORKLOAD_CERT", filepath.Join(dir, "cert"))
	t.Setenv("AS_WORKLOAD_KEY", filepath.Join(dir, "key"))
	t.Setenv("AS_WORKLOAD_CA", filepath.Join(dir, "ca"))

	client, err := buildArtifactClient()
	if err != nil || client == nil {
		t.Fatalf("buildArtifactClient gave client=%v err=%v: a deployment could not reach the Artifacts owner", client, err)
	}
	view := client.WithTokenSource(func(context.Context) (string, error) { return "task-token", nil })
	if _, err := view.Get(ctx, "artifact"); err != nil {
		t.Fatalf("owner read through the production client: %v", err)
	}

	saw, served := peer.observed()
	if served != 1 {
		t.Fatalf("the owner fixture served %d requests, want exactly the one the production client made", served)
	}
	if got := saw.Get("traceparent"); !strings.HasPrefix(got, "00-"+traceID+"-") {
		t.Fatalf("the Artifacts hop left the production client with traceparent=%q, want it to continue run trace %s", got, traceID)
	}
	if saw.Get("baggage") != "" || saw.Get("tracestate") != "" {
		t.Fatalf("only the validated W3C parent may leave the process, got baggage=%q tracestate=%q",
			saw.Get("baggage"), saw.Get("tracestate"))
	}
	if saw.Get("Authorization") != "Bearer task-token" {
		t.Fatalf("the owner call carried %q, want the task credential the SDK sets after the decorator", saw.Get("Authorization"))
	}
	for _, values := range saw {
		for _, value := range values {
			if strings.Contains(value, obsCanary) {
				t.Fatalf("request content reached a header value: %q", value)
			}
		}
	}
	obsAssertHeaderSet(t, saw, "Authorization", "Content-Type", "Traceparent")
}

// TestObsStateClientCompositionCarriesTheTraceThroughARealCall is the behavioural
// answer for the certless State hop: the client is built by the production helpers
// (buildStateClient for a deployment without a workload bundle, wrapped by
// tracedStateClient exactly as the runner composes it) and a real State call is
// watched. The control half matters: the same call without the wrapper must LEAVE the
// trace, which is what makes the wrapper load-bearing rather than decorative.
func TestObsStateClientCompositionCarriesTheTraceThroughARealCall(t *testing.T) {
	ctx, traceID := obsTracedContext(t)
	peer := &obsPeer{}
	srv := httptest.NewServer(peer.handlerFunc())
	t.Cleanup(srv.Close)

	// Control: what Space would have built on its own, with no wrapper. Whether the
	// stub's answer decodes is beside the point -- the served tally below proves the
	// request left, which is the only thing this leg is about.
	_, _ = space.NewStateClient(srv.URL, nil).GetRun(ctx, "task-token", "run-control")
	if saw, served := peer.observed(); served != 1 || saw.Get("traceparent") != "" {
		t.Fatalf("the unwrapped State hop carried traceparent=%q after %d requests: the control must leave the trace, "+
			"or the wrapper proves nothing", saw.Get("traceparent"), served)
	}

	// Production composition for a deployment with no workload certificate bundle.
	stateHTTPClient, err := buildStateClient(config{})
	if err != nil {
		t.Fatalf("buildStateClient for a certless deployment: %v", err)
	}
	if stateHTTPClient != nil {
		t.Fatal("buildStateClient stopped returning the certless nil client; update this leg to the new contract")
	}
	_, _ = space.NewStateClient(srv.URL, tracedStateClient(stateHTTPClient)).GetRun(ctx, "task-token", "run-traced")
	saw, served := peer.observed()
	if served != 2 {
		t.Fatalf("the State stub served %d requests, want the control plus the traced call", served)
	}
	if got := saw.Get("traceparent"); !strings.HasPrefix(got, "00-"+traceID+"-") {
		t.Fatalf("the State hop left with traceparent=%q, want it to continue run trace %s", got, traceID)
	}
	if saw.Get("baggage") != "" || saw.Get("tracestate") != "" {
		t.Fatalf("only the validated W3C parent may leave the process, got baggage=%q tracestate=%q",
			saw.Get("baggage"), saw.Get("tracestate"))
	}
	obsAssertHeaderSet(t, saw, "Authorization", "Content-Type", "Traceparent")
}
