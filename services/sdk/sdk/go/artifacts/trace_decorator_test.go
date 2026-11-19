package artifacts_test

// trace_decorator_test.go -- the Artifacts leg of the end-to-end trace (PDF rev
// 3.2 observability: one trace across Gateway -> Space -> Model -> State/Artifacts).
//
// The rest of the platform propagates by wrapping the http.Transport
// (pkg/otel.Transport). This SDK cannot accept that wrapper: it inspects the
// transport itself to prove verified mTLS, and a wrapper hides the material it has
// to verify. So the SDK offers a request decorator instead, and these legs pin both
// halves of the deal: the caller's trace context reaches the wire on the owner API
// and on the presigned upload, and a decorator can buy itself nothing but metadata
// -- the task credential stays the SDK's to set, and a blob PUT that gained a
// credential is refused before it leaves the process.

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	aot "github.com/ahp-platform/ahp/pkg/otel"
	sdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const traceCanary = "CANARY-ARTIFACT-BODY-must-not-reach-headers-4c1f"

// tracedContext installs a provider and returns a live span context, so what gets
// propagated is what a running agent turn would carry -- not a hand-written header.
func tracedContext(t *testing.T) (context.Context, string) {
	t.Helper()
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	ctx, span := tp.Tracer("ahp.space").Start(context.Background(), "space.run")
	// span.End takes options, so it is not a func() and cannot be handed to
	// t.Cleanup directly; handing it over unwrapped left this leg uncompilable,
	// which is how the ROW25 negative legs came to have never run.
	t.Cleanup(func() { span.End() })
	return ctx, span.SpanContext().TraceID().String()
}

// injectTrace is the production decorator shape (cmd/as-runner): the W3C parent and
// nothing else.
func injectTrace(r *http.Request) {
	if tp := aot.TraceParent(r.Context()); tp != "" {
		r.Header.Set("traceparent", tp)
	}
}

// requestTotal is the key under which the recorder keeps the number of requests the
// whole rig saw, across every fixture it is wired into.
const requestTotal = "|requests"

// obsRecorder is the observation rig shared by the fixtures in this file. It records
// header values under "<key>", the whole header set each peer saw, and a tally of every
// request it served. Completeness is part of the assertion: a leg that only inspects the
// keys it chose to record cannot see a hop nobody recorded, so each fixture counts its
// requests and each leg asserts the tally. All methods are safe from handler goroutines
// and from the leg that reads them back.
type obsRecorder struct {
	mu   sync.Mutex
	seen map[string]string
	hits map[string]int
}

func newObsRecorder() *obsRecorder {
	return &obsRecorder{seen: map[string]string{}, hits: map[string]int{}}
}

// note records one observed value.
func (r *obsRecorder) note(key, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen[key] = value
}

// count records that the fixture behind this recorder served one request.
func (r *obsRecorder) count(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hits[key]++
	r.hits[requestTotal]++
}

func (r *obsRecorder) snapshot() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]string, len(r.seen))
	for k, v := range r.seen {
		out[k] = v
	}
	return out
}

// tally reports how many requests each fixture served, plus the total under
// requestTotal. A leg that expects two hops fails on three, whoever made them.
func (r *obsRecorder) tally() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(r.hits))
	for k, v := range r.hits {
		out[k] = v
	}
	return out
}

// transportHeaderKeys are the headers net/http itself adds to an outgoing request.
// They say nothing about this SDK or its decorator, so they are subtracted before
// the header set is compared; everything left on the wire was put there by the
// caller's decorator or by this package.
var transportHeaderKeys = map[string]bool{
	"Accept-Encoding": true, "User-Agent": true, "Content-Length": true, "Host": true,
	"Transfer-Encoding": true, "Te": true, "Connection": true, "Keep-Alive": true,
	"Proxy-Connection": true, "Upgrade": true, "Expect": true, "Trailer": true,
}

// headerKeySet names every header key the peer actually received, canonicalised and
// sorted, so a leg can assert the complete set instead of a chosen subset.
func headerKeySet(h http.Header) string {
	keys := make([]string, 0, len(h))
	for key := range h {
		keys = append(keys, http.CanonicalHeaderKey(key))
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// assertDecoratorHeaderSet fails when the peer saw a header this leg never
// authorised, or lost one it required. Comparing only the keys a fixture happens to
// read is what let a new decorator header pass unnoticed.
func assertDecoratorHeaderSet(t *testing.T, recorded string, want ...string) {
	t.Helper()
	got := map[string]bool{}
	for _, key := range strings.Split(recorded, ",") {
		if key == "" || transportHeaderKeys[key] {
			continue
		}
		got[key] = true
	}
	expected := map[string]bool{}
	for _, key := range want {
		expected[key] = true
	}
	for key := range got {
		if !expected[key] {
			t.Fatalf("the peer saw %q, which this leg never authorised (observed set %q): a decorator header was added", key, recorded)
		}
	}
	for _, key := range want {
		if !got[key] {
			t.Fatalf("the peer never saw %q (observed set %q): the hop stopped carrying it", key, recorded)
		}
	}
}

// rawHeaderKeySet names every header key the peer received exactly as it arrived. It is the
// pair of headerKeySet: canonicalising is what makes a set readable, and it is also what
// hides a header smuggled in under a non-canonical spelling -- which matters, because
// net/http writes the map under whatever key it holds, so that is precisely the spelling
// that reaches the wire while a Header.Get check stays blind to it.
func rawHeaderKeySet(h http.Header) string {
	keys := make([]string, 0, len(h))
	for key := range h {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// assertCanonicallySpelledHeaders is "nothing was smuggled past a case-sensitive check",
// read from outside the process: every header the peer saw arrived spelled the way
// http.CanonicalHeaderKey spells it, so no second credential travelled beside the one the
// SDK set and no second authority beside the one it dialed.
func assertCanonicallySpelledHeaders(t *testing.T, recorded string) {
	t.Helper()
	for _, key := range strings.Split(recorded, ",") {
		if key == "" {
			continue
		}
		if key != http.CanonicalHeaderKey(key) {
			t.Fatalf("the peer saw a header named %q: a non-canonical spelling is written to the wire verbatim and is "+
				"the one spelling a canonicalising guard cannot see (observed %q)", key, recorded)
		}
	}
}

// assertRequestTally compares the requests the rig actually served against the ones
// this leg intends. Without it a leg only proves things about the hops it happened to
// record, and an extra hop stays invisible no matter what the key checks say.
func assertRequestTally(t *testing.T, got, want map[string]int) {
	t.Helper()
	for key, wantCount := range want {
		if got[key] != wantCount {
			t.Fatalf("the rig served %d requests for %q, want %d (tally %v)", got[key], key, wantCount, got)
		}
	}
	for key, seen := range got {
		if _, expected := want[key]; !expected {
			t.Fatalf("the rig served %d requests for %q, which this leg never expected (tally %v): a hop left that nobody recorded",
				seen, key, got)
		}
	}
}

// ownerFixture is the authenticated owner API: it answers the metadata call and
// echoes bytes, and it records on EVERY request which trace headers arrived -- the
// leg is about what left the process, not about one endpoint.
func ownerFixture(t *testing.T, rec *obsRecorder) (*httptest.Server, *http.Client) {
	t.Helper()
	data := []byte(traceCanary)
	sum := sha256.Sum256(data)
	return mtlsFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.count(r.URL.Path)
		rec.note(r.URL.Path+"|traceparent", r.Header.Get("traceparent"))
		rec.note(r.URL.Path+"|authorization", r.Header.Get("Authorization"))
		rec.note(r.URL.Path+"|baggage", r.Header.Get("baggage"))
		rec.note(r.URL.Path+"|tracestate", r.Header.Get("tracestate"))
		// The whole header set, not just the four keys above: the leg that claims
		// "metadata and nothing else" has to see every header that left.
		rec.note(r.URL.Path+"|headers", headerKeySet(r.Header))
		// The spelling too, not only the name: two headers can share a canonical name and
		// only one of them is the credential this SDK set.
		rec.note(r.URL.Path+"|headers_raw", rawHeaderKeySet(r.Header))
		if r.URL.Path == "/v1/artifacts/Get" {
			_ = json.NewEncoder(w).Encode(sdk.ArtifactRef{
				ID: "artifact", Version: "v1", SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data)),
			})
			return
		}
		_, _ = w.Write(data)
	}))
}

// blobFixture is the presigned destination: bytes and trace context yes, credentials
// never.
func blobFixture(t *testing.T, rec *obsRecorder) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	blob := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		rec.count("upload")
		rec.note("upload|traceparent", r.Header.Get("traceparent"))
		rec.note("upload|authorization", r.Header.Get("Authorization"))
		rec.note("upload|headers", headerKeySet(r.Header))
		rec.note("upload|headers_raw", rawHeaderKeySet(r.Header))
		w.WriteHeader(http.StatusOK)
	}))
	blob.TLS = &tls.Config{ClientAuth: tls.RequestClientCert}
	blob.StartTLS()
	t.Cleanup(blob.Close)
	return blob, &hits
}

// uploadClient mirrors the production wiring: a separate client whose transport
// carries no workload identity into the blob store.
func uploadClient(t *testing.T, blob *httptest.Server, hc *http.Client) *http.Client {
	t.Helper()
	upload := blob.Client()
	uploadTr := upload.Transport.(*http.Transport).Clone()
	uploadTr.TLSClientConfig.Certificates = hc.Transport.(*http.Transport).TLSClientConfig.Certificates
	upload.Transport = uploadTr
	return upload
}

func putArtifact(ctx context.Context, c *sdk.Client, url string) error {
	data := []byte(traceCanary)
	sum := sha256.Sum256(data)
	return c.Upload(ctx, sdk.Upload{ID: "artifact", PresignedURL: url,
		ExpectedSHA: hex.EncodeToString(sum[:]), ExpectedSize: int64(len(data))}, data)
}

const ownerGet = "/v1/artifacts/Get"

func TestOwnerAndUploadRequestsCarryTheCallersTraceContext(t *testing.T) {
	ctx, traceID := tracedContext(t)
	rec := newObsRecorder()

	srv, hc := ownerFixture(t, rec)
	blob, hits := blobFixture(t, rec)

	c, err := sdk.New(sdk.Config{
		BaseURL:          srv.URL,
		HTTPClient:       hc,
		UploadHTTPClient: uploadClient(t, blob, hc),
		TokenSource:      func(context.Context) (string, error) { return "task-token", nil },
		RequestDecorator: injectTrace,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if _, err := c.Get(ctx, "artifact"); err != nil {
		t.Fatalf("owner read: %v", err)
	}
	if err := putArtifact(ctx, c, blob.URL); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("the blob store saw %d requests, want 1", hits.Load())
	}

	seen := rec.snapshot()
	for _, key := range []string{ownerGet + "|traceparent", "upload|traceparent"} {
		got := seen[key]
		if got == "" {
			t.Fatalf("%s: that hop left without a traceparent, so the artifact leg starts its own trace: %v", key, seen)
		}
		if !strings.HasPrefix(got, "00-"+traceID+"-") {
			t.Fatalf("%s = %q does not continue the run trace %s", key, got, traceID)
		}
	}
	if seen[ownerGet+"|authorization"] != "Bearer task-token" {
		t.Fatalf("owner call carried %q, want the task credential", seen[ownerGet+"|authorization"])
	}
	if seen["upload|authorization"] != "" {
		t.Fatal("the presigned upload disclosed a credential")
	}
	for key, value := range seen {
		if strings.HasSuffix(key, "|baggage") || strings.HasSuffix(key, "|tracestate") {
			if value != "" {
				t.Fatalf("%s = %q: only the validated W3C parent may leave this process", key, value)
			}
		}
		if strings.Contains(value, traceCanary) {
			t.Fatalf("artifact content reached an observed header value (%s)", key)
		}
	}
	// Completeness of observation is part of the claim: the two checks above only
	// speak about what the recorder wrote down, so the tally of requests the rig
	// actually served is asserted too. One extra hop -- traced, credential-carrying,
	// whatever -- fails here rather than passing unnoticed.
	assertRequestTally(t, rec.tally(), map[string]int{ownerGet: 1, "upload": 1, requestTotal: 2})
	// And the header set is compared whole: a decorator that starts adding a header
	// is refused by this leg, not by the four keys a fixture happened to read.
	assertDecoratorHeaderSet(t, seen[ownerGet+"|headers"], "Authorization", "Content-Type", "Traceparent")
	assertDecoratorHeaderSet(t, seen["upload|headers"], "Traceparent")
}

// TestRequestDecoratorCannotAddCredentials is the credential half of the decorator contract:
// the owner credential belongs to the SDK, which sets it after the decorator ran, and a
// presigned PUT that gained a credential is refused before it leaves the process.
//
// The "none" case is not decoration. Without a case where the decorator adds nothing, this leg
// stays green for a guard that refuses every upload -- which is precisely how the shipped
// defect fixed in 76862a03 looked from outside: a guard nobody could tell apart from a defence.
func TestRequestDecoratorCannotAddCredentials(t *testing.T) {
	ctx, traceID := tracedContext(t)

	for _, added := range []string{"none", "authorization"} {
		t.Run(added, func(t *testing.T) {
			rec := newObsRecorder()
			srv, hc := ownerFixture(t, rec)
			ownerDials := countDials(t, hc)
			blob, hits := blobFixture(t, rec)
			upload := uploadClient(t, blob, hc)
			blobDials := countDials(t, upload)
			tc := new(traceCounter)
			callCtx := tc.context(ctx)

			c, err := sdk.New(sdk.Config{
				BaseURL:          srv.URL,
				HTTPClient:       hc,
				UploadHTTPClient: upload,
				TokenSource:      func(context.Context) (string, error) { return taskBearer, nil },
				RequestDecorator: func(r *http.Request) {
					injectTrace(r)
					if added == "authorization" {
						r.Header.Set("Authorization", "Bearer attacker-chosen-authority")
					}
				},
			})
			if err != nil {
				t.Fatalf("client: %v", err)
			}

			// Owner API: the credential is the SDK's to set, so the decorator cannot swap
			// it -- and refusing an upload must not cost the trace context either.
			if _, err := c.Get(callCtx, "artifact"); err != nil {
				t.Fatalf("owner read: %v", err)
			}
			if got := ownerDials.Load(); got != 1 {
				t.Fatalf("the owner read dialed %d times, want exactly 1", got)
			}
			seen := rec.snapshot()
			if got := seen[ownerGet+"|authorization"]; got != "Bearer "+taskBearer {
				t.Fatalf("a decorator overwrote the task credential: %q", got)
			}
			if got := seen[ownerGet+"|traceparent"]; !strings.HasPrefix(got, "00-"+traceID+"-") {
				t.Fatalf("owner call carried traceparent=%q, want it to continue run trace %s", got, traceID)
			}
			assertDecoratorHeaderSet(t, seen[ownerGet+"|headers"], "Authorization", "Content-Type", "Traceparent")

			err = putArtifact(callCtx, c, signedUploadURL(blob.URL))
			if added == "none" {
				if err != nil {
					t.Fatalf("an upload whose decorator adds no credential was refused: %v -- every "+
						"artifact export by a worker would fail the same way", err)
				}
				if got := blobDials.Load(); got != 1 {
					t.Fatalf("the control upload dialed the blob store %d times, want exactly 1", got)
				}
				if got := tc.gotConn.Load(); got != 2 {
					t.Fatalf("%d connection handoffs for the owner read and the upload, want 2: the refused "+
						"case below reads this counter, so it has to be provably live", got)
				}
				seen = rec.snapshot()
				if got := seen["upload|authorization"]; got != "" {
					t.Fatal("the control upload disclosed a credential")
				}
				assertDecoratorHeaderSet(t, seen["upload|headers"], "Traceparent")
				assertRequestTally(t, rec.tally(), map[string]int{ownerGet: 1, "upload": 1, requestTotal: 2})
				return
			}

			if code := contracts.Code(err); code != contracts.ErrInvalid {
				t.Fatalf("upload through a credential-adding decorator = %v (code %v), want INVALID", err, code)
			}
			// One request written -- the owner read. A second write would mean the refused
			// upload went out anyway, whatever this call returned.
			if got := tc.wrote.Load(); got != 1 {
				t.Fatalf("%d requests were written to a transport, want 1 (the owner read only): "+
					"the refused upload reached the wire", got)
			}
			if got := blobDials.Load(); got != 0 {
				t.Fatalf("a refused upload dialed the blob store %d times, want 0", got)
			}
			if hits.Load() != 0 {
				t.Fatalf("a refused upload still reached the blob store %d times", hits.Load())
			}
			assertRequestTally(t, rec.tally(), map[string]int{ownerGet: 1, requestTotal: 1})
		})
	}
}

// TestPresignedUploadRefusesADecoratorThatRewritesHost closes the class where a
// decorator cannot attach a credential outright but steers the blob PUT elsewhere:
// net/http honours Header["Host"] when the request's own Host field is empty, and it
// honours that field outright when it is set. Either rewrite must be refused before the
// request leaves, because a presigned URL names the only authority it may go to.
//
// The "carries the parent only" case is not decoration: without a positive control this
// leg could be green because the guard refuses everything, which is the failure it
// shipped with once -- a guard that never lets a legitimate export through looks
// exactly like a guard that stops an attack, from the outside.
func TestPresignedUploadRefusesADecoratorThatRewritesHost(t *testing.T) {
	ctx, traceID := tracedContext(t)

	for _, where := range []string{"none", "header", "field"} {
		t.Run(where, func(t *testing.T) {
			// One rig per case: the tally of each case has to describe only that case.
			rec := newObsRecorder()
			srv, hc := ownerFixture(t, rec)
			blob, hits := blobFixture(t, rec)

			c, err := sdk.New(sdk.Config{
				BaseURL:          srv.URL,
				HTTPClient:       hc,
				UploadHTTPClient: uploadClient(t, blob, hc),
				TokenSource:      func(context.Context) (string, error) { return "task-token", nil },
				RequestDecorator: func(r *http.Request) {
					injectTrace(r)
					switch where {
					case "header":
						r.Header.Set("Host", "authority-chosen-by-the-decorator.example")
					case "field":
						r.Host = "authority-chosen-by-the-decorator.example"
					}
				},
			})
			if err != nil {
				t.Fatalf("client: %v", err)
			}

			err = putArtifact(ctx, c, blob.URL)
			if where == "none" {
				// Positive control: the production wiring configures a decorator, so the
				// only thing it must not survive is a rewrite. A worker exporting an
				// artifact has to get a successful, traced upload.
				if err != nil {
					t.Fatalf("an upload whose decorator only carries the trace parent was refused: %v -- "+
						"every artifact export by a worker would fail the same way", err)
				}
				if hits.Load() != 1 {
					t.Fatalf("the blob store saw %d requests, want the one upload", hits.Load())
				}
				seen := rec.snapshot()
				if got := seen["upload|traceparent"]; !strings.HasPrefix(got, "00-"+traceID+"-") {
					t.Fatalf("the control upload carried traceparent=%q, want it to continue run trace %s", got, traceID)
				}
				assertDecoratorHeaderSet(t, seen["upload|headers"], "Traceparent")
				assertRequestTally(t, rec.tally(), map[string]int{"upload": 1, requestTotal: 1})
				return
			}

			if contracts.Code(err) != contracts.ErrInvalid {
				t.Fatalf("upload through a decorator rewriting Host in the %q form = %v, want INVALID", where, err)
			}
			if hits.Load() != 0 {
				t.Fatalf("a refused upload still reached the blob store %d times", hits.Load())
			}
			assertRequestTally(t, rec.tally(), map[string]int{requestTotal: 0})
		})
	}
}

// ---------------------------------------------------------------------------
// F3 -- the decorator may not move the destination (spec:
// reports/mac-director/F3-authority-pinning-findings-20261110.md).
//
// Config.RequestDecorator is func(*http.Request): it is handed the mutable request, so it
// can rewrite req.URL as easily as its headers. Headers are not the destination -- net/http
// dials req.URL -- so the credential guard by itself left a decorator free to steer a
// validly signed presigned PUT, or an owner call that is about to be given the task Bearer
// token, to a host the decorator chose. These legs pin the destination that actually gets
// dialed, and they pin it with a dial counter: a refusal that arrives after the request
// left would prove nothing.
// ---------------------------------------------------------------------------

const (
	// taskBearer is the task credential these fixtures hand the SDK. It must appear in no
	// error message: a refusal that quoted it would put a live credential in a log line.
	taskBearer = "task-token"
	// presignCanary stands in for the signature a blob store puts in the query of a
	// presigned URL. Same rule -- it belongs in the URL, never in an error string.
	presignCanary = "CANARY-PRESIGNED-SIGNATURE-9f2e"
	// presignCredentialCanary is the second half of what a real presigned query carries.
	presignCredentialCanary = "CANARY-PRESIGNED-CREDENTIAL-7ab4"
	// hostileAuthority is where a rewritten request would end up.
	hostileAuthority = "authority-chosen-by-the-decorator.example"
)

// countDials makes the destination observable at the one place that decides where bytes
// go: the dial. Both request paths of the SDK clone their transport, and Transport.Clone
// copies DialContext, so a counter installed before sdk.New is the counter the request is
// dialed with. That claim is not assumed -- the positive-control legs below require exactly
// one dial, so a counter the clone lost would read zero where a request really left, and
// the leg would fail instead of reporting a quiet green.
func countDials(t *testing.T, hc *http.Client) *atomic.Int64 {
	t.Helper()
	tr, ok := hc.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("fixture transport is %T, want *http.Transport: the dial counter belongs on the transport the SDK clones", hc.Transport)
	}
	dial := tr.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	var dials atomic.Int64
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return dial(ctx, network, address)
	}
	return &dials
}

// traceCounter counts the moments that only exist once the SDK has handed a request to the
// transport: being given a connection, and having written the request. It is the per-request
// view -- a dial counter on its own could read zero for a request carried by a pooled
// connection -- so the refusal legs read both, and neither one inspects the request, so a
// decorator cannot hide a handoff by changing something on it.
type traceCounter struct {
	gotConn atomic.Int64
	wrote   atomic.Int64
}

// context returns a context whose httptrace feeds this counter. The SDK builds every request
// from the caller's context, so the trace rides along to the transport without the SDK having
// to know about it.
func (c *traceCounter) context(ctx context.Context) context.Context {
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn:      func(httptrace.GotConnInfo) { c.gotConn.Add(1) },
		WroteRequest: func(httptrace.WroteRequestInfo) { c.wrote.Add(1) },
	})
}

// signedUploadURL is a presigned destination as a blob store issues one: path and signature
// in the query, so the refusal legs can prove an error string drops both.
func signedUploadURL(base string) string {
	return base + "/blobs/artifact?X-Amz-Credential=" + presignCredentialCanary + "&X-Amz-Signature=" + presignCanary
}

// assertRefusedBeforeDial is the shape every decoration-refusal leg shares: not one dial and
// not one connection handed over, a typed INVALID that names the guard which fired, and
// neither the credential nor the signature inside the message a caller will log.
//
// The phrase is matched, not just the code, because any other INVALID would hide the absence
// of the guard: an upload refused for its shape and an upload refused for a smuggled
// credential are different findings, and a leg that accepts either proves neither.
func assertRefusedBeforeDial(t *testing.T, err error, phrase string, counters map[string]*atomic.Int64) {
	t.Helper()
	// Counters are read first on purpose. Everything else a refusal can get right --
	// typed, well-worded, secret-free -- is worthless if the request already left, and a
	// leg that reports the wording first hides that fact behind a message.
	for name, counter := range counters {
		if got := counter.Load(); got != 0 {
			t.Fatalf("%s = %d: the request reached the transport, so whatever this error says, the bytes went where the decorator moved them", name, got)
		}
	}
	if err == nil {
		t.Fatal("the SDK sent a request the decorator had rewritten")
	}
	if code := contracts.Code(err); code != contracts.ErrInvalid {
		t.Fatalf("a rewritten request = %v (code %v), want INVALID", err, code)
	}
	if !strings.Contains(err.Error(), phrase) {
		t.Fatalf("refused for some other reason (%v): this leg asserts the %q guard, so a different INVALID does not count as evidence", err, phrase)
	}
	for _, secret := range []string{taskBearer, presignCanary, presignCredentialCanary} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("the refusal leaked %q into an error string that a caller will log: %v", secret, err)
		}
	}
}

// assertAuthorityRefusal pins what refusing a moved destination has to look like: a typed
// INVALID naming the authority guard specifically, carrying neither the credential nor the
// signature, and not one dial to the authority the decorator chose.
func assertAuthorityRefusal(t *testing.T, err error, counters map[string]*atomic.Int64) {
	t.Helper()
	assertRefusedBeforeDial(t, err, "change request authority", counters)
}

// assertRewriteRefusal pins the same shape for a request whose path or query moved while its
// authority stayed: refused before the dial, named as a rewrite, and NOT reported as an
// unknown outcome -- an answer cannot be unknown for a request that was never sent, and a
// caller told "Seal outcome unknown" goes on to reconcile an operation it did not perform.
func assertRewriteRefusal(t *testing.T, err error, counters map[string]*atomic.Int64) {
	t.Helper()
	assertRefusedBeforeDial(t, err, "rewrite the request", counters)
	if sdk.IsOutcomeUnknown(err) {
		t.Fatalf("a request that was never sent is reported as an unknown outcome (%v): the caller would go and reconcile "+
			"an operation it did not perform, which is how a rewrite turns into a duplicate write", err)
	}
}

func assertNoCredentialEcho(t *testing.T, errs ...error) {
	t.Helper()
	for _, err := range errs {
		if err != nil && strings.Contains(err.Error(), smuggledCanary) {
			t.Fatalf("the refusal quoted the header value the decorator chose (%v): attacker-chosen text does not belong "+
				"where a caller reads why a call failed", err)
		}
	}
}

// TestRequestDecoratorCannotMoveTheOwnerCallToAnotherAuthority is the owner half of F3. The
// decorator runs before the SDK sets Authorization, which was introduced to stop it swapping
// the task authority; a URL rewrite defeats the same ordering by another route -- the call
// still gets the Bearer token, it just gets it at the wrong host.
func TestRequestDecoratorCannotMoveTheOwnerCallToAnotherAuthority(t *testing.T) {
	ctx, traceID := tracedContext(t)

	for _, moved := range []string{"none", "host", "scheme", "field"} {
		t.Run(moved, func(t *testing.T) {
			rec := newObsRecorder()
			srv, hc := ownerFixture(t, rec)
			ownerDials := countDials(t, hc)
			blob, hits := blobFixture(t, rec)
			upload := uploadClient(t, blob, hc)
			blobDials := countDials(t, upload)
			tc := new(traceCounter)
			callCtx := tc.context(ctx)

			c, err := sdk.New(sdk.Config{
				BaseURL:          srv.URL,
				HTTPClient:       hc,
				UploadHTTPClient: upload,
				TokenSource:      func(context.Context) (string, error) { return taskBearer, nil },
				RequestDecorator: func(r *http.Request) {
					injectTrace(r)
					switch moved {
					case "host":
						r.URL.Host = hostileAuthority
					case "scheme":
						r.URL.Scheme = "http"
					case "field":
						// ART-F3d: neither the URL nor the header map. net/http takes the
						// Host: line from this FIELD, in preference to the URL
						// (net/http/request.go:601-605), so a decorator that sets it moves the
						// request line while the connection, the verified mTLS peer and the
						// task Bearer all stay correct -- behind any vhost or reverse proxy
						// that is another destination, reached with the right credentials.
						r.Host = hostileAuthority
					}
				},
			})
			if err != nil {
				t.Fatalf("client: %v", err)
			}

			_, err = c.Get(callCtx, "artifact")
			if moved == "none" {
				// Positive control, and the proof that the dial counters work: the
				// production wiring configures a decorator, so an unmodified request has to
				// reach the owner exactly once and stay traced.
				if err != nil {
					t.Fatalf("an owner call whose decorator only carries the trace parent was refused: %v -- "+
						"every artifact read by a worker would fail the same way", err)
				}
				if got := ownerDials.Load(); got != 1 {
					t.Fatalf("the owner call dialed %d times, want exactly 1 -- the refusal legs read this counter, "+
						"and a counter that did not survive Transport.Clone reads zero for a request that really left", got)
				}
				if got := blobDials.Load(); got != 0 {
					t.Fatalf("an owner read dialed the blob store %d times, want 0", got)
				}
				if got := tc.gotConn.Load(); got != 1 {
					t.Fatalf("the owner call was handed to the transport %d times, want exactly 1: this is the per-request counter every refusal leg reads, so a zero here would mean those legs are reading nothing", got)
				}
				if got := tc.wrote.Load(); got != 1 {
					t.Fatalf("the owner call wrote %d requests, want exactly 1", got)
				}
				seen := rec.snapshot()
				if got := seen[ownerGet+"|authorization"]; got != "Bearer "+taskBearer {
					t.Fatalf("owner call carried %q, want the task credential", got)
				}
				if got := seen[ownerGet+"|traceparent"]; !strings.HasPrefix(got, "00-"+traceID+"-") {
					t.Fatalf("owner call carried traceparent=%q, want it to continue run trace %s", got, traceID)
				}
				assertDecoratorHeaderSet(t, seen[ownerGet+"|headers"], "Authorization", "Content-Type", "Traceparent")
				assertRequestTally(t, rec.tally(), map[string]int{ownerGet: 1, requestTotal: 1})
				return
			}

			assertAuthorityRefusal(t, err, map[string]*atomic.Int64{
				"owner dials": ownerDials, "blob dials": blobDials,
				"owner connection handoffs": &tc.gotConn, "owner request writes": &tc.wrote,
			})
			if hits.Load() != 0 {
				t.Fatalf("the blob store saw %d requests during a refused owner call, want 0", hits.Load())
			}
			// The leak this finding is about: no peer recorded a task credential, because no
			// peer was reached at all. Reading the recorded value beats reading the error text
			// -- a refusal that mentioned the token would still be a refusal.
			for key, value := range rec.snapshot() {
				if strings.HasSuffix(key, "|authorization") && value != "" {
					t.Fatalf("%s recorded %q during a refused owner call: the task credential left the process", key, value)
				}
			}
			assertRequestTally(t, rec.tally(), map[string]int{requestTotal: 0})
		})
	}
}

// TestPresignedUploadCannotBeMovedToAnotherAuthorityByTheDecorator is the upload half of F3:
// the signature in the presigned URL is valid for one authority, and it is the only
// credential that PUT carries. Moving the request does not need any header at all.
func TestPresignedUploadCannotBeMovedToAnotherAuthorityByTheDecorator(t *testing.T) {
	ctx, traceID := tracedContext(t)

	for _, moved := range []string{"none", "host", "scheme"} {
		t.Run(moved, func(t *testing.T) {
			rec := newObsRecorder()
			srv, hc := ownerFixture(t, rec)
			ownerDials := countDials(t, hc)
			blob, hits := blobFixture(t, rec)
			upload := uploadClient(t, blob, hc)
			blobDials := countDials(t, upload)
			tc := new(traceCounter)
			callCtx := tc.context(ctx)

			c, err := sdk.New(sdk.Config{
				BaseURL:          srv.URL,
				HTTPClient:       hc,
				UploadHTTPClient: upload,
				TokenSource:      func(context.Context) (string, error) { return taskBearer, nil },
				RequestDecorator: func(r *http.Request) {
					injectTrace(r)
					switch moved {
					case "host":
						r.URL.Host = hostileAuthority
					case "scheme":
						r.URL.Scheme = "http"
					}
				},
			})
			if err != nil {
				t.Fatalf("client: %v", err)
			}

			err = putArtifact(callCtx, c, signedUploadURL(blob.URL))
			if moved == "none" {
				// Positive control: a signed URL keeps a query, so a guard written against
				// the whole URL would refuse every export in the field. Scheme and authority
				// are what has to match; a signature is allowed to be there.
				if err != nil {
					t.Fatalf("an upload whose decorator only carries the trace parent was refused: %v -- "+
						"every artifact export by a worker would fail the same way", err)
				}
				if got := blobDials.Load(); got != 1 {
					t.Fatalf("the upload dialed the blob store %d times, want exactly 1 -- see countDials: this leg "+
						"is what proves the counters the refusal legs rely on actually work", got)
				}
				if got := ownerDials.Load(); got != 0 {
					t.Fatalf("a presigned upload dialed the owner API %d times, want 0: the upload path carries no task credential and must not gain a hop", got)
				}
				if hits.Load() != 1 {
					t.Fatalf("the blob store saw %d requests, want the one upload", hits.Load())
				}
				if got := tc.gotConn.Load(); got != 1 {
					t.Fatalf("the upload was handed to the transport %d times, want exactly 1: the refusal legs read this counter, so it has to be provably live", got)
				}
				if got := tc.wrote.Load(); got != 1 {
					t.Fatalf("the upload wrote %d requests, want exactly 1", got)
				}
				seen := rec.snapshot()
				if got := seen["upload|traceparent"]; !strings.HasPrefix(got, "00-"+traceID+"-") {
					t.Fatalf("the control upload carried traceparent=%q, want it to continue run trace %s", got, traceID)
				}
				if got := seen["upload|authorization"]; got != "" {
					t.Fatal("the presigned upload disclosed a credential")
				}
				assertDecoratorHeaderSet(t, seen["upload|headers"], "Traceparent")
				assertRequestTally(t, rec.tally(), map[string]int{"upload": 1, requestTotal: 1})
				return
			}

			assertAuthorityRefusal(t, err, map[string]*atomic.Int64{
				"blob dials": blobDials, "owner dials": ownerDials,
				"upload connection handoffs": &tc.gotConn, "upload request writes": &tc.wrote,
			})
			if hits.Load() != 0 {
				t.Fatalf("the blob store saw %d requests during a refused upload, want 0", hits.Load())
			}
			assertRequestTally(t, rec.tally(), map[string]int{requestTotal: 0})
		})
	}
}

// ---------------------------------------------------------------------------
// ART-F3c, ART-F1edge, ART-F4. What these three have in common is that each is a claim
// about the GUARD being right, not merely present: a guard that pins half of a destination
// lets the other half move, a guard that fires on the shape of a legitimate URL is
// indistinguishable from one that stopped an attack, and a guard that reads one spelling of
// a header misses the credential in every other. Every leg below therefore carries a case
// that must go through.
// ---------------------------------------------------------------------------

// TestRequestDecoratorCannotRewriteTheOwnerOperation is ART-F3c on the owner path. Pinning
// Scheme and Host pins the building, not the door: every operation lives on the same
// authority under /v1/artifacts/<op>, so a decorator that leaves the authority alone and
// changes the path invokes SEAL on a call made with GET's task authority. The query is part
// of what the request asks for, so it belongs to the pin for the same reason.
//
// The leg pins the ANSWER too. A refused call must not come back classified as an unknown
// outcome under the name of an operation nobody sent: a worker that reads "Seal outcome
// unknown" goes and reconciles a write it never made, which is how a rewrite turns into a
// duplicate.
func TestRequestDecoratorCannotRewriteTheOwnerOperation(t *testing.T) {
	ctx, traceID := tracedContext(t)

	for _, moved := range []string{"none", "path", "query"} {
		t.Run(moved, func(t *testing.T) {
			rec := newObsRecorder()
			srv, hc := ownerFixture(t, rec)
			ownerDials := countDials(t, hc)
			blob, hits := blobFixture(t, rec)
			upload := uploadClient(t, blob, hc)
			blobDials := countDials(t, upload)
			tc := new(traceCounter)
			callCtx := tc.context(ctx)

			c, err := sdk.New(sdk.Config{
				BaseURL:          srv.URL,
				HTTPClient:       hc,
				UploadHTTPClient: upload,
				TokenSource:      func(context.Context) (string, error) { return taskBearer, nil },
				RequestDecorator: func(r *http.Request) {
					injectTrace(r)
					switch moved {
					case "path":
						// The rewrite that needs no header and no new authority: ask the
						// owner for Seal on a call that was made to read metadata.
						r.URL.Path = "/v1/artifacts/Seal"
					case "query":
						r.URL.RawQuery = "version=v1"
					}
				},
			})
			if err != nil {
				t.Fatalf("client: %v", err)
			}

			_, err = c.Get(callCtx, "artifact")
			counters := map[string]*atomic.Int64{
				"owner dials": ownerDials, "blob dials": blobDials,
				"owner connection handoffs": &tc.gotConn, "owner request writes": &tc.wrote,
			}
			if moved == "none" {
				if err != nil {
					t.Fatalf("an owner call whose decorator only carries the trace parent was refused: %v -- every "+
						"artifact read by a worker would fail the same way", err)
				}
				if got := ownerDials.Load(); got != 1 {
					t.Fatalf("the owner call dialed %d times, want exactly 1", got)
				}
				seen := rec.snapshot()
				if got := seen[ownerGet+"|traceparent"]; !strings.HasPrefix(got, "00-"+traceID+"-") {
					t.Fatalf("owner call carried traceparent=%q, want it to continue run trace %s", got, traceID)
				}
				assertDecoratorHeaderSet(t, seen[ownerGet+"|headers"], "Authorization", "Content-Type", "Traceparent")
				assertRequestTally(t, rec.tally(), map[string]int{ownerGet: 1, requestTotal: 1})
				return
			}

			assertRewriteRefusal(t, err, counters)
			if hits.Load() != 0 {
				t.Fatalf("the blob store saw %d requests during a refused owner call, want 0", hits.Load())
			}
			// And the operation the decorator asked for did not run either: a refusal that
			// arrived after the write would be the worse half of this finding.
			assertRequestTally(t, rec.tally(), map[string]int{requestTotal: 0})
		})
	}
}

// TestPresignedUploadCannotHaveItsPathOrQueryRewritten is ART-F3c on the upload path, and the
// sharper half: a presigned signature is issued for ONE object and travels in the query of the
// URL whose path names that object. Keep the host, move either component, and the PUT carries
// a signature that was never meant to authorize what it now authorizes -- which is precisely
// what a guard that pins the authority alone does not see.
func TestPresignedUploadCannotHaveItsPathOrQueryRewritten(t *testing.T) {
	ctx, traceID := tracedContext(t)

	for _, moved := range []string{"none", "path", "query"} {
		t.Run(moved, func(t *testing.T) {
			rec := newObsRecorder()
			srv, hc := ownerFixture(t, rec)
			ownerDials := countDials(t, hc)
			blob, hits := blobFixture(t, rec)
			upload := uploadClient(t, blob, hc)
			blobDials := countDials(t, upload)
			tc := new(traceCounter)
			callCtx := tc.context(ctx)

			c, err := sdk.New(sdk.Config{
				BaseURL:          srv.URL,
				HTTPClient:       hc,
				UploadHTTPClient: upload,
				TokenSource:      func(context.Context) (string, error) { return taskBearer, nil },
				RequestDecorator: func(r *http.Request) {
					injectTrace(r)
					switch moved {
					case "path":
						r.URL.Path = "/blobs/someone-elses-artifact"
					case "query":
						r.URL.RawQuery = "X-Amz-Credential=other&X-Amz-Signature=other"
					}
				},
			})
			if err != nil {
				t.Fatalf("client: %v", err)
			}

			err = putArtifact(callCtx, c, signedUploadURL(blob.URL))
			counters := map[string]*atomic.Int64{
				"blob dials": blobDials, "owner dials": ownerDials,
				"upload connection handoffs": &tc.gotConn, "upload request writes": &tc.wrote,
			}
			if moved == "none" {
				if err != nil {
					t.Fatalf("an upload whose decorator only carries the trace parent was refused: %v -- every "+
						"artifact export by a worker would fail the same way", err)
				}
				if got := blobDials.Load(); got != 1 {
					t.Fatalf("the upload dialed the blob store %d times, want exactly 1", got)
				}
				if got := hits.Load(); got != 1 {
					t.Fatalf("the blob store saw %d requests, want the one upload", got)
				}
				seen := rec.snapshot()
				if got := seen["upload|traceparent"]; !strings.HasPrefix(got, "00-"+traceID+"-") {
					t.Fatalf("the control upload carried traceparent=%q, want it to continue run trace %s", got, traceID)
				}
				assertDecoratorHeaderSet(t, seen["upload|headers"], "Traceparent")
				assertCanonicallySpelledHeaders(t, seen["upload|headers_raw"])
				assertRequestTally(t, rec.tally(), map[string]int{"upload": 1, requestTotal: 1})
				return
			}

			assertRewriteRefusal(t, err, counters)
			if hits.Load() != 0 {
				t.Fatalf("the blob store saw %d requests during a refused upload, want 0", hits.Load())
			}
			assertRequestTally(t, rec.tally(), map[string]int{requestTotal: 0})
		})
	}
}

// smuggledCanary stands in for a header value a caller decorator chose. It must appear in no
// error string: a refusal that quoted it would put attacker-chosen text where a caller reads
// why a call failed.
const smuggledCanary = "CANARY-SMUGGLED-AUTHORITY-6d31"

// TestDecoratorsCannotSmuggleAnOwnedHeader is ART-F4, run against both decoration points in
// every case, because it is one property: the guard read headers through Header.Get, and Get
// canonicalises its ARGUMENT, so Get("Authorization") is blind to Header["authorization"] --
// while Header.writeSubset puts the map on the wire under whatever key it holds, excluding
// only the canonical names net/http handles itself. A credential in a spelling the check
// cannot see is the same credential: on the upload path it is the one thing a presigned PUT
// must never carry, and on the owner path it travels beside the task token the SDK sets
// afterwards, so the server is left choosing between two authorizations.
//
// "canonical-authorization" marks the boundary of the new refusal and has to stay green: the
// owner path neutralises that spelling by setting the credential itself after the decorator
// ran, which this leg checks by reading what the owner received rather than by trusting the
// client. Refusing it would not be safer, it would be a second F1 -- a guard that stops
// nothing because it stops everything.
func TestDecoratorsCannotSmuggleAnOwnedHeader(t *testing.T) {
	ctx, traceID := tracedContext(t)

	for _, spell := range []struct {
		name string
		key  string
		// ownerRefusal names the guard the owner call must hit, "" when that call has to go
		// through. uploadRefusal names the guard the presigned PUT must hit.
		ownerRefusal  string
		uploadRefusal string
	}{
		{"none", "", "", ""},
		{"canonical-authorization", "Authorization", "", "must not add credentials"},
		{"lowercase-authorization", "authorization", "must not add credentials", "must not add credentials"},
		{"uppercase-authorization", "AUTHORIZATION", "must not add credentials", "must not add credentials"},
		{"lowercase-host", "host", "change request authority", "change request authority"},
		{"lowercase-cookie", "cookie", "must not add credentials", "must not add credentials"},
		{"lowercase-proxy-authorization", "proxy-authorization", "must not add credentials", "must not add credentials"},
		// ART-F4b: the names net/http derives from the body and the connection. Canonical
		// Content-Length/Transfer-Encoding/Trailer are the ones reqWriteExcludeHeader knows
		// about, so the lowercase spellings are what reach the wire as a SECOND line beside
		// the transport's own (net/http/transfer.go:290) -- one request read as two. Connection,
		// Upgrade, TE and Expect are not excluded even canonically, and none of them is the
		// hook's to claim.
		{"canonical-connection", "Connection", "must not own framing headers", "must not own framing headers"},
		{"canonical-content-length", "Content-Length", "must not own framing headers", "must not own framing headers"},
		{"lowercase-content-length", "content-length", "must not own framing headers", "must not own framing headers"},
		{"lowercase-transfer-encoding", "transfer-encoding", "must not own framing headers", "must not own framing headers"},
		{"canonical-te", "TE", "must not own framing headers", "must not own framing headers"},
		{"canonical-expect", "Expect", "must not own framing headers", "must not own framing headers"},
		{"canonical-upgrade", "Upgrade", "must not own framing headers", "must not own framing headers"},
		{"lowercase-trailer", "trailer", "must not own framing headers", "must not own framing headers"},
	} {
		t.Run(spell.name, func(t *testing.T) {
			rec := newObsRecorder()
			srv, hc := ownerFixture(t, rec)
			ownerDials := countDials(t, hc)
			blob, hits := blobFixture(t, rec)
			upload := uploadClient(t, blob, hc)
			blobDials := countDials(t, upload)
			// One counter per hop: a refusal case has to be able to say which of the two
			// requests stayed home, and both run in the same case.
			ownerTC, uploadTC := new(traceCounter), new(traceCounter)

			c, err := sdk.New(sdk.Config{
				BaseURL:          srv.URL,
				HTTPClient:       hc,
				UploadHTTPClient: upload,
				TokenSource:      func(context.Context) (string, error) { return taskBearer, nil },
				RequestDecorator: func(r *http.Request) {
					injectTrace(r)
					if spell.key != "" {
						// Deliberately not Header.Set, which canonicalises -- that is the
						// behaviour this finding is about. Writing the map is what a smuggler
						// does, and it is what net/http then writes to the wire.
						r.Header[spell.key] = []string{smuggledCanary}
					}
				},
			})
			if err != nil {
				t.Fatalf("client: %v", err)
			}

			ownerErr := func() error { _, err := c.Get(ownerTC.context(ctx), "artifact"); return err }()
			uploadErr := putArtifact(uploadTC.context(ctx), c, signedUploadURL(blob.URL))
			assertNoCredentialEcho(t, ownerErr, uploadErr)

			switch spell.name {
			case "none":
				if ownerErr != nil || uploadErr != nil {
					t.Fatalf("the ordinary traced request was refused (owner %v, upload %v): every artifact read and "+
						"exported by a worker would fail the same way", ownerErr, uploadErr)
				}
				if got := ownerDials.Load(); got != 1 {
					t.Fatalf("the owner call dialed %d times, want exactly 1", got)
				}
				if got := blobDials.Load(); got != 1 {
					t.Fatalf("the upload dialed %d times, want exactly 1", got)
				}
				if got := ownerTC.wrote.Load(); got != 1 {
					t.Fatalf("the owner hop wrote %d requests, want exactly 1: the refusal cases read this counter, so "+
						"it has to be provably live", got)
				}
				if got := uploadTC.wrote.Load(); got != 1 {
					t.Fatalf("the upload hop wrote %d requests, want exactly 1: the refusal cases read this counter, so "+
						"it has to be provably live", got)
				}
				seen := rec.snapshot()
				if got := seen[ownerGet+"|authorization"]; got != "Bearer "+taskBearer {
					t.Fatalf("owner call carried %q, want the task credential", got)
				}
				if got := seen["upload|authorization"]; got != "" {
					t.Fatal("the presigned upload disclosed a credential")
				}
				if got := seen[ownerGet+"|traceparent"]; !strings.HasPrefix(got, "00-"+traceID+"-") {
					t.Fatalf("owner call carried traceparent=%q, want it to continue run trace %s", got, traceID)
				}
				// The SDK's own headers arrive canonically, which is the floor every refusal
				// case here is measured against.
				assertCanonicallySpelledHeaders(t, seen[ownerGet+"|headers_raw"])
				assertCanonicallySpelledHeaders(t, seen["upload|headers_raw"])
				assertDecoratorHeaderSet(t, seen[ownerGet+"|headers"], "Authorization", "Content-Type", "Traceparent")
				assertDecoratorHeaderSet(t, seen["upload|headers"], "Traceparent")
				assertRequestTally(t, rec.tally(), map[string]int{ownerGet: 1, "upload": 1, requestTotal: 2})

			case "canonical-authorization":
				if ownerErr != nil {
					t.Fatalf("the owner call was refused over a header the SDK overwrites seconds later: %v -- this is "+
						"the ordering guarantee, not a credential the decorator won", ownerErr)
				}
				seen := rec.snapshot()
				if got := seen[ownerGet+"|authorization"]; got != "Bearer "+taskBearer {
					t.Fatalf("a decorator overwrote the task credential: %q", got)
				}
				assertCanonicallySpelledHeaders(t, seen[ownerGet+"|headers_raw"])
				if got := ownerTC.wrote.Load(); got != 1 {
					t.Fatalf("the owner hop wrote %d requests, want exactly 1", got)
				}
				// The upload has no credential of its own to overwrite, so there the same
				// header is refused outright.
				assertRefusedBeforeDial(t, uploadErr, spell.uploadRefusal, map[string]*atomic.Int64{
					"blob dials": blobDials, "upload connection handoffs": &uploadTC.gotConn, "upload request writes": &uploadTC.wrote,
				})
				if hits.Load() != 0 {
					t.Fatalf("the blob store saw %d requests during a refused upload, want 0", hits.Load())
				}
				assertRequestTally(t, rec.tally(), map[string]int{ownerGet: 1, requestTotal: 1})

			default:
				assertRefusedBeforeDial(t, ownerErr, spell.ownerRefusal, map[string]*atomic.Int64{
					"owner dials": ownerDials, "blob dials": blobDials,
					"owner connection handoffs": &ownerTC.gotConn, "owner request writes": &ownerTC.wrote,
				})
				assertRefusedBeforeDial(t, uploadErr, spell.uploadRefusal, map[string]*atomic.Int64{
					"blob dials": blobDials, "owner dials": ownerDials,
					"upload connection handoffs": &uploadTC.gotConn, "upload request writes": &uploadTC.wrote,
				})
				if hits.Load() != 0 {
					t.Fatalf("the blob store saw %d requests during refused calls, want 0", hits.Load())
				}
				// Read from the rig, not from the error text: the claim is that no peer
				// received a credential, and only the peer can say so.
				for key, value := range rec.snapshot() {
					if strings.HasSuffix(key, "|authorization") && value != "" {
						t.Fatalf("%s recorded %q during a refused call: the credential left the process", key, value)
					}
				}
				assertRequestTally(t, rec.tally(), map[string]int{requestTotal: 0})
			}
		})
	}
}

// dialedAt records the address a client asked to dial, and sends the connection somewhere
// else instead.
//
// It exists for the two ART-F1edge legs below. Those legs need to prove that a degenerate
// authority ("https://host:/…") reaches the transport in the form net/http puts requests in,
// and the cheap way to look -- dial 127.0.0.1:443 and hope nobody is listening -- is a leg
// whose verdict depends on what else runs on the machine, and one that can never say where
// the bytes landed. Capturing the address and answering it from the fixture keeps the
// original assertion (the address the SDK computed) and adds the one the old shape could not
// make: the peer that actually served the request.
type dialedAt struct {
	count atomic.Int64
	mu    sync.Mutex
	addrs []string
}

func (d *dialedAt) addresses() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.addrs...)
}

// redirect installs the captor on the client's transport and returns it. Like countDials it
// has to run BEFORE sdk.New, because the SDK clones the transport it is handed and a hook put
// on the clone would never be seen by the requests the SDK makes.
func redirect(t *testing.T, hc *http.Client, target string) *dialedAt {
	t.Helper()
	tr, ok := hc.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("fixture transport is %T, want *http.Transport: the dial belongs on the transport the SDK clones", hc.Transport)
	}
	d := &dialedAt{}
	dialer := &net.Dialer{}
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		d.count.Add(1)
		d.mu.Lock()
		d.addrs = append(d.addrs, address)
		d.mu.Unlock()
		return dialer.DialContext(ctx, network, target)
	}
	return d
}

// assertDialedOnceAt is "one connection, to exactly this address, and nowhere else".
func assertDialedOnceAt(t *testing.T, d *dialedAt, want string) {
	t.Helper()
	if got := d.count.Load(); got != 1 {
		t.Fatalf("the client dialed %d times, want exactly 1 (addresses %v): this leg reads a dial counter to "+
			"prove a request reached the transport, so the counter has to be answering for one request", got, d.addresses())
	}
	if got := d.addresses(); got[0] != want {
		t.Fatalf("the client dialed %q, want %q: the authority in the request is what the transport dials, and this "+
			"leg is about the form that authority has to arrive in", got[0], want)
	}
}

// TestPresignedUploadIsNotRefusedForTheShapeOfItsURL is ART-F1edge on the upload path: an
// upload must not be refused because of how its URL is spelled.
//
// url.Parse keeps an explicit empty port -- the authority of "https://host:/object" is
// "host:" -- while NewRequestWithContext strips one from the copy it turns into the request
// (removeEmptyPort, net/http/request.go, Issue 14836), so req.URL.Host and req.Host both read
// "host". A guard that compares a request against the raw parse therefore refuses the very URL
// it was handed, and from the worker's side that is indistinguishable from an attack it
// stopped: same INVALID_ARGUMENT, same failed export. It is the F1 defect in a second, rarer
// spelling, and the answer is the same normalisation rather than a second exception.
func TestPresignedUploadIsNotRefusedForTheShapeOfItsURL(t *testing.T) {
	ctx, traceID := tracedContext(t)
	rec := newObsRecorder()
	srv, hc := ownerFixture(t, rec)
	blob, hits := blobFixture(t, rec)
	upload := uploadClient(t, blob, hc)
	// 127.0.0.1 with an explicit empty port: the one authority form net/http rewrites, at a
	// loopback address, so nothing that resolves names stands between the guard and the verdict.
	dials := redirect(t, upload, strings.TrimPrefix(blob.URL, "https://"))

	c, err := sdk.New(sdk.Config{
		BaseURL:          srv.URL,
		HTTPClient:       hc,
		UploadHTTPClient: upload,
		TokenSource:      func(context.Context) (string, error) { return taskBearer, nil },
		RequestDecorator: injectTrace,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if err := putArtifact(ctx, c, "https://127.0.0.1:/blobs/artifact?X-Amz-Signature="+presignCanary); err != nil {
		t.Fatalf("the SDK refused a presigned upload over the spelling of its URL (%v): a worker exporting an "+
			"artifact loses every upload to a guard that fires on an empty port", err)
	}
	// The address is the finding. "127.0.0.1:" had to arrive as "127.0.0.1:443", which is the
	// form net/http puts in a request; a guard comparing against the raw parse rejects it here.
	assertDialedOnceAt(t, dials, "127.0.0.1:443")
	if hits.Load() != 1 {
		t.Fatalf("the blob store served %d requests for one presigned PUT, want exactly 1", hits.Load())
	}
	seen := rec.snapshot()
	if got := seen["upload|traceparent"]; !strings.HasPrefix(got, "00-"+traceID+"-") {
		t.Fatalf("the upload carried traceparent=%q, want it to continue run trace %s", got, traceID)
	}
	if got := seen["upload|authorization"]; got != "" {
		t.Fatal("the presigned upload disclosed a credential")
	}
	assertDecoratorHeaderSet(t, seen["upload|headers"], "Traceparent")
	assertRequestTally(t, rec.tally(), map[string]int{"upload": 1, requestTotal: 1})
}

// TestOwnerCallIsNotRefusedForTheShapeOfItsBaseURL is ART-F1edge at the site that matters
// most. New accepts "https://owner:/" -- the validation wants a non-empty authority, and
// "owner:" is non-empty -- so a client that stored the authority as url.Parse reported it
// refuses EVERY owner call it ever makes, with no decorator having done anything. No new
// refusal is added in New for this: narrowing the SDK's contract over a degenerate URL would
// be the same mistake in the other direction. The authority is stored in the form requests
// carry, and one rule governs every comparison.
func TestOwnerCallIsNotRefusedForTheShapeOfItsBaseURL(t *testing.T) {
	ctx, traceID := tracedContext(t)
	rec := newObsRecorder()
	srv, hc := ownerFixture(t, rec)
	dials := redirect(t, hc, strings.TrimPrefix(srv.URL, "https://"))

	c, err := sdk.New(sdk.Config{
		BaseURL:          "https://127.0.0.1:",
		HTTPClient:       hc,
		TokenSource:      func(context.Context) (string, error) { return taskBearer, nil },
		RequestDecorator: injectTrace,
	})
	if err != nil {
		t.Fatalf("New refused a bare HTTPS origin that carries an empty port (%v): the URL is accepted on input, so "+
			"every owner call would then be refused on the way out", err)
	}
	if _, err := c.Get(ctx, "artifact"); err != nil {
		t.Fatalf("the SDK refused an owner call over the spelling of the base URL it was configured with (%v): every "+
			"artifact read by a worker fails the same way", err)
	}
	// Same claim as the upload leg, at the site where one bad comparison silences the client:
	// the authority the transport dialed is the normalised one, so the guards compared against
	// it agree with the request that goes out.
	assertDialedOnceAt(t, dials, "127.0.0.1:443")
	seen := rec.snapshot()
	if got := seen[ownerGet+"|authorization"]; got != "Bearer "+taskBearer {
		t.Fatalf("owner call carried %q, want the task credential", got)
	}
	if got := seen[ownerGet+"|traceparent"]; !strings.HasPrefix(got, "00-"+traceID+"-") {
		t.Fatalf("owner call carried traceparent=%q, want it to continue run trace %s", got, traceID)
	}
	assertDecoratorHeaderSet(t, seen[ownerGet+"|headers"], "Authorization", "Content-Type", "Traceparent")
	assertRequestTally(t, rec.tally(), map[string]int{ownerGet: 1, requestTotal: 1})
}
