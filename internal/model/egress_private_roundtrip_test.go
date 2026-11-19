package model

// W-2: registration must actually UNBLOCK egress, end to end. Until this leg
// existed, D1 was proven only up to "the guard let the request reach the dial
// path": nothing ever completed an HTTP exchange with a provider addressed by a
// registered RFC1918 origin, so the claim "an internal provider is reachable"
// was untested past the gate.
//
// No loopback and no TCP listener are involved: the provider is a real HTTP
// server on a private unix socket, reached through a dialer the guard still
// validates first. The dialer records the address the guard asked for, which is
// the pinned registered origin -- that is the fact under test.

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

// privateOrigin is the stage-2 shape: the provider lives inside the guest
// network, addressed by its RFC1918 literal and a non-default port.
const (
	privateOriginURL    = "http://172.18.0.6:8000/v1"
	privateOriginSocket = "provider.sock"
)

// unixProvider runs an OpenAI-compatible fake provider on a private socket and
// reports the destination the guard asked to dial.
type unixProvider struct {
	dialed  atomic.Value // string
	path    atomic.Value // string: the path the provider was asked to serve
	hits    atomic.Int64
	socket  string
	server  *httptest.Server
	cleanup func()
}

func newUnixProvider(t *testing.T, apiKey string) *unixProvider {
	t.Helper()
	dir, err := os.MkdirTemp("", "ahp-w2")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	sock := filepath.Join(dir, privateOriginSocket)
	p := &unixProvider{socket: sock}
	handler := func(w http.ResponseWriter, r *http.Request) {
		p.hits.Add(1)
		p.path.Store(r.URL.Path)
		if r.Method != http.MethodPost {
			t.Errorf("provider saw %s, want POST", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			t.Errorf("provider saw path %q, want .../chat/completions", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+apiKey {
			t.Errorf("provider saw auth %q, want the configured key", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sseBody([]string{"internal", " reply"},
			&Usage{PromptTokens: 11, CompletionTokens: 7, TotalTokens: 18})))
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(handler))
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("unix listener: %v", err)
	}
	srv.Listener = ln
	srv.Start()
	p.server = srv
	p.cleanup = func() { srv.Close(); _ = os.RemoveAll(dir) }
	t.Cleanup(p.cleanup)
	return p
}

// dialSocket satisfies the guard's dial contract: it is called only with an
// address the policy already validated, and it records that address.
func (p *unixProvider) dialSocket(_ context.Context, _, address string) (net.Conn, error) {
	p.dialed.Store(address)
	d := &net.Dialer{Timeout: 5 * time.Second}
	return d.DialContext(context.Background(), "unix", p.socket)
}

func (p *unixProvider) client(t *testing.T, secret secretstore.QwenSecret) *QwenClient {
	t.Helper()
	c, err := newQwenClient(secret, netpolicy.WithDialer(netDialerFunc(p.dialSocket)))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return c
}

// refuseOrStream reports the refusal an uncallable origin must produce, wherever
// it is produced: a client that refuses to be built is the strongest form of
// "this destination is not callable", and neither form may touch the network.
func refuseOrStream(t *testing.T, p *unixProvider, secret secretstore.QwenSecret) error {
	t.Helper()
	c, buildErr := newQwenClient(secret, netpolicy.WithDialer(netDialerFunc(p.dialSocket)))
	if buildErr != nil {
		return buildErr
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, streamErr := c.Stream(ctx, ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}}})
	if streamErr == nil {
		t.Fatal("expected the attempt to be refused")
	}
	if !IsPreSendRefusal(streamErr) {
		t.Fatalf("want a pre-send refusal, got %v", streamErr)
	}
	return streamErr
}

type netDialerFunc func(ctx context.Context, network, address string) (net.Conn, error)

func (f netDialerFunc) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return f(ctx, network, address)
}

func drain(t *testing.T, st Stream) (string, Usage) {
	t.Helper()
	var b strings.Builder
	for {
		d, err := st.Next(context.Background())
		if errors.Is(err, ErrStreamDone) {
			break
		}
		if err != nil {
			t.Fatalf("delta: %v", err)
		}
		b.WriteString(d.Text)
	}
	return b.String(), st.Usage()
}

func privateSecret() secretstore.QwenSecret {
	return secretstore.QwenSecret{
		APIKey:       "sk-w2-do-not-leak",
		BaseURL:      privateOriginURL,
		Model:        "model-internal",
		PrivateHosts: []string{"172.18.0.6:8000"},
	}
}

// W-2: a registered RFC1918 origin completes a real exchange and reports usage.
func TestW2RegisteredRFC1918ProviderRoundTrip(t *testing.T) {
	p := newUnixProvider(t, "sk-w2-do-not-leak")
	c := p.client(t, privateSecret())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := c.Stream(ctx, ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatalf("stream to a registered internal origin: %v", err)
	}
	defer st.Close()

	text, usage := drain(t, st)
	if text != "internal reply" {
		t.Fatalf("assembled text = %q, want the provider reply", text)
	}
	if usage.PromptTokens == 0 || usage.CompletionTokens == 0 || usage.TotalTokens == 0 {
		t.Fatalf("usage must be reported by a real round trip, got %+v", usage)
	}
	if got := p.dialed.Load(); got != "172.18.0.6:8000" {
		t.Fatalf("the guard must dial the registered origin, dialed %v", got)
	}
	if p.hits.Load() != 1 {
		t.Fatalf("the provider must have served exactly one request, saw %d", p.hits.Load())
	}
}

// W-2 control: without registration the same origin is refused before any byte
// leaves the process, so the success above is attributable to registration.
func TestW2UnregisteredRFC1918ProviderIsRefusedBeforeSend(t *testing.T) {
	p := newUnixProvider(t, "sk-w2-do-not-leak")
	secret := privateSecret()
	secret.PrivateHosts = nil
	err := refuseOrStream(t, p, secret)
	if err == nil {
		t.Fatal("an unregistered private origin must not complete")
	}
	if !strings.Contains(err.Error(), "PrivateHosts") {
		t.Fatalf("the refusal must name the mechanism that would authorise it, got %v", err)
	}
	if p.hits.Load() != 0 {
		t.Fatalf("no request may reach the provider, saw %d", p.hits.Load())
	}
	if got := p.dialed.Load(); got != nil {
		t.Fatalf("the guard must not ask to dial, asked for %v", got)
	}
}

// W-2: the private branch must not become a general key to RFC1918: a registered
// host answers only the origin it was registered for.
func TestW2RegistrationDoesNotOpenOtherPortsOnTheSameHost(t *testing.T) {
	p := newUnixProvider(t, "sk-w2-do-not-leak")
	secret := privateSecret()
	secret.BaseURL = "http://172.18.0.6:8001/v1"
	err := refuseOrStream(t, p, secret)
	if err == nil {
		t.Fatal("a port that was never registered must not be reachable")
	}
	if p.hits.Load() != 0 {
		t.Fatalf("no request may reach the provider, saw %d", p.hits.Load())
	}
}
