package netpolicy

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeResolver returns canned answers per host.
type fakeResolver struct {
	mu      sync.Mutex
	answers map[string][]string
	lookups []string
	err     error
}

func (r *fakeResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lookups = append(r.lookups, host)
	if r.err != nil {
		return nil, r.err
	}
	addrs, ok := r.answers[host]
	if !ok {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	return addrs, nil
}

func (r *fakeResolver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.lookups)
}

// recordingDialer records every dial target and refuses to actually connect.
type recordingDialer struct {
	mu      sync.Mutex
	targets []string
	conn    net.Conn
}

func (d *recordingDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	d.mu.Lock()
	d.targets = append(d.targets, addr)
	conn := d.conn
	d.mu.Unlock()
	if conn != nil {
		return conn, nil
	}
	return nil, errors.New("refused by test dialer")
}

func (d *recordingDialer) dialed() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.targets))
	copy(out, d.targets)
	return out
}

// mustParse parses a URL and fails the test on error.
func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u
}

func basePolicy() Policy {
	return Policy{
		AllowedHosts:        []string{"api.example.com", "legacy.example.com:8443"},
		AllowedPathPrefixes: []string{"/api"},
		AllowedMethods:      []string{"GET", "POST"},
	}
}

func TestPolicyValidateRejectsMalformedEntries(t *testing.T) {
	cases := []struct {
		name   string
		policy Policy
	}{
		{"wildcard", Policy{AllowedHosts: []string{"*.example.com"}}},
		{"wildcard suffix", Policy{AllowedHosts: []string{"example.*"}}},
		{"userinfo in entry", Policy{AllowedHosts: []string{"user:pass@example.com"}}},
		{"scheme in entry", Policy{AllowedHosts: []string{"https://example.com"}}},
		{"path in entry", Policy{AllowedHosts: []string{"example.com/api"}}},
		{"percent in entry", Policy{AllowedHosts: []string{"ex%61mple.com"}}},
		{"empty entry", Policy{AllowedHosts: []string{""}}},
		{"bad port", Policy{AllowedHosts: []string{"example.com:notaport"}}},
		{"port out of range", Policy{AllowedHosts: []string{"example.com:70000"}}},
		{"empty label", Policy{AllowedHosts: []string{"example..com"}}},
		{"underscore label", Policy{AllowedHosts: []string{"bad_name.example.com"}}},
		{"hyphen bounded", Policy{AllowedHosts: []string{"-example.com"}}},
		{"loopback literal", Policy{AllowedHosts: []string{"127.0.0.1"}}},
		{"loopback v6", Policy{AllowedHosts: []string{"::1"}}},
		{"loopback mapped", Policy{AllowedHosts: []string{"::ffff:127.0.0.1"}}},
		{"metadata literal", Policy{AllowedHosts: []string{"169.254.169.254"}}},
		{"link local", Policy{AllowedHosts: []string{"169.254.10.10"}}},
		{"unspecified", Policy{AllowedHosts: []string{"0.0.0.0"}}},
		{"multicast", Policy{AllowedHosts: []string{"224.0.0.1"}}},
		{"private in public list", Policy{AllowedHosts: []string{"10.0.0.5"}}},
		{"private host loopback", Policy{PrivateHosts: []string{"127.0.0.1"}}},
		{"private host metadata", Policy{PrivateHosts: []string{"169.254.169.254"}}},
		{"private host linklocal v6", Policy{PrivateHosts: []string{"fe80::1"}}},
		{"negative body", Policy{AllowedHosts: []string{"a.example.com"}, MaxResponseBytes: -1}},
		{"negative timeout", Policy{AllowedHosts: []string{"a.example.com"}, Timeout: -time.Second}},
		{"bad method", Policy{AllowedHosts: []string{"a.example.com"}, AllowedMethods: []string{"GET /"}}},
		{"empty method", Policy{AllowedHosts: []string{"a.example.com"}, AllowedMethods: []string{""}}},
		{"relative prefix", Policy{AllowedHosts: []string{"a.example.com"}, AllowedPathPrefixes: []string{"api"}}},
		{"encoded prefix", Policy{AllowedHosts: []string{"a.example.com"}, AllowedPathPrefixes: []string{"/a%70i"}}},
		{"query in prefix", Policy{AllowedHosts: []string{"a.example.com"}, AllowedPathPrefixes: []string{"/api?x=1"}}},
		{"duplicate host", Policy{AllowedHosts: []string{"API.example.com.", "api.example.com"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.policy.Validate()
			if err == nil {
				t.Fatalf("policy %+v accepted, want rejection", tc.policy)
			}
			if !errors.Is(err, ErrPolicy) {
				t.Fatalf("error %v does not wrap ErrPolicy", err)
			}
			var e *Error
			if !errors.As(err, &e) || e.Code() != "INVALID_ARGUMENT" {
				t.Fatalf("error %v should carry INVALID_ARGUMENT", err)
			}
		})
	}
}

func TestPolicyValidateAcceptsGoodEntries(t *testing.T) {
	p := Policy{
		AllowedHosts: []string{
			"api.example.com",
			"Trailing-Dot.example.COM.",
			"port.example.com:8443",
			"203.0.113.99", // TEST-NET is rejected; use a routable literal below
		},
	}
	// Replace the documentation range entry with a routable literal.
	p.AllowedHosts[len(p.AllowedHosts)-1] = "93.184.216.34"
	p.PrivateHosts = []string{"mcp.internal", "10.10.0.7:9000", "192.168.4.5"}
	if err := p.Validate(); err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
	if err := (Policy{}).Validate(); err != nil {
		t.Fatalf("empty policy must be valid (permits nothing): %v", err)
	}
}

func TestValidateURLHostMatching(t *testing.T) {
	p := basePolicy()
	good := []string{
		"https://api.example.com/api",
		"https://api.example.com/api/v1/items",
		"https://api.example.com/api",
		"https://API.Example.COM./api",
		"https://api.example.com:443/api",
	}
	for _, raw := range good {
		if err := p.ValidateURL(mustParse(t, raw), "GET"); err != nil {
			t.Errorf("ValidateURL(%q) = %v, want accept", raw, err)
		}
	}

	bad := []string{
		"https://apievil.example.com/api",
		"https://api.example.com.evil.net/api",
		"https://evil.net/api.example.com",
		"https://evil.net/@api.example.com/api",
		"https://evil.net#.api.example.com/api",
		"https://api.example.com:8443/api", // port not registered for this host
		"https://legacy.example.com/api",   // host registered only with :8443
		"http://api.example.com/api",       // plaintext to a public host
		"https://api.example.com/apievil",  // segment boundary
		"https://api.example.com/v1/api",   // prefix must be leading
		"https://other.example.com/api",    // not registered
		"https://api.example.com",          // empty path canonicalises to /api? no: "/"
	}
	for _, raw := range bad {
		if err := p.ValidateURL(mustParse(t, raw), "GET"); err == nil {
			t.Errorf("ValidateURL(%q) accepted, want rejection", raw)
		}
	}

	// The registered host:port entry works over TLS on that port.
	if err := p.ValidateURL(mustParse(t, "https://legacy.example.com:8443/api"), "GET"); err != nil {
		t.Errorf("registered host:port rejected: %v", err)
	}
}

func TestValidateURLRejectsUserinfoAndFragment(t *testing.T) {
	p := basePolicy()
	for _, raw := range []string{
		"https://user@api.example.com/api",
		"https://user:pass@api.example.com/api",
		"https://:@api.example.com/api",
	} {
		u := mustParse(t, raw)
		if err := p.ValidateURL(u, "GET"); !errors.Is(err, ErrHostNotAllowed) {
			t.Errorf("userinfo %q: got %v, want ErrHostNotAllowed", raw, err)
		}
	}

	// Fragments are dropped by url.Parse, so build one by hand.
	u := mustParse(t, "https://api.example.com/api")
	u.Fragment = "top"
	if err := p.ValidateURL(u, "GET"); !errors.Is(err, ErrPathNotAllowed) {
		t.Errorf("fragment: got %v, want ErrPathNotAllowed", err)
	}

	// An encoded authority is a smuggling attempt, never a valid request.
	raw := mustParse(t, "https://api.example.com/api")
	raw.Host = "ex%61mple.com"
	if err := p.ValidateURL(raw, "GET"); !errors.Is(err, ErrHostNotAllowed) {
		t.Errorf("encoded authority: got %v, want ErrHostNotAllowed", err)
	}
}

func TestValidateURLIPLiteralsRequireRegistration(t *testing.T) {
	p := Policy{
		AllowedHosts: []string{"93.184.216.34", "[2001:4860:4860::8888]"},
	}
	if err := p.ValidateURL(mustParse(t, "https://93.184.216.34/api"), "GET"); err != nil {
		t.Errorf("registered literal rejected: %v", err)
	}
	if err := p.ValidateURL(mustParse(t, "https://93.184.216.35/api"), "GET"); !errors.Is(err, ErrHostNotAllowed) {
		t.Errorf("unregistered literal: got %v, want ErrHostNotAllowed", err)
	}
	// A mapped spelling of a registered literal is not the same registration.
	if err := p.ValidateURL(mustParse(t, "https://[::ffff:93.184.216.34]/api"), "GET"); !errors.Is(err, ErrHostNotAllowed) {
		t.Errorf("mapped literal alias: got %v, want ErrHostNotAllowed", err)
	}
	if err := p.ValidateURL(mustParse(t, "https://[2001:4860:4860::8888]/api"), "GET"); err != nil {
		t.Errorf("registered v6 literal rejected: %v", err)
	}
	// Canonicalisation: an expanded v6 spelling canonicalises to the same
	// registered address.
	expanded := mustParse(t, "https://[2001:4860:4860:0000:0000:0000:0000:8888]/api")
	if got, err := canonicalHost(expanded.Hostname()); err != nil || got != "[2001:4860:4860::8888]" {
		t.Errorf("expanded v6 canonicalises to %q, %v", got, err)
	}
	if err := p.ValidateURL(expanded, "GET"); err != nil {
		t.Errorf("expanded bracketed v6 literal rejected: %v", err)
	}
}

func TestValidateURLMethods(t *testing.T) {
	p := basePolicy()
	if err := p.ValidateURL(mustParse(t, "https://api.example.com/api"), "POST"); err != nil {
		t.Errorf("POST rejected: %v", err)
	}
	for _, m := range []string{"PUT", "DELETE", "PATCH", "get", ""} {
		if err := p.ValidateURL(mustParse(t, "https://api.example.com/api"), m); !errors.Is(err, ErrMethodNotAllowed) {
			t.Errorf("method %q: got %v, want ErrMethodNotAllowed", m, err)
		}
	}

	// Default method set is the read set.
	def := Policy{AllowedHosts: []string{"api.example.com"}}
	for _, m := range []string{"GET", "HEAD", "OPTIONS"} {
		if err := def.ValidateURL(mustParse(t, "https://api.example.com/anything"), m); err != nil {
			t.Errorf("default set rejects %q: %v", m, err)
		}
	}
	if err := def.ValidateURL(mustParse(t, "https://api.example.com/anything"), "POST"); !errors.Is(err, ErrMethodNotAllowed) {
		t.Errorf("default set accepts POST: %v", err)
	}
}

func TestValidateURLPathBoundaries(t *testing.T) {
	p := Policy{
		AllowedHosts:        []string{"api.example.com"},
		AllowedPathPrefixes: []string{"/api", "/v2/tools/"},
	}
	accept := []string{
		"https://api.example.com/api",
		"https://api.example.com/api/",
		"https://api.example.com/api/v1/items",
		"https://api.example.com/v2/tools/run",
		"https://api.example.com/v2/tools",
		"https://api.example.com//api//x", // duplicate slashes canonicalise away
		"https://api.example.com/x/../api/y",
		"https://api.example.com/api/%7Euser", // encoded non-separator is fine
	}
	for _, raw := range accept {
		if err := p.ValidateURL(mustParse(t, raw), "GET"); err != nil {
			t.Errorf("accept %q: %v", raw, err)
		}
	}
	reject := []string{
		"https://api.example.com/apievil",
		"https://api.example.com/ap",
		"https://api.example.com/",
		"https://api.example.com/v2/toolsx/run",
	}
	for _, raw := range reject {
		if err := p.ValidateURL(mustParse(t, raw), "GET"); !errors.Is(err, ErrPathNotAllowed) {
			t.Errorf("reject %q: got %v, want ErrPathNotAllowed", raw, err)
		}
	}
}

func TestValidateURLEscapedSeparatorBypass(t *testing.T) {
	p := Policy{AllowedHosts: []string{"api.example.com"}, AllowedPathPrefixes: []string{"/api"}}
	// "/api%2f..%2fadmin" decodes to "/api/../admin" which would escape the
	// prefix if the raw form were matched.
	for _, raw := range []string{
		"https://api.example.com/api%2f..%2fadmin",
		"https://api.example.com/api%2Fsecrets",
		"https://api.example.com/api%5c..%5cadmin",
	} {
		u := mustParse(t, raw)
		if u.Path != "" {
			// url.Parse decodes the escapes into Path; make sure the gate sees
			// the escaped separator and refuses rather than normalising.
			if !strings.Contains(u.RawPath, "%2f") && !strings.Contains(u.RawPath, "%2F") &&
				!strings.Contains(u.RawPath, "%5c") && !strings.Contains(u.RawPath, "%5C") {
				t.Fatalf("test premise broken for %q (RawPath=%q)", raw, u.RawPath)
			}
		}
		if err := p.ValidateURL(u, "GET"); !errors.Is(err, ErrPathNotAllowed) {
			t.Errorf("escaped separator %q: got %v, want ErrPathNotAllowed", raw, err)
		}
	}
}

func TestValidateURLRejectsControlBytesInPath(t *testing.T) {
	p := Policy{AllowedHosts: []string{"api.example.com"}, AllowedPathPrefixes: []string{"/api"}}
	u := mustParse(t, "https://api.example.com/api/x")
	u.Path = "/api/x\x00"
	if err := p.ValidateURL(u, "GET"); !errors.Is(err, ErrPathNotAllowed) {
		t.Errorf("NUL path: got %v, want ErrPathNotAllowed", err)
	}
	u2 := mustParse(t, "https://api.example.com/api/x")
	u2.Path = "/api/x\nY: injected"
	if err := p.ValidateURL(u2, "GET"); !errors.Is(err, ErrPathNotAllowed) {
		t.Errorf("control path: got %v, want ErrPathNotAllowed", err)
	}
}

func TestValidateURLNilAndOpaque(t *testing.T) {
	p := basePolicy()
	if err := p.ValidateURL(nil, "GET"); err == nil {
		t.Fatal("nil URL accepted")
	}
	if err := p.ValidateURL(&url.URL{Scheme: "https", Opaque: "api.example.com/api"}, "GET"); err == nil {
		t.Fatal("opaque URL accepted")
	}
	if err := p.ValidateURL(&url.URL{Scheme: "ftp", Host: "api.example.com", Path: "/api"}, "GET"); !errors.Is(err, ErrSchemeNotAllowed) {
		t.Errorf("unsupported scheme: got %v, want ErrSchemeNotAllowed", err)
	}
}

func TestIsForbiddenIP(t *testing.T) {
	forbidden := []string{
		"0.0.0.0", "0.1.2.3",
		"127.0.0.1", "127.5.5.5",
		"::1",
		"::ffff:127.0.0.1",
		"::", "::ffff:0.0.0.0",
		"169.254.169.254", "169.254.10.1",
		"::ffff:169.254.169.254",
		"fe80::1", "fe80::ffff:1.2.3.4",
		"224.0.0.1", "ff02::1", "::ffff:224.0.0.1",
		"100.64.0.1", "100.127.255.255",
		"192.0.0.1", "192.0.2.1", "198.18.0.1", "198.19.0.1",
		"198.51.100.1", "203.0.113.1", "240.0.0.1", "255.255.255.255",
		"2001:db8::1",
	}
	for _, s := range forbidden {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("bad test literal %q", s)
		}
		if !IsForbiddenIP(ip) {
			t.Errorf("IsForbiddenIP(%s) = false, want true", s)
		}
	}

	allowed := []string{
		"93.184.216.34", "8.8.8.8", "2001:4860:4860::8888",
		"2606:4700:4700::1111",
	}
	for _, s := range allowed {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("bad test literal %q", s)
		}
		if IsForbiddenIP(ip) {
			t.Errorf("IsForbiddenIP(%s) = true, want false", s)
		}
	}

	// Private space is policy-gated, not absolutely forbidden.
	for _, s := range []string{"10.0.0.1", "172.16.0.1", "192.168.1.1", "fd00::1", "::ffff:10.0.0.1"} {
		ip := net.ParseIP(s)
		if IsForbiddenIP(ip) {
			t.Errorf("IsForbiddenIP(%s) = true, want false (gated by PrivateHosts)", s)
		}
		if !IsPrivateRangeIP(ip) {
			t.Errorf("IsPrivateRangeIP(%s) = false, want true", s)
		}
	}
	if IsForbiddenIP(nil) != true {
		t.Error("IsForbiddenIP(nil) = false, want true")
	}
	if IsPrivateRangeIP(nil) {
		t.Error("IsPrivateRangeIP(nil) = true, want false")
	}
}

func TestIsMetadataIP(t *testing.T) {
	yes := []string{"169.254.169.254", "::ffff:169.254.169.254", "fd00:ec2::254"}
	for _, s := range yes {
		if !IsMetadataIP(net.ParseIP(s)) {
			t.Errorf("IsMetadataIP(%s) = false, want true", s)
		}
	}
	no := []string{"169.254.1.1", "8.8.8.8", "10.0.0.1"}
	for _, s := range no {
		if IsMetadataIP(net.ParseIP(s)) {
			t.Errorf("IsMetadataIP(%s) = true, want false", s)
		}
	}
}

func TestCanonicalHelpers(t *testing.T) {
	if got, err := cleanAbsolutePath("/a/./b//c/../d/"); err != nil || got != "/a/b/d/" {
		t.Errorf("cleanAbsolutePath = %q, %v", got, err)
	}
	if got, err := cleanAbsolutePath("/../../etc/passwd"); err != nil || got != "/etc/passwd" {
		t.Errorf("dot-dot escape not contained: %q, %v", got, err)
	}
	if _, err := cleanAbsolutePath("relative/path"); err == nil {
		t.Error("relative path accepted")
	}
	if !prefixMatches("/api", "/api") || !prefixMatches("/api", "/api/x") {
		t.Error("prefix match broken")
	}
	if prefixMatches("/api", "/apievil") || prefixMatches("/api", "/") {
		t.Error("prefix boundary broken")
	}
	if got, err := canonicalDNSName("Foo.Example.COM."); err != nil || got != "foo.example.com" {
		t.Errorf("canonicalDNSName = %q, %v", got, err)
	}
	if got, err := canonicalEntry("[2001:0DB8:0:0::1]:8443"); err == nil {
		t.Errorf("documentation-range v6 entry accepted: %q", got)
	}
}

// --- transport level tests -------------------------------------------------

func newTestClient(t *testing.T, p Policy, r Resolver, d Dialer) *http.Client {
	t.Helper()
	c, err := NewClient(p, WithResolver(r), WithDialer(d))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func TestDialValidatesEveryResolvedAddress(t *testing.T) {
	p := Policy{AllowedHosts: []string{"api.example.com"}}
	res := &fakeResolver{answers: map[string][]string{
		"api.example.com": {"93.184.216.34", "127.0.0.1"},
	}}
	dial := &recordingDialer{}
	c := newTestClient(t, p, res, dial)

	_, err := c.Get("https://api.example.com/api")
	if !errors.Is(err, ErrAddressForbidden) {
		t.Fatalf("mixed public/loopback answers: got %v, want ErrAddressForbidden", err)
	}
	if got := dial.dialed(); len(got) != 0 {
		t.Fatalf("dialled despite forbidden answer: %v", got)
	}
}

func TestDialDeniesMetadataAndLinkLocal(t *testing.T) {
	for _, addr := range []string{"169.254.169.254", "::ffff:169.254.169.254", "fe80::1", "::1", "fd00:ec2::254"} {
		p := Policy{AllowedHosts: []string{"api.example.com"}}
		res := &fakeResolver{answers: map[string][]string{"api.example.com": {addr}}}
		dial := &recordingDialer{}
		c := newTestClient(t, p, res, dial)
		_, err := c.Get("https://api.example.com/api")
		if !errors.Is(err, ErrAddressForbidden) {
			t.Errorf("addr %s: got %v, want ErrAddressForbidden", addr, err)
		}
		if got := dial.dialed(); len(got) != 0 {
			t.Errorf("addr %s dialled: %v", addr, got)
		}
	}
}

func TestDialDeniesPrivateSpaceUnlessRegistered(t *testing.T) {
	res := &fakeResolver{answers: map[string][]string{"internal.example.com": {"10.0.0.5"}}}

	unregistered := Policy{AllowedHosts: []string{"internal.example.com"}}
	c := newTestClient(t, unregistered, res, &recordingDialer{})
	if _, err := c.Get("https://internal.example.com/api"); !errors.Is(err, ErrAddressForbidden) {
		t.Fatalf("private answer accepted without registration: %v", err)
	}

	registered := Policy{PrivateHosts: []string{"internal.example.com"}}
	c2 := newTestClient(t, registered, res, &recordingDialer{})
	_, err := c2.Get("http://internal.example.com/api")
	// The recording dialer refuses the socket, but the address gate must have
	// passed, so the failure is a dial error, not a policy denial.
	if errors.Is(err, ErrAddressForbidden) {
		t.Fatalf("registered private host denied: %v", err)
	}
}

func TestDialPinsValidatedIPAndKeepsOriginalHost(t *testing.T) {
	p := Policy{AllowedHosts: []string{"api.example.com"}}
	res := &fakeResolver{answers: map[string][]string{"api.example.com": {"93.184.216.34"}}}
	dial := &recordingDialer{}
	c := newTestClient(t, p, res, dial)

	_, _ = c.Get("https://api.example.com/api")
	targets := dial.dialed()
	if len(targets) == 0 {
		t.Fatal("no dial attempted")
	}
	host, port, err := net.SplitHostPort(targets[0])
	if err != nil {
		t.Fatalf("dial target %q: %v", targets[0], err)
	}
	if host != "93.184.216.34" {
		t.Errorf("dial target host = %q, want the validated IP (rebinding guard)", host)
	}
	if port != "443" {
		t.Errorf("dial port = %q, want 443", port)
	}
	if res.count() == 0 {
		t.Error("resolver never consulted")
	}
}

func TestDialUsesTrailingDotFreeHostForResolution(t *testing.T) {
	p := Policy{AllowedHosts: []string{"api.example.com"}}
	res := &fakeResolver{answers: map[string][]string{"api.example.com": {"93.184.216.34"}}}
	c := newTestClient(t, p, res, &recordingDialer{})
	_, _ = c.Get("https://API.Example.COM./api")
	if res.count() != 1 {
		t.Fatalf("lookups = %d, want 1", res.count())
	}
	res.mu.Lock()
	got := res.lookups[0]
	res.mu.Unlock()
	if got != "api.example.com" {
		t.Errorf("resolved %q, want canonical %q", got, "api.example.com")
	}
}

func TestDialRejectsUnregisteredHostDirectly(t *testing.T) {
	// The transport must not dial a host that is not registered even if the
	// policy gate is bypassed by a hand-built request.
	p := Policy{AllowedHosts: []string{"api.example.com"}}
	res := &fakeResolver{answers: map[string][]string{"evil.example.com": {"93.184.216.34"}}}
	dial := &recordingDialer{}
	c := newTestClient(t, p, res, dial)

	req, err := http.NewRequest("GET", "https://evil.example.com/api", nil)
	if err != nil {
		t.Fatal(err)
	}
	// CheckRedirect does not run for the initial request, so the dial guard is
	// the layer that must refuse.
	if _, err := c.Do(req); err == nil {
		t.Fatal("unregistered host dialled")
	}
	if got := dial.dialed(); len(got) != 0 {
		t.Fatalf("dialled unregistered host: %v", got)
	}
}

func TestDialHonoursContextDeadline(t *testing.T) {
	p := Policy{AllowedHosts: []string{"api.example.com"}}
	res := &fakeResolver{answers: map[string][]string{"api.example.com": {"93.184.216.34"}}}
	blocked := &blockingDialer{}
	c := newTestClient(t, p, res, blocked)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://api.example.com/api", nil)
	start := time.Now()
	_, err := c.Do(req)
	if err == nil {
		t.Fatal("request succeeded despite blocked dial")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("context deadline not honoured, took %v", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "deadline") {
		t.Logf("dial error was %v", err)
	}
}

type blockingDialer struct{}

func (blockingDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// --- live tests (never leave the machine) ----------------------------------

// The production policy never authorises loopback, so the live tests register
// public addresses and inject a dialer that maps each approved public IP to
// the co-hosted local TLS listener. The guard still resolves, validates and
// pins the registered public IP; only the socket endpoint is remapped by the
// test seam.

// TEST-NET ranges are documentation space and forbidden by policy, so the
// fixtures use ordinary globally routable addresses.
const (
	pubIP1 = "70.0.0.10"
	pubIP2 = "70.0.0.11"
	pubIP3 = "70.0.0.12"
	pubIP4 = "70.0.0.13"
	pubIP5 = "70.0.0.14"
)

// mapDialer rewrites dial targets from a registered public IP to a local
// listener address. Any other target is refused.
type mapDialer struct {
	mu      sync.Mutex
	mapping map[string]string
	targets []string
}

func (d *mapDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.targets = append(d.targets, addr)
	repl := d.mapping[net.JoinHostPort(host, port)]
	d.mu.Unlock()
	if repl == "" {
		return nil, errors.New("mapDialer: target not mapped")
	}
	var dd net.Dialer
	return dd.DialContext(ctx, network, repl)
}

func (d *mapDialer) mapTo(publicHost, publicPort, localAddr string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mapping[net.JoinHostPort(publicHost, publicPort)] = localAddr
}

func (d *mapDialer) dialed() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.targets...)
}

// publicResolver answers every name with the single public IP "pub".
type publicResolver struct{ ip string }

func (r publicResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	return []string{r.ip}, nil
}

func TestLiveHTTPSAndRedirects(t *testing.T) {
	var gotAuth, gotCookie []string
	var mu sync.Mutex

	mux := http.NewServeMux()
	mux.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuth = append(gotAuth, r.Header.Get("Authorization"))
		gotCookie = append(gotCookie, r.Header.Get("Cookie"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.HandleFunc("/api/redirect-same", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/data", http.StatusFound)
	})
	mux.HandleFunc("/api/redirect-chain", func(w http.ResponseWriter, r *http.Request) {
		n := r.URL.Query().Get("n")
		if n == "3" {
			http.Redirect(w, r, "/api/data", http.StatusFound)
			return
		}
		http.Redirect(w, r, "/api/redirect-chain?n=4", http.StatusFound)
	})
	mux.HandleFunc("/api/redirect-loop", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/redirect-loop", http.StatusFound)
	})
	mux.HandleFunc("/api/big", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("a", 4096)))
	})
	mux.HandleFunc("/private/leak", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("secret"))
	})

	srv := newLoopbackTLSServer(t, mux)

	dial := &mapDialer{mapping: map[string]string{}}
	dial.mapTo(pubIP1, "443", srv.listenerAddr())

	p := Policy{
		AllowedHosts:        []string{pubIP1},
		AllowedPathPrefixes: []string{"/api"},
		AllowedMethods:      []string{"GET"},
		MaxResponseBytes:    1024,
		Timeout:             5 * time.Second,
	}
	client, err := NewClient(p, WithResolver(publicResolver{ip: pubIP1}), WithDialer(dial))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	injectSkipVerify(t, client)

	// 1. Plain allowed request over real TLS through the injected dialer.
	resp, err := client.Get("https://" + pubIP1 + "/api/data")
	if err != nil {
		t.Fatalf("allowed request failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(body), "ok") {
		t.Fatalf("unexpected body %q", body)
	}

	// 2. Credentials survive a same-origin redirect.
	req, _ := http.NewRequest("GET", "https://"+pubIP1+"/api/redirect-same", nil)
	req.Header.Set("Authorization", "Bearer super-secret")
	req.Header.Set("Cookie", "session=abc")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("same-origin redirect failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	mu.Lock()
	sameOriginAuth := gotAuth[len(gotAuth)-1]
	mu.Unlock()
	if sameOriginAuth != "Bearer super-secret" {
		t.Errorf("same-origin redirect lost Authorization: %q", sameOriginAuth)
	}

	// 3. Cross-origin redirect strips Authorization and Cookie. The target is
	// a second registered public IP mapped to a second local listener.
	crossMux := http.NewServeMux()
	crossMux.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuth = append(gotAuth, r.Header.Get("Authorization"))
		gotCookie = append(gotCookie, r.Header.Get("Cookie"))
		mu.Unlock()
		_, _ = w.Write([]byte("cross"))
	})
	second := newLoopbackTLSServer(t, crossMux)

	crossDial := &mapDialer{mapping: map[string]string{}}
	crossDial.mapTo(pubIP1, "443", srv.listenerAddr())
	crossDial.mapTo(pubIP2, "443", second.listenerAddr())

	cross := Policy{
		AllowedHosts:        []string{pubIP1, pubIP2},
		AllowedPathPrefixes: []string{"/api"},
		AllowedMethods:      []string{"GET"},
		MaxResponseBytes:    1024,
		Timeout:             5 * time.Second,
	}
	crossClient, err := NewClient(cross, WithResolver(publicResolver{ip: pubIP1}), WithDialer(crossDial))
	if err != nil {
		t.Fatal(err)
	}
	injectSkipVerify(t, crossClient)

	// The redirect handler on the first server points at the second origin.
	crossTarget := "https://" + pubIP2 + "/api/data"
	crossMux2 := http.NewServeMux()
	crossMux2.HandleFunc("/api/redirect-cross", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, crossTarget, http.StatusFound)
	})
	crossMux2.HandleFunc("/api/data", mux.ServeHTTP)
	srcSrv := newLoopbackTLSServer(t, crossMux2)
	crossDial.mapTo(pubIP1, "443", srcSrv.listenerAddr())

	req, _ = http.NewRequest("GET", "https://"+pubIP1+"/api/redirect-cross", nil)
	req.Header.Set("Authorization", "Bearer super-secret")
	req.Header.Set("Cookie", "session=abc")
	req.Header.Set("X-Api-Key", "key-123")
	resp, err = crossClient.Do(req)
	if err != nil {
		t.Fatalf("cross-origin redirect failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	mu.Lock()
	crossAuth := gotAuth[len(gotAuth)-1]
	crossCookie := gotCookie[len(gotCookie)-1]
	mu.Unlock()
	if crossAuth != "" || crossCookie != "" {
		t.Errorf("cross-origin redirect leaked credentials: auth=%q cookie=%q", crossAuth, crossCookie)
	}

	// 4. A redirect to an unregistered origin is refused.
	evilDial := &mapDialer{mapping: map[string]string{}}
	evilDial.mapTo(pubIP3, "443", srcSrv.listenerAddr())
	evilDial.mapTo(pubIP2, "443", second.listenerAddr())
	evil := Policy{
		AllowedHosts:        []string{pubIP3},
		AllowedPathPrefixes: []string{"/api"},
		AllowedMethods:      []string{"GET"},
	}
	evilClient, err := NewClient(evil, WithResolver(publicResolver{ip: pubIP3}), WithDialer(evilDial))
	if err != nil {
		t.Fatal(err)
	}
	injectSkipVerify(t, evilClient)
	if _, err := evilClient.Get("https://" + pubIP3 + "/api/redirect-cross"); !errors.Is(err, ErrRedirectDenied) {
		t.Errorf("redirect to unregistered origin: got %v, want ErrRedirectDenied", err)
	}

	// 5. Redirect loops stop at the cap.
	loopDial := &mapDialer{mapping: map[string]string{}}
	loopDial.mapTo(pubIP4, "443", srv.listenerAddr())
	loopPolicy := Policy{
		AllowedHosts:        []string{pubIP4},
		AllowedPathPrefixes: []string{"/api"},
		AllowedMethods:      []string{"GET"},
	}
	loopClient, err := NewClient(loopPolicy, WithResolver(publicResolver{ip: pubIP4}), WithDialer(loopDial))
	if err != nil {
		t.Fatal(err)
	}
	injectSkipVerify(t, loopClient)
	if _, err := loopClient.Get("https://" + pubIP4 + "/api/redirect-loop"); err == nil {
		t.Error("redirect loop terminated without error")
	} else if !errors.Is(err, ErrTooManyRedirects) && !errors.Is(err, ErrRedirectDenied) {
		t.Errorf("redirect loop error = %v, want redirect denial", err)
	}

	// 6. Response body cap.
	bigDial := &mapDialer{mapping: map[string]string{}}
	bigDial.mapTo(pubIP5, "443", srv.listenerAddr())
	bigPolicy := Policy{
		AllowedHosts:        []string{pubIP5},
		AllowedPathPrefixes: []string{"/api"},
		AllowedMethods:      []string{"GET"},
		MaxResponseBytes:    16,
	}
	bigClient, err := NewClient(bigPolicy, WithResolver(publicResolver{ip: pubIP5}), WithDialer(bigDial))
	if err != nil {
		t.Fatal(err)
	}
	injectSkipVerify(t, bigClient)
	resp, err = bigClient.Get("https://" + pubIP5 + "/api/big")
	if err == nil {
		_, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if !errors.Is(readErr, ErrBodyTooLarge) {
			t.Errorf("oversized body read error = %v, want ErrBodyTooLarge", readErr)
		}
	} else if !errors.Is(err, ErrBodyTooLarge) {
		t.Errorf("oversized body error = %v, want ErrBodyTooLarge", err)
	}

	// 7. Path prefix enforced on the live request, on both the explicit and
	// the default-port authority, even after a successful request left an
	// idle connection in the pool.
	if _, err := client.Get("https://" + pubIP1 + ":443/private/leak"); !errors.Is(err, ErrPathNotAllowed) {
		t.Errorf("path outside prefix: got %v, want ErrPathNotAllowed", err)
	}
	if _, err := client.Get("https://" + pubIP1 + "/private/leak"); !errors.Is(err, ErrPathNotAllowed) {
		t.Errorf("path outside prefix (port-less): got %v, want ErrPathNotAllowed", err)
	}

	// 8. Regression: a disallowed method on the initial request is refused
	// even after a successful request reused the pooled connection.
	if _, err := client.Get("https://" + pubIP1 + "/api/data"); err != nil {
		t.Fatalf("warm-up request failed: %v", err)
	}
	badMethodReq, err := http.NewRequest(http.MethodDelete, "https://"+pubIP1+"/api/data", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Do(badMethodReq); !errors.Is(err, ErrMethodNotAllowed) {
		t.Errorf("disallowed method after warm request: got %v, want ErrMethodNotAllowed", err)
	}
}

// injectSkipVerify mutates the *http.Transport underneath the client, through
// the test-only unwrap below, so self-signed loopback certificates are
// accepted. The outer validation wrapper (limitTransport) is left in place.
func injectSkipVerify(t *testing.T, c *http.Client) {
	t.Helper()
	tr := baseTransport(c.Transport)
	if tr == nil {
		t.Fatal("no *http.Transport under client transport")
	}
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{} // test-only, self-signed loopback cert
	}
	tr.TLSClientConfig.InsecureSkipVerify = true
}

// baseTransport unwraps the client transport chain to the underlying
// *http.Transport. Same-package test seam only.
func baseTransport(rt http.RoundTripper) *http.Transport {
	for rt != nil {
		switch tr := rt.(type) {
		case *http.Transport:
			return tr
		case *limitTransport:
			rt = tr.base
		default:
			return nil
		}
	}
	return nil
}

type loopbackServer struct {
	srv *httptest.Server
}

// listenerAddr returns the local endpoint the test dialer maps approved
// public IPs to.
func (s *loopbackServer) listenerAddr() string {
	return strings.TrimPrefix(s.srv.URL, "https://")
}

func newLoopbackTLSServer(t *testing.T, h http.Handler) *loopbackServer {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = false
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return &loopbackServer{srv: srv}
}
