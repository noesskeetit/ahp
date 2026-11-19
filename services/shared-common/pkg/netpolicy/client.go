package netpolicy

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Default limits applied when the corresponding Policy field is zero.
const (
	// DefaultMaxResponseBytes caps a response body at 8 MiB.
	DefaultMaxResponseBytes int64 = 8 << 20
	// DefaultTimeout bounds one logical request, redirects included.
	DefaultTimeout = 30 * time.Second
	// DefaultDialTimeout bounds TCP connection establishment.
	DefaultDialTimeout = 5 * time.Second
	// DefaultTLSHandshakeTimeout bounds the TLS handshake.
	DefaultTLSHandshakeTimeout = 10 * time.Second
	// DefaultExpectContinueTimeout bounds the wait for a 100-continue.
	DefaultExpectContinueTimeout = time.Second
	// DefaultIdleConnTimeout bounds an idle keep-alive connection.
	DefaultIdleConnTimeout = 60 * time.Second
	// DefaultKeepAlive bounds TCP keep-alive probes.
	DefaultKeepAlive = 30 * time.Second

	// maxRedirects is the hard cap on redirect hops per logical request.
	maxRedirects = 3
	// maxAuthorityBytes bounds a canonical host name.
	maxAuthorityBytes = 253
	// maxPathBytes bounds a request path.
	maxPathBytes = 4096
)

// credentialHeaders are stripped whenever a redirect crosses an origin.
var credentialHeaders = []string{
	"Authorization",
	"Cookie",
	"Proxy-Authorization",
	"Proxy-Authenticate",
	"Www-Authenticate",
	"X-Api-Key",
	"X-Auth-Token",
	"X-Xsrf-Token",
	"X-Csrf-Token",
}

// Resolver resolves a host name to IP addresses.
type Resolver interface {
	LookupHost(ctx context.Context, host string) (addrs []string, err error)
}

// Dialer establishes a connection to a network address.
type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// Option customises the client built by NewClient. It exists so tests can
// inject a deterministic resolver and dial path; production callers normally
// pass none.
type Option func(*config)

type config struct {
	resolver Resolver
	dialer   Dialer
}

// WithResolver installs a custom name resolution path. This is the seam tests
// use to simulate DNS rebinding.
func WithResolver(r Resolver) Option {
	return func(c *config) { c.resolver = r }
}

// WithDialer installs a custom dial path. Resolution still goes through the
// resolver (or the platform default) and every address is still validated.
func WithDialer(d Dialer) Option {
	return func(c *config) { c.dialer = d }
}

// compiled is a Policy after validation and canonicalisation.
type compiled struct {
	allowedHosts []string
	privateHosts []string
	methods      map[string]struct{}
	prefixes     []string
	maxBody      int64
	timeout      time.Duration
}

// compile validates the policy and canonicalises every entry.
func (p Policy) compile() (*compiled, error) {
	if p.MaxResponseBytes < 0 {
		return nil, policyErr("MaxResponseBytes must not be negative")
	}
	if p.Timeout < 0 {
		return nil, policyErr("Timeout must not be negative")
	}

	c := &compiled{
		allowedHosts: make([]string, 0, len(p.AllowedHosts)),
		privateHosts: make([]string, 0, len(p.PrivateHosts)),
		methods:      make(map[string]struct{}, len(p.AllowedMethods)+3),
		prefixes:     make([]string, 0, len(p.AllowedPathPrefixes)),
		maxBody:      p.MaxResponseBytes,
		timeout:      p.Timeout,
	}

	for _, entry := range p.AllowedHosts {
		key, err := canonicalEntry(entry)
		if err != nil {
			return nil, err
		}
		if err := checkEntryAddress(key, false); err != nil {
			return nil, err
		}
		if contains(c.allowedHosts, key) {
			return nil, policyErr("duplicate AllowedHosts entry %q", entry)
		}
		c.allowedHosts = append(c.allowedHosts, key)
	}
	for _, entry := range p.PrivateHosts {
		key, err := canonicalEntry(entry)
		if err != nil {
			return nil, err
		}
		if err := checkEntryAddress(key, true); err != nil {
			return nil, err
		}
		if contains(c.privateHosts, key) {
			return nil, policyErr("duplicate PrivateHosts entry %q", entry)
		}
		c.privateHosts = append(c.privateHosts, key)
	}

	methods := p.AllowedMethods
	if len(methods) == 0 {
		methods = []string{http.MethodGet, http.MethodHead, http.MethodOptions}
	}
	for _, m := range methods {
		up := strings.ToUpper(strings.TrimSpace(m))
		if !validMethodToken(up) {
			return nil, policyErr("invalid HTTP method %q in AllowedMethods", m)
		}
		c.methods[up] = struct{}{}
	}

	for _, prefix := range p.AllowedPathPrefixes {
		canon, err := canonicalPrefix(prefix)
		if err != nil {
			return nil, err
		}
		c.prefixes = append(c.prefixes, canon)
	}
	sort.Strings(c.prefixes)

	if c.maxBody == 0 {
		c.maxBody = DefaultMaxResponseBytes
	}
	if c.timeout == 0 {
		c.timeout = DefaultTimeout
	}
	return c, nil
}

// checkEntryAddress rejects policy entries that name an unreachable address.
// Public entries may only name globally routable addresses; private entries
// may additionally name RFC1918/ULA space but never loopback, link-local,
// metadata, multicast or unspecified addresses.
func checkEntryAddress(entry string, private bool) error {
	host, _ := splitCanonical(entry)
	ip := parseIPLiteral(host)
	if ip == nil {
		return nil
	}
	if IsForbiddenIP(ip) {
		return policyErr("host entry %q names an address that is never reachable", entry)
	}
	if !private && IsPrivateRangeIP(ip) {
		return policyErr("host entry %q names a private address; register it in PrivateHosts", entry)
	}
	if !private && !ip.IsGlobalUnicast() {
		return policyErr("host entry %q names a non-global address; register it in PrivateHosts", entry)
	}
	return nil
}

// hasHost reports whether any canonical entry in list names host, ignoring the
// port an entry may have pinned.
func hasHost(list []string, host string) bool {
	for _, entry := range list {
		if h, _ := splitCanonical(entry); h == host {
			return true
		}
	}
	return false
}

func contains(list []string, v string) bool {
	for _, e := range list {
		if e == v {
			return true
		}
	}
	return false
}

func validMethodToken(m string) bool {
	if m == "" {
		return false
	}
	for i := 0; i < len(m); i++ {
		if !isTokenByte(m[i]) {
			return false
		}
	}
	return true
}

func isTokenByte(c byte) bool {
	switch c {
	case '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=', '{', '}', ' ', '\t':
		return false
	}
	return c >= 0x21 && c <= 0x7e
}

// Validate reports whether the policy can be compiled into a client.
func (p Policy) Validate() error {
	_, err := p.compile()
	return err
}

// ValidateURL reports whether u and method fall inside the policy. It checks
// scheme, authority, method and path, but not resolved addresses: those are
// validated at dial time by the transport. The client transport applies the
// same gate to every request, including the initial one. A nil URL is
// rejected.
func (p Policy) ValidateURL(u *url.URL, method string) error {
	c, err := p.compile()
	if err != nil {
		return err
	}
	return c.validateURL(u, method)
}

// NewClient returns an *http.Client that enforces policy on every request and
// every redirect hop. The returned client never consults environment proxies
// and never follows more than three redirects.
func NewClient(policy Policy, opts ...Option) (*http.Client, error) {
	c, err := policy.compile()
	if err != nil {
		return nil, err
	}
	cfg := &config{}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	resolver := cfg.resolver
	if resolver == nil {
		resolver = &net.Resolver{}
	}
	dialer := cfg.dialer
	if dialer == nil {
		dialer = &net.Dialer{
			Timeout:   DefaultDialTimeout,
			KeepAlive: DefaultKeepAlive,
		}
	}

	guard := &guardDialer{resolver: resolver, dialer: dialer, policy: c}

	transport := &http.Transport{
		// Environment proxies are never consulted: a proxy would both bypass
		// address validation and leak the destination.
		Proxy:                 nil,
		DialContext:           guard.DialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       DefaultIdleConnTimeout,
		TLSHandshakeTimeout:   DefaultTLSHandshakeTimeout,
		ExpectContinueTimeout: DefaultExpectContinueTimeout,
		// TLS config is left nil so the transport derives SNI and verification
		// from the URL host, which is the original host even though the socket
		// is pinned to a validated IP.
	}

	client := &http.Client{
		Transport: &limitTransport{base: transport, policy: c, maxBody: c.maxBody},
		Timeout:   c.timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return c.checkRedirect(req, via)
		},
	}
	return client, nil
}

// validateURL applies the request-level gate.
func (c *compiled) validateURL(u *url.URL, method string) error {
	if u == nil {
		return policyErr("url must not be nil")
	}
	if u.Opaque != "" {
		return denied(ErrSchemeNotAllowed, "opaque URLs are not permitted")
	}
	if u.User != nil {
		return denied(ErrHostNotAllowed, "userinfo in the URL is never permitted")
	}
	if u.Fragment != "" || strings.Contains(u.RawPath, "#") {
		return denied(ErrPathNotAllowed, "URL fragments are never permitted")
	}

	scheme := strings.ToLower(u.Scheme)
	if scheme == "" {
		return denied(ErrSchemeNotAllowed, "URL scheme must be explicit")
	}

	rawHost := u.Hostname()
	if rawHost == "" {
		return denied(ErrHostNotAllowed, "URL has no host")
	}
	if strings.Contains(u.Host, "@") {
		return denied(ErrHostNotAllowed, "userinfo in the URL is never permitted")
	}
	// url.URL does not decode escapes in the authority, so an encoded
	// authority would be a smuggling attempt rather than a valid request.
	if strings.Contains(u.Host, "%") {
		return denied(ErrHostNotAllowed, "encoded characters in the URL authority are not permitted")
	}

	host, err := canonicalHost(rawHost)
	if err != nil {
		return denied(ErrHostNotAllowed, "%v", err)
	}
	port := u.Port()

	private := entryMatches(c.privateHosts, host, port, scheme)
	public := entryMatches(c.allowedHosts, host, port, scheme)
	switch {
	case private:
		// Registered internal destination: http: is allowed.
	case public:
		if scheme != "https" {
			return denied(ErrSchemeNotAllowed, "host %q is only reachable over https", host)
		}
	default:
		return denied(ErrHostNotAllowed, "host %q is not registered", host)
	}

	if _, ok := c.methods[method]; !ok {
		return denied(ErrMethodNotAllowed, "method %q is not allowed", method)
	}

	if len(u.Path) > maxPathBytes {
		return denied(ErrPathNotAllowed, "path exceeds %d bytes", maxPathBytes)
	}
	path, err := canonicalRequestPath(u)
	if err != nil {
		return err
	}
	if len(c.prefixes) > 0 {
		for _, prefix := range c.prefixes {
			if prefixMatches(prefix, path) {
				return nil
			}
		}
		return denied(ErrPathNotAllowed, "path %q is outside the allowed prefixes", path)
	}
	return nil
}

// checkRedirect re-validates a redirect target and strips credentials that
// cross an origin boundary.
func (c *compiled) checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > maxRedirects {
		return denied(ErrTooManyRedirects, "more than %d redirects", maxRedirects)
	}
	if err := c.validateURL(req.URL, req.Method); err != nil {
		return denied(ErrRedirectDenied, "%v", err)
	}
	prev := via[len(via)-1]
	if sameOrigin(prev.URL, req.URL) {
		return nil
	}
	for _, h := range credentialHeaders {
		req.Header.Del(h)
	}
	// A cross-origin redirect must not keep a body or its framing headers.
	if req.Body != nil && req.Body != http.NoBody {
		if err := req.Body.Close(); err != nil {
			return denied(ErrRedirectDenied, "cannot release redirect body: %v", err)
		}
		req.Body = http.NoBody
		req.ContentLength = 0
		req.Header.Del("Content-Length")
		req.Header.Del("Content-Type")
		req.Header.Del("Transfer-Encoding")
	}
	return nil
}

// sameOrigin compares scheme, host and port, treating a scheme-default port as
// equivalent to an empty one.
func sameOrigin(a, b *url.URL) bool {
	if a == nil || b == nil {
		return false
	}
	if !strings.EqualFold(a.Scheme, b.Scheme) {
		return false
	}
	if !strings.EqualFold(a.Hostname(), b.Hostname()) {
		return false
	}
	return effectivePort(a) == effectivePort(b)
}

func effectivePort(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	return defaultPort(strings.ToLower(u.Scheme))
}

// guardDialer resolves the request host, validates every returned address and
// then pins the connection to one validated IP while leaving TLS to present
// the original host.
type guardDialer struct {
	resolver Resolver
	dialer   Dialer
	policy   *compiled
}

func (g *guardDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, denied(ErrHostNotAllowed, "malformed dial address")
	}
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, denied(ErrAddressForbidden, "only TCP networks may be dialled")
	}

	canon, err := canonicalHost(host)
	if err != nil {
		return nil, denied(ErrHostNotAllowed, "%v", err)
	}
	// The dial target must still be an allowed destination: a connection may
	// only be established for a request that already passed validateURL. The
	// scheme is unknown here, so either default port may satisfy an entry that
	// did not pin one. A public entry additionally pins its scheme default, so
	// a test dialer mapping an allowed public IP to a local listener still
	// dials the registered port.
	if !entryMatches(g.policy.allowedHosts, canon, port, "https") &&
		!entryMatches(g.policy.privateHosts, canon, port, "http") &&
		!entryMatches(g.policy.privateHosts, canon, port, "https") &&
		!hasHost(g.policy.allowedHosts, canon) {
		return nil, denied(ErrHostNotAllowed, "host %q is not registered", canon)
	}

	// An IP literal needs no resolution and is never dialled unless its exact
	// canonical form is registered above.
	// A dial target that is already an IP literal is validated directly; no
	// resolution (and therefore no second lookup) ever happens for literals.
	if ip := parseIPLiteral(canon); ip != nil {
		if err := g.checkAddress(ip, canon); err != nil {
			return nil, err
		}
		return g.dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
	}

	addrs, err := g.resolver.LookupHost(ctx, canon)
	if err != nil {
		return nil, denied(ErrAddressForbidden, "host %q could not be resolved", canon)
	}
	if len(addrs) == 0 {
		return nil, denied(ErrAddressForbidden, "host %q has no addresses", canon)
	}

	// Validate every resolved address before dialling any of them: a resolver
	// that mixes public and internal answers is rejected outright rather than
	// silently narrowed, which closes the DNS-rebinding window.
	ips := make([]net.IP, 0, len(addrs))
	for _, raw := range addrs {
		ip := net.ParseIP(raw)
		if ip == nil {
			return nil, denied(ErrAddressForbidden, "resolver returned a non-IP address for %q", canon)
		}
		if err := g.checkAddress(ip, canon); err != nil {
			return nil, err
		}
		ips = append(ips, ip)
	}
	// Dial the pinned, already-validated IP; no second lookup ever happens.
	var lastErr error
	for _, ip := range ips {
		conn, derr := g.dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if derr != nil {
			lastErr = derr
			continue
		}
		return conn, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, denied(ErrAddressForbidden, "no usable address for %q", canon)
}

// checkAddress applies the address gate. Loopback, link-local, metadata,
// multicast, unspecified and other non-global ranges are always denied;
// RFC1918/ULA is denied unless the host is registered in PrivateHosts.
func (g *guardDialer) checkAddress(ip net.IP, host string) error {
	if IsMetadataIP(ip) {
		return denied(ErrAddressForbidden, "metadata endpoint is not reachable")
	}
	if IsForbiddenIP(ip) {
		return denied(ErrAddressForbidden, "address %s is in a forbidden range", ip)
	}
	if IsPrivateRangeIP(ip) {
		// The dial-time host carries no port, so registration is checked
		// without one; an entry that pinned a port still authorises the name.
		if !hasHost(g.policy.privateHosts, host) {
			return denied(ErrAddressForbidden, "host %q resolves to private space but is not registered in PrivateHosts", host)
		}
	}
	return nil
}

// limitTransport enforces the request-level policy gate on every hop and
// wraps responses with a hard byte cap.
type limitTransport struct {
	base    http.RoundTripper
	policy  *compiled
	maxBody int64
}

func (t *limitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Every request reaching the transport — the initial one and any reused
	// connection — must pass the URL/method gate; dial-time checks alone
	// cannot see the path or method.
	if err := t.policy.validateURL(req.URL, req.Method); err != nil {
		return nil, err
	}
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp == nil {
		return resp, err
	}
	if resp.Body == nil || resp.Body == http.NoBody {
		return resp, nil
	}
	if resp.ContentLength > t.maxBody {
		_ = resp.Body.Close()
		return nil, denied(ErrBodyTooLarge, "declared body of %d bytes exceeds the %d byte limit",
			resp.ContentLength, t.maxBody)
	}
	resp.Body = &limitedBody{
		Reader: io.LimitReader(resp.Body, t.maxBody+1),
		closer: resp.Body,
		max:    t.maxBody,
	}
	return resp, nil
}

// limitedBody enforces the cap and reports overflow on Read or Close.
type limitedBody struct {
	Reader io.Reader
	closer io.Closer
	max    int64
	read   int64
	over   bool
}

func (b *limitedBody) Read(p []byte) (int, error) {
	if b.over {
		return 0, denied(ErrBodyTooLarge, "body exceeds the %d byte limit", b.max)
	}
	n, err := b.Reader.Read(p)
	b.read += int64(n)
	if b.read > b.max {
		b.over = true
		return n, denied(ErrBodyTooLarge, "body exceeds the %d byte limit", b.max)
	}
	return n, err
}

func (b *limitedBody) Close() error {
	cerr := b.closer.Close()
	if b.over || b.read > b.max {
		return denied(ErrBodyTooLarge, "body exceeds the %d byte limit", b.max)
	}
	return cerr
}

// Drain reads a response body up to the policy cap and reports overflow
// instead of buffering without bound. It is a convenience for callers that
// want the body of a client already configured with MaxResponseBytes.
func Drain(resp *http.Response, max int64) ([]byte, error) {
	if resp == nil || resp.Body == nil {
		return nil, policyErr("response has no body")
	}
	if max <= 0 {
		max = DefaultMaxResponseBytes
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, denied(ErrBodyTooLarge, "body exceeds the %d byte limit", max)
	}
	return data, nil
}

// compile-time assertions
var (
	_ http.RoundTripper = (*limitTransport)(nil)
	_ io.ReadCloser     = (*limitedBody)(nil)
)
