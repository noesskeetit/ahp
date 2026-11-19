package netpolicy

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

// Policy describes the egress allowance of one client. The zero value permits
// nothing: an empty AllowedHosts rejects every request.
type Policy struct {
	// AllowedHosts lists public destinations. Entries are canonical DNS names
	// ("api.example.com") or exact "host:port" pairs; a bare IP literal is
	// accepted only in its exact canonical textual form. Wildcards are
	// rejected. A request may reach these hosts over https: only.
	AllowedHosts []string

	// AllowedMethods is the method allow-list. Empty means the idempotent
	// read set: GET, HEAD, OPTIONS.
	AllowedMethods []string

	// AllowedPathPrefixes are canonical absolute paths matched on segment
	// boundaries: "/api" matches "/api" and "/api/x" but never "/apievil".
	// An empty list permits any path.
	AllowedPathPrefixes []string

	// PrivateHosts registers internal destinations that may be reached over
	// plaintext http: and over RFC1918/ULA address space. It never unlocks
	// loopback, link-local, metadata, multicast or unspecified addresses, and
	// there is deliberately no broader "allow private" toggle.
	PrivateHosts []string

	// MaxResponseBytes caps a response body. Zero selects
	// DefaultMaxResponseBytes; negative is rejected.
	MaxResponseBytes int64

	// Timeout bounds one logical request including redirects. Zero selects
	// DefaultTimeout; negative is rejected.
	Timeout time.Duration
}

// Sentinel errors returned by this package. They are wrapped in the shared
// contracts error vocabulary (see Error), so callers may branch either on the
// sentinel with errors.Is or on the code with contracts.Code.
var (
	// ErrPolicy reports a policy that cannot be compiled into a client.
	ErrPolicy = errors.New("netpolicy: invalid policy")
	// ErrHostNotAllowed reports a destination host outside the allow-list.
	ErrHostNotAllowed = errors.New("netpolicy: host not allowed")
	// ErrSchemeNotAllowed reports plaintext http: to a non-private host.
	ErrSchemeNotAllowed = errors.New("netpolicy: scheme not allowed")
	// ErrMethodNotAllowed reports a method outside the method allow-list.
	ErrMethodNotAllowed = errors.New("netpolicy: method not allowed")
	// ErrPathNotAllowed reports a path outside the prefix allow-list.
	ErrPathNotAllowed = errors.New("netpolicy: path not allowed")
	// ErrAddressForbidden reports a resolved address in a range that may
	// never be dialled (loopback, link-local, metadata, multicast, ...).
	ErrAddressForbidden = errors.New("netpolicy: address forbidden")
	// ErrRedirectDenied reports a redirect that fails re-validation.
	ErrRedirectDenied = errors.New("netpolicy: redirect denied")
	// ErrTooManyRedirects reports more than the allowed redirect hops.
	ErrTooManyRedirects = errors.New("netpolicy: too many redirects")
	// ErrBodyTooLarge reports a response body over Policy.MaxResponseBytes.
	ErrBodyTooLarge = errors.New("netpolicy: response body exceeds limit")
)

// Error is the error shape of this package: a wrapped sentinel plus a
// human-readable message that never carries URLs, credentials or body content.
type Error struct {
	Sentinel error
	Message  string
}

// Error implements error.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message == "" {
		return e.Sentinel.Error()
	}
	return e.Sentinel.Error() + ": " + e.Message
}

// Unwrap exposes the sentinel for errors.Is.
func (e *Error) Unwrap() error { return e.Sentinel }

// Code returns the stable platform error code for err.
func (e *Error) Code() string {
	if errors.Is(e, ErrPolicy) {
		return codeInvalid
	}
	return codeForbidden
}

// Stable codes matching the platform error vocabulary.
const (
	codeInvalid  = "INVALID_ARGUMENT"
	codeForbidden = "FORBIDDEN"
)

func policyErr(format string, args ...any) error {
	return &Error{Sentinel: ErrPolicy, Message: fmt.Sprintf(format, args...)}
}

func denied(err error, format string, args ...any) error {
	return &Error{Sentinel: err, Message: fmt.Sprintf(format, args...)}
}

// IsForbiddenIP reports whether addr belongs to a range that must never be
// dialled from an egress client, regardless of policy: loopback (in any family
// or IPv4-mapped form), link-local unicast and multicast — which includes the
// 169.254.169.254 cloud metadata endpoint and its mapped form — unspecified,
// multicast, CGNAT, benchmark, documentation and reserved space.
//
// RFC1918 and IPv6 ULA are deliberately NOT reported here: they are permitted
// only for destinations registered in Policy.PrivateHosts, which is a separate
// check (see IsPrivateRangeIP).
func IsForbiddenIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4 // canonicalise ::ffff:a.b.c.d so it behaves like a.b.c.d
	} else if ip.To16() == nil {
		return true
	}

	switch {
	case ip.IsUnspecified(),
		ip.IsLoopback(),
		ip.IsMulticast(),
		ip.IsInterfaceLocalMulticast(),
		ip.IsLinkLocalMulticast(),
		ip.IsLinkLocalUnicast():
		return true
	}

	if v4 := ip.To4(); v4 != nil {
		switch {
		case v4[0] == 0: // 0.0.0.0/8 "this host on this network"
			return true
		case v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127: // CGNAT 100.64.0.0/10
			return true
		case v4[0] == 192 && v4[1] == 0 && v4[2] == 0: // 192.0.0.0/24 IETF protocol space
			return true
		case v4[0] == 192 && v4[1] == 0 && v4[2] == 2: // TEST-NET-1
			return true
		case v4[0] == 198 && (v4[1] == 18 || v4[1] == 19): // benchmarking 198.18.0.0/15
			return true
		case v4[0] == 198 && v4[1] == 51 && v4[2] == 100: // TEST-NET-2
			return true
		case v4[0] == 203 && v4[1] == 0 && v4[2] == 113: // TEST-NET-3
			return true
		case v4[0] >= 240: // reserved and broadcast
			return true
		}
		return false
	}

	// 2001:db8::/32 documentation space is never a real destination.
	if ip[0] == 0x20 && ip[1] == 0x01 && ip[2] == 0x0d && ip[3] == 0xb8 {
		return true
	}
	if ip.IsPrivate() {
		return false // ULA is policy-gated, not absolutely forbidden.
	}
	return !ip.IsGlobalUnicast()
}

// IsMetadataIP reports whether addr is a well-known cloud metadata endpoint.
// Every metadata address is also forbidden by IsForbiddenIP; this helper
// exists for precise errors and tests.
func IsMetadataIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		return v4[0] == 169 && v4[1] == 254 && v4[2] == 169 && v4[3] == 254
	}
	// Provider-specific IPv6 metadata endpoint.
	return ip.String() == "fd00:ec2::254"
}

// IsPrivateRangeIP reports whether addr is in RFC1918 or IPv6 ULA space — the
// only non-global space a registered PrivateHosts entry may unlock.
func IsPrivateRangeIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		return v4[0] == 10 ||
			(v4[0] == 172 && v4[1] >= 16 && v4[1] <= 31) ||
			(v4[0] == 192 && v4[1] == 168)
	}
	if ip.To16() == nil {
		return false
	}
	return ip.IsPrivate()
}

// parseIPLiteral parses a bare or bracketed IP literal, returning nil when the
// host is not an IP literal at all.
func parseIPLiteral(host string) net.IP {
	if host == "" {
		return nil
	}
	if host[0] == '[' {
		if len(host) < 3 || host[len(host)-1] != ']' {
			return nil
		}
		return net.ParseIP(host[1 : len(host)-1])
	}
	if strings.Contains(host, ":") {
		// An unbracketed IPv6 literal is not a valid URL authority.
		return nil
	}
	return net.ParseIP(host)
}

// canonicalDNSName validates and canonicalises an ASCII DNS name: lower case,
// a single trailing dot removed, labels bounded and free of illegal bytes.
func canonicalDNSName(host string) (string, error) {
	if host == "" {
		return "", policyErr("host must not be empty")
	}
	if len(host) > maxAuthorityBytes {
		return "", policyErr("host name exceeds %d bytes", maxAuthorityBytes)
	}
	if strings.ContainsAny(host, "%@\\") {
		return "", policyErr("host %q contains forbidden characters", host)
	}
	name := strings.ToLower(host)
	name = strings.TrimSuffix(name, ".")
	if name == "" {
		return "", policyErr("host must not be the DNS root")
	}
	if strings.Contains(name, "..") || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") {
		return "", policyErr("host %q has an empty label", host)
	}
	for _, label := range strings.Split(name, ".") {
		if label == "" || len(label) > 63 {
			return "", policyErr("host %q has an empty or over-long label", host)
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return "", policyErr("host %q has a hyphen-bounded label", host)
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			switch {
			case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			default:
				return "", policyErr("host %q contains the illegal byte %q", host, c)
			}
		}
	}
	return name, nil
}

// canonicalHost canonicalises the host part of an authority: IP literals are
// normalised to their canonical textual form, DNS names are case folded and
// stripped of a trailing dot.
func canonicalHost(host string) (string, error) {
	if host == "" {
		return "", policyErr("host must not be empty")
	}
	if host[0] == '[' {
		ip := parseIPLiteral(host)
		if ip == nil {
			return "", policyErr("malformed IPv6 literal %q", host)
		}
		if ip.To4() != nil {
			return "", policyErr("IPv4 address %q must not be bracketed", host)
		}
		return "[" + ip.String() + "]", nil
	}
	if strings.Contains(host, ":") {
		// An unbracketed IPv6 literal is legitimate; only a raw port is not.
		// Parse before rejecting so "2001:db8::1" is not misread as host:port.
		if ip := net.ParseIP(host); ip != nil && ip.To4() == nil {
			return "[" + ip.String() + "]", nil
		}
		return "", policyErr("host %q must not contain a raw port; use the URL authority", host)
	}
	if ip := parseIPLiteral(host); ip != nil {
		return ip.String(), nil
	}
	return canonicalDNSName(host)
}

// canonicalEntry canonicalises a policy host entry, which may carry an
// explicit port.
func canonicalEntry(entry string) (string, error) {
	if entry == "" {
		return "", policyErr("host entry must not be empty")
	}
	if strings.ContainsAny(entry, "@/?#%") {
		return "", policyErr("host entry %q contains forbidden characters", entry)
	}
	if strings.Contains(entry, "*") {
		return "", policyErr("wildcard host entry %q is not allowed", entry)
	}
	host, port, err := splitHostPort(entry)
	if err != nil {
		return "", err
	}
	canon, err := canonicalHost(host)
	if err != nil {
		return "", policyErr("host entry %q: %v", entry, err)
	}
	// An entry naming an absolutely-forbidden address (documentation space
	// included) is rejected at canonicalisation time.
	if ip := parseIPLiteral(canon); ip != nil && IsForbiddenIP(ip) {
		return "", policyErr("host entry %q names an address that is never reachable", entry)
	}
	if port == "" {
		return canon, nil
	}
	return canon + ":" + port, nil
}

// splitHostPort splits an entry into canonical host and explicit port. It
// accepts "host", "host:port", "[v6]" and "[v6]:port".
func splitHostPort(entry string) (host, port string, err error) {
	if strings.HasPrefix(entry, "[") {
		end := strings.Index(entry, "]")
		if end < 0 {
			return "", "", policyErr("host entry %q has an unterminated IPv6 literal", entry)
		}
		host = entry[:end+1]
		rest := entry[end+1:]
		if rest == "" {
			return host, "", nil
		}
		if rest[0] != ':' {
			return "", "", policyErr("host entry %q has trailing garbage after the IPv6 literal", entry)
		}
		port = rest[1:]
	} else {
		if i := strings.LastIndex(entry, ":"); i >= 0 {
			host, port = entry[:i], entry[i+1:]
			if !allDigits(port) {
				// Not a port at all: a stray colon inside a DNS name.
				return "", "", policyErr("host entry %q has an invalid port", entry)
			}
		} else {
			host = entry
		}
	}
	if port != "" {
		if !allDigits(port) || len(port) > 5 {
			return "", "", policyErr("host entry %q has an invalid port", entry)
		}
		if _, perr := net.LookupPort("tcp", port); perr != nil {
			return "", "", policyErr("host entry %q has an invalid port: %v", entry, perr)
		}
	}
	return host, port, nil
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// entryMatches reports whether the canonical request host and port are listed
// in list. An entry without an explicit port binds the scheme default port; an
// entry with a port must match it exactly, which is what keeps an unusual port
// unreachable unless it was registered deliberately.
func entryMatches(list []string, host, port, scheme string) bool {
	for _, entry := range list {
		entryHost, entryPort := splitCanonical(entry)
		if entryHost != host {
			continue
		}
		if entryPort != "" {
			return entryPort == port
		}
		if port == "" || port == defaultPort(scheme) {
			return true
		}
	}
	return false
}

// splitCanonical splits an already canonicalised entry.
func splitCanonical(entry string) (host, port string) {
	if strings.HasPrefix(entry, "[") {
		end := strings.Index(entry, "]")
		if end < 0 {
			return entry, ""
		}
		host = entry[:end+1]
		if rest := entry[end+1:]; strings.HasPrefix(rest, ":") {
			port = rest[1:]
		}
		return host, port
	}
	if i := strings.LastIndex(entry, ":"); i >= 0 {
		return entry[:i], entry[i+1:]
	}
	return entry, ""
}

func defaultPort(scheme string) string {
	switch scheme {
	case "https":
		return "443"
	case "http":
		return "80"
	}
	return ""
}

// cleanAbsolutePath resolves "." and ".." segments and collapses repeated
// slashes. The result always starts with "/" and never contains "." or "..".
func cleanAbsolutePath(p string) (string, error) {
	if !strings.HasPrefix(p, "/") {
		return "", policyErr("path %q is not absolute", p)
	}
	for _, seg := range strings.Split(p, "/") {
		for i := 0; i < len(seg); i++ {
			if seg[i] < 0x20 || seg[i] == 0x7f {
				return "", denied(ErrPathNotAllowed, "path contains a control character")
			}
		}
	}
	trailing := strings.HasSuffix(p, "/")
	segments := strings.Split(p, "/")
	out := make([]string, 0, len(segments))
	for _, seg := range segments {
		switch seg {
		case "", ".":
			continue
		case "..":
			if len(out) > 0 {
				out = out[:len(out)-1]
			}
		default:
			out = append(out, seg)
		}
	}
	res := "/" + strings.Join(out, "/")
	if trailing && res != "/" {
		res += "/"
	}
	return res, nil
}

// prefixMatches reports whether path is at or below prefix on a segment
// boundary, so "/api" never matches "/apievil".
func prefixMatches(prefix, path string) bool {
	if prefix == "" {
		return true
	}
	if path == prefix {
		return true
	}
	if strings.HasPrefix(path, prefix) {
		return path[len(prefix)] == '/'
	}
	return false
}

// canonicalPrefix normalises a configured path prefix.
func canonicalPrefix(prefix string) (string, error) {
	if prefix == "" || prefix[0] != '/' {
		return "", policyErr("path prefix %q must be an absolute path", prefix)
	}
	if strings.Contains(prefix, "%") {
		return "", policyErr("path prefix %q must not be percent-encoded", prefix)
	}
	if strings.ContainsAny(prefix, "?#") {
		return "", policyErr("path prefix %q must not contain a query or fragment", prefix)
	}
	clean, err := cleanAbsolutePath(prefix)
	if err != nil {
		return "", policyErr("path prefix %q: %v", prefix, err)
	}
	clean = strings.TrimSuffix(clean, "/")
	return clean, nil
}

// canonicalRequestPath decodes and normalises a request path, rejecting
// escapes that could smuggle separators or control bytes.
func canonicalRequestPath(u *url.URL) (string, error) {
	if strings.Contains(u.RawPath, "%2f") || strings.Contains(u.RawPath, "%2F") ||
		strings.Contains(u.RawPath, "%5c") || strings.Contains(u.RawPath, "%5C") {
		return "", denied(ErrPathNotAllowed, "escaped path separator is not permitted")
	}
	if strings.ContainsRune(u.Path, 0) {
		return "", denied(ErrPathNotAllowed, "path contains a NUL byte")
	}
	path := u.Path
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		return "", denied(ErrPathNotAllowed, "path is not absolute")
	}
	return cleanAbsolutePath(path)
}
