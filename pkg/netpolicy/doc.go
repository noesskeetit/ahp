// Package netpolicy implements a hardened, dependency-free HTTP egress client
// for outbound calls made on behalf of AHP workloads (webhooks, external tool
// bridges, MCP gateways).
//
// # What it enforces
//
// A Policy declares which hosts, methods and path prefixes a client may reach.
// NewClient returns an *http.Client whose every request — including each
// redirect hop — passes the same gate:
//
//   - HTTPS only. Plaintext http: is accepted exclusively for destinations
//     registered in Policy.PrivateHosts.
//   - No userinfo, no fragment, no wildcard entries, no encoded-authority
//     tricks.
//   - IP literals are rejected unless the exact canonical textual form is
//     registered in AllowedHosts or PrivateHosts.
//   - Hostnames are canonicalised (ASCII case folded, trailing dot removed)
//     before matching, and only after every percent-escape has been decoded,
//     so "%2f", "%00" and friends cannot smuggle an authority past the
//     matcher.
//   - Path prefixes match on segment boundaries: "/api" matches "/api" and
//     "/api/x" but never "/apievil". Escaped separators ("%2f", "%5c") and
//     NUL or control bytes in a path are rejected outright.
//   - Dial-time resolution validates every returned address before a socket is
//     opened, and the connection is then pinned to a validated IP while TLS
//     still presents the original SNI and Host header. That closes the
//     DNS-rebinding gap between validation and connection.
//   - Loopback, link-local (including the 169.254.169.254 cloud metadata
//     endpoint, fe80::/64 and the ::ffff:169.254.0.0/96 mapped form),
//     unspecified, multicast and other non-global ranges are always denied —
//     even for a registered private host. RFC1918/ULA space is permitted only
//     for destinations explicitly listed in PrivateHosts.
//   - Environment proxies are never consulted, so proxy bypass and
//     proxy-mediated SSRF are structurally impossible.
//   - Redirects are capped at three and re-validated. Authorization, Cookie,
//     Proxy-Authorization, X-Api-Key and the other credential headers are
//     dropped whenever the origin (scheme, host, port) changes.
//   - Response bodies are capped by MaxResponseBytes; a body that exceeds the
//     cap surfaces as ErrBodyTooLarge on Read/Close.
//
// # What it does not do
//
// This package is an HTTP client, not a sandbox. It cannot constrain code that
// opens raw sockets, spawns processes or resolves names through another
// runtime. Full egress enforcement still requires network policy at the
// deployment boundary (Kubernetes NetworkPolicy, mesh egress gateway or an
// enforced proxy); this client is defence in depth, not the boundary.
//
// # Testing hooks
//
// Resolver and Dialer are exported so tests can inject a deterministic name
// resolution and dial path without touching the real network. No test in this
// package reaches the internet.
package netpolicy
