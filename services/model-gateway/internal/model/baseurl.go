package model

// base_url configuration is refused here, at the boundary where the client is
// built, rather than repaired. Two properties make repair the wrong answer:
// AllowedPathPrefixes is derived from the very same string, so a path mistake is
// invisible to the path gate (it would allow exactly what the mistake produced);
// and any "fix" changes which endpoint egress reaches, which is precisely the
// fact an SSRF decision is audited against. Each branch therefore carries its own
// stable reason so operators can script on it.

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// Stable reason classes for a refused base_url.
const (
	ReasonBaseURLDuplicatedPath   = "model_base_url_duplicated_completion_path"
	ReasonBaseURLNotAbsolute      = "model_base_url_not_absolute"
	ReasonBaseURLUnparseable      = "model_base_url_unparseable"
	ReasonBaseURLPathNotCanonical = "model_base_url_path_not_canonical"
	ReasonBaseURLPortEmpty        = "model_base_url_port_empty"
	ReasonBaseURLPortInvalid      = "model_base_url_port_invalid"
	ReasonBaseURLCredentials      = "model_base_url_carries_credentials"
	ReasonBaseURLQueryOrFragment  = "model_base_url_carries_query_or_fragment"
)

// completionSuffix is what completionsPath appends to the configured prefix.
const completionSuffix = "/chat/completions"

// BaseURLConfigError reports a base_url the client refuses to be built from. The
// detail never carries credentials: a refused URL may be journalled verbatim.
type BaseURLConfigError struct {
	Reason string
	Detail string
}

func (e *BaseURLConfigError) Error() string {
	return "model: qwen base_url rejected (" + e.Reason + "): " + e.Detail
}

// IsBaseURLConfigError reports whether err is (or wraps) a base_url refusal.
func IsBaseURLConfigError(err error) bool {
	var target *BaseURLConfigError
	return errors.As(err, &target)
}

// BaseURLConfigReason returns the reason class of a base_url refusal, or "".
func BaseURLConfigReason(err error) string {
	var target *BaseURLConfigError
	if errors.As(err, &target) {
		return target.Reason
	}
	return ""
}

// baseURLConfigReasons is the set of reason classes this file can produce.
var baseURLConfigReasons = map[string]bool{
	ReasonBaseURLDuplicatedPath:   true,
	ReasonBaseURLNotAbsolute:      true,
	ReasonBaseURLUnparseable:      true,
	ReasonBaseURLPathNotCanonical: true,
	ReasonBaseURLPortEmpty:        true,
	ReasonBaseURLPortInvalid:      true,
	ReasonBaseURLCredentials:      true,
	ReasonBaseURLQueryOrFragment:  true,
}

// IsBaseURLConfigReason reports whether a carried reason class came from a
// refused base_url, so a durable record can keep it instead of a generic one.
func IsBaseURLConfigReason(reason string) bool { return baseURLConfigReasons[reason] }

// parseQwenBaseURL parses the configured base_url and refuses the shapes that
// would silently change the destination. The authority is inspected textually
// first, because net/url rejects a bad port before the caller can name it.
func parseQwenBaseURL(raw string) (*url.URL, error) {
	schemeRest := raw
	if i := strings.Index(raw, "://"); i >= 0 {
		schemeRest = raw[i+3:]
	} else {
		// The value is not quoted. It comes from the secret store, and the shape
		// that reaches this branch -- something without "://" -- is exactly the
		// shape an operator pastes when a credential is part of the string.
		return nil, &BaseURLConfigError{
			Reason: ReasonBaseURLNotAbsolute,
			Detail: "the value carries no scheme separator; base_url must be an absolute http(s) URL with a host " +
				"(for example https://api.example.com/v1); the configured value is not quoted here because it may itself carry the credential",
		}
	}
	authority := schemeRest
	if i := strings.IndexAny(authority, "/?#"); i >= 0 {
		authority = authority[:i]
	}
	hostport := authority
	if i := strings.LastIndex(authority, "@"); i >= 0 {
		return nil, &BaseURLConfigError{
			Reason: ReasonBaseURLCredentials,
			Detail: "the authority carries userinfo; provider credentials come from the secret store only",
		}
	}
	if port, present := portOfAuthority(hostport); present {
		if port == "" {
			return nil, &BaseURLConfigError{
				Reason: ReasonBaseURLPortEmpty,
				Detail: "authority " + describeAuthority(hostport) +
					" names a port separator with no port; omit the colon to use the scheme default",
			}
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			// The port text is not quoted either: what looks like a port is a
			// common place for a password to land once a URL was pasted wrong
			// (http://user%40host:PASS@... has no plain "@" for the check above).
			return nil, &BaseURLConfigError{
				Reason: ReasonBaseURLPortInvalid,
				Detail: "authority " + describeAuthority(hostport) +
					" names a port that is not a number in 1..65535",
			}
		}
	}

	u, err := url.Parse(raw)
	if err != nil {
		// net/url's error text repeats the URL, so the wrapped error is not
		// returned: it would put the configured value into the caller's answer and
		// into the terminal cause, which is the leak this branch is here to close.
		return nil, &BaseURLConfigError{
			Reason: ReasonBaseURLUnparseable,
			Detail: "the value cannot be parsed as a URL; the configured value is not quoted here because it may itself carry the credential",
		}
	}
	if u.Host == "" {
		return nil, &BaseURLConfigError{
			Reason: ReasonBaseURLUnparseable,
			Detail: "the value names no host; base_url must be an absolute http(s) URL with a host (for example https://api.example.com/v1)",
		}
	}
	if u.User != nil {
		return nil, &BaseURLConfigError{
			Reason: ReasonBaseURLCredentials,
			Detail: "the authority carries userinfo; provider credentials come from the secret store only",
		}
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, &BaseURLConfigError{
			Reason: ReasonBaseURLQueryOrFragment,
			Detail: "the configured prefix carries a query or fragment that is not sent; drop it and name the prefix alone",
		}
	}
	for _, seg := range strings.Split(u.Path, "/") {
		if seg == "." || seg == ".." {
			return nil, &BaseURLConfigError{
				Reason: ReasonBaseURLPathNotCanonical,
				Detail: "the configured path contains a \".\" or \"..\" segment, which the request path would fold " +
					"into a different prefix than the one allowed; the path is not quoted here because it may itself carry the credential",
			}
		}
	}
	// The comparison is case-folded on purpose. HTTP paths are case-sensitive,
	// which is exactly why a configured /V1/Chat/Completions was accepted while
	// the request it produced (/V1/Chat/Completions/chat/completions) 404s at
	// every provider that serves the lowercase endpoint -- the silent failure this
	// gate exists to remove. A refusal the operator can read is recoverable; a
	// 404 from the upstream is not.
	if strings.HasSuffix(strings.ToLower(strings.TrimRight(u.Path, "/")), completionSuffix) {
		return nil, &BaseURLConfigError{
			Reason: ReasonBaseURLDuplicatedPath,
			Detail: "the configured path already names the chat-completions endpoint; base_url is an API prefix " +
				"(for example /v1), and " + completionSuffix + " is appended to it; the path is not quoted here because it may itself carry the credential",
		}
	}
	return u, nil
}

// portOfAuthority reports the port text of an authority without userinfo, and
// whether an explicit separator was present at all.
func portOfAuthority(authority string) (string, bool) {
	if strings.HasPrefix(authority, "[") {
		end := strings.Index(authority, "]")
		if end < 0 {
			return "", false
		}
		rest := authority[end+1:]
		if strings.HasPrefix(rest, ":") {
			return rest[1:], true
		}
		return "", false
	}
	if i := strings.LastIndex(authority, ":"); i >= 0 && !strings.Contains(authority[i+1:], ":") {
		return authority[i+1:], strings.Count(authority, ":") == 1
	}
	return "", false
}

// describeAuthority names the part of a refused authority that is safe to put in
// a refusal. The base_url arrives from the secret store, so any position inside
// it can be the credential -- userinfo written without an "@", a password in what
// looks like a port, a token in the path. Only a plain host, or host:port, is
// repeated; anything else is described as unquotable instead of being echoed.
func describeAuthority(authority string) string {
	if safe := safeAuthorityForMessage(authority); safe != "" {
		return safe
	}
	return "the configured authority (value not quoted)"
}

// safeAuthorityForMessage returns the authority unchanged when it is a plain
// hostname or IP literal with an optional port, and "" for anything else.
func safeAuthorityForMessage(authority string) string {
	if authority == "" {
		return ""
	}
	host, port := authority, ""
	if h, p, err := net.SplitHostPort(authority); err == nil {
		host, port = h, p
	} else if strings.ContainsAny(authority, ":[]") {
		return ""
	}
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 0 || n > 65535 {
			return ""
		}
	}
	if strings.HasPrefix(authority, "[") {
		if ip := net.ParseIP(host); ip == nil || ip.To16() == nil {
			return ""
		}
		if port == "" {
			return "[" + host + "]"
		}
		return "[" + host + "]:" + port
	}
	if ip := net.ParseIP(host); ip != nil && ip.To4() != nil {
		if port == "" {
			return host
		}
		return host + ":" + port
	}
	if !isPlainHostname(host) {
		return ""
	}
	if port == "" {
		return host
	}
	return host + ":" + port
}

// isPlainHostname accepts the RFC-1123 labels a provider endpoint actually uses.
// Anything else -- an "@", a "%", an underscore, an exclamation mark -- is a
// value this function has no business repeating to a caller.
func isPlainHostname(host string) bool {
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			digit := c >= '0' && c <= '9'
			lower := c >= 'a' && c <= 'z'
			upper := c >= 'A' && c <= 'Z'
			if !digit && !lower && !upper && c != '-' {
				return false
			}
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
	}
	return true
}
