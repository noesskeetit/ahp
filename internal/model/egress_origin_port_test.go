package model

import (
	"context"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

// F-2: the egress registration must describe the ORIGIN the client is
// configured to call -- host and port together -- and when it cannot reach that
// origin the failure must name the real fact (a port), never a scheme.

// A base_url on an explicit, non-default port is the operator's configured
// destination: registering only the hostname made the client deny its own
// base_url with "host is not registered".
func TestF2BaseURLWithExplicitPortReachesItsOwnOrigin(t *testing.T) {
	res := fixedResolver{m: map[string][]string{"api.example.com": {"93.184.216.34"}}}
	sec := secretstore.QwenSecret{APIKey: "k", BaseURL: "https://api.example.com:8443/v1", Model: "m"}
	e, dialed, build := runEgress(t, sec, res)
	if build != nil {
		t.Fatalf("custom-port public origin must build: %v", build)
	}
	if !dialed {
		t.Fatalf("custom-port public origin must be reachable, denied=%v err=%v", guardDenied(e), e)
	}
	if guardDenied(e) {
		t.Fatalf("the configured base_url must not be denied as unregistered: %v", e)
	}
}

// Stage 2 runs the provider inside the guest network, so the base_url itself is
// an RFC1918 literal. Registering it must actually make it callable.
func TestF2RegisteredPrivateIPLiteralBaseURLIsCallable(t *testing.T) {
	sec := secretstore.QwenSecret{
		APIKey:       "k",
		BaseURL:      "http://172.18.0.6:8000/v1",
		Model:        "m",
		PrivateHosts: []string{"172.18.0.6:8000"},
	}
	e, dialed, build := runEgress(t, sec, fixedResolver{})
	if build != nil {
		t.Fatalf("a registered private base_url must build: %v", build)
	}
	if !dialed {
		t.Fatalf("a registered private base_url must reach the dial path, denied=%v err=%v", guardDenied(e), e)
	}
	if guardDenied(e) {
		t.Fatalf("registered private origin must not be guard-denied: %v", e)
	}
}

// The same origin WITHOUT registration keeps today's refusal: no silent widening.
func TestF2UnregisteredPrivateIPLiteralBaseURLStaysRefused(t *testing.T) {
	sec := secretstore.QwenSecret{APIKey: "k", BaseURL: "http://172.18.0.6:8000/v1", Model: "m"}
	e, dialed, build := runEgress(t, sec, fixedResolver{})
	if dialed {
		t.Fatal("an unregistered private origin must never reach the dial path")
	}
	if build == nil && !guardDenied(e) {
		t.Fatalf("unregistered private origin must be refused by policy compile or guard, err=%v", e)
	}
}

// When the operator registered the internal host on another port, the refusal
// must state the port fact. The scheme text "only reachable over https" is a
// lie about an internal endpoint: nothing about this failure is a scheme issue.
func TestF2PrivateEntryPortMismatchNamesThePortNotTheScheme(t *testing.T) {
	for _, tc := range []struct{ name, baseURL, registered string }{
		{"explicit-port-vs-explicit-port", "http://provider.internal:9000/v1", "provider.internal:8000"},
		{"explicit-port-vs-default-port", "http://provider.internal:9000/v1", "provider.internal"},
		{"ip-literal-port-mismatch", "http://172.18.0.6:9000/v1", "172.18.0.6:8000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sec := secretstore.QwenSecret{APIKey: "k", BaseURL: tc.baseURL, Model: "m", PrivateHosts: []string{tc.registered}}
			res := fixedResolver{m: map[string][]string{"provider.internal": {"172.18.0.6"}}}
			e, dialed, build := runEgress(t, sec, res)
			if dialed {
				t.Fatal("a port that was never registered must not be dialed")
			}
			cause := ""
			if build != nil {
				cause = build.Error()
			} else if e != nil {
				cause = e.Error()
			} else {
				t.Fatal("expected a refusal for a mismatched port")
			}
			if strings.Contains(cause, "https") {
				t.Fatalf("a port mismatch must not be reported as a scheme problem: %q", cause)
			}
			if !strings.Contains(cause, "9000") || !strings.Contains(cause, "8000") && !strings.Contains(cause, "default") {
				t.Fatalf("the refusal must name both the requested and the registered port, got %q", cause)
			}
			if build == nil {
				t.Fatalf("the mismatch must be refused at construction, not per request: %v", e)
			}
		})
	}
}

// A registered internal host answering the base_url port must not be
// double-registered as a public origin: the private branch is what admits it.
func TestF2PrivateOriginIsNotRegisteredAsPublic(t *testing.T) {
	sec := secretstore.QwenSecret{
		APIKey:       "k",
		BaseURL:      "http://172.18.0.6:8000/v1",
		Model:        "m",
		PrivateHosts: []string{"172.18.0.6:8000"},
	}
	c, err := newQwenClient(sec, netpolicy.WithResolver(fixedResolver{}), netpolicy.WithDialer(recordDialer{dialed: new(string)}))
	if err != nil {
		t.Fatalf("must build: %v", err)
	}
	if c == nil {
		t.Fatal("nil client")
	}
}

// Root's condition on F-2: an https configuration must survive byte-for-byte.
// Every configuration that works today keeps working: a private host registered
// at the port the base_url names, a private host whose bare entry answers the
// scheme default, a public host on the default port, and a public host on the
// port the base_url names (the latter is the F-2 fix itself, the destination is
// the operator's own configured origin over https).
func TestF2HTTPSConfigurationsKeepTheirOutcome(t *testing.T) {
	for _, tc := range []struct{ name, baseURL, registered string }{
		{"public-default-port", "https://api.example.com/v1", ""},
		{"public-explicit-port", "https://api.example.com:8443/v1", ""},
		{"private-registered-same-port", "https://provider.internal:8443/v1", "provider.internal:8443"},
		{"private-bare-entry-default-port", "https://provider.internal/v1", "provider.internal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sec := secretstore.QwenSecret{APIKey: "k", BaseURL: tc.baseURL, Model: "m"}
			if tc.registered != "" {
				sec.PrivateHosts = []string{tc.registered}
			}
			res := fixedResolver{m: map[string][]string{
				"api.example.com":   {"93.184.216.34"},
				"provider.internal": {"172.18.0.6"},
			}}
			e, dialed, build := runEgress(t, sec, res)
			if build != nil {
				t.Fatalf("policy must build: %v", build)
			}
			if !dialed || guardDenied(e) {
				t.Fatalf("this https origin must reach the dial path, dialed=%v denied=%v err=%v", dialed, guardDenied(e), e)
			}
		})
	}
}

// The other half of the condition: a port the operator never registered is
// refused for https too -- with the origin and both ports named, never a scheme
// claim. F-2 must not turn its own port registration into a key that opens a
// port PrivateHosts pointed somewhere else.
func TestF2HTTPSPortMismatchIsRefusedWithTheOriginAsTheCause(t *testing.T) {
	sec := secretstore.QwenSecret{
		APIKey:       "k",
		BaseURL:      "https://provider.internal:9443/v1",
		Model:        "m",
		PrivateHosts: []string{"provider.internal:8443"},
	}
	err := func() error {
		dialed := ""
		c, buildErr := newQwenClient(sec, netpolicy.WithResolver(fixedResolver{m: map[string][]string{"provider.internal": {"172.18.0.6"}}}),
			netpolicy.WithDialer(recordDialer{dialed: &dialed}))
		if buildErr != nil {
			return buildErr
		}
		if _, streamErr := c.Stream(context.Background(), ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}}}); streamErr != nil {
			if dialed != "" {
				t.Fatalf("a port that was never registered must not be dialed, dialed %s", dialed)
			}
			return streamErr
		}
		t.Fatal("https on an unregistered port must not be reachable")
		return nil
	}()
	if err == nil {
		t.Fatal("expected a refusal")
	}
	cause := err.Error()
	if !strings.Contains(cause, "9443") || !strings.Contains(cause, "8443") {
		t.Fatalf("the refusal must name the requested and the registered port, got %q", cause)
	}
	if strings.Contains(cause, "only reachable over https") {
		t.Fatalf("the cause must not claim a scheme problem: %q", cause)
	}
	if !strings.Contains(cause, "provider.internal:9443") {
		t.Fatalf("the refusal must name the origin the client is configured for, got %q", cause)
	}
}

// N-1: the refusal must describe the registration the operator actually made.
// Telling someone who registered provider.internal:8000 that "an entry without a
// port answers only the scheme default" sends them to the wrong sentence.
func TestF2PortMismatchWordingFollowsTheRegisteredForm(t *testing.T) {
	cases := []struct {
		name       string
		baseURL    string
		registered string
		wantBare   bool
	}{
		{"registered-with-port", "http://provider.internal:9000/v1", "provider.internal:8000", false},
		{"registered-bare", "http://provider.internal:9000/v1", "provider.internal", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sec := secretstore.QwenSecret{APIKey: "k", BaseURL: tc.baseURL, Model: "m", PrivateHosts: []string{tc.registered}}
			_, _, build := runEgress(t, sec, fixedResolver{m: map[string][]string{"provider.internal": {"172.18.0.6"}}})
			if build == nil {
				t.Fatal("expected a construction refusal")
			}
			msg := build.Error()
			hasBareSentence := strings.Contains(msg, "an entry without a port")
			if hasBareSentence != tc.wantBare {
				t.Fatalf("registered %q: bare-entry sentence present=%v want=%v, message=%q", tc.registered, hasBareSentence, tc.wantBare, msg)
			}
			if tc.wantBare && !strings.Contains(msg, "80") {
				t.Fatalf("a bare registration must be explained with the scheme default port, got %q", msg)
			}
			if !tc.wantBare && !strings.Contains(msg, "provider.internal:8000") {
				t.Fatalf("the registered origin must be quoted as written, got %q", msg)
			}
		})
	}
}

// C1 (review 7c5a8758): one quoting rule per file.
//
// The base_url refusals learned to describe an authority instead of printing it
// (describeAuthority: a plain host[:port] passes through, anything else becomes
// "the configured authority (value not quoted)"). privatePortMismatch was left
// outside that rule and joined PrivateHosts entries verbatim -- and those entries
// are operator configuration that legitimately arrives from the route's secret
// record, which is the same class of value whose echo in base_url was just
// classified as a credential leak. A refusal may name what it refused only where
// the shape is provably an authority.
//
// The leg also pins the opposite direction: a plain host:port registration stays
// quoted exactly as written, because the operator's ability to read their own
// entry is the whole point of the message.
func TestPortMismatchRefusalQuotesEntriesOnlyThroughTheOneRule(t *testing.T) {
	const marker = "MARKER-NEVER-QUOTE-4c7d"
	t.Run("non-authority entry is not quoted verbatim", func(t *testing.T) {
		sec := secretstore.QwenSecret{
			APIKey: "k", BaseURL: "http://provider.internal:9000/v1", Model: "m",
			PrivateHosts: []string{"provider.internal:" + marker + "-8000"},
		}
		_, _, build := runEgress(t, sec, fixedResolver{m: map[string][]string{"provider.internal": {"172.18.0.6"}}})
		if build == nil {
			t.Fatal("expected a construction refusal for the port mismatch")
		}
		msg := build.Error()
		if strings.Contains(msg, marker) {
			t.Fatalf("the refusal quotes registration text it cannot prove is an authority: %q", msg)
		}
		// Refusing to quote must not mean refusing to say anything: the caller still
		// has to learn which port was dialed and that a registration exists.
		if !strings.Contains(msg, "9000") {
			t.Fatalf("the refusal must still name the dialed port, got %q", msg)
		}
		if !strings.Contains(msg, "value not quoted") {
			t.Fatalf("an unquotable entry must be described as such, got %q", msg)
		}
	})

	t.Run("plain registration stays quoted as written", func(t *testing.T) {
		sec := secretstore.QwenSecret{
			APIKey: "k", BaseURL: "http://provider.internal:9000/v1", Model: "m",
			PrivateHosts: []string{"provider.internal:8000"},
		}
		_, _, build := runEgress(t, sec, fixedResolver{m: map[string][]string{"provider.internal": {"172.18.0.6"}}})
		if build == nil {
			t.Fatal("expected a construction refusal for the port mismatch")
		}
		if msg := build.Error(); !strings.Contains(msg, "provider.internal:8000") {
			t.Fatalf("a plain host:port registration must be quoted as written, got %q", msg)
		}
	})
}
