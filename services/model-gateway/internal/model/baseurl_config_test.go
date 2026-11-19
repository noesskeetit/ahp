package model

// base_url is an API prefix: completionsPath (internal/model/inference.go) appends
// /chat/completions, and AllowedPathPrefixes is derived from the SAME string. A
// malformed prefix therefore cannot be caught by the path gate -- the gate would
// allow exactly whatever the mistake produced (N-3 tautology). The client must
// refuse such configuration before it exists, naming the branch that failed; a
// silent repair would change which endpoint egress actually reaches and would
// defeat an audit of the path allowlist.
//
// The reason strings below are product contract values (stable, scripted on).

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

func buildRefused(t *testing.T, baseURL string) error {
	t.Helper()
	dialed := ""
	c, err := newQwenClient(
		secretstore.QwenSecret{APIKey: "k", BaseURL: baseURL, Model: "m"},
		netpolicy.WithResolver(fixedResolver{m: map[string][]string{"api.example.com": {"93.184.216.34"}}}),
		netpolicy.WithDialer(recordDialer{dialed: &dialed}),
	)
	if err == nil {
		t.Fatalf("base_url %q must be refused, not accepted", baseURL)
	}
	if c != nil {
		t.Fatalf("a refused configuration must not produce a client for %q", baseURL)
	}
	if dialed != "" {
		t.Fatalf("a refused configuration must never dial, dialed %s", dialed)
	}
	return err
}

// A prefix that already ends in the completion suffix yields a doubled path
// (<base>/chat/completions/chat/completions) and a 404 from any provider.
func TestBaseURLDuplicatedCompletionPathIsRefused(t *testing.T) {
	for _, base := range []string{
		"https://api.example.com/v1/chat/completions",
		"https://api.example.com/chat/completions",
	} {
		err := buildRefused(t, base)
		if !strings.Contains(err.Error(), "model_base_url_duplicated_completion_path") {
			t.Fatalf("%q: want the duplicated-path reason, got %v", base, err)
		}
	}
}

// Dot segments let the derived path leave the prefix the operator meant; the
// guard would compute the same folded prefix and allow it.
func TestBaseURLPathOutsideTheCanonicalPrefixIsRefused(t *testing.T) {
	for _, base := range []string{
		"https://api.example.com/v1/chat/completions/../../admin",
		"https://api.example.com/./v1",
		"https://api.example.com/v1/secret/..",
	} {
		err := buildRefused(t, base)
		if !strings.Contains(err.Error(), "model_base_url_path_not_canonical") {
			t.Fatalf("%q: want the non-canonical-path reason, got %v", base, err)
		}
		if strings.Contains(err.Error(), "model_base_url_duplicated_completion_path") {
			t.Fatalf("%q: branches must not share a reason: %v", base, err)
		}
	}
}

// An authority with a colon and no port ("host:") is not the scheme default; it
// is a typo that would otherwise dial an unspecified port.
func TestBaseURLEmptyPortIsRefused(t *testing.T) {
	err := buildRefused(t, "https://api.example.com:/v1")
	if !strings.Contains(err.Error(), "model_base_url_port_empty") {
		t.Fatalf("want the empty-port reason, got %v", err)
	}
}

// A non-numeric or out-of-range port must fail with the port named, not with a
// generic "not a valid URL".
func TestBaseURLInvalidPortIsRefused(t *testing.T) {
	for _, base := range []string{
		"https://api.example.com:abc/v1",
		"https://api.example.com:0/v1",
		"https://api.example.com:99999/v1",
	} {
		err := buildRefused(t, base)
		if !strings.Contains(err.Error(), "model_base_url_port_invalid") {
			t.Fatalf("%q: want the invalid-port reason, got %v", base, err)
		}
	}
}

// Credentials do not belong in base_url: they would be journalled in every
// refusal message. The refusal must not echo the secret either.
func TestBaseURLCredentialsAreRefusedAndNotEchoed(t *testing.T) {
	err := buildRefused(t, "https://user:super-secret-value@api.example.com/v1")
	if !strings.Contains(err.Error(), "model_base_url_carries_credentials") {
		t.Fatalf("want the credentials reason, got %v", err)
	}
	if strings.Contains(err.Error(), "super-secret-value") || strings.Contains(err.Error(), "user:") {
		t.Fatalf("the refusal must not echo credentials: %v", err)
	}
}

// Query and fragment are not carried into the request path, so accepting them
// would be a silent rewrite of the configured destination.
func TestBaseURLQueryOrFragmentIsRefused(t *testing.T) {
	for _, base := range []string{
		"https://api.example.com/v1?api_key=1",
		"https://api.example.com/v1#copy-a",
	} {
		err := buildRefused(t, base)
		if !strings.Contains(err.Error(), "model_base_url_carries_query_or_fragment") {
			t.Fatalf("%q: want the query/fragment reason, got %v", base, err)
		}
	}
}

// The other half of the requirement: refusal, not repair. A legitimate prefix --
// with or without a trailing slash, and a bare host -- must still be served at
// exactly /v1/chat/completions, proving nothing was normalized behind the
// operator's back.
func TestBaseURLLegitimatePrefixIsNotRewritten(t *testing.T) {
	for _, base := range []string{
		"http://172.18.0.6:8000/v1",
		"http://172.18.0.6:8000/v1/",
		"http://172.18.0.6:8000",
	} {
		p := newUnixProvider(t, "sk-w2-do-not-leak")
		secret := secretstore.QwenSecret{APIKey: "sk-w2-do-not-leak", BaseURL: base, Model: "m",
			PrivateHosts: []string{"172.18.0.6:8000"}}
		c, err := newQwenClient(secret, netpolicy.WithDialer(netDialerFunc(p.dialSocket)))
		if err != nil {
			t.Fatalf("%q must be a legitimate prefix: %v", base, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		st, err := c.Stream(ctx, ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}}})
		cancel()
		if err != nil {
			t.Fatalf("%q: %v", base, err)
		}
		st.Close()
		if got, _ := p.path.Load().(string); got != "/v1/chat/completions" {
			t.Fatalf("%q: provider served %q, want /v1/chat/completions (nothing may be rewritten)", base, got)
		}
	}
}
