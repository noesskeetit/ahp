// Command provider-upstreams answers one question with a machine-checkable
// verdict: how many REAL inference upstreams does this deployment actually have?
//
// Why this exists. Label-aware routing and per-copy quotas are only honest if the
// copies are physically different places. The routing decision was once
// bookkeeping only -- one client, one secret, one socket -- and the operator file
// still declares both modelrun copies behind the SAME secret_ref. Since the
// runtime resolves each copy's endpoint from ITS OWN secret, one secret means one
// destination: the second copy is an accounting distinction, not a second
// provider, and no test may claim otherwise.
//
// What it prints: per declared copy the secret reference, the endpoint HOST it
// resolves to, and a connectivity probe. It never prints a credential, a key, a
// DSN, or a full URL with userinfo -- only the host and a status word.
//
// Verdict and exit code:
//
//	0  at least two copies resolve to two DISTINCT reachable hosts
//	3  distinct hosts declared, but fewer than two are reachable right now
//	4  one physical destination declared (copies share a secret_ref) -- the second
//	   upstream is an external prerequisite, and "provider copies are live" is
//	   false on this deployment
//	5  nothing could be resolved (secret store unreachable or unconfigured)
//
//	provider-upstreams -providers infra/model/providers.json
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/model/routing"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
)

const (
	exitDistinct      = 0
	exitUnreachable   = 3
	exitSingleHop     = 4
	exitUnresolvedAll = 5
)

// envRefPattern mirrors the provider table's rule for a secret_ref that names an
// environment variable instead of a key-value path. Kept in sync with
// internal/execbridge/provider_clients.go: secretRefToKVPath, which is the
// product's own resolution rule.
var envRefPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{2,64}$`)

func main() {
	providers := flag.String("providers", "infra/model/providers.json", "provider table to inspect")
	timeout := flag.Duration("timeout", 5*time.Second, "per-copy network probe budget")
	caFile := flag.String("ca-file", os.Getenv("AHP_PROVIDER_CA_FILE"), "PEM bundle of private CA certificates (optional)")
	flag.Parse()

	roots, err := loadRoots(*caFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "provider-upstreams: %v\n", err)
		os.Exit(exitUnresolvedAll)
	}

	table, err := routing.LoadFile(*providers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "provider-upstreams: cannot read %s: %v\n", *providers, err)
		os.Exit(exitUnresolvedAll)
	}
	if len(table.Providers) == 0 {
		fmt.Println("provider-upstreams: the table declares no provider")
		os.Exit(exitUnresolvedAll)
	}

	// Declared-identity check, done before any network call: copies that share one
	// secret_ref cannot land on different sockets, because the secret carries the
	// endpoint. This is the honest answer available offline.
	byRef := map[string][]string{}
	for _, p := range table.Providers {
		byRef[p.SecretRef] = append(byRef[p.SecretRef], p.Name)
	}
	fmt.Printf("declared copies: %d, distinct secret references: %d\n", len(table.Providers), len(byRef))
	for ref, names := range byRef {
		if len(names) > 1 {
			fmt.Printf("  shared secret_ref %q -> copies %v: ONE physical destination, not %d\n", ref, names, len(names))
		}
	}

	resolver, resolverErr := secretstore.NewFromEnv()
	if resolverErr != nil {
		fmt.Printf("secret store unavailable: %v\n", resolverErr)
	}

	type outcome struct {
		name, host, status string
	}
	var outcomes []outcome
	for _, p := range table.Providers {
		o := outcome{name: p.Name}
		kvPath, err := secretRefToKVPath(p.SecretRef)
		switch {
		case resolverErr != nil:
			o.status = "unresolved(secret store unavailable)"
		case err != nil:
			o.status = "unresolved(" + err.Error() + ")"
		default:
			ctx, cancel := context.WithTimeout(context.Background(), *timeout)
			secret, serr := resolver.ReadQwen(ctx, kvPath)
			cancel()
			if serr != nil {
				o.status = "unresolved(secret unreadable)"
				break
			}
			host, herr := hostOf(secret.BaseURL)
			if herr != nil {
				o.status = "unresolved(endpoint malformed)"
				break
			}
			o.host = host
			o.status = probe(host, *timeout, roots)
		}
		outcomes = append(outcomes, o)
		fmt.Printf("copy=%-22s secret_ref=%-34s host=%-28s probe=%s\n", o.name, p.SecretRef, dash(o.host), o.status)
	}

	reachable := map[string]bool{}
	resolved := map[string]bool{}
	for _, o := range outcomes {
		if o.host != "" {
			resolved[o.host] = true
		}
		if o.host != "" && o.status == "reachable" {
			reachable[o.host] = true
		}
	}
	fmt.Printf("distinct resolved hosts: %d, distinct reachable hosts: %d\n", len(resolved), len(reachable))

	switch {
	case len(reachable) >= 2:
		fmt.Printf("VERDICT: %d real upstreams reachable -- per-copy routing and per-copy quota are physically meaningful\n", len(reachable))
		os.Exit(exitDistinct)
	case len(resolved) >= 2:
		fmt.Println("VERDICT: distinct upstreams are configured, fewer than two are reachable from here (network/prerequisite)")
		os.Exit(exitUnreachable)
	case len(resolved) == 1:
		fmt.Println("VERDICT: ONE real upstream -- the extra copies are accounting distinctions; a second real upstream is an external prerequisite")
		os.Exit(exitSingleHop)
	default:
		fmt.Println("VERDICT: no upstream could be resolved from this environment (prerequisite: reachable secret store)")
		os.Exit(exitUnresolvedAll)
	}
}

func dash(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

func secretRefToKVPath(ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", fmt.Errorf("empty secret_ref")
	}
	if envRefPattern.MatchString(ref) {
		v := strings.TrimSpace(os.Getenv(ref))
		if v == "" {
			return "", fmt.Errorf("environment variable %s named by secret_ref is unset", ref)
		}
		ref = v
	}
	if !strings.Contains(ref, "/data/") {
		ref = "secret/data/" + strings.TrimPrefix(ref, "/")
	}
	return ref, nil
}

// hostOf keeps only the host[:port]: the rest of the URL is not this tool's
// business, and printing less is the point.
func hostOf(base string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(base))
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		return "", fmt.Errorf("no host")
	}
	return u.Host, nil
}

// probe reports reachability in two honest steps: can the port be reached at all,
// and -- if TLS is offered -- is the certificate trusted. An untrusted chain is
// reported separately and never folded into "reachable", because an operator
// pointing a copy at an endpoint with a private CA has to see that distinction;
// so does anyone reading this output as evidence.
//
// No credential and no request body leave this process.
func probe(host string, budget time.Duration, roots *x509.CertPool) string {
	d := net.Dialer{Timeout: budget}
	conn, terr := d.Dial("tcp", host)
	if terr != nil {
		return "unreachable(" + classify(terr) + ")"
	}
	_ = conn.Close()

	hostOnly := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		hostOnly = h
	}
	tlsConn, err := tls.DialWithDialer(&net.Dialer{Timeout: budget}, "tcp", host, &tls.Config{
		ServerName: hostOnly,
		RootCAs:    roots,
	})
	if err == nil {
		_ = tlsConn.Close()
		return "reachable"
	}
	if strings.Contains(err.Error(), "certificate") || strings.Contains(err.Error(), "x509") {
		return "reachable-tls-unverified"
	}
	// Port open, no TLS conversation: a clear-text in-tenant endpoint.
	return "reachable-plaintext"
}

// loadRoots builds the trust pool from an optional PEM bundle. A nil pool means
// the system roots, which is what a public upstream must satisfy.
func loadRoots(path string) (*x509.CertPool, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw) {
		return nil, fmt.Errorf("no certificate found in %s", path)
	}
	return pool, nil
}

// classify reduces a network error to a word, so nothing about the endpoint's
// identity beyond reachability is reported.
func classify(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "no such host"):
		return "dns"
	case strings.Contains(msg, "connection refused"):
		return "refused"
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline"):
		return "timeout"
	default:
		return "tls-or-network"
	}
}
