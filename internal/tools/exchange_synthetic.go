package tools

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"
)

// NewSyntheticExchangeResolver adds only the explicitly approved local fixture
// route. Normal connectors still use netpolicy unchanged. This is not evidence
// of production egress: the operator pins one HTTPS literal-loopback URL and CA.
func NewSyntheticExchangeResolver(profile string, registrations map[string]ExchangeRegistration, issue AssertionIssuer, authorize ConnectionAuthority, fallback CredentialResolver) (*ExchangeResolver, error) {
	if profile != "synthetic" {
		return nil, contracts.NewError(contracts.ErrForbidden, "tools: explicit synthetic profile required")
	}
	return newExchangeResolver(registrations, issue, authorize, fallback, func(cfg ExchangeRegistration) (*OAuthAdapter, error) {
		if cfg.SyntheticLoopbackCAFile == "" {
			return NewOAuthAdapter(ProviderBoundary{TokenURL: cfg.TokenURL, Audience: cfg.Audience, Policy: cfg.Policy})
		}
		u, e := url.Parse(cfg.TokenURL)
		if e != nil || u.Scheme != "https" || u.Port() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, contracts.NewError(contracts.ErrInvalid, "tools: invalid synthetic token URL")
		}
		ip := net.ParseIP(u.Hostname())
		if ip == nil || !ip.IsLoopback() {
			return nil, contracts.NewError(contracts.ErrForbidden, "tools: synthetic token endpoint must be literal loopback")
		}
		pem, e := os.ReadFile(cfg.SyntheticLoopbackCAFile)
		if e != nil {
			return nil, contracts.NewError(contracts.ErrInvalid, "tools: synthetic authority CA unavailable")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, contracts.NewError(contracts.ErrInvalid, "tools: synthetic authority CA invalid")
		}
		pinned := net.JoinHostPort(u.Hostname(), u.Port())
		transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: 3 * time.Second, DisableKeepAlives: true, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if address != pinned {
				return nil, contracts.NewError(contracts.ErrForbidden, "tools: synthetic dial target changed")
			}
			return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, network, address)
		}}
		client := &http.Client{Transport: transport, Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		policy := cfg.Policy
		policy.MaxResponseBytes = 64 << 10
		return &OAuthAdapter{client: client, boundary: ProviderBoundary{TokenURL: cfg.TokenURL, Audience: cfg.Audience, Policy: policy}}, nil
	})
}
