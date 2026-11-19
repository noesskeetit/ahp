package tools

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

// ExchangeRegistration pins both the token endpoint and the destination token.
// It is operator configuration, never caller arguments; it contains no secret.
type ExchangeRegistration struct {
	TokenURL                string           `json:"token_url"`
	Audience                string           `json:"audience"`
	Resource                string           `json:"resource"`
	Scope                   string           `json:"scope"`
	Policy                  netpolicy.Policy `json:"egress"`
	SyntheticLoopbackCAFile string           `json:"synthetic_loopback_ca_file,omitempty"`
}

type AssertionIssuer func(context.Context, contracts.Scope, string, ExchangeRegistration) (string, error)
type ConnectionAuthority func(context.Context, contracts.Scope, string) error

// ExchangeResolver keeps returned credentials only on the synchronous ToolGW
// call stack. It has no token cache, persistence, logs or fallback after denial.
type ExchangeResolver struct {
	registrations map[string]ExchangeRegistration
	adapters      map[string]*OAuthAdapter
	issuer        AssertionIssuer
	authorize     ConnectionAuthority
	fallback      CredentialResolver
}

func NewExchangeResolver(registrations map[string]ExchangeRegistration, issue AssertionIssuer, authorize ConnectionAuthority, fallback CredentialResolver, opts ...netpolicy.Option) (*ExchangeResolver, error) {
	return newExchangeResolver(registrations, issue, authorize, fallback, func(cfg ExchangeRegistration) (*OAuthAdapter, error) {
		if cfg.SyntheticLoopbackCAFile != "" {
			return nil, contracts.NewError(contracts.ErrForbidden, "tools: synthetic loopback requires the explicit synthetic profile")
		}
		return NewOAuthAdapter(ProviderBoundary{TokenURL: cfg.TokenURL, Audience: cfg.Audience, Policy: cfg.Policy}, opts...)
	})
}

func newExchangeResolver(registrations map[string]ExchangeRegistration, issue AssertionIssuer, authorize ConnectionAuthority, fallback CredentialResolver, adapter func(ExchangeRegistration) (*OAuthAdapter, error)) (*ExchangeResolver, error) {
	if len(registrations) == 0 || issue == nil || authorize == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "tools: exchange authority configuration required")
	}
	r := &ExchangeResolver{registrations: map[string]ExchangeRegistration{}, adapters: map[string]*OAuthAdapter{}, issuer: issue, authorize: authorize, fallback: fallback}
	for name, cfg := range registrations {
		u, err := url.Parse(cfg.TokenURL)
		if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || name == "" || cfg.Audience == "" || cfg.Resource == "" || cfg.Scope == "" || len(cfg.Policy.AllowedPathPrefixes) == 0 {
			return nil, contracts.NewError(contracts.ErrInvalid, "tools: incomplete exchange registration")
		}
		a, err := adapter(cfg)
		if err != nil {
			return nil, err
		}
		// A subject assertion must never be replayed, including 307/308 on the
		// same origin. Only the exact operator-registered endpoint receives it.
		a.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		r.registrations[name], r.adapters[name] = cfg, a
	}
	return r, nil
}

func (r *ExchangeResolver) Resolve(ctx context.Context, scope contracts.Scope, connection string) (Credential, error) {
	cfg, ok := r.registrations[connection]
	if !ok {
		if r.fallback != nil {
			return r.fallback.Resolve(ctx, scope, connection)
		}
		return Credential{}, contracts.NewError(contracts.ErrNotFound, "tools: credential connection unavailable")
	}
	if err := r.authorize(ctx, scope, connection); err != nil {
		return Credential{}, err
	}
	assertion, err := r.issuer(ctx, scope, connection, cfg)
	if err != nil {
		return Credential{}, err
	}
	token, err := r.adapters[connection].Exchange(ctx, ExchangeRequest{SubjectToken: assertion, SubjectTokenType: "urn:ietf:params:oauth:token-type:jwt", Audience: cfg.Audience, Scope: cfg.Scope, Resource: cfg.Resource})
	if err != nil {
		return Credential{}, contracts.NewError(contracts.ErrUnavailable, "tools: registered token exchange refused")
	}
	if token.IssuedTokenType != AccessTokenTokenType || !strings.EqualFold(token.TokenType, "Bearer") || token.ExpiresIn <= 0 || token.ExpiresIn > 3600 || token.Scope != cfg.Scope || strings.ContainsAny(token.AccessToken, "\r\n") || len(token.AccessToken) > 16<<10 {
		return Credential{}, contracts.NewError(contracts.ErrForbidden, "tools: invalid exchanged access token")
	}
	if err := r.authorize(ctx, scope, connection); err != nil {
		return Credential{}, err
	}
	return Credential{Bearer: token.AccessToken}, nil
}

// AuthorizedResolver applies resource policy to static and personal connectors
// too. A connector cannot evade IAM just by selecting a different secret source.
type AuthorizedResolver struct {
	Authority ConnectionAuthority
	Next      CredentialResolver
}

func (r AuthorizedResolver) Resolve(ctx context.Context, scope contracts.Scope, connection string) (Credential, error) {
	if r.Authority == nil || r.Next == nil {
		return Credential{}, contracts.NewError(contracts.ErrUnavailable, "tools: connection authority unavailable")
	}
	if err := r.Authority(ctx, scope, connection); err != nil {
		return Credential{}, err
	}
	c, err := r.Next.Resolve(ctx, scope, connection)
	if err != nil {
		return Credential{}, err
	}
	if err := r.Authority(ctx, scope, connection); err != nil {
		return Credential{}, err
	}
	return c, nil
}
