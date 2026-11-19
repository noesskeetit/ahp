package tools

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

// OAuthProvider is an operator-registered connector, never browser input.
// ClientSecretPath names Vault data containing client_secret; empty means public PKCE client.
type OAuthProvider struct {
	AuthorizeURL     string           `json:"authorize_url"`
	TokenURL         string           `json:"token_url"`
	RevokeURL        string           `json:"revoke_url"`
	ClientID         string           `json:"client_id"`
	ClientSecretPath string           `json:"client_secret_path,omitempty"`
	RedirectURI      string           `json:"redirect_uri"`
	Scopes           []string         `json:"scopes"`
	Policy           netpolicy.Policy `json:"egress"`
}

type oauthProvider struct {
	config OAuthProvider
	client *http.Client
}

func newOAuthProvider(p OAuthProvider, opts ...netpolicy.Option) (*oauthProvider, error) {
	if p.ClientID == "" || p.AuthorizeURL == "" || p.TokenURL == "" || len(p.Policy.AllowedPathPrefixes) == 0 {
		return nil, oauthInvalid("provider registration incomplete")
	}
	for _, endpoint := range []struct{ raw, method string }{{p.AuthorizeURL, "GET"}, {p.TokenURL, "POST"}, {p.RevokeURL, "POST"}} {
		if endpoint.raw == "" {
			continue
		}
		u, e := url.Parse(endpoint.raw)
		if e != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, oauthInvalid("invalid provider endpoint")
		}
		if e = p.Policy.ValidateURL(u, endpoint.method); e != nil {
			return nil, e
		}
	}
	u, e := url.Parse(p.RedirectURI)
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1"))) {
		return nil, oauthInvalid("invalid registered redirect URI")
	}
	client, e := netpolicy.NewClient(p.Policy, opts...)
	if e != nil {
		return nil, e
	}
	// OAuth credentials must never be replayed to redirects, even same-origin ones.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &oauthProvider{config: p, client: client}, nil
}

type oauthTokens struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	Scope        string `json:"scope"`
	ExpiresIn    int64  `json:"expires_in"`
}

func (p *oauthProvider) request(ctx context.Context, endpoint string, form url.Values, clientSecret string, token bool) (oauthTokens, error) {
	form.Set("client_id", p.config.ClientID)
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if e != nil {
		return oauthTokens{}, oauthInvalid("invalid provider request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	if clientSecret != "" {
		req.SetBasicAuth(url.QueryEscape(p.config.ClientID), url.QueryEscape(clientSecret))
	}
	resp, e := p.client.Do(req)
	if e != nil {
		return oauthTokens{}, contracts.NewError(contracts.ErrUnavailable, "oauth: provider unavailable")
	}
	defer resp.Body.Close()
	body, e := netpolicy.Drain(resp, 1<<20)
	if e != nil {
		return oauthTokens{}, oauthInvalid("invalid provider response")
	}
	if resp.StatusCode != http.StatusOK {
		return oauthTokens{}, oauthDenied("provider rejected request")
	}
	if !token {
		return oauthTokens{}, nil
	}
	var t oauthTokens
	if json.Unmarshal(body, &t) != nil || t.AccessToken == "" || !strings.EqualFold(t.TokenType, "Bearer") || t.ExpiresIn <= 0 || t.ExpiresIn > int64((365*24*time.Hour)/time.Second) {
		return oauthTokens{}, oauthInvalid("invalid token response")
	}
	allowed := map[string]bool{}
	for _, s := range p.config.Scopes {
		allowed[s] = true
	}
	for _, s := range strings.Fields(t.Scope) {
		if !allowed[s] {
			return oauthTokens{}, oauthDenied("provider expanded requested scope")
		}
	}
	return t, nil
}

func oauthInvalid(msg string) error { return contracts.NewError(contracts.ErrInvalid, "oauth: "+msg) }
func oauthDenied(msg string) error  { return contracts.NewError(contracts.ErrForbidden, "oauth: "+msg) }
