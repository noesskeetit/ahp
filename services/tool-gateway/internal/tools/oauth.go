package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

const (
	// TokenExchangeGrantType is the RFC 8693 grant identifier.
	TokenExchangeGrantType = "urn:ietf:params:oauth:grant-type:token-exchange"
	// AccessTokenTokenType is the RFC 8693 identifier for JWT/access tokens.
	AccessTokenTokenType = "urn:ietf:params:oauth:token-type:access_token"
)

// ProviderBoundary is the explicitly bounded external OAuth provider this
// adapter may talk to. External providers are a hard boundary: the token URL
// must pass netpolicy (public https or an allow-listed private host), and the
// minted audience is pinned unless the caller asks for the boundary default.
type ProviderBoundary struct {
	TokenURL string
	// Audience, when set, is the only audience the adapter will request; a
	// mismatched caller audience is rejected as ErrForbidden.
	Audience string
	Policy   netpolicy.Policy
}

// OAuthAdapter performs RFC 8693 token-exchange against one bounded provider.
type OAuthAdapter struct {
	client   *http.Client
	boundary ProviderBoundary
}

// NewOAuthAdapter builds an adapter whose outbound transport enforces the
// boundary netpolicy (host/method/path allow-lists, no redirect to foreign
// origins, no loopback/link-local/metadata reachability).
func NewOAuthAdapter(boundary ProviderBoundary, opts ...netpolicy.Option) (*OAuthAdapter, error) {
	if boundary.TokenURL == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "tools: oauth token url required")
	}
	u, err := url.Parse(boundary.TokenURL)
	if err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "tools: oauth token url invalid")
	}
	if err := boundary.Policy.ValidateURL(u, http.MethodPost); err != nil {
		return nil, err
	}
	client, err := netpolicy.NewClient(boundary.Policy, opts...)
	if err != nil {
		return nil, err
	}
	return &OAuthAdapter{client: client, boundary: boundary}, nil
}

// ExchangeRequest is an RFC 8693 token-exchange request.
type ExchangeRequest struct {
	SubjectToken     string
	SubjectTokenType string
	Audience         string
	Scope            string
	Resource         string
}

// ExchangeResponse is the parsed token-exchange response.
type ExchangeResponse struct {
	AccessToken     string `json:"access_token"`
	IssuedTokenType string `json:"issued_token_type"`
	TokenType       string `json:"token_type"`
	Scope           string `json:"scope"`
	ExpiresIn       int64  `json:"expires_in"`
}

// Exchange performs the token exchange. It never follows a redirect out of the
// boundary (netpolicy denies cross-origin redirects) and never logs the token.
func (a *OAuthAdapter) Exchange(ctx context.Context, req ExchangeRequest) (ExchangeResponse, error) {
	if req.SubjectToken == "" {
		return ExchangeResponse{}, contracts.NewError(contracts.ErrInvalid, "tools: subject_token required")
	}
	if a.boundary.Audience != "" {
		if req.Audience != "" && req.Audience != a.boundary.Audience {
			return ExchangeResponse{}, contracts.NewError(contracts.ErrForbidden, "tools: oauth audience outside boundary")
		}
		if req.Audience == "" {
			req.Audience = a.boundary.Audience
		}
	}
	subjectType := req.SubjectTokenType
	if subjectType == "" {
		subjectType = AccessTokenTokenType
	}
	form := url.Values{}
	form.Set("grant_type", TokenExchangeGrantType)
	form.Set("subject_token", req.SubjectToken)
	form.Set("subject_token_type", subjectType)
	if req.Audience != "" {
		form.Set("audience", req.Audience)
	}
	if req.Scope != "" {
		form.Set("scope", req.Scope)
	}
	if req.Resource != "" {
		form.Set("resource", req.Resource)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.boundary.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return ExchangeResponse{}, fmt.Errorf("tools: oauth request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	httpReq.Header.Set("Accept", "application/json")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return ExchangeResponse{}, err // netpolicy errors (ErrHostNotAllowed, ErrRedirectDenied, ...) bubble up
	}
	defer resp.Body.Close()

	max := a.boundary.Policy.MaxResponseBytes
	if max <= 0 {
		max = 1 << 20
	}
	body, err := netpolicy.Drain(resp, max)
	if err != nil {
		return ExchangeResponse{}, err
	}
	if resp.StatusCode != http.StatusOK {
		// Do not echo the provider body; it may carry protocol details. Status only.
		return ExchangeResponse{}, contracts.NewError(contracts.ErrForbidden, fmt.Sprintf("tools: oauth provider rejected (status %d)", resp.StatusCode))
	}
	if _, err := contracts.Digest(json.RawMessage(body)); err != nil {
		return ExchangeResponse{}, contracts.NewError(contracts.ErrInvalid, "tools: malformed token exchange response")
	}
	var out ExchangeResponse
	if err := json.Unmarshal(bytes.TrimSpace(body), &out); err != nil {
		return ExchangeResponse{}, contracts.NewError(contracts.ErrInvalid, "tools: oauth provider response malformed")
	}
	if out.AccessToken == "" {
		return ExchangeResponse{}, contracts.NewError(contracts.ErrInvalid, "tools: oauth provider returned no access token")
	}
	return out, nil
}
