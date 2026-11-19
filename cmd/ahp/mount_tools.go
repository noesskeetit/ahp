package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/internal/tools/toolsapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// This file mounts the accepted Tool & Credential Gateway (internal/tools) as a
// product HTTP face at /v1/tools/, exactly like the accepted Registry/Triggers
// faces: behind the SAME authorize + verifier + mTLS contract, gated by
// AHP_ENABLE_TOOLS (default OFF) so the default build and every existing role
// stay byte-identical and the operator's journey is untouched.
//
// The face exposes Call/Get/Fence/Approve/ConnectionState (PDF rev 3.2, с. 44-45
// "MCP-пулы по владельцу полномочий", с. 151). Business logic is the accepted
// tools.Service verbatim; this file only wires it to its collaborators.

// toolsConfig is the operator-supplied JSON that names the MCP connection pools
// and the callable tools. There is no product default: with AHP_ENABLE_TOOLS=1
// but no (or invalid) AHP_TOOLS_CONFIG the mount fails closed and the plane is
// absent, rather than mounting a gateway that calls nothing.
type toolsConfig struct {
	Exchange map[string]tools.ExchangeRegistration `json:"exchange,omitempty"`
	OAuth    map[string]tools.OAuthProvider        `json:"oauth,omitempty"`
	Egress   map[string]netpolicy.Policy           `json:"egress,omitempty"`

	Connections map[string]string         `json:"connections"` // name -> MCP streamable HTTP endpoint
	Tools       map[string]tools.ToolSpec `json:"tools"`       // alias -> static tool spec
}

// humanPermissions is the live human-permission read the approvals plane needs
// (identity.Store.BrowserUserBySubject). Permissions are rows in the durable
// identity schema, not claims: a permission revoked at the operator stops the
// next Approve immediately, exactly as the exchange re-derives capabilities.
type humanPermissions interface {
	BrowserTokenActive(ctx context.Context, jti string) (identity.BrowserSession, error)
	BrowserUserBySubject(ctx context.Context, tenant, subject string) (identity.BrowserUser, error)
}

// toolsHumanAuthorize is the authorize callback for the human-facing legs of
// the plane (Approve, Approvals) under transport.TaskOrUser (DEF-TGW-2,
// director contract 2026-11-02): the browser credential must be a live human
// personally holding the tools:approve sub-capability. A task token is refused
// here unconditionally -- a task can never sign a human approval.
func toolsHumanAuthorize(humans humanPermissions, log *slog.Logger) transport.Authorize {
	// One uniform refusal for every reason (director contract 2026-11-02,
	// foreign-principal leg): the HTTP response must not tell an unpermitted
	// principal WHETHER the thing it probed exists -- not the approval request,
	// not even whether its own subject is a provisioned user of this tenant.
	// The real reason is audited server-side only.
	deny := func(reason string, attrs ...any) error {
		if log != nil {
			log.Warn("tools: approvals plane refused", append([]any{"reason", reason}, attrs...)...)
		}
		return contracts.NewError(contracts.ErrForbidden,
			"tools: this principal may not use the approvals plane")
	}
	return func(ctx context.Context, claims auth.Claims, capability string) error {
		if capability != toolsapi.ApproveCapability && capability != toolsapi.ConnectCapability {
			return deny("unexpected capability", "capability", capability)
		}
		if claims.Kind != auth.KindHuman {
			return deny("approvals plane is human-only", "tenant", claims.Tenant)
		}
		// Signature/expiry are not live session authority. Logout, refresh rotation
		// and subject revocation retire JTI bindings before the JWT expires.
		if humans == nil || claims.JTI == "" {
			return deny("human session binding missing")
		}
		session, err := humans.BrowserTokenActive(ctx, claims.JTI)
		if err != nil || session.Tenant != claims.Tenant || session.Subject != claims.Subject {
			return deny("human session inactive or mismatched")
		}
		user, err := humans.BrowserUserBySubject(ctx, claims.Tenant, claims.Subject)
		if err != nil {
			return deny("approver identity not found", "tenant", claims.Tenant, "subject", claims.Subject)
		}
		for _, p := range user.Permissions {
			if p == capability {
				if authority, ok := humans.(interface {
					RequireHumanContainers(context.Context, auth.Claims, string) error
				}); ok {
					if err := authority.RequireHumanContainers(ctx, claims, capability); err != nil {
						return deny("human resource policy denied")
					}
				}
				return nil
			}
		}
		return deny("approver lacks capability", "tenant", claims.Tenant, "subject", claims.Subject)
	}
}

func (s *server) tryMountTools(ctx context.Context, mux *http.ServeMux,
	verifier *auth.Verifier, authorize transport.Authorize, humans humanPermissions) error {
	if s.iam != nil {
		if _, ok := humans.(interface {
			RequireHumanContainers(context.Context, auth.Claims, string) error
		}); !ok {
			return fmt.Errorf("tools: synthetic profile requires live human resource authority")
		}
	}
	schema := envOr("AHP_TOOLS_SCHEMA", "ahp_tools_all")
	raw := envOr("AHP_TOOLS_CONFIG", "")
	if raw == "" {
		return fmt.Errorf("tools: AHP_ENABLE_TOOLS=1 requires AHP_TOOLS_CONFIG (connection + tool pools)")
	}
	var cfg toolsConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return fmt.Errorf("tools: parse AHP_TOOLS_CONFIG: %w", err)
	}
	if len(cfg.Connections) == 0 || len(cfg.Tools) == 0 {
		return fmt.Errorf("tools: AHP_TOOLS_CONFIG must define at least one connection and one tool")
	}
	caPool, err := toolsTrustPool()
	if err != nil {
		return err
	}
	// Credential resolution is selected explicitly: a production stand resolves
	// per-connection MCP credentials from Vault (the accepted tools.VaultResolver);
	// a static development bearer is used ONLY when the operator asks for it by
	// name, and the mount then logs credentials=dev-static. With neither
	// configured the mount fails closed instead of calling tool servers unauthenticated.
	needsStatic := false
	for name := range cfg.Connections {
		_, exchange := cfg.Exchange[name]
		if _, ok := cfg.OAuth[name]; !ok && !exchange {
			needsStatic = true
		}
	}
	for name := range cfg.OAuth {
		if _, ok := cfg.Connections[name]; !ok {
			return fmt.Errorf("tools: OAuth registration references an unknown connection")
		}
	}
	for name := range cfg.Exchange {
		if _, ok := cfg.Connections[name]; !ok {
			return fmt.Errorf("tools: exchange registration references an unknown connection")
		}
		if _, ok := cfg.OAuth[name]; ok {
			return fmt.Errorf("tools: conflicting connector credential modes")
		}
	}
	creds, credSource, err := toolsCredentialResolver()
	if err != nil && (needsStatic || len(cfg.OAuth)+len(cfg.Exchange) == 0 || envOr("AHP_TOOLS_VAULT_PATHS", "") != "" || envOr("AHP_TOOLS_DEV_BEARER", "") != "") {
		return fmt.Errorf("tools: %w", err)
	}
	var personal *tools.OAuthManager
	if len(cfg.OAuth) > 0 {
		sink, ok := humans.(interface {
			AddLabels(context.Context, contracts.Scope, string, labels.Set) (identity.Grant, error)
		})
		if !ok {
			return fmt.Errorf("tools: personal OAuth requires live Identity private-label authority")
		}
		vault, err := secretstore.NewFromEnv()
		if err != nil {
			return err
		}
		check := func(ctx context.Context, scope contracts.Scope) error {
			p, ok := transport.FromContext(ctx)
			if !ok || p.Claims.Kind != auth.KindTask || p.Scope != scope {
				return contracts.NewError(contracts.ErrForbidden, "oauth: verified task required")
			}
			return authorize(ctx, p.Claims, "tools")
		}
		personal, err = tools.NewOAuthManager(ctx, s.pool, schema, tools.OAuthConfig{Providers: cfg.OAuth, Secrets: vault, SecretPrefix: envOr("AHP_TOOLS_OAUTH_VAULT_PREFIX", "secret/data/ahp/tools-oauth"), Authorize: check, LabelPrivate: func(ctx context.Context, scope contracts.Scope) error {
			p, ok := transport.FromContext(ctx)
			if !ok || p.Claims.Kind != auth.KindTask || p.Scope != scope || p.Claims.Task == "" {
				return contracts.NewError(contracts.ErrForbidden, "oauth: task identity required for labels")
			}
			_, err := sink.AddLabels(ctx, scope, p.Claims.Task, labels.Set{Subjects: []string{scope.Subject}})
			return err
		}})
		if err != nil {
			return err
		}
		creds = tools.OAuthResolver{OAuth: personal, Fallback: creds}
		credSource = "vault-personal-oauth"
	}
	connectionAuthority := s.toolsConnectionAuthority()
	if len(cfg.Exchange) > 0 {
		if s.iam == nil || s.exchangeSigner == nil {
			return fmt.Errorf("tools: token exchange requires live resource IAM and role=all Identity owner")
		}
		if envOr("AHP_IAM_PROFILE", "local") == "synthetic" {
			creds, err = tools.NewSyntheticExchangeResolver("synthetic", cfg.Exchange, s.issueToolAssertion, connectionAuthority, creds)
		} else {
			creds, err = tools.NewExchangeResolver(cfg.Exchange, s.issueToolAssertion, connectionAuthority, creds)
		}
		if err != nil {
			return err
		}
		credSource = "bounded-token-exchange"
	}
	if s.iam != nil {
		creds = tools.AuthorizedResolver{Authority: connectionAuthority, Next: creds}
	}
	if credSource == "dev-static" {
		s.logger.Warn("tools credentials=dev-static (one bearer for every connection); set AHP_TOOLS_VAULT_PATHS for per-connection Vault credentials")
	}

	conns := make(map[string]tools.Connector, len(cfg.Connections))
	for name, endpoint := range cfg.Connections {
		endpoint := endpoint
		connectionName := name
		var egressClient *http.Client
		_, exchangedConnection := cfg.Exchange[name]
		if _, personalConnection := cfg.OAuth[name]; personalConnection || exchangedConnection {
			policy, ok := cfg.Egress[name]
			if !ok || len(policy.AllowedPathPrefixes) == 0 {
				return fmt.Errorf("tools: OAuth MCP connection requires registered domain/method/path egress")
			}
			egressClient, err = netpolicy.NewClient(policy)
			if err != nil {
				return err
			}
			egressClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		}
		conns[name] = func(ctx context.Context, cred tools.Credential) (tools.MCPClient, error) {
			if egressClient != nil {
				p, ok := transport.FromContext(ctx)
				if !ok || p.Claims.Kind != auth.KindTask {
					return nil, contracts.NewError(contracts.ErrForbidden, "tools: verified task required")
				}
				if err := authorize(ctx, p.Claims, "tools"); err != nil {
					return nil, err
				}
				if s.iam != nil {
					if err := connectionAuthority(ctx, p.Scope, connectionName); err != nil {
						return nil, err
					}
				}
				client := *egressClient
				client.Transport = bearerInjector{base: client.Transport, bearer: cred.Bearer}
				return connectMCPWithClient(ctx, endpoint, &client)
			}
			return connectMCP(ctx, endpoint, cred, caPool)
		}
	}
	// Read-time revalidation of personal grants for GetResult. Guarded
	// explicitly: a typed-nil *OAuthManager in an interface field would not
	// equal nil and would turn "no personal connectors configured" into a panic.
	var grants tools.GrantChecker
	if personal != nil {
		grants = personal
	}
	svc, err := tools.New(ctx, s.pool, schema, tools.Config{
		Guardian:     tools.NewPolicyGuardian("front"),
		OAuth:        personal,
		Grants:       grants,
		Credentials:  creds,
		Connections:  conns,
		Tools:        cfg.Tools,
		Verifier:     verifier,
		Audience:     "ahp",
		UserAudience: toolsUserAudience(humans != nil),
	})
	if err != nil {
		return fmt.Errorf("tools service: %w", err)
	}
	if owner, ok := any(svc).(TenantDeletionOwner); ok {
		s.registerTenantDeletionOwner("tools", owner)
	}
	s.registerSubjectDeletionOwner("tools", svc)
	var handler http.Handler
	if humans != nil {
		handler, err = toolsapi.NewHandlerWithUsers(svc, verifier, authorize,
			toolsUserAudience(true), toolsHumanAuthorize(humans, s.logger))
	} else {
		// No browser identity on this front (dev-authorize stands): the human
		// legs keep the accepted single-audience shape, verified against the
		// plane audience, and no durable permission store is consulted.
		handler, err = toolsapi.NewHandler(svc, verifier, authorize)
	}
	if err != nil {
		return fmt.Errorf("tools handler: %w", err)
	}
	mux.Handle("/v1/tools/", handler)
	s.logger.Info("tools plane mounted prefix=/v1/tools/", "schema", schema, "tools", len(cfg.Tools), "connections", len(cfg.Connections), "credentials", credSource)
	return nil
}

// toolsUserAudience names the human browser audience for the approvals legs
// (same env convention as the workspace user face). Only an Identity-composed
// front (browser composition ON, human permission store present) uses it.
func toolsUserAudience(browserMode bool) string {
	if a := strings.TrimSpace(envOr("AHP_TOOLS_USER_AUDIENCE", "")); a != "" {
		return a
	}
	if browserMode {
		return "ahp-browser"
	}
	return "ahp"
}

// toolsCredentialResolver picks the credential source for MCP connections.
// AHP_TOOLS_VAULT_PATHS is a JSON map of connection name -> Vault secret path
// (that secret holds {"bearer": "...", "headers": {...}}), consumed by
// tools.VaultResolver. AHP_TOOLS_DEV_BEARER is the explicit development fallback.
// Neither set is an error: the gateway must not silently call tool servers with
// no credential, and it must not borrow one credential for every connection.
func toolsCredentialResolver() (tools.CredentialResolver, string, error) {
	if raw := envOr("AHP_TOOLS_VAULT_PATHS", ""); raw != "" {
		var paths map[string]string
		if err := json.Unmarshal([]byte(raw), &paths); err != nil {
			return nil, "", fmt.Errorf("parse AHP_TOOLS_VAULT_PATHS: %w", err)
		}
		if len(paths) == 0 {
			return nil, "", fmt.Errorf("AHP_TOOLS_VAULT_PATHS must name at least one connection")
		}
		store, err := secretstore.NewFromEnv()
		if err != nil {
			return nil, "", fmt.Errorf("secret store for AHP_TOOLS_VAULT_PATHS: %w", err)
		}
		return tools.NewVaultResolver(store, paths), "vault", nil
	}
	if bearer := envOr("AHP_TOOLS_DEV_BEARER", ""); bearer != "" {
		return devToolsResolver{bearer: bearer}, "dev-static", nil
	}
	return nil, "", fmt.Errorf("no credential source: set AHP_TOOLS_VAULT_PATHS (production) or AHP_TOOLS_DEV_BEARER (development)")
}

// devToolsResolver supplies a single development bearer to every MCP connection.
// The raw credential is never journalled by the tool plane (only its presence is
// observable). A production deployment replaces this with the Vault-backed
// resolver the tools package already provides.
type devToolsResolver struct{ bearer string }

func (r devToolsResolver) Resolve(context.Context, contracts.Scope, string) (tools.Credential, error) {
	return tools.Credential{Bearer: r.bearer}, nil
}

// connectMCP opens a real MCP session to a streamable-HTTP endpoint. When a CA
// pool is configured (mTLS to the tool server) the client presents the front's
// own certificate and pins that CA; otherwise it trusts the system roots. Only
// the resolved bearer (plus any mTLS material) reaches the tool server.
func connectMCP(ctx context.Context, endpoint string, cred tools.Credential, caPool *x509.CertPool) (tools.MCPClient, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caPool != nil {
		tlsCfg.RootCAs = caPool
	}
	base := &http.Transport{TLSClientConfig: tlsCfg}
	httpClient := &http.Client{Transport: bearerInjector{base: base, bearer: cred.Bearer}}
	return connectMCPWithClient(ctx, endpoint, httpClient)
}

func connectMCPWithClient(ctx context.Context, endpoint string, httpClient *http.Client) (tools.MCPClient, error) {
	// Ingress bound on the provider's answer. This is the single place an MCP
	// session is opened on this front, so every connector -- static, personal
	// and exchanged -- inherits the same finite ceiling instead of trusting the
	// upstream to be small.
	httpClient = tools.LimitUpstream(httpClient, tools.DefaultMaxUpstreamBytes)
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "ahp-tools-gateway", Version: "1"}, nil).Connect(ctx,
		&mcp.StreamableClientTransport{Endpoint: endpoint, HTTPClient: httpClient}, nil)
	if err != nil {
		return nil, err
	}
	return mcpClient{cs}, nil
}

// mcpClient adapts a streamable client session to tools.MCPClient.
type mcpClient struct{ cs *mcp.ClientSession }

func (m mcpClient) CallTool(ctx context.Context, p *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	return m.cs.CallTool(ctx, p)
}
func (m mcpClient) Close() error { return m.cs.Close() }

// bearerInjector attaches the resolved credential as Authorization: Bearer.
type bearerInjector struct {
	base   http.RoundTripper
	bearer string
}

func (b bearerInjector) RoundTrip(r *http.Request) (*http.Response, error) {
	if b.bearer != "" {
		r2 := r.Clone(r.Context())
		r2.Header.Set("Authorization", "Bearer "+b.bearer)
		return b.base.RoundTrip(r2)
	}
	return b.base.RoundTrip(r)
}

// toolsTrustPool builds the CA pool for mTLS to tool servers, or nil when the
// deployment does not pin a tool CA (system roots are used then).
func toolsTrustPool() (*x509.CertPool, error) {
	caFile := envOr("AHP_TOOLS_CLIENT_CA", "")
	if caFile == "" {
		return nil, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tools: read client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tools: client CA did not parse")
	}
	return pool, nil
}
