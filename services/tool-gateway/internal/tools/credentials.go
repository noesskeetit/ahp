package tools

import (
	"context"
	"net/url"
	"strings"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Credential carries the short-lived secrets used to open one MCP session.
// It must never be journalled or logged; only its presence is observable.
type Credential struct {
	// Bearer is the access token for Authorization: Bearer <Bearer>, if any.
	Bearer string
	// Headers are additional static request headers (e.g. tenant hints).
	Headers map[string]string
}

// CredentialResolver maps a (scope, connection) pair to a Credential. Secrets
// come from the secret store; the tool plane never reads a user's source env.
type CredentialResolver interface {
	Resolve(ctx context.Context, scope contracts.Scope, connection string) (Credential, error)
}

// VaultResolver resolves MCP credentials from the project Vault secret store.
// Each connection name maps to a Vault path whose read data carries a "bearer"
// string and an optional "headers" map.
type VaultResolver struct {
	store *secretstore.Client
	paths map[string]string
}

// NewVaultResolver binds connection names to Vault secret paths. The store may
// be nil, in which case Resolve fails closed with ErrNotFound for any path.
func NewVaultResolver(store *secretstore.Client, paths map[string]string) *VaultResolver {
	cp := make(map[string]string, len(paths))
	for k, v := range paths {
		cp[k] = v
	}
	return &VaultResolver{store: store, paths: cp}
}

// Resolve reads the connection's secret and projects it into a Credential.
// Unknown connections and a missing store fail closed with ErrNotFound.
func (r *VaultResolver) Resolve(ctx context.Context, scope contracts.Scope, connection string) (Credential, error) {
	if r == nil || r.store == nil {
		return Credential{}, contracts.NewError(contracts.ErrNotFound, "tools: no secret store configured for connection")
	}
	path, ok := r.paths[connection]
	if !ok {
		return Credential{}, contracts.NewError(contracts.ErrNotFound, "tools: unknown credential connection")
	}
	if err := scope.Validate(); err != nil {
		return Credential{}, err
	}
	// Both owner dimensions are mandatory. A legacy singleton path must never
	// turn the first connected human's token into a tenant-wide credential.
	if !strings.Contains(path, "{tenant}") || !strings.Contains(path, "{subject}") {
		return Credential{}, contracts.NewError(contracts.ErrForbidden, "tools: credential path must be scoped to tenant and subject")
	}
	for _, part := range []string{scope.Tenant, scope.Subject, scope.Deployment} {
		if part == "." || part == ".." {
			return Credential{}, contracts.NewError(contracts.ErrInvalid, "tools: invalid credential owner")
		}
	}
	path = strings.NewReplacer("{tenant}", url.PathEscape(scope.Tenant), "{subject}", url.PathEscape(scope.Subject), "{deployment}", url.PathEscape(scope.Deployment)).Replace(path)
	data, err := r.store.Read(ctx, path)
	if err != nil {
		return Credential{}, err
	}
	cred := Credential{}
	if b, ok := data["bearer"].(string); ok && b != "" {
		cred.Bearer = b
	}
	if raw, ok := data["headers"].(map[string]any); ok {
		cred.Headers = make(map[string]string, len(raw))
		for k, v := range raw {
			if s, ok := v.(string); ok {
				cred.Headers[k] = s
			}
		}
	}
	return cred, nil
}

// OAuthResolver routes registered personal connections through their durable lifecycle.
// It never falls back to a static credential after an OAuth refusal.
type OAuthResolver struct {
	OAuth    *OAuthManager
	Fallback CredentialResolver
}

func (r OAuthResolver) Resolve(ctx context.Context, scope contracts.Scope, connection string) (Credential, error) {
	if r.OAuth != nil && r.OAuth.has(connection) {
		return r.OAuth.Resolve(ctx, scope, connection)
	}
	if r.Fallback == nil {
		return Credential{}, contracts.NewError(contracts.ErrNotFound, "tools: unknown credential connection")
	}
	return r.Fallback.Resolve(ctx, scope, connection)
}
