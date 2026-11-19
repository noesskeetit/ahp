package main

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// A trigger stores the task reference, never a credential. Every fire checks
// the live Identity grant and mints under that exact scoped task.
type identityTriggerAuthority struct {
	store    *identity.Store
	signer   *auth.Signer
	verifier *auth.Verifier
}

func (a identityTriggerAuthority) IssueToken(ctx context.Context, scope contracts.Scope, ref string, ttl time.Duration) (string, time.Time, error) {
	if a.store == nil || a.signer == nil || a.verifier == nil || ref == "" {
		return "", time.Time{}, contracts.NewError(contracts.ErrForbidden, "triggers: live Identity task authority is required")
	}
	if _, err := a.store.Evaluate(ctx, scope, ref, "execution"); err != nil {
		return "", time.Time{}, err
	}
	token, err := a.store.MintTask(ctx, scope, ref, a.signer, "ahp", ttl)
	if err != nil {
		return "", time.Time{}, err
	}
	claims, err := a.verifier.Verify(token, "ahp", auth.KindTask, auth.WorkloadID(scope))
	if err != nil {
		return "", time.Time{}, err
	}
	return token, time.Unix(claims.ExpiresAt, 0).UTC(), nil
}

func (a identityTriggerAuthority) IsTaskActive(ctx context.Context, scope contracts.Scope, task string) (bool, error) {
	if a.store == nil || task == "" {
		return false, contracts.NewError(contracts.ErrForbidden, "triggers: live Identity task authority is required")
	}
	_, err := a.store.Evaluate(ctx, scope, task, "execution")
	if err == nil {
		return true, nil
	}
	if contracts.Code(err) == contracts.ErrNotFound || contracts.Code(err) == contracts.ErrForbidden {
		return false, nil
	}
	return false, err
}

type triggerVaultReader interface {
	Read(context.Context, string) (map[string]any, error)
}

// Paths are operator-owned aliases. Every alias resolves inside the complete
// tenant/deployment/subject scope; a caller can never supply a raw Vault path.
type scopedVaultSecretResolver struct {
	store triggerVaultReader
	paths map[string]string
}

func triggerSecretResolverFromEnv() (*scopedVaultSecretResolver, error) {
	r := &scopedVaultSecretResolver{paths: map[string]string{}}
	raw := strings.TrimSpace(envOr("AHP_TRIGGERS_VAULT_PATHS", ""))
	if raw == "" {
		return r, nil
	} // Schedules need no webhook signing secret.
	if err := json.Unmarshal([]byte(raw), &r.paths); err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "triggers: invalid Vault path configuration")
	}
	for _, path := range r.paths {
		if err := validateTriggerVaultPath(path); err != nil {
			return nil, err
		}
	}
	store, err := secretstore.NewFromEnv()
	if err != nil {
		return nil, err
	}
	r.store = store
	return r, nil
}

func validateTriggerVaultPath(path string) error {
	segments := strings.Split(path, "/")
	for _, field := range []string{"{tenant}", "{deployment}", "{subject}"} {
		found := false
		for _, segment := range segments {
			if segment == field {
				found = true
			}
		}
		if !found {
			return contracts.NewError(contracts.ErrForbidden, "triggers: Vault secret path needs separate tenant, deployment and subject segments")
		}
	}
	if strings.ContainsAny(path, "?#%") || strings.Contains(path, "://") {
		return contracts.NewError(contracts.ErrInvalid, "triggers: invalid Vault secret path")
	}
	for _, part := range strings.Split(path, "/") {
		if part == "." || part == ".." {
			return contracts.NewError(contracts.ErrInvalid, "triggers: invalid Vault secret path")
		}
	}
	return nil
}

func (r *scopedVaultSecretResolver) Resolve(ctx context.Context, scope contracts.Scope, ref string) ([]byte, error) {
	if r == nil || r.store == nil {
		return nil, contracts.NewError(contracts.ErrNotFound, "triggers: no Vault signing secret configured")
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	path, ok := r.paths[ref]
	if !ok {
		return nil, contracts.NewError(contracts.ErrNotFound, "triggers: unknown signing secret reference")
	}
	if err := validateTriggerVaultPath(path); err != nil {
		return nil, err
	}
	for _, part := range []string{scope.Tenant, scope.Deployment, scope.Subject} {
		if part == "." || part == ".." {
			return nil, contracts.NewError(contracts.ErrInvalid, "triggers: invalid signing secret owner")
		}
	}
	path = strings.NewReplacer("{tenant}", url.PathEscape(scope.Tenant), "{deployment}", url.PathEscape(scope.Deployment), "{subject}", url.PathEscape(scope.Subject)).Replace(path)
	secret, err := r.store.Read(ctx, path)
	if err != nil {
		return nil, err
	}
	value, ok := secret["secret"].(string)
	if !ok || value == "" {
		return nil, contracts.NewError(contracts.ErrNotFound, "triggers: Vault signing secret is empty")
	}
	return []byte(value), nil
}
