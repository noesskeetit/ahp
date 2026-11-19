package embed

import (
	"context"
	"os"
	"strings"

	"github.com/ahp-platform/ahp/internal/model/routing"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

const EnvPolicyFile = "AHP_MEMORY_EMBEDDING_POLICY_FILE"
const EnvProvider = "AHP_MEMORY_EMBEDDING_PROVIDER"

// PolicyFromEnv binds this one HTTP backend to an explicit label-aware provider
// route. A policy never silently changes the endpoint or embedding model.
func PolicyFromEnv(b *Backend) (func(context.Context, contracts.Scope, labels.Set) error, error) {
	path, provider := strings.TrimSpace(os.Getenv(EnvPolicyFile)), strings.TrimSpace(os.Getenv(EnvProvider))
	if path == "" || provider == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: POLICY_FILE and PROVIDER are required")
	}
	router, err := routing.New(path, nil)
	if err != nil {
		return nil, err
	}
	p, ok := router.Table().ByName()[provider]
	if !ok {
		return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: provider is absent from policy")
	}
	offered := false
	for _, m := range p.Models {
		if m == b.model {
			offered = true
		}
	}
	if !offered {
		return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: model is absent from provider policy")
	}
	return func(_ context.Context, scope contracts.Scope, set labels.Set) error {
		if _, err := router.Reload(); err != nil {
			return err
		}
		route, err := router.Resolve(scope, routing.Request{Labels: set, Model: b.model})
		if err != nil {
			return err
		}
		if route.Provider != provider || route.Model != b.model {
			return contracts.NewError(contracts.ErrForbidden, "memory/embed: route does not match the configured embedding space")
		}
		return nil
	}, nil
}
