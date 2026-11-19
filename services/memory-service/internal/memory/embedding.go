package memory

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// embed combines stored record labels with the live task ceiling before calling
// the dependency. An HTTP-backed, identified source always requires a policy.
func (s *Store) embed(ctx context.Context, scope contracts.Scope, text string, record labels.Set) ([]float32, error) {
	set := record
	if hasSecret(set.Taints) {
		return nil, contracts.NewError(contracts.ErrForbidden, "memory: secret data cannot be embedded")
	}
	if s.embeddingSpace != "" && s.embeddingPolicy == nil {
		return nil, contracts.NewError(contracts.ErrForbidden, "memory: embedding policy is required")
	}
	if s.embeddingPolicy != nil {
		if err := s.embeddingPolicy(ctx, scope, set); err != nil {
			return nil, err
		}
	}
	return s.embedder(ctx, text)
}
