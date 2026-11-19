package main

import (
	"context"
	"fmt"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/pkg/auth"
)

// Browser identity intentionally has no deployment. The model hop uses the
// run's verified task identity, exactly as artifact and delegation consumers do.
type scopedGatewayModel struct {
	model     core.ModelClient
	authority runScopeAttester
}

func (m scopedGatewayModel) verified(ctx context.Context) (context.Context, error) {
	scope, token, ok := core.RunScopeFromContext(ctx)
	if !ok || token == "" || m.authority.verifier == nil || m.authority.workload == "" {
		return nil, fmt.Errorf("model gateway requires verified run authority")
	}
	claims, err := m.authority.verifier.Verify(token, m.authority.taskAudience, auth.KindTask, m.authority.workload)
	if err != nil {
		return nil, fmt.Errorf("model gateway run credential: %w", err)
	}
	if scope.Tenant != claims.Tenant || scope.Subject != claims.Subject || (scope.Deployment != "" && scope.Deployment != claims.Deployment) {
		return nil, fmt.Errorf("model gateway run scope differs from verified task identity")
	}
	return core.WithRunScope(ctx, claims.Scope(), token), nil
}

func (m scopedGatewayModel) Generate(ctx context.Context, req *core.ModelRequest) (*core.ModelResponse, error) {
	ctx, err := m.verified(ctx)
	if err != nil {
		return nil, err
	}
	return m.model.Generate(ctx, req)
}
func (m scopedGatewayModel) GenerateStream(ctx context.Context, req *core.ModelRequest, emit func(core.ModelDelta) error) (*core.ModelResponse, error) {
	ctx, err := m.verified(ctx)
	if err != nil {
		return nil, err
	}
	if streaming, ok := m.model.(core.StreamingModelClient); ok {
		return streaming.GenerateStream(ctx, req, emit)
	}
	return m.model.Generate(ctx, req)
}
