package gateway

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// These are gateway-authored references, never credentials or caller assertions.
const (
	MetaGrantContext    = "ahp.grant_context"
	MetaGrantTask       = "ahp.grant_task"
	MetaGrantDeployment = "ahp.grant_deployment"
)

func isGrantMarker(k string) bool {
	return k == MetaGrantContext || k == MetaGrantTask || k == MetaGrantDeployment
}

func (s *Service) grantMetadata(ctx context.Context, scope contracts.Scope) (map[string]any, error) {
	if _, ok := transport.FromContext(ctx); !ok && s.cfg.TaskVerifier == nil {
		return nil, nil
	}
	claims, err := s.presentedTaskClaims(ctx)
	if err != nil {
		return nil, err
	}
	if claims.Kind != auth.KindTask {
		return nil, attestationError(ReasonAttestationNotTask, "gateway: grant binding requires a task credential")
	}
	if claims.Tenant != scope.Tenant {
		return nil, attestationError(ReasonAttestationTenantMismatch, "gateway: task credential tenant does not match the accepting scope")
	}
	if claims.Subject != scope.Subject {
		return nil, attestationError(ReasonAttestationSubjectMismatch, "gateway: task credential subject does not match the accepting scope")
	}
	if claims.Context == "" || claims.Task == "" || claims.Scope().Validate() != nil {
		return nil, attestationError(ReasonAttestationUnverifiable, "gateway: verified task binding is incomplete")
	}
	return map[string]any{MetaGrantContext: claims.Context, MetaGrantTask: claims.Task, MetaGrantDeployment: claims.Deployment}, nil
}
