//go:build ahp_exec

package main

import (
	"context"
	"net/http"

	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func (s *server) tryMountModelGateway(ctx context.Context, mux *http.ServeMux, verifier *auth.Verifier, authorize transport.Authorize) error {
	ids := s.identityStore()
	if err := ids.Migrate(ctx); err != nil {
		return err
	}
	bridge, err := execbridge.FromEnv(ctx, s.pool)
	if err != nil {
		return err
	}
	bridge.WithTaskLabels(identityContextLabels{store: ids})
	if err := s.configureModelBudgets(ctx, bridge); err != nil {
		return err
	}
	if err := s.configureModelFiles(ctx, bridge, ids); err != nil {
		return err
	}
	s.registerTenantDeletionOwner("model", bridge)
	s.registerSubjectDeletionOwner("model", bridge)
	if err := s.mountTreeBudget(ctx, mux, verifier, authorize); err != nil {
		return err
	}
	h := bridge.ModelAPI(verifier, authorize)
	for _, op := range []string{"SubmitModel", "StreamModel", "GetModel", "CancelModel", "Reserve", "CopyFile", "GetFile", "DeleteFile"} {
		mux.Handle("/v1/model/"+op, h)
	}
	s.logger.Info("Model Gateway API mounted", "provider_configured", bridge.ProviderConfigured(), "routing", bridge.RoutingMounted())
	return nil
}
