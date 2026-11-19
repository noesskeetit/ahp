package main

// The Model Gateway keeps its own accounting (ahp_model.budgets, generations,
// reservations) and the acceptance has to compare one quantity at both ends of a
// delegation, yet the composed front could not answer it: the read model
// (BudgetStore.TreeCostByTask) is in-process only, so a verifier reads Postgres
// by hand and the UI shows tariff text where a measured figure belongs.
//
// This mount follows the accepted W2-A pattern: it exposes the already-accepted
// read method behind the SAME verifier + authorize + mTLS contract the State face
// uses, adds no business branch and no writer, and is OFF by default
// (AHP_ENABLE_MODEL_COST) so every existing role stays byte-identical.

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/model/modelapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// tryMountModelCost migrates nothing new (the Model Gateway schema is the
// accepted one) and mounts the read-only cost face at /v1/model/.
func (s *server) tryMountModelCost(ctx context.Context, mux *http.ServeMux,
	verifier *auth.Verifier, authorize transport.Authorize) error {
	budgets := model.NewBudgetStore(s.pool)
	if err := budgets.Migrate(ctx); err != nil {
		return fmt.Errorf("model budgets migrate: %w", err)
	}
	handler, err := modelapi.NewHTTPHandler(budgets, verifier, authorize)
	if err != nil {
		return fmt.Errorf("model cost handler: %w", err)
	}
	mux.Handle(modelapi.RouteRoot, handler)
	s.logger.Info("model cost read face mounted",
		"prefix", modelapi.RouteRoot, "operations", "TreeCost",
		"capability", modelapi.CapabilityCost, "writes", "none")
	return nil
}
