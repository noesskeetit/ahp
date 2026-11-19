//go:build ahp_exec

package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/modelapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func (s *server) mountTreeBudget(ctx context.Context, mux *http.ServeMux, v *auth.Verifier, authorize transport.Authorize) error {
	workloads := envListOr("AHP_MODEL_TREE_REPORTER_WORKLOADS")
	if s.stateOwner == nil || s.stateOwnerAuthorize == nil {
		if len(workloads) > 0 {
			return fmt.Errorf("tree budget reporter allowlist requires a composed State owner authority")
		}
		return nil
	}
	budgets := model.NewBudgetStore(s.pool)
	if err := budgets.Migrate(ctx); err != nil {
		return err
	}
	authority, err := modelapi.NewStateTreeAuthority(s.stateOwner, workloads, s.stateOwnerAuthorize)
	if err != nil {
		return err
	}
	if len(workloads) == 0 {
		s.logger.Warn("tree budget API denies all callers: AHP_MODEL_TREE_REPORTER_WORKLOADS is empty")
	}
	h, err := modelapi.NewTreeBudgetHandler(budgets, v, authorize, authority)
	if err != nil {
		return err
	}
	for _, op := range modelapi.TreeBudgetOperations {
		mux.Handle("/v1/model/"+op, h)
	}
	return nil
}
