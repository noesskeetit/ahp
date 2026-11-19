//go:build ahp_data

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	legacy "github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/memory/ahpbridge"
	"github.com/ahp-platform/ahp/internal/memory/hindsight"
	ltm "github.com/ahp-platform/ahp/internal/memory/longterm"
	pgstore "github.com/ahp-platform/ahp/internal/memory/longterm/pgstore"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type hindsightAuthority interface {
	MemoryGrant(context.Context, contracts.Scope, string) (ahpbridge.Grant, error)
	AddMemoryLabels(context.Context, contracts.Scope, string, labels.Set) error
}

func (s *server) tryMountHindsightMemory(ctx context.Context, mux *http.ServeMux, verifier *auth.Verifier, authorize transport.Authorize, mode string, grants GrantLabels) error {
	if s.cascade == nil {
		return fmt.Errorf("Hindsight memory requires the data deletion coordinator (AHP_ENABLE_DATA=1)")
	}
	if mode != authorityIdentityMode {
		return fmt.Errorf("Hindsight memory requires live Identity authority")
	}
	actual, ok := grants.(hindsightAuthority)
	if !ok {
		return fmt.Errorf("Hindsight memory requires Identity grant and label journal adapters")
	}
	runtimeDeployments := envListOr("AHP_MEMORY_RUNTIME_DEPLOYMENTS")
	origin := envOr("AHP_MEMORY_HINDSIGHT_URL", "")
	if origin == "" {
		return fmt.Errorf("AHP_MEMORY_HINDSIGHT_URL is required")
	}
	engine, err := hindsight.New(origin, envOr("AHP_MEMORY_HINDSIGHT_API_KEY", ""))
	if err != nil {
		return err
	}
	art, workspaces, _, err := s.newDataPlanes(ctx)
	if err != nil {
		return err
	}
	if err := art.Migrate(ctx); err != nil {
		return err
	}
	if err := workspaces.Migrate(ctx); err != nil {
		return err
	}
	// A rollback corpus is still platform data. Retain the upstream owner API
	// for erasure if legacy tables exist, without configuring an embedding route
	// or reinterpreting the stored vectors.
	var oldOwner *legacy.Store
	var hasLegacy bool
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass('ahp_memory.memories') IS NOT NULL`).Scan(&hasLegacy); err != nil {
		return err
	}
	if hasLegacy {
		oldOwner, err = newMemoryStore(s.pool, art, workspaces, authorize)
		if err != nil {
			return err
		}
		if err = oldOwner.Migrate(ctx); err != nil {
			return err
		}
	}
	store := pgstore.UsePool(s.pool)
	if err := store.Migrate(ctx); err != nil {
		return err
	}
	stateStore := state.New(s.pool)
	access, err := ahpbridge.NewAccess(ahpbridge.Dependencies{
		Authorize: authorize, Evaluate: func(ctx context.Context, scope contracts.Scope, task, cap string) (ahpbridge.Grant, error) {
			return actual.MemoryGrant(ctx, scope, task)
		},
		AddLabels: actual.AddMemoryLabels,
		PrivateProject: func(ctx context.Context, scope contracts.Scope, project string) error {
			if _, err := workspaces.GetWorkspace(ctx, scope, project); err != nil {
				return err
			}
			members, err := workspaces.ListMembers(ctx, scope, project)
			if err != nil {
				return err
			}
			mapped := make([]ahpbridge.Member, 0, len(members))
			for _, member := range members {
				mapped = append(mapped, ahpbridge.Member{Subject: member.Subject, Role: member.Role})
			}
			return ahpbridge.RequirePrivateWorkspace(scope.Subject, mapped)
		},
		ContextProject: func(ctx context.Context, scope contracts.Scope, contextID string) (string, error) {
			project, err := ltm.ProjectForContext(ctx, store, scope.Tenant, scope.Subject, contextID)
			if errors.Is(err, ltm.ErrMissing) {
				return "", nil // A conversation without a project uses only personal memory.
			}
			return project, err
		},
		Source: func(ctx context.Context, c auth.Claims, project, ref string) error {
			return verifyMemorySource(ctx, stateStore, c, project, ref)
		},
		RuntimeDeployments: runtimeDeployments, ControlDeployments: envListOr("AHP_MEMORY_CONTROL_DEPLOYMENTS"),
	})
	if err != nil {
		return err
	}
	service := ltm.New(store, engine, access)
	handler, err := ahpbridge.NewHandler(service, verifier, authorize)
	if err != nil {
		return err
	}
	mountMemoryHandlers(mux, handler)
	s.cascade = withMemoryDeletion(s.cascade, hindsightSubjectOwner{service: service, legacyOwner: oldOwner, authorize: memoryStoreAuthorize(authorize)}, workspaces)
	go s.runHindsightMemory(ctx, service, access, stateStore)
	s.logger.Info("Hindsight memory mounted", "prefix", "/v1/memory/", "projects", "single-owner Workspace", "authority", "Identity live grants and context journal")
	return nil
}

type hindsightSubjectOwner struct {
	service     *ltm.Service
	legacyOwner *legacy.Store
	authorize   legacy.Authorize
}

func (h hindsightSubjectOwner) AuthorizeSubjectDeletion(ctx context.Context, scope contracts.Scope, subject string) error {
	if subject != scope.Subject {
		return contracts.NewError(contracts.ErrForbidden, "subject target mismatch")
	}
	return h.authorize(ctx, scope, legacy.CapabilityDeleteSubject, "subject:"+subject)
}
func (h hindsightSubjectOwner) DeleteSubject(ctx context.Context, scope contracts.Scope, subject string, keys ...string) (int, error) {
	if err := h.AuthorizeSubjectDeletion(ctx, scope, subject); err != nil {
		return 0, err
	}
	legacyCount := 0
	if h.legacyOwner != nil {
		if len(keys) == 0 {
			keys = []string{"hindsight-subject-erasure"}
		}
		var err error
		legacyCount, err = h.legacyOwner.DeleteSubject(ctx, scope, subject, keys...)
		if err != nil {
			return 0, err
		}
	}
	pending, count, err := h.service.EraseSubject(ctx, scope.Tenant, subject)
	count += legacyCount
	if err != nil {
		return 0, err
	}
	if pending {
		return count, contracts.NewError(contracts.ErrUnavailable, "Hindsight bank cleanup is pending")
	}
	return count, nil
}

func (h hindsightSubjectOwner) FenceTenant(ctx context.Context, scope contracts.Scope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := h.authorize(ctx, scope, legacy.CapabilityDeleteSubject, "tenant:"+scope.Tenant); err != nil {
		return err
	}
	if h.legacyOwner != nil {
		if err := h.legacyOwner.FenceTenant(ctx, scope); err != nil {
			return err
		}
	}
	return h.service.Store.FenceTenant(ctx, scope.Tenant)
}
func (h hindsightSubjectOwner) TenantScopes(ctx context.Context, scope contracts.Scope) ([]contracts.Scope, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if err := h.authorize(ctx, scope, legacy.CapabilityDeleteSubject, "tenant:"+scope.Tenant); err != nil {
		return nil, err
	}
	owners, err := h.service.Store.TenantOwners(ctx, scope.Tenant)
	if err != nil {
		return nil, err
	}
	out := make([]contracts.Scope, 0, len(owners))
	for _, owner := range owners {
		out = append(out, contracts.Scope{Tenant: owner.TenantID, Subject: owner.UserID, Deployment: owner.ActorID})
	}
	if h.legacyOwner != nil {
		old, err := h.legacyOwner.TenantScopes(ctx, scope)
		if err != nil {
			return nil, err
		}
		seen := map[contracts.Scope]bool{}
		for _, target := range out {
			seen[target] = true
		}
		for _, target := range old {
			if !seen[target] {
				out = append(out, target)
				seen[target] = true
			}
		}
	}
	return out, nil
}

var _ tenantMemoryOwner = hindsightSubjectOwner{}

func (s *server) runHindsightMemory(ctx context.Context, service *ltm.Service, access *ahpbridge.Access, st *state.Store) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pass, cancel := context.WithTimeout(ctx, 45*time.Second)
			events, err := st.ClaimRunCompletions(pass, 20)
			if err == nil {
				for _, event := range events {
					var payload struct {
						Type      string `json:"type"`
						ProjectID string `json:"project_id"`
					}
					err = json.Unmarshal(event.Run.Payload, &payload)
					if err == nil && payload.Type == "turn" {
						var evidence state.RunEvidence
						evidence, err = st.ReadRunEvidence(pass, event.Authority.Scope(), event.Run.ID, 99)
						if err == nil {
							sources := memoryEvidenceSources(evidence)
							if len(sources) > 0 {
								err = ahpbridge.ConsumeCompletion(pass, service, access, ahpbridge.Completion{ID: event.ID, ThreadID: event.Thread.ID, ProjectID: payload.ProjectID, Sequence: event.Run.Sequence, CreatedAt: event.CreatedAt, Claims: event.Authority, Sources: sources})
							}
						}
					}
					code := ""
					permanent := false
					if err != nil {
						code = "memory_delivery_unavailable"
						var domain *ltm.Error
						if errors.As(err, &domain) {
							code = domain.Code
							permanent = domain.Status < 500 && domain.Status != 408 && domain.Status != 429
						}
						if contracts.Code(err) == contracts.ErrNotFound || contracts.Code(err) == contracts.ErrForbidden {
							permanent = true
							code = "source_unavailable"
						}
					}
					if markErr := st.FinishRunCompletion(pass, event.ID, event.LeaseKey, code, permanent); markErr != nil {
						s.logger.Warn("memory completion acknowledgement failed")
					}
				}
			}
			if err := service.Tick(pass); err != nil && pass.Err() == nil {
				s.logger.Warn("memory worker pass failed")
			}
			cancel()
		}
	}
}
