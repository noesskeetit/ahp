//go:build ahp_data

package main

import (
	"context"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func (s *server) attachWorkspaceIAM(ws *workspace.Store) {
	if s.iam == nil {
		return
	}
	ids := s.identityStore()
	ws.SetResourceAuthority(func(ctx context.Context, scope contracts.Scope, facts workspace.MembershipFacts, action string) error {
		return ids.RequireResource(ctx, identity.ResourceRequest{Scope: scope, Resource: identity.Resource{Kind: "project", ID: facts.ProjectID}, Action: action, ProjectRole: facts.Role})
	})
}
