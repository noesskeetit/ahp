package memory

import (
	"context"
	"time"
)

type BindRequest struct {
	ContextID string `json:"context_id"`
	ProjectID string `json:"project_id"`
}
type ProjectBinding struct {
	ContextID string    `json:"context_id"`
	ProjectID string    `json:"project_id"`
	CreatedAt time.Time `json:"created_at"`
}

// ProjectForContext reads a server-owned binding. A run's opaque payload is
// evidence, never the authority that chooses its memory bank.
func ProjectForContext(ctx context.Context, store Store, tenant, user, contextID string) (string, error) {
	var binding ProjectBinding
	err := store.Within(ctx, (Identity{TenantID: tenant, UserID: user}).Owner(), func(tx Tx) error { return tx.Get("binding", digest(contextID), &binding) })
	return binding.ProjectID, err
}
func (s *Service) BindProject(ctx context.Context, id Identity, key string, req BindRequest) (ProjectBinding, error) {
	var out ProjectBinding
	if id.ActorKind != "console" && id.ActorKind != "platform" {
		return out, Fail(403, "console_required", "runtime cannot choose a conversation's project")
	}
	if req.ContextID == "" || len(req.ContextID) > 200 || req.ProjectID == "" {
		return out, Fail(400, "invalid_binding", "context and private Workspace are required")
	}
	if req.ContextID != id.ContextID {
		return out, Fail(403, "wrong_context", "binding must use the verified ingress context")
	}
	err := s.mutate(ctx, id, key, "bind-project", req, func(tx Tx) (any, error) {
		binding, err := mustRecord(tx, "binding", digest(req.ContextID), ProjectBinding{ContextID: req.ContextID, ProjectID: req.ProjectID, CreatedAt: s.Now()})
		if err != nil {
			return nil, err
		}
		if binding.ProjectID != req.ProjectID {
			return nil, Fail(409, "project_binding_conflict", "a conversation cannot switch projects")
		}
		if _, err := s.touch(tx, req.ProjectID); err != nil {
			return nil, err
		}
		return binding, tx.Put("binding", digest(req.ContextID), binding)
	}, &out)
	return out, err
}
