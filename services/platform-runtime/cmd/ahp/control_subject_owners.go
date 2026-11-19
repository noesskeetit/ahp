package main

import (
	"context"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"sort"
)

// SubjectDeletionOwner fences admission/dispatch across every deployment before
// discovery, then erases its own data only when effects and accounting settle.
// UNKNOWN and unavailable remote owners must keep control deletion pending.
type SubjectDeletionOwner interface {
	FenceSubject(context.Context, string, string) error
	DeleteSubject(context.Context, string, string) error
}

func (s *server) registerSubjectDeletionOwner(name string, owner SubjectDeletionOwner) {
	if s.subjectDeletionOwners == nil {
		s.subjectDeletionOwners = map[string]SubjectDeletionOwner{}
	}
	s.subjectDeletionOwners[name] = owner
}
func snapshotSubjectOwners(owners map[string]SubjectDeletionOwner) map[string]SubjectDeletionOwner {
	result := make(map[string]SubjectDeletionOwner, len(owners))
	for name, owner := range owners {
		result[name] = owner
	}
	return result
}
func (c *stateDeletionCoordinator) subjectOwnerNames() ([]string, error) {
	for _, name := range c.control.RequiredSubjectOwners {
		if name == "" || c.control.SubjectOwners[name] == nil {
			return nil, contracts.NewError(contracts.ErrUnavailable, "ahp: subject deletion owner unavailable: "+name)
		}
	}
	names := make([]string, 0, len(c.control.SubjectOwners))
	for name, owner := range c.control.SubjectOwners {
		if name == "" || owner == nil {
			return nil, contracts.NewError(contracts.ErrUnavailable, "ahp: subject deletion owner unavailable")
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

var requiredSubjectOwners = []string{"compute", "gateway", "model", "tools", "triggers", "registry", "identity"}

func (s *server) registerControlSubjectOwners() {
	ids := identity.New(s.pool)
	resolver := func(ctx context.Context, tenant, task string) (string, error) {
		owner, err := ids.TaskSubject(ctx, tenant, task)
		if err == nil {
			return owner, nil
		}
		if contracts.Code(err) != contracts.ErrNotFound {
			return "", err
		}
		if gateway, ok := s.subjectDeletionOwners["gateway"].(interface {
			TaskSubject(context.Context, string, string) (string, error)
		}); ok {
			return gateway.TaskSubject(ctx, tenant, task)
		}
		return "", err
	}
	s.registerSubjectDeletionOwner("registry", registry.New(s.pool, registry.WithTaskSubjectResolver(resolver)))
	s.registerSubjectDeletionOwner("identity", ids)
}
