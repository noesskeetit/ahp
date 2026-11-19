//go:build ahp_data

package main

import (
	"context"
	"fmt"
	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/jackc/pgx/v5/pgxpool"
)

// stateLiveness composes owner APIs. The combined PoC wires local owners; each
// owner can use its own database pool. A remote topology must provide the same
// typed contract through authenticated clients rather than sharing SQL access.
type stateLiveness struct{ owners []references.Owner }

func newStateLiveness(pool *pgxpool.Pool) *stateLiveness {
	memoryOwner := memory.ReferenceSource(pool, envOr("AHP_MEMORY_SCHEMA", "ahp_memory"))
	// An explicitly configured legacy schema is a required owner, including
	// on a native deployment with a rollback corpus. A typo must fail closed.
	if envOr("AHP_MEMORY_BACKEND", "hindsight") == "hindsight" && envOr("AHP_MEMORY_SCHEMA", "") == "" {
		memoryOwner = memory.ReferenceSourceIfPresent(pool, envOr("AHP_MEMORY_SCHEMA", "ahp_memory"))
	}
	out := &stateLiveness{owners: []references.Owner{
		state.CommonReferenceSourceIfPresent(pool, envOr("AHP_STATE_SCHEMA", "ahp_state"), commonStateConfigured()),
		gateway.ReferenceSource(pool, envOr("AHP_EXEC_SCHEMA", "ahp_exec")),
		identity.ReferenceSource(pool, envOr("AHP_IDENTITY_SCHEMA", "ahp_identity")),
		workspace.ReferenceSource(pool, envOr("AHP_WORKSPACE_SCHEMA", "ahp_workspace")),
		memoryOwner,
	}}
	return out
}
func (l *stateLiveness) RefsAlive(ctx context.Context, tenant string, refs []artifacts.Ref) (map[artifacts.Ref]bool, error) {
	in := make([]references.Ref, len(refs))
	out := map[artifacts.Ref]bool{}
	for i, r := range refs {
		in[i] = references.Ref{Kind: r.Kind, ID: r.ID}
		out[r] = false
	}
	for _, owner := range l.owners {
		result, err := owner.InspectReferences(ctx, tenant, in, nil)
		if err != nil {
			return nil, fmt.Errorf("liveness: reference owner unavailable: %w", err)
		}
		for _, r := range refs {
			if result.Alive[references.Ref{Kind: r.Kind, ID: r.ID}] {
				out[r] = true
			}
		}
	}
	return out, nil
}
func (l *stateLiveness) ArtifactsReferenced(ctx context.Context, tenant string, arts []artifacts.ArtifactQuery) (map[string]bool, error) {
	in := make([]references.Artifact, len(arts))
	out := map[string]bool{}
	for i, a := range arts {
		in[i] = references.Artifact{ID: a.ID, Digest: a.Digest}
	}
	for _, owner := range l.owners {
		result, err := owner.InspectReferences(ctx, tenant, nil, in)
		if err != nil {
			return nil, fmt.Errorf("liveness: artifact reference owner unavailable: %w", err)
		}
		for id, used := range result.Referenced {
			if used {
				out[id] = true
			}
		}
	}
	return out, nil
}
