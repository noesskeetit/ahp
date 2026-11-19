package memory

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func workspaceSearchRequest(t *testing.T, raw string) SearchRequest {
	t.Helper()
	var request SearchRequest
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		t.Fatal(err)
	}
	return request
}

func TestWorkspaceSearchNarrowsBeforeRankingAndLabelEmission(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	scope := e.scope("alice")
	ids := map[string]string{}
	for _, project := range []string{"selected", "other"} {
		e.addWorkspace("alice", project)
		p, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindWorkspace, Workspace: project, Text: "release policy for " + project, OpKey: "proposal-" + project, Evidence: []Evidence{{SourceKind: "user_statement"}}})
		mustOK(t, err)
		_, err = e.mem.Review(ctx, scope, p.Memory.ID, DecisionApprove, "review-"+project)
		mustOK(t, err)
		ids[project] = p.Memory.ID
	}
	var emitted labels.Set
	e.mem.readLabels = func(_ context.Context, _ contracts.Scope, set labels.Set) error {
		emitted = labels.Merge(emitted, set)
		return nil
	}
	for _, request := range []string{
		`{"workspace":"selected","query":"release","limit":50}`,
		`{"workspace":"selected","kind":"workspace","limit":50}`,
	} {
		emitted = labels.Set{}
		got, err := e.mem.Search(ctx, scope, workspaceSearchRequest(t, request))
		mustOK(t, err)
		if len(got.Hits) != 1 || got.Hits[0].Memory.ID != ids["selected"] {
			t.Fatalf("selected workspace admitted other corpus: %+v", got.Hits)
		}
		if len(emitted.Workspaces) != 1 || emitted.Workspaces[0] != "selected" {
			t.Fatalf("other project tainted reading task: %v", emitted.Workspaces)
		}
	}
}

func TestWorkspaceSearchRefusesRevokedMembershipBeforeEmbedding(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	var calls atomic.Int32
	e.mem.embedder = func(ctx context.Context, text string) ([]float32, error) { calls.Add(1); return embedText(ctx, text) }
	scope := e.scope("alice")
	e.addWorkspace("alice", "selected")
	e.members["alice"] = nil // already-issued scope remains valid; membership was revoked
	_, err := e.mem.Search(ctx, scope, workspaceSearchRequest(t, `{"workspace":"selected","query":"private project query"}`))
	mustErr(t, err, contracts.ErrForbidden)
	if calls.Load() != 0 {
		t.Fatal("revoked project query reached embedding provider")
	}
}

func TestWorkspaceSearchRejectsContradictoryScope(t *testing.T) {
	e := newEnv(t)
	for _, raw := range []string{
		`{"workspace":"selected","kind":"user"}`,
		`{"workspace":"selected","kind":"agent"}`,
		`{"workspace":"https://foreign/"}`,
	} {
		_, err := e.mem.Search(t.Context(), e.scope("alice"), workspaceSearchRequest(t, raw))
		mustErr(t, err, contracts.ErrInvalid)
	}
}
