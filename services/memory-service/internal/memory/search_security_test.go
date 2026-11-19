package memory

import (
	"context"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func activate(t *testing.T, e *env, scope contracts.Scope, req ProposeRequest) Memory {
	t.Helper()
	p, err := e.mem.Propose(context.Background(), scope, req)
	mustOK(t, err)
	m, err := e.mem.Review(context.Background(), scope, p.Memory.ID, DecisionApprove, "review-"+req.OpKey)
	mustOK(t, err)
	return m
}

func TestSearchExcludesProposalsAndOtherAgents(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	_, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "pending approval", OpKey: "pending"})
	mustOK(t, err)
	other := alice
	other.Deployment = "other-agent"
	activate(t, e, other, ProposeRequest{Kind: KindAgent, Text: "foreign agent knowledge", OpKey: "foreign", Evidence: []Evidence{{SourceKind: "verified_observation"}}})
	got, err := e.mem.Search(ctx, alice, SearchRequest{Limit: 50})
	mustOK(t, err)
	if len(got.Hits) != 0 {
		t.Fatalf("unapproved or other agent records leaked: %v", got.Hits)
	}
}

func TestCandidatesApplyLabelsBeforeLimit(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	bob := e.scope("bob")
	e.addWorkspace("alice", "proj")
	e.addWorkspace("bob", "proj")
	e.setCeiling("alice", labels.Set{Subjects: []string{"alice"}})
	activate(t, e, alice, ProposeRequest{Kind: KindWorkspace, Workspace: "proj", Text: "confidential marker", OpKey: "private", Evidence: []Evidence{{SourceKind: "user_statement"}}})
	ids, err := e.mem.lexCandidates(ctx, bob, []string{"proj"}, "", "confidential")
	mustOK(t, err)
	if len(ids) != 0 {
		t.Fatalf("private record entered ranked candidates: %v", ids)
	}
}

func TestSearchMembershipRevokedDuringEmbedding(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	bob := e.scope("bob")
	e.addWorkspace("alice", "proj")
	e.addWorkspace("bob", "proj")
	activate(t, e, alice, ProposeRequest{Kind: KindWorkspace, Workspace: "proj", Text: "workspace note", OpKey: "member", Evidence: []Evidence{{SourceKind: "user_statement"}}})
	e.mem.embedder = func(ctx context.Context, text string) ([]float32, error) {
		e.members["bob"] = nil
		return embedText(ctx, text)
	}
	got, err := e.mem.Search(ctx, bob, SearchRequest{Query: "workspace", Limit: 50})
	mustOK(t, err)
	if len(got.Hits) != 0 {
		t.Fatal("revoked membership remained readable at emission")
	}
}

func TestSearchRechecksStateAndAccumulatesLabels(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	m := activate(t, e, alice, ProposeRequest{Kind: KindUser, Text: "private known fact", OpKey: "private-read"})
	calls := 0
	e.mem.readLabels = func(_ context.Context, _ contracts.Scope, set labels.Set) error {
		calls++
		if len(set.Subjects) != 1 || set.Subjects[0] != "alice" {
			t.Fatal("source labels missing")
		}
		return contracts.NewError(contracts.ErrUnavailable, "identity unavailable")
	}
	if _, err := e.mem.Search(ctx, alice, SearchRequest{Query: "private"}); err == nil {
		t.Fatal("label sink failure emitted data")
	}
	if _, err := e.mem.Get(ctx, alice, m.ID); err == nil {
		t.Fatal("Get ignored label sink failure")
	}
	if calls != 2 {
		t.Fatalf("label sink calls=%d", calls)
	}
	e.mem.readLabels = nil
	e.mem.embedder = func(ctx context.Context, text string) ([]float32, error) {
		_, err := e.pool.Exec(ctx, `UPDATE ahp_memory.memories SET state='revoked' WHERE id=$1`, m.ID)
		if err != nil {
			return nil, err
		}
		return embedText(ctx, text)
	}
	got, err := e.mem.Search(ctx, alice, SearchRequest{Query: "private"})
	mustOK(t, err)
	if len(got.Hits) != 0 {
		t.Fatal("revoked source survived final state check")
	}
}

func TestBM25AndProvenanceGraph(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	a := activate(t, e, alice, ProposeRequest{Kind: KindUser, Text: "copper copper", OpKey: "seed"})
	b := activate(t, e, alice, ProposeRequest{Kind: KindUser, Text: "separate related concept", OpKey: "neighbor"})
	hidden := activate(t, e, e.scope("bob"), ProposeRequest{Kind: KindUser, Text: "not readable", OpKey: "hidden"})
	// Fixture provenance edges exercise the query; evidence artifact publication
	// is separately tested against the actual Artifacts owner in memory_test.go.
	_, err := e.pool.Exec(ctx, `UPDATE ahp_memory.memories SET evidence='[{"source_kind":"verified_observation","artifacts":["shared-proof"]}]'::jsonb WHERE id=ANY($1)`, []string{a.ID, b.ID, hidden.ID})
	mustOK(t, err)
	e.mem.embedder = nil
	got, err := e.mem.Search(ctx, alice, SearchRequest{Query: "copper"})
	mustOK(t, err)
	seen := map[string]bool{}
	for _, h := range got.Hits {
		seen[h.Memory.ID] = true
	}
	if !seen[a.ID] || !seen[b.ID] || seen[hidden.ID] {
		t.Fatalf("graph failed authorized traversal: %v", seen)
	}
	mustOK(t, e.mem.Delete(ctx, alice, b.ID, "delete-neighbor"))
	got, err = e.mem.Search(ctx, alice, SearchRequest{Query: "copper"})
	mustOK(t, err)
	if len(got.Hits) != 1 || got.Hits[0].Memory.ID != a.ID {
		t.Fatal("deleted graph edge remained searchable")
	}
	long := activate(t, e, alice, ProposeRequest{Kind: KindUser, Text: "copper copper " + strings.Repeat("unrelated ", 100), OpKey: "long"})
	ids, err := e.mem.lexCandidates(ctx, alice, nil, KindUser, "copper")
	mustOK(t, err)
	if len(ids) != 2 || ids[0] != a.ID || ids[1] != long.ID {
		t.Fatalf("BM25 length normalization failed: %v", ids)
	}
}

func TestEmbeddingPolicyDeniesBeforeHTTPAndSpaceInvalidatesVectors(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	m := activate(t, e, alice, ProposeRequest{Kind: KindUser, Text: "index source", OpKey: "space"})
	e.mem.embeddingSpace = "another-model-revision"
	ids, err := e.mem.vectorCandidates(ctx, alice, nil, KindUser, serializeVector(mustEmbed(t, "index source")))
	mustOK(t, err)
	if len(ids) != 0 {
		t.Fatal("different embedding spaces were compared")
	}
	called := false
	e.mem.embedder = func(context.Context, string) ([]float32, error) { called = true; return nil, nil }
	e.mem.embeddingPolicy = func(context.Context, contracts.Scope, labels.Set) error {
		return contracts.NewError(contracts.ErrForbidden, "tenant route denied")
	}
	_, err = e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "forbidden egress", OpKey: "egress"})
	mustErr(t, err, contracts.ErrForbidden)
	if called {
		t.Fatal("rejected data reached embedding transport")
	}
	_, err = e.mem.Search(ctx, alice, SearchRequest{Query: m.Text})
	mustErr(t, err, contracts.ErrForbidden)
	if called {
		t.Fatal("query bypassed embedding route policy")
	}
}
func mustEmbed(t *testing.T, text string) []float32 {
	t.Helper()
	v, err := embedText(context.Background(), text)
	mustOK(t, err)
	return v
}
