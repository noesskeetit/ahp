package memory

import (
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func TestSupersedesCannotCrossWorkspace(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	scope := e.scope("alice")
	for _, w := range []string{"selected", "other"} {
		e.addWorkspace("alice", w)
	}
	a, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindWorkspace, Workspace: "other", Text: "source", OpKey: "source", Evidence: []Evidence{{SourceKind: "user_statement"}}})
	mustOK(t, err)
	_, err = e.mem.Review(ctx, scope, a.Memory.ID, DecisionApprove, "source-review")
	mustOK(t, err)
	_, err = e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindWorkspace, Workspace: "selected", Text: "wrong namespace correction", OpKey: "correction", Supersedes: a.Memory.ID, Evidence: []Evidence{{SourceKind: "agent_proposal"}}})
	mustErr(t, err, contracts.ErrForbidden)
}

func TestSupersedesInheritsPrivateSourceAndReplaysAfterReview(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	scope := e.scope("alice")
	e.addWorkspace("alice", "selected")
	e.setCeiling("alice", labels.Set{Subjects: []string{"alice"}})
	a, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindWorkspace, Workspace: "selected", Text: "private source", OpKey: "source", Evidence: []Evidence{{SourceKind: "user_statement"}}})
	mustOK(t, err)
	_, err = e.mem.Review(ctx, scope, a.Memory.ID, DecisionApprove, "source-review")
	mustOK(t, err)
	e.setCeiling("alice", labels.Set{})
	req := ProposeRequest{Kind: KindWorkspace, Workspace: "selected", Text: "corrected private fact", OpKey: "correction", Supersedes: a.Memory.ID, Evidence: []Evidence{{SourceKind: "agent_proposal"}}}
	p, err := e.mem.Propose(ctx, scope, req)
	mustOK(t, err)
	if len(p.Memory.Labels.Subjects) != 1 || p.Memory.Labels.Subjects[0] != "alice" {
		t.Fatalf("correction lost private source: %v", p.Memory.Labels)
	}
	_, err = e.mem.Review(ctx, scope, p.Memory.ID, DecisionApprove, "correction-review")
	mustOK(t, err)
	replay, err := e.mem.Propose(ctx, scope, req)
	mustOK(t, err)
	if !replay.Replay || replay.Memory.ID != p.Memory.ID {
		t.Fatal("reviewed supersession broke same-key replay")
	}
}

func TestSupersedesReviewKeepsSourceNarrowingAfterProposal(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	scope := e.scope("alice")
	e.addWorkspace("alice", "selected")
	a, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindWorkspace, Workspace: "selected", Text: "original", OpKey: "source", Evidence: []Evidence{{SourceKind: "user_statement"}}})
	mustOK(t, err)
	_, err = e.mem.Review(ctx, scope, a.Memory.ID, DecisionApprove, "source-review")
	mustOK(t, err)
	p, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindWorkspace, Workspace: "selected", Text: "correction", OpKey: "correction", Supersedes: a.Memory.ID, Evidence: []Evidence{{SourceKind: "agent_proposal"}}})
	mustOK(t, err)
	_, err = e.mem.AddLabels(ctx, scope, a.Memory.ID, "narrow-source", labels.Set{Subjects: []string{"alice"}})
	mustOK(t, err)
	active, err := e.mem.Review(ctx, scope, p.Memory.ID, DecisionApprove, "correction-review")
	mustOK(t, err)
	if len(active.Labels.Subjects) != 1 || active.Labels.Subjects[0] != "alice" {
		t.Fatalf("review lost source narrowing: %v", active.Labels)
	}
}

func TestSupersedesReviewRefusesAStaleSecondReplacement(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	scope := e.scope("alice")
	e.addWorkspace("alice", "selected")
	req := ProposeRequest{Kind: KindWorkspace, Workspace: "selected", Text: "original", OpKey: "source", Evidence: []Evidence{{SourceKind: "user_statement"}}}
	a, err := e.mem.Propose(ctx, scope, req)
	mustOK(t, err)
	_, err = e.mem.Review(ctx, scope, a.Memory.ID, DecisionApprove, "source-review")
	mustOK(t, err)
	proposals := make([]string, 2)
	for i, key := range []string{"first", "second"} {
		req.OpKey = key
		req.Text = key
		req.Supersedes = a.Memory.ID
		p, err := e.mem.Propose(ctx, scope, req)
		mustOK(t, err)
		proposals[i] = p.Memory.ID
	}
	_, err = e.mem.Review(ctx, scope, proposals[0], DecisionApprove, "first-review")
	mustOK(t, err)
	_, err = e.mem.Review(ctx, scope, proposals[1], DecisionApprove, "second-review")
	mustErr(t, err, contracts.ErrConflict)
	second, err := e.mem.Get(ctx, scope, proposals[1])
	mustOK(t, err)
	if second.State != StateProposed {
		t.Fatal("stale correction became active")
	}
}
