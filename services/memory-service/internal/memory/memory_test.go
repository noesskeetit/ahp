package memory

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func ctxT(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	return c
}

// TestProposeIdempotencyAndKeyReuse exercises the operation-key contract: a
// replay returns the same record, a reuse with different args is refused.
func TestProposeIdempotencyAndKeyReuse(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")

	res, err := e.mem.Propose(ctx, alice, ProposeRequest{
		Kind: KindUser, Text: "alice prefers tea", OpKey: "opk-1", SourceKind: "chat",
	})
	mustOK(t, err)
	if res.Replay {
		t.Fatal("first write must not be a replay")
	}
	if res.Memory.State != StateProposed {
		t.Fatalf("Propose must land as proposed, got %s", res.Memory.State)
	}
	id := res.Memory.ID

	replay, err := e.mem.Propose(ctx, alice, ProposeRequest{
		Kind: KindUser, Text: "alice prefers tea", OpKey: "opk-1", SourceKind: "chat",
	})
	mustOK(t, err)
	if !replay.Replay || replay.Memory.ID != id {
		t.Fatalf("expected replay of %s, got replay=%v id=%s", id, replay.Replay, replay.Memory.ID)
	}

	_, err = e.mem.Propose(ctx, alice, ProposeRequest{
		Kind: KindUser, Text: "alice prefers coffee", OpKey: "opk-1", SourceKind: "chat",
	})
	mustErr(t, err, string(contracts.ErrKeyReuse))
}

// TestSecretFirewall confirms a secret-tainted ceiling or evidence is refused
// outright (secret data may never enter memory).
func TestSecretFirewall(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")

	e.setCeiling("alice", labels.Set{Taints: []string{labels.TaintSecret}})
	_, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "secret via ceiling", OpKey: "opk-ceiling"})
	mustErr(t, err, string(contracts.ErrForbidden))

	e.setCeiling("alice", labels.Set{})
	_, err = e.mem.Propose(ctx, alice, ProposeRequest{
		Kind: KindWorkspace, Workspace: "proj", Text: "secret via evidence", OpKey: "opk-ev",
		Evidence: []Evidence{{SourceKind: "doc", Taints: []string{labels.TaintSecret}}},
	})
	mustErr(t, err, string(contracts.ErrForbidden))
}

// TestEvidenceRequired confirms shared scopes require evidence and a source
// kind, while a user-scope note does not.
func TestEvidenceRequired(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	e.addWorkspace("alice", "proj")

	_, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindWorkspace, Workspace: "proj", Text: "no ev", OpKey: "opk-ws"})
	mustErr(t, err, string(contracts.ErrForbidden))
	_, err = e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindAgent, Text: "no ev", OpKey: "opk-ag"})
	mustErr(t, err, string(contracts.ErrForbidden))
	_, err = e.mem.Propose(ctx, alice, ProposeRequest{
		Kind: KindWorkspace, Workspace: "proj", Text: "bad ev", OpKey: "opk-ws2",
		Evidence: []Evidence{{SourceKind: ""}},
	})
	mustErr(t, err, string(contracts.ErrForbidden))

	if _, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "private note", OpKey: "opk-user"}); err != nil {
		t.Fatalf("user scope must not require evidence: %v", err)
	}
}

// TestPrivateCeilingAgentForbiddenAndWorkspaceDowngrade enforces the PDF rule:
// a private source may not enter scope agent, and into a workspace rubric it is
// downgraded to private_to_user.
func TestPrivateCeilingAgentForbiddenAndWorkspaceDowngrade(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	e.setCeiling("alice", labels.Set{Subjects: []string{"alice"}})
	e.addWorkspace("alice", "proj")

	_, err := e.mem.Propose(ctx, alice, ProposeRequest{
		Kind: KindAgent, Text: "must not enter agent", OpKey: "opk-ag",
		Evidence: []Evidence{{SourceKind: "doc"}},
	})
	mustErr(t, err, string(contracts.ErrForbidden))

	res, err := e.mem.Propose(ctx, alice, ProposeRequest{
		Kind: KindWorkspace, Workspace: "proj", Text: "private fact in project", OpKey: "opk-ws",
		Evidence: []Evidence{{SourceKind: "doc"}},
	})
	mustOK(t, err)
	if len(res.Memory.Labels.Subjects) != 1 || res.Memory.Labels.Subjects[0] != "alice" {
		t.Fatalf("private ceiling into workspace must downgrade to private_to_user, got subjects=%v", res.Memory.Labels.Subjects)
	}
}

// TestTwoSubjectsIndependent is the core privacy property for acceptance 5: a
// private fact written by one subject never leaks to another subject through
// Get or Search, even when both are members of the same workspace and even
// after the fact is written into a shared (workspace) rubric. Meanwhile a
// genuinely tenant-wide (agent) memory is shared.
func TestTwoSubjectsIndependent(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	bob := e.scope("bob")
	e.addWorkspace("alice", "proj")
	e.addWorkspace("bob", "proj") // same live workspace membership

	// (1) Alice's user-scope memory is invisible to Bob.
	aliceUser, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "alice medical note", OpKey: "opk-a-user"})
	if err != nil {
		t.Fatal(err)
	}

	// (2) Alice writes a private-ceiling fact into the shared project rubric
	// (downgraded to private_to_user); Bob must NOT read it despite membership.
	e.setCeiling("alice", labels.Set{Subjects: []string{"alice"}})
	privRes, err := e.mem.Propose(ctx, alice, ProposeRequest{
		Kind: KindWorkspace, Workspace: "proj", Text: "alice private in project", OpKey: "opk-a-ws",
		Evidence: []Evidence{{SourceKind: "doc"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	e.setCeiling("alice", labels.Set{})

	// (3) Alice writes a tenant-wide agent memory (shared with everyone).
	agRes, err := e.mem.Propose(ctx, alice, ProposeRequest{
		Kind: KindAgent, Text: "team uses go", OpKey: "opk-a-agent",
		Evidence: []Evidence{{SourceKind: "doc"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Bob cannot Get Alice's private workspace-downgraded record.
	if _, err := e.mem.Get(ctx, bob, privRes.Memory.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("Bob must not Get private_to_user record, err=%v code=%s", err, contracts.Code(err))
	}
	// Bob CAN Get the shared agent record.
	if _, err := e.mem.Get(ctx, bob, agRes.Memory.ID); err != nil {
		t.Fatalf("Bob should Get shared agent record: %v", err)
	}

	// Bob's search over everything must never surface Alice's private facts.
	for _, q := range []string{"medical", "alice", "private", "project"} {
		sr, err := e.mem.Search(ctx, bob, SearchRequest{Query: q, Limit: 50})
		if err != nil {
			t.Fatalf("search %q: %v", q, err)
		}
		for _, h := range sr.Hits {
			if h.Memory.Subject == "alice" && h.Memory.Kind != KindAgent {
				t.Fatalf("Bob searched private alice record via %q: kind=%s text=%q", q, h.Memory.Kind, h.Memory.Text)
			}
		}
	}
	// Direct-by-id enumeration as Bob (no query) must exclude Alice's private ws record.
	sr, err := e.mem.Search(ctx, bob, SearchRequest{Limit: 50})
	mustOK(t, err)
	for _, h := range sr.Hits {
		if h.Memory.ID == privRes.Memory.ID {
			t.Fatal("Bob's enumeration must not include Alice's private_to_user record")
		}
	}

	// (5) Dedup oracle: Bob proposing the SAME text as Alice's private user
	// memory must not learn her record exists. The Similar hints for the user
	// kind are subject-scoped, so Bob's proposal must not name Alice's id even
	// though the text is byte-identical (exact digest + cosine both match).
	bobDup, err := e.mem.Propose(ctx, bob, ProposeRequest{Kind: KindUser, Text: "alice medical note", OpKey: "opk-b-dup"})
	if err != nil {
		t.Fatalf("bob duplicate propose: %v", err)
	}
	for _, sh := range bobDup.Similar {
		if sh.ID == aliceUser.Memory.ID {
			t.Fatalf("dedup oracle leaked Alice's user record id=%s (score=%.3f exact=%v) to Bob", sh.ID, sh.Score, sh.Exact)
		}
	}
	// Sanity: a same-subject proposal of the same text DOES see the exact match,
	// proving the fix subject-scopes the oracle rather than disabling de-dup.
	aliceDup, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "alice medical note", OpKey: "opk-a-dup"})
	if err != nil {
		t.Fatalf("alice duplicate propose: %v", err)
	}
	found := false
	for _, sh := range aliceDup.Similar {
		if sh.ID == aliceUser.Memory.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("same-subject dedup must still report the exact match")
	}

	// Alice still reads her own private record.
	if _, err := e.mem.Get(ctx, alice, privRes.Memory.ID); err != nil {
		t.Fatalf("Alice must read her own private record: %v", err)
	}
}

// TestGetNoOracleNotFound confirms unknown/malformed ids are indistinguishable
// (no existence oracle).
func TestGetNoOracleNotFound(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	for _, bad := range []string{"nope-not-an-id!!", "mem-0000000000000000000000"} {
		if _, err := e.mem.Get(ctx, alice, bad); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("Get(%q) code=%s want NOT_FOUND", bad, contracts.Code(err))
		}
	}
}

// TestDedupExact confirms an identical text re-proposed under a fresh key is
// reported as an exact duplicate hint.
func TestDedupExact(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	first, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "the project uses postgres", OpKey: "opk-d1"})
	mustOK(t, err)
	if len(first.Similar) != 0 {
		t.Fatalf("first write should have no similar, got %d", len(first.Similar))
	}
	second, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "the project uses postgres", OpKey: "opk-d2"})
	mustOK(t, err)
	var exact bool
	for _, s := range second.Similar {
		if s.Exact && s.ID == first.Memory.ID {
			exact = true
		}
	}
	if !exact {
		t.Fatalf("expected an exact duplicate hint for %s, got %+v", first.Memory.ID, second.Similar)
	}
}

// TestReviewApproveRejectSupersede exercises proposal review and supersedes.
func TestReviewApproveRejectSupersede(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")

	a, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "v1", OpKey: "opk-r-a"})
	mustOK(t, err)
	approved, err := e.mem.Review(ctx, alice, a.Memory.ID, DecisionApprove, "review-a")
	mustOK(t, err)
	if approved.State != StateActive {
		t.Fatalf("approve should activate, got %s", approved.State)
	}
	// Re-review of an active record is a conflict.
	_, err = e.mem.Review(ctx, alice, a.Memory.ID, DecisionApprove, "review-a-again")
	mustErr(t, err, string(contracts.ErrConflict))
	// Bad decision is invalid.
	bad, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "bad decision target", OpKey: "opk-r-bad"})
	mustOK(t, err)
	_, err = e.mem.Review(ctx, alice, bad.Memory.ID, "maybe", "review-bad")
	mustErr(t, err, string(contracts.ErrInvalid))

	// Supersede: approve b which supersedes active a -> a becomes superseded.
	b, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "v2", OpKey: "opk-r-b", Supersedes: a.Memory.ID})
	mustOK(t, err)
	if _, err := e.mem.Review(ctx, alice, b.Memory.ID, DecisionApprove, "review-b"); err != nil {
		t.Fatal(err)
	}
	if got, err := e.mem.Get(ctx, alice, a.Memory.ID); err != nil {
		t.Fatal(err)
	} else if got.State != StateSuperseded {
		t.Fatalf("superseded target must move to superseded, got %s", got.State)
	}

	// Reject path.
	c, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "to reject", OpKey: "opk-r-c"})
	mustOK(t, err)
	rejected, err := e.mem.Review(ctx, alice, c.Memory.ID, DecisionReject, "review-c")
	mustOK(t, err)
	if rejected.State != StateRejected {
		t.Fatalf("reject should set rejected, got %s", rejected.State)
	}
}

// TestAddLabelsNarrowsVisibility shows AddLabels can only tighten visibility.
func TestAddLabelsNarrowsVisibility(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	bob := e.scope("bob")

	// A tenant-wide agent memory is readable by another subject.
	res, err := e.mem.Propose(ctx, alice, ProposeRequest{
		Kind: KindAgent, Text: "shared convention", OpKey: "opk-lbl",
		Evidence: []Evidence{{SourceKind: "doc"}},
	})
	mustOK(t, err)
	if _, err := e.mem.Get(ctx, bob, res.Memory.ID); err != nil {
		t.Fatalf("shared agent record should be readable by Bob before narrowing: %v", err)
	}
	// The narrowing commits, but its content-bearing response now observes
	// the new ACL: Alice cannot receive the source that she restricted to Bob.
	_, err = e.mem.AddLabels(ctx, alice, res.Memory.ID, "opk-lbl-2", labels.Set{Subjects: []string{"bob"}})
	mustErr(t, err, contracts.ErrNotFound)
	if _, err := e.mem.Get(ctx, alice, res.Memory.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("after narrowing to Bob, Alice must not read it, code=%s", contracts.Code(err))
	}
	if _, err := e.mem.Get(ctx, bob, res.Memory.ID); err != nil {
		t.Fatalf("Bob must read the record narrowed to him: %v", err)
	}
}

// TestDeleteTombstone confirms deletion leaves every read path.
func TestDeleteTombstone(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	res, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "delete me please", OpKey: "opk-del"})
	mustOK(t, err)
	mustOK(t, e.mem.Delete(ctx, alice, res.Memory.ID, "opk-del-2"))
	if _, err := e.mem.Get(ctx, alice, res.Memory.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("deleted record must be gone from Get, code=%s", contracts.Code(err))
	}
	// Delete is idempotent.
	mustOK(t, e.mem.Delete(ctx, alice, res.Memory.ID, "opk-del-2"))
	// Search excludes it.
	sr, err := e.mem.Search(ctx, alice, SearchRequest{Query: "delete", Limit: 20})
	mustOK(t, err)
	for _, h := range sr.Hits {
		if h.Memory.ID == res.Memory.ID {
			t.Fatal("deleted record surfaced in search")
		}
	}
}

// TestDeleteSubject confirms the compliance hook tombstones all a subject's
// memories across scopes and is idempotent.
func TestDeleteSubject(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	bob := e.scope("bob")

	if _, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "u1", OpKey: "opk-ds-u1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.mem.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "u2", OpKey: "opk-ds-u2"}); err != nil {
		t.Fatal(err)
	}
	activate(t, e, bob, ProposeRequest{Kind: KindUser, Text: "bob keeps", OpKey: "opk-ds-b"})

	n, err := e.mem.DeleteSubject(ctx, alice, "alice", "delete-subject")
	mustOK(t, err)
	if n != 2 {
		t.Fatalf("expected 2 tombstoned for alice, got %d", n)
	}
	again, err := e.mem.DeleteSubject(ctx, alice, "alice", "delete-subject-next")
	mustOK(t, err)
	if again != 0 {
		t.Fatalf("second delete-subject should be idempotent, got %d", again)
	}
	// Bob is untouched.
	bobSearch, err := e.mem.Search(ctx, bob, SearchRequest{Limit: 20})
	mustOK(t, err)
	if len(bobSearch.Hits) != 1 {
		t.Fatalf("Bob should still have exactly 1 memory, got %d", len(bobSearch.Hits))
	}
}

// TestSearchHybrid confirms lexical search returns the relevant record for its
// owner.
func TestSearchHybrid(t *testing.T) {
	e := newEnv(t)
	ctx := ctxT(t)
	alice := e.scope("alice")
	activate(t, e, alice, ProposeRequest{Kind: KindUser, Text: "alice prefers coffee in the morning", OpKey: "opk-s1"})
	activate(t, e, alice, ProposeRequest{Kind: KindUser, Text: "alice enjoys hiking", OpKey: "opk-s2"})
	sr, err := e.mem.Search(ctx, alice, SearchRequest{Query: "coffee", Limit: 10})
	mustOK(t, err)
	var found bool
	for _, h := range sr.Hits {
		if h.Memory.Text == "alice prefers coffee in the morning" {
			found = true
		}
	}
	if !found {
		t.Fatalf("lexical search missed the coffee memory: %+v", sr.Hits)
	}
}
