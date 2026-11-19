package workspace

import (
	"context"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// Trusted CI for code candidates (PDF §3.7 с.37 «подключения проверок»,
// §7 с.48; root-аудит 11-workspace-store.md п.2: trusted CI привязан к
// digest, browser pass-флаг не достаточен). The candidate merges code only
// on a verdict that a registered CI runner task posted against exactly these
// bytes; the author's own verdict — even carrying a task id — is refused.
func TestTrustedCIGateForCodeCandidate(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	const alice, bob, runner = "alice", "bob", "ci-runner-1"

	w, err := e.ws.Create(ctx, e.scope(alice), "svc")
	mustOK(t, err)
	mustOK(t, e.ws.AddMember(ctx, e.scope(alice), w.Name, bob))
	mustOK(t, e.ws.AddMember(ctx, e.scope(alice), w.Name, runner))

	// Registry is owner-only and refuses self-registration: the author must
	// not be able to launder their own tasks into CI verdicts.
	mustErr(t, e.ws.RegisterCIRunner(ctx, e.scope(bob), w.Name, "ci-2"), string(contracts.ErrForbidden))
	mustErr(t, e.ws.RegisterCIRunner(ctx, e.scope(alice), w.Name, alice), string(contracts.ErrForbidden))
	mustOK(t, e.ws.RegisterCIRunner(ctx, e.scope(alice), w.Name, runner))
	runners, err := e.ws.ListCIRunners(ctx, e.scope(alice), w.Name)
	mustOK(t, err)
	if len(runners) != 1 || runners[0].Subject != runner {
		t.Fatalf("runner registry wrong: %+v", runners)
	}

	// A code candidate: main.go carries the trusted-CI burden.
	f := e.uploadSeal(t, alice, "main.go", "package main\n", labels.Set{})
	e.share(t, alice, w.Name, f)
	cand, err := e.ws.CreateCandidate(ctx, e.scope(alice), w.Name, "")
	mustOK(t, err)
	_, err = e.ws.UpdateCandidateFiles(ctx, e.scope(alice), cand.ID, []File{f})
	mustOK(t, err)

	// Author self-attests with a task id: accepted as a check, worthless for
	// merging code.
	mustOK(t, e.ws.SetCheckWithProvenance(ctx, e.scope(alice), cand.ID, "unit-tests", "passed", "",
		CheckProvenance{Task: "task-alice-1"}))
	mustErr(t, publishErr(e.ws, ctx, e.scope(alice), cand.ID, ""), string(contracts.ErrConflict))

	// An unregistered member's task verdict is equally worthless.
	mustOK(t, e.ws.SetCheckWithProvenance(ctx, e.scope(bob), cand.ID, "unit-tests", "passed", "",
		CheckProvenance{Task: "task-bob-1", Root: "root-bob"}))
	mustErr(t, publishErr(e.ws, ctx, e.scope(alice), cand.ID, ""), string(contracts.ErrConflict))

	// A registered runner with no task id carries no provenance either.
	mustOK(t, e.ws.SetCheckWithProvenance(ctx, e.scope(runner), cand.ID, "unit-tests", "passed", "",
		CheckProvenance{}))
	mustErr(t, publishErr(e.ws, ctx, e.scope(alice), cand.ID, ""), string(contracts.ErrConflict))

	// A trusted runner that says failed must be honoured, not silently passed.
	mustOK(t, e.ws.SetCheckWithProvenance(ctx, e.scope(runner), cand.ID, "unit-tests", "failed", "vet",
		CheckProvenance{Task: "ci-task-1", Root: "ci-root"}))
	mustErr(t, publishErr(e.ws, ctx, e.scope(alice), cand.ID, ""), string(contracts.ErrConflict))

	// The green verdict of the registered runner merges, and its provenance
	// is visible on the review.
	mustOK(t, e.ws.SetCheckWithProvenance(ctx, e.scope(runner), cand.ID, "unit-tests", "passed", "",
		CheckProvenance{Task: "ci-task-2", Root: "ci-root"}))
	rev, err := e.ws.Publish(ctx, e.scope(alice), cand.ID, "")
	mustOK(t, err)

	review, err := e.ws.CandidateDetails(ctx, e.scope(alice), cand.ID)
	mustOK(t, err)
	if len(review.Checks) != 1 {
		t.Fatalf("want one check, got %+v", review.Checks)
	}
	c := review.Checks[0]
	if !c.Trusted || c.RunnerTask != "ci-task-2" || c.RunnerSubject != runner {
		t.Fatalf("provenance not recorded: %+v", c)
	}
	if c.TargetDigest != rev.Digest {
		t.Fatalf("check not bound to merged digest: %s vs %s", c.TargetDigest, rev.Digest)
	}

	// Revising the bytes clears the green: the next merge needs a fresh
	// trusted run over the new digest (stale-green replay is impossible).
	f2 := e.uploadSeal(t, alice, "main2.go", "package main\nfunc main(){}\n", labels.Set{})
	e.share(t, alice, w.Name, f2)
	cand2, err := e.ws.CreateCandidate(ctx, e.scope(alice), w.Name, rev.ID)
	mustOK(t, err)
	_, err = e.ws.UpdateCandidateFiles(ctx, e.scope(alice), cand2.ID, append([]File{f}, f2))
	mustOK(t, err)
	mustErr(t, publishErr(e.ws, ctx, e.scope(alice), cand2.ID, rev.ID), string(contracts.ErrConflict))
	mustOK(t, e.ws.SetCheckWithProvenance(ctx, e.scope(runner), cand2.ID, "unit-tests", "passed", "",
		CheckProvenance{Task: "ci-task-3"}))
	rev2, err := e.ws.Publish(ctx, e.scope(alice), cand2.ID, rev.ID)
	mustOK(t, err)
	if head, err := e.ws.GetHead(ctx, e.scope(alice), w.Name); err != nil || head != rev2.ID {
		t.Fatalf("head = %q want %q (%v)", head, rev2.ID, err)
	}

	// A deregistered runner loses trust for verdicts posted afterwards.
	mustOK(t, e.ws.DeregisterCIRunner(ctx, e.scope(alice), w.Name, runner))
	f3 := e.uploadSeal(t, alice, "main3.go", "package main\nvar x = 1\n", labels.Set{})
	e.share(t, alice, w.Name, f3)
	cand3, err := e.ws.CreateCandidate(ctx, e.scope(alice), w.Name, rev2.ID)
	mustOK(t, err)
	_, err = e.ws.UpdateCandidateFiles(ctx, e.scope(alice), cand3.ID, append([]File{f, f2}, f3))
	mustOK(t, err)
	mustOK(t, e.ws.SetCheckWithProvenance(ctx, e.scope(runner), cand3.ID, "unit-tests", "passed", "",
		CheckProvenance{Task: "ci-task-4"}))
	mustErr(t, publishErr(e.ws, ctx, e.scope(alice), cand3.ID, rev2.ID), string(contracts.ErrConflict))
}

// The trusted gate must not move the document/data boundary: VerifyCandidate
// keeps being the honest integrity path there, and untrusted-but-green checks
// still merge plain documents exactly as before.
func TestTrustedCIGateKeepsDocumentsUnaffected(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	w, err := e.ws.Create(ctx, e.scope("alice"), "docs")
	mustOK(t, err)
	f := e.uploadSeal(t, "alice", "notes.md", "# notes", labels.Set{})
	rev := e.pub(t, "alice", w.Name, "", []File{f}) // untrusted "ci" check, documents
	if rev.ID == "" {
		t.Fatal("document candidate failed to merge")
	}
	// Executable mode bit is decisive even for a document-looking name.
	bin := e.uploadSeal(t, "alice", "run.txt", "\x7fELF fake", labels.Set{})
	bin.Mode = 0o755
	e.share(t, "alice", w.Name, bin)
	cand, err := e.ws.CreateCandidate(ctx, e.scope("alice"), w.Name, rev.ID)
	mustOK(t, err)
	_, err = e.ws.UpdateCandidateFiles(ctx, e.scope("alice"), cand.ID, []File{f, bin})
	mustOK(t, err)
	mustOK(t, e.ws.SetCheckWithProvenance(ctx, e.scope("alice"), cand.ID, "ci", "passed", "",
		CheckProvenance{Task: "task-a"}))
	mustErr(t, publishErr(e.ws, ctx, e.scope("alice"), cand.ID, rev.ID), string(contracts.ErrConflict))
}

// Classification is by executable bit and source names only: an extensionless
// data file is not silently reclassified into the code path.
func TestRequiresTrustedCIClassification(t *testing.T) {
	code := [][]File{
		{{Path: "cmd/main.go", Mode: 0o644}},
		{{Path: "Makefile", Mode: 0o644}},
		{{Path: "Dockerfile", Mode: 0o644}},
		{{Path: "notes.txt", Mode: 0o755}},
		{{Path: "a/b/c/shell.sh", Mode: 0o644}},
	}
	for i, files := range code {
		if !requiresTrustedCI(files) {
			t.Fatalf("case %d: %v should require trusted CI", i, files)
		}
	}
	docs := [][]File{
		{{Path: "a1", Mode: 0o644}},
		{{Path: "notes.md", Mode: 0o644}},
		{{Path: "data/report.csv", Mode: 0o600}},
		{{Path: "img.PNG", Mode: 0o644}},
		{{Path: ".hidden", Mode: 0o644}},
	}
	for i, files := range docs {
		if requiresTrustedCI(files) {
			t.Fatalf("case %d: %v should not require trusted CI", i, files)
		}
	}
}

// publishErr adapts Publish to the single-error form mustErr consumes.
func publishErr(ws *Store, ctx context.Context, sc contracts.Scope, id, head string) error {
	_, err := ws.Publish(ctx, sc, id, head)
	return err
}
