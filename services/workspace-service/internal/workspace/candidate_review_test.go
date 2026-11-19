package workspace

import (
	"context"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func TestCandidateIntegrityDoesNotInventCodeCheckSuccess(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	sc := e.scope("alice")
	w, err := e.ws.Create(ctx, sc, "integrity")
	mustOK(t, err)
	f := e.uploadSeal(t, "alice", "document.txt", "verified document bytes", labels.Set{})
	c, err := e.ws.CreateCandidate(ctx, sc, w.Name, "")
	mustOK(t, err)
	c, err = e.ws.UpdateCandidateFiles(ctx, sc, c.ID, []File{f})
	mustOK(t, err)
	mustOK(t, e.ws.SetCheck(ctx, sc, c.ID, "required-code-tests", "failed", "failure must stay"))
	d, err := e.ws.VerifyCandidate(ctx, sc, c.ID)
	mustOK(t, err)
	if len(d.Files) != 1 || len(d.Checks) != 2 {
		t.Fatalf("incomplete details: %+v", d)
	}
	failed := false
	integrity := false
	for _, check := range d.Checks {
		if check.Name == "required-code-tests" {
			failed = check.Status == "failed"
		}
		if check.Name == "sealed-byte-integrity" {
			integrity = check.Status == "passed" && check.TargetDigest == c.Digest
		}
	}
	if !failed || !integrity {
		t.Fatalf("check state lost: %+v", d.Checks)
	}
	e.share(t, "alice", w.Name, f)
	if _, err = e.ws.Publish(ctx, sc, c.ID, ""); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("failed required check did not block publish: %v", err)
	}
	mustOK(t, e.ws.SetCheck(ctx, sc, c.ID, "required-code-tests", "passed", "executed by trusted test runner"))
	_, err = e.ws.Publish(ctx, sc, c.ID, "")
	mustOK(t, err)
}

func TestCandidateIntegrityRejectsWrongDigestAndForeignReads(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	sc := e.scope("alice")
	w, err := e.ws.Create(ctx, sc, "wrong-digest")
	mustOK(t, err)
	f := e.uploadSeal(t, "alice", "secret.txt", "original bytes", labels.Set{})
	c, err := e.ws.CreateCandidate(ctx, sc, w.Name, "")
	mustOK(t, err)
	f.Digest = shaHex("different bytes")
	_, err = e.ws.UpdateCandidateFiles(ctx, sc, c.ID, []File{f})
	mustOK(t, err)
	if _, err = e.ws.VerifyCandidate(ctx, sc, c.ID); err == nil {
		t.Fatal("wrong SHA accepted")
	}
	var n int
	mustOK(t, e.pool.QueryRow(ctx, "SELECT count(*) FROM ahp_workspace.checks WHERE candidate_id=$1 AND status='passed'", c.ID).Scan(&n))
	if n != 0 {
		t.Fatal("bad content gained a passing check")
	}
	if _, err = e.ws.CandidateDetails(ctx, e.scope("bob"), c.ID); err == nil {
		t.Fatal("outsider saw candidate")
	}
	mustOK(t, e.ws.AddMember(ctx, sc, w.Name, "bob"))
	if _, err = e.ws.CandidateDetails(ctx, e.scope("bob"), c.ID); err == nil {
		t.Fatal("workspace membership bypassed private artifact read")
	}
}

func TestCandidateIntegrityCannotApproveExecutableSource(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	sc := e.scope("alice")
	w, err := e.ws.Create(ctx, sc, "source-code")
	mustOK(t, err)
	f := e.uploadSeal(t, "alice", "main.go", "package main", labels.Set{})
	c, err := e.ws.CreateCandidate(ctx, sc, w.Name, "")
	mustOK(t, err)
	_, err = e.ws.UpdateCandidateFiles(ctx, sc, c.ID, []File{f})
	mustOK(t, err)
	if _, err = e.ws.VerifyCandidate(ctx, sc, c.ID); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("source code was approved: %v", err)
	}
	var count int
	mustOK(t, e.pool.QueryRow(ctx, "SELECT count(*) FROM ahp_workspace.checks WHERE candidate_id=$1", c.ID).Scan(&count))
	if count != 0 {
		t.Fatal("source code gained a fabricated check")
	}
}
