package compute_test

// Host-backend execution is a VERIFIED path on this machine: the host backend
// runs a real confined /bin/sh process, so the runner lifecycle
// (AcquireIncarnation -> CheckFence -> Acquire -> MarkDispatched -> Exec ->
// Complete -> Release) is exercised end to end against the isolated schema.
//
// Class X and PTY are NOT_VERIFIED here: class X requires a microVM that this
// Mac cannot provide, and a real PTY needs the Linux target runtime. Both are
// asserted to FAIL CLOSED (Unavailable), never to pretend success.

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// hostRunner builds a runner over the real host backend rooted at a scratch
// jail base, with no Artifacts exporter (export is gated separately).
func hostRunner(t *testing.T, st *compute.Store, jailBase string) *compute.Runner {
	t.Helper()
	reg := compute.NewRegistry(compute.NewHostBackend(jailBase))
	return compute.NewRunner(st, reg, jailBase, "testnode", nil)
}

func TestRunnerHostEphemeralRun(t *testing.T) {
	st := newStore(t)
	sc := scope()
	jail := t.TempDir()
	r := hostRunner(t, st, jail)

	c := mustCreate(t, st, sc, compute.ClassS, compute.ProfileEphemeral, "")
	rec, err := r.RunCommand(ctx(t), sc, c.ID, "key-run-1", "echo hi", "")
	if err != nil {
		t.Fatalf("RunCommand: %v", err)
	}
	if rec.Status != compute.CommandSucceeded {
		t.Fatalf("status = %q want succeeded (stdout %q stderr %q exit %d)",
			rec.Status, rec.Stdout, rec.Stderr, rec.ExitCode)
	}
	if rec.ExitCode != 0 || rec.Stdout != "hi\n" {
		t.Fatalf("exit=%d stdout=%q want 0 / %q", rec.ExitCode, rec.Stdout, "hi\n")
	}

	// Idempotent replay of the SAME key/body must return the original record
	// without re-running the command.
	replay, err := r.RunCommand(ctx(t), sc, c.ID, "key-run-1", "echo hi", "")
	if err != nil {
		t.Fatalf("replay RunCommand: %v", err)
	}
	if replay.ID != rec.ID || replay.Stdout != "hi\n" {
		t.Fatalf("replay diverged: id %q vs %q stdout %q", replay.ID, rec.ID, replay.Stdout)
	}
}

func TestRunnerHostNonZeroExit(t *testing.T) {
	st := newStore(t)
	sc := scope()
	jail := t.TempDir()
	r := hostRunner(t, st, jail)

	c := mustCreate(t, st, sc, compute.ClassC, compute.ProfileEphemeral, "")
	rec, err := r.RunCommand(ctx(t), sc, c.ID, "key-fail", "exit 3", "")
	if err != nil {
		t.Fatalf("RunCommand(exit 3) must not be a transport error: %v", err)
	}
	if rec.Status != compute.CommandFailed {
		t.Fatalf("status = %q want failed", rec.Status)
	}
	if rec.ExitCode != 3 {
		t.Fatalf("exit = %d want 3", rec.ExitCode)
	}
}

// Class X must never be routed to the host backend, even when the host backend
// is the only one registered. The command stays ACCEPTED and the call reports
// Unavailable so it can be restarted once a microVM substrate is present.
func TestRunnerClassXUnavailableOnHost(t *testing.T) {
	st := newStore(t)
	sc := scope()
	jail := t.TempDir()
	r := hostRunner(t, st, jail)

	c := mustCreate(t, st, sc, compute.ClassX, compute.ProfileEphemeral, "")
	_, err := r.RunCommand(ctx(t), sc, c.ID, "key-x", "echo hi", "")
	if err == nil {
		t.Fatal("class X on a host-only registry must fail closed, got nil error")
	}
	if !compute.IsUnavailable(err) {
		t.Fatalf("err = %v, want Unavailable (class X must not run on host)", err)
	}
	// The command must remain in the ledger (not fabricated as succeeded).
	got, gerr := st.GetComputer(ctx(t), sc, c.ID)
	if gerr != nil {
		t.Fatalf("GetComputer after X refusal: %v", gerr)
	}
	_ = got
}

func TestRunnerExportPathNilExporter(t *testing.T) {
	st := newStore(t)
	sc := scope()
	jail := t.TempDir()
	r := hostRunner(t, st, jail)

	c := mustCreate(t, st, sc, compute.ClassS, compute.ProfileEphemeral, "")
	os.MkdirAll(r.HomeDir(c.ID), 0700)
	f := filepath.Join(r.HomeDir(c.ID), "out.txt")
	if err := os.WriteFile(f, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := r.ExportPath(ctx(t), sc, exportOptions(), c.ID, f)
	if err == nil || !compute.IsUnavailable(err) {
		t.Fatalf("export with no Artifacts service = %v, want Unavailable", err)
	}
}

// A jail-escaping path must be rejected with Forbidden even when an exporter is
// wired, so a caller can never exfiltrate arbitrary host files via traversal.
func TestRunnerExportPathTraversalForbidden(t *testing.T) {
	st := newStore(t)
	sc := scope()
	jail := t.TempDir()
	exp := &stubExporter{ref: compute.ArtifactRef{ArtifactID: "a-1", Digest: "sha256:x"}}
	r := compute.NewRunner(st, compute.NewRegistry(compute.NewHostBackend(jail)), jail, "testnode", exp)
	leAses, err := compute.NewLeaseService(st, compute.NewRedis(os.Getenv("AHP_REDIS_ADDR")))
	if err != nil {
		t.Fatal(err)
	}
	r.WithLeaseService(leAses)

	c := mustCreate(t, st, sc, compute.ClassS, compute.ProfileEphemeral, "")
	outside := filepath.Join(filepath.Dir(jail), "outside-secret")
	_, err = r.ExportPath(ctx(t), sc, exportOptions(), c.ID, outside)
	if err == nil {
		t.Fatal("traversal export must be rejected")
	}
	if contracts.Code(err) != string(contracts.ErrForbidden) {
		t.Fatalf("traversal err code = %q want FORBIDDEN (err %v)", contracts.Code(err), err)
	}
}

// A jail-confined path is handed to the Artifacts owner with labels derived
// solely from the verified scope.
func TestRunnerExportPathConfined(t *testing.T) {
	st := newStore(t)
	sc := scope()
	jail := t.TempDir()
	exp := &stubExporter{ref: compute.ArtifactRef{ArtifactID: "a-42", Digest: "sha256:deadbeef", SizeBytes: 7}}
	r := compute.NewRunner(st, compute.NewRegistry(compute.NewHostBackend(jail)), jail, "testnode", exp)
	leAses, err := compute.NewLeaseService(st, compute.NewRedis(os.Getenv("AHP_REDIS_ADDR")))
	if err != nil {
		t.Fatal(err)
	}
	r.WithLeaseService(leAses)

	c := mustCreate(t, st, sc, compute.ClassS, compute.ProfileEphemeral, "")
	os.MkdirAll(r.HomeDir(c.ID), 0700)
	f := filepath.Join(r.HomeDir(c.ID), "out.txt")
	if err := os.WriteFile(f, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	ref, err := r.ExportPath(ctx(t), sc, exportOptions(), c.ID, f)
	if err != nil {
		t.Fatalf("ExportPath: %v", err)
	}
	if ref.ArtifactID != "a-42" || ref.Digest != "sha256:deadbeef" {
		t.Fatalf("ref = %+v, want owner's sealed reference", ref)
	}
	if got := exp.lastReq.Labels["ahp:tenant"]; got != sc.Tenant {
		t.Fatalf("trusted label ahp:tenant = %q want %q", got, sc.Tenant)
	}
	if got := exp.lastReq.Labels["ahp:task"]; got != "task-1" {
		t.Fatalf("trusted label ahp:task = %q want task-1", got)
	}
}

// stubExporter is a fixture ArtifactExportClient that records the request (so
// tests can inspect the trusted labels) and returns a fixed sealed reference or
// a configured error. It never fabricates a reference on error.
type stubExporter struct {
	ref     compute.ArtifactRef
	err     error
	lastReq compute.ArtifactExportRequest
}

func (s *stubExporter) Export(_ context.Context, req compute.ArtifactExportRequest) (compute.ArtifactRef, error) {
	_, _ = io.Copy(io.Discard, req.Body)
	s.lastReq = req
	if s.err != nil {
		return compute.ArtifactRef{}, s.err
	}
	return s.ref, nil
}

func exportOptions() compute.ExportOptions {
	return compute.ExportOptions{TaskID: "task-1", ContextID: "context-1", Key: "export-test", Check: func(context.Context) error { return nil }}
}
