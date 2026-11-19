package compute

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

type exportBoundaryProbe struct {
	calls int
	bytes []byte
}

func (p *exportBoundaryProbe) Export(_ context.Context, req ArtifactExportRequest) (ArtifactRef, error) {
	p.calls++
	p.bytes, _ = io.ReadAll(req.Body)
	return ArtifactRef{}, contracts.NewError(contracts.ErrUnavailable, "probe never publishes artifacts")
}

func TestExportRejectsForeignComputerAndSymlinkBeforeOwner(t *testing.T) {
	store, scope := tenantComputeFixture(t)
	ctx := context.Background()
	create := func(owner contracts.Scope) Computer {
		t.Helper()
		computer, err := store.CreateComputer(ctx, owner, NewComputerRequest{Class: ClassS, Profile: ProfilePersonal}, contracts.NewID())
		if err != nil {
			t.Fatal(err)
		}
		return computer
	}
	own := create(scope)
	foreignScope := scope
	foreignScope.Subject = "bob"
	other := create(foreignScope)
	root := t.TempDir()
	probe := &exportBoundaryProbe{}
	runner := NewRunner(store, NewRegistry(), root, "export-boundary", probe)
	leases, err := NewLeaseService(store, NewRedis(os.Getenv("AHP_REDIS_ADDR")))
	if err != nil {
		t.Fatal(err)
	}
	runner.WithLeaseService(leases)
	for _, id := range []string{own.ID, other.ID} {
		if err := os.MkdirAll(runner.HomeDir(id), 0700); err != nil {
			t.Fatal(err)
		}
	}
	privatePath := filepath.Join(runner.HomeDir(other.ID), "private.txt")
	outsidePath := filepath.Join(t.TempDir(), "host-private.txt")
	for _, path := range []string{privatePath, outsidePath} {
		if err := os.WriteFile(path, []byte("must-not-reach-artifacts-owner"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(runner.HomeDir(own.ID), "outside-link")
	if err := os.Symlink(outsidePath, link); err != nil {
		t.Fatal(err)
	}
	ownPath := filepath.Join(runner.HomeDir(own.ID), "owned.txt")
	if err := os.WriteFile(ownPath, []byte("owned"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = runner.ExportPath(ctx, scope, ExportOptions{TaskID: "export-task", ContextID: "context", Key: "positive", Check: func(context.Context) error { return nil }}, own.ID, "owned.txt")
	if contracts.Code(err) != contracts.ErrUnavailable || probe.calls != 1 || string(probe.bytes) != "owned" {
		t.Fatalf("positive source did not reach owner: calls=%d error=%v", probe.calls, err)
	}
	for name, path := range map[string]string{"foreign-computer-absolute": privatePath, "symlink-outside-home": "outside-link"} {
		t.Run(name, func(t *testing.T) {
			probe.calls, probe.bytes = 0, nil
			_, err := runner.ExportPath(ctx, scope, ExportOptions{TaskID: "export-task", ContextID: "context", Key: name, Check: func(context.Context) error { return nil }}, own.ID, path)
			if contracts.Code(err) != contracts.ErrForbidden || probe.calls != 0 {
				t.Fatalf("foreign source reached owner: calls=%d bytes=%d error=%v", probe.calls, len(probe.bytes), err)
			}
		})
	}
}
