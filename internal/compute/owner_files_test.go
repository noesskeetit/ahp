package compute

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestOwnerDeletionRejectsSymlinkedComputerRoot(t *testing.T) {
	for _, mode := range []string{"subject", "tenant"} {
		t.Run(mode, func(t *testing.T) {
			s, scope := tenantComputeFixture(t)
			ctx := context.Background()
			root := t.TempDir()
			own, err := s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassS, Profile: ProfilePersonal}, "own")
			if err != nil {
				t.Fatal(err)
			}
			foreignScope := scope
			foreignScope.Subject += "-foreign"
			if mode == "tenant" {
				foreignScope.Tenant += "-foreign"
			}
			foreign, err := s.CreateComputer(ctx, foreignScope, NewComputerRequest{Class: ClassS, Profile: ProfilePersonal}, "foreign")
			if err != nil {
				t.Fatal(err)
			}
			foreignRoot := filepath.Join(root, "computers", foreign.ID)
			foreignFile := filepath.Join(foreignRoot, "snapshots", "secret")
			writeOwnerFileFixture(t, foreignFile)
			ownRoot := filepath.Join(root, "computers", own.ID)
			if err = os.Symlink(foreignRoot, ownRoot); err != nil {
				t.Fatal(err)
			}
			if _, err = s.RecordSnapshot(ctx, scope, own.ID, SnapshotKind("directory"), filepath.Join(ownRoot, "snapshots", "secret"), "digest", 26); err != nil {
				t.Fatal(err)
			}
			runner := NewRunner(s, NewRegistry(), root, "test", nil)
			if mode == "subject" {
				if err = runner.FenceSubject(ctx, scope.Tenant, scope.Subject); err != nil {
					t.Fatal(err)
				}
				err = runner.DeleteSubject(ctx, scope.Tenant, scope.Subject)
			} else {
				if err = runner.FenceTenant(ctx, scope.Tenant); err != nil {
					t.Fatal(err)
				}
				err = runner.DeleteTenant(ctx, scope.Tenant)
			}
			assertOwnerFileFixture(t, foreignFile)
			if err == nil {
				t.Fatal("symlinked owner root accepted")
			}
			if _, err = s.GetComputer(ctx, scope, own.ID); err != nil {
				t.Fatal("refused cleanup erased recovery metadata", err)
			}
		})
	}
}

func writeOwnerFileFixture(t *testing.T, name string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte("foreign bytes must survive"), 0600); err != nil {
		t.Fatal(err)
	}
}

func assertOwnerFileFixture(t *testing.T, name string) {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil || string(data) != "foreign bytes must survive" {
		t.Fatalf("foreign file changed: %q %v", data, err)
	}
}

func TestOwnerFilesRejectSymlinkAncestorsBeforeAnyRemoval(t *testing.T) {
	for _, which := range []string{"jail", "computers", "procs", "snapshot-parent", "dangling-snapshot-parent"} {
		t.Run(which, func(t *testing.T) {
			base := t.TempDir()
			ownFile := filepath.Join(base, "computers", "owned", "home", "keep")
			foreign := filepath.Join(base, "foreign", "secret")
			writeOwnerFileFixture(t, ownFile)
			writeOwnerFileFixture(t, foreign)
			snapshot := filepath.Join(base, "computers", "owned", "snapshots", "secret")
			switch which {
			case "jail":
				alias := filepath.Join(t.TempDir(), "alias")
				if err := os.Symlink(base, alias); err != nil {
					t.Fatal(err)
				}
				base = alias
				snapshot = filepath.Join(base, "computers", "owned", "snapshots", "secret")
			case "computers":
				original := filepath.Join(base, "original-computers")
				if err := os.Rename(filepath.Join(base, "computers"), original); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(original, filepath.Join(base, "computers")); err != nil {
					t.Fatal(err)
				}
				ownFile = filepath.Join(original, "owned", "home", "keep")
			case "procs":
				if err := os.Symlink(filepath.Dir(foreign), filepath.Join(base, "procs")); err != nil {
					t.Fatal(err)
				}
			case "snapshot-parent", "dangling-snapshot-parent":
				target := filepath.Dir(foreign)
				if which == "dangling-snapshot-parent" {
					target = filepath.Join(base, "absent")
				}
				if err := os.Symlink(target, filepath.Dir(snapshot)); err != nil {
					t.Fatal(err)
				}
			}
			files, err := prepareOwnerFiles(base, []string{"owned"}, []string{snapshot})
			if files != nil {
				files.Close()
			}
			if err == nil {
				t.Fatal("unsafe inventory accepted")
			}
			assertOwnerFileFixture(t, foreign)
			assertOwnerFileFixture(t, ownFile)
		})
	}
}

// Deterministically change the filesystem after preparation: the mutator must
// not resolve either a parent directory or a snapshot ancestor again by path.
func TestOwnerFilesDoNotFollowLateSymlinkReplacement(t *testing.T) {
	for _, which := range []string{"computer-root", "computers-parent", "snapshot-parent"} {
		t.Run(which, func(t *testing.T) {
			base := t.TempDir()
			ownRoot := filepath.Join(base, "computers", "owned")
			ownFile := filepath.Join(ownRoot, "snapshots", "secret")
			foreignRoot := filepath.Join(base, "foreign-parent", "owned")
			foreign := filepath.Join(foreignRoot, "snapshots", "secret")
			writeOwnerFileFixture(t, ownFile)
			writeOwnerFileFixture(t, foreign)
			files, err := prepareOwnerFiles(base, []string{"owned"}, []string{ownFile})
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			var original, target string
			switch which {
			case "computer-root":
				original, target = ownRoot, foreignRoot
			case "computers-parent":
				original, target = filepath.Dir(ownRoot), filepath.Dir(foreignRoot)
			case "snapshot-parent":
				original, target = filepath.Dir(ownFile), filepath.Dir(foreign)
			}
			if err = os.Rename(original, original+"-moved"); err != nil {
				t.Fatal(err)
			}
			if err = os.Symlink(target, original); err != nil {
				t.Fatal(err)
			}
			if err = files.RemoveAll(); err != nil {
				t.Fatal(err)
			}
			assertOwnerFileFixture(t, foreign)
		})
	}
}
