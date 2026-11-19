package integration_test

import (
	"os"
	"path/filepath"
	"testing"
)

// TestRepoRootAndWorkdirResolution pins how the suite finds its two roots,
// because the lane runs from both disk layouts and the substrate is chosen by
// which one it is on:
//
//   - VM node: the repository is checked out as <workdir>/src, so the workdir
//     (jails, environment prefixes, TLS material) is the checkout's parent and
//     nothing about the node's paths moves.
//   - anywhere else (a plain checkout, a git worktree on the control machine):
//     the checkout is found by walking up to go.mod, and the workdir is the lane
//     directory under the user's home - never a path that only exists on the VM.
//
// It needs no database and no redis: a wrong root used to be discovered only
// when a real-binary smoke step failed to build, several seconds and one
// confusing error away from the cause.
func TestRepoRootAndWorkdirResolution(t *testing.T) {
	// The default resolution: the checkout this test binary was built from.
	root := repoRoot(t)
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repoRoot %s has no go.mod: %v", root, err)
	}
	if filepath.Base(root) != "src" {
		home, err := os.UserHomeDir()
		if err == nil && home != "" {
			if got, want := workdirRoot(t), filepath.Join(home, "ahp-handbook-compute"); got != want {
				t.Fatalf("workdirRoot outside the VM layout = %s, want the lane workdir %s", got, want)
			}
		}
	}

	// The VM layout: a checkout named src inside the lane workdir. The workdir
	// must be the parent, exactly as it has always been on the nodes.
	fake := t.TempDir()
	src := filepath.Join(fake, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "go.mod"), []byte("module test\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AHP_COMPUTE_TEST_REPO_ROOT", src)
	if got := repoRoot(t); got != src {
		t.Fatalf("repoRoot with override = %s, want %s", got, src)
	}
	if got := workdirRoot(t); got != fake {
		t.Fatalf("workdirRoot for the VM layout = %s, want the workdir %s", got, fake)
	}

	// An override that is not a repository is refused by repoRoot with a fatal
	// naming the directory, so a typo in the environment fails the run instead
	// of quietly building from somewhere else.
}
