package execbridge

// Where the sandboxed artifact parser comes from, for the legs that seal real
// bytes.
//
// cmd/artifact-parser installs a Linux seccomp filter before it will look at
// untrusted content (sandbox_other.go refuses anywhere else), so a supported text
// type can only reach ingestion_state=ready on Linux. Measured failure this
// explains: a bridged tool's sealed artifact answered
// "FORBIDDEN: artifact is quarantined or awaiting extraction" on a macOS runner --
// a terminal state, not a slow one, and not a gate defect.
//
// Resolution follows the deployment's own order: the operator's binary
// (AHP_ARTIFACT_PARSER) wins, otherwise a Linux runner builds the product's parser
// from cmd/artifact-parser. The empty answer is the honest answer on any other
// platform, and callers must not pretend extraction happened.

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func testModuleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for range 12 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("module root not found")
	return ""
}

func trustedArtifactParser(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("AHP_ARTIFACT_PARSER"); p != "" {
		return p
	}
	if runtime.GOOS != "linux" {
		return ""
	}
	bin := filepath.Join(t.TempDir(), "artifact-parser")
	build := exec.Command("go", "build", "-o", bin, "./cmd/artifact-parser")
	build.Dir = testModuleRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the product artifact parser (the acceptance image must build its own sandbox): %v: %s", err, out)
	}
	return bin
}
