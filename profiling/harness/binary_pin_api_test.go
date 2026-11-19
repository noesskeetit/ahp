package harness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// BinaryIsPinned recognises the two per-run layouts the team uses (runs/<stamp>/
// and bin-<stamp>/) and refuses anything writable.
func TestBinaryIsPinnedRecognisesImmutableLayouts(t *testing.T) {
	dir := t.TempDir()
	pinned := filepath.Join(dir, "runs", "20261109T221500Z-abc", "bin", "ahp-profiling")
	if err := os.MkdirAll(filepath.Dir(pinned), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pinned, []byte("x"), 0o555); err != nil {
		t.Fatal(err)
	}
	ok, why := BinaryIsPinned(pinned)
	if !ok {
		t.Fatalf("a chmod a-w copy under runs/<stamp>/ must count as pinned: %s", why)
	}
	writable := filepath.Join(dir, "runs", "20261109T221500Z-abc", "bin", "writable")
	if err := os.WriteFile(writable, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if ok, why = BinaryIsPinned(writable); ok || !strings.Contains(why, "writab") {
		t.Fatalf("a writable copy is not immutable: ok=%v why=%q", ok, why)
	}
	if ok, why = BinaryIsPinned("/work/lane/bin/ahp-profiling"); ok || !strings.Contains(why, "mutable") {
		t.Fatalf("lane bin/ must read as mutable: ok=%v why=%q", ok, why)
	}
	if ok, why = BinaryIsPinned(""); ok || why == "" {
		t.Fatalf("no binary path must be refused with a reason: ok=%v why=%q", ok, why)
	}
}
