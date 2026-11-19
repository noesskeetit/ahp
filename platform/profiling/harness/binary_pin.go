package harness

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// A run may only measure a binary it cannot change.
//
// Acceptance caught this twice on the same class: a published number bound to
// `bin/ahp 7d77e893...` when the run had actually used `5ccf40b8...`. The lane's
// bin/ is a mutable build target -- it is overwritten by the next build -- so a
// profile whose artifact names a path under it describes a binary that may no
// longer exist. The team's accepted layout is a per-run copy
// (`.../runs/<stamp>/bin/...` or `.../bin-<stamp>/...`) made read-only, and the
// quality guard refuses anything else from a FINAL profile.

// BinaryPathIsPerRun is the pure path-shape half of the rule (usable by the
// guard without touching the filesystem).
func BinaryPathIsPerRun(path string) (bool, string) {
	clean := filepath.ToSlash(strings.TrimSpace(path))
	if clean == "" {
		return false, "no measured binary recorded: the profile does not say which bytes produced these numbers"
	}
	seg := strings.Split(clean, "/")
	for _, s := range seg {
		if s == "runs" {
			return true, "per-run runs/<stamp>/ layout"
		}
		if strings.HasPrefix(s, "bin-") && len(s) > 4 && isDigit(byte(s[4])) {
			return true, "per-run bin-<stamp>/ layout"
		}
	}
	return false, "measured binary lives outside a per-run directory (runs/<stamp>/bin/ or bin-<stamp>/): the lane bin/ is a mutable build target, so a published number can be bound to a binary that no longer exists"
}

// BinaryIsPinned is the full check: per-run layout AND an immutable copy.
func BinaryIsPinned(path string) (bool, string) {
	ok, why := BinaryPathIsPerRun(path)
	if !ok {
		return false, why
	}
	st, err := os.Stat(filepath.Clean(path))
	if err != nil {
		return false, fmt.Sprintf("per-run copy cannot be verified: %v", err)
	}
	if st.Mode().Perm()&0o222 != 0 {
		return false, "per-run copy is still writable (chmod a-w it): a run must not be able to change what it measured"
	}
	return true, "immutable per-run copy (layout " + why + ", chmod a-w)"
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

// UsePinnedBinary records an EXISTING read-only copy as the measured binary and
// skips the build: a measurement window must never compile the product, and the
// artifact must name the exact bytes it ran.
func UsePinnedBinary(repoRoot, path string) (VersionInfo, error) {
	rev, dirty := gitRevision(repoRoot)
	vi := VersionInfo{SourceRevision: rev, SourceDirty: dirty, HarnessRevision: rev}
	abs, err := filepath.Abs(path)
	if err != nil {
		return vi, err
	}
	ok, why := BinaryIsPinned(abs)
	vi.BinaryPinned = ok
	vi.BinaryPinReason = why
	vi.BinaryPath = abs
	vi.BuildCommand = "none (pinned copy measured as-is; " + why + ")"
	raw, err := os.ReadFile(abs)
	if err != nil {
		return vi, fmt.Errorf("harness: pinned binary unreadable: %w", err)
	}
	sum := sha256.Sum256(raw)
	vi.BinarySHA256 = hex.EncodeToString(sum[:])
	st, err := os.Stat(abs)
	if err == nil {
		vi.BinaryBuiltAtUTC = st.ModTime().UTC().Format(time.RFC3339)
	}
	if !ok {
		return vi, fmt.Errorf("harness: refusing to measure an unpinned binary: %s", why)
	}
	return vi, nil
}
