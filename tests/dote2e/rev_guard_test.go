package dote2e_test

// Revision-guard semantics (director mandate 2026-11-09): a run whose stand or
// worktree does not carry the expected integration revision must be refused -
// BLOCKED with a reason - never certified under someone else's rev. Pure test,
// no Docker, no stand.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRevGuardRefusesForeignRevisions(t *testing.T) {
	full := "3557aa3eeaa0fd220e1a72cae42d7225b5c07ec6"
	other := "b61aec698ede0d8d938b44d9efec096d482dc82b"
	cases := []struct {
		name         string
		expected     string
		build        string
		worktree     string
		deltaHarness bool
		wantBlocked  bool
	}{
		{"unset guard passes", "", other, other, false, false},
		{"full match passes", full, full, full, false, false},
		{"short prefix match passes", "3557aa3", full, full, false, false},
		{"build rev foreign blocks", full, other, full, false, true},
		{"worktree rev foreign blocks", full, full, other, false, true},
		{"worktree with verified harness-only delta passes", full, full, other, true, false},
		{"unknown worktree falls back to build rev", full, full, "", false, false},
		{"garbage short expected blocks", "abc", full, full, false, true},
	}
	for _, tc := range cases {
		reason := revGuardReason(tc.expected, tc.build, tc.worktree, tc.deltaHarness)
		if tc.wantBlocked && reason == "" {
			t.Errorf("%s: expected a blocking reason, got none", tc.name)
		}
		if !tc.wantBlocked && reason != "" {
			t.Errorf("%s: expected pass, got reason %q", tc.name, reason)
		}
		if tc.wantBlocked {
			for _, frag := range []string{tc.expected, tc.build} {
				if frag != "" && len(frag) >= 7 && !contains(reason, frag) && !contains(reason, "worktree") {
					t.Errorf("%s: reason must name the revisions: %q", tc.name, reason)
				}
			}
		}
	}
}

// TestVCSProvenanceRequiredBySectionZero enforces docs/acceptance-run.md §0:
// a run is accepted only when the binaries themselves report vcs.revision ==
// expected and vcs.modified == false. A manifest without the vcs block (or a
// modified/dirty stamp) must be refused - an ldflags stamp is not provenance.
func TestVCSProvenanceRequiredBySectionZero(t *testing.T) {
	full := "df009e50258e8acd0e109a3533b172759b0918a9"
	other := "aec3fc9d31a25b2333638e295ed49e4555f608b8"
	clean := binaryVCS{Revision: full, Modified: "false"}
	cases := []struct {
		name        string
		expected    string
		vcs         map[string]binaryVCS
		wantBlocked bool
	}{
		{"unset guard passes", "", nil, false},
		{"all binaries clean and matching", full,
			map[string]binaryVCS{"ahp": clean, "as-runner": clean}, false},
		{"missing vcs block blocks", full, nil, true},
		{"modified binary blocks", full,
			map[string]binaryVCS{"ahp": clean, "as-runner": {Revision: full, Modified: "true"}}, true},
		{"foreign revision blocks", full,
			map[string]binaryVCS{"ahp": clean, "as-runner": {Revision: other, Modified: "false"}}, true},
	}
	for _, tc := range cases {
		reason := vcsProvenanceReason(tc.expected, tc.vcs)
		if tc.wantBlocked && reason == "" {
			t.Errorf("%s: expected a blocking reason, got none", tc.name)
		}
		if !tc.wantBlocked && reason != "" {
			t.Errorf("%s: expected pass, got reason %q", tc.name, reason)
		}
	}
	if label := worktreeDeltaLabel(other, full, true); label != "harness-only-delta(tests/dote2e)" {
		t.Errorf("harness delta mislabeled: %q", label)
	}
	if label := worktreeDeltaLabel(other, full, false); label != "foreign" {
		t.Errorf("foreign worktree mislabeled: %q", label)
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// TestAcceptanceManifestBinding: the manifest binding must refuse a manifest
// that names a different revision or a tree that is not that revision's real
// git tree, and pass a manifest that names both correctly.
func TestAcceptanceManifestBinding(t *testing.T) {
	rev := "f14585d2df3cd354f443b887c9c3e26d8c9e9371"
	other := "684bae9cf0b99c69209c23cc67cb75828ab0cc86"
	if reason := acceptanceManifestReason("", rev); reason != "" {
		t.Errorf("unset path must pass, got %q", reason)
	}
	if reason := acceptanceManifestReason("/nonexistent/manifest.json", rev); reason == "" {
		t.Error("unreadable manifest must block")
	}
	wrongRev := t.TempDir() + "/m.json"
	if err := writeFile(wrongRev, `{"source":{"revision":"`+other+`","tree":"x"}}`); err != nil {
		t.Fatal(err)
	}
	if reason := acceptanceManifestReason(wrongRev, rev); reason == "" {
		t.Error("manifest naming another revision must block")
	}
	wrongTree := t.TempDir() + "/m.json"
	if err := writeFile(wrongTree, `{"source":{"revision":"`+rev+`","tree":"deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"}}`); err != nil {
		t.Fatal(err)
	}
	if reason := acceptanceManifestReason(wrongTree, rev); reason == "" {
		t.Error("manifest with a forged tree must block")
	}
	good := t.TempDir() + "/m.json"
	out, err := exec.Command("git", "-C", repoRoot(t), "rev-parse", rev+"^{tree}").Output()
	if err != nil {
		// Token grammar owned by scripts/skip-verdict.mjs; tag source_revision names
		// "the revision under test is not in this checkout", which is neither a store
		// nor a parser dependency, so it is not squeezed into a registered tag.
		t.Skipf("SKIP_ENV[tag=source_revision] reason=rev_not_in_checkout missing=GIT_OBJECT -- a skip here means not-covered, not passed: candidate commit unavailable: %v", err)
	}
	if err := writeFile(good, `{"source":{"revision":"`+rev+`","tree":"`+trimTail(out)+`"}}`); err != nil {
		t.Fatal(err)
	}
	if reason := acceptanceManifestReason(good, rev); reason != "" {
		t.Errorf("correct manifest must pass, got %q", reason)
	}
}

func writeFile(path, content string) error { return os.WriteFile(path, []byte(content), 0o600) }

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func trimTail(b []byte) string { return strings.TrimSpace(string(b)) }
