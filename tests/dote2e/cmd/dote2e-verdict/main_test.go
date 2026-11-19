package main

// The exit-code contract is the point of this tool, so it is tested with fixture
// artifacts written to a temporary directory: no stand, no network, no database.

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func writeArtifact(t *testing.T, name string, doc map[string]any) string {
	t.Helper()
	clean := map[string]any{
		"scenario": "fixture-scenario", "verdict": "PASS",
		"product_revision": "0123456789abcdef0123456789abcdef01234567",
		"dirty":            false, "completed_legs": []string{"j1"},
	}
	for key, value := range doc {
		clean[key] = value
	}
	raw, err := json.Marshal(clean)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}

func TestExitCodesFollowTheVerdictField(t *testing.T) {
	cases := []struct {
		name    string
		doc     map[string]any
		want    int
		wantsIn string
	}{
		{"pass is zero", map[string]any{}, exitOK, "verdict=PASS"},
		{"blocked is four", map[string]any{"verdict": "BLOCKED", "blocked_legs": []string{"j2"},
			"blocked_reasons": map[string]any{"j2": "missing_terminal_frame"}}, exitBlocked, "missing_terminal_frame"},
		{"fail is five", map[string]any{"verdict": "FAIL", "failed_legs": []string{"j3"},
			"failed_reasons": map[string]any{"j3": "generation_failed"}}, exitNotPassed, "generation_failed"},
		{"defect is five", map[string]any{"verdict": "DEFECT"}, exitNotPassed, "verdict=DEFECT"},
		{"dry run is unusable", map[string]any{"dry_run": true, "dry_run_note": "stand bring-up"}, exitUnusable, "dry_run"},
		{"dirty tree is unusable", map[string]any{"dirty": true}, exitUnusable, "dirty"},
		{"missing product revision is unusable", map[string]any{"product_revision": ""}, exitUnusable, "product_revision"},
		{"pass with no legs is unusable", map[string]any{"completed_legs": []string{}}, exitUnusable, "no completed legs"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeArtifact(t, "artifact.json", tc.doc)
			code, line := classify(path, "")
			if code != tc.want {
				t.Fatalf("exit %d, want %d: %s", code, tc.want, line)
			}
			if !strings.Contains(line, tc.wantsIn) {
				t.Fatalf("line %q must name %q", line, tc.wantsIn)
			}
		})
	}
}

func TestUnreadableAndUnparsableArtifactsAreNeverZero(t *testing.T) {
	if code, line := classify(filepath.Join(t.TempDir(), "absent.json"), ""); code != exitUnusable {
		t.Fatalf("a missing artifact must be unusable, got %d: %s", code, line)
	}
	path := filepath.Join(t.TempDir(), "broken.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if code, line := classify(path, ""); code != exitUnusable {
		t.Fatalf("an unparsable artifact must be unusable, got %d: %s", code, line)
	}
	noVerdict := writeArtifact(t, "no-verdict.json", map[string]any{"verdict": ""})
	if code, _ := classify(noVerdict, ""); code != exitUnusable {
		t.Fatalf("an artifact without a verdict must be unusable, got %d", code)
	}
}

func TestExpectedRevisionIsEnforced(t *testing.T) {
	path := writeArtifact(t, "artifact.json", map[string]any{})
	if code, _ := classify(path, "ffffffffffffffffffffffffffffffffffffffff"); code != exitUnusable {
		t.Fatalf("a foreign product_revision must be refused, got %d", code)
	}
	if code, line := classify(path, "0123456789abcdef0123456789abcdef01234567"); code != exitOK {
		t.Fatalf("the expected revision must pass, got %d: %s", code, line)
	}
}

func TestListFileArgumentsAreExpandedVerbatim(t *testing.T) {
	// Artifact paths in this workspace contain spaces; a list file must survive them.
	dir := t.TempDir()
	spaced := filepath.Join(dir, "with space", "dot-e2e-scenario-personal-dot-journey.json")
	if err := os.MkdirAll(filepath.Dir(spaced), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	raw, err := json.Marshal(map[string]any{"scenario": "personal-dot-journey", "verdict": "BLOCKED",
		"product_revision": "0123456789abcdef0123456789abcdef01234567", "dirty": false,
		"blocked_legs": []string{"j4"}, "blocked_reasons": map[string]any{"j4": "frame_free_without_durable_evidence"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(spaced, raw, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	list := filepath.Join(dir, "artifacts.txt")
	if err := os.WriteFile(list, []byte(spaced+"\n"), 0o644); err != nil {
		t.Fatalf("write list: %v", err)
	}
	expanded := expandPathArguments([]string{"@" + list})
	if len(expanded) != 1 || expanded[0] != spaced {
		t.Fatalf("list file must expand to the exact path, got %v", expanded)
	}
	if code, line := classify(expanded[0], ""); code != exitBlocked {
		t.Fatalf("a blocked artifact named through a list file must exit %d, got %d: %s", exitBlocked, code, line)
	}
}

// The contract of this command IS its exit code, so the empty-input cases are
// proven by running the built binary and reading its real rc - the same numbers a
// reviewer gets by hand. A helper that returns a code proves less than the tool.
func buildReaderBinary(t *testing.T) string {
	t.Helper()
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("the reader contract is proven by running it, which needs the go toolchain: %v", err)
	}
	bin := filepath.Join(t.TempDir(), "dote2e-verdict")
	build := exec.Command(goTool, "build", "-o", bin, ".")
	if out, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build the reader: %v\n%s", buildErr, out)
	}
	return bin
}

func runReader(t *testing.T, bin string, args ...string) (int, string) {
	t.Helper()
	out, err := exec.Command(bin, args...).CombinedOutput()
	if err == nil {
		return 0, string(out)
	}
	status, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("run the reader: %v", err)
	}
	return status.ExitCode(), string(out)
}

func writeListFile(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write list: %v", err)
	}
	return path
}

// TestNamingZeroArtifactsIsNeverGreen is the RED leg for DEF-VERDICT-EMPTY-LIST.
// A list file is read AFTER the emptiness decision in the broken shape: the tool
// saw one argument ("@list.txt"), considered the input non-empty, iterated over
// the expansion of that file (zero paths), and exited with the best code it had
// never used - 0. A green verdict over zero artifacts is the same lie as a green
// verdict over zero legs, which this tool already refuses (main.go: PASS with no
// completed legs is unusable).
func TestNamingZeroArtifactsIsNeverGreen(t *testing.T) {
	bin := buildReaderBinary(t)
	cases := []struct {
		name string
		args []string
	}{
		{"no arguments at all", nil},
		{"list file that is empty", []string{"@" + writeListFile(t, "empty-list.txt", "")}},
		{"list file of blanks only", []string{"@" + writeListFile(t, "blank-list.txt", "   \n\n \t \n  \n")}},
		{"list file with only a comment-free newline", []string{"@" + writeListFile(t, "newline-list.txt", "\n")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, line := runReader(t, bin, tc.args...)
			if code != exitUnusable {
				t.Fatalf("naming zero artifacts must exit %d, got %d: %s", exitUnusable, code, line)
			}
			if !strings.Contains(line, "no artifacts named") {
				t.Fatalf("the refusal must name its reason, got: %s", line)
			}
		})
	}
}

// TestBlankLinesDoNotCondemnAListThatNamesArtifacts keeps the fix honest: blanks
// are dropped, not punished. A list with real paths plus stray blank lines must
// still report the verdict of the artifacts it actually names.
func TestBlankLinesDoNotCondemnAListThatNamesArtifacts(t *testing.T) {
	bin := buildReaderBinary(t)
	pass := writeArtifact(t, "pass.json", map[string]any{})
	blocked := writeArtifact(t, "blocked.json", map[string]any{"verdict": "BLOCKED",
		"blocked_legs": []string{"j4"}, "blocked_reasons": map[string]any{"j4": "missing_terminal_frame"}})
	list := writeListFile(t, "mixed.txt", "\n   \n"+pass+"\n\t\n"+blocked+"\n   \n")
	code, line := runReader(t, bin, "@"+list)
	if code != exitBlocked {
		t.Fatalf("a blocked artifact behind blank lines must still exit %d, got %d: %s", exitBlocked, code, line)
	}
	if !strings.Contains(line, "missing_terminal_frame") {
		t.Fatalf("the report must carry the artifact's own reason: %s", line)
	}
}

// TestGreenExitRequiresANamedArtifact states the other half of the rule: 0 is
// reachable, but only through an artifact that was actually named and read.
func TestGreenExitRequiresANamedArtifact(t *testing.T) {
	bin := buildReaderBinary(t)
	pass := writeArtifact(t, "pass.json", map[string]any{})
	for _, args := range [][]string{{pass}, {"@" + writeListFile(t, "one.txt", pass+"\n")}} {
		code, line := runReader(t, bin, args...)
		if code != exitOK {
			t.Fatalf("a named passing artifact must exit %d, got %d: %s", exitOK, code, line)
		}
		if !strings.Contains(line, "verdict=PASS") {
			t.Fatalf("the line must carry the verdict it read: %s", line)
		}
	}
	if code, line := runReader(t, bin, filepath.Join(t.TempDir(), "does-not-exist.json")); code != exitUnusable {
		t.Fatalf("a missing artifact must stay unusable, got %d: %s", code, line)
	}
}

// TestExpansionOfAnEmptyListFileIsTheEmptinessThatMatters pins the invariant the
// decision depends on: expansion, not the raw argument list, is what says how
// many artifacts are named.
func TestExpansionOfAnEmptyListFileIsTheEmptinessThatMatters(t *testing.T) {
	empty := writeListFile(t, "empty-list.txt", "")
	if expanded := expandPathArguments([]string{"@" + empty}); len(expanded) != 0 {
		t.Fatalf("an empty list file must expand to nothing, got %v", expanded)
	}
	blank := writeListFile(t, "blank-list.txt", "  \n\t\n")
	if expanded := expandPathArguments([]string{"@" + blank}); len(expanded) != 0 {
		t.Fatalf("a blank list file must expand to nothing, got %v", expanded)
	}
	// An unreadable list file stays a NAMED argument so it is reported, not hushed.
	missing := filepath.Join(t.TempDir(), "no-such-list.txt")
	if expanded := expandPathArguments([]string{"@" + missing}); len(expanded) != 1 {
		t.Fatalf("an unreadable list file must still be reported, got %v", expanded)
	}
}
