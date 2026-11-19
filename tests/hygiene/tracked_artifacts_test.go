package hygiene

// Repository-hygiene legs for DEF-LANE-TRACKED-BINARY.
//
// An ELF executable named `ahp` was tracked in this repository since fd064a26.
// The consequence is not disk space: `go build ./cmd/ahp/` writes ./ahp into the
// repository root, so a build performed to test something overwrote a TRACKED
// file, the work tree reported dirty, and every primary log taken from that tree
// carried a dirty-base stamp. That is the same class of defect that made two
// sets of measurements untrustworthy on 2026-11-10 (a build-info stamp of
// 8e4d8766 / vcs.modified=true on binaries that were presented as a clean canon).
//
// What these legs hold shut:
//
//  1. No tracked file may be a compiled executable image (ELF / Mach-O / PE).
//  2. The set of tracked binary blobs is a frozen list of web assets, so a new
//     one has to be a deliberate, reviewed act rather than `git add .`.
//  3. Every root-level build output a `go build ./cmd/<name>` could produce is
//     ignored, so building cannot dirty the tree. `bin/` remains the sanctioned
//     output directory, and scripts/build-product.sh is the step that produces
//     and ATTESTS a product binary (vcs.revision == HEAD, vcs.modified == false).
//
// No compilation, no network, no container: these legs read git metadata only.
// They skip -- visibly, with a registered tag -- where git metadata is not
// mounted, which is the case for a read-only source mount inside a test image.

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const skipTagGitWorktree = "git_worktree"

// frozenBinaryAssets is the complete set of tracked binary blobs allowed in the
// tree: product documentation and web assets, none of them an executable. A new
// entry means someone is checking in a byte blob; that is a decision, not a habit.
var frozenBinaryAssets = map[string]string{
	"web/aifactory/assets/ai-agents.png":                        "image",
	"web/aifactory/evidence/desktop-stacks.png":                 "screenshot",
	"web/aifactory/evidence/memory-api.png":                     "screenshot",
	"web/aifactory/evidence/mobile-wizard.png":                  "screenshot",
	"web/aifactory/evidence/sandbox.png":                        "screenshot",
	"web/aifactory/evidence/snapshot-detail.png":                "screenshot",
	"web/aifactory/assets/fonts/SBSansInterface-Regular.woff2":  "font",
	"web/aifactory/assets/fonts/SBSansInterface-Semibold.woff2": "font",
	"web/aifactory/assets/fonts/SBSansTextMono-Regular.woff2":   "font",
}

// executableMagics identify a compiled image, as opposed to a script (which may
// legitimately be mode 100755) or an asset.
var executableMagics = []struct {
	name  string
	match func(b []byte) bool
}{
	{"ELF", func(b []byte) bool { return bytes.HasPrefix(b, []byte{0x7f, 'E', 'L', 'F'}) }},
	{"Mach-O", func(b []byte) bool {
		for _, m := range [][]byte{
			{0xfe, 0xed, 0xfa, 0xce}, {0xce, 0xfa, 0xed, 0xfe},
			{0xcf, 0xfa, 0xed, 0xfe}, {0xfe, 0xed, 0xfa, 0xcf},
			{0xca, 0xfe, 0xba, 0xbe},
		} {
			if bytes.HasPrefix(b, m) {
				return true
			}
		}
		return false
	}},
	{"PE", func(b []byte) bool { return bytes.HasPrefix(b, []byte{'M', 'Z'}) }},
}

func repoRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skipf("SKIP_ENV[tag=%s] reason=git_metadata_unavailable missing=GIT_TOPLEVEL -- a skip here means not-covered, not passed", skipTagGitWorktree)
	}
	return strings.TrimSpace(string(out))
}

// gitAt runs git at the repository root on purpose. `go test` starts a test binary
// in the package directory, so a bare `git ls-files` here would describe the
// package, not the product tree, and would report an empty index as if that were
// a clean result.
func gitAt(root string, args ...string) *exec.Cmd {
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	return cmd
}

func trackedFiles(t *testing.T, root string) []string {
	t.Helper()
	out, err := gitAt(root, "ls-files", "-z").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	var files []string
	for _, f := range bytes.Split(out, []byte{0}) {
		if len(f) > 0 {
			files = append(files, string(f))
		}
	}
	if len(files) == 0 {
		t.Fatal("git ls-files returned nothing: this leg cannot prove anything about an empty index")
	}
	return files
}

func headBytes(path string, n int) ([]byte, error) {
	handle, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer handle.Close()
	buf := make([]byte, n)
	read, _ := handle.Read(buf)
	return buf[:read], nil
}

func executableImageKind(head []byte) string {
	for _, magic := range executableMagics {
		if magic.match(head) {
			return magic.name
		}
	}
	return ""
}

// TestNoTrackedExecutableImage is the leg that would have failed on 2026-11-10:
// ./ahp was a 28 MB statically linked ELF in the index.
func TestNoTrackedExecutableImage(t *testing.T) {
	root := repoRoot(t)
	var offenders []string
	for _, name := range trackedFiles(t, root) {
		head, err := headBytes(filepath.Join(root, name), 8)
		if err != nil {
			// Tracked but absent is the lane's own problem, and not this leg's claim.
			continue
		}
		if kind := executableImageKind(head); kind != "" {
			offenders = append(offenders, kind+" "+name)
		}
	}
	if len(offenders) > 0 {
		t.Fatalf("tracked executable images found (%d): %v -- a tracked build output makes a plain "+
			"`go build ./cmd/...` dirty the tree, and a dirty tree stamps every log taken from it; "+
			"untrack it (git rm --cached) and build via scripts/build-product.sh", len(offenders), offenders)
	}
}

// TestTrackedBinaryBlobsAreTheFrozenAssetList keeps "no binaries" enforceable
// without pretending the web assets do not exist.
func TestTrackedBinaryBlobsAreTheFrozenAssetList(t *testing.T) {
	root := repoRoot(t)
	var found, unexpected []string
	for _, name := range trackedFiles(t, root) {
		head, err := headBytes(filepath.Join(root, name), 8000)
		if err != nil {
			continue
		}
		if !bytes.Contains(head, []byte{0}) {
			continue
		}
		found = append(found, name)
		if _, allowed := frozenBinaryAssets[name]; !allowed {
			unexpected = append(unexpected, name)
		}
	}
	if len(unexpected) > 0 {
		t.Fatalf("tracked binary blobs outside the frozen asset list: %v -- add to the list only as a "+
			"reviewed decision; an executable image never belongs here (see TestNoTrackedExecutableImage)", unexpected)
	}
	if len(found) != len(frozenBinaryAssets) {
		t.Fatalf("tracked binary assets changed shape: found %d, frozen list has %d -- update the list deliberately, and note that a deleted asset is also a change", len(found), len(frozenBinaryAssets))
	}
}

// TestRootLevelBuildOutputsAreIgnored proves the specific failure mode: building a
// command in place must not show up in `git status`.
func TestRootLevelBuildOutputsAreIgnored(t *testing.T) {
	root := repoRoot(t)
	entries, err := os.ReadDir(filepath.Join(root, "cmd"))
	if err != nil {
		t.Fatalf("read cmd/: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("cmd/ is empty: this leg would prove nothing")
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if out, err := gitAt(root, "check-ignore", "-q", name).Output(); err != nil {
			t.Fatalf("./%s (the output of `go build ./cmd/%s` with no -o) is not ignored: %v (%s) -- add it to .gitignore, do not track it", name, name, err, out)
		}
	}

	// bin/ is the sanctioned output directory; if it stopped being ignored,
	// scripts/build-product.sh would start dirtying the tree instead.
	if err := gitAt(root, "check-ignore", "-q", "bin/ahp").Run(); err != nil {
		t.Fatal("bin/ is not ignored: scripts/build-product.sh writes there, and its output must not dirty the tree")
	}

	// End-to-end form of the original defect: a file appearing at the root under a
	// build-output name must leave `git status` clean.
	probe := filepath.Join(root, "ahp")
	if _, err := os.Stat(probe); err == nil {
		t.Fatalf("%s exists on disk while untracked: remove the stale build output, otherwise a lane cannot tell it from a fresh build", probe)
	}
	if err := os.WriteFile(probe, []byte("probe: not a real build output"), 0o600); err != nil {
		t.Fatalf("write probe: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(probe) })
	status, err := gitAt(root, "status", "--porcelain", "--", "ahp").Output()
	if err != nil {
		t.Fatalf("git status: %v", err)
	}
	if strings.TrimSpace(string(status)) != "" {
		t.Fatalf("after a build wrote ./ahp the tree reports %q: it must report nothing", strings.TrimSpace(string(status)))
	}
}

// dockerScratchPatterns are local, non-source directories that a repository-root
// build context must never swallow. `/.git/` belongs here for a reason worth
// stating: a context that contains .git lets `go build -buildvcs` stamp whatever
// the checkout on the build host happens to be, which is how a build ends up
// claiming a revision nobody asked for.
var dockerScratchPatterns = []string{"/bin/", "/build-out/", "/.git/", "/coverage/"}

func ignorePatterns(t *testing.T, root, name string) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	patterns := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		patterns[line] = true
	}
	if len(patterns) == 0 {
		t.Fatalf("%s has no patterns: this leg would prove nothing", name)
	}
	return patterns
}

// TestDockerBuildContextCarriesNoCompiledOutput closes the other half of
// DEF-LANE-TRACKED-BINARY. Untracking ./ahp stops a build from dirtying the tree;
// this stops a build from SILENTLY SHIPPING one. infra/Dockerfile.ahp compiles
// with `context: .` and `COPY . .` (compose.yaml:99-101), so while the ELF was in
// the index every dev image also contained a compiled binary whose revision nobody
// could determine, and the last `go build` run by any lane decided its contents.
//
// The leg is mechanical on purpose: it compares the two ignore files against the
// one list that actually matters -- the names under cmd/ -- so they cannot drift
// apart from each other or from the set of buildable commands.
func TestDockerBuildContextCarriesNoCompiledOutput(t *testing.T) {
	root := repoRoot(t)
	gitignore := ignorePatterns(t, root, ".gitignore")
	dockerignore := ignorePatterns(t, root, ".dockerignore")

	entries, err := os.ReadDir(filepath.Join(root, "cmd"))
	if err != nil {
		t.Fatalf("read cmd/: %v", err)
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	if len(names) == 0 {
		t.Fatal("cmd/ is empty: this leg would prove nothing")
	}
	for _, name := range names {
		if !gitignore["/"+name] {
			t.Fatalf(".gitignore does not ignore ./%s (the output of `go build ./cmd/%s`): add /%s", name, name, name)
		}
		if !dockerignore["/"+name] {
			t.Fatalf(".dockerignore does not exclude /%s, so a repository-root build context can ship a compiled "+
				"binary of unknown provenance: add /%s", name, name)
		}
	}

	for _, pattern := range dockerScratchPatterns {
		if !dockerignore[pattern] {
			t.Fatalf(".dockerignore does not exclude %s: local build output or VCS state must not reach a build context", pattern)
		}
	}

	// The one Dockerfile documented as built from the repository root must compile
	// from source rather than copy a build output, and it must expose a revision
	// pin instead of implying one. `COPY . .` is allowed precisely because the
	// patterns above keep compiled outputs out of what it copies.
	dockerfile := filepath.Join(root, "infra", "Dockerfile.ahp")
	raw, err := os.ReadFile(dockerfile)
	if err != nil {
		t.Fatalf("read infra/Dockerfile.ahp: %v", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "COPY") || strings.HasPrefix(trimmed, "COPY --from=") {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) < 2 {
			continue
		}
		source := strings.TrimPrefix(fields[1], "./")
		if dockerignore["/"+source] {
			t.Fatalf("%s: %q copies %s, which .dockerignore exists to keep out of the context: compile from source "+
				"or build the image from an explicitly attested binary (scripts/build-candidate.sh)", dockerfile, trimmed, source)
		}
	}
	if !strings.Contains(string(raw), "ARG REVISION") {
		t.Fatalf("%s: no ARG REVISION -- an image that cannot carry Go build-info must at least expose an explicit "+
			"revision hook rather than let callers believe the image proves its own origin", dockerfile)
	}
}
