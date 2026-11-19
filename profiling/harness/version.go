package harness

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// VersionInfo binds a measurement to an exact source revision and binary.
type VersionInfo struct {
	SourceRevision string `json:"source_revision"`
	SourceDirty    bool   `json:"source_dirty"`
	BuildCommand   string `json:"build_command"`
	BinaryPath     string `json:"binary_path"`
	BinarySHA256   string `json:"binary_sha256"`
	// BinaryPinned says the measured binary is an immutable per-run copy. A
	// profile built through the lane's mutable bin/ describes bytes that the
	// next build overwrites, which is how published numbers got bound to the
	// wrong binary twice.
	BinaryPinned     bool   `json:"binary_pinned"`
	BinaryPinReason  string `json:"binary_pin_reason,omitempty"`
	BinaryBuiltAtUTC string `json:"binary_built_at_utc"`
	HarnessRevision  string `json:"harness_revision"`
	// ProductRevision is the PRODUCT revision the measured binary was built
	// from, declared explicitly. Go's automatic vcs.revision stamp proves the
	// repository owning the working tree, which is not necessarily the
	// product tree (internal/buildinfo pins this hazard), so it is never a
	// substitute for an explicit declaration.
	ProductRevision string `json:"product_revision"`
	// ProductDeltaOutsideProfiling counts changed files between
	// ProductRevision and the harness revision outside profiling/: 0 proves
	// the measured product code IS ProductRevision. -1 = not verifiable.
	// MeasuredBinaryBuild is what the measured binary itself claims (vcs stamp,
	// tags, toolchain), read from the bytes rather than declared. Null only when
	// nothing was measured; a present block with a null revision says the stamp
	// is absent and why.
	MeasuredBinaryBuild          *BuildInfo `json:"measured_binary_build"`
	ProductDeltaOutsideProfiling int        `json:"product_delta_outside_profiling"`
	// ImageDigests pins the exact container images (name -> sha256 digest)
	// the measured stack ran on; a final profile without them is anonymous.
	ImageDigests map[string]string `json:"image_digests,omitempty"`
	// AcceptanceManifest links the profile to the director's attestation of the
	// accepted candidate (source revision + tree + subject binary + image +
	// quota). Acceptance asked for this explicitly: every profile number must
	// say which attested source/binary/image it belongs to, otherwise a profile
	// of one revision can be read as a profile of another.
	AcceptanceManifest *ManifestRef `json:"acceptance_manifest,omitempty"`
}

// ManifestRef is the digest of the acceptance manifest this profile claims to
// belong to, plus the fields a reader needs to check that claim without
// opening the manifest.
type ManifestRef struct {
	Path                string   `json:"path"`
	SHA256              string   `json:"sha256"`
	Schema              string   `json:"schema,omitempty"`
	GeneratedUTC        string   `json:"generated_utc,omitempty"`
	SourceRevision      string   `json:"source_revision,omitempty"`
	SourceTree          string   `json:"source_tree,omitempty"`
	SubjectBinarySHA256 string   `json:"measured_subject_binary_sha256,omitempty"`
	LiveImageID         string   `json:"live_image_id,omitempty"`
	ProductDiffPaths    []string `json:"product_diff_since_measured_revision,omitempty"`
	Quota               string   `json:"quota_attestation,omitempty"`
	// SubjectBinaryEqualsRun is null until a comparison is possible: the
	// manifest attests a binary copied out of the live container, while a
	// profile measures a binary built from source. Same bytes is the exception,
	// not the rule, so agreement is reported, never assumed.
	SubjectBinaryEqualsRun *bool `json:"subject_binary_equals_run_binary"`
}

// LoadManifestRef digests and parses an acceptance manifest. An unreadable or
// unparsable manifest is a hard error: a profile that points at an attestation
// it cannot read is worse than one that admits it has none.
func LoadManifestRef(path string) (*ManifestRef, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("manifest " + path + " unreadable: " + err.Error())
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, errors.New("manifest " + path + " is not JSON: " + err.Error())
	}
	sum := sha256.Sum256(raw)
	ref := &ManifestRef{Path: path, SHA256: hex.EncodeToString(sum[:])}
	top := func(k string) string {
		if s, ok := doc[k].(string); ok {
			return s
		}
		return ""
	}
	sub := func(section, k string) string {
		if m, ok := doc[section].(map[string]any); ok {
			if s, ok := m[k].(string); ok {
				return s
			}
		}
		return ""
	}
	ref.Schema = top("schema")
	ref.GeneratedUTC = top("generated_utc")
	ref.SourceRevision = sub("source", "revision")
	ref.SourceTree = sub("source", "tree")
	ref.SubjectBinarySHA256 = sub("binary", "measured_subject_binary_sha256")
	ref.LiveImageID = sub("image", "live_image_id")
	if m, ok := doc["quota_attestation"].(map[string]any); ok {
		if s, ok := m["cgroup_read_from_inside"].(string); ok {
			ref.Quota = s
		}
	}
	if m, ok := doc["binary"].(map[string]any); ok {
		if list, ok := m["product_diff_since_measured_revision"].([]any); ok {
			for _, it := range list {
				if s, ok := it.(string); ok {
					ref.ProductDiffPaths = append(ref.ProductDiffPaths, s)
				}
			}
		}
	}
	if ref.SourceRevision == "" {
		return nil, errors.New("manifest " + path + " carries no source.revision: nothing to bind a profile to")
	}
	return ref, nil
}

func gitRevision(dir string) (string, bool) {
	out, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown", false
	}
	rev := strings.TrimSpace(string(out))
	dirtyOut, err := exec.Command("git", "-C", dir, "status", "--porcelain").Output()
	if err != nil {
		return rev, false
	}
	return rev, strings.TrimSpace(string(dirtyOut)) != ""
}

// BuildPlatform compiles the ahp binary (with the execution build tag so the
// model gateway plane is present) and records its exact identity.
func BuildPlatform(repoRoot string) (VersionInfo, error) {
	rev, dirty := gitRevision(repoRoot)
	vi := VersionInfo{SourceRevision: rev, SourceDirty: dirty, HarnessRevision: rev}
	bin := repoRoot + "/bin/ahp-profiling"
	vi.BinaryPath = bin
	vi.BuildCommand = "go build -tags ahp_exec -o bin/ahp-profiling ./cmd/ahp"
	started := time.Now().UTC()
	cmd := exec.Command("go", "build", "-tags", "ahp_exec", "-o", bin, "./cmd/ahp")
	cmd.Dir = repoRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		return vi, &BuildError{Err: err, Output: string(out)}
	}
	sum, err := FileSHA256(bin)
	if err != nil {
		return vi, err
	}
	vi.BinarySHA256 = sum
	vi.BinaryBuiltAtUTC = started.Format(time.RFC3339)
	// Honest by default: this path is a build target, so it is not a pin.
	vi.BinaryPinned, vi.BinaryPinReason = BinaryIsPinned(bin)
	return vi, nil
}

// BuildError carries build output for diagnostics (output contains no secrets).
type BuildError struct {
	Err    error
	Output string
}

func (e *BuildError) Error() string { return "build failed: " + e.Err.Error() }

// FileSHA256 returns the hex sha256 of a file.
func FileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// BuildInfo is what the MEASURED binary itself says it was built from. It is
// read out of the binary (go version -m), not from the working tree, because a
// profile now measures a binary nobody in the measurement built -- the canonical
// candidate build arrives from the integrator, and the lane tree describes the
// HARNESS, not the subject. Root acceptance asks for the binary's own
// vcs.revision/vcs.modified next to the declared product_revision: a declaration
// and a stamp that disagree is exactly how published numbers got attached to the
// wrong candidate once already.
//
// Absence is stated, never guessed: a -trimpath build from a detached checkout
// can carry no vcs stamp at all, and then revision is null with a reason rather
// than an empty string a reader might mistake for "revision zero".
type BuildInfo struct {
	Revision  *string `json:"revision"`
	Modified  *bool   `json:"modified"`
	Module    *string `json:"module"`
	GoVersion *string `json:"go_version"`
	BuildTags *string `json:"build_tags"`
	GOOS      *string `json:"goos"`
	GOARCH    *string `json:"goarch"`
	Trimpath  *bool   `json:"trimpath"`
	// Source names the command the facts came from, so a reviewer can re-run it.
	Source string `json:"source"`
	// UnavailableReason explains a null revision. "the binary carries no vcs
	// stamp" is a fact about the build; silently leaving the field out would
	// make it look like the harness forgot to look.
	UnavailableReason string `json:"unavailable_reason,omitempty"`
}

// ReadBinaryBuildInfo asks the binary itself. It never fails a measurement: an
// unreadable stamp is recorded as a reason, because the profile is still valid
// evidence about behaviour -- it just cannot claim the bytes are attested.
func ReadBinaryBuildInfo(path string) *BuildInfo {
	info := &BuildInfo{Source: "go version -m " + path}
	out, err := exec.Command("go", "version", "-m", path).CombinedOutput()
	if err != nil {
		info.UnavailableReason = "go version -m failed: " + strings.TrimSpace(string(out))
		return info
	}
	parseGoVersionM(string(out), info)
	ensureRevisionReason(info)
	return info
}

// ensureRevisionReason keeps the contract honest: a null revision is only ever
// a null WITH a reason. An unexplained null is the placeholder defect wearing a
// different hat -- the reader cannot tell "the build is unstamped" from "the
// harness never looked".
func ensureRevisionReason(info *BuildInfo) {
	if info.Revision == nil && info.UnavailableReason == "" {
		info.UnavailableReason = "the binary carries no vcs stamp (built outside a git checkout or with buildvcs=off): its revision cannot be read from the bytes and is only declared by product_revision"
	}
}

// parseGoVersionM reads the tab-separated records `go version -m` prints:
//
//	/path/bin:
//		build	vcs.revision=abc123
//		build	vcs.modified=false
//		mod	github.com/ahp-platform/ahp	(devel)
//
// Unknown keys are ignored on purpose: this is a witness, not a compiler.
func parseGoVersionM(out string, info *BuildInfo) {
	strp := func(v string) *string { return &v }
	boolp := func(v bool) *bool { return &v }
	for _, line := range strings.Split(out, "\n") {
		parts := strings.Split(strings.TrimPrefix(line, "\t"), "\t")
		if len(parts) < 2 {
			continue
		}
		switch parts[0] {
		case "mod":
			if len(parts) >= 3 && parts[1] != "" {
				info.Module = strp(parts[1] + "@" + parts[2])
			}
			continue
		case "build":
		default:
			continue
		}
		key, val, ok := strings.Cut(parts[1], "=")
		if !ok {
			continue
		}
		switch key {
		case "vcs.revision":
			info.Revision = strp(val)
		case "vcs.modified":
			if b, err := strconv.ParseBool(val); err == nil {
				info.Modified = boolp(b)
			}
		case "go_version":
			info.GoVersion = strp(val)
		case "-tags":
			info.BuildTags = strp(val)
		case "GOOS":
			info.GOOS = strp(val)
		case "GOARCH":
			info.GOARCH = strp(val)
		case "-trimpath":
			if b, err := strconv.ParseBool(val); err == nil {
				info.Trimpath = boolp(b)
			}
		}
	}
	if info.GoVersion == nil {
		// The header line carries the toolchain, in one of two shapes:
		//   /path/to/bin: go1.26.2                       (go >= 1.24 output)
		//   go version go1.26.2 darwin/arm64             (older output)
		for _, line := range strings.Split(out, "\n") {
			if _, tail, ok := strings.Cut(line, ":"); ok && strings.HasPrefix(strings.TrimSpace(tail), "go") {
				info.GoVersion = strp(strings.TrimSpace(tail))
				break
			}
			if v := strings.Fields(line); len(v) >= 4 && v[0] == "go" && v[1] == "version" && strings.HasPrefix(v[2], "go") {
				info.GoVersion = strp(v[2] + " " + v[3])
				break
			}
		}
	}
}
