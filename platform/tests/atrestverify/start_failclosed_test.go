package atrestverify_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/internal/atrest/objectcipher"
	"github.com/ahp-platform/ahp/internal/buildinfo"
)

// L4 - enabling at-rest encryption without any durable key material must refuse at START-UP,
// for both roles that mount the cipher (data plane and workspace plane).
//
// Why this matters: the alternative is a process that boots, seals with a volatile in-memory key,
// and cannot open its own bytes after a restart - data loss presented as a working deployment.
// The claim is therefore tested three ways: the exported start-up seam itself, and then the REAL
// product binary for each mounting role, so that the answer does not depend on the harness calling
// the same function the harness is testing.
func TestL4StartFailsClosedWithoutKeyMaterial(t *testing.T) {
	l := startLeg(t, "L4_start_failclosed_no_key_material")
	requireLive(t)

	// --- part A: the exported start-up seam ------------------------------------------------
	client := minioClient(t)
	t.Setenv("AHP_ATREST_ARTIFACTS_ENABLED", "1")
	t.Setenv(atrestEnvRoot, "")
	t.Setenv(atrestEnvByok, "")
	ciph, err := objectcipher.FromEnv(contextOr(t), client)
	if err == nil {
		t.Fatalf("FromEnv accepted an enabled toggle with no root secret and no BYOK: cipher=%v", ciph != nil)
	}
	if ciph != nil {
		t.Fatal("FromEnv returned both a cipher and an error")
	}
	msg := err.Error()
	l.set("seam_error", msg)
	if !strings.Contains(msg, atrestEnvRoot) || !strings.Contains(msg, atrestEnvByok) {
		t.Fatalf("the refusal must name the two env vars an operator can act on, got: %s", msg)
	}

	// Control: the same call WITH durable material must succeed, otherwise the refusal above
	// could be explained by a broken fixture instead of by the missing material.
	rootFile := filepath.Join(t.TempDir(), "root.key")
	if err := os.WriteFile(rootFile, []byte(hexKey()), 0o600); err != nil {
		t.Fatalf("root material fixture: %v", err)
	}
	t.Setenv(atrestEnvRoot, rootFile)
	ciphOK, errOK := objectcipher.FromEnv(contextOr(t), client)
	if errOK != nil {
		t.Fatalf("control with durable root material failed: %v", errOK)
	}
	if ciphOK == nil || !ciphOK.Enabled() {
		t.Fatal("control: material present but no enabled cipher was built")
	}
	l.set("control_with_root_material_builds_cipher", true)

	// And OFF must be inert: no cipher, no error.
	t.Setenv("AHP_ATREST_ARTIFACTS_ENABLED", "0")
	ciphOff, errOff := objectcipher.FromEnv(contextOr(t), client)
	if errOff != nil || ciphOff != nil {
		t.Fatalf("OFF must be inert (nil,nil), got cipher=%v err=%v", ciphOff != nil, errOff)
	}
	l.set("off_is_inert_nil_nil", true)

	// --- part B: the real binary, both mounting roles --------------------------------------
	bin := buildVerifyBinary(t)
	ident := checkBinaryIdentity(t, bin)
	l.set("binary_identity", ident)
	bucket := childBucket(t)
	scratchDSN := createScratchDatabase(t, "bin")

	cases := []struct {
		name string
		role []string
		env  []string
	}{
		{"role_data", []string{"-role", "data", "-listen", "127.0.0.1:0"}, nil},
		{"role_all_workspace_plane", []string{"-role", "all", "-listen", "127.0.0.1:0"},
			[]string{"AHP_ENABLE_WORKSPACE=1"}},
	}
	results := map[string]any{}
	for _, c := range cases {
		env := childEnv(scratchDSN, bucket, true, nil)
		env = append(env, c.env...)
		code, stderr, timedOut := runBinary(t, bin, c.role, env, 90*time.Second)
		fact := map[string]any{
			"exit_code": code, "ran_until_killed": timedOut,
			"refused":                code != 0 && !timedOut,
			"named_root_env":         strings.Contains(stderr, atrestEnvRoot),
			"named_byok_env":         strings.Contains(stderr, atrestEnvByok),
			"atrest_refusal_present": strings.Contains(stderr, "atrest: dynamic authority requires"),
		}
		if line := findLine(stderr, "atrest: dynamic authority requires"); line != "" {
			fact["error_line"] = scrub(line)
		}
		results[c.name] = fact
		if !boolOf(fact["refused"]) || !boolOf(fact["atrest_refusal_present"]) {
			t.Errorf("%s: enabling at-rest without key material must refuse at start-up; exit=%d ran_until_killed=%v refusal=%v stderr=%q",
				c.name, code, timedOut, fact["atrest_refusal_present"], truncate(scrub(tail(stderr, 600))))
		}
	}

	// Control run of the same role WITH material: the at-rest refusal must be gone. Whatever
	// else the process needs is not this leg's claim, so the assertion is only about the refusal.
	envCtl := childEnv(scratchDSN, bucket, true, []string{atrestEnvRoot + "=" + rootFile})
	codeC, stderrC, timedOutC := runBinary(t, bin, cases[0].role, envCtl, 45*time.Second)
	results["role_data_control_with_material"] = map[string]any{
		"exit_code": codeC, "ran_until_killed": timedOutC,
		"atrest_refusal_present": strings.Contains(stderrC, "atrest: dynamic authority requires"),
	}
	if strings.Contains(stderrC, "atrest: dynamic authority requires") {
		t.Errorf("control run still refused for missing material although %s was configured", atrestEnvRoot)
	}
	l.set("binary", results)
	l.set("level", "exported seam + real product binary for the two mounting roles")
	l.set("secret_policy", "stderr is scanned in memory; only a scrubbed error line appears in evidence")
}

// --- helpers --------------------------------------------------------------------------------

const (
	atrestEnvRoot     = "AHP_ATREST_ROOT_SECRET_FILE"
	atrestEnvByok     = "AHP_ATREST_BYOK_FILE"
	atrestEnvToggle   = "AHP_ATREST_ARTIFACTS_ENABLED"
	atrestRefusalMark = "atrest: dynamic authority requires"
)

func boolOf(v any) bool { b, _ := v.(bool); return b }

func truncate(s string) string {
	if len(s) <= 600 {
		return s
	}
	return s[:600] + "...(truncated)"
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

func findLine(hay, needle string) string {
	for _, line := range strings.Split(hay, "\n") {
		if strings.Contains(line, needle) {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

// scrub removes any value this process knows to be secret from a string destined for evidence.
func scrub(s string) string {
	for _, key := range []string{
		"AHP_MINIO_ROOT_USER", "AHP_MINIO_ROOT_PASSWORD", "AHP_TEST_DATABASE_URL",
		"DATABASE_URL", "AHP_DATABASE_URL", "AHP_TEST_REDIS_URL", "AHP_REDIS_ADDR",
		"AHP_DEV_JWT_SEED", "AHP_LLM_API_KEY",
	} {
		if v := os.Getenv(key); v != "" {
			s = strings.ReplaceAll(s, v, "[redacted]")
		}
	}
	if envDB != "" {
		s = strings.ReplaceAll(s, envDB, "[db]")
	}
	return s
}

// childEnv builds the child environment from an explicit allowlist: the binary gets exactly the
// inputs this leg is about, never a wholesale copy of the harness environment.
func childEnv(dsn, bucket string, toggleOn bool, extra []string) []string {
	env := []string{
		"PATH=/usr/bin:/bin",
		"AHP_DATABASE_URL=" + dsn,
		"AHP_S3_ENDPOINT=" + objectStoreForChild(),
		"AHP_S3_BUCKET=" + bucket,
		"AHP_MINIO_ROOT_USER=" + os.Getenv("AHP_MINIO_ROOT_USER"),
		"AHP_MINIO_ROOT_PASSWORD=" + os.Getenv("AHP_MINIO_ROOT_PASSWORD"),
		"AHP_S3_REGION=" + envOr("AHP_S3_REGION", "us-east-1"),
		"AHP_DEV_JWT_SEED=" + os.Getenv("AHP_DEV_JWT_SEED"),
		atrestEnvToggle + "=" + map[bool]string{true: "1", false: "0"}[toggleOn],
	}
	return append(env, extra...)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// runBinary starts the product binary and reports its exit code plus captured stderr.
// timedOut=true means the process was still alive at the deadline (i.e. it did NOT fail closed).
func runBinary(t *testing.T, bin string, args, env []string, limit time.Duration) (code int, stderr string, timedOut bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = env
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	cmd.Stdout = os.Stderr // stdout carries nothing but startup noise; kept out of evidence
	err := cmd.Run()
	timedOut = ctx.Err() == context.DeadlineExceeded
	if err == nil {
		return 0, errBuf.String(), timedOut
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode(), errBuf.String(), timedOut
	}
	return -1, errBuf.String() + "\nspawn error: " + err.Error(), timedOut
}

var builtBinary string

// buildVerifyBinary compiles the product binary the start-up leg runs, and stamps it with the
// identity channel this product defines as authoritative: internal/buildinfo documents that Go's
// automatic VCS stamp resolves the repository that OWNS the working-tree path, so a build made in
// a linked worktree under another repository gets stamped with THAT repository's commit. Measured
// here, not inferred: an unstamped lane build of this tree carried vcs.revision = the company-brain
// HQ repository's HEAD, an object that does not exist in the product repository at all. The
// injected -X stamp is therefore the only identity a lane binary may claim, and the leg verifies it
// through the binary's own --version channel rather than through a name.
func buildVerifyBinary(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("AHP_ATREST_VERIFY_BINARY"); p != "" {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("AHP_ATREST_VERIFY_BINARY is not usable: %v", err)
		}
		return p
	}
	if builtBinary == "" {
		rev, when, modified := treeIdentity(t)
		out := filepath.Join(t.TempDir(), "ahp-atverify")
		ldflags := fmt.Sprintf("-X github.com/ahp-platform/ahp/internal/buildinfo.Revision=%s"+
			" -X github.com/ahp-platform/ahp/internal/buildinfo.CommitTime=%s"+
			" -X github.com/ahp-platform/ahp/internal/buildinfo.Modified=%s", rev, when, modified)
		ctx, cancel := context.WithTimeout(context.Background(), 420*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "go", "build", "-tags", "ahp_exec,ahp_data",
			"-ldflags", ldflags, "-o", out, "./cmd/ahp")
		cmd.Dir = moduleRoot(t) // the test process runs with CWD inside tests/..., so the build needs the module root
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("building the product binary for the start-up leg failed: %v: %s", err, truncate(stderr.String()))
		}
		builtBinary = out
	}
	return builtBinary
}

// treeIdentity reads revision, commit time and dirty state from the tree the build is invoked in.
func treeIdentity(t *testing.T) (revision, commitTime, modified string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rev, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("git rev-parse: %v", err)
	}
	when, err := exec.CommandContext(ctx, "git", "log", "-1", "--format=%cI").Output()
	if err != nil {
		t.Fatalf("git log: %v", err)
	}
	st, err := exec.CommandContext(ctx, "git", "status", "--porcelain").Output()
	if err != nil {
		t.Fatalf("git status: %v", err)
	}
	revision = strings.TrimSpace(string(rev))
	if len(revision) < 40 {
		t.Fatalf("unexpected HEAD output: %q", truncate(revision))
	}
	if len(strings.TrimSpace(string(st))) > 0 {
		t.Fatalf("refusing to attest a build from a dirty tree: product bytes would not belong to a commit")
	}
	return revision, strings.TrimSpace(string(when)), "false"
}

// binaryIdentity asks the executable itself which revision it is, through the product's own
// identity channel, and records the automatic Go stamp alongside as contrast. A stamp that names
// another repository is reported, never trusted.
func binaryIdentity(t *testing.T, bin string) map[string]any {
	t.Helper()
	out := map[string]any{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "-version")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s -version failed: %v", bin, err)
	}
	line := strings.TrimSpace(stderr.String())
	out["version_line"] = scrub(truncate(line))
	out["reported_revision"] = fieldOf(line, "revision=")
	out["reported_modified"] = fieldOf(line, "modified=")
	out["reported_source"] = fieldOf(line, "identity_source=")

	stampCtx, stampCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer stampCancel()
	raw, err := exec.CommandContext(stampCtx, "go", "version", "-m", bin).Output()
	if err == nil {
		for _, l := range strings.Split(string(raw), "\n") {
			fields := strings.Fields(l)
			if len(fields) < 3 || fields[0] != "build" {
				continue
			}
			if fields[1] == "vcs.revision" || fields[1] == "vcs.modified" {
				out["go_automatic_stamp_"+fields[1]] = fields[2]
			}
		}
	}
	if f, err := os.Open(bin); err == nil {
		h := sha256.New()
		_, _ = io.Copy(h, f)
		_ = f.Close()
		out["binary_sha256"] = hex.EncodeToString(h.Sum(nil))
	}
	return out
}

// fieldOf reads a "key=value" token out of the version line.
func fieldOf(line, key string) string {
	for _, tok := range strings.Fields(line) {
		if strings.HasPrefix(tok, key) {
			return strings.TrimPrefix(tok, key)
		}
	}
	return ""
}

// checkBinaryIdentity is the provenance assertion of this leg: the binary that is about to be
// started must name the revision under verification, from the injected (authoritative) source,
// and the tree must be clean.
func checkBinaryIdentity(t *testing.T, bin string) map[string]any {
	t.Helper()
	ident := binaryIdentity(t, bin)
	rev, dirty, err := gitProvenance(context.Background())
	if err != nil {
		t.Fatalf("git provenance: %v", err)
	}
	if dirty {
		t.Errorf("the working tree carries uncommitted changes; product bytes are not attributable to a commit")
	}
	if got, _ := ident["reported_revision"].(string); got != rev {
		t.Errorf("the binary reports revision %v, the tree under verification is %s", ident["reported_revision"], rev)
	}
	if got, _ := ident["reported_source"].(string); got != string(buildinfo.SourceInjected) {
		t.Errorf("identity came from %v, not from the injected stamp; Go's automatic stamp cannot be trusted inside a lane worktree", ident["reported_source"])
	}
	if got, _ := ident["reported_modified"].(string); got != "false" {
		t.Errorf("the binary reports modified=%v", ident["reported_modified"])
	}
	if auto, ok := ident["go_automatic_stamp_vcs.revision"].(string); ok && auto != "" && auto != rev {
		note("Go's automatic vcs.revision on this binary names %s, which is not the product revision %s: the stamp resolves the repository that owns the worktree path, so it must never be used as evidence", auto, rev)
		ident["go_automatic_stamp_names_other_repository"] = true
	}
	ident["tree_revision"] = rev
	return ident
}

// moduleRoot resolves the module directory from the go.mod the toolchain reports for this
// package directory, so a nested build is never relative to the test's CWD.
func moduleRoot(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	gomod := strings.TrimSpace(string(out))
	if gomod == "" || gomod == os.DevNull {
		t.Fatal("no go.mod found for this module")
	}
	return filepath.Dir(gomod)
}

// childBucket creates the bucket the child binary is told to use, and removes it afterwards.
func childBucket(t *testing.T) string {
	t.Helper()
	client := minioClient(t)
	bucket := "go-atverify-bin-" + runIdentValue()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("child bucket: %v", err)
	}
	t.Cleanup(func() {
		c, cn := context.WithTimeout(context.Background(), 60*time.Second)
		defer cn()
		for obj := range client.ListObjects(c, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if obj.Err != nil {
				break
			}
			_ = client.RemoveObject(c, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
		}
		_ = client.RemoveBucket(c, bucket)
	})
	return bucket
}

// hexKey returns a 64-hex-character root key file body (32 bytes of key material). It is printed
// nowhere and lives only inside a t.TempDir() that the test framework removes.
func hexKey() string {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(raw[:])
}

// objectStoreForChild is the object-store endpoint the child binary is pointed at.
func objectStoreForChild() string { return os.Getenv("AHP_S3_ENDPOINT") }
