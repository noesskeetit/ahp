package executor_test

// Where the real dependencies of the artifact-plane legs come from, and what an
// absent dependency means.
//
// Two failure modes were measured on this base and both make the wide gate
// unreadable:
//   - ten legs of this package FATAL when AHP_TEST_DATABASE_URL / AHP_S3_ENDPOINT
//     / the MinIO credentials are absent, so a default `go test ./internal/executor/`
//     reports a RED internal/executor for a property of the runner, not of the
//     product (receipt: logs/executor-baseline-noenv.log - 10 FAIL, 0 SKIP);
//   - TestArtifactPlaneFromEnvPreservesParserReadinessGate FATAL'd when
//     AHP_ARTIFACT_PARSER is unset, and a macOS runner cannot build the real one
//     because cmd/artifact-parser installs a Linux seccomp filter before it will
//     look at untrusted content (cmd/artifact-parser/sandbox_other.go).
//
// The remedy is the pattern the execbridge lane already had accepted (Volta,
// 18eebc17): the leg skips with a machine-readable token naming the exact missing
// variables and stating that a skip is NOT coverage. Silent passing and
// red-because-of-the-environment are both wrong; the token keeps the counter
// honest either way.

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/executor"
)

const (
	skipTokenRealDeps = "SKIP_ENV[tag=real_deps]"
	skipTokenParser   = "SKIP_ENV[tag=artifact_parser]"
	skipNotCoverage   = "a skip here means not-covered, not passed"
)

func envAbsent(names ...string) []string {
	var missing []string
	for _, n := range names {
		if os.Getenv(n) == "" {
			missing = append(missing, n)
		}
	}
	return missing
}

// realDepsSkipReason names the missing PostgreSQL / object-store configuration.
// Non-empty means "do not run"; empty means every prerequisite is present.
func realDepsSkipReason() string {
	missing := envAbsent("AHP_TEST_DATABASE_URL", "AHP_S3_ENDPOINT",
		"AHP_MINIO_ROOT_USER", "AHP_MINIO_ROOT_PASSWORD")
	if len(missing) == 0 {
		return ""
	}
	return skipTokenRealDeps + " reason=real_store_env_absent missing=" + strings.Join(missing, ",") +
		" -- these legs measure the real artifact plane against PostgreSQL and object storage: " +
		"they are evidence only when they RAN, and " + skipNotCoverage
}

// trustedParserSkipReason answers for the legs whose property is the REAL sandboxed
// parser (JSON validation, the seccomp sandbox). The product's own parser can only
// run on Linux, so on any other platform the only honest answer is a named skip --
// a stub must never stand in for a sandbox.
func trustedParserSkipReason() string {
	if runtime.GOOS == "linux" || os.Getenv(executor.EnvArtifactParser) != "" {
		return ""
	}
	return skipTokenParser + " reason=trusted_parser_absent missing=" +
		strings.Join(envAbsent(executor.EnvArtifactParser), ",") +
		" -- cmd/artifact-parser refuses to run outside Linux by design, so these legs need the " +
		"operator's binary in " + executor.EnvArtifactParser + "; " + skipNotCoverage
}

// realTrustedParser resolves the parser the deployment itself would use: the
// operator's binary first, then on Linux a build of the product's own parser.
// Empty means "no real parser here" and callers must say so, not fake it.
func realTrustedParser(t *testing.T) string {
	t.Helper()
	if p := os.Getenv(executor.EnvArtifactParser); p != "" {
		return p
	}
	if runtime.GOOS != "linux" {
		return ""
	}
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for range 12 {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		root = filepath.Dir(root)
	}
	bin := filepath.Join(t.TempDir(), "artifact-parser")
	build := exec.Command("go", "build", "-o", bin, "./cmd/artifact-parser")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the product artifact parser: %v: %s", err, out)
	}
	return bin
}

// contractParser is the fallback for legs whose property belongs to the STORE
// rather than to the parser: idempotent replay, read-back of a sealed version.
//
// From the store's side a parser is any absolute-path executable that reads the
// bytes on stdin and writes UTF-8 text to stdout (internal/artifacts/parser.go
// extract: absolute path, exit 0, valid UTF-8, no NUL). This stub satisfies that
// contract and nothing else -- it validates no JSON and installs no sandbox, so
// it must never be used by a leg that claims to cover the parser itself. The leg
// logs which parser it ran with, so the log says what was actually measured.
func contractParser(t *testing.T) string {
	t.Helper()
	if p := realTrustedParser(t); p != "" {
		t.Log("parser_source=real")
		return p
	}
	path := filepath.Join(t.TempDir(), "contract-parser")
	if err := os.WriteFile(path, []byte("#!/bin/sh\ncat\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Log("parser_source=lane_contract_stub -- a stdin-to-stdout stand-in for the operator's " +
		"parser: enough for the store's read-back path, it is NOT the sandboxed parser")
	return path
}

// TestRealDepsSkipReasonIsMachineReadable pins the contract of the skip itself,
// so the text cannot rot into a bare "skipped" and make the counter unreadable
// again -- that rot is the defect being fixed, not a detail.
func TestRealDepsSkipReasonIsMachineReadable(t *testing.T) {
	t.Setenv("AHP_TEST_DATABASE_URL", "")
	t.Setenv("AHP_S3_ENDPOINT", "")
	t.Setenv("AHP_MINIO_ROOT_USER", "")
	t.Setenv("AHP_MINIO_ROOT_PASSWORD", "")
	reason := realDepsSkipReason()
	for _, want := range []string{
		skipTokenRealDeps,
		"reason=real_store_env_absent",
		"missing=AHP_TEST_DATABASE_URL,AHP_S3_ENDPOINT,AHP_MINIO_ROOT_USER,AHP_MINIO_ROOT_PASSWORD",
		"not passed",
	} {
		if !strings.Contains(reason, want) {
			t.Fatalf("the real-deps skip reason must carry %q, got %q", want, reason)
		}
	}

	// A partially configured lane must name only what is actually absent, so an
	// operator is not sent after variables that are already set.
	t.Setenv("AHP_TEST_DATABASE_URL", "postgres://127.0.0.1:1/lane")
	t.Setenv("AHP_S3_ENDPOINT", "http://127.0.0.1:29780")
	t.Setenv("AHP_MINIO_ROOT_USER", "lane-access-key")
	reason = realDepsSkipReason()
	if reason == "" || strings.Contains(reason, "missing=AHP_TEST_DATABASE_URL") {
		t.Fatalf("partial configuration must report only the absent variable, got %q", reason)
	}
	if !strings.Contains(reason, "missing=AHP_MINIO_ROOT_PASSWORD") {
		t.Fatalf("partial configuration must report AHP_MINIO_ROOT_PASSWORD, got %q", reason)
	}

	// With everything present the reason is empty: the legs run, they do not skip.
	t.Setenv("AHP_MINIO_ROOT_PASSWORD", "lane-secret-value-not-printed")
	if got := realDepsSkipReason(); got != "" {
		t.Fatalf("a fully configured lane must not skip, got %q", got)
	}

	// The parser token carries the same shape, and its platform rule is pinned:
	// the token exists precisely because the real sandbox is Linux-only.
	t.Setenv(executor.EnvArtifactParser, "")
	preason := trustedParserSkipReason()
	if runtime.GOOS == "linux" {
		if preason != "" {
			t.Fatalf("on Linux the lane builds the product parser, so no skip: %q", preason)
		}
		return
	}
	for _, want := range []string{skipTokenParser, "reason=trusted_parser_absent",
		"missing=" + executor.EnvArtifactParser, "not passed"} {
		if !strings.Contains(preason, want) {
			t.Fatalf("the parser skip reason must carry %q, got %q", want, preason)
		}
	}
}
