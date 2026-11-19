package model

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// sealRealArtifact uploads and seals one artifact through the ordinary owner API.
func sealRealArtifact(t *testing.T, ctx context.Context, source *artifacts.Store, a FileActor, mime, body string) artifacts.ArtifactRef {
	t.Helper()
	sum := sha256.Sum256([]byte(body))
	upload, err := source.BeginUploadWithBinding(ctx, a.Scope, artifacts.Binding{ContextID: a.Context, TaskID: a.Task}, "gate-"+hex.EncodeToString(sum[:8]), artifacts.UploadRequest{Name: "gated.txt", MIME: mime, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body))})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, upload.PresignedURL, bytes.NewReader([]byte(body)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("upload %d", resp.StatusCode)
	}
	ref, err := source.Seal(ctx, a.Scope, upload.ID)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

// moduleRootForTest walks up from the test's directory to the module root, so a
// leg can build a product binary without knowing where anyone checked this out.
func moduleRootForTest(t *testing.T) string {
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

// trustedArtifactParser resolves the parser the artifact plane requires before it
// will hand out bytes of a supported text type (internal/artifacts/parser.go runs
// it, and cmd/artifact-parser installs a Linux seccomp filter before reading
// untrusted bytes -- sandbox_other.go refuses to run anywhere else).
//
// Resolution order is the deployment's order: the operator's binary
// (AHP_ARTIFACT_PARSER) wins; otherwise, on Linux, the leg builds the product's
// own parser from cmd/artifact-parser, which is the same штатный путь the roles
// use and keeps the leg reproducible on any Linux runner instead of depending on
// one machine's export. On a non-Linux host NO legitimate parser can exist, so
// the answer is empty and the caller must not pretend extraction happened.
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
	build.Dir = moduleRootForTest(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the product artifact parser (the acceptance image must be able to build its own sandbox): %v: %s", err, out)
	}
	return bin
}

// ingestionState reads the artifact's extraction state out of band, so a failure
// says WHICH state the plane was in instead of leaving the reader to guess.
func ingestionState(t *testing.T, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(context.Background(),
		`SELECT ingestion_state FROM ahp_artifacts.artifacts WHERE id=$1`, id).Scan(&state); err != nil {
		t.Fatalf("read the artifact ingestion state: %v", err)
	}
	return state
}

func TestProviderFileRealArtifactOwnerAndContext(t *testing.T) {
	e, s, a, _, p := fileFixture(t)
	// The gate runs BEFORE any client is built: an endpoint inherited from the
	// environment is not a permission to write, and a check placed after
	// NewArtifactPlane would be a check placed after the first write.
	if reason := artifactPlaneSkipReason(); reason != "" {
		t.Log(reason)
		t.Skip(reason)
	}
	raw := os.Getenv("AHP_S3_ENDPOINT")
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal("bad object endpoint")
	}
	client, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""), Secure: u.Scheme == "https"})
	if err != nil {
		t.Fatal(err)
	}
	bucket := "model-files-" + e.u
	parser := trustedArtifactParser(t)
	source, err := artifacts.New(e.pool, client, bucket, func(context.Context, contracts.Scope, string, string) error { return nil }, artifacts.WithGrantLabels(func(context.Context, contracts.Scope) (labels.Set, error) {
		return labels.Set{Subjects: []string{a.Scope.Subject}}, nil
	}), artifacts.WithParserCommand(artifacts.ParserCommand{Path: parser}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = source.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for obj := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if obj.Err == nil {
				_ = client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
			}
		}
		_ = client.RemoveBucket(ctx, bucket)
	})
	// A supported text type is only deliverable once the sandboxed parser has
	// extracted it. With a parser the leg runs that realistic case end to end;
	// without one (a non-Linux host cannot run the seccomp sandbox at all) the leg
	// proves the refusal is the missing parser and then exercises the copy
	// semantics on a type that needs no extraction. Nothing is weakened: the
	// extraction gate is asserted on both sides, and no leg passes by pretending
	// extraction happened.
	t.Logf("artifact parser: %s", map[bool]string{true: "trusted parser present", false: "NONE on " + runtime.GOOS + " (sandbox is Linux-only)"}[parser != ""])
	if parser == "" {
		blocked := sealRealArtifact(t, ctx, source, a, "text/plain", "extraction-gated bytes")
		t.Logf("text/plain artifact %s sealed into ingestion_state=%q with no parser", blocked.ID, ingestionState(t, e.pool, blocked.ID))
		s.cfg.Source = source
		if _, err := s.Copy(ctx, a, "gate", blocked.ID); err == nil || p.upload.Load() != 0 {
			t.Fatal("unextracted text bytes were copied: the extraction gate is not enforced")
		} else {
			t.Logf("extraction gate refused as designed: %v", err)
		}
	}
	mime := "application/octet-stream"
	if parser != "" {
		mime = "text/plain"
	}
	data := []byte("sealed owner bytes")
	sum := sha256.Sum256(data)
	upload, err := source.BeginUploadWithBinding(ctx, a.Scope, artifacts.Binding{ContextID: a.Context, TaskID: a.Task}, "real-file", artifacts.UploadRequest{Name: "private-original-name.txt", MIME: mime, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, upload.PresignedURL, bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal("object upload failed")
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("object upload %d", resp.StatusCode)
	}
	ref, err := source.Seal(ctx, a.Scope, upload.ID)
	if err != nil {
		t.Fatal(err)
	}
	s.cfg.Source = source
	bad := a
	bad.Context = "other-context"
	if _, err = s.Copy(ctx, bad, "wrong-context", ref.ID); err == nil || p.upload.Load() != 0 {
		t.Fatal("context escape dispatched")
	}
	t.Logf("%s artifact %s sealed into ingestion_state=%q before the owner read", mime, ref.ID, ingestionState(t, e.pool, ref.ID))
	f, err := s.Copy(ctx, a, "real-copy", ref.ID)
	if err != nil || f.Status != FileReady {
		t.Fatalf("owner copy %s %v (mime=%s, ingestion_state=%q, parser=%q)", f.Status, err, mime, ingestionState(t, e.pool, ref.ID), parser)
	}
	if f.SHA256 != ref.SHA256 || p.upload.Load() != 1 {
		t.Fatal("copy didn't use immutable source")
	}
	foreign := a
	foreign.Scope.Subject = "other"
	if _, err = s.Copy(ctx, foreign, "foreign", ref.ID); err == nil || p.upload.Load() != 1 {
		t.Fatal("foreign source copied")
	}
}
