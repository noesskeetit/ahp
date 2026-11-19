package compute_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

// These are protocol unit tests. The real mTLS/PG/MinIO composition proof is
// cmd/ahp/TestComputeExportRealOwners, not this controlled owner.
type exportOwnerProbe struct {
	request   sdk.UploadRequest
	body      []byte
	sealed    bool
	fail      error
	malformed bool
	puts      int
}

func (p *exportOwnerProbe) BeginUpload(_ context.Context, in sdk.UploadRequest) (sdk.Upload, error) {
	p.request = in
	return sdk.Upload{ID: "artifact-one", ExpectedSHA: in.SHA256, ExpectedSize: in.Size}, p.fail
}
func (p *exportOwnerProbe) Upload(_ context.Context, _ sdk.Upload, data []byte) error {
	p.body = data
	p.puts++
	return p.fail
}
func (p *exportOwnerProbe) Seal(ctx context.Context, id string) (sdk.ArtifactRef, error) {
	p.sealed = true
	return p.Get(ctx, id)
}
func (p *exportOwnerProbe) Get(_ context.Context, id string) (sdk.ArtifactRef, error) {
	if p.fail != nil {
		return sdk.ArtifactRef{}, p.fail
	}
	if !p.sealed {
		return sdk.ArtifactRef{}, contracts.NewError(contracts.ErrNotFound, "unsealed")
	}
	if p.malformed {
		return sdk.ArtifactRef{}, nil
	}
	return sdk.ArtifactRef{ID: id, Version: "version-one", SHA256: p.request.SHA256, Size: p.request.Size, Tenant: "tenant", Deployment: "deployment", Subject: "alice", TaskID: "task", ContextID: "context"}, nil
}
func exportRequest() compute.ArtifactExportRequest {
	return compute.ArtifactExportRequest{Scope: contracts.Scope{Tenant: "tenant", Deployment: "deployment", Subject: "alice"}, ComputerID: "computer", TaskID: "task", ContextID: "context", Key: "export-key", SourcePath: "out.txt", Filename: "out.txt", ContentType: "text/plain", Body: strings.NewReader("payload"), SizeHint: 7, Check: func(context.Context) error { return nil }}
}
func TestArtifactExporterSuccessAndReplay(t *testing.T) {
	p := &exportOwnerProbe{}
	e, err := compute.NewOwnerArtifactExporter(p)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := e.Export(ctx(t), exportRequest())
	if err != nil {
		t.Fatal(err)
	}
	if ref.ArtifactID != "artifact-one" || ref.Version != "version-one" || ref.SizeBytes != 7 || string(p.body) != "payload" {
		t.Fatalf("unexpected owner result: %+v", ref)
	}
	same, err := e.Export(ctx(t), exportRequest())
	if err != nil || same != ref || p.puts != 1 {
		t.Fatalf("replay=%+v puts=%d err=%v", same, p.puts, err)
	}
}
func TestArtifactExporterOwnerRefusalsAndNoFabrication(t *testing.T) {
	for _, code := range []string{contracts.ErrForbidden, contracts.ErrNotFound, contracts.ErrConflict, contracts.ErrInvalid, contracts.ErrUnavailable} {
		p := &exportOwnerProbe{fail: contracts.NewError(code, "owner refusal")}
		e, _ := compute.NewOwnerArtifactExporter(p)
		if _, err := e.Export(ctx(t), exportRequest()); contracts.Code(err) != code || p.puts != 0 {
			t.Fatalf("%s: puts=%d err=%v", code, p.puts, err)
		}
	}
	p := &exportOwnerProbe{malformed: true}
	e, _ := compute.NewOwnerArtifactExporter(p)
	if _, err := e.Export(ctx(t), exportRequest()); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("fabricated owner ref accepted: %v", err)
	}
}
func TestArtifactExporterRevocationBeforeSeal(t *testing.T) {
	p := &exportOwnerProbe{}
	e, _ := compute.NewOwnerArtifactExporter(p)
	r := exportRequest()
	r.Check = func(context.Context) error {
		if p.puts > 0 {
			return contracts.NewError(contracts.ErrForbidden, "revoked after PUT")
		}
		return nil
	}
	if _, err := e.Export(ctx(t), r); contracts.Code(err) != contracts.ErrForbidden || p.sealed || p.puts != 1 {
		t.Fatalf("publication after revoke: sealed=%v puts=%d err=%v", p.sealed, p.puts, err)
	}
}
func TestArtifactExporterInputFailClosed(t *testing.T) {
	for name, change := range map[string]func(*compute.ArtifactExportRequest){"missing-key": func(r *compute.ArtifactExportRequest) { r.Key = "" }, "missing-authority": func(r *compute.ArtifactExportRequest) { r.Check = nil }, "changed-size": func(r *compute.ArtifactExportRequest) { r.SizeHint = 8 }, "unreadable": func(r *compute.ArtifactExportRequest) { r.Body = io.LimitReader(errorReader{}, 8) }} {
		t.Run(name, func(t *testing.T) {
			p := &exportOwnerProbe{}
			e, _ := compute.NewOwnerArtifactExporter(p)
			r := exportRequest()
			change(&r)
			if _, err := e.Export(ctx(t), r); err == nil || p.puts != 0 {
				t.Fatalf("bad input accepted: %v", err)
			}
		})
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func TestNewArtifactExporterFromEnvUnset(t *testing.T) {
	t.Setenv("AHP_ARTIFACTS_URL", "")
	client, err := compute.NewArtifactExporterFromEnv()
	if err != nil || client != nil {
		t.Fatalf("unset: %v %v", client, err)
	}
}
func TestNewArtifactExporterRemoteRequiresBoundWorkload(t *testing.T) {
	for _, base := range []string{"", "ftp://example.test", "http://127.0.0.1:1", "https://example.test"} {
		if _, err := compute.NewArtifactExporter(base, &http.Client{}, func(context.Context) (string, error) { return "task", nil }); err == nil {
			t.Fatalf("unverified remote accepted: %s", base)
		}
	}
	t.Setenv("AHP_ARTIFACTS_URL", "https://example.test")
	t.Setenv("AHP_ARTIFACTS_TOKEN", "static-not-a-real-secret")
	if _, err := compute.NewArtifactExporterFromEnv(); err == nil {
		t.Fatal("static bearer accepted")
	}
}
