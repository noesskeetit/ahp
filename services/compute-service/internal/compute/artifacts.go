package compute

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	artifactsdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

// ArtifactExportRequest comes from a verified task and one confined snapshot.
// Check revalidates the source authority/fence before each owner operation.
type ArtifactExportRequest struct {
	Scope                                          contracts.Scope
	ComputerID, TaskID, ContextID, Key, SourcePath string
	Labels                                         map[string]string // Legacy diagnostics, never access-control labels.
	Filename, ContentType                          string
	Body                                           io.Reader
	SizeHint                                       int64
	Check                                          func(context.Context) error
}

type ArtifactExportClient interface {
	Export(context.Context, ArtifactExportRequest) (ArtifactRef, error)
}
type TaskTokenSource func(context.Context) (string, error)

// ArtifactOwner is the public owner contract. A co-located typed facade keeps
// the verified principal; separate workloads use the HTTPS/mTLS SDK.
type ArtifactOwner interface {
	BeginUpload(context.Context, artifactsdk.UploadRequest) (artifactsdk.Upload, error)
	Upload(context.Context, artifactsdk.Upload, []byte) error
	Seal(context.Context, string) (artifactsdk.ArtifactRef, error)
	Get(context.Context, string) (artifactsdk.ArtifactRef, error)
}
type ArtifactExporter struct{ owner ArtifactOwner }

func NewOwnerArtifactExporter(owner ArtifactOwner) (*ArtifactExporter, error) {
	if owner == nil {
		return nil, invalid("Artifacts owner required")
	}
	return &ArtifactExporter{owner: owner}, nil
}
func NewArtifactExporter(baseURL string, httpc *http.Client, token TaskTokenSource) (*ArtifactExporter, error) {
	if token == nil {
		return nil, invalid("current task token source required")
	}
	client, err := artifactsdk.New(artifactsdk.Config{BaseURL: baseURL, HTTPClient: httpc, TokenSource: artifactsdk.TokenSource(token)})
	if err != nil {
		return nil, err
	}
	return NewOwnerArtifactExporter(client)
}

type exportTokenKey struct{}

// NewArtifactExporterFromEnv requires an explicit HTTPS/mTLS workload. Only the
// current verified request supplies a bearer; the owner rejects a different AZP.
func NewArtifactExporterFromEnv() (ArtifactExportClient, error) {
	base := strings.TrimSpace(os.Getenv("AHP_ARTIFACTS_URL"))
	if base == "" {
		return nil, nil
	}
	if os.Getenv("AHP_ARTIFACTS_TOKEN") != "" {
		return nil, invalid("static artifact export bearer is unsupported")
	}
	cert, err := tls.LoadX509KeyPair(os.Getenv("AHP_ARTIFACTS_TLS_CERT"), os.Getenv("AHP_ARTIFACTS_TLS_KEY"))
	if err != nil {
		return nil, invalid("artifact export workload certificate required")
	}
	ca, err := os.ReadFile(os.Getenv("AHP_ARTIFACTS_TLS_CA"))
	if err != nil {
		return nil, invalid("artifact export CA required")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, invalid("invalid artifact export CA")
	}
	hc := &http.Client{Timeout: 45 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{cert}}}}
	return NewArtifactExporter(base, hc, func(ctx context.Context) (string, error) {
		token, _ := ctx.Value(exportTokenKey{}).(string)
		if token == "" {
			return "", contracts.NewError(contracts.ErrForbidden, "verified export task token required")
		}
		return token, nil
	})
}

// Export uses a stable operation key. Path and bytes belong to its immutable
// request: replay cannot replace an earlier export or fabricate an ArtifactRef.
func (e *ArtifactExporter) Export(ctx context.Context, req ArtifactExportRequest) (ArtifactRef, error) {
	if req.Body == nil || req.Key == "" || req.TaskID == "" || req.ContextID == "" || req.SourcePath == "" || req.Check == nil {
		return ArtifactRef{}, invalid("export requires key, task, context, source and current authority")
	}
	if err := req.Scope.Validate(); err != nil {
		return ArtifactRef{}, err
	}
	if err := req.Check(ctx); err != nil {
		return ArtifactRef{}, err
	}
	data, err := io.ReadAll(io.LimitReader(exportContextReader{ctx, req.Body}, artifactsdk.MaxSize+1))
	if err != nil {
		return ArtifactRef{}, Unavailable("source", "cannot read export snapshot")
	}
	if len(data) == 0 || int64(len(data)) > artifactsdk.MaxSize || (req.SizeHint >= 0 && int64(len(data)) != req.SizeHint) {
		return ArtifactRef{}, invalid("export snapshot size is invalid or changed")
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	keyRaw, _ := json.Marshal([]string{"compute-export-v1", req.Scope.Tenant, req.Scope.Deployment, req.Key})
	key := sha256.Sum256(keyRaw)
	sourceRaw, _ := json.Marshal([]string{req.ComputerID, req.SourcePath})
	pathHash := sha256.Sum256(sourceRaw)
	filename := req.Filename
	if len(filename) > 183 {
		filename = filename[:183]
		for !utf8.ValidString(filename) && len(filename) > 0 {
			filename = filename[:len(filename)-1]
		}
	}
	name := "export-" + hex.EncodeToString(pathHash[:]) + "-" + filename
	if err = req.Check(ctx); err != nil {
		return ArtifactRef{}, err
	}
	up, err := e.owner.BeginUpload(ctx, artifactsdk.UploadRequest{Key: "compute:" + hex.EncodeToString(key[:]), Name: name, MIME: req.ContentType, SHA256: digest, Size: int64(len(data)), Labels: labels.Set{}})
	if err != nil {
		return ArtifactRef{}, err
	}
	valid := func(ref artifactsdk.ArtifactRef) (ArtifactRef, error) {
		if ref.ID != up.ID || ref.Version == "" || ref.SHA256 != digest || ref.Size != int64(len(data)) || ref.Tenant != req.Scope.Tenant || ref.Deployment != req.Scope.Deployment || ref.Subject != req.Scope.Subject || ref.ContextID != req.ContextID || ref.TaskID != req.TaskID {
			return ArtifactRef{}, contracts.NewError(contracts.ErrConflict, "Artifacts owner returned a different export identity")
		}
		return ArtifactRef{ArtifactID: ref.ID, Version: ref.Version, Digest: ref.SHA256, SizeBytes: ref.Size}, nil
	}
	// Reconcile before a new PUT: a lost reply may already have committed Seal.
	if ref, getErr := e.owner.Get(ctx, up.ID); getErr == nil {
		if err = req.Check(ctx); err != nil {
			return ArtifactRef{}, err
		}
		return valid(ref)
	} else if contracts.Code(getErr) != contracts.ErrNotFound {
		return ArtifactRef{}, getErr
	}
	if err = req.Check(ctx); err != nil {
		return ArtifactRef{}, err
	}
	if err = e.owner.Upload(ctx, up, data); err != nil {
		return ArtifactRef{}, err
	}
	if err = req.Check(ctx); err != nil {
		return ArtifactRef{}, err
	}
	ref, err := e.owner.Seal(ctx, up.ID)
	if err != nil {
		return ArtifactRef{}, err
	}
	return valid(ref)
}

// prepareExport opens through one computer's root. Inspection and reading use
// the same file descriptor. Archives reject links/special files and bound input.
func prepareExport(ctx context.Context, root *os.Root, path string) (io.ReadCloser, int64, string, string, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, "", "", func() {}, err
	}
	f, err := root.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, 0, "", "", func() {}, contracts.NewError(contracts.ErrForbidden, "export source is outside its computer or unavailable")
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, "", "", func() {}, err
	}
	if info.Mode().IsRegular() {
		if info.Size() <= 0 || info.Size() > artifactsdk.MaxSize {
			f.Close()
			return nil, 0, "", "", func() {}, invalid("export file exceeds byte bounds")
		}
		defer f.Close()
		tmp, err := os.CreateTemp("", "ahp-compute-snapshot-*")
		if err != nil {
			return nil, 0, "", "", func() {}, err
		}
		cleanup := func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }
		if err = copyExportSnapshot(ctx, tmp, f, info); err != nil {
			cleanup()
			return nil, 0, "", "", func() {}, err
		}
		if _, err = tmp.Seek(0, io.SeekStart); err != nil {
			cleanup()
			return nil, 0, "", "", func() {}, err
		}
		return tmp, info.Size(), "application/octet-stream", filepath.Base(path), cleanup, nil
	}
	f.Close()
	if !info.IsDir() {
		return nil, 0, "", "", func() {}, invalid("only regular files and directories can be exported")
	}
	dir, err := root.OpenRoot(path)
	if err != nil {
		return nil, 0, "", "", func() {}, contracts.NewError(contracts.ErrForbidden, "export directory escaped its computer")
	}
	defer dir.Close()
	tmp, err := os.CreateTemp("", "ahp-compute-export-*.tar.gz")
	if err != nil {
		return nil, 0, "", "", func() {}, err
	}
	cleanup := func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }
	w := &exportBoundWriter{out: tmp, remaining: artifactsdk.MaxSize}
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	var input int64
	entries := 0
	err = fs.WalkDir(dir.FS(), ".", func(path string, d fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if path == "." {
			return nil
		}
		entries++
		if entries > 10000 {
			return invalid("export archive has too many entries")
		}
		if d.Type()&os.ModeSymlink != 0 {
			return contracts.NewError(contracts.ErrForbidden, "archive links are unsupported")
		}
		file, err := dir.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			return err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return invalid("archive contains a special file")
		}
		if !info.IsDir() {
			input += info.Size()
		}
		if input > artifactsdk.MaxSize {
			return invalid("archive source exceeds byte bound")
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(path)
		if err = tw.WriteHeader(header); err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		return copyExportSnapshot(ctx, tw, file, info)
	})
	if err == nil {
		err = tw.Close()
	}
	if err == nil {
		err = gz.Close()
	}
	if err == nil {
		err = tmp.Sync()
	}
	if err != nil {
		cleanup()
		return nil, 0, "", "", func() {}, err
	}
	size, err := tmp.Seek(0, io.SeekEnd)
	if err == nil {
		_, err = tmp.Seek(0, io.SeekStart)
	}
	if err != nil {
		cleanup()
		return nil, 0, "", "", func() {}, err
	}
	return tmp, size, "application/gzip", filepath.Base(path) + ".tar.gz", cleanup, nil
}

type exportBoundWriter struct {
	out       io.Writer
	remaining int64
}

func (w *exportBoundWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, invalid("compressed export exceeds byte bound")
	}
	n, err := w.out.Write(p)
	w.remaining -= int64(n)
	return n, err
}

// exportSnapshotSource allows the metadata check and copy to inspect the same
// descriptor. The interface also permits deterministic concurrent-write tests.
type exportSnapshotSource interface {
	io.Reader
	Stat() (os.FileInfo, error)
}
type exportContextReader struct {
	ctx    context.Context
	source io.Reader
}

func (r exportContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.source.Read(p)
}
func copyExportSnapshot(ctx context.Context, out io.Writer, in exportSnapshotSource, before os.FileInfo) error {
	n, err := io.Copy(out, io.LimitReader(exportContextReader{ctx, in}, before.Size()+1))
	if err != nil {
		return err
	}
	after, err := in.Stat()
	if err != nil {
		return err
	}
	if n != before.Size() || !os.SameFile(before, after) || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) || after.Mode() != before.Mode() {
		return contracts.NewError(contracts.ErrConflict, "export source changed during snapshot")
	}
	return ctx.Err()
}
