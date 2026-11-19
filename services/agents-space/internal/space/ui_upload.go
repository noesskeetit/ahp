package space

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const UIUploadPath = "/u-ahp/v1/artifacts/upload"

// UploadProxyConfig configures the authenticated upload metadata bridge.
// operation=BeginUpload/Seal serves direct browser PUT uploads. The original
// body-upload route remains a bounded compatibility path for older clients.
type UploadProxyConfig struct {
	ArtifactsURL string
	Client       *http.Client
	TempDir      string
	MaxBytes     int64
	Timeout      time.Duration
}

type uploadProxy struct {
	base             string
	machine, objects *http.Client
	tempDir          string
	maxBytes         int64
	timeout          time.Duration
	slots            chan struct{}
}

func NewUploadProxy(c UploadProxyConfig) (http.Handler, error) {
	base, err := artifactsOrigin("the upload proxy", c.ArtifactsURL)
	if err != nil {
		return nil, err
	}
	if c.Client == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "upload requires Space workload client")
	}
	if c.MaxBytes <= 0 || c.MaxBytes > artifacts.MaxSize {
		c.MaxBytes = artifacts.MaxSize
	}
	if c.Timeout <= 0 {
		c.Timeout = 2 * time.Minute
	}
	machine := *c.Client
	machine.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	// Deliberately separate transport: never present workload certificates or task
	// authorization to the object store named by a trusted BeginUpload response.
	objects := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone(), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &uploadProxy{base: base, machine: &machine, objects: objects, tempDir: c.TempDir, maxBytes: c.MaxBytes, timeout: c.Timeout, slots: make(chan struct{}, 4)}, nil
}

func (p *uploadProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodPost {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "unknown upload route"))
		return
	}
	token, ok := ScopedTaskFromContext(r.Context())
	if !ok || token == "" {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "upload requires an exchanged task token"))
		return
	}
	if op := r.URL.Query().Get("operation"); op != "" {
		p.directUpload(w, r, token, op)
		return
	}
	name, key := r.URL.Query().Get("name"), r.URL.Query().Get("key")
	if name == "" || len(name) > 255 || path.Base(name) != name || strings.ContainsAny(name, "\\\r\n\x00") || key == "" || len(key) > 256 {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "upload needs a filename and a stable operation key"))
		return
	}
	select {
	case p.slots <- struct{}{}:
		defer func() { <-p.slots }()
	default:
		transport.WriteError(w, r, contracts.NewError(contracts.ErrConflict, "upload capacity is busy; retry"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), p.timeout)
	defer cancel()
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Now().Add(p.timeout))
	defer controller.SetReadDeadline(time.Time{})
	file, err := os.CreateTemp(p.tempDir, "ahp-human-upload-*")
	if err != nil {
		transport.WriteError(w, r, contracts.NewError("", "upload temporary storage unavailable"))
		return
	}
	defer os.Remove(file.Name())
	defer file.Close()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(file, h), http.MaxBytesReader(w, r.Body, p.maxBytes))
	if err != nil || n == 0 {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "upload is empty, incomplete, or exceeds the size limit"))
		return
	}
	digest := hex.EncodeToString(h.Sum(nil))
	mime := r.Header.Get("Content-Type")
	if mime == "" {
		mime = "application/octet-stream"
	}
	var upload artifacts.Upload
	if err = p.call(ctx, token, "BeginUpload", map[string]any{"key": key, "name": name, "mime": mime, "sha256": digest, "size": n, "labels": map[string]any{}}, &upload); err != nil {
		writeRefusal(w, r, err)
		return
	}
	u, err := url.Parse(upload.PresignedURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" || upload.ID == "" {
		transport.WriteError(w, r, contracts.NewError("", "invalid storage upload response"))
		return
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		transport.WriteError(w, r, contracts.NewError("", "upload spool unavailable"))
		return
	}
	put, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), file)
	if err != nil {
		transport.WriteError(w, r, contracts.NewError("", "upload request unavailable"))
		return
	}
	put.ContentLength = n
	response, err := p.objects.Do(put)
	if err != nil {
		transport.WriteError(w, r, contracts.NewError("", "object upload failed"))
		return
	}
	response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		transport.WriteError(w, r, contracts.NewError("", "object upload refused"))
		return
	}
	var ref artifacts.ArtifactRef
	if err = p.call(ctx, token, "Seal", map[string]string{"id": upload.ID}, &ref); err != nil {
		writeRefusal(w, r, err)
		return
	}
	if ref.ID != upload.ID || ref.SHA256 != digest || ref.Size != n || ref.Version == "" {
		transport.WriteError(w, r, contracts.NewError("", "sealed upload metadata mismatch"))
		return
	}
	transport.WriteJSON(w, http.StatusOK, ref)
}

func (p *uploadProxy) call(ctx context.Context, token, op string, body, out any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.base+artifactsOperationPath+op, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	setServiceRequest(req, token, "application/json")
	res, err := p.machine.Do(req)
	if err != nil {
		return contracts.NewError("", "artifact upload service unavailable")
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return artifactsRefusal(res)
	}
	if err = json.NewDecoder(io.LimitReader(res.Body, maxGrantBodyBytes)).Decode(out); err != nil {
		return contracts.NewError("", "invalid artifact upload response")
	}
	return nil
}
