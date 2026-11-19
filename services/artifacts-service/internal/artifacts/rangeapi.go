package artifacts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// APIHandler serves the artifact HTTP surface (B114/B116):
//   - GET  /v1/artifacts/{id}     — range-capable read (206 or 200)
//   - POST /v1/artifacts/exports  — export upload (multipart → seal → card)
type APIHandler struct {
	store *Store
	task  func(http.Handler) http.Handler
}

func NewAPIHandler(store *Store, verifier *auth.Verifier, authorize transport.Authorize) *APIHandler {
	return &APIHandler{
		store: store,
		task:  transport.Task(verifier, "ahp", "artifacts", authorize),
	}
}

func (h *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.task(http.HandlerFunc(h.route)).ServeHTTP(w, r)
}

func (h *APIHandler) route(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/exports"):
		h.handleExport(w, r)
	case r.Method == http.MethodGet:
		h.handleRange(w, r)
	default:
		http.Error(w, `{"code":"NOT_FOUND","message":"not found"}`, http.StatusNotFound)
	}
}

func (h *APIHandler) handleRange(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/artifacts/")
	if id == "" || id == r.URL.Path {
		http.Error(w, `{"code":"INVALID","message":"missing artifact id"}`, http.StatusBadRequest)
		return
	}
	principal, ok := transport.FromContext(r.Context())
	if !ok {
		http.Error(w, `{"code":"FORBIDDEN","message":"no principal"}`, http.StatusForbidden)
		return
	}
	scope := principal.Scope
	var offset, count int64
	isRange := false
	if rh := r.Header.Get("Range"); rh != "" {
		start, end, ok := parseRangeHeader(rh)
		if !ok {
			http.Error(w, `{"code":"INVALID","message":"bad range"}`, http.StatusRequestedRangeNotSatisfiable)
			return
		}
		offset = start
		if end < 0 {
			count = 0
		} else {
			count = end - start + 1
		}
		isRange = true
	}
	rc, ref, err := h.store.OpenRange(r.Context(), scope, id, offset, count)
	if err != nil {
		switch contracts.Code(err) {
		case contracts.ErrNotFound:
			http.Error(w, `{"code":"NOT_FOUND","message":"artifact not found"}`, http.StatusNotFound)
		case contracts.ErrForbidden:
			http.Error(w, `{"code":"FORBIDDEN","message":"forbidden"}`, http.StatusForbidden)
		case contracts.ErrInvalid:
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", ref.Size))
			http.Error(w, `{"code":"INVALID","message":"range not satisfiable"}`, http.StatusRequestedRangeNotSatisfiable)
		default:
			http.Error(w, `{"code":"INTERNAL","message":"internal error"}`, http.StatusInternalServerError)
		}
		return
	}
	defer rc.Close()
	if isRange {
		end := offset + count - 1
		if count == 0 {
			end = ref.Size - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, end, ref.Size))
		w.Header().Set("Content-Length", fmt.Sprint(end-offset+1))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", fmt.Sprint(ref.Size))
		w.WriteHeader(http.StatusOK)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = io.Copy(w, rc)
}

func (h *APIHandler) handleExport(w http.ResponseWriter, r *http.Request) {
	mr, err := r.MultipartReader()
	if err != nil {
		http.Error(w, `{"code":"INVALID","message":"not multipart"}`, http.StatusBadRequest)
		return
	}
	type fields struct {
		taskID, computerID, tenant, deployment, subject, filename, contentType string
		content                                                                io.Reader
	}
	var f fields
	readStr := func(r io.Reader) string {
		b, _ := io.ReadAll(io.LimitReader(r, 4096))
		return strings.TrimSpace(string(b))
	}
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, `{"code":"INVALID","message":"bad multipart"}`, http.StatusBadRequest)
			return
		}
		switch part.FormName() {
		case "task_id":
			f.taskID = readStr(part)
		case "computer_id":
			f.computerID = readStr(part)
		case "tenant":
			f.tenant = readStr(part)
		case "deployment":
			f.deployment = readStr(part)
		case "subject":
			f.subject = readStr(part)
		case "content":
			f.filename = part.FileName()
			if ct := part.Header.Get("Content-Type"); ct != "" {
				f.contentType = ct
			}
			f.content = part
		}
		if f.content != nil {
			break
		}
		_ = part.Close()
	}
	if f.content == nil {
		http.Error(w, `{"code":"INVALID","message":"missing content"}`, http.StatusBadRequest)
		return
	}
	principal, ok := transport.FromContext(r.Context())
	if !ok {
		http.Error(w, `{"code":"FORBIDDEN","message":"no principal"}`, http.StatusForbidden)
		return
	}
	scope := principal.Scope
	body, err := io.ReadAll(io.LimitReader(f.content, MaxSize+1))
	if err != nil {
		http.Error(w, `{"code":"INVALID","message":"read failed"}`, http.StatusBadRequest)
		return
	}
	if int64(len(body)) > MaxSize {
		http.Error(w, `{"code":"INVALID","message":"too large"}`, http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	key := fmt.Sprintf("export/%s/%s", f.computerID, f.filename)
	sum := sha256.Sum256(body)
	up, err := h.store.BeginUpload(ctx, scope, key, UploadRequest{
		Name:   f.filename,
		MIME:   f.contentType,
		SHA256: hex.EncodeToString(sum[:]),
		Size:   int64(len(body)),
	})
	if err != nil {
		http.Error(w, `{"code":"INVALID","message":"begin upload: "+err.Error()}`, http.StatusBadRequest)
		return
	}
	putReq, _ := http.NewRequest(http.MethodPut, up.PresignedURL, strings.NewReader(string(body)))
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		http.Error(w, `{"code":"INTERNAL","message":"upload failed"}`, http.StatusInternalServerError)
		return
	}
	_, _ = io.Copy(io.Discard, putResp.Body)
	_ = putResp.Body.Close()
	if putResp.StatusCode/100 != 2 {
		http.Error(w, `{"code":"INTERNAL","message":"object store rejected"}`, http.StatusInternalServerError)
		return
	}
	ref, err := h.store.Seal(ctx, scope, up.ID)
	if err != nil {
		http.Error(w, `{"code":"CONFLICT","message":"seal failed"}`, http.StatusConflict)
		return
	}
	card := map[string]any{
		"artifact_id": ref.ID,
		"version":     ref.Version,
		"digest":      ref.SHA256,
		"size_bytes":  ref.Size,
		"sealed_at":   time.Now().UTC().Format(time.RFC3339),
		"name":        ref.Name,
		"mime":        ref.MIME,
		"labels":      ref.Labels,
		"provenance": map[string]string{
			"task_id":     f.taskID,
			"computer_id": f.computerID,
			"tenant":      scope.Tenant,
			"subject":     scope.Subject,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(card)
}

func parseRangeHeader(h string) (start, end int64, ok bool) {
	if !strings.HasPrefix(h, "bytes=") {
		return 0, 0, false
	}
	spec := h[6:]
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, false
	}
	startStr, endStr := spec[:dash], spec[dash+1:]
	if startStr != "" {
		start = parseDigits(startStr)
		if start < 0 {
			return 0, 0, false
		}
	}
	if endStr == "" {
		return start, -1, true
	}
	end = parseDigits(endStr)
	if end < start {
		return 0, 0, false
	}
	return start, end, true
}

func parseDigits(s string) int64 {
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return -1
		}
		n = n*10 + int64(c-'0')
	}
	return n
}
