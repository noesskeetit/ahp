package space

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// directUpload forwards bounded metadata only. Object bytes travel from the
// browser to a short-lived PUT capability; the exchanged task stays here.
func (p *uploadProxy) directUpload(w http.ResponseWriter, r *http.Request, token, op string) {
	ctx, cancel := context.WithTimeout(r.Context(), p.timeout)
	defer cancel()
	r = r.WithContext(ctx)
	switch op {
	case "BeginUpload":
		var in struct {
			Key    string `json:"key"`
			Name   string `json:"name"`
			MIME   string `json:"mime"`
			SHA256 string `json:"sha256"`
			Size   int64  `json:"size"`
		}
		if err := transport.ReadJSON(w, r, &in, 4096); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		if in.Size <= 0 || in.Size > p.maxBytes {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "upload size exceeds limit"))
			return
		}
		var up artifacts.Upload
		if err := p.call(r.Context(), token, op, in, &up); err != nil {
			writeRefusal(w, r, err)
			return
		}
		u, err := url.Parse(up.PresignedURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" || up.ID == "" {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrConflict, "invalid upload capability"))
			return
		}
		// The object key is implementation detail; only the PUT URL is needed.
		up.Object = ""
		transport.WriteJSON(w, http.StatusOK, up)
	case "Seal", "Readiness":
		var in struct {
			ID string `json:"id"`
		}
		if err := transport.ReadJSON(w, r, &in, 4096); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		if strings.TrimSpace(in.ID) == "" {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "artifact id required"))
			return
		}
		var out any
		if err := p.call(r.Context(), token, op, in, &out); err != nil {
			writeRefusal(w, r, err)
			return
		}
		transport.WriteJSON(w, http.StatusOK, out)
	default:
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "unknown upload operation"))
	}
}
