package artifactsapi

import (
	"net/http"

	"github.com/ahp-platform/ahp/pkg/transport"
)

func (h *Handler) process(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in idRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	out, err := h.store.Process(ctx, scope, in.ID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, out)
}
func (h *Handler) readTextChunk(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		ID            string `json:"id"`
		Version       string `json:"version"`
		Offset        int64  `json:"offset"`
		Limit         int    `json:"limit"`
		ContextBudget int    `json:"context_budget"`
	}
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	out, err := h.store.ReadTextChunk(ctx, scope, in.ID, in.Version, in.Offset, in.Limit, in.ContextBudget)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, out)
}
func (h *Handler) searchText(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		ID      string `json:"id"`
		Version string `json:"version"`
		Query   string `json:"query"`
	}
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	matched, err := h.store.SearchText(ctx, scope, in.ID, in.Version, in.Query)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"id": in.ID, "version": in.Version, "matched": matched})
}
