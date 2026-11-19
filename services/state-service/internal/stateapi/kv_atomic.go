package stateapi

import (
	"net/http"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type putKVBatchRequest struct {
	Fence state.Fence          `json:"fence"`
	Batch state.KVBatchRequest `json:"batch"`
}

type getKVBatchRequest struct {
	Keys []state.KVRead `json:"keys"`
}

func (h *Handler) putKVBatch(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in putKVBatchRequest
	// Bound the complete envelope; each item keeps the store's existing KV bound.
	if err = transport.ReadJSON(w, r, &in, 4<<20); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	out, err := h.store.PutKVBatch(r.Context(), scope, in.Fence, in.Batch)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, out)
}

func (h *Handler) getKVBatch(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in getKVBatchRequest
	if err = transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	items, err := h.store.GetKVBatch(r.Context(), scope, in.Keys)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}
