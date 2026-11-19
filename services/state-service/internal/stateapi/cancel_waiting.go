package stateapi

import (
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"net/http"
)

func (h *Handler) cancelWaitingRun(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "verified principal required"))
		return
	}
	var in getRunRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.CancelWaitingRun(r.Context(), p.Scope, in.RunID, p.Claims.Context); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okResponse{OK: true})
}
