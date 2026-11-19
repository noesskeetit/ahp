package stateapi

import (
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"net/http"
)

func (h *Handler) putCommonRoot(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "task principal required"))
		return
	}
	var in struct {
		Fence state.Fence             `json:"fence"`
		Root  state.CommonRootRequest `json:"root"`
	}
	if e := transport.ReadJSON(w, r, &in, maxBodyBytes); e != nil {
		transport.WriteError(w, r, e)
		return
	}
	run, e := h.store.GetRun(r.Context(), p.Scope, in.Fence.RunID)
	if e != nil {
		transport.WriteError(w, r, e)
		return
	}
	thread, e := h.store.GetThread(r.Context(), p.Scope, run.ThreadID)
	if e != nil {
		transport.WriteError(w, r, e)
		return
	}
	if p.Claims.Task != run.TaskID || p.Claims.Context != thread.ContextID {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "common root is outside task/context"))
		return
	}
	out, e := h.store.PutCommonRoot(r.Context(), p.Scope, in.Fence, in.Root)
	if e != nil {
		transport.WriteError(w, r, e)
		return
	}
	transport.WriteJSON(w, http.StatusOK, out)
}
func (h *Handler) pruneCommonHistory(w http.ResponseWriter, r *http.Request) {
	scope, e := principal(r)
	if e != nil {
		transport.WriteError(w, r, e)
		return
	}
	var req state.CommonPruneRequest
	if e := transport.ReadJSON(w, r, &req, maxBodyBytes); e != nil {
		transport.WriteError(w, r, e)
		return
	}
	out, e := h.store.PruneCommonHistory(r.Context(), scope, req)
	if e != nil {
		transport.WriteError(w, r, e)
		return
	}
	transport.WriteJSON(w, http.StatusOK, out)
}
