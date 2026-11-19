package artifactsapi

import (
	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"net/http"
)

func (h *Handler) transferIdentity(w http.ResponseWriter, r *http.Request) (artifacts.TransferIdentity, bool) {
	p, ok := transport.FromContext(r.Context())
	if !ok || !h.store.TransferEnabled() {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "checkpoint transfer requires live authority"))
		return artifacts.TransferIdentity{}, false
	}
	return artifacts.TransferIdentity{Scope: p.Scope, Binding: artifacts.Binding{ContextID: p.Claims.Context, TaskID: p.Claims.Task}, GrantID: p.Claims.Grant}, true
}
func (h *Handler) transferGrant(w http.ResponseWriter, r *http.Request) {
	p, ok := h.transferIdentity(w, r)
	if !ok {
		return
	}
	var in artifacts.TransferGrantRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	out, err := h.store.MintTransferGrant(r.Context(), p, in)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, out)
}
func (h *Handler) copyFromTransferGrant(w http.ResponseWriter, r *http.Request) {
	p, ok := h.transferIdentity(w, r)
	if !ok {
		return
	}
	var in struct {
		Grant string `json:"grant"`
		Key   string `json:"key"`
	}
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	out, err := h.store.CopyFromTransferGrant(r.Context(), p, in.Grant, in.Key)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, out)
}
