package identityapi

import (
	"net/http"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func (h *api) cancelChild(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok || r.PathValue("task") == "" || r.PathValue("task") != p.Claims.Task {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: parent must equal the authenticated task"))
		return
	}
	var body struct {
		Deployment string `json:"deployment,omitempty"`
	}
	if r.ContentLength != 0 && !h.readBody(w, r, &body) {
		return
	}
	if err := h.cfg.Store.CancelDescendantInDeployment(r.Context(), p.Scope, p.Claims.Task, r.PathValue("child"), body.Deployment); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
