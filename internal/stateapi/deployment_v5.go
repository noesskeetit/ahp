package stateapi

import (
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/transport"
	"net/http"
)

// Only the administrative route admits these methods, with an additional
// deployment-transfer capability and a Store-configured custody coordinator.
func (h *Handler) exportDeploymentV5(w http.ResponseWriter, r *http.Request) {
	sc, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		MigrationID string `json:"migration_id"`
		NativeBuild string `json:"native_build"`
	}
	if err = transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	image, err := h.store.ExportDeploymentV5(r.Context(), state.DeploymentUnit{Tenant: sc.Tenant, Deployment: sc.Deployment}, in.MigrationID, in.NativeBuild)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, image)
}
func (h *Handler) importDeploymentV5(w http.ResponseWriter, r *http.Request) {
	sc, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		Archive     state.DeploymentV5 `json:"archive"`
		NativeBuild string             `json:"native_build"`
	}
	if err = transport.ReadJSON(w, r, &in, state.MaxDeploymentV3Bytes+4096); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	receipt, err := h.store.ImportDeploymentV5(r.Context(), state.DeploymentUnit{Tenant: sc.Tenant, Deployment: sc.Deployment}, in.Archive, in.NativeBuild)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, receipt)
}
