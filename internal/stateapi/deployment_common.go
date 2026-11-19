package stateapi

import (
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/transport"
	"net/http"
)

// Only the administrative route admits these methods, with an additional
// deployment-transfer capability and a Store-configured custody coordinator.
func (h *Handler) exportDeploymentCommon(w http.ResponseWriter, r *http.Request, version int) {
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
	export := h.store.ExportDeploymentV6
	if version == 7 {
		export = h.store.ExportDeploymentV7
	}
	image, err := export(r.Context(), state.DeploymentUnit{Tenant: sc.Tenant, Deployment: sc.Deployment}, in.MigrationID, in.NativeBuild)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, image)
}
func (h *Handler) importDeploymentCommon(w http.ResponseWriter, r *http.Request, version int) {
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
	importImage := h.store.ImportDeploymentV6
	if version == 7 {
		importImage = h.store.ImportDeploymentV7
	}
	receipt, err := importImage(r.Context(), state.DeploymentUnit{Tenant: sc.Tenant, Deployment: sc.Deployment}, in.Archive, in.NativeBuild)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, receipt)
}
