package stateapi

import (
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/transport"
	"net/http"
)

// These routes are admitted ONLY by serveAdmin (human + mTLS + IAM), never by
// the task data route. Trusted coordinator configuration is required in Store.
func (h *Handler) exportScopeV2(w http.ResponseWriter, r *http.Request) {
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
	e, err := h.store.ExportScopeV2(r.Context(), sc, in.MigrationID, in.NativeBuild)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, e)
}
func (h *Handler) importScopeV2(w http.ResponseWriter, r *http.Request) {
	sc, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		Archive     state.ScopeV2 `json:"archive"`
		NativeBuild string        `json:"native_build"`
	}
	if err = transport.ReadJSON(w, r, &in, state.MaxScopeV2Bytes+4096); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	receipt, err := h.store.ImportScopeV2(r.Context(), sc, in.Archive, in.NativeBuild)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, receipt)
}
