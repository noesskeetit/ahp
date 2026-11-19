package registry

// The Coordinator chooses an installation, never the version or runtime URL.
// Pin the authenticated run's choice before dispatch. This is one pin per turn,
// not one immutable version for the entire conversation: new turns can take a
// rollout, while a retry/recovery of an existing turn keeps its original config.

import (
	"net/http"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func (h *HTTPHandler) pinAgentTurn(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	principal, ok := transport.FromContext(r.Context())
	if !ok || principal.Claims.Kind != auth.KindTask || principal.Claims.Task == "" || principal.Claims.Context == "" {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "agent turn requires an authenticated run task and conversation"))
		return
	}
	var in struct {
		InstallationID string `json:"installation_id"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// Identity task names are deployment-scoped, whereas Registry's historical
	// task_pins key is tenant-wide. Include the complete verified scope to avoid
	// collisions between two Dots using the same caller-owned message key.
	pin := contracts.OperationKey(scope, principal.Claims.Task, "registry.agent-turn", principal.Claims.Context)
	if _, err := h.store.PinForSubject(r.Context(), scope, in.InstallationID, pin); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	target, err := h.store.ResolveConfig(r.Context(), scope.Tenant, in.InstallationID, pin)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"target": target})
}
