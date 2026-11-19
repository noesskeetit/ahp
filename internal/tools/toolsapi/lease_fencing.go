package toolsapi

import (
	"net/http"

	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type toolLeaseInput struct {
	SessionID string `json:"session_id"`
	Epoch     int64  `json:"lease_epoch"`
	Proof     string `json:"tool_lease_proof"`
}

func (h *Handler) verifiedToolLease(caller auth.Claims, in toolLeaseInput) (tools.ToolLease, error) {
	expires, err := h.leaseProof.VerifyToolLease(in.Proof, caller, in.SessionID, in.Epoch)
	if err != nil {
		return tools.ToolLease{}, err
	}
	return tools.ToolLease{SessionID: in.SessionID, Epoch: in.Epoch, ExpiresAt: expires, ContextID: caller.Context}, nil
}

func (h *Handler) fenceAndLookup(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "toolsapi: verified task required"))
		return
	}
	var in struct {
		Key string `json:"key"`
		toolLeaseInput
	}
	if err := transport.ReadJSON(w, r, &in, maxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	lease, err := h.verifiedToolLease(p.Claims, in.toolLeaseInput)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	result, err := h.svc.FenceAndLookup(r.Context(), p.Scope, in.Key, lease)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	switch result.Status {
	case "SAFE_TO_RETRY":
		transport.WriteJSON(w, http.StatusNotFound, map[string]string{"code": result.Status})
	case "IN_FLIGHT":
		transport.WriteJSON(w, http.StatusAccepted, result)
	case "OUTCOME_UNKNOWN":
		transport.WriteError(w, r, contracts.NewReasoned(contracts.ErrConflict, result.Status, "tool outcome requires reconciliation"))
	default:
		transport.WriteJSON(w, http.StatusOK, result)
	}
}
