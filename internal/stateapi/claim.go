package stateapi

import (
	"context"
	"net/http"
	"time"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// cleanupTimeout bounds lease-release cleanup even when the request context
// is already canceled.
const cleanupTimeout = 5 * time.Second

// claimRun is the deployment-only route: identity comes exclusively from the
// verified workload certificate (transport.Workload), no token and no subject
// is accepted. A granted lease is always paired with a freshly issued task
// token; if issuing is denied the lease is released as canceled and the denial
// is returned, so an unauthorized run is never handed out tokenless.
func (h *Handler) claimRun(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"stateapi: no verified principal on this request"))
		return
	}
	var in claimRunRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	d, err := ttl(in.TTLms)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	lease, err := h.store.ClaimRunWithFilter(r.Context(), p.Scope.Tenant, p.Scope.Deployment, in.Worker, d,
		state.ClaimFilter{RunID: in.RunID, PayloadType: in.PayloadType})
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if lease == nil {
		transport.WriteJSON(w, http.StatusOK, claimResponse{Lease: nil})
		return
	}
	token, err := h.issue(r.Context(), lease.Scope, lease.Run.TaskID)
	if err == nil && token == "" {
		err = contracts.NewError(contracts.ErrForbidden,
			"stateapi: task issuer returned an empty token")
	}
	if err != nil {
		// The grant is undone as canceled before the denial surfaces, on a
		// bounded context so cleanup survives a canceled request.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), cleanupTimeout)
		defer cancel()
		// A canceled run owes its reason too (DEF-RELEASE-REASON-1): State refuses an
		// unexplained terminal through either door, and the honest sentence here is the
		// one the lines above already say - the grant was undone because no token came.
		_ = h.store.ReleaseWithReasonCause(ctx, lease.Scope, lease.Run.ID,
			lease.Worker, lease.Epoch, "canceled", "",
			"stateapi: task grant could not be issued")
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, claimResponse{Lease: lease, TaskToken: token})
}
