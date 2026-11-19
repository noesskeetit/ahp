package identityapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type delegateTargetBody struct {
	delegateBody
	ParentDeployment string `json:"parent_deployment"`
	Deployment       string `json:"deployment"`
}

func (h *api) delegateToTarget(w http.ResponseWriter, r *http.Request) {
	broker, ok := transport.FromContext(r.Context())
	if !ok || !h.cfg.workloadAllowed(broker.Scope, h.cfg.DelegatorWorkloadIDs, h.cfg.DelegatorDeployments) || h.cfg.ResolveDelegationTarget == nil {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: target delegation requires an authorized broker and target policy"))
		return
	}
	var body delegateTargetBody
	if !h.readBody(w, r, &body) {
		return
	}
	// This field is only a workload-verification hint. Signed claims must match
	// it, and the certificate tenant remains mandatory across both deployments.
	parentScope := contracts.Scope{Tenant: broker.Scope.Tenant, Deployment: body.ParentDeployment, Subject: "verification"}
	if err := parentScope.Validate(); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	bearer := strings.Fields(r.Header.Get("Authorization"))
	if len(bearer) != 2 || !strings.EqualFold(bearer[0], "Bearer") {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: parent task bearer required"))
		return
	}
	claims, err := h.cfg.Verifier.Verify(bearer[1], h.cfg.Audience, auth.KindTask, auth.WorkloadID(parentScope))
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if claims.Tenant != broker.Scope.Tenant || claims.Task != r.PathValue("task") {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: parent task and tenant must match verified authority"))
		return
	}
	if err := h.cfg.Store.Authorize(r.Context(), claims, "identity:delegate"); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	target, err := h.cfg.ResolveDelegationTarget(r.Context(), claims.Scope(), body.Deployment)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	ttl, err := time.ParseDuration(body.TTLOut)
	if err != nil {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "identityapi: ttl is required and must be a valid duration"))
		return
	}
	grant, err := h.cfg.Store.DelegateToTarget(r.Context(), claims.Scope(), claims.Task, identity.DelegateRequest{
		Task: body.Task, Context: body.Context, Deployment: body.Deployment, Capabilities: body.Capabilities, TTL: ttl,
		MaxDepth: body.MaxDepth, MaxFanout: body.MaxFanout, MaxActive: body.MaxActive, AllowSelf: body.AllowSelf,
	}, target)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.cfg.Store.MintTask(r.Context(), grant.Scope(), grant.Task, h.cfg.Signer, h.cfg.Audience, 0)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusCreated, delegateResponse{Grant: grant, Token: token})
}
