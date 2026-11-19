package stateapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const AdminCapability = "state:admin"
const TenantDeleteCapability = "state:tenant-delete"
const DeploymentTransferCapability = "state:deployment-transfer"

// AdminConfig permits the internal control plane to act for a subject inside
// its own tenant. Authorize must verify both the human's administrative grant
// and that Deployment is an approved control-plane workload.
type AdminConfig struct {
	Audience  string
	Authorize transport.Authorize
}

// NewWithAdmin adds POST /v1/state-admin/{Operation}. It requires verified
// mTLS and a human administrative credential. Task credentials are refused
// before calling Authorize, even if a caller misconfigures its policy callback.
func NewWithAdmin(store *state.Store, verifier *auth.Verifier, authorize transport.Authorize, issue TaskIssuer, cfg AdminConfig) (http.Handler, error) {
	if cfg.Audience == "" || cfg.Authorize == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "stateapi: admin audience and authorizer are required")
	}
	base, err := New(store, verifier, authorize, issue)
	if err != nil {
		return nil, err
	}
	h := base.(*Handler)
	h.adminAuthorize = cfg.Authorize
	h.admin = transport.TaskOrUser(verifier, audience, cfg.Audience, AdminCapability, func(rctx context.Context, claims auth.Claims, capability string) error {
		if claims.Kind != auth.KindHuman {
			return contracts.NewError(contracts.ErrForbidden, "stateapi: task credentials cannot perform administrative operations")
		}
		return cfg.Authorize(rctx, claims, capability)
	})
	return h, nil
}

func adminOperation(op string) bool {
	switch op {
	case "PruneCommonHistory", "SetLegalhold", "RemoveLegalhold", "DeleteSubject", "ExportThread", "ControlDeleteSubject", "ControlDeleteTenant",
		"PruneCheckpointHistory", "ExportScopeV2", "ImportScopeV2", "ExportDeploymentV3", "ImportDeploymentV3", "ExportDeploymentV4", "ImportDeploymentV4", "ExportDeploymentV5", "ImportDeploymentV5", "ExportDeploymentV6", "ImportDeploymentV6", "ExportDeploymentV7", "ImportDeploymentV7":
		return true
	}
	return false
}

func (h *Handler) serveAdmin(w http.ResponseWriter, r *http.Request) {
	op := strings.TrimPrefix(r.URL.Path, "/v1/state-admin/")
	if !adminOperation(op) {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "stateapi: unknown admin operation"))
		return
	}
	if h.admin == nil {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "stateapi: admin route is not configured"))
		return
	}
	h.admin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := transport.FromContext(r.Context())
		if !ok || p.Claims.Kind != auth.KindHuman {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "stateapi: administrative human required"))
			return
		}
		if op == "ControlDeleteTenant" {
			if err := h.adminAuthorize(r.Context(), p.Claims, TenantDeleteCapability); err != nil {
				transport.WriteError(w, r, err)
				return
			}
		}
		deploymentTransfer := strings.HasPrefix(op, "ExportDeploymentV") || strings.HasPrefix(op, "ImportDeploymentV")
		if deploymentTransfer {
			if err := h.adminAuthorize(r.Context(), p.Claims, DeploymentTransferCapability); err != nil {
				transport.WriteError(w, r, err)
				return
			}
		}
		var in struct {
			Target struct {
				Deployment string `json:"deployment"`
				Subject    string `json:"subject"`
			} `json:"target"`
			Input json.RawMessage `json:"input"`
		}
		limit := int64(maxBodyBytes)
		if op == "ImportScopeV2" {
			limit = state.MaxScopeV2Bytes + 4096
		}
		if strings.HasPrefix(op, "ImportDeploymentV") {
			limit = state.MaxDeploymentV3Bytes + 4096
		}
		if err := transport.ReadJSON(w, r, &in, limit); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		target := contracts.Scope{Tenant: p.Scope.Tenant, Deployment: in.Target.Deployment, Subject: in.Target.Subject}
		if deploymentTransfer {
			if in.Target.Subject != "" {
				transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "deployment transfer cannot select one subject"))
				return
			}
			target.Subject = p.Claims.Subject
		}
		if op == "ControlDeleteTenant" {
			target = p.Scope
			target.Subject = p.Claims.Subject
		}
		if err := target.Validate(); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		if len(in.Input) == 0 {
			in.Input = json.RawMessage(`{}`)
		}
		clone := r.Clone(transport.WithPrincipal(r.Context(), transport.Principal{Claims: p.Claims, Scope: target}))
		clone.Body = io.NopCloser(bytes.NewReader(in.Input))
		clone.ContentLength = int64(len(in.Input))
		switch op {
		case "PruneCommonHistory":
			h.pruneCommonHistory(w, clone)
		case "ExportDeploymentV6":
			h.exportDeploymentCommon(w, clone, 6)
		case "ImportDeploymentV6":
			h.importDeploymentCommon(w, clone, 6)
		case "ExportDeploymentV7":
			h.exportDeploymentCommon(w, clone, 7)
		case "ImportDeploymentV7":
			h.importDeploymentCommon(w, clone, 7)
		case "ExportDeploymentV5":
			h.exportDeploymentV5(w, clone)
		case "ImportDeploymentV5":
			h.importDeploymentV5(w, clone)
		case "ExportDeploymentV4":
			h.exportDeploymentV4(w, clone)
		case "ImportDeploymentV4":
			h.importDeploymentV4(w, clone)
		case "ExportDeploymentV3":
			h.exportDeploymentV3(w, clone)
		case "ImportDeploymentV3":
			h.importDeploymentV3(w, clone)
		case "ExportScopeV2":
			h.exportScopeV2(w, clone)
		case "ImportScopeV2":
			h.importScopeV2(w, clone)
		case "ControlDeleteTenant":
			h.controlDeleteTenant(w, clone)
		case "ControlDeleteSubject":
			h.controlDeleteSubject(w, clone)
		case "SetLegalhold":
			h.setLegalhold(w, clone)
		case "RemoveLegalhold":
			h.removeLegalhold(w, clone)
		case "DeleteSubject":
			h.deleteSubject(w, clone)
		case "ExportThread":
			h.exportThread(w, clone)
		case "PruneCheckpointHistory":
			h.pruneCheckpointHistory(w, clone)
		}
	})).ServeHTTP(w, r)
}
