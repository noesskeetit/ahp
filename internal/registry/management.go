package registry

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const AdminCapability = "registry:admin"

// SecurityVerdictCapability is the capability consulted on the security
// ("проверка ИБ") clearance route. It is intentionally distinct from
// AdminCapability: recording a clearance is a narrower, higher-authority act
// reserved for a live human security administrator, so the authorize callback
// can grant it without granting the whole administrative surface -- and, unlike
// ordinary registry administration, it is never granted to a task.
const SecurityVerdictCapability = "registry:security-verdict"

// AdministrativeOperation identifies tenant-wide writes. This list is also used
// by the AS edge to preserve a human credential for current IAM authorization.
func AdministrativeOperation(op string) bool {
	switch op {
	case "AdminListSkills", "AdminGetSkill", "AdminAccess", "PutConfig", "PutModelRoute", "DeleteModelRoute", "PutSkill":
		return true
	}
	return false
}

// AttachManagementAuthority installs current human/session authorization at the
// owner. Task callers still need the explicit administrative capability.
func AttachManagementAuthority(handler http.Handler, verifier *auth.Verifier, audience string, authorize transport.Authorize) error {
	h, ok := handler.(*HTTPHandler)
	if !ok || verifier == nil || authorize == nil {
		return invalid("management authority required")
	}
	h.admin = transport.TaskOrUser(verifier, registryAudience, audience, AdminCapability, authorize)
	return nil
}

func (h *HTTPHandler) profile(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		ExpectedRevision int64           `json:"expected_revision"`
		Data             json.RawMessage `json:"data"`
	}
	if err = transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var p Profile
	if r.URL.Path == "/v1/registry/PutProfile" {
		p, err = h.store.PutProfile(r.Context(), scope, in.ExpectedRevision, in.Data)
	} else {
		p, err = h.store.GetProfile(r.Context(), scope)
	}
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"profile": p})
}
func (h *HTTPHandler) pinDot(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		Key       string `json:"key"`
		ContextID string `json:"context_id"`
	}
	if err = transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	out, err := h.store.PinDotConfig(r.Context(), scope, in.Key, in.ContextID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"snapshot": out})
}
func (h *HTTPHandler) modelRoute(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		Route            string          `json:"route"`
		ExpectedRevision int64           `json:"expected_revision"`
		Document         json.RawMessage `json:"document"`
	}
	if err = transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var result any
	switch r.URL.Path {
	case "/v1/registry/PutModelRoute":
		result, err = h.store.PutModelRoute(r.Context(), scope.Tenant, in.Route, in.ExpectedRevision, in.Document)
	case "/v1/registry/DeleteModelRoute":
		err = h.store.DeleteModelRoute(r.Context(), scope.Tenant, in.Route, in.ExpectedRevision)
		result = map[string]bool{"deleted": err == nil}
	case "/v1/registry/ListModelRoutes":
		result, err = h.store.ListModelRoutes(r.Context(), scope.Tenant)
	default:
		result, err = h.store.ResolveModelRoute(r.Context(), scope.Tenant, in.Route)
	}
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	key := "route"
	if r.URL.Path == "/v1/registry/ListModelRoutes" {
		key = "routes"
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{key: result})
}
func (h *HTTPHandler) skill(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		SkillID          string          `json:"skill_id"`
		ExpectedRevision int64           `json:"expected_revision"`
		Revision         int64           `json:"revision"`
		Level            string          `json:"level"`
		Bundle           json.RawMessage `json:"bundle"`
	}
	if err = transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	personal := r.URL.Path == "/v1/registry/PutPersonalSkill" || r.URL.Path == "/v1/registry/GetPersonalSkill" || r.URL.Path == "/v1/registry/ListPersonalSkills"
	subject := ""
	if personal {
		subject = scope.Subject
	}
	var out any
	key := "skill"
	switch r.URL.Path {
	case "/v1/registry/PutSkill", "/v1/registry/PutPersonalSkill":
		out, err = h.store.PutScopedSkill(r.Context(), scope, personal, in.SkillID, in.Level, in.ExpectedRevision, in.Bundle)
	case "/v1/registry/ListSkills", "/v1/registry/ListPersonalSkills", "/v1/registry/AdminListSkills":
		out, err = h.store.ListScopedSkills(r.Context(), scope.Tenant, subject)
		key = "skills"
	default:
		out, err = h.store.GetScopedSkill(r.Context(), scope.Tenant, subject, in.SkillID, in.Revision)
	}
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if r.URL.Path == "/v1/registry/GetSkill" {
		if skill, ok := out.(Skill); ok && skill.Level != SkillLevelUser {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "skill revision not found at this level"))
			return
		}
	}
	if r.URL.Path == "/v1/registry/ListSkills" {
		all := out.([]Skill)
		visible := make([]Skill, 0, len(all))
		for _, skill := range all {
			if skill.Level == SkillLevelUser {
				visible = append(visible, skill)
			}
		}
		out = visible
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{key: out})
}

// AttachSecurityAuthority installs the human security authority that is allowed
// to record security ("проверка ИБ") clearances. It is separate from
// AttachManagementAuthority on purpose: even a caller that has been granted the
// ordinary administrative capability, or a workload holding a valid task token
// for that capability, still cannot write a clearance here. The guard admits a
// principal only when the verified bearer is a human (KindHuman) and authorize
// -- which the caller supplies to bind it to a live session/role -- permits the
// SecurityVerdictCapability. A task credential is refused structurally, before
// authorize is consulted, so no mis-wired authorize callback can let the
// artifact publisher self-issue the clearance its own image needs.
func AttachSecurityAuthority(handler http.Handler, verifier *auth.Verifier, humanAudience string, authorize transport.Authorize) error {
	h, ok := handler.(*HTTPHandler)
	if !ok || verifier == nil || humanAudience == "" || authorize == nil {
		return invalid("registry: security authority requires verified human policy")
	}
	inner := transport.TaskOrUser(verifier, registryAudience, humanAudience, SecurityVerdictCapability, authorize)
	h.securityAuth = func(next http.Handler) http.Handler {
		return inner(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			principal, ok := transport.FromContext(r.Context())
			if !ok || principal.Claims.Kind != auth.KindHuman {
				transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
					"security verdicts require a human security authority"))
				return
			}
			next.ServeHTTP(w, r)
		}))
	}
	return nil
}

// recordSecurityVerdict is the write side of the stage/prod supply-chain gate.
// It records, for one immutable OCI digest, the security clearance an operator
// security authority issued; requireDeployAdmission reads it when promoting to
// stage or prod. The identity gate is the securityAuth guard above -- the
// handler adds no authorization of its own, only input validation.
func (h *HTTPHandler) recordSecurityVerdict(w http.ResponseWriter, r *http.Request) {
	var in struct {
		OCIDigest string    `json:"oci_digest"`
		AgentID   string    `json:"agent_id"`
		Outcome   string    `json:"outcome"`
		Source    string    `json:"source"`
		CheckedAt time.Time `json:"checked_at"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	verdict, err := h.store.RecordSecurityVerdict(r.Context(), SecurityVerdict{
		OCIDigest: in.OCIDigest, AgentID: in.AgentID, Outcome: in.Outcome,
		Source: in.Source, CheckedAt: in.CheckedAt, ExpiresAt: in.ExpiresAt,
	})
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"security_verdict": verdict})
}
