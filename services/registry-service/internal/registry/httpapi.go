package registry

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// registryAudience and registryCapability are the token audience and the
// capability the authorize callback is consulted with on every registry route.
// They mirror the stateapi mount so the composed front guards this face exactly
// as it guards the accepted State face.
const (
	registryAudience   = "ahp"
	registryCapability = "registry"
	registryMaxBody    = 1 << 20
)

// HTTPHandler exposes the accepted Registry Store over HTTP for the composed
// front. It is a thin route adapter: every handler reads the caller's tenant
// from the VERIFIED principal installed by transport.Task -- never from a header
// or the request body -- and then calls an existing, already-accepted Store
// method verbatim. No new business branch is introduced here.
type HTTPHandler struct {
	store *Store
	task  func(http.Handler) http.Handler
	admin func(http.Handler) http.Handler
	// dispatcher is the optional run dispatcher that makes /v1/registry/Invoke fire
	// the same executor the trigger plane uses. When nil the route is present but
	// fails closed, so the accepted CatalogList/Publish/admission routes never
	// regress on fronts that do not wire invoke.
	dispatcher      RunDispatcher
	ownerObserver   OwnerRunObserver
	observationAuth func(http.Handler) http.Handler
	// securityAuth guards /v1/registry/RecordSecurityVerdict, the write side of
	// the stage/prod supply-chain gate. It is left nil unless an operator
	// attaches a human security authority, so the route fails closed: neither a
	// task nor an unconfigured front can record a security clearance.
	securityAuth func(http.Handler) http.Handler
	routes       map[string]func(http.ResponseWriter, *http.Request)
}

// NewHTTPHandler builds the registry HTTP face. authorize may not be nil; a nil
// authorize makes transport.Task fail every request closed.
func NewHTTPHandler(store *Store, verifier *auth.Verifier, authorize transport.Authorize) (http.Handler, error) {
	switch {
	case store == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "registry: store must not be nil")
	case verifier == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "registry: verifier must not be nil")
	case authorize == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "registry: authorize must not be nil")
	}
	h := &HTTPHandler{
		store: store,
		task:  transport.Task(verifier, registryAudience, registryCapability, authorize),
		admin: transport.Task(verifier, registryAudience, AdminCapability, authorize),
	}
	h.routes = map[string]func(http.ResponseWriter, *http.Request){
		"AdminAccess": func(w http.ResponseWriter, r *http.Request) {
			transport.WriteJSON(w, http.StatusOK, map[string]bool{"allowed": true})
		},
		"GetProfile": h.profile, "PutProfile": h.profile, "PinDotConfig": h.pinDot,
		"PutModelRoute": h.modelRoute, "ResolveModelRoute": h.modelRoute, "ListModelRoutes": h.modelRoute, "DeleteModelRoute": h.modelRoute,
		"AdminListSkills": h.skill, "AdminGetSkill": h.skill, "PutSkill": h.skill, "GetSkill": h.skill, "ListSkills": h.skill, "PutPersonalSkill": h.skill, "GetPersonalSkill": h.skill, "ListPersonalSkills": h.skill,
		"CatalogList": h.catalogList,
		"Publish":     h.publish,
		// Item 6 (PDF rev 3.2) admission and deployment face (helper26): thin
		// route adapters over the already-accepted, already-unit-tested Store
		// methods. They add NO business branch and read identity the same way the
		// two routes above do -- tenant/publisher from the VERIFIED principal that
		// transport.Task installed from the mTLS client certificate, never from a
		// header or the request body. PutVersion is the eval-gated admission
		// ("оценки перед публикацией"): the mounted store carries
		// WithEvalGate(evals), so publishing a version whose digest has no passing
		// verdict is refused on the HTTP layer (409), not only in the Store.
		"RegisterAgent":   h.registerAgent,
		"PutVersion":      h.putVersion,
		"Install":         h.install,
		"Promote":         h.promote,
		"Rollback":        h.rollback,
		"ListDeployments": h.listDeployments,
		"GetInvokedTask":  h.getInvokedTask,
		"Invoke":          h.invoke,
		"PinAgentTurn":    h.pinAgentTurn,
		"SendAgentTurn":   h.agentTurnControl,
		"ReadAgentTurn":   h.agentTurnControl,
		"CancelAgentTurn": h.agentTurnControl,
		"PutConfig":       h.putConfig,
		"GetConfig":       h.getConfig,
		"ResolveConfig":   h.resolveConfig,
		// RecordSecurityVerdict writes a security ("проверка ИБ") clearance that
		// the stage/prod admission gate reads. It is served only through the
		// dedicated human security authority below, never through the ordinary
		// task/admin faces, so the artifact publisher's own runtime can never
		// self-issue a clearance for its image.
		"RecordSecurityVerdict": h.recordSecurityVerdict,
	}
	return h, nil
}

func (h *HTTPHandler) putConfig(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		AgentID          string          `json:"agent_id"`
		ExpectedRevision int64           `json:"expected_revision"`
		Data             json.RawMessage `json:"data"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	config, err := h.store.PutConfig(r.Context(), scope.Tenant, in.AgentID, in.ExpectedRevision, in.Data)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"config": config})
}

func (h *HTTPHandler) getConfig(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		AgentID string `json:"agent_id"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	config, err := h.store.GetConfig(r.Context(), scope.Tenant, in.AgentID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"config": config})
}

func (h *HTTPHandler) resolveConfig(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		InstallationID string `json:"installation_id"`
		TaskID         string `json:"task_id"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	config, err := h.store.ResolveConfig(r.Context(), scope.Tenant, in.InstallationID, in.TaskID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"effective": config})
}

// AttachInvokeDispatcher wires a run dispatcher onto an already-built registry
// HTTP handler, enabling /v1/registry/Invoke. It is additive: the other routes are
// unaffected, and a nil dispatcher leaves invoke present-but-fail-closed.
func AttachInvokeDispatcher(handler http.Handler, d RunDispatcher) error {
	h, ok := handler.(*HTTPHandler)
	if !ok {
		return contracts.NewError(contracts.ErrInvalid, "registry: handler is not *HTTPHandler")
	}
	h.dispatcher = d
	return nil
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.Path, "/v1/registry/")
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "registry: unknown route"))
		return
	}
	fn, ok := h.routes[rest]
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "registry: unknown route"))
		return
	}
	if rest == "GetInvokedTask" && h.observationAuth != nil {
		h.observationAuth(http.HandlerFunc(fn)).ServeHTTP(w, r)
		return
	}
	if rest == "RecordSecurityVerdict" {
		if h.securityAuth == nil {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
				"registry: security verdict authority is not configured (fail closed)"))
			return
		}
		h.securityAuth(http.HandlerFunc(fn)).ServeHTTP(w, r)
		return
	}
	if AdministrativeOperation(rest) {
		h.admin(http.HandlerFunc(fn)).ServeHTTP(w, r)
		return
	}
	h.task(http.HandlerFunc(fn)).ServeHTTP(w, r)
}

func (h *HTTPHandler) registryPrincipal(r *http.Request) (contracts.Scope, error) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
			"registry: no verified principal on this request")
	}
	return p.Scope, nil
}

// catalogList answers with the tenant's published catalog cards, narrowed to the
// caller-supplied roles. Roles are a within-tenant visibility filter, not an
// identity claim: the hard boundary (tenant) always comes from the verified
// principal, so a foreign tenant can never list another tenant's catalog.
func (h *HTTPHandler) catalogList(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		Roles []string `json:"roles"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	cards, err := h.store.CatalogCards(r.Context(), scope.Tenant, in.Roles)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"cards": cards})
}

// publish publishes an installation into the caller's tenant catalog with the
// role audience employees must hold to see it (PDF rev 3.2, c. 42). The tenant
// is the verified principal's; the installation and roles are the admin's data.
func (h *HTTPHandler) publish(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		InstallationID string   `json:"installation_id"`
		Roles          []string `json:"roles"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.PublishWithRoles(r.Context(), scope.Tenant, in.InstallationID, in.Roles); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"published": in.InstallationID})
}

// -- Item 6 admission + deployment face (helper26) --------------------------
//
// Each handler below is the same shape as catalogList/publish: read the caller's
// scope from the VERIFIED principal, read the non-identity data from the body,
// then call one already-accepted Store method verbatim and surface its contract
// error unchanged through transport.WriteError. No new decision is made here.

// registerAgent binds an agent to the publisher identity of the VERIFIED caller.
// The publisher is the principal's subject, never a body field: an agent and its
// versions belong to the publisher that claimed it (PDF rev 3.2, c. 42), and a
// body-named publisher would let any caller forge authorship.
func (h *HTTPHandler) registerAgent(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		AgentID string `json:"agent_id"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	agent, err := h.store.RegisterAgent(r.Context(), scope.Subject, in.AgentID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"agent": agent})
}

// putVersion publishes one immutable version of an agent the caller owns. This
// is the eval-gated admission: the mounted store enforces a passing verdict for
// the exact digest, so a publication without one is refused at the HTTP layer.
func (h *HTTPHandler) putVersion(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		AgentID  string   `json:"agent_id"`
		Digest   string   `json:"digest"`
		Manifest Manifest `json:"manifest"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	version, err := h.store.PutVersion(r.Context(), scope.Subject, in.AgentID, in.Digest, in.Manifest)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"version": version})
}

// install creates an installation of an immutable version in the caller's tenant.
// The tenant comes from the verified principal; a foreign tenant can never write
// into another tenant's catalog (Store.Install binds the row to this tenant).
func (h *HTTPHandler) install(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		AgentID     string       `json:"agent_id"`
		VersionID   string       `json:"version_id"`
		Environment string       `json:"environment"`
		Key         string       `json:"key"`
		Granted     Capabilities `json:"granted"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	installation, err := h.store.Install(r.Context(), scope.Tenant, in.AgentID, in.VersionID, in.Environment, in.Key, in.Granted)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"installation": installation})
}

// promote moves the caller tenant's installation to a new version (an update),
// recording the rollout in the deployment history inside the same transaction.
func (h *HTTPHandler) promote(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		InstallationID string `json:"installation_id"`
		VersionID      string `json:"version_id"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	installation, err := h.store.Promote(r.Context(), scope.Tenant, in.InstallationID, in.VersionID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"installation": installation})
}

// rollback moves the caller tenant's installation back to its previous version.
func (h *HTTPHandler) rollback(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		InstallationID string `json:"installation_id"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	installation, err := h.store.Rollback(r.Context(), scope.Tenant, in.InstallationID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"installation": installation})
}

// listDeployments answers the rollout history of one installation of the caller
// tenant, newest first. The tenant is the verified principal's, so a foreign
// tenant reading another tenant's history is scoped out by the Store query.
func (h *HTTPHandler) listDeployments(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		InstallationID string `json:"installation_id"`
		Limit          int    `json:"limit"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	deployments, err := h.store.ListDeployments(r.Context(), scope.Tenant, in.InstallationID, in.Limit)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"deployments": deployments})
}
