// Package identityapi exposes the identity authority over HTTP.
//
// Two classes of endpoints exist:
//
//   - Operator control-plane (control/cancel-subtree, control/revoke-subject):
//     protected by an mTLS workload certificate whose deployment is on the
//     fail-closed ControlPlaneDeployments allowlist, and bounded to that
//     certificate's tenant. Ordinary services (for example Space) are not on
//     it and can never reach these admin switches.
//   - Lineage cancel (roots/{task}/cancel): an agent or service cancels exactly
//     its OWN subtree with a task token that carries identity:cancel; the target
//     task and scope come from the verified token, never from a request body.
//   - Root bootstrap (CreateRoot): protected by a human bearer token only.
//     A task token cannot open a root, so the first credential is never
//     gated by a credential that only that root could mint (no bootstrap
//     paradox). The human's tenant and subject come from the signed token;
//     the human only chooses the deployment for the new tree.
//   - Agent-facing (Delegate, Mint, Get, Evaluate, AddLabels):
//     protected by mTLS + a valid task token carrying the delegation claims.
//
// Tree ceilings (depth, fan-out, active subtree) and the self-delegation
// exception are trusted-manifest parameters. The public create-root body
// cannot inflate them: the handler ignores those fields and lets the
// identity package apply its defaults.
//
// The signer used by MintTask is injected at construction; no external
// network call happens inside a database transaction.
package identityapi

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// identityNamespace is the control/agent namespace this package owns. The catch-all
// in Handler must cover exactly the prefix its callers mount it under (cmd/ahp/main.go
// uses "/v1/identity/"), so it is a constant here and not a deployment knob: a knob
// would let a deployment mount this handler outside the guard that answers its
// unrouted paths.
const identityNamespace = "/v1/identity"

// Config wires the HTTP layer to its dependencies.
type Config struct {
	Store    *identity.Store
	Verifier *auth.Verifier
	Signer   *auth.Signer
	// Audience is the JWT audience both verifier and signer use.
	Audience string
	// Browser, when set, additionally mounts the Agent Space UI's login and
	// current-user surface. It is optional so the accepted control and agent
	// planes stay byte-identical for deployments that do not serve a UI.
	Browser *BrowserConfig

	// ControlExchange mounts the service-facing control routes
	// (/v1/identity/control/introspect, /v1/identity/control/exchange) on a deployment
	// that serves no browser UI. False by default, which keeps every accepted plane
	// byte-identical; when Browser is set the routes already travel with it and this
	// field changes nothing.
	ControlExchange bool
	// Auth selects how the browser surface authenticates a human. nil selects
	// the local credential development profile (NewLocalAuthenticator over the
	// Store). A production deployment injects an OIDCAuthenticator pointed at a
	// real issuer/JWKS/IAM; the local profile must never be described as the
	// corporate IdP.
	Auth identity.Authenticator
	// ExchangerDeployments lists the deployments whose verified workload
	// certificate may convert a live browser human session into a scoped task
	// token (the Human -> authorized Space -> scoped task boundary, PDF 3.5).
	// It is fail-closed: empty authorises nobody. In production this
	// authorisation belongs to the IAM/service mesh; the allowlist is the local
	// stand-in for that trusted-service relationship and must not be treated as
	// production authorisation.
	ExchangerDeployments []string
	// Exact workload lists are mandatory in the synthetic IAM composition.
	// Legacy deployment-only lists remain solely for the local profile.
	RequireExactWorkloadIDs bool
	ExchangerWorkloadIDs    []string
	DelegatorWorkloadIDs    []string
	ControlPlaneWorkloadIDs []string
	// ExchangeRequired names the human permission a subject must hold for a
	// trusted service to exchange its browser session for a scoped task. Empty
	// selects defaultExchangePermission.
	ExchangeRequired string
	// ControlPlaneDeployments lists the deployments whose verified workload
	// certificate may perform subject-wide revocation (revoke-subject). It is
	// fail-closed: empty authorises nobody, so revoke-subject is refused until an
	// operator wires the real IAM/service-mesh trust. It is the local stand-in
	// for that authority and must never be described as production IAM, nor
	// widened to make an ordinary service (for example Space) an administrator.
	// It also gates control/cancel-subtree, the operator kill switch that may
	// cancel a subtree it does not itself act inside. A service cancelling its
	// OWN child instead uses the lineage route roots/{task}/cancel with a task
	// token (see cancelSubtree, cancelByTask).
	ControlPlaneDeployments []string
	// DelegatorDeployments may broker a verified parent task into a target
	// deployment. Empty denies every broker; ordinary Delegate is unchanged.
	DelegatorDeployments []string
	// ResolveDelegationTarget reads current server-owned manifest/installation
	// policy. A nil resolver denies delegation; request bodies are never policy.
	ResolveDelegationTarget func(context.Context, contracts.Scope, string) (identity.DelegationTarget, error)
	// Logger is where the exchange surface records WARN entries for every
	// refusal, each carrying the stable reason code and the non-secret request
	// identifiers (task, context, azp, subject hash). nil selects slog.Default().
	Logger *slog.Logger
}

// requiredExchangePermission is the human permission a subject must hold for a
// trusted service to exchange its browser session for a scoped task. Empty
// selects the documented default.
const defaultExchangePermission = "space:access"

// ExchangeRequiredPermission returns the entitlement a human must hold to have
// a task minted on its behalf.
func (c Config) ExchangeRequiredPermission() string {
	if strings.TrimSpace(c.ExchangeRequired) == "" {
		return defaultExchangePermission
	}
	return c.ExchangeRequired
}

// Handler returns the fully-wired HTTP handler for the identity service.
func Handler(cfg Config) http.Handler {
	h := &api{cfg: cfg}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", h.health)

	if cfg.Browser != nil {
		h.mountBrowser(mux)
	} else if cfg.ControlExchange {
		h.mountControl(mux)
	}

	mux.Handle("POST /v1/identity/roots",
		transport.Human(cfg.Verifier, cfg.Audience)(http.HandlerFunc(h.createRoot)))
	mux.Handle("GET /v1/identity/roots/{task}",
		transport.Task(cfg.Verifier, cfg.Audience, "identity:read", cfg.Store.Authorize)(http.HandlerFunc(h.getRoot)))
	mux.Handle("POST /v1/identity/roots/{task}/delegate",
		transport.Task(cfg.Verifier, cfg.Audience, "identity:delegate", cfg.Store.Authorize)(http.HandlerFunc(h.delegate)))
	mux.Handle("POST /v1/identity/roots/{task}/delegate-to-target",
		transport.Workload()(http.HandlerFunc(h.delegateToTarget)))
	mux.Handle("POST /v1/identity/tasks/{task}/issue-token",
		transport.Workload()(http.HandlerFunc(h.issueTaskToken)))
	mux.Handle("POST /v1/identity/roots/{task}/mint",
		transport.Task(cfg.Verifier, cfg.Audience, "identity:mint", cfg.Store.Authorize)(http.HandlerFunc(h.mint)))
	mux.Handle("POST /v1/identity/roots/{task}/evaluate",
		transport.Task(cfg.Verifier, cfg.Audience, "identity:evaluate", cfg.Store.Authorize)(http.HandlerFunc(h.evaluate)))
	mux.Handle("POST /v1/identity/roots/{task}/labels",
		transport.Task(cfg.Verifier, cfg.Audience, "identity:labels", cfg.Store.Authorize)(http.HandlerFunc(h.addLabels)))
	mux.Handle("POST /v1/identity/roots/{task}/children/{child}/cancel",
		transport.Task(cfg.Verifier, cfg.Audience, "identity:cancel", cfg.Store.Authorize)(http.HandlerFunc(h.cancelChild)))
	mux.Handle("POST /v1/identity/roots/{task}/cancel",
		transport.Task(cfg.Verifier, cfg.Audience, "identity:cancel", cfg.Store.Authorize)(http.HandlerFunc(h.cancelByTask)))

	mux.Handle("POST /v1/identity/control/cancel-subtree",
		transport.Workload()(http.HandlerFunc(h.cancelSubtree)))
	mux.Handle("POST /v1/identity/control/revoke-subject",
		transport.Workload()(http.HandlerFunc(h.revokeSubject)))

	// The namespaces this handler is accountable for, and the catch-all it owns
	// inside each. Without them an unrouted path in one of them falls through to
	// Go's default answer - text/plain "404 page not found" - which is what the live
	// human edge was measured serving for /v1/identity/nope, /u-ahp/v1/nope and
	// /u-ahp/v1/auth/nope on pin debf8b1, while every other reserved namespace on the
	// same origin answers the contract envelope. See http_surface_test.go.
	namespaces := []string{identityNamespace}
	if cfg.Browser != nil {
		namespaces = append(namespaces, cfg.Browser.prefix())
	}
	h.catchAll = make(map[string]bool, len(namespaces))
	for _, ns := range namespaces {
		pattern := ns + "/{rest...}"
		h.catchAll[pattern] = true
		mux.HandleFunc(pattern, h.unrouted)
	}
	h.self = mux

	return mux
}

type api struct {
	cfg Config

	// self is the mux this handler serves. unrouted asks it which verbs a real route
	// serves at a path, so the route table stays in one place and this check cannot
	// drift from it.
	self *http.ServeMux
	// catchAll holds the patterns unrouted itself is registered under; when Match
	// returns one of them, nothing real lives at that path.
	catchAll map[string]bool
}

// unroutedVerbs is the closed set of verbs the product registers, in alphabetical
// order so the Allow header it produces is stable. It is a verb list, not a route
// table: the routes stay in the mux.
var unroutedVerbs = []string{
	http.MethodDelete, http.MethodGet, http.MethodOptions,
	http.MethodPatch, http.MethodPost, http.MethodPut,
}

// unrouted answers a path inside a namespace this process owns that has no route
// behind it, per decisions/2026-10-31-http-surface-no-false-200.md: 404 with
// {"code":"NOT_FOUND"}, never the app shell and never Go's text answer.
//
// The one case it must not claim is a path that does have a route, just not for this
// verb. A bare {rest...} catch-all matches those too and would downgrade an honest
// 405 + Allow into "no route here" (measured: probe-mux-20261101T0920Z), so the verb
// mismatch keeps the answer Go gave before, bytes and all -
// tools/stand-html-render-check.sh reads that 405 as the difference between a login
// route that exists and a shell standing over a dead one.
func (h *api) unrouted(w http.ResponseWriter, r *http.Request) {
	if served := h.verbsAt(r); len(served) > 0 {
		w.Header().Set("Allow", strings.Join(served, ", "))
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)

		return
	}

	transport.WriteJSON(w, http.StatusNotFound, &contracts.Error{
		Code:    contracts.ErrNotFound,
		Message: "no route at " + r.URL.Path,
	})
}

// verbsAt returns the verbs a real route serves at r's path, excluding the verb r
// itself used. mux.Handler answers with an empty pattern when the probed verb has no
// route here (it hands back its own method-mismatch handler instead), and with one of
// h.catchAll when the only match is unrouted itself; both mean "no route". Anything
// else means the route exists and only the caller's verb is wrong.
func (h *api) verbsAt(r *http.Request) []string {
	var served []string

	for _, verb := range unroutedVerbs {
		if verb == r.Method {
			continue
		}

		probe := r.Clone(r.Context())
		probe.Method = verb

		if _, pattern := h.self.Handler(probe); pattern != "" && !h.catchAll[pattern] {
			served = append(served, verb)
		}
	}

	return served
}

// logger returns the configured logger, defaulting to slog.Default() so the
// surface is never silent and callers that do not configure one still get WARN
// journal entries.
func (h *api) logger() *slog.Logger {
	if h.cfg.Logger != nil {
		return h.cfg.Logger
	}
	return slog.Default()
}

func (h *api) health(w http.ResponseWriter, r *http.Request) {
	transport.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readBody decodes a request body and ANSWERS the rejection.
//
// transport.ReadJSON returns the contract error and writes nothing, so the bare
// `if err := transport.ReadJSON(w, r, &body, 0); err != nil { return }` this package
// used everywhere let net/http complete the exchange as 200 with an empty body. A
// caller that sent {"ttl_out":"30m"} instead of {"ttl":"30m"} received a success
// status and no grant, and Space - which tests `status != 201` - could only report
// "delegation did not happen". Rejections on this face must be answered, and a
// rejected write must not have happened: tests/identity/root_body_rejection_test.go
// pins both halves, after decisions/2026-10-31-http-surface-no-false-200.md.
func (h *api) readBody(w http.ResponseWriter, r *http.Request, out any) bool {
	if err := transport.ReadJSON(w, r, out, 0); err != nil {
		transport.WriteError(w, r, err)
		return false
	}
	return true
}

type createRootBody struct {
	Task         string   `json:"task"`
	Context      string   `json:"context"`
	Capabilities []string `json:"capabilities"`
	TTLOut       string   `json:"ttl"`
	// Deployment selects which deployment hosts the new tree; the human may
	// choose it, but tenant and subject are fixed by the signed token.
	Deployment string `json:"deployment"`
	// The ceiling fields are accepted on the wire for manifest-carrying
	// internal callers but deliberately never read here: a public create-root
	// request cannot raise a tree ceiling or enable self-delegation. They are
	// dropped so the identity package applies DefaultMaxDepth/Fanout/Active.
	MaxDepth  int  `json:"max_depth,omitempty"`
	MaxFanout int  `json:"max_fanout,omitempty"`
	MaxActive int  `json:"max_active,omitempty"`
	AllowSelf bool `json:"allow_self,omitempty"`
}

func (h *api) createRoot(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: no principal"))
		return
	}
	var body createRootBody
	if !h.readBody(w, r, &body) {
		return
	}
	ttl, err := time.ParseDuration(body.TTLOut)
	if err != nil {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "identityapi: ttl is required and must be a valid duration"))
		return
	}
	// Tenant and subject are taken from the signed human token so a caller can
	// never open a root in another tenant or impersonate another subject; the
	// human only selects the deployment that will host the new tree. Cross-
	// tenant subjects and deployments stay invisible.
	scope := contracts.Scope{
		Tenant:     p.Claims.Tenant,
		Deployment: body.Deployment,
		Subject:    p.Claims.Subject,
	}
	grant, err := h.cfg.Store.CreateRoot(r.Context(), scope, identity.RootRequest{
		Task:         body.Task,
		Context:      body.Context,
		Deployment:   body.Deployment,
		Capabilities: body.Capabilities,
		TTL:          ttl,
		// Ceilings intentionally omitted (trusted-manifest only).
	})
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusCreated, grant)
}

func (h *api) getRoot(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: no principal"))
		return
	}
	grant, err := h.cfg.Store.Get(r.Context(), p.Scope, r.PathValue("task"))
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, grant)
}

type delegateBody struct {
	Task         string   `json:"task"`
	Context      string   `json:"context"`
	Capabilities []string `json:"capabilities"`
	TTLOut       string   `json:"ttl"`
	MaxDepth     int      `json:"max_depth,omitempty"`
	MaxFanout    int      `json:"max_fanout,omitempty"`
	MaxActive    int      `json:"max_active,omitempty"`
	AllowSelf    bool     `json:"allow_self,omitempty"`
}

func (h *api) delegate(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: no principal"))
		return
	}
	var body delegateBody
	if !h.readBody(w, r, &body) {
		return
	}
	ttl, err := time.ParseDuration(body.TTLOut)
	if err != nil {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "identityapi: ttl is required and must be a valid duration"))
		return
	}
	// A task token may delegate only directly under its OWN grant: the path
	// parent must equal the token's task, never an ancestor or a sibling, or a
	// narrowed token could act above itself. Store.Delegate additionally enforces
	// child capabilities as a subset of this (== the caller's) grant server-side,
	// so a client cannot widen by assertion.
	parent := r.PathValue("task")
	if parent == "" || parent != p.Claims.Task {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"identityapi: a task token delegates only under its own grant"))
		return
	}
	grant, err := h.cfg.Store.Delegate(r.Context(), p.Scope, parent, identity.DelegateRequest{
		Task:         body.Task,
		Context:      body.Context,
		Deployment:   p.Scope.Deployment,
		Capabilities: body.Capabilities,
		TTL:          ttl,
		MaxDepth:     body.MaxDepth,
		MaxFanout:    body.MaxFanout,
		MaxActive:    body.MaxActive,
		AllowSelf:    body.AllowSelf,
	})
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// E1: the delegated child credential is issued in the SAME response, so the
	// consumer never has to mint a child under a bearer that is not the child.
	// The token is short-lived (a default, capped by the child grant's remaining
	// life); the grant itself is the durable, revocable authority. The envelope
	// embeds the grant so readers that only decode the grant still work.
	token, err := h.cfg.Store.MintTask(r.Context(), p.Scope, grant.Task, h.cfg.Signer, h.cfg.Audience, 0)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusCreated, delegateResponse{Grant: grant, Token: token})
}

// delegateResponse is the frozen E1 delegate result: the created child grant
// with, in the same 201, a task token bound to that child. identity.Grant is
// embedded so its JSON fields are unchanged for readers that ignore "token".
type delegateResponse struct {
	identity.Grant
	Token string `json:"token"`
}

type mintBody struct {
	Audience string `json:"audience,omitempty"`
	TTLOut   string `json:"ttl,omitempty"`
}

func (h *api) mint(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: no principal"))
		return
	}
	var body mintBody
	// An absent body is the documented way to mint with the default audience and
	// ttl. A body that IS present and cannot be read is a caller error: this route
	// used to discard the rejection and sign a credential for a ttl/audience it
	// never parsed.
	if r.ContentLength != 0 && !h.readBody(w, r, &body) {
		return
	}

	audience := body.Audience
	if audience == "" {
		audience = h.cfg.Audience
	}
	var ttl time.Duration
	if body.TTLOut != "" {
		ttl, _ = time.ParseDuration(body.TTLOut)
	}
	// A task token may mint a credential for ITSELF or for one of its verified
	// DESCENDANTS (so a parent that just delegated a child can mint under it),
	// never for an ancestor, a sibling or an unrelated task. The descendant is
	// resolved from the stored parent chain, not from a client assertion, so a
	// narrowed token cannot mint above or beside itself. The minted grant's own
	// live capabilities are the ceiling regardless.
	task := r.PathValue("task")
	if task == "" {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"identityapi: mint requires a task"))
		return
	}
	if task != p.Claims.Task {
		below, err := h.cfg.Store.IsDescendant(r.Context(), p.Scope, p.Claims.Task, task)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		if !below {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
				"identityapi: a task token mints only for its own grant or a descendant"))
			return
		}
	}
	token, err := h.cfg.Store.MintTask(r.Context(), p.Scope, task,
		h.cfg.Signer, audience, ttl)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]string{"token": token})
}

type evaluateBody struct {
	Capability string `json:"capability"`
}

func (h *api) evaluate(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: no principal"))
		return
	}
	var body evaluateBody
	if !h.readBody(w, r, &body) {
		return
	}
	grant, err := h.cfg.Store.Evaluate(r.Context(), p.Scope, r.PathValue("task"), body.Capability)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, grant)
}

type addLabelsBody struct {
	Subjects   []string `json:"subjects,omitempty"`
	Workspaces []string `json:"workspaces,omitempty"`
	Taints     []string `json:"taints,omitempty"`
}

func (h *api) addLabels(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: no principal"))
		return
	}
	var body addLabelsBody
	if !h.readBody(w, r, &body) {
		return
	}
	grant, err := h.cfg.Store.AddLabels(r.Context(), p.Scope, r.PathValue("task"),
		labels.Set{Subjects: body.Subjects, Workspaces: body.Workspaces, Taints: body.Taints})
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, grant)
}

type cancelSubtreeBody struct {
	Tenant     string `json:"tenant"`
	Deployment string `json:"deployment"`
	Subject    string `json:"subject"`
	Task       string `json:"task"`
}

func (h *api) cancelSubtree(w http.ResponseWriter, r *http.Request) {
	// control/cancel-subtree is the operator kill switch: unlike the lineage
	// route it can cancel a subtree the caller does not itself act inside, so it
	// must never be reachable by an ordinary workload. Only a deployment on the
	// fail-closed ControlPlaneDeployments allowlist may use it, and only within
	// its own certificate tenant, so it is never a cross-tenant switch. Generic
	// services such as Space are not control planes: to cancel their own child
	// run they present a task token to roots/{task}/cancel instead. The target
	// scope is therefore trusted only because the caller is a trusted operator,
	// not because any valid certificate implies it.
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: no principal"))
		return
	}
	if !h.cfg.workloadAllowed(p.Scope, h.cfg.ControlPlaneWorkloadIDs, h.cfg.ControlPlaneDeployments) {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"identityapi: caller deployment is not an authorized control plane"))
		return
	}
	var body cancelSubtreeBody
	if !h.readBody(w, r, &body) {
		return
	}
	if body.Tenant != p.Scope.Tenant {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"identityapi: cancel tenant does not match the caller's workload certificate"))
		return
	}
	scope := contracts.Scope{Tenant: body.Tenant, Deployment: body.Deployment, Subject: body.Subject}
	if err := h.cfg.Store.CancelSubtree(r.Context(), scope, body.Task); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// cancelByTask is the lineage-guarded cancellation an agent or a service uses
// to stop its OWN subtree (PDF: a downstream CancelSubtree denies immediately,
// with no need for the agent to cooperate). The scope and the target task come
// only from the verified task token: transport.Task has bound tenant and
// deployment to the caller certificate and checked the token's live grant for
// identity:cancel. A caller therefore cancels exactly the task it holds and can
// never name another subject's tree, and siblings are untouched because only
// {task} and its descendants are revoked.
func (h *api) cancelByTask(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: no principal"))
		return
	}
	task := r.PathValue("task")
	if task == "" || task != p.Claims.Task {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"identityapi: a task token cancels only its own task"))
		return
	}
	if err := h.cfg.Store.CancelSubtree(r.Context(), p.Scope, task); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type revokeSubjectBody struct {
	Tenant  string `json:"tenant"`
	Subject string `json:"subject"`
}

func (h *api) revokeSubject(w http.ResponseWriter, r *http.Request) {
	// Subject-wide tombstone crosses deployments, so unlike cancel it cannot be
	// bounded by an actor chain. It is gated by the certificate tenant (never
	// cross-tenant) and by an explicit, fail-closed control-plane allowlist. An
	// empty allowlist refuses everyone, which keeps revoke-subject from silently
	// becoming a capability of any workload that happens to present a cert.
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: no principal"))
		return
	}
	var body revokeSubjectBody
	if !h.readBody(w, r, &body) {
		return
	}
	if body.Tenant != p.Scope.Tenant {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"identityapi: revoke tenant does not match the caller's workload certificate"))
		return
	}
	if !h.cfg.workloadAllowed(p.Scope, h.cfg.ControlPlaneWorkloadIDs, h.cfg.ControlPlaneDeployments) {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"identityapi: caller deployment is not an authorized control plane"))
		return
	}
	if err := h.cfg.Store.RevokeSubject(r.Context(), body.Tenant, body.Subject); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
