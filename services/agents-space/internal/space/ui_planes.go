package space

// Human-edge pipes for the planes a person has to operate from a browser: Registry
// (choose/publish/install/promote/rollback), Evals (the verdict a publication needs),
// Triggers (schedules and what they fired) and Memory (lifecycle and search).
//
// These are the same pipe as ui_resolve.go and ui_cost.go, generalized: the browser shows
// a human session, this process exchanges it for a scoped TASK token (ExchangeMiddleware),
// and only that exchanged token is carried to the plane's machine face over Space's own
// workload mTLS. The pipe exists because a browser can never present a workload
// certificate: measured on the live stand, pointing a browser at a transport.Task face is
// HTTP 403 "token audience does not match", which is a category error and not a wiring
// choice.
//
// Workspace Publish, Registry GetInvokedTask and Tools approvals use a human-only
// owner path: those operations carry the verified human JWT to the plane over mTLS.
// They never send it to an agent runtime or mint execution authority.
//
// What a pipe never does: accept tenant, subject or deployment from
// a request body or path (the plane authorizes from the VERIFIED token scope, which is how
// the same machine face already answers a task token); widen capabilities (Identity
// attenuates what the human was provisioned with, and a missing capability comes back as
// the plane's own 403, untouched); invent an answer (upstream status and body pass through
// verbatim, so a refusal cannot become an empty success screen); or serve an operation that
// was not named when the pipe was built (an unknown operation is a 404 from THIS route, not
// a blind proxy of a browser's guess at the machine surface).
//
// Deliberately absent: triggers/CreateWebhook (a webhook carries a shared secret, and a
// browser must never hold or transmit one), memory/DeleteSubject and memory/Fence
// (subject-wide destruction and internal fencing are not a screen action).

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Browser-visible prefixes of the four plane surfaces. Each is a subtree: the operation name
// is the last path segment, and it has to be one this surface names.
const (
	UIProfilePath   = "/u-ahp/v1/profile/"
	UIRegistryPath  = "/u-ahp/v1/registry/"
	UIEvalsPath     = "/u-ahp/v1/evals/"
	UITriggersPath  = "/u-ahp/v1/triggers/"
	UIMemoryPath    = "/u-ahp/v1/memory/"
	UIComputePath   = "/u-ahp/v1/compute/"
	UIWorkspacePath = "/u-ahp/v1/workspace/"
	UIArtifactsPath = "/u-ahp/v1/artifacts/"
	UIToolsPath     = "/u-ahp/v1/tools/"
	maxPlaneBody    = 4 << 20
	maxPlaneRespLen = 8 << 20
	planeCallBudget = 15 * time.Second
)

// PlaneSpec describes one human surface: where its machine face lives, and which of its
// operations a person is allowed to reach from a screen.
type PlaneSpec struct {
	// RoutePrefix is the browser-visible prefix, e.g. UIRegistryPath.
	RoutePrefix string
	// MachinePrefix is the plane's own API prefix, e.g. "/v1/registry/".
	MachinePrefix string
	// AllowedOps are the operations this surface forwards. Everything else is a 404 here.
	AllowedOps []string
	// OriginEnv names the deployment variable holding the plane ORIGIN, for error text.
	OriginEnv string
}

// Plane surfaces as the product defines them. Kept as data so a mount test can assert the
// whole browser-visible contract in one place.
var (
	ProfilePlane  = PlaneSpec{RoutePrefix: UIProfilePath, MachinePrefix: "/v1/registry/", OriginEnv: "AS_REGISTRY_URL", AllowedOps: []string{"GetProfile", "PutProfile", "PutPersonalSkill", "GetPersonalSkill", "ListPersonalSkills"}}
	RegistryPlane = PlaneSpec{
		RoutePrefix: UIRegistryPath, MachinePrefix: "/v1/registry/", OriginEnv: "AS_REGISTRY_URL",
		AllowedOps: []string{"CatalogList", "ListDeployments", "RegisterAgent", "PutVersion", "Publish", "Install", "Promote", "Rollback", "Invoke", "GetInvokedTask", "PutConfig", "GetConfig", "ResolveConfig", "AdminListSkills", "AdminGetSkill", "AdminAccess", "PutModelRoute", "ResolveModelRoute", "ListModelRoutes", "DeleteModelRoute", "PutSkill", "GetSkill", "ListSkills"},
	}
	EvalsPlane = PlaneSpec{
		RoutePrefix: UIEvalsPath, MachinePrefix: "/v1/evals/", OriginEnv: "AS_EVALS_URL",
		AllowedOps: []string{"Verdict", "RunEval"},
	}
	TriggersPlane = PlaneSpec{
		RoutePrefix: UITriggersPath, MachinePrefix: "/v1/triggers/", OriginEnv: "AS_TRIGGERS_URL",
		AllowedOps: []string{"CreateSchedule", "List", "Get", "Revoke", "FireNow"},
	}
	MemoryPlane = PlaneSpec{
		RoutePrefix: UIMemoryPath, MachinePrefix: "/v1/memory/", OriginEnv: "AS_MEMORY_URL",
		AllowedOps: []string{"Remember", "Search", "Settings", "UpdateSettings", "Status", "Operation", "Clear", "Correct", "Invalidate", "Propose", "Read", "Get", "Review", "AddLabels", "Delete"},
	}
	ComputePlane = PlaneSpec{
		RoutePrefix: UIComputePath, MachinePrefix: "/v1/compute/", OriginEnv: "AS_COMPUTE_URL",
		AllowedOps: []string{"CreateComputer", "GetComputer", "AcquireIncarnation", "StopIncarnation", "Sleep", "Wake", "CostReport", "Exec", "Run", "GetCommand", "ListEnvironment", "Snapshot"},
	}
	WorkspacePlane = PlaneSpec{
		RoutePrefix: UIWorkspacePath, MachinePrefix: "/v1/workspace/", OriginEnv: "AS_WORKSPACE_URL",
		AllowedOps: []string{"Create", "GetWorkspace", "ListMembers", "AddMember", "RemoveMember", "CreateCandidate", "UpdateCandidateFiles", "GetCandidate", "CandidateDetails", "VerifyCandidate", "ListCandidates", "RejectCandidate", "Publish", "GetHead", "GetRevision", "ListRevisions", "ListFiles", "ReadFile", "ListConflictBranches"},
	}
	ArtifactsPlane = PlaneSpec{RoutePrefix: UIArtifactsPath, MachinePrefix: "/v1/artifacts/", OriginEnv: "AS_ARTIFACTS_URL", AllowedOps: []string{"Publish"}}
	ToolsPlane     = PlaneSpec{
		RoutePrefix: UIToolsPath, MachinePrefix: "/v1/tools/", OriginEnv: "AS_TOOLS_URL",
		AllowedOps: []string{"Approvals", "Approve", "OAuthConnections", "OAuthBegin", "OAuthCallback", "OAuthRevoke"},
	}
)

// PlanePipes returns the explicit browser surfaces in mount order.
func PlanePipes() []PlaneSpec {
	return []PlaneSpec{ProfilePlane, RegistryPlane, EvalsPlane, TriggersPlane, MemoryPlane, ComputePlane, WorkspacePlane, ArtifactsPlane}
}

// PlaneProxyConfig wires one human surface to its machine face.
type PlaneProxyConfig struct {
	// Spec names the prefixes and the allowed operations.
	Spec PlaneSpec
	// Origin is the plane ORIGIN (scheme://host[:port], no API path); a URL carrying the
	// path is refused here rather than producing a doubled path at request time.
	Origin string
	// Client presents Space's OWN workload certificate.
	Client *http.Client
	// CallTimeout bounds the machine hop.
	CallTimeout time.Duration
}

type planeProxy struct {
	spec     PlaneSpec
	base     string
	allowed  map[string]bool
	client   *http.Client
	timeout  time.Duration
	opLength int
	human    bool
}

// planeOrigin validates a plane origin the same way the Artifacts proxies do.
func planeOrigin(env, raw string) (string, error) {
	if raw == "" {
		return "", contracts.NewError(contracts.ErrInvalid, "space: "+env+" is required for this human surface")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", contracts.NewError(contracts.ErrInvalid, "space: "+env+" must be an absolute http(s) URL")
	}
	if strings.Trim(u.Path, "/") != "" {
		return "", contracts.NewError(contracts.ErrInvalid,
			"space: "+env+" must name the origin only; the human surface adds the plane API path itself")
	}
	return strings.TrimRight(raw, "/"), nil
}

// NewPlaneProxy builds one human surface. It fails closed on a missing client or an empty
// operation list: a surface that forwards nothing is a route that lies.
func NewPlaneProxy(cfg PlaneProxyConfig) (http.Handler, error) {
	if len(cfg.Spec.AllowedOps) == 0 {
		return nil, contracts.NewError(contracts.ErrInvalid, "space: a human surface needs at least one allowed operation")
	}
	if cfg.Client == nil {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"space: the human surface for "+cfg.Spec.RoutePrefix+" needs Space's own workload mTLS client")
	}
	base, err := planeOrigin(cfg.Spec.OriginEnv, cfg.Origin)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]bool, len(cfg.Spec.AllowedOps))
	longest := 0
	for _, op := range cfg.Spec.AllowedOps {
		if strings.TrimSpace(op) == "" || strings.ContainsAny(op, "/\\") {
			return nil, contracts.NewError(contracts.ErrInvalid, "space: an allowed operation name is malformed")
		}
		allowed[op] = true
		if len(op) > longest {
			longest = len(op)
		}
	}
	timeout := cfg.CallTimeout
	if timeout <= 0 {
		timeout = planeCallBudget
	}
	return &planeProxy{
		spec: cfg.Spec, base: base, allowed: allowed, client: cfg.Client, timeout: timeout, opLength: longest,
	}, nil
}

// NewHumanToolsProxy preserves the human credential specifically for approvals.
// Tools verifies the live browser session and tools:approve itself. It must not
// receive a delegated task token for a human decision.
func NewHumanToolsProxy(origin string, client *http.Client) (http.Handler, error) {
	h, err := NewPlaneProxy(PlaneProxyConfig{Spec: ToolsPlane, Origin: origin, Client: client})
	if err != nil {
		return nil, err
	}
	h.(*planeProxy).human = true
	return h, nil
}

// AllowedOperations reports the forwarded operations, sorted - for mount assertions.
func (p *planeProxy) AllowedOperations() []string {
	ops := make([]string, 0, len(p.allowed))
	for op := range p.allowed {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	return ops
}

func (p *planeProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Every machine operation on these faces is a POST RPC; a GET here would be a browser
	// guessing at the surface, and a link the person can lose by refreshing is not a
	// product contract.
	if r.Method != http.MethodPost {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "space: unknown route"))
		return
	}
	op := strings.TrimPrefix(r.URL.Path, p.spec.RoutePrefix)
	if op == "" || strings.Contains(op, "/") || len(op) > p.opLength || !p.allowed[op] {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "space: unknown route"))
		return
	}
	token, ok := ScopedTaskFromContext(r.Context())
	if p.human || (p.spec.RoutePrefix == UIWorkspacePath && op == "Publish") ||
		(p.spec.RoutePrefix == UIRegistryPath && (op == "GetInvokedTask" || registry.AdministrativeOperation(op))) {
		principal, verified := transport.FromContext(r.Context())
		ok = verified && principal.Claims.Kind == auth.KindHuman
		token = bearerToken(r)
	}
	if !ok || token == "" {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"space: this surface needs an exchanged task token (no human JWT is forwarded to "+p.spec.RoutePrefix+")"))
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxPlaneBody+1))
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if int64(len(body)) > maxPlaneBody {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "space: request body is too large"))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), p.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.base+p.spec.MachinePrefix+op, bytes.NewReader(body))
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if p.human && op == "OAuthCallback" {
		for _, cookie := range r.Cookies() {
			if strings.HasPrefix(cookie.Name, "ahp_oauth_") {
				req.AddCookie(cookie)
			}
		}
	}

	resp, err := p.client.Do(req)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxPlaneRespLen))
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// The plane's answer is the contract, refusal included: a 403 for a capability the
	// person was never provisioned with has to reach the screen as a 403.
	w.Header().Set("Cache-Control", "no-store")
	if p.human && (op == "OAuthBegin" || op == "OAuthCallback") {
		for _, cookie := range resp.Cookies() {
			if strings.HasPrefix(cookie.Name, "ahp_oauth_") {
				http.SetCookie(w, cookie)
			}
		}
	}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(payload)
}
