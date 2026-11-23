// Package toolsapi is the thin HTTP face over the accepted internal/tools tool
// and credential gateway (PDF rev 3.2, с. 44-45 "Tool & Credential Gateway":
// "MCP-пулы по владельцу полномочий"; с. 151 Call/Get/Fence/Approve, OAuth-
// подключение, журнал эффектов по ключам операций, guard неопределённых
// исходов). It adds no business branch: every route reads the caller's scope
// from the VERIFIED principal (never a header or body) and calls an existing,
// already-accepted tools.Service method verbatim.
//
// Identity and authorization mirror the accepted State / Registry faces:
//   - the task-facing routes (Call/Get/Fence/ConnectionState) demand mTLS plus
//     a bound task token via transport.Task, guarded by the same authorize
//     callback, so a foreign tenant can never reach another tenant's effects;
//   - Approve is the one human-facing route: it requires a KindHuman bearer via
//     transport.Human AND forwards that same bearer to Service.Approve, which
//     re-verifies it as human and binds the approval to a canonical digest. A
//     task token can never approve -- the human-only invariant is enforced at
//     the edge and in the service, never relaxed here.
//
// Content-freedom: every operation view carries identifiers, enums and numbers
// only, and NO route ever echoes a tool's request arguments, prompts or
// credentials back.
//
// GetResult is the single deliberate exception, and it is narrow by contract
// (TOOLS-RESULT contract, 2026-11-09): it releases the stored RESULT of one
// SUCCESSFUL operation, to the owner authority that actually performed it,
// with the producing context proven from labels written at dispatch time and
// the current read authority revalidated. Everything else about results stays
// content-free: pending, uncertain and failed operations are reported by
// status only, out-of-line output is returned as an immutable Artifacts
// reference that the caller must fetch again under its CURRENT authority, and
// a result whose provenance is missing or foreign fails closed. The existing
// content-free Get is unchanged.
package toolsapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	// audience and capability mirror the stateapi/registry mount so the composed
	// front guards this face exactly as it guards the accepted faces.
	audience   = "ahp"
	capability = "tools"
	// ApproveCapability is the human sub-capability of this plane (director
	// contract 2026-11-02, DEF-TGW-2): the human Approve route and the pending
	// approvals worklist are guarded by transport.TaskOrUser with the accepted
	// user-audience pattern, exactly like the workspace user face. A task token
	// is NEVER authorised for it -- a task cannot sign a human approval.
	ApproveCapability = "tools:approve"
	// maxBody bounds every request body.
	maxBody = 1 << 20
)

// Handler is the mounted tool gateway: POST /v1/tools/{Operation}.
type Handler struct {
	svc         *tools.Service
	task        func(http.Handler) http.Handler
	human       func(http.Handler) http.Handler
	oauthHuman  func(http.Handler) http.Handler
	routes      map[string]func(http.ResponseWriter, *http.Request)
	leaseProof  *auth.Verifier
	strictLease bool
}

// NewHandler builds the tool-gateway HTTP face. Every dependency is mandatory:
// a nil anywhere fails closed at construction time; a nil authorize makes the
// task middleware refuse every request.
func NewHandler(svc *tools.Service, verifier *auth.Verifier, authorize transport.Authorize) (http.Handler, error) {
	return newHandler(svc, verifier, authorize, "", nil)
}

// NewHandlerWithToolLeases serves a compact deployment whose tool calls must
// carry State API lease proof. Mount this instead of the legacy task handler
// for the deployment. Lease verification uses the dedicated State public key.
func NewHandlerWithToolLeases(ctx context.Context, svc *tools.Service, verifier *auth.Verifier, authorize transport.Authorize, leaseProof *auth.Verifier) (http.Handler, error) {
	return NewHandlerWithToolLeasesAndUsers(ctx, svc, verifier, authorize, leaseProof, "", nil)
}

// NewHandlerWithToolLeasesAndUsers requires a State proof on every tool call
// and preserves the independent browser Identity guards on human routes.
func NewHandlerWithToolLeasesAndUsers(ctx context.Context, svc *tools.Service, verifier *auth.Verifier, authorize transport.Authorize,
	leaseProof *auth.Verifier, userAudience string, humanAuthorize transport.Authorize) (http.Handler, error) {
	return newHandlerWithToolLeases(ctx, svc, verifier, authorize, leaseProof, userAudience, humanAuthorize, true)
}

// NewCoexistingHandlerWithToolLeases allows existing task clients and requires
// lease proof for VM principals, without a downgrade to an unleased operation.
func NewCoexistingHandlerWithToolLeases(ctx context.Context, svc *tools.Service, verifier *auth.Verifier, authorize transport.Authorize,
	leaseProof *auth.Verifier, userAudience string, humanAuthorize transport.Authorize) (http.Handler, error) {
	return newHandlerWithToolLeases(ctx, svc, verifier, authorize, leaseProof, userAudience, humanAuthorize, false)
}

func newHandlerWithToolLeases(ctx context.Context, svc *tools.Service, verifier *auth.Verifier, authorize transport.Authorize,
	leaseProof *auth.Verifier, userAudience string, humanAuthorize transport.Authorize, strict bool) (http.Handler, error) {
	if leaseProof == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "toolsapi: State verifier required")
	}
	h, err := newHandler(svc, verifier, authorize, userAudience, humanAuthorize)
	if err != nil {
		return nil, err
	}
	if err := svc.MigrateToolLeases(ctx); err != nil {
		return nil, err
	}
	h.leaseProof = leaseProof
	h.strictLease = strict
	h.routes["FenceAndLookup"] = h.fenceAndLookup
	if strict {
		delete(h.routes, "Get")
		delete(h.routes, "GetResult")
		delete(h.routes, "Fence")
	}
	return h, nil
}

// NewHandlerWithUsers mounts the human-facing legs (Approve, Approvals) with
// the accepted transport.TaskOrUser pattern (DEF-TGW-2, director contract
// 2026-11-02): the sanctioned human credential of an Identity-composed front
// is minted for userAudience (browser flow), carries no workload binding, and
// is authorised through humanAuthorize with capability ApproveCapability. The
// task routes are byte-identical to NewHandler; a task token stays unauthorised
// for the human legs (humanAuthorize denies KindTask, and Service.Approve
// re-verifies the bearer as human).
func NewHandlerWithUsers(svc *tools.Service, verifier *auth.Verifier, authorize transport.Authorize,
	userAudience string, humanAuthorize transport.Authorize) (http.Handler, error) {
	return newHandler(svc, verifier, authorize, userAudience, humanAuthorize)
}

func newHandler(svc *tools.Service, verifier *auth.Verifier, authorize transport.Authorize,
	userAudience string, humanAuthorize transport.Authorize) (*Handler, error) {
	switch {
	case svc == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "toolsapi: service must not be nil")
	case verifier == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "toolsapi: verifier must not be nil")
	case authorize == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "toolsapi: authorize must not be nil")
	}
	humanGuard := transport.Human(verifier, audience)
	if userAudience != "" {
		if humanAuthorize == nil {
			return nil, contracts.NewError(contracts.ErrInvalid, "toolsapi: userAudience requires a human authorize callback")
		}
		humanGuard = transport.TaskOrUser(verifier, audience, userAudience, ApproveCapability, humanAuthorize)
	}
	oauthGuard := transport.Human(verifier, audience)
	if userAudience != "" {
		oauthGuard = transport.TaskOrUser(verifier, audience, userAudience, ConnectCapability, humanAuthorize)
	}
	h := &Handler{
		svc:        svc,
		task:       transport.Task(verifier, audience, capability, authorize),
		human:      humanGuard,
		oauthHuman: oauthGuard,
	}
	h.routes = map[string]func(http.ResponseWriter, *http.Request){
		"Call":             h.call,
		"Get":              h.get,
		"GetResult":        h.getResult,
		"Fence":            h.fence,
		"Approve":          h.approve,
		"Decline":          h.decline,
		"Approvals":        h.approvals,
		"ConnectionState":  h.connectionState,
		"OAuthConnections": h.oauth,
		"OAuthBegin":       h.oauth,
		"OAuthCallback":    h.oauth,
		"OAuthRevoke":      h.oauth,
	}
	return h, nil
}

// ServeHTTP dispatches /v1/tools/{Operation} to the route-specific handler,
// wrapping every task route in transport.Task and the human route in
// transport.Human. An unknown route or method is a 404.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "toolsapi: unknown route"))
		return
	}
	rest, ok := strings.CutPrefix(r.URL.Path, "/v1/tools/")
	if !ok || rest == "" || strings.Contains(rest, "/") {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "toolsapi: unknown route"))
		return
	}
	fn, ok := h.routes[rest]
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "toolsapi: unknown route"))
		return
	}
	// Approve authenticates a human; every other route authenticates a task.
	gate := h.task
	// The HITL worklist is one human-facing surface: signing (Approve), refusing
	// (Decline) and reading the queue (Approvals) share the accepted
	// human-or-user guard and its tools:approve sub-capability. A task token is
	// never authorised for any of the three.
	if rest == "Approve" || rest == "Decline" || rest == "Approvals" {
		gate = h.human
	}
	if strings.HasPrefix(rest, "OAuth") {
		gate = h.oauthHuman
	}
	gate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p, ok := transport.FromContext(r.Context()); ok && p.Claims.Kind == auth.KindTask && isLeasedTaskPrincipal(p.Claims) {
			switch rest {
			case "Call":
				if h.leaseProof == nil {
					transport.WriteError(w, r, leaseVerifierRequired())
					return
				}
			case "Get", "GetResult", "Fence":
				transport.WriteError(w, r, leaseVerifierRequired())
				return
			}
		}
		fn(w, r)
	})).ServeHTTP(w, r)
}

func isLeasedTaskPrincipal(claims auth.Claims) bool {
	return slices.Contains(claims.Capabilities, "tools.leased") || strings.HasPrefix(claims.Context, "vm-session:")
}

func leaseVerifierRequired() error {
	return contracts.NewError(contracts.ErrForbidden, "toolsapi: leased task principal requires a lease-verifying Tool Gateway")
}

// An internal leased ledger key must only be reachable through the lease
// authority. Public legacy keys are otherwise unaffected in coexistence mode.
func (h *Handler) blockInternalLeaseKey(w http.ResponseWriter, r *http.Request, key string) bool {
	if h.leaseProof == nil || !tools.IsLeasedOperationKey(key) {
		return false
	}
	transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "toolsapi: leased operation keys require lease proof"))
	return true
}

// scope reads the isolation scope from the verified principal.
func (h *Handler) scope(r *http.Request) (contracts.Scope, error) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
			"toolsapi: no verified principal on this request")
	}
	return p.Scope, nil
}

// call journals and runs one tool call keyed by an idempotency key. The scope is
// the verified principal's; key, tool and arguments come from the body. Arguments
// are forwarded to the service but NEVER echoed back.
func (h *Handler) call(w http.ResponseWriter, r *http.Request) {
	scope, err := h.scope(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		Key       string          `json:"key"`
		Tool      string          `json:"tool"`
		Arguments json.RawMessage `json:"arguments"`
		toolLeaseInput
	}
	if err := transport.ReadJSON(w, r, &in, maxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// Provenance for the effect is authored HERE, from the verified principal,
	// so the durable result can later prove which context paid for it. A body
	// field could be chosen by the caller and would prove nothing.
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"toolsapi: no verified principal on this request"))
		return
	}
	prov := tools.CallProvenance{TaskID: p.Claims.Task, ContextID: p.Claims.Context}
	var op operations.Operation
	hasLease := in.toolLeaseInput != (toolLeaseInput{})
	switch {
	case h.leaseProof == nil && hasLease:
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "toolsapi: lease-aware endpoint required"))
		return
	case h.leaseProof != nil && (h.strictLease || hasLease || isLeasedTaskPrincipal(p.Claims)):
		lease, verifyErr := h.verifiedToolLease(p.Claims, in.toolLeaseInput)
		if verifyErr != nil {
			transport.WriteError(w, r, verifyErr)
			return
		}
		op, err = h.svc.CallWithToolLease(r.Context(), scope, in.Key, in.Tool, in.Arguments, prov, lease)
	default:
		if h.blockInternalLeaseKey(w, r, in.Key) {
			return
		}
		op, err = h.svc.CallWithProvenance(r.Context(), scope, in.Key, in.Tool, in.Arguments, prov)
	}
	if err != nil {
		if errors.Is(err, tools.ErrAuthRequired) {
			transport.WriteJSON(w, http.StatusConflict, map[string]string{"code": "AUTH_REQUIRED", "reason": "AUTH_REQUIRED", "status": "auth-required", "message": "Подключите личный аккаунт в разделе подключений"})
			return
		}
		if errors.Is(err, tools.ErrApprovalDeclined) {
			// A refusal already given is not a refusal waiting for an answer:
			// report it explicitly so no one re-prompts the employee or retries
			// the same operation key.
			transport.WriteError(w, r, contracts.NewReasoned(contracts.ErrConflict, "APPROVAL_DECLINED",
				"toolsapi: the employee declined this operation; retry needs a new operation key and a fresh human decision"))
			return
		}
		if errors.Is(err, tools.ErrApprovalRequired) {
			transport.WriteError(w, r, contracts.NewReasoned(contracts.ErrConflict, "APPROVAL_REQUIRED",
				"toolsapi: human approval required for this canonical action digest"))
			return
		}
		// An operation was still journalled (FAILED / SIDE_EFFECT_UNKNOWN): the
		// terminal status is the observable outcome and carries no user content, so
		// it is returned rather than flattened into a 500.
		if op.Status != "" {
			transport.WriteJSON(w, http.StatusOK, view(r, op))
			return
		}
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, view(r, op))
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	scope, err := h.scope(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		Key string `json:"key"`
	}
	if err := transport.ReadJSON(w, r, &in, maxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if h.blockInternalLeaseKey(w, r, in.Key) {
		return
	}
	op, err := h.svc.Get(r.Context(), scope, in.Key)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, view(r, op))
}

// getResult releases the owner's stored result for one operation key. The
// reading authority is the VERIFIED principal and nothing else: its task and
// context come from verified claims (never from the body), so the caller
// cannot choose the binding its read is checked against.
func (h *Handler) getResult(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"toolsapi: no verified principal on this request"))
		return
	}
	var in struct {
		Key string `json:"key"`
	}
	if err := transport.ReadJSON(w, r, &in, maxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if h.blockInternalLeaseKey(w, r, in.Key) {
		return
	}
	res, err := h.svc.GetResult(r.Context(), p.Scope,
		tools.ResultReader{TaskID: p.Claims.Task, ContextID: p.Claims.Context}, in.Key)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, res)
}

func (h *Handler) fence(w http.ResponseWriter, r *http.Request) {
	scope, err := h.scope(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		Key string `json:"key"`
	}
	if err := transport.ReadJSON(w, r, &in, maxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if h.blockInternalLeaseKey(w, r, in.Key) {
		return
	}
	op, err := h.svc.Fence(r.Context(), scope, in.Key)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, view(r, op))
}

// approve binds a HUMAN principal to a pending approval for the canonical digest
// of {tool, arguments}. The presenting bearer must be a KindHuman token for this
// audience (enforced twice: transport.Human at the edge and Service.Approve in
// the journal), and the tenant is the human's own verified tenant.
func (h *Handler) approve(w http.ResponseWriter, r *http.Request) {
	scope, err := h.scope(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// Second human-only layer (defense in depth): under TaskOrUser a task bearer
	// could reach this route only if authorize were mis-wired; the principal
	// itself must be human here, and Service.Approve re-verifies the bearer.
	if p, ok := transport.FromContext(r.Context()); !ok || p.Claims.Kind != auth.KindHuman {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"toolsapi: approve is human-only"))
		return
	}
	var in struct {
		RequestID string `json:"request_id"`
		Digest    string `json:"digest"`
		Tool      string `json:"tool"`
	}
	if err := transport.ReadJSON(w, r, &in, maxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := bearer(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	who, err := h.svc.ApproveRequest(r.Context(), scope, in.RequestID, in.Digest, in.Tool, token)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{
		"approved":    true,
		"tool":        in.Tool,
		"digest":      in.Digest,
		"approved_by": who,
	})
}

// decline records a HUMAN refusal of one pending approval request (GAP-DOTE2E-7,
// PDF rev 3.2 §5.4: согласование приходит человеку, и человек вправе и отказать).
// It is the mirror of approve under the same human-only invariants: the bearer
// is re-verified as human by the service, the answer is bound to the exact
// operation (tenant, deployment, subject, operation key, digest, tool), and the
// refusal is journalled with attribution. Nothing here executes or discloses
// content: the response repeats only the identifiers the caller already had.
func (h *Handler) decline(w http.ResponseWriter, r *http.Request) {
	scope, err := h.scope(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if p, ok := transport.FromContext(r.Context()); !ok || p.Claims.Kind != auth.KindHuman {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"toolsapi: decline is human-only"))
		return
	}
	var in struct {
		RequestID string `json:"request_id"`
		Digest    string `json:"digest"`
		Tool      string `json:"tool"`
	}
	if err := transport.ReadJSON(w, r, &in, maxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := bearer(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	who, err := h.svc.DeclineRequest(r.Context(), scope, in.RequestID, in.Digest, in.Tool, token)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{
		"declined":    true,
		"tool":        in.Tool,
		"digest":      in.Digest,
		"declined_by": who,
	})
}

// approvals lists this tenant's PENDING (unbound) approval requests -- the
// human approver's worklist. Content-free: identifiers, tool alias, timestamp.
func (h *Handler) approvals(w http.ResponseWriter, r *http.Request) {
	scope, err := h.scope(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if p, ok := transport.FromContext(r.Context()); !ok || p.Claims.Kind != auth.KindHuman {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"toolsapi: the approvals worklist is human-only"))
		return
	}
	var in struct {
		Limit int `json:"limit"`
	}
	if r.Body != nil && r.ContentLength != 0 {
		if err := transport.ReadJSON(w, r, &in, maxBody); err != nil {
			transport.WriteError(w, r, err)
			return
		}
	}
	list, err := h.svc.ListPendingApprovals(r.Context(), scope)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if in.Limit > 0 && in.Limit < len(list) {
		list = list[:in.Limit]
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"approvals": list})
}

// connectionState answers with the configured MCP connection names and their
// configured/ready state as an enum. It exposes no endpoint URLs and no
// credentials -- identifiers and an enum only.
func (h *Handler) connectionState(w http.ResponseWriter, r *http.Request) {
	scope, err := h.scope(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	_ = scope
	names := h.svc.Connections()
	states := make([]map[string]any, 0, len(names))
	for _, n := range names {
		states = append(states, map[string]any{"connection": n, "state": "configured"})
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"connections": states})
}

// view projects an operation into a content-free response: identifiers, an enum
// status and numbers. The durable result payload (which may carry tool output)
// is intentionally excluded.
// view is the caller-visible shape of an operation. It carries no arguments
// and no tool output; the one exception is the journalled failure summary, which
// exists precisely so a human surface can explain a failed call, and the trace
// id that lets an operator find the server-side record of it.
func view(r *http.Request, op operations.Operation) map[string]any {
	out := map[string]any{
		"key":        op.Key,
		"kind":       op.Kind,
		"target":     op.Target,
		"digest":     op.Digest,
		"status":     op.Status,
		"version":    op.Version,
		"created_at": op.CreatedAt,
		"updated_at": op.UpdatedAt,
		"scope":      op.Scope,
	}
	// The failure summary carries the trace of the request that failed (not of
	// this read), so an operator can find the full server-side cause.
	if f, ok := tools.FailureFromPayload(op.Result); ok {
		out["failure"] = f
	}
	return out
}

// bearer extracts the raw bearer token from the Authorization header so it can
// be forwarded to Service.Approve for human re-verification.
func bearer(r *http.Request) (string, error) {
	const p = "Bearer "
	raw := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(raw, p) {
		return "", contracts.NewError(contracts.ErrForbidden, "toolsapi: bearer token required")
	}
	return strings.TrimSpace(strings.TrimPrefix(raw, p)), nil
}
