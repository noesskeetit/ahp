// Package artifactsapi is the thin HTTP facade over the artifact store.
//
// Identity rules (non-negotiable):
//   - the isolation scope is ALWAYS transport.FromContext; request bodies may
//     never carry scope/tenant/subject/deployment fields (strict JSON rejects
//     unknown keys);
//   - every route demands mTLS plus a bound task token via transport.Task and
//     is consulted once with the coarse "artifacts" capability; the fine-
//     grained per-artifact decision (labels, ownership, workspace membership)
//     lives in the store, which is re-checked on every call;
//   - Download streams the exact sealed object version and honours Range; it
//     never returns a presigned URL. Upload uses the presigned PUT URL that
//     BeginUpload returns to the caller only.
package artifactsapi

import (
	"context"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	// audience is the fixed token audience of the Artifact API.
	audience = "ahp"
	// capability is the coarse capability authorize is consulted with.
	capability = "artifacts"
	// maxBodyBytes bounds every request body.
	maxBodyBytes = 1 << 20
)

// Handler is the mounted Artifact API: POST /v1/artifacts/{Operation}.
type Handler struct {
	store      *artifacts.Store
	gc         *artifacts.GC
	task       func(http.Handler) http.Handler
	operations map[string]func(http.ResponseWriter, *http.Request)
}

// New wires the facade. Every dependency is mandatory: a nil anywhere fails
// closed at construction time. The GC options configure the storage collector
// (see internal/artifacts/gc.go); without a Liveness option only registered
// pins protect a live reference, which is the fail-closed default.
func New(store *artifacts.Store, verifier *auth.Verifier, authorize transport.Authorize, gcOpts ...artifacts.GCOption) (http.Handler, error) {
	switch {
	case store == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "artifactsapi: store must not be nil")
	case verifier == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "artifactsapi: verifier must not be nil")
	case authorize == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "artifactsapi: authorize must not be nil")
	}
	gc, err := artifacts.NewGC(store, gcOpts...)
	if err != nil {
		return nil, err
	}
	h := &Handler{
		store: store,
		gc:    gc,
		task:  transport.Task(verifier, audience, capability, authorize),
	}
	h.operations = map[string]func(http.ResponseWriter, *http.Request){
		"TransferGrant":         h.transferGrant,
		"CopyFromTransferGrant": h.copyFromTransferGrant,
		"BeginUpload":           h.beginUpload,
		"Seal":                  h.seal,
		"Get":                   h.get,
		"Resolve":               h.resolve,
		"Download":              h.download,
		"DownloadGrant":         h.downloadGrant,
		"OpenDownloadGrant":     h.openDownloadGrant,
		"Delete":                h.delete,
		"Purge":                 h.purge,
		"SetRetention":          h.setRetention,
		"SetLegalHold":          h.setLegalHold,
		"MarkDerivation":        h.markDerivation,
		"Readiness":             h.readiness,
		"Process":               h.process,
		"ReadTextChunk":         h.readTextChunk,
		"SearchText":            h.searchText,
		"GetLifecycle":          h.getLifecycle,
		"CheckPublication":      h.checkPublication,
		"Publish":               h.publish,
		"DeleteByContext":       h.deleteByContext,
		"DeleteByTasks":         h.deleteByTasks,
		"DeleteBySubject":       h.deleteBySubject,
		"GC":                    h.gcRun,
		"Restore":               h.restore,
		"PinRef":                h.pinRef,
		"GrantContext":          h.grantContext,
		"RevokeContext":         h.revokeContext,
		"GetContextGrant":       h.getContextGrant,
		"UnpinRef":              h.unpinRef,
		"ListPins":              h.listPins,
		"GCAudit":               h.gcAudit,
	}
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "artifactsapi: unknown route"))
		return
	}
	rest, ok := strings.CutPrefix(r.URL.Path, "/v1/artifacts/")
	if !ok || strings.Contains(rest, "/") {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "artifactsapi: unknown route"))
		return
	}
	fn, ok := h.operations[rest]
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "artifactsapi: unknown route"))
		return
	}
	h.task(http.HandlerFunc(fn)).ServeHTTP(w, r)
}

// principal extracts the middleware-installed scope; handlers never accept
// identity from any other source.
func principal(r *http.Request) (contracts.Scope, error) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
			"artifactsapi: no verified principal on this request")
	}
	return p.Scope, nil
}

// readRequest is principal plus the caller's context claim installed on the
// request context, which is what the store's context-scoped read gate reads.
// The claim comes from the VERIFIED token: a caller that could name its own
// context would name the victim's instead.
func readRequest(r *http.Request) (context.Context, contracts.Scope, error) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		return nil, contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
			"artifactsapi: no verified principal on this request")
	}
	// The per-session human authority (ctx_scope=authority) is scoped to the
	// subject and tenant, not one conversation, so it downloads its own
	// artifacts across conversations. Every other credential keeps its context
	// claim, so its read stays bound to exactly its own conversation.
	callerCtx := p.Claims.Context
	if p.Claims.CtxScope == auth.CtxScopeAuthority {
		callerCtx = ""
	}
	return artifacts.WithCallerContext(r.Context(), callerCtx), p.Scope, nil
}

var okBody = map[string]bool{"ok": true}
