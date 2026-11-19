// Package computeapi is the thin, identity-scoped HTTP facade over the Compute
// plane (class S/C/X computers, disposable incarnations, volume fencing,
// activity leases, the durable command ledger, snapshots, environment and
// project copy-on-write revisions, and artifact exports).
//
// Identity rules (non-negotiable, mirroring internal/stateapi):
//   - the isolation scope is ALWAYS transport.FromContext; request bodies may
//     never carry scope/tenant/subject/deployment fields (strict JSON rejects
//     unknown keys);
//   - every route demands mTLS plus a bound task token via transport.Task;
//   - nothing is executed directly here: state transitions go through the
//     durable *compute.Store, and execution/PTY/export go through the
//     *compute.Runner, which performs the AcquireVM -> Mount(fenced) -> Exec ->
//     Save/Export -> DestroyVM lifecycle and the SIDE_EFFECT_UNKNOWN fencing.
//
// Compute defines two HTTP statuses beyond the shared transport map: a
// copy-on-write commit clash is WORKSPACE_CONFLICT (409) and a backend that
// cannot run in this environment (for example class X without KVM) is
// UNAVAILABLE (503). Both are surfaced by writeError, not the default 500.
package computeapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	// audience is the fixed token audience of the Compute API.
	audience = "ahp"
	// capability is the capability authorize is consulted with on every route.
	capability = "compute"
	// apiPrefix is the mount point; routes are POST {prefix}{Operation}.
	apiPrefix = "/v1/compute/"
	// maxBodyBytes bounds every request body.
	maxBodyBytes = 1 << 20
	// defaultLeaseTTL is used when an AcquireLease request omits ttl_ms.
	defaultLeaseTTL = 60 * time.Second
)

// Handler is the mounted Compute API: POST /v1/compute/{Operation}.
type Handler struct {
	store  *compute.Store
	runner *compute.Runner
	// sessions is the MOCK PTY/browser facade. Nil on every deployment that has not
	// opted into it, which the session routes answer with 503 and a reason rather than
	// an empty-looking session.
	sessions   *compute.SessionService
	authorize  transport.Authorize
	task       func(http.Handler) http.Handler
	operations map[string]func(http.ResponseWriter, *http.Request)
}

// New wires the facade. store/verifier/authorize are mandatory and fail closed
// at construction time; runner may be nil, in which case the store-only routes
// keep working while Run/Export answer FORBIDDEN (a read-only deployment).
func New(store *compute.Store, verifier *auth.Verifier, authorize transport.Authorize, runner *compute.Runner) (http.Handler, error) {
	return NewWithOptions(store, verifier, authorize, runner, Options{})
}

// Options carries the optional faces a mount may add. It exists so mounting the mock
// session facade does not change the signature every existing caller of New compiles
// against, and so "which extra face is on this plane" is one readable value.
type Options struct {
	// Sessions mounts the PTY/browser session facade. It must be a MOCK service; the
	// type is that service, so a non-mock implementation cannot be passed by accident.
	Sessions *compute.SessionService
}

// NewWithOptions wires the facade with the optional faces the mount asked for.
func NewWithOptions(store *compute.Store, verifier *auth.Verifier, authorize transport.Authorize,
	runner *compute.Runner, opts Options) (http.Handler, error) {
	switch {
	case store == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "computeapi: store must not be nil")
	case verifier == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "computeapi: verifier must not be nil")
	case authorize == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "computeapi: authorize must not be nil")
	}
	h := &Handler{
		store:     store,
		runner:    runner,
		sessions:  opts.Sessions,
		authorize: authorize,
		task:      transport.Task(verifier, audience, capability, authorize),
	}
	h.operations = map[string]func(http.ResponseWriter, *http.Request){
		"CreateComputer":       h.createComputer,
		"GetComputer":          h.getComputer,
		"AcquireIncarnation":   h.acquireIncarnation,
		"StopIncarnation":      h.stopIncarnation,
		"Destroy":              h.destroy,
		"Exec":                 h.exec,
		"Run":                  h.run,
		"MarkDispatched":       h.markDispatched,
		"Complete":             h.complete,
		"GetCommand":           h.getCommand,
		"Fence":                h.fence,
		"Cancel":               h.cancel,
		"Restart":              h.restart,
		"CommitWorkspace":      h.commitWorkspace,
		"WorkspaceHead":        h.workspaceHead,
		"EnsureEnvironment":    h.ensureEnvironment,
		"BeginInstall":         h.beginInstall,
		"FinishInstall":        h.finishInstall,
		"CurrentEnvironment":   h.currentEnvironment,
		"Snapshot":             h.snapshot,
		"Export":               h.export,
		"AcquireLease":         h.acquireLease,
		"ReleaseLease":         h.releaseLease,
		"CurrentFence":         h.currentFence,
		"TypedPatch":           h.typedPatch,
		"StateHead":            h.stateHead,
		"StateRevisions":       h.stateRevisions,
		"DeriveBranch":         h.deriveBranch,
		"MergeBranch":          h.mergeBranch,
		"Sleep":                h.sleep,
		"Wake":                 h.wake,
		"CostReport":           h.costReport,
		"TakeWriterLease":      h.takeWriterLease,
		"ReleaseWriterLease":   h.releaseWriterLease,
		"AcquireActivityLease": h.acquireActivityLease,
		"ReleaseActivityLease": h.releaseActivityLease,
		"ActivityLeases":       h.activityLeases,
		"RunInstall":           h.runInstall,
		"ListEnvironment":      h.listEnvironment,
		"PublishVersion":       h.publishVersion,
		"DrainVersion":         h.drainVersion,
		"RollbackVersion":      h.rollbackVersion,
		"GetActiveVersion":     h.getActiveVersion,
		// Session facade (MOCK): PTY sessions and browser workers. Registered always,
		// so the face is present and refuses honestly when no facade is mounted,
		// instead of answering from the SPA fallback as an unmounted route would.
		"OpenPTY":       h.openPTY,
		"PTYInput":      h.ptyInput,
		"PTYOutput":     h.ptyOutput,
		"ClosePTY":      h.closeSession,
		"OpenBrowser":   h.openBrowser,
		"BrowserAction": h.browserAction,
		"CloseBrowser":  h.closeSession,
	}
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		unknownRoute(w, r)
		return
	}
	rest, ok := strings.CutPrefix(r.URL.Path, apiPrefix)
	if !ok {
		unknownRoute(w, r)
		return
	}
	fn, ok := h.operations[rest]
	if !ok {
		unknownRoute(w, r)
		return
	}
	h.task(http.HandlerFunc(fn)).ServeHTTP(w, r)
}

func unknownRoute(w http.ResponseWriter, r *http.Request) {
	transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "computeapi: unknown route"))
}

// principal extracts the middleware-installed scope; handlers never accept
// identity from any other source.
func principal(r *http.Request) (contracts.Scope, error) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
			"computeapi: no verified principal on this request")
	}
	return p.Scope, nil
}

// principalTask returns the bound task id from the verified principal, or ""
// when the caller is not a task principal. Export binds it as a trusted label.
func principalTask(r *http.Request) string {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		return ""
	}
	return p.Claims.Task
}

// writeError extends the shared transport mapping with Compute's two extra
// statuses: WORKSPACE_CONFLICT is a 409 copy-on-write clash, and an
// UnavailableError is a 503 (the substrate cannot serve this class right now,
// e.g. class X without KVM). Everything else defers to transport.WriteError.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case err == nil:
		return
	case contracts.Code(err) == compute.ErrWorkspaceConflict:
		transport.WriteJSON(w, http.StatusConflict,
			&contracts.Error{Code: compute.ErrWorkspaceConflict, Message: err.Error()})
	case contracts.Code(err) == compute.ErrStaleHash:
		transport.WriteJSON(w, http.StatusConflict,
			&contracts.Error{Code: compute.ErrStaleHash, Message: err.Error()})
	case compute.IsUnavailable(err):
		transport.WriteJSON(w, http.StatusServiceUnavailable,
			&contracts.Error{Code: "UNAVAILABLE", Message: err.Error()})
	default:
		transport.WriteError(w, r, err)
	}
}
