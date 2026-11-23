package statejournal

import (
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const coordinatorRoot = "/internal/v1/state/coordinator"

func mountCoordinatorRoutes(parent *http.ServeMux, store *Store, authorize transport.Authorize, workloadIDs []string) {
	allowed := slices.Clone(workloadIDs)
	for i, workload := range allowed {
		allowed[i] = strings.TrimSpace(workload)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET "+coordinatorRoot+"/vm-reconcile", readRoute(func(r *http.Request, _ transport.Principal) (any, error) {
		query := r.URL.Query()
		var cursor *journal.VMReconcileCursor
		if query.Has("after") || query.Has("after_tenant") || query.Has("after_session") {
			at, err := time.Parse(time.RFC3339Nano, query.Get("after"))
			if err != nil {
				return nil, invalidHTTP("invalid VM reconcile cursor")
			}
			cursor = &journal.VMReconcileCursor{At: at, TenantID: query.Get("after_tenant"), SessionID: query.Get("after_session")}
		}
		return store.ListGlobalRecoverableVMPage(r.Context(), query.Get("status"), cursor, 64)
	}))
	const sessions = coordinatorRoot + "/scopes/{tenant}/{deployment}/{subject}/sessions"
	mux.HandleFunc("POST "+sessions+":enqueue", jsonRoute(func(r *http.Request, _ transport.Principal, req EnqueueRequest) (any, error) {
		if req.ExecutionMode != journal.ExecutionVMManaged {
			return nil, invalidHTTP("coordination requires a VM-managed session")
		}
		return store.EnqueueTurn(r.Context(), coordinatorTargetScope(r), req)
	}))
	mux.HandleFunc("GET "+sessions+"/{session}", readRoute(func(r *http.Request, _ transport.Principal) (any, error) {
		scope, session := coordinatorTargetScope(r), r.PathValue("session")
		if r.URL.Query().Get("view") == "resume" {
			return store.inspectSession(r.Context(), scope, session, true)
		}
		return store.getTurnStatus(r.Context(), scope, session, true)
	}))
	mux.HandleFunc("GET "+sessions+"/{session}/events", readRoute(func(r *http.Request, caller transport.Principal) (any, error) {
		return readSessionEvents(r, caller, store, authorize, coordinatorTargetScope(r), true)
	}))
	mux.HandleFunc("POST "+sessions+"/{session}/vm:cancel", jsonRoute(func(r *http.Request, _ transport.Principal, req struct {
		TurnSeq int64 `json:"turn_seq,omitempty"`
	}) (any, error) {
		return store.CancelTurn(r.Context(), coordinatorTargetScope(r), r.PathValue("session"), req.TurnSeq)
	}))
	mux.HandleFunc("POST "+sessions+"/{session}/vm:requeue-fenced", jsonRoute(func(r *http.Request, _ transport.Principal, req journal.FenceRequeueRequest) (any, error) {
		return struct{}{}, store.RequeueFencedVMTurn(r.Context(), coordinatorTargetScope(r), r.PathValue("session"), req)
	}))
	guard := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caller, ok := transport.FromContext(r.Context())
		if !ok || !slices.Contains(allowed, auth.WorkloadID(caller.Scope)) {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "workload is not a State coordinator"))
			return
		}
		if err := authorize(r.Context(), caller.Claims, "state.coordinate"); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		mux.ServeHTTP(w, r)
	})
	parent.Handle(coordinatorRoot, guard)
	parent.Handle(coordinatorRoot+"/", guard)
}

func coordinatorTargetScope(r *http.Request) contracts.Scope {
	return contracts.Scope{
		Tenant: r.PathValue("tenant"), Deployment: r.PathValue("deployment"), Subject: r.PathValue("subject"),
	}
}
