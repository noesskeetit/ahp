package stateapi

import (
	"net/http"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func (h *Handler) createThread(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in createThreadRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	thread, err := h.store.CreateThread(r.Context(), scope, in.ContextID, in.Key)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, thread)
}

func (h *Handler) getThread(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in getThreadRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	thread, err := h.store.GetThread(r.Context(), scope, in.ThreadID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, thread)
}

func (h *Handler) getThreadByContext(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in getThreadByContextRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	thread, err := h.store.GetThreadByContext(r.Context(), scope, in.ContextID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, thread)
}

func (h *Handler) createRun(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"stateapi: no verified principal on this request"))
		return
	}
	var in createRunRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// The run's task is bound to the presenting token: a body task_id must
	// match Claims.Task, and an absent one defaults to it.
	taskID := in.TaskID
	if taskID == "" {
		taskID = p.Claims.Task
	} else if taskID != p.Claims.Task {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"stateapi: task_id does not match the presenting task token"))
		return
	}
	run, err := h.store.CreateRun(r.Context(), p.Scope, in.ThreadID, taskID, in.Key, in.Payload)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, run)
}

func (h *Handler) getRun(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in getRunRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	run, err := h.store.GetRun(r.Context(), scope, in.RunID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, run)
}

func (h *Handler) listRuns(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in listRunsRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	runs, err := h.store.ListRuns(r.Context(), scope, in.ThreadID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string][]state.Run{"runs": runs})
}

func (h *Handler) heartbeat(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in heartbeatRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	d, err := ttl(in.TTLms)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	expires, err := h.store.Heartbeat(r.Context(), scope, in.RunID, in.Worker, in.Epoch, d)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, heartbeatResponse{ExpiresAt: expires})
}

func (h *Handler) release(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in releaseRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// The reason travels on the SAME call that ends the run: one transition, one
	// write, so a run cannot be terminal while its verdict is still in flight.
	if err := h.store.ReleaseWithReasonCause(r.Context(), scope, in.RunID, in.Worker, in.Epoch,
		in.Status, in.CloseReason, in.FailureCause); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) suspend(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in suspendRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.Suspend(r.Context(), scope, in.RunID, in.Worker, in.Epoch, in.WaitFor, in.Deadline); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

// listThreadsRequest carries only pagination: the isolation scope is taken from
// the token, never from the body (transport.ReadJSON rejects any scope/tenant/
// subject/deployment key).
type listThreadsRequest struct {
	Cursor string `json:"cursor"`
	Limit  int    `json:"limit"`
}

// listThreadsResponse is one durable page: the newest-first threads in scope,
// the opaque next cursor ("" when exhausted) and an explicit has_more flag so
// clients need not infer continuation from an empty page.
type listThreadsResponse struct {
	Threads    []state.Thread `json:"threads"`
	NextCursor string         `json:"next_cursor"`
	HasMore    bool           `json:"has_more"`
}

// listThreads returns the caller's own threads, newest first, with durable
// keyset pagination. Isolation is structural: scope is the verified principal,
// so a task token only ever enumerates the threads of its own
// tenant/deployment/subject.
func (h *Handler) listThreads(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in listThreadsRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	threads, next, err := h.store.ListThreads(r.Context(), scope, in.Cursor, in.Limit)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, listThreadsResponse{
		Threads:    threads,
		NextCursor: next,
		HasMore:    next != "",
	})
}

// pruneCheckpointHistory applies the checkpoint history bound of one thread on
// behalf of the control plane. Retention is an administrative act: PDF rev3.2
// puts retention on the internal AHP control-plane route and states that a call
// with a task token must be refused (accept/space-pdf-rev32.txt:1787-1792,
// :1918-1920), so this operation is reachable only through /v1/state-admin/
// with a human administrative credential, never through /v1/state/.
//
// The response is the store's report verbatim, refusals included: an operator
// has to be able to see what the bound refused to delete and why.
func (h *Handler) pruneCheckpointHistory(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in state.CheckpointPruneRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	report, err := h.store.PruneCheckpointHistory(r.Context(), scope, in)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, report)
}
