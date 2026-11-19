package stateapi

// Research-only common-record operations. They reuse task middleware and bind
// each request to the presenting task/context, never to record owner_hint.
import (
	"net/http"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type commitRecordsRequest struct {
	Fence state.Fence `json:"fence"`
	state.CommonCommit
}

func (h *Handler) commitRecords(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "missing task principal"))
		return
	}
	var in commitRecordsRequest
	if err := transport.ReadJSON(w, r, &in, 9<<20); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	run, err := h.store.GetRun(r.Context(), p.Scope, in.Fence.RunID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	thread, err := h.store.GetThread(r.Context(), p.Scope, run.ThreadID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if p.Claims.Task != run.TaskID || p.Claims.Context != thread.ContextID {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "common run is outside task/context"))
		return
	}
	out, err := h.store.CommitRecords(r.Context(), p.Scope, in.Fence, in.CommonCommit)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, out)
}

type readRecordsRequest struct {
	ThreadID  string `json:"thread_id"`
	Namespace string `json:"namespace"`
	Revision  int64  `json:"revision,omitempty"`
}

func (h *Handler) readRecords(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "missing task principal"))
		return
	}
	var in readRecordsRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	thread, err := h.store.GetThread(r.Context(), p.Scope, in.ThreadID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if p.Claims.Context != thread.ContextID {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "common thread is outside task context"))
		return
	}
	out, err := h.store.ReadRecords(r.Context(), p.Scope, in.ThreadID, in.Namespace, in.Revision)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, out)
}

// A fresh thread has common:null and ordered KV entries. Historical cut +
// current shared values is deliberately not offered as a coherent snapshot.
type readRecordsBundleRequest struct {
	KnownRevision int64          `json:"known_revision,omitempty"`
	ThreadID      string         `json:"thread_id"`
	Namespace     string         `json:"namespace"`
	Keys          []state.KVRead `json:"keys"`
}

func (h *Handler) readRecordsBundle(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "missing task principal"))
		return
	}
	var in readRecordsBundleRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	thread, err := h.store.GetThread(r.Context(), p.Scope, in.ThreadID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if p.Claims.Context != thread.ContextID {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "common thread is outside task context"))
		return
	}
	out, err := h.store.ReadRecordsBundleSince(r.Context(), p.Scope, in.ThreadID, in.Namespace, in.Keys, in.KnownRevision)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, out)
}
