// Package memoryapi is the thin HTTP facade over the Memory store.
//
// Identity rules match the artifact and workspace APIs: the isolation scope
// always comes from transport.FromContext (mTLS + bound task token); request
// bodies never carry tenant/subject/deployment; the fine-grained visibility and
// lifecycle decisions (review capability, per-record read, label narrowing,
// tombstone) live in the store and are re-checked on every call. The facade
// only enforces the coarse "memory" capability at the gateway.
package memoryapi

import (
	"context"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	audience     = "ahp"
	capability   = "memory"
	maxBodyBytes = 1 << 20
)

// Handler is the mounted Memory API: POST /v1/memory/{Operation}.
type Handler struct {
	store      *memory.Store
	task       func(http.Handler) http.Handler
	operations map[string]func(http.ResponseWriter, *http.Request)
}

// New wires the facade. Every dependency is mandatory.
func New(store *memory.Store, verifier *auth.Verifier, authorize transport.Authorize) (http.Handler, error) {
	switch {
	case store == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "memoryapi: store must not be nil")
	case verifier == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "memoryapi: verifier must not be nil")
	case authorize == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "memoryapi: authorize must not be nil")
	}
	h := &Handler{store: store, task: transport.Task(verifier, audience, capability, authorize)}
	h.operations = map[string]func(http.ResponseWriter, *http.Request){
		"Propose":       h.propose,
		"Search":        h.search,
		"Reindex":       h.reindex,
		"Read":          h.read,
		"Get":           h.getOperation,
		"Review":        h.review,
		"AddLabels":     h.addLabels,
		"Delete":        h.delete,
		"DeleteSubject": h.deleteSubject,
		"Fence":         h.fenceOperation,
	}
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "memoryapi: unknown route"))
		return
	}
	rest, ok := strings.CutPrefix(r.URL.Path, "/v1/memory/")
	if !ok || rest == "" || strings.Contains(rest, "/") {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "memoryapi: unknown route"))
		return
	}
	fn, ok := h.operations[rest]
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "memoryapi: unknown route"))
		return
	}
	h.task(http.HandlerFunc(fn)).ServeHTTP(w, r)
}

func principal(r *http.Request) (contracts.Scope, error) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
			"memoryapi: no verified principal on this request")
	}
	return p.Scope, nil
}

var okBody = map[string]bool{"ok": true}

type operationRequest struct {
	OpKey string `json:"op_key"`
}

func (h *Handler) getOperation(w http.ResponseWriter, r *http.Request) {
	h.operation(w, r, h.store.GetOperation)
}
func (h *Handler) fenceOperation(w http.ResponseWriter, r *http.Request) {
	h.operation(w, r, h.store.Fence)
}
func (h *Handler) operation(w http.ResponseWriter, r *http.Request, f func(context.Context, contracts.Scope, string) (memory.Operation, error)) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in operationRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	out, err := f(r.Context(), scope, in.OpKey)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, out)
}

type idRequest struct {
	ID    string `json:"id"`
	OpKey string `json:"op_key,omitempty"`
}

type readRequest struct {
	ID        string `json:"id"`
	Workspace string `json:"workspace,omitempty"`
}

type reviewRequest struct {
	OpKey    string `json:"op_key"`
	ID       string `json:"id"`
	Decision string `json:"decision"`
}

type addLabelsRequest struct {
	ID     string     `json:"id"`
	OpKey  string     `json:"op_key"`
	Labels labels.Set `json:"labels"`
}

type deleteSubjectRequest struct {
	OpKey   string `json:"op_key"`
	Subject string `json:"subject"`
}

type deleteSubjectResult struct {
	Deleted int `json:"deleted"`
}

func (h *Handler) propose(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in memory.ProposeRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	res, err := h.store.Propose(r.Context(), scope, in)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, res)
}

func (h *Handler) search(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in memory.SearchRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	res, err := h.store.Search(r.Context(), scope, in)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, res)
}

func (h *Handler) read(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in readRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var m memory.Memory
	if in.Workspace != "" {
		m, err = h.store.GetInWorkspace(r.Context(), scope, in.ID, in.Workspace)
	} else {
		m, err = h.store.Get(r.Context(), scope, in.ID)
	}
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, m)
}

func (h *Handler) review(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in reviewRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	m, err := h.store.Review(r.Context(), scope, in.ID, in.Decision, in.OpKey)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, m)
}

func (h *Handler) addLabels(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in addLabelsRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	m, err := h.store.AddLabels(r.Context(), scope, in.ID, in.OpKey, in.Labels)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, m)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in idRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.Delete(r.Context(), scope, in.ID, in.OpKey); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) deleteSubject(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in deleteSubjectRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	n, err := h.store.DeleteSubject(r.Context(), scope, in.Subject, in.OpKey)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, deleteSubjectResult{Deleted: n})
}

func (h *Handler) reindex(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in memory.ReindexRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	out, err := h.store.Reindex(r.Context(), scope, in)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, out)
}
