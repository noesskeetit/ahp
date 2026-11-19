package stateapi

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"net/http"
)

// DeletionCoordinator is called only inside an authenticated State handler,
// using its verified target scope, including the admin target on DeleteSubject.
// Ready must fail before the State mutation when an owner is unavailable.
type DeletionCoordinator interface {
	Ready(context.Context, contracts.Scope, string) error
	Complete(context.Context, contracts.Scope, string, string) error
}

// WithDeletionCoordinator configures a newly constructed facade. The root must
// call it before serving; it does not add an unauthenticated retry route.
func WithDeletionCoordinator(handler http.Handler, coordinator DeletionCoordinator) (http.Handler, error) {
	h, ok := handler.(*Handler)
	if !ok || coordinator == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "stateapi: handler and deletion coordinator required")
	}
	h.deletion = coordinator
	return h, nil
}

func (h *Handler) writeDeletionFailure(w http.ResponseWriter, r *http.Request, scope contracts.Scope, operation, resource string, err error) {
	receipt, readErr := h.store.GetDeletion(r.Context(), scope, operation, resource)
	if readErr != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusServiceUnavailable, map[string]any{"code": contracts.ErrUnavailable, "message": "deletion is pending; retry the same operation", "deletion_id": receipt.ID, "state": "pending", "artifacts_done": receipt.ArtifactsDone, "memory_done": receipt.MemoryDone})
}

func (h *Handler) controlDeleteSubject(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		Key string `json:"key"`
	}
	if err = transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	control, ok := h.deletion.(interface {
		ControlDeleteSubject(context.Context, contracts.Scope, string) (map[string]any, error)
	})
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrUnavailable, "stateapi: global subject deletion is not configured"))
		return
	}
	result, err := control.ControlDeleteSubject(r.Context(), scope, in.Key)
	if err != nil {
		if result != nil {
			transport.WriteJSON(w, http.StatusServiceUnavailable, result)
		} else {
			transport.WriteError(w, r, err)
		}
		return
	}
	transport.WriteJSON(w, http.StatusOK, result)
}
func (h *Handler) controlDeleteTenant(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		Key string `json:"key"`
	}
	if err = transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	control, ok := h.deletion.(interface {
		ControlDeleteTenant(context.Context, contracts.Scope, string) (map[string]any, error)
	})
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrUnavailable, "stateapi: tenant deletion is not configured"))
		return
	}
	result, err := control.ControlDeleteTenant(r.Context(), scope, in.Key)
	if err != nil {
		if result != nil {
			transport.WriteJSON(w, http.StatusServiceUnavailable, result)
		} else {
			transport.WriteError(w, r, err)
		}
		return
	}
	transport.WriteJSON(w, http.StatusOK, result)
}
