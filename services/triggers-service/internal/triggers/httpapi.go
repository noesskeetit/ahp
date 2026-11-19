package triggers

import (
	"net/http"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// triggersAudience and triggersCapability mirror the accepted State face so the
// composed front guards this face with the same authorize + verifier contract.
const (
	triggersAudience   = "ahp"
	triggersCapability = "triggers"
	triggersMaxBody    = 1 << 20
)

// HTTPHandler exposes the accepted Triggers Service over HTTP for the composed
// front. It is a thin route adapter: every handler takes the caller's scope from
// the VERIFIED principal installed by transport.Task -- never from a header or
// the body -- and calls an existing, already-accepted Service method verbatim.
type HTTPHandler struct {
	svc       *Service
	task      func(http.Handler) http.Handler
	routes    map[string]func(http.ResponseWriter, *http.Request)
	authorize transport.Authorize
}

// NewHTTPHandler builds the triggers HTTP face. authorize may not be nil.
func NewHTTPHandler(svc *Service, verifier *auth.Verifier, authorize transport.Authorize) (http.Handler, error) {
	switch {
	case svc == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "triggers: service must not be nil")
	case verifier == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "triggers: verifier must not be nil")
	case authorize == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "triggers: authorize must not be nil")
	}
	h := &HTTPHandler{
		svc:       svc,
		authorize: authorize,
		task:      transport.Task(verifier, triggersAudience, triggersCapability, authorize),
	}
	h.routes = map[string]func(http.ResponseWriter, *http.Request){
		"CreateSchedule": h.createSchedule,
		"CreateWebhook":  h.createWebhook,
		"List":           h.list,
		"Get":            h.get,
		"Revoke":         h.revoke,
		"FireNow":        h.fireNow,
	}
	return h, nil
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.Path, "/v1/triggers/")
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "triggers: unknown route"))
		return
	}
	fn, ok := h.routes[rest]
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "triggers: unknown route"))
		return
	}
	h.task(http.HandlerFunc(fn)).ServeHTTP(w, r)
}

func (h *HTTPHandler) principal(r *http.Request) (contracts.Scope, error) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
			"triggers: no verified principal on this request")
	}
	return p.Scope, nil
}

func (h *HTTPHandler) createSchedule(w http.ResponseWriter, r *http.Request) {
	scope, err := h.principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var spec CreateScheduleSpec
	if err := transport.ReadJSON(w, r, &spec, triggersMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.bindCreateAuthority(r, &spec.ParentTask, &spec.DelegationRef); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	trig, err := h.svc.CreateSchedule(r.Context(), scope, spec)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, trig)
}

func (h *HTTPHandler) createWebhook(w http.ResponseWriter, r *http.Request) {
	scope, err := h.principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var spec CreateWebhookSpec
	if err := transport.ReadJSON(w, r, &spec, triggersMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.bindCreateAuthority(r, &spec.ParentTask, &spec.DelegationRef); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	trig, err := h.svc.CreateWebhook(r.Context(), scope, spec)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, trig)
}

// list answers GET or POST; a GET carries no body, so the caller's scope is all
// that is read, straight from the verified principal.
func (h *HTTPHandler) list(w http.ResponseWriter, r *http.Request) {
	scope, err := h.principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if r.Method != http.MethodGet {
		var in struct{}
		if err := transport.ReadJSON(w, r, &in, triggersMaxBody); err != nil {
			transport.WriteError(w, r, err)
			return
		}
	}
	trigs, err := h.svc.List(r.Context(), scope)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"triggers": trigs})
}

func (h *HTTPHandler) get(w http.ResponseWriter, r *http.Request) {
	scope, err := h.principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		TriggerID string `json:"trigger_id"`
	}
	if err := transport.ReadJSON(w, r, &in, triggersMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	trig, err := h.svc.Get(r.Context(), scope, in.TriggerID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, trig)
}

func (h *HTTPHandler) revoke(w http.ResponseWriter, r *http.Request) {
	scope, err := h.principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		TriggerID string `json:"trigger_id"`
	}
	if err := transport.ReadJSON(w, r, &in, triggersMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	trig, err := h.svc.Revoke(r.Context(), scope, in.TriggerID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, trig)
}

// fireNow processes due triggers only in the authenticated task scope. The
// background scheduler separately uses the global ProcessDue entry point.
func (h *HTTPHandler) fireNow(w http.ResponseWriter, r *http.Request) {
	scope, err := h.principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		Limit int `json:"limit"`
	}
	if err := transport.ReadJSON(w, r, &in, triggersMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	fired, err := h.svc.ProcessDueScoped(r.Context(), scope, time.Now().UTC(), in.Limit)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"fired": fired})
}

// A public task may schedule only the authority it actually presents. The
// reference is a durable grant task key, not a client-selected turn identifier.
func (h *HTTPHandler) bindCreateAuthority(r *http.Request, parent, ref *string) error {
	p, ok := transport.FromContext(r.Context())
	if !ok || p.Claims.Task == "" || p.Claims.Kind != auth.KindTask || (*parent != "" && *parent != p.Claims.Task) || (*ref != "" && *ref != p.Claims.Task) {
		return contracts.NewError(contracts.ErrForbidden, "triggers: parent and delegation reference must match the authenticated task")
	}
	if err := h.authorize(r.Context(), p.Claims, "execution"); err != nil {
		return err
	}
	*parent, *ref = p.Claims.Task, p.Claims.Task
	return nil
}
