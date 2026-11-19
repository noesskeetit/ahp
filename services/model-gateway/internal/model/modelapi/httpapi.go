// Package modelapi exposes the Model Gateway's read-only answers over HTTP.
//
// It exists for one question the acceptance could not ask without opening
// Postgres by hand: "what did this task and its delegated children cost"
// (PDF rev3.2 :347 "стоимость задачи", and :1789 for what a tree budget means).
// The numbers themselves come from the accepted read model
// (model.BudgetStore.TreeCostByTask); this package adds no accounting, no
// interpretation and no writer.
//
// The shape follows the accepted read faces (internal/evals, internal/registry):
// the caller's identity is taken ONLY from the verified principal the transport
// middleware installs from an mTLS-bound token, never from a header or the body,
// and the tenant boundary is enforced by the ledger read itself, so a foreign
// task id answers as absent rather than as forbidden.
package modelapi

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// RouteRoot is the prefix this face is mounted at, exported so the front
// registers exactly what the face serves instead of repeating the literal.
const RouteRoot = "/v1/model/"

const (
	audience  = "ahp"
	maxBody   = 1 << 20
	operation = "TreeCost"
	maxTaskID = 256
)

// CapabilityCost is the grant capability this face authorizes under.
//
// No new capability is introduced: reading what a run consumed is the same kind
// of fact the accepted State face already answers (internal/stateapi authorizes
// under "state"), and the acceptance complaint this answers came from
// /v1/space/GetTurn -- a grant that reads a run's state can read its cost. The
// director can move this to a dedicated "model" capability, which is a grant
// migration, not a code change.
const CapabilityCost = "state"

// CostReader is the whole dependency of this face, and the type system is what
// keeps it read-only: the ledger's holds and settlements are not reachable from
// here, so a handler cannot fabricate the number it reports.
type CostReader interface {
	TreeCostByTask(ctx context.Context, scope contracts.Scope, taskID string) ([]model.CostTree, error)
}

// HTTPHandler serves /v1/model/ read operations.
type HTTPHandler struct {
	costs  CostReader
	task   func(http.Handler) http.Handler
	routes map[string]http.HandlerFunc
	now    func() time.Time
}

var _ http.Handler = (*HTTPHandler)(nil)

// NewHTTPHandler builds the read face. authorize may not be nil: a nil authorize
// makes transport.Task deny every request, which is the correct failure mode but
// a confusing one to discover at runtime, so it is refused at construction.
func NewHTTPHandler(costs CostReader, verifier *auth.Verifier, authorize transport.Authorize) (http.Handler, error) {
	switch {
	case costs == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "modelapi: cost reader must not be nil")
	case verifier == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "modelapi: verifier must not be nil")
	case authorize == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "modelapi: authorize must not be nil")
	}
	h := &HTTPHandler{
		costs: costs,
		task:  transport.Task(verifier, audience, CapabilityCost, authorize),
		now:   func() time.Time { return time.Now().UTC() },
	}
	h.routes = map[string]http.HandlerFunc{operation: h.treeCost}
	return h, nil
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.Path, RouteRoot)
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "modelapi: unknown route"))
		return
	}
	fn, ok := h.routes[rest]
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "modelapi: unknown operation"))
		return
	}
	h.task(fn).ServeHTTP(w, r)
}

// principalScope takes the caller's scope from the verified principal. An absent
// principal is a denial, not a zero scope: the tenant boundary would otherwise
// silently become the empty string.
func principalScope(r *http.Request) (contracts.Scope, error) {
	p, ok := transport.FromContext(r.Context())
	if !ok || p.Scope.Tenant == "" {
		return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
			"modelapi: no verified principal with a tenant on this request")
	}
	return p.Scope, nil
}

// treeCost answers, for one task id, every budget tree that task takes part in.
//
// Two things the caller must not get wrong are stated in the payload: the units
// present (a tree in tokens and a tree in steps are different quantities and are
// never added), and the fact that no money is in the ledger. A task belonging to
// another tenant answers 404, the same way an absent one does.
func (h *HTTPHandler) treeCost(w http.ResponseWriter, r *http.Request) {
	scope, err := principalScope(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		TaskID string `json:"task_id"`
	}
	if err := transport.ReadJSON(w, r, &in, maxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	taskID := strings.TrimSpace(in.TaskID)
	if taskID == "" {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "modelapi: TreeCost needs a task_id"))
		return
	}
	if len(taskID) > maxTaskID {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "modelapi: task_id is implausibly long"))
		return
	}
	trees, err := h.costs.TreeCostByTask(r.Context(), scope, taskID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	units := make([]string, 0, len(trees))
	seen := map[string]bool{}
	for i := range trees {
		if !seen[trees[i].Unit] {
			seen[trees[i].Unit] = true
			units = append(units, trees[i].Unit)
		}
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{
		"task_id": taskID,
		// units is a warning by construction: more than one entry means one task
		// carries more than one denomination, and a total across them is a lie.
		"units":        units,
		"trees":        trees,
		"generated_at": h.now().Format(time.RFC3339Nano),
	})
}
