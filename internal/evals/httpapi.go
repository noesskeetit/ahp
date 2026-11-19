package evals

import (
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// evalsAudience and evalsCapability pin the read face to the SAME authorize
// contract the accepted Registry face uses. Per the director's decision the
// planner does NOT introduce an "evals" capability: verdicts are read under the
// "registry" capability, so one scoped registry grant covers publishing and
// reading the verdict that gates it.
const (
	evalsAudience   = "ahp"
	evalsCapability = "registry"
	evalsMaxBody    = 1 << 20
)

// HTTPHandler exposes the latest verdict and an operator-allowlisted RunEval.
// The runner computes and records results; callers cannot assert a pass.
// The caller's identity is
// taken ONLY from the verified principal transport.Task installs (mTLS client
// cert -> authorize under "registry"), never from a header or the body.
type HTTPHandler struct {
	store  *Store
	runner *Runner
	task   func(http.Handler) http.Handler
	routes map[string]func(http.ResponseWriter, *http.Request)
}

// NewHTTPHandler builds the evals HTTP face. authorize may not be nil;
// a nil authorize makes transport.Task fail every request closed.
func NewHTTPHandler(store *Store, verifier *auth.Verifier, authorize transport.Authorize) (http.Handler, error) {
	switch {
	case store == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "evals: store must not be nil")
	case verifier == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "evals: verifier must not be nil")
	case authorize == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "evals: authorize must not be nil")
	}
	h := &HTTPHandler{
		store: store,
		task:  transport.Task(verifier, evalsAudience, evalsCapability, authorize),
	}
	h.routes = map[string]func(http.ResponseWriter, *http.Request){
		"Verdict": h.verdict,
		"RunEval": h.runEval,
	}
	return h, nil
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.Path, "/v1/evals/")
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "evals: unknown route"))
		return
	}
	fn, ok := h.routes[rest]
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "evals: unknown route"))
		return
	}
	h.task(http.HandlerFunc(fn)).ServeHTTP(w, r)
}

// verdict answers the latest verdict for one version digest plus a content-free
// pass summary. It is read-only; it cannot create, edit or delete a verdict.
func (h *HTTPHandler) verdict(w http.ResponseWriter, r *http.Request) {
	if _, ok := transport.FromContext(r.Context()); !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "evals: no verified principal on this request"))
		return
	}
	var in struct {
		AgentDigest string `json:"agent_digest"`
	}
	if err := transport.ReadJSON(w, r, &in, evalsMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	v, err := h.store.LatestVerdict(r.Context(), in.AgentDigest)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	passing, err := h.store.HasPassingVerdict(r.Context(), in.AgentDigest)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	fresh, err := h.store.Freshness(r.Context(), in.AgentDigest)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	summary := map[string]int{"total": len(v.Tasks), "passed": 0}
	for _, t := range v.Tasks {
		if t.Passed {
			summary["passed"]++
		}
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{
		"verdict": map[string]any{
			"id":            v.ID,
			"agent_digest":  v.AgentDigest,
			"task_set_id":   v.TaskSetID,
			"setup_digest":  v.SetupDigest,
			"outcome":       string(v.Outcome),
			"passing":       passing,
			"total_cost":    v.TotalCost,
			"created_at":    v.CreatedAt,
			"task_summary":  summary,
			"policy_id":     v.PolicyID,
			"policy_digest": v.PolicyDigest,
			"target_kind":   string(v.TargetKind),
		},
		"freshness": map[string]any{"fresh": fresh.Fresh, "reason": fresh.Reason},
	})
}

// AttachRunner enables the production writer only after the operator allowlist
// was loaded. Without it RunEval fails closed, rather than accepting assertions.
func AttachRunner(handler http.Handler, runner *Runner) error {
	h, ok := handler.(*HTTPHandler)
	if !ok || runner == nil {
		return contracts.NewError(contracts.ErrInvalid, "evals: invalid runner attachment")
	}
	h.runner = runner
	return nil
}
func (h *HTTPHandler) runEval(w http.ResponseWriter, r *http.Request) {
	principal, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "evals: verified principal required"))
		return
	}
	if h.runner == nil {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrUnavailable, "evals: operator runner not configured"))
		return
	}
	var in struct {
		SuiteID     string `json:"suite_id"`
		AgentDigest string `json:"agent_digest"`
	}
	if err := transport.ReadJSON(w, r, &in, evalsMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	v, err := h.runner.RunEval(r.Context(), RunRequest{Scope: principal.Scope, Tenant: principal.Scope.Tenant, SuiteID: in.SuiteID, AgentDigest: in.AgentDigest, Token: token})
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"verdict": v})
}
