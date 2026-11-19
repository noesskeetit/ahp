// Package modelapi exposes Model-owned tree accounting to agent runtimes.
package modelapi

import (
	"errors"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/treedelegate"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// TreeBudgetOperations are registered on the Model owner, never on AS.
var TreeBudgetOperations = []string{"ChargeStep", "ReserveDelegation", "SettleDelegation"}

// DelegationGrant is an opaque logical reference; clients cannot select ledger IDs.
type DelegationGrant struct {
	Ref     string `json:"ref"`
	Granted int64  `json:"granted"`
}

// DelegationOutcome records delivery certainty and child-reported consumption.
type DelegationOutcome struct {
	ChildTaskID   string `json:"child_task_id"`
	ReachedChild  bool   `json:"reached_child"`
	StepsConsumed int64  `json:"steps_consumed"`
	Reported      bool   `json:"reported"`
}
type treeRequest struct {
	RunID     string            `json:"run_id"`
	Bound     int64             `json:"bound"`
	Step      int               `json:"step,omitempty"`
	Ref       string            `json:"ref,omitempty"`
	Requested int64             `json:"requested,omitempty"`
	Outcome   DelegationOutcome `json:"outcome,omitempty"`
}

// NewTreeBudgetHandler keeps all accounting inside Model's atomic ledger. Scope
// comes only from current verified task authority. The State owner attests the
// run selector and its bound; no caller-supplied identity or
// caller-supplied budget IDs are accepted.
func NewTreeBudgetHandler(store *model.BudgetStore, v *auth.Verifier, authorize transport.Authorize, authority TreeAuthority) (http.Handler, error) {
	if store == nil || v == nil || authorize == nil || authority == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "tree budget dependencies required")
	}
	ledger, err := treedelegate.New(store)
	if err != nil {
		return nil, err
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := transport.FromContext(r.Context())
		if !ok || p.Claims.Task == "" || p.Claims.Context == "" {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "bound task context required"))
			return
		}
		var in treeRequest
		if err := transport.ReadJSON(w, r, &in, 1<<20); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		if in.Bound <= 0 || in.Bound > 1e12 || in.Step < 0 || int64(in.Step) >= in.Bound || in.Outcome.StepsConsumed < 0 || len(in.Ref) > 512 || len(in.Outcome.ChildTaskID) > 512 {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "invalid tree budget request"))
			return
		}
		bound, err := authority(r.Context(), p.Claims, in.RunID)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		if in.Bound != bound {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "requested bound differs from persisted run"))
			return
		}
		run, err := ledger.ForRun(p.Scope, in.RunID, bound)
		var out any = map[string]bool{"ok": true}
		if err == nil {
			switch strings.TrimPrefix(r.URL.Path, "/v1/model/") {
			case "ChargeStep":
				err = run.ChargeStep(r.Context(), in.Step)
			case "ReserveDelegation":
				var res treedelegate.Reservation
				res, err = run.ReserveDelegation(r.Context(), contracts.Scope{}, in.Ref, in.Requested)
				out = DelegationGrant{Ref: res.Ref, Granted: res.Granted}
			case "SettleDelegation":
				var res treedelegate.Reservation
				res, err = run.LookupDelegation(r.Context(), in.Ref)
				if err == nil {
					err = run.SettleDelegation(r.Context(), res, treedelegate.Outcome{ChildTaskID: in.Outcome.ChildTaskID, ReachedChild: in.Outcome.ReachedChild, Reported: in.Outcome.Reported, StepsConsumed: in.Outcome.StepsConsumed})
				}
			}
		}
		if err != nil {
			if errors.Is(err, treedelegate.ErrNoCapacity) {
				err = contracts.NewError(contracts.ErrConflict, "no delegation capacity")
			}
			transport.WriteError(w, r, err)
			return
		}
		transport.WriteJSON(w, http.StatusOK, out)
	})
	task := transport.Task(v, "ahp", "model", authorize)(handler)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		valid := false
		for _, op := range TreeBudgetOperations {
			if r.URL.Path == "/v1/model/"+op {
				valid = true
				break
			}
		}
		if r.Method != http.MethodPost || !valid {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "unknown tree budget operation"))
			return
		}
		task.ServeHTTP(w, r)
	}), nil
}
