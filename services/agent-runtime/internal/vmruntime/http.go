package vmruntime

import (
	"context"
	"net/http"
	"slices"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type Runtime interface {
	EnsureRunning(context.Context, Execution) error
	Fence(context.Context, Execution) error
}

// Handler exposes only the coordinator-facing execution lifecycle. The
// coordinator's workload certificate and live dispatch grant are both required.
func Handler(runtime Runtime, verifier *auth.Verifier, allowlist []string, authorize transport.Authorize) http.Handler {
	mux := http.NewServeMux()
	if verifier == nil {
		return mux
	}
	protected := transport.Task(verifier, "ahp", "runtime.dispatch", func(ctx context.Context, claims auth.Claims, cap string) error {
		if !slices.Contains(allowlist, auth.WorkloadID(claims.Scope())) || authorize == nil {
			return contracts.NewError(contracts.ErrForbidden, "workload is not a runtime coordinator")
		}
		return authorize(ctx, claims, cap)
	})
	register := func(path string, ensure bool) {
		mux.Handle("POST "+path, protected(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Execution
				// Older coordinators use turn_seq for both operations.
				LegacyTurnSeq int64 `json:"turn_seq,omitempty"`
			}
			if err := transport.ReadJSON(w, r, &body, 8192); err != nil {
				transport.WriteError(w, r, err)
				return
			}
			var err error
			if ensure {
				if body.LaunchTurnSeq == 0 {
					body.LaunchTurnSeq = body.LegacyTurnSeq
				}
				err = runtime.EnsureRunning(r.Context(), body.Execution)
			} else {
				err = runtime.Fence(r.Context(), body.Execution)
			}
			if err != nil {
				transport.WriteError(w, r, err)
				return
			}
			w.WriteHeader(http.StatusOK)
		})))
	}
	register("/internal/v1/executions:ensure", true)
	register("/internal/v1/executions:fence", false)
	return mux
}
