package modelapi

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// TreeAuthority attests a persisted run's bound and the caller's permission to
// report its step/child outcomes. It must never derive a bound from an HTTP body.
type TreeAuthority func(context.Context, auth.Claims, string) (int64, error)

// StateRunReader is the State owner's scoped read contract. Composition may
// provide the in-process owner; remote compositions can provide an API adapter.
type StateRunReader interface {
	GetRun(context.Context, contracts.Scope, string) (state.Run, error)
	GetThread(context.Context, contracts.Scope, string) (state.Thread, error)
}

// NewStateTreeAuthority permits only explicitly pinned AS workload reporters.
// Generic/custom agents remain untrusted even with a valid model task token.
// An empty allowlist denies every caller. The bound is the persisted State run
// payload; task, subject, deployment and context must all match current claims.
func NewStateTreeAuthority(states StateRunReader, workloads []string, authorize transport.Authorize) (TreeAuthority, error) {
	if states == nil || authorize == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "State run reader required")
	}
	allowed := map[string]bool{}
	for _, raw := range workloads {
		id := strings.TrimSpace(raw)
		u, err := url.Parse(id)
		if err != nil || u.Scheme != "spiffe" || u.Host != "ahp" || u.User != nil || strings.ContainsAny(id, "*%?#") {
			return nil, contracts.NewError(contracts.ErrInvalid, "explicit AS workload SPIFFE identity required")
		}
		parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
		if len(parts) != 4 || parts[0] != "tenant" || parts[1] == "" || parts[2] != "deployment" || parts[3] == "" {
			return nil, contracts.NewError(contracts.ErrInvalid, "invalid AS workload SPIFFE identity")
		}
		allowed[id] = true
	}
	return func(ctx context.Context, claims auth.Claims, runID string) (int64, error) {
		if !allowed[auth.WorkloadID(claims.Scope())] {
			return 0, contracts.NewError(contracts.ErrForbidden, "workload is not an AS tree reporter")
		}
		if err := authorize(ctx, claims, "state"); err != nil {
			return 0, err
		}
		run, err := states.GetRun(ctx, claims.Scope(), runID)
		if err != nil {
			return 0, err
		}
		if run.TaskID != claims.Task {
			return 0, contracts.NewError(contracts.ErrForbidden, "run task differs from verified authority")
		}
		thread, err := states.GetThread(ctx, claims.Scope(), run.ThreadID)
		if err != nil {
			return 0, err
		}
		if thread.ContextID != claims.Context {
			return 0, contracts.NewError(contracts.ErrForbidden, "run context differs from verified authority")
		}
		var payload struct {
			MaxSteps int64 `json:"max_steps"`
		}
		if json.Unmarshal(run.Payload, &payload) != nil || payload.MaxSteps <= 0 || payload.MaxSteps > 1e12 {
			return 0, contracts.NewError(contracts.ErrForbidden, "run has no persisted caller-stated step bound")
		}
		return payload.MaxSteps, nil
	}, nil
}
