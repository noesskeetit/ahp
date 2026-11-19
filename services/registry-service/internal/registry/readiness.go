package registry

import (
	"context"
	"net/http"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ReadinessProber answers the one question the pipeline asks before traffic is
// switched to a version: is the runtime for this immutable image actually up and
// serving ("проб готовности ДО переключения трафика", rev 3.2 c. 41)? It is the
// same probe the per-invocation dispatcher runs, factored into a single function
// so "ready" never means two different things.
type ReadinessProber interface {
	ProbeRuntimeReady(ctx context.Context, b RuntimeBinding) error
}

// httpReadinessProber probes a runtime's bound ReadyURL over the operator's mTLS
// client. It is the production prober; tests may inject a scripted one.
type httpReadinessProber struct{ client *http.Client }

// NewHTTPReadinessProber builds the production prober. Redirects are disabled so
// a readiness check can never be pointed at another origin or forward workload
// credentials elsewhere.
func NewHTTPReadinessProber(client *http.Client) ReadinessProber {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	c := *client
	if c.Timeout == 0 || c.Timeout > 10*time.Second {
		c.Timeout = 5 * time.Second
	}
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &httpReadinessProber{client: &c}
}

func (p *httpReadinessProber) ProbeRuntimeReady(ctx context.Context, b RuntimeBinding) error {
	return probeRuntimeReady(ctx, p.client, b)
}

// WithPromoteReadinessGate makes Promote probe the target version's runtime
// BEFORE the installation's version pointer -- and therefore live traffic --
// moves, and refuses to switch when the runtime is unreachable, not ready, or
// misidentified. The gate is only ever active when a prober is wired AND the
// installation's environment is listed here; when a prober is wired but no
// environments are passed, stage and prod are gated by default -- the levels the
// PDF subjects to a restart-chaos/availability stage. A gate configured with no
// prober fails closed rather than switching traffic unchecked.
//
// Auto-rollback (AutoRollbackIfUnready) uses the same gate: an installation whose
// live runtime stops answering is switched back to the previous version the
// registry recorded, while tasks already pinned to the failed version keep
// running there (drain, rev 3.2 c. 41/42).
func WithPromoteReadinessGate(prober ReadinessProber, environments ...string) Option {
	return func(s *Store) {
		s.prober = prober
		envs := environments
		if len(envs) == 0 {
			envs = []string{EnvironmentStage, EnvironmentProd}
		}
		s.readinessGateEnvs = make(map[string]bool, len(envs))
		for _, e := range envs {
			s.readinessGateEnvs[e] = true
		}
	}
}

// readinessGateActive reports whether traffic switching for this environment must
// be probed first.
func (s *Store) readinessGateActive(environment string) bool {
	return s.prober != nil && s.readinessGateEnvs[environment]
}

// gatePromoteReadiness probes the promotion target before any write. It is called
// outside the promotion transaction on purpose: a readiness check is a network
// round-trip and must not be made while holding the installation lock.
func (s *Store) gatePromoteReadiness(ctx context.Context, inst Installation, targetVersionID string) error {
	if !s.readinessGateActive(inst.Environment) {
		return nil
	}
	// A runtime binding is keyed by the immutable manifest digest, not the
	// version row's id, so resolve the target's digest first.
	target, err := s.GetVersion(ctx, targetVersionID)
	if err != nil {
		return err
	}
	b, err := s.ResolveRuntime(ctx, inst.Tenant, inst.Environment, inst.AgentID, target.Digest)
	if err != nil {
		return contracts.NewError(contracts.ErrUnavailable,
			"promote readiness gate: the promotion target has no registered runtime to probe; traffic not switched")
	}
	if err := s.prober.ProbeRuntimeReady(ctx, b); err != nil {
		return contracts.NewError(contracts.ErrUnavailable,
			"promote readiness gate: the promotion target's runtime is not ready; traffic not switched")
	}
	return nil
}

// AutoRollbackIfUnready is the automatic half of the pipeline's probe-then-switch
// rule: when an installation's currently-live runtime stops passing the readiness
// probe, traffic is switched back to the previous version the registry recorded,
// without waiting for an operator. Tasks already pinned to the failed version are
// not moved -- they keep running there and drain by the normal window.
//
// It is a no-op (rolled back = false) when the environment is not gated or no
// prober is wired, so it is safe to run unconditionally from a supervisory loop.
// When the live runtime is ready it returns the installation unchanged. When it
// is not, it rolls back; a rollback with no previous version (a first deploy)
// surfaces the error rather than silently stranding traffic.
func (s *Store) AutoRollbackIfUnready(ctx context.Context, tenant, installationID string) (Installation, bool, error) {
	inst, err := s.GetInstallation(ctx, tenant, installationID)
	if err != nil {
		return Installation{}, false, err
	}
	if !s.readinessGateActive(inst.Environment) {
		return inst, false, nil
	}
	ready := true
	live, verr := s.GetVersion(ctx, inst.VersionID)
	var b RuntimeBinding
	var rerr error
	if verr != nil {
		rerr = verr
	} else {
		b, rerr = s.ResolveRuntime(ctx, tenant, inst.Environment, inst.AgentID, live.Digest)
	}
	if rerr != nil {
		ready = false
	} else if perr := s.prober.ProbeRuntimeReady(ctx, b); perr != nil {
		ready = false
	}
	if ready {
		return inst, false, nil
	}
	rolled, err := s.Rollback(ctx, tenant, installationID)
	if err != nil {
		return inst, false, err
	}
	return rolled, true, nil
}
