//go:build ahp_exec

package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/compute/computeapi"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// computeMountPrefix mirrors computeapi's unexported apiPrefix. It is a literal
// here for the same reason the single-origin prefixes are literals: the route
// table must be readable without importing the tier, and
// role_compute_test.go / single_origin_test.go pin this string against
// internal/compute/computeapi/api.go so the two can never drift apart silently.
const (
	computeMountPrefix = "/v1/compute/"
	// envEnableCompute is the operator's opt-in. Unset means the compute plane is
	// NOT mounted at all: every existing composition that does not set it answers
	// exactly as it did before this file, and no Postgres migration for the
	// compute schema is attempted by a process that never asked for it.
	envEnableCompute = "AHP_ENABLE_COMPUTE"
)

// computeReconcileInterval is how often this process sweeps dispatched commands that
// stopped answering. The default is well inside the dispatch budget so a dead holder is
// noticed in one budget window rather than never; an operator may set
// AHP_COMPUTE_RECONCILE_INTERVAL to "0" to take the sweep off this process, which the
// loop itself logs as a deliberate, loud choice. A value that does not parse is refused
// rather than guessed at: a typo in a liveness knob must not silently mean "off".
func computeReconcileInterval() time.Duration {
	raw := envOr("AHP_COMPUTE_RECONCILE_INTERVAL", "15s")
	d, err := time.ParseDuration(raw)
	if err != nil {
		panic("ahp: AHP_COMPUTE_RECONCILE_INTERVAL is not a duration: " + raw)
	}
	return d
}

// computeRequested reports whether the operator asked for the compute plane in
// this process. Only the literal "1" turns it on - a typo must not half-mount a
// plane that executes commands.
func computeRequested() bool { return envOr(envEnableCompute, "0") == "1" }

// buildComputeHandler wires the Compute plane for in-process mounting.
//
// Why this function exists at all: until now the only thing that ever mounted
// computeapi was the standalone cmd/compute-host binary, while the stand config
// already carried AHP_LISTEN_COMPUTE - a variable no Go code reads (grep over
// the tree: zero hits). The measured consequence is the chain gap the deputy
// named: a user turn could never reach an isolated command, because the live
// aggregate answered /v1/compute/ from the SPA fallback instead of Compute.
//
// The plane is co-located on an existing mux rather than given its own port:
// the port the stand config names (18088) is already held by an unrelated
// stand process, and a mount that cannot bind must not be the reason a flip
// half-succeeds.
//
// Object storage for artifact export is NOT fabricated: when the exporter is
// unconfigured the plane is mounted read-only (runner == nil), which
// computeapi implements as Run/Export -> FORBIDDEN, and the reason is logged.
func (s *server) buildComputeHandler(ctx context.Context) (http.Handler, error) {
	if s.pool == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "ahp: the compute plane needs a database pool")
	}
	store := compute.New(s.pool)
	if err := store.Migrate(ctx); err != nil {
		return nil, err
	}
	// Lifecycle writers may wait for an export row guard while occupying the
	// ordinary pool. Current Identity checks have an independent bounded read
	// pool, so source publication never waits for the writer it is fencing.
	readConfig := s.pool.Config()
	readConfig.MaxConns = 2
	readConfig.MinConns = 0
	authorityPool, err := pgxpool.NewWithConfig(ctx, readConfig)
	if err != nil {
		return nil, err
	}
	keepAuthority := false
	defer func() {
		if !keepAuthority {
			authorityPool.Close()
		}
	}()
	if err := authorityPool.Ping(ctx); err != nil {
		return nil, err
	}
	jailBase := envOr("AHP_COMPUTE_JAIL_BASE", "")
	backends := []compute.Backend{compute.NewHostBackend(jailBase)}
	if sock := envOr("AHP_COMPUTE_FC_SOCKET", ""); sock != "" {
		// Class X is served only when a supervisor socket is supplied. Without it
		// the registry reports class X unavailable and fails closed - never a
		// silent host fallback.
		backends = append(backends, compute.NewMicroVMBackend("firecracker", sock))
	}
	if envOr("AHP_COMPUTE_MOCK_BACKEND", "0") == "1" {
		// Operator opt-in, and only ever labelled "mock". This is a simulation that
		// executes nothing: it exists so the sandbox lifecycle can be exercised on a
		// host where /dev/kvm is unavailable (measured EACCES), and it is never a
		// fallback for a missing real backend. Every answer it gives carries the
		// marker, and it adds no claim about isolation, latency or density.
		backends = append(backends, compute.NewMockBackend())
		s.logger.Info("compute role: MOCK backend mounted (executes nothing)",
			"mock_marker", compute.MockMarker)
	}
	registry := compute.NewRegistry(backends...)
	if envOr("AHP_COMPUTE_MOCK_BACKEND", "0") == "1" {
		registry.SetClassBackend(compute.ClassX, compute.MockBackendName)
	}

	var runner *compute.Runner
	var exporter compute.ArtifactExportClient
	if s.artifactExportOwner != nil {
		exporter, err = compute.NewOwnerArtifactExporter(s.artifactExportOwner)
	} else {
		exporter, err = compute.NewArtifactExporterFromEnv()
	}
	switch {
	case err != nil:
		return nil, err
	case exporter == nil:
		// NewArtifactExporterFromEnv answers (nil, nil) with no AHP_ARTIFACTS_URL.
		// Handing that nil to the runner would build a "writable" plane whose export
		// fails only when a command has already run - a fabricated readiness. The
		// plane is mounted read-only instead, and the reason is said out loud.
		s.logger.Warn("compute plane mounted READ-ONLY: no artifact exporter configured, Run/Export answer FORBIDDEN",
			"env", "AHP_ARTIFACTS_URL")
	default:
		runner = compute.NewRunner(store, registry, jailBase, envOr("AHP_COMPUTE_NODE", ""), exporter)
		// The Redis lease layer (writer lease B50, activity/sleep leases B52) was
		// wired only in cmd/compute-host. A plane mounted through this file therefore
		// answered every lease-gated operation -- Sleep, Wake, TypedPatch, environment
		// install -- with `503 UNAVAILABLE: lease service is not configured`, while the
		// mount log still said "compute plane mounted": a plane that looks up and cannot
		// sleep. That is the same overclaim DEF-COMPUTE-1 is about, one layer down, so it
		// is wired here the same way the host does it.
		//
		// The two failure modes are deliberately different. No AHP_REDIS_ADDR at all is a
		// deployment choice: the plane mounts and says OUT LOUD that lease-gated calls
		// will refuse. A configured address that does not answer is a broken dependency:
		// reporting it is better than serving a plane whose sleep path silently lies, so
		// it fails the mount and the caller sees the explicit 503 face.
		if addr := envOr("AHP_REDIS_ADDR", ""); addr != "" {
			rdb := compute.NewRedis(addr)
			if err := rdb.Ping(ctx); err != nil {
				return nil, fmt.Errorf("compute lease layer: redis at %s is not reachable: %w", addr, err)
			}
			leaseService, err := compute.NewLeaseService(store, rdb)
			if err != nil {
				return nil, fmt.Errorf("compute lease layer: %w", err)
			}
			runner = runner.WithLeaseService(leaseService)
			s.logger.Info("compute plane: Redis lease layer wired", "addr", addr)
		} else {
			s.logger.Warn("compute plane mounted WITHOUT the Redis lease layer: Sleep/Wake/TypedPatch answer UNAVAILABLE",
				"env", "AHP_REDIS_ADDR", "consequence", "lease-gated operations refuse by design; nothing is faked")
		}
	}

	// DEF-COMPUTE-6: a dispatch has to end. The heartbeat renews the budget while a
	// command runs, and this sweep is what turns a dead holder's silence into a terminal
	// verdict instead of a permanently open ledger row. It is started wherever this plane
	// can execute, and its absence is logged by the loop itself rather than left implicit.
	if runner != nil {
		interval := computeReconcileInterval()
		stopReconcile := runner.StartCommandReconcileLoop(context.WithoutCancel(ctx), interval)
		s.logger.Info("compute plane: dispatch reconciliation loop started",
			"interval", interval.String(), "budget", compute.CommandDispatchBudget.String(),
			"heartbeat", compute.CommandHeartbeatInterval.String())
		// The loop outlives this function by design (a plane that stopped sweeping would
		// reproduce the defect it fixes), so the only stop handle is process teardown.
		_ = stopReconcile
	}

	verifier, _ := devIdentityAuth()
	// Same coarse gate as the artifact plane: a task token (never a human or
	// service token), the "compute" capability the tier asks about, and a
	// complete scope. Without this any bearer token would reach a command
	// execution face.
	ids := s.identityStoreFor(authorityPool)
	if err := ids.Migrate(ctx); err != nil {
		return nil, err
	}
	allow := ids.Authorize
	// The PTY / browser-worker session facade (PDF s.29-30). Mounted ONLY next to the
	// mock backend, because the facade's implementation is a simulation that executes
	// nothing and the real substrate is not implemented: mounting it beside a real
	// backend would advertise sessions whose transcript is not the terminal the caller
	// thinks it has. Without it the routes stay registered and answer 503 with the
	// reason, which is a refusal an operator can act on.
	var sessions *compute.SessionService
	opts := computeapi.Options{}
	if envOr("AHP_COMPUTE_MOCK_BACKEND", "0") == "1" {
		sessions = compute.NewMockSessionService(store)
		opts.Sessions = sessions
		s.logger.Info("compute role: MOCK session facade mounted (PTY/browser; executes nothing)",
			"mock_marker", compute.MockMarker)
	}
	handler, err := computeapi.NewWithOptions(store, verifier, allow, runner, opts)
	if err != nil {
		return nil, err
	}
	if runner != nil {
		s.registerTenantDeletionOwner("compute", runner)
		s.registerSubjectDeletionOwner("compute", runner)
	} else {
		s.registerTenantDeletionOwner("compute", store)
		s.registerSubjectDeletionOwner("compute", store)
	}
	backendName, marker := registry.Disclosure()
	if marker != "" {
		handler = disclosingPlane{backend: backendName, marker: marker, next: handler}
		s.logger.Info("compute plane: every answer will disclose the backend",
			"backend", backendName, headerDisclosure, marker)
	}
	s.logger.Info("compute plane mounted", "prefix", computeMountPrefix,
		"jail_base", jailBase, "microvm", envOr("AHP_COMPUTE_FC_SOCKET", "") != "",
		"writable", runner != nil, "backend_disclosed", marker != "")
	keepAuthority = true
	context.AfterFunc(ctx, authorityPool.Close)
	return handler, nil
}

// headerBackend is set on every answer of the Compute plane so a caller can tell
// WHICH substrate answered without opening the body, and headerDisclosure carries
// the backend's own statement of its nature. They exist because a payload field is
// strippable: a proxy, a cache or a client that projects one field can lose it, and
// then a mock reads as a hypervisor. A header on the face is the smallest place
// where "every answer carries it" is true by construction -- including the answers
// that never reach a backend (401, 403, unknown sandbox, refusal).
const (
	headerBackend    = "AHP-Compute-Backend"
	headerDisclosure = "AHP-Compute-Disclosure"
)

// disclosingPlane stamps the registry's disclosure on every response. A registry
// with no disclosing backend adds nothing (it has nothing to disclose), so ordinary
// compositions get the byte-identical answers they had before.
type disclosingPlane struct {
	backend, marker string
	next            http.Handler
}

func (d disclosingPlane) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if d.marker != "" {
		w.Header().Set(headerBackend, d.backend)
		w.Header().Set(headerDisclosure, d.marker)
	}
	d.next.ServeHTTP(w, r)
}

// mountComputePlane routes the Compute API onto mux.
func mountComputePlane(mux *http.ServeMux, handler http.Handler) {
	mux.Handle(computeMountPrefix, handler)
}

// computeUnavailableFace answers the Compute prefix while the plane is absent for
// a reason: the operator asked for it and the mount failed.
//
// Why a face at all instead of simply forwarding: after a failed mount the prefix
// would otherwise go to the runner, whose answer (404 on a front, or a plane the
// caller cannot tell apart from this one) hides the fact that THIS process was
// asked and could not deliver. An explicit 503 with a code is what makes the
// matrix honest -- the caller draws "unavailable", not success, and not somebody
// else's not-found.
//
// The reason text in the body is a fixed sentence naming the switch, NOT the
// underlying error: mount failures come from Postgres and object storage and their
// error strings can carry connection details. The full error goes to the log, where
// the operator reads it. Same rule the operator-visible-cause fix uses.
type computeUnavailableFace struct{}

func (computeUnavailableFace) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(headerBackend, "unavailable")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(`{"code":"unavailable","message":"the compute plane was requested (AHP_ENABLE_COMPUTE=1) but could not be mounted in this process; the server log carries the reason"}` + "\n"))
}

// tryMountCompute co-locates the compute plane on a shared mux. It is a no-op
// (not an error) when the operator did not ask for the plane, so the "all" role
// stays exactly what it was for every composition that predates this file.
//
// A FAILED mount degrades, it does not abort (DEF-COMPUTE-1's second half): the
// caller has already mounted the execution plane on this same mux, and one optional
// plane being unable to start must not be reported as, or become, the loss of the
// planes that could. The answer is a Warn plus the explicit 503 face, and
// computeRouteOwned tells the single-origin edge who owns the prefix either way.
// serveCompute -- the split topology, where this process exists ONLY to serve
// Compute -- stays strict: a compute host with no compute plane has nothing to
// serve, and starting it anyway would be a fabricated readiness.
func (s *server) tryMountCompute(ctx context.Context, mux *http.ServeMux) error {
	if !computeRequested() {
		return nil
	}
	handler, err := s.buildComputeHandler(ctx)
	if err != nil {
		s.logger.Warn("compute plane NOT mounted; the other planes of this process are unaffected",
			"prefix", computeMountPrefix, "err", err,
			"consequence", "POST "+computeMountPrefix+"* answers 503 unavailable with a reason")
		s.computePlaneMounted = false
		s.computeRouteOwned = true
		mux.Handle(computeMountPrefix, computeUnavailableFace{})
		return nil
	}
	s.computePlaneMounted = true
	s.computeRouteOwned = true
	mountComputePlane(mux, handler)
	return nil
}

// serveCompute runs the compute plane on its own address (split topology).
func (s *server) serveCompute(ctx context.Context, addr string) error {
	if !computeRequested() {
		return nil
	}
	handler, err := s.buildComputeHandler(ctx)
	if err != nil {
		return err
	}
	mux := s.baseMux()
	mountComputePlane(mux, handler)
	return s.runServer(ctx, "compute", addr, mux)
}
