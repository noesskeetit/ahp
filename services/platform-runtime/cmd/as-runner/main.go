// Command as-runner starts the Agents Space + Core runtime as a single
// deployment. It reads config from environment variables, wires the Space
// coordinator, Core runtime, and HTTP handler, then listens for requests.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/runtimeprofile"
	"github.com/ahp-platform/ahp/internal/space"
	aot "github.com/ahp-platform/ahp/pkg/otel"
	"github.com/ahp-platform/ahp/web"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)
	stopProfile, profileErr := runtimeprofile.StartFromEnv("as-runner")
	if profileErr != nil {
		logger.Warn("runtime diagnostic profile unavailable", "code", "profile_init_failed")
	}
	defer stopProfile()

	// Tracing is mounted here exactly as the platform front mounts it, because the
	// runner IS a role: without Setup there is no tracer provider and no
	// propagator, and without the middleware the user-facing plane -- the one a
	// person actually touches -- produces no span at all. With OTEL_EXPORTER_OTLP_ENDPOINT
	// unset the exporter stays local, so this changes nothing observable on the wire.
	otelShutdown, otelErr := aot.Setup(context.Background(), aot.Config{
		ServiceName: "as-runner",
		Endpoint:    envDefault("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		Insecure:    envDefault("OTEL_EXPORTER_OTLP_INSECURE", "") == "true",
	})
	if otelErr != nil {
		logger.Error("as-runner: otel setup failed", "err", otelErr)
		os.Exit(1)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer scancel()
		_ = otelShutdown(sctx)
	}()

	cfg := loadConfig()
	logger.Info("as-runner starting",
		"listen", cfg.ListenAddr,
		"state_url", cfg.StateURL,
		"worker", cfg.WorkerID,
	)

	// State hop uses Space's OWN workload mTLS when a cert bundle is configured, so
	// State sees the Space workload identity (never any browser credential).
	stateHTTPClient, err := buildStateClient(cfg)
	if err != nil {
		logger.Error("as-runner: state mTLS configuration invalid", "err", err)
		os.Exit(1)
	}
	stateClient := space.NewStateClient(cfg.StateURL, tracedStateClient(stateHTTPClient))

	// DEF-H24-11: name the plane that refuses Space's workload certificate before the first
	// browser run discovers it as a 403. Read-only, bounded, never fatal: a plane that is
	// briefly down must not block a restart, and every real hop still fails closed alone.
	probeConfiguredPlanes(logger, cfg, stateHTTPClient)

	// Real model + tool executor wired from the environment. Both fail closed:
	// a misconfigured deployment refuses to start rather than silently running
	// a stub model or an echo tool executor that would fake a real turn.
	model, modelID, err := buildModel()
	if err != nil {
		logger.Error("as-runner: model configuration invalid", "err", err)
		os.Exit(1)
	}
	artifactClient, err := buildArtifactClient()
	if err != nil {
		logger.Error("as-runner: artifact API configuration invalid", "err", err)
		os.Exit(1)
	}
	tools, toolNames, err := buildToolsWithArtifactClient(artifactClient)
	if err != nil {
		logger.Error("as-runner: tool executor configuration invalid", "err", err)
		os.Exit(1)
	}
	logWiring(logger, modelID, toolNames)

	// Budget tree (AHP_DELEGATE_TREE_BUDGET). Off by default; when on, the loop
	// charges every consumed model step against the run's stated budget and the
	// emil adapter reserves a delegated child's bound out of the remainder, so the
	// sum of a parent's and its children's steps cannot exceed what the caller gave
	// the parent. Fail closed: a deployment that claims to account a budget tree it
	// cannot reach must not start.
	budgets, berr := buildTreeBudget(logger)
	if berr != nil {
		logger.Error("as-runner: delegate budget tree configuration invalid", "err", berr)
		os.Exit(1)
	}
	runTreeBudget = budgets
	var accountant core.StepAccountant
	if budgets != nil {
		accountant = budgets
	}
	// The attester a delegating hop uses to name its own run's grant task (DEF-H10-15).
	// Built from the same published keys and the same pinned workload as the ledger
	// and the artifact path, so one run cannot be named three different ways. If it
	// cannot be built the runner still serves -- a hop then declares the run id and
	// announces, on the emil WARN line, that the receiving plane can only audit a
	// parent it holds locally. That is the pre-fix behaviour, kept reachable but loud.
	if att, aerr := buildRunScopeAttester(); aerr != nil {
		logger.Error("as-runner: run authority attester unavailable; a delegated hop will declare its parent by run id and will be refused by any plane that cannot see that run locally", "err", aerr)
	} else {
		runAuthorityScope = att
		logger.Info("as-runner: delegated-hop parent attestation enabled",
			"attester", att.enabled(), "task_audience", att.taskAudience)
	}

	memoryRuntime, memoryErr := buildMemory(stateHTTPClient)
	if memoryErr != nil {
		logger.Error("as-runner: memory configuration invalid", "err", memoryErr)
		os.Exit(1)
	}
	var memoryPort core.MemoryDriver
	var checkMemoryProject func(context.Context, string, string, string, string) error
	var checkSharedMemoryProject func(context.Context, string, string, string, string) error
	if memoryRuntime != nil {
		if !holdsCapability(splitCSV(cfg.RunGrantCaps), "memory") {
			logger.Error("as-runner: AS_RUN_GRANT_CAPS must include memory when AS_MEMORY_URL is set")
			os.Exit(1)
		}
		memoryPort = memoryRuntime
		checkMemoryProject = memoryRuntime.BindProject
		if memoryRuntime.sharedOrigin != "" {
			if !holdsCapability(splitCSV(cfg.RunGrantCaps), "workspace") {
				logger.Error("as-runner: AS_RUN_GRANT_CAPS must include workspace for shared project memory")
				os.Exit(1)
			}
			checkSharedMemoryProject = memoryRuntime.BindSharedProject
		}
		logger.Info("as-runner: Hindsight memory enabled")
	}
	var outputArtifacts core.OutputArtifacts
	if artifactClient != nil {
		outputArtifacts = artifactClient
	}
	runtime, configPinner, err := buildProfileRuntime(cfg, core.RuntimeConfig{
		Artifacts:      outputArtifacts,
		Memory:         memoryPort,
		Model:          model,
		Tools:          tools,
		State:          stateClient,
		Durable:        buildEngineDurable(stateClient, logger),
		StepAccountant: accountant,
	})

	if err != nil {
		logger.Error("as-runner: profile runtime configuration invalid", "err", err)
		os.Exit(1)
	}
	var coreRunner space.CoreRunner = runtime
	if runAuthorityScope.enabled() {
		coreRunner = scopedCoreRunner{runner: runtime, authority: runAuthorityScope}
	}

	authMW, err := buildAuth(logger, cfg)
	if err != nil {
		logger.Error("as-runner: auth configuration invalid", "err", err)
		os.Exit(1)
	}

	// Human->scoped-task boundary (PDF §3.5). When human browser auth is enabled,
	// Space MUST exchange the verified human JWT for a scoped Task token via a
	// trusted server-side Space->Identity operation, and forbid the raw bearer from
	// reaching State. Refuse to start human traffic without a configured exchanger
	// rather than leak the human JWT. Tier-2: the same boundary additionally requires a
	// per-run run-grant issuer so a browser run NEVER executes under the shared ingress
	// authority; refuse to start without it (fail closed).
	humanAuthMW := authMW
	requireScoped := authMW != nil
	var runGrants space.RunGrantIssuer
	requireRunGrant := false
	if requireScoped {
		exchanger, xerr := buildExchanger(logger, cfg)
		if xerr != nil {
			logger.Error("as-runner: human->task exchange is required for human auth but is not usable", "err", xerr)
			os.Exit(1)
		}
		if !runAuthorityScope.enabled() || runAuthorityScope.workload == "" {
			logger.Error("as-runner: human->task exchange requires published task keys and a pinned workload")
			os.Exit(1)
		}
		exchangeAuth := space.ExchangeMiddleware(exchanger, authMW)
		authority := runAuthorityScope
		authMW = func(next http.Handler) http.Handler { return exchangeAuth(authority.bindHumanScope(next)) }
		logger.Info("as-runner: human->scoped-task exchange enabled (subject/tenant from human_token; deployment from workload cert)")
		issuer, gerr := buildRunGrantIssuer(logger, cfg)
		if gerr != nil {
			logger.Error("as-runner: Tier-2 per-run run grant is required for human auth but is not usable", "err", gerr)
			os.Exit(1)
		}
		// A run under a real Identity grant can only do what its grant literally holds, and the
		// capability the State API asks Identity about on every task route is "state"
		// (internal/stateapi). Requesting run caps without it configures a run that Identity refuses
		// the moment it tries to persist anything - a guaranteed mid-turn 403 - so refuse at
		// startup and name the missing capability instead of letting the run die later.
		if err := validateRunGrantCaps(splitCSV(cfg.RunGrantCaps)); err != nil {
			logger.Error("as-runner: the Tier-2 run grant is configured with capabilities it cannot use", "err", err)
			os.Exit(1)
		}
		runGrants = issuer
		requireRunGrant = true
		logger.Info("as-runner: Tier-2 per-run run grants enabled (browser runs never use the shared ingress authority; cancel revokes the run subtree)")
	}

	var agentPinner space.AgentTargetPinner
	var agentControl space.AgentControl
	if requireRunGrant && cfg.RegistryURL != "" {
		ownerClient, ownerErr := buildStateClient(cfg)
		if ownerErr == nil {
			agentPinner, ownerErr = space.NewAgentTargetClient(cfg.RegistryURL, ownerClient)
		}
		if ownerErr == nil {
			agentControl, ownerErr = space.NewAgentControlClient(cfg.RegistryURL, ownerClient)
		}
		if ownerErr != nil {
			logger.Error("as-runner: addressed-agent owner API is not usable", "err", ownerErr)
			os.Exit(1)
		}
	}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		ConfigPinner: configPinner, AgentPinner: agentPinner, AgentControl: agentControl,
		ProjectMemory: memoryRuntime != nil, CheckProject: checkMemoryProject, CheckSharedProject: checkSharedMemoryProject,
		StateClient:         stateClient,
		Core:                coreRunner,
		WorkerID:            cfg.WorkerID,
		TurnHistoryThreads:  parseTurnHistoryThreads(cfg.TurnHistoryThreads, logger),
		TurnHistoryBudget:   parseTurnHistoryBudget(cfg.TurnHistoryBudgetMS, logger),
		TurnDispatchTimeout: parseTurnDispatchTimeout(cfg.TurnDispatchTimeoutMS, logger),
		RequireDispatcher:   cfg.RequireTurnDispatcher == "1",
		RunGrantIssuer:      runGrants,
		RequireRunGrant:     requireRunGrant,
		RunGrantCaps:        splitCSV(cfg.RunGrantCaps),
		RunGrantTTL:         parseRunGrantTTL(cfg.RunGrantTTL),
	})

	handler := space.NewHandler(space.HandlerConfig{
		Coordinator:       coord,
		Auth:              authMW,
		RequireScopedTask: requireScoped,
	})

	download, derr := buildDownloadProxy(logger, cfg)
	if derr != nil {
		logger.Error("as-runner: the human download route is configured but not usable", "err", derr)
		os.Exit(1)
	}

	if download != nil {
		// The SAME boundary as the Space API: verify the human JWT, then exchange it for a
		// scoped task token. Without it the route answers 403 on its own (fail closed) -
		// it never falls back to forwarding the browser's credential.
		if authMW != nil {
			download = authMW(download)
		} else {
			logger.Warn("as-runner: human download route is mounted without human auth; every call will be refused")
		}
	}

	resolve, rerr := buildResolveProxy(logger, cfg)
	if rerr != nil {
		logger.Error("as-runner: the human digest route is configured but not usable", "err", rerr)
		os.Exit(1)
	}
	if resolve != nil {
		// The same boundary as the download route and the Space API: verify the human
		// JWT, then exchange it for a scoped task token. Refused on its own without it.
		if authMW != nil {
			resolve = authMW(resolve)
		} else {
			logger.Warn("as-runner: human digest route is mounted without human auth; every call will be refused")
		}
	}

	cost, cerr := buildCostProxy(logger, cfg, coord)
	if cerr != nil {
		logger.Error("as-runner: the human cost route is configured but not usable", "err", cerr)
		os.Exit(1)
	}
	if cost != nil {
		// The same human boundary as every other browser route: verify the session, then
		// exchange it for a scoped task token. The Model Gateway authorizes a TASK token,
		// so without the exchange this route refuses -- it never forwards the browser JWT.
		if authMW != nil {
			cost = authMW(cost)
		} else {
			logger.Warn("as-runner: human cost route is mounted without human auth; every call will be refused")
		}
	}

	planes, perr := buildPlaneProxies(logger, cfg)
	if perr != nil {
		logger.Error("as-runner: a human plane surface is configured but not usable", "err", perr)
		os.Exit(1)
	}
	for route, plane := range planes {
		if route == space.UIToolsPath {
			if humanAuthMW != nil {
				planes[route] = humanAuthMW(plane)
			}
			continue
		}
		// The same human boundary as every other browser route: verify the session, then
		// exchange it for a scoped task token. The plane authorizes a TASK token, so without
		// the exchange these routes refuse -- they never forward the browser JWT.
		if authMW != nil {
			planes[route] = authMW(plane)
		} else {
			logger.Warn("as-runner: human plane route is mounted without human auth; every call will be refused",
				"route", route)
		}
	}

	mux := buildMuxWithPlanes(handler, download, resolve, cost, planes)

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      tracedMux(mux),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Orphan sweep (PDF: аренда/подбор сирот — примитивы State API). Runs that lost their
	// worker, or that were accepted while no worker was alive, are claimed by this runner
	// with its own workload identity - no client call is required for recovery.
	claimInterval := parseClaimInterval(cfg.ClaimIntervalMS, logger)
	coord.StartOrphanClaimLoop(ctx, claimInterval)

	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server error", "err", err)
		os.Exit(1)
	}
	logger.Info("as-runner stopped")
}

// defaultClaimInterval is how often the runner sweeps State for runs that lost their
// worker or were never dispatched, when AS_CLAIM_INTERVAL_MS is unset.
const defaultClaimInterval = 2 * time.Second

type config struct {
	ListenAddr   string
	StateURL     string
	WorkerID     string
	AuthIssuer   string
	AuthAudience string
	AuthKeysRaw  string
	// Human->task exchange (PDF §3.5): a server-side trusted Space->Identity
	// operation. When human auth is enabled these MUST be configured, or the
	// deployment refuses to start (a browser human JWT must never reach State).
	IdentityExchangeURL string
	WorkloadCert        string
	WorkloadKey         string
	WorkloadCA          string
	// Tier-2 per-run run grant. When human auth is enabled, browser runs get their OWN
	// Identity Delegate grant (a child of the ingress authority) instead of the shared
	// ingress token. The Identity service base is derived from IdentityExchangeURL unless
	// AS_IDENTITY_URL overrides it. Caps/TTL are optional (Identity intersects caps and
	// applies defaults). These MUST be usable or the deployment refuses to start.
	IdentityURL  string
	RunGrantCaps string
	RunGrantTTL  string
	// Orphan sweep interval in milliseconds (PDF "подбор сирот"). Default 2000; 0 or a
	// negative value disables the sweep and keeps client-triggered dispatch only.
	ClaimIntervalMS string
	// TurnHistoryThreads / TurnHistoryBudgetMS bound the post-restart durable turn read
	// (unscoped ListTurns fan-out). Empty => space defaults; a negative thread count
	// disables the durable read; a negative budget disables it too.
	TurnHistoryThreads  string
	TurnHistoryBudgetMS string

	// TurnDispatchTimeoutMS bounds how long a turn accepted while NO claim loop is
	// running stays `pending` before Space terminalizes it (DEF-H30-x). Empty => the
	// space default; a negative value disables the watchdog. It never arms while the
	// sweep is enabled. RequireTurnDispatcher ("1") makes AcceptTurn refuse a turn this
	// front cannot dispatch (503 no_turn_dispatcher) instead of accepting it into that
	// bounded window - opt in only for a deployment that must never queue what it can't run.
	TurnDispatchTimeoutMS string
	RequireTurnDispatcher string

	// Human download route (PDF rev3.2 рис. 7 "скачивание через прокси: текущие права,
	// точная версия"). AS_ARTIFACTS_URL unset = the route is not mounted at all; when it IS
	// set, Space's own workload certificate is mandatory (see buildDownloadProxy), because
	// a proxy that cannot present a workload identity has nothing honest to answer with.
	ArtifactsURL     string
	ArtifactGrantTTL string

	// Human cost read (PDF rev3.2 :347 "стоимость задачи"). AS_MODEL_COST_URL unset = the
	// route is not mounted at all and the accepted surfaces are byte-identical. When it IS
	// set it must be an ORIGIN (no API path), and Space's own workload certificate is
	// mandatory: the Model Gateway authorizes a scoped task token bound to a workload, so a
	// process that cannot present one has nothing honest to ask with.
	ModelCostURL string

	// Human plane surfaces (PDF rev3.2 :323 operator surface, :957 "оценки перед
	// публикацией"). Each origin is an OPTIONAL deployment knob: with the variable unset the
	// route is not mounted at all and every accepted surface stays byte-identical. When set
	// it must be an ORIGIN without an API path, and Space's own workload certificate is
	// mandatory, because the machine face authorizes a scoped task token bound to a workload
	// and a browser can never present one.
	RegistryURL  string
	EvalsURL     string
	TriggersURL  string
	MemoryURL    string
	ComputeURL   string
	WorkspaceURL string
	ToolsURL     string
}

// stateCapability is the capability State consults Identity about on its task routes
// (internal/stateapi: capability = "state"). A run grant that does not hold it cannot persist.
const stateCapability = "state"

// validateRunGrantCaps refuses a Tier-2 capability set that cannot drive the product.
func validateRunGrantCaps(caps []string) error {
	if !holdsCapability(caps, stateCapability) {
		return fmt.Errorf("AS_RUN_GRANT_CAPS must include %q (the capability State consults Identity about), got %v",
			stateCapability, caps)
	}
	return nil
}

// holdsCapability reports whether want appears verbatim in have.
func holdsCapability(have []string, want string) bool {
	for _, v := range have {
		if v == want {
			return true
		}
	}
	return false
}

func loadConfig() config {
	return config{
		ListenAddr:            envDefault("AS_LISTEN", ":8090"),
		StateURL:              envDefault("AS_STATE_URL", "http://localhost:8081"),
		WorkerID:              envDefault("AS_WORKER_ID", fmt.Sprintf("runner-%d", os.Getpid())),
		AuthIssuer:            os.Getenv("AS_AUTH_ISSUER"),
		AuthAudience:          os.Getenv("AS_AUTH_AUDIENCE"),
		AuthKeysRaw:           os.Getenv("AS_AUTH_KEYS"),
		IdentityExchangeURL:   envDefault("AS_IDENTITY_EXCHANGE_URL", ""),
		WorkloadCert:          os.Getenv("AS_WORKLOAD_CERT"),
		WorkloadKey:           os.Getenv("AS_WORKLOAD_KEY"),
		WorkloadCA:            os.Getenv("AS_WORKLOAD_CA"),
		IdentityURL:           envDefault("AS_IDENTITY_URL", ""),
		RunGrantCaps:          os.Getenv("AS_RUN_GRANT_CAPS"),
		RunGrantTTL:           envDefault("AS_RUN_GRANT_TTL", "15m"),
		ClaimIntervalMS:       envDefault("AS_CLAIM_INTERVAL_MS", "2000"),
		TurnHistoryThreads:    envDefault("AS_TURN_HISTORY_THREADS", ""),
		TurnHistoryBudgetMS:   envDefault("AS_TURN_HISTORY_BUDGET_MS", ""),
		TurnDispatchTimeoutMS: envDefault("AS_TURN_DISPATCH_TIMEOUT_MS", ""),
		RequireTurnDispatcher: os.Getenv("AS_REQUIRE_TURN_DISPATCHER"),
		ArtifactsURL:          envDefault("AS_ARTIFACTS_URL", ""),
		ModelCostURL:          envDefault("AS_MODEL_COST_URL", ""),
		RegistryURL:           envDefault("AS_REGISTRY_URL", ""),
		EvalsURL:              envDefault("AS_EVALS_URL", ""),
		TriggersURL:           envDefault("AS_TRIGGERS_URL", ""),
		MemoryURL:             envDefault("AS_MEMORY_URL", ""),
		ComputeURL:            envDefault("AS_COMPUTE_URL", ""),
		WorkspaceURL:          envDefault("AS_WORKSPACE_URL", ""),
		ToolsURL:              envDefault("AS_TOOLS_URL", ""),
		ArtifactGrantTTL:      envDefault("AS_ARTIFACT_GRANT_TTL", ""),
	}
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// buildEngineDurable returns a stateless-engine durability sink that persists every
// Core-loop step to the State API THROUGH the Emil run-store seam (adapters/emil ->
// the existing StateClient.AppendStream), realizing the PDF "stateless Core Agent pool
// stores steps in the Agent State Store by State API contract". It is OFF by default so
// the default deployment behavior is unchanged; enable with AS_ENGINE_DURABLE=1 (a
// deployment/composition choice). The authoritative State stream dedups identical step
// keys idempotently and fences stale writers; returns nil (emit-only) when disabled.
func buildEngineDurable(stateClient *space.StateClient, logger *slog.Logger) space.DurabilitySink {
	if os.Getenv("AS_ENGINE_DURABLE") != "1" {
		return nil
	}
	logger.Info("as-runner: engine durability via State API seam enabled (AS_ENGINE_DURABLE=1)")
	return emil.NewRunStoreDurabilitySink(emil.NewStateRunStore(stateClient))
}

// buildAuth wires the human-token Auth middleware for the Space browser edge
// from environment configuration. When no trusted keys are configured it
// returns a nil middleware, and the Space handler then fails closed (every
// /v1/space/* call returns 403) rather than serving unauthenticated requests.
//
// Config:
//
//	AS_AUTH_ISSUER   identity issuer id (required to enable auth)
//	AS_AUTH_AUDIENCE expected aud claim (defaults to the Space default)
//	AS_AUTH_KEYS     comma-separated list of kid=<base64 Ed25519 public key>
//
// buildMux assembles the standalone Space deployment HTTP surface: the Space
// API under /v1/space/, the web UI at /, and a liveness probe at /healthz. It is
// extracted from main so the exact production routing is exercised by tests.
// downloadHandler is the human-edge artifact download route (space.UIDownloadPath), or
// nil when the deployment has no Artifacts surface - then the path is not a route at all.
// It is wrapped in main with the SAME auth + human->task exchange middleware that guards
// the Space API, because the only authority the proxy may carry onward is the exchanged
// scoped task token - never the browser's own credential.
func buildMux(spaceHandler, downloadHandler http.Handler) *http.ServeMux {
	return buildMuxWithResolve(spaceHandler, downloadHandler, nil)
}

// buildMuxWithResolve is buildMux plus the human digest resolver. It exists as its own
// function for the same reason buildMux does: so the routing a test asserts is the
// routing the process installs. resolveHandler is nil when the deployment has no
// Artifacts surface, and then the frozen path is not a route of ours at all.
func buildMuxWithResolve(spaceHandler, downloadHandler, resolveHandler http.Handler) *http.ServeMux {
	return buildMuxWithCost(spaceHandler, downloadHandler, resolveHandler, nil)
}

// buildMuxWithCost is buildMux with every human edge route, including the read-only cost
// read. It stays the single place the browser-visible routing is decided so a test can
// assert the routing the process installs. costHandler is nil when the deployment has no
// Model Gateway surface, and then the path is simply not a route of ours.
func buildMuxWithCost(spaceHandler, downloadHandler, resolveHandler, costHandler http.Handler) *http.ServeMux {
	return buildMuxWithPlanes(spaceHandler, downloadHandler, resolveHandler, costHandler, nil)
}

// buildMuxWithPlanes is buildMux with the human plane surfaces (registry/evals/triggers/
// memory) mounted at their browser prefixes. A route absent from the map is not a route of
// ours at all, so an unconfigured deployment answers exactly as it did before this file: the
// single-origin edge then returns its API-shaped 404, which the UI shows as unavailable
// rather than as success.
func buildMuxWithPlanes(spaceHandler, downloadHandler, resolveHandler, costHandler http.Handler, planes map[string]http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/v1/space/", spaceHandler)
	if downloadHandler != nil {
		mux.Handle(space.UIDownloadPath, downloadHandler)
	}
	if resolveHandler != nil {
		mux.Handle(space.UIResolvePath, resolveHandler)
	}
	if costHandler != nil {
		mux.Handle(space.UICostPath, costHandler)
	}
	for route, plane := range planes {

		mux.Handle(route, plane)
	}
	mux.Handle("/", web.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}

func buildAuth(logger *slog.Logger, cfg config) (func(http.Handler) http.Handler, error) {
	if cfg.AuthIssuer == "" || cfg.AuthKeysRaw == "" {
		logger.Warn("as-runner: human auth disabled (no AS_AUTH_ISSUER/AS_AUTH_KEYS); Space API fails closed")
		return nil, nil
	}
	keys, err := parseAuthKeys(cfg.AuthKeysRaw)
	if err != nil {
		return nil, err
	}
	mw, err := space.HumanAuthMiddleware(space.HumanAuthConfig{
		Issuer:   cfg.AuthIssuer,
		Audience: cfg.AuthAudience,
		Keys:     keys,
	})
	if err != nil {
		return nil, err
	}
	logger.Info("as-runner: human token auth enabled", "issuer", cfg.AuthIssuer, "keys", len(keys))
	return mw, nil
}

// parseAuthKeys decodes a comma-separated kid=<base64 Ed25519 public key> list
// into the verifier key map. Both standard (padded) and raw (unpadded) base64
// are accepted; each decoded key must be exactly 32 bytes.
func parseAuthKeys(raw string) (map[string]ed25519.PublicKey, error) {
	out := map[string]ed25519.PublicKey{}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		kid, enc, ok := strings.Cut(pair, "=")
		if !ok || strings.TrimSpace(kid) == "" {
			return nil, fmt.Errorf("as-runner: invalid AS_AUTH_KEYS entry %q", pair)
		}
		kid = strings.TrimSpace(kid)
		enc = strings.TrimSpace(enc)
		b, derr := base64.StdEncoding.DecodeString(enc)
		if derr != nil {
			b, derr = base64.RawStdEncoding.DecodeString(enc)
			if derr != nil {
				return nil, fmt.Errorf("as-runner: AS_AUTH_KEYS %s is not valid base64", kid)
			}
		}
		if len(b) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("as-runner: AS_AUTH_KEYS %s must decode to a 32-byte Ed25519 public key", kid)
		}
		out[kid] = ed25519.PublicKey(b)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("as-runner: AS_AUTH_KEYS decoded to no keys")
	}
	return out, nil
}

// parseClaimInterval turns AS_CLAIM_INTERVAL_MS into a sweep interval. An unset/empty or
// unparsable value falls back to the default 2s; an explicit non-positive value disables
// the sweep (then only a client-triggered DispatchTurn advances a run).
func parseClaimInterval(raw string, logger *slog.Logger) time.Duration {
	if raw == "" {
		return defaultClaimInterval
	}
	ms, err := strconv.Atoi(raw)
	if err != nil {
		logger.Warn("as-runner: AS_CLAIM_INTERVAL_MS is not an integer, using default",
			"value", raw, "default_ms", defaultClaimInterval.Milliseconds())
		return defaultClaimInterval
	}
	if ms <= 0 {
		logger.Warn("as-runner: orphan sweep disabled by configuration; runs advance only when a client dispatches them")
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// parseTurnHistoryThreads reads AS_TURN_HISTORY_THREADS: how many of a caller's threads an
// unscoped ListTurns may walk to rebuild turn history after an as-runner restart. Empty or
// unparsable => space.DefaultTurnHistoryThreads; negative => the durable read is disabled;
// above the ceiling => the ceiling (space clamps it).
func parseTurnHistoryThreads(raw string, logger *slog.Logger) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0 // space.NewCoordinator applies the default
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		logger.Warn("as-runner: AS_TURN_HISTORY_THREADS unparsable, using the default durable turn read", "value", raw)
		return 0
	}
	return v
}

// parseTurnHistoryBudget reads AS_TURN_HISTORY_BUDGET_MS: the wall-time bound on that same
// walk, so one slow State cannot hang the browser. Empty/unparsable => the default.
func parseTurnHistoryBudget(raw string, logger *slog.Logger) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		logger.Warn("as-runner: AS_TURN_HISTORY_BUDGET_MS unparsable, using the default durable turn budget", "value", raw)
		return 0
	}
	if v < 0 {
		return -1 // disable the durable walk
	}
	return time.Duration(v) * time.Millisecond
}

// parseTurnDispatchTimeout reads AS_TURN_DISPATCH_TIMEOUT_MS: the DEF-H30-x window a turn
// accepted while no claim loop runs stays pending before Space terminalizes it. The value
// is forwarded in turnHistoryBudget's shape (space.turnDispatchTimeout decodes it): empty/
// unparsable => 0 => the space default; negative => -1 => the watchdog is disabled. It is
// irrelevant while the sweep is on, because a running claim loop declares the dispatcher and
// the watchdog never arms beside it.
func parseTurnDispatchTimeout(raw string, logger *slog.Logger) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		logger.Warn("as-runner: AS_TURN_DISPATCH_TIMEOUT_MS unparsable, using the default no-dispatcher window", "value", raw)
		return 0
	}
	if v < 0 {
		return -1 // disable the DEF-H30-x watchdog
	}
	return time.Duration(v) * time.Millisecond
}
