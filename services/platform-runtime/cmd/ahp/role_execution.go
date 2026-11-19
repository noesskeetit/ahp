//go:build ahp_exec

package main

import (
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	aot "github.com/ahp-platform/ahp/pkg/otel"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// serveExecution mounts the execution plane (the durable A2A gateway) on its
// own address. The executor is supplied through the gateway constructor's
// injection point: by default the composed Model Gateway bridge
// (internal/execbridge), whose every model turn is a durable generation admitted
// against a per-task token budget, so the plane holds no provider credential.
// AHP_EXEC_MODEL_VIA_GATEWAY=0 opts out to the canonical Qwen+tools executor.
// Nothing here is ever fabricated: with no provider runtime secret the bridge
// drains tasks to a terminal FAILED with the real reason. This whole file is
// gated behind the ahp_exec build tag so the default build and every other role
// compile without internal/gateway or a2a-go.
func (s *server) serveExecution(ctx context.Context, addr string) error {
	handler, err := s.buildExecutionHandler(ctx)
	if err != nil {
		return err
	}
	mux := s.baseMux()
	mountExecutionPlane(mux, handler)
	if err := s.tryMountCompute(ctx, mux); err != nil {
		return err
	}
	return s.runServer(ctx, "execution", addr, aot.HTTPMiddleware("execution")(mux))
}

// mountExecutionPlane routes the execution plane onto mux. The A2A faces live
// under the /a2a/ subtree (public, internal, health), and the gateway serves
// its public Agent Card at the canonical well-known path -- which is NOT under
// /a2a/ -- so that path is routed to the same handler too. The gateway 404s the
// well-known path when no card is configured, so this is a no-op unless the
// operator published a card; nothing is fabricated.
func mountExecutionPlane(mux *http.ServeMux, handler http.Handler) {
	mux.Handle("/a2a/", handler)
	mux.Handle(gateway.WellKnownAgentCardPath, handler)
}

// tryMountExecution co-locates the execution plane on a shared mux for the
// "all" development role.
func (s *server) tryMountExecution(ctx context.Context, mux *http.ServeMux) error {
	handler, err := s.buildExecutionHandler(ctx)
	if err != nil {
		return err
	}
	mountExecutionPlane(mux, handler)
	// The compute plane belongs to the execution role: running a command is
	// execution, and mounting it here means the aggregate and the split
	// topology get the same route table. It is a no-op unless AHP_ENABLE_COMPUTE=1.
	return s.tryMountCompute(ctx, mux)
}

func (s *server) buildExecutionHandler(ctx context.Context) (http.Handler, error) {
	idStore := s.identityStore()
	if err := idStore.Migrate(ctx); err != nil {
		return nil, err
	}
	// The gateway store requires a schema matching ^ahp_[a-z][a-z0-9_]{0,40}$. All
	// gateway / ledger tables live in this Postgres schema, scoped inside the
	// process's own database (isolation is enforced by the store regex).
	schema := envOr("AHP_EXEC_SCHEMA", "ahp_exec")
	gstore, err := gateway.NewStore(s.pool, schema)
	if err != nil {
		return nil, err
	}
	if err := gstore.Migrate(ctx); err != nil {
		return nil, err
	}
	ledger, err := operations.New(s.pool, schema)
	if err != nil {
		return nil, err
	}
	if err := ledger.Migrate(ctx); err != nil {
		return nil, err
	}
	// Execution-role injection point. Default: the Model Gateway bridge, so an
	// accepted A2A task runs a real core.Runtime turn whose model calls are
	// durable internal/model generations -- budget-admitted BEFORE any spend,
	// effect-ledger fenced, settled from real usage. The bridge never receives a
	// provider key; the credential stays behind the model.Inference the Model
	// Gateway owns, and with no runtime secret it fails closed to FAILED rather
	// than inventing a turn.
	//
	// AHP_EXEC_MODEL_VIA_GATEWAY=0 is the explicit opt-out that restores the
	// canonical Qwen+tools->object-storage executor. It is the only wiring that
	// reads a direct provider runtime secret, and NewFromEnv fails closed when
	// that secret or object storage is unconfigured.
	var exec gateway.Executor
	if envOr("AHP_EXEC_MODEL_VIA_GATEWAY", "1") == "0" {
		direct, derr := executor.NewFromEnv(ctx, s.pool)
		if derr != nil {
			return nil, derr
		}
		exec = direct
		s.logger.Info("execution role: direct provider executor (gateway opt-out)",
			"via_gateway", false, "wiring", "internal/executor")
	} else {
		// The gateway wiring is the default, and it used to be model-only: a
		// bridged turn could never seal the artifact that the Agent Card this same
		// process publishes advertises, so the public face answered COMPLETED with
		// nothing to download while its budget was really spent.
		//
		// The artifact plane carries BOTH doors and both are now wired: its tool
		// registry (write_artifact) reaches the model -- the Model Gateway face does
		// have a tool wire, internal/model reassembles streamed tool_calls and
		// gateway_model.go sends the definitions -- and its export door remains the
		// fallback for a turn where the model wrote nothing itself. Dropping
		// plane.Tools here was the defect that made the deployed face seal the
		// model's chat reply instead of the file the model asked to write.
		// Without object storage the plane says so out loud instead of pretending it
		// can serve the skill.
		var exports execbridge.ArtifactExporter
		var tools *core.ToolRegistry
		if executor.ObjectStorageConfigured() {
			plane, perr := executor.ArtifactPlaneFromEnv(ctx, s.pool)
			if perr != nil {
				return nil, perr
			}
			exports = plane
			tools = plane.Tools
		} else {
			s.logger.Warn("execution role: object storage not configured, turn-result export not mounted",
				"consequence", "a completed A2A task will carry no downloadable artifact",
				"env_required", executor.EnvS3Endpoint)
		}
		bridge, berr := execbridge.FromEnvWithArtifactsAndTools(ctx, s.pool, exports, tools)
		if bridge != nil {
			// The routing ceiling also reads what the CONVERSATION carries now:
			// the platform's label journal accumulates labels per context, and a
			// deployment record cannot express "this task picked up a taint".
			bridge.WithTaskLabels(taskLabelJournal(ctx, s.pool))
		}
		if berr != nil {
			return nil, berr
		}
		bridge.WithTaskLabels(identityContextLabels{store: idStore})
		if err := s.configureModelBudgets(ctx, bridge); err != nil {
			return nil, err
		}
		s.registerTenantDeletionOwner("model", bridge)
		exec = bridge
		s.logger.Info("execution role: model turns routed through the Model Gateway",
			"via_gateway", true, "wiring", "internal/execbridge",
			"model", bridge.Model(), "provider_configured", bridge.ProviderConfigured(),
			"artifact_export_mounted", exports != nil,
			"tool_wire", bridge.ToolsMounted(),
			"task_token_limit", bridge.TaskTokenLimit())
	}
	// Identity arrives over published Identity keys, the same verifier the faces
	// use. Hoisted above the service because the acceptance point needs it too:
	// when a delegated child names a parent this plane cannot see, the lineage is
	// taken from the task credential that presents itself, verified here rather
	// than assumed from the ingress that happened to be configured (DEF-H10-8).
	verifier, _ := devIdentityAuth()
	aud := envOr("AHP_EXEC_AUDIENCE", "ahp")
	// One operator number, two consumers: the ceiling the gateway validates a
	// caller-stated max_steps against is exactly the per-attempt bound the bridge
	// runs with, so a request that asks for more is refused at the door instead of
	// being quietly cut down mid-flight.
	svc, err := gateway.NewService(gstore, ledger, exec, nil, gateway.Config{
		StepBudgetCeiling: execbridge.MaxStepsFromEnv(),
		TaskVerifier:      verifier,
		TaskAudience:      aud,
		// An Invoke names the artifact it selected; this is what checks the name
		// before a task runs (DEF: root audit, "invoke runs the shared endpoint
		// without the selected artifact"). A claim that cannot be verified here is
		// refused, so the acceptance answer "the selected runtime served it" is a
		// checked fact or a refusal, never an assumption.
		Invocations: artifactVerifier(s.pool),
	})
	if err != nil {
		return nil, err
	}
	s.logger.Info("execution role: delegated-child acceptance configured",
		"parent_attestation_verifier", verifier != nil, "parent_attestation_audience", aud,
		"invocation_artifact_verifier", true)
	// Durable drain worker: the gateway persists an accepted task as SUBMITTED on
	// the public face and runs it only when a worker claims it from the store. The
	// HTTP face alone never advances a task, so the execution role must start the
	// fenced worker loop. Start it for the role lifetime: an accepted task is then
	// drained SUBMITTED -> WORKING -> terminal by the canonical executor (native
	// Qwen + tool round-trip -> sealed MinIO artifact -> durable ledger). Shutdown
	// cancels ctx, which releases held leases so a restart resumes without
	// double-running a side effect (closes iv-119: composed role=all mounted the
	// face but started no consumer, leaving tasks stranded at SUBMITTED). The
	// internal/gateway semantics are unchanged -- this only drives the existing
	// accepted Service from the execution injection point.
	s.registerTenantDeletionOwner("gateway", gstore)
	s.registerSubjectDeletionOwner("gateway", gstore)
	executor.StartWorker(ctx, svc)
	failClosed := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "execution internal face disabled in this build", http.StatusForbidden)
		})
	}
	// The execution plane is addressed over A2A as a workload/deployment
	// principal, not as a raw human (director arbitration d71a7b0e, option A). A
	// human token carries no deployment claim, so its scope can never pass the
	// gateway's full-scope (tenant AND deployment AND subject) predicate. The
	// Public face therefore accepts a SCOPED task token (KindTask bound to the
	// mTLS certificate workload) via the canonical transport.Task path, so
	// Principal.Scope is complete. It stays fail-closed on an incomplete scope, a
	// foreign tenant/subject and a missing client certificate; the coarse
	// authorize callback mirrors the accepted state/data plane harness and real
	// isolation is enforced downstream by the scope (tenant AND deployment AND
	// subject). internal/gateway semantics are unchanged.
	const execCapability = "execution"
	authorize := idStore.Authorize
	// The public Agent Card is an operator-provided, fail-closed document: it is
	// loaded from AHP_EXEC_AGENT_CARD_FILE only when that env names a file, and
	// then must satisfy the platform contract and leak nothing (no /a2a/internal,
	// no DSN, no credentials). With no file configured the plane serves no card
	// (404), exactly as before -- nothing is ever invented. The card is published
	// on the PUBLIC face only (StaticAgentCardHandler at the well-known path),
	// mounted by the callers that own the mux; internal/gateway semantics are
	// unchanged.
	internalPath := envOr("AHP_EXEC_INTERNAL_PATH", "/a2a/internal")
	card, err := gateway.LoadAgentCardFile(envOr("AHP_EXEC_AGENT_CARD_FILE", ""), internalPath)
	if err != nil {
		return nil, err
	}
	if card != nil {
		s.logger.Info("execution role: publishing public Agent Card",
			"name", card.Name, "version", card.Version,
			"skills", len(card.Skills), "path", gateway.WellKnownAgentCardPath)
	}
	// The authenticated extended card agrees with the public well-known card by
	// construction: the same operator-provided document (or none), so the two
	// never disagree. GetExtendedAgentCard keeps its library contract (nil card ->
	// EXTENDED_CARD_NOT_CONFIGURED) when no card is configured.
	h := gateway.NewHandler(svc, gateway.HandlerConfig{ExtendedCard: card})
	return gateway.NewServer(h, gateway.ServerConfig{
		AgentCard:    card,
		Public:       gateway.AuthConfig{Middleware: transport.Task(verifier, aud, execCapability, authorize)},
		Internal:     gateway.AuthConfig{Middleware: failClosed},
		PublicPath:   envOr("AHP_EXEC_PUBLIC_PATH", "/a2a/public"),
		InternalPath: internalPath,
		HealthPath:   "/a2a/healthz",
	})
}

// identityTaskLabels is the routing ceiling's other half: the label journal the
// platform keeps per conversation (internal/identity), read through Identity's own
// scope-bound accessor. The composition root adapts it; the bridge stays free of
// the identity schema.
//
// It lives in this file because this is where the bridge is mounted: the
// composition invariant (tests/execution TestExecbridgeMountedOnlyInComposedBinary)
// allows execbridge to be named only at the mounting points, and an adapter that
// satisfies one of its interfaces IS such a name. Moving it to a tidier file would
// have meant widening that allowlist for a cosmetic reason.
type identityTaskLabels struct {
	store *identity.Store
}

func (l identityTaskLabels) LabelsForContext(ctx context.Context, scope contracts.Scope, contextID string) (labels.Set, error) {
	set, found, err := l.store.ContextLabels(ctx, scope, contextID)
	if err != nil {
		return labels.Set{}, err
	}
	if !found {
		return labels.Set{}, execbridge.ErrTaskLabelJournalAbsent
	}
	return set, nil
}

// taskLabelJournal returns the journal reader when this process's database
// actually carries the identity journal. Where it does not, routing is left with
// the deployment record alone and says so in its own log line -- mounting a reader
// that can only error would turn every generation into a refusal for a wiring
// fact, not a policy decision.
func taskLabelJournal(ctx context.Context, pool *pgxpool.Pool) execbridge.TaskLabelJournal {
	var regclass *string
	if err := pool.QueryRow(ctx, `SELECT to_regclass('ahp_identity.contexts')::text`).Scan(&regclass); err != nil || regclass == nil {
		return nil
	}
	return identityTaskLabels{store: identity.New(pool)}
}
