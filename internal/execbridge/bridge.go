package execbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/model/routing"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DefaultModelSchema is the Postgres schema the Model Gateway owns. Generations
// and budgets are fixed there by internal/model; the effect ledger follows the
// same schema so one plane owns one namespace.
const DefaultModelSchema = "ahp_model"

// RootBudgetKey identifies token budgets in the authoritative task tree.
// Root creation and child allocation are idempotent on
// (tenant, subject, deployment, task, key), so a retried attempt re-uses the
// same ceiling instead of opening a second one.
const RootBudgetKey = "execbridge-task-root"

const (
	defaultMaxSteps       = 4
	defaultTaskTokenLimit = 20000
	defaultStepReserve    = 4000
	defaultMaxTokens      = 1024
)

// Config wires a Bridge. Inference is the only provider handle and it is owned
// by the Model Gateway: the bridge itself never sees a credential.
type Config struct {
	// Schema is the effect-ledger schema; defaults to DefaultModelSchema.
	Schema string
	// Inference is required. Use FromEnv to resolve it from the runtime secret,
	// or pass UnconfiguredInference to mount the plane with no provider.
	Inference model.Inference
	// ModelName pins the model identifier recorded on generations; empty takes
	// the inference handle's own Model().
	ModelName string
	// Tools is optional. Without it a bridged turn is model-only. With it the
	// model receives the tool definitions on every provider request and its
	// tool calls are dispatched by the runtime -- the Model Gateway face DOES
	// have a tool wire (internal/model parses streamed tool_calls fragments and
	// reassembles them; internal/execbridge/gateway_model.go carries the
	// definitions both ways), so a plane that mounts tools must pass them here.
	Tools *core.ToolRegistry
	// Artifacts is the plane's own export door: the sealed object a completed
	// turn leaves behind when the model did not write one itself. It is the
	// FALLBACK, not the only door: when a tool call already sealed an artifact
	// this turn, that seal is what the task carries and the fallback export is
	// skipped, so a turn never ends with the model's file overwritten or
	// duplicated by the plane's own text.
	// Nil means this face cannot deliver an artifact and says so at startup.
	Artifacts ArtifactExporter
	// MaxSteps bounds the core.Runtime loop per attempt.
	MaxSteps int
	// TaskTokenLimit is the per-task root budget ceiling in tokens. Every step
	// reservation is admitted against it BEFORE any provider call.
	TaskTokenLimit int64
	// ChildTokenLimit bounds each delegated slice; zero selects half the parent ceiling.
	ChildTokenLimit int64
	// StepReserve is the per-step admission bound.
	StepReserve int64
	// MaxTokens is the per-request completion cap sent to the provider.
	MaxTokens int
	// QuotaProvider names the upstream whose provider quotas (RPM/TPM) gate
	// generation admission. Empty means no quota gate at all: every tenant is
	// unlimited and the quota tables are not even migrated, so an unconfigured
	// deployment is byte-identical to one without the feature.
	QuotaProvider string
	// QuotaRequired is the operator's fail-closed profile. PDF rev3.2 gives the
	// Model Gateway the provider quotas (:1627) and bounds each installation's
	// model quotas per tenant (:2495), so an operator who declares quotas
	// mandatory must not be served a gateway that quietly holds none: with this
	// set and no provider named, the bridge refuses to mount rather than starting
	// an unlimited plane and letting the provider's 429s be the alarm.
	//
	// It is opt-in and defaults to false, which is what keeps every existing
	// acceptance path -- whose deployments name a provider and bind per-tenant
	// rows -- byte-for-byte unchanged.
	QuotaRequired bool
	// QuotaWindowSeconds overrides the per-row window for reads this bridge
	// reports; limits themselves live in ahp_model.provider_quotas per tenant.
	QuotaWindowSeconds int
	// SecretResolver and InferenceFactory are the physical-provider seams. With
	// both unset the production pair is used: the process's own Vault handle and
	// model.NewQwenClient, which pins egress to the host inside the secret. A
	// regression sets them to place two REAL HTTP sinks behind two declared
	// copies and prove the route decides the socket (ARCH-02) -- the substitute is
	// the secret store, never the transport or the selection logic.
	SecretResolver   SecretResolver
	InferenceFactory inferenceFactory
	// TaskLabels is the task/context label journal the routing ceiling is merged
	// from (Identity's journal, wired by the composition root). Nil keeps routing
	// checked against the operator's deployment record alone, and the routed line
	// says task_label_journal=not_mounted instead of implying more was read.
	TaskLabels TaskLabelJournal
	// RoutingFile points at the provider table the label-aware router reads
	// (PDF :1620-1630, "маршруты с учётом меток"). Empty means no router: the
	// bridge routes to the single provider named by the environment, exactly as
	// it did before routing existed, which is what keeps every existing
	// acceptance path byte-identical.
	RoutingFile string
	// ExecutionClass is the class this deployment runs under (PDF :1532/:1542,
	// quotas and routes per tenant AND class). Empty means the deployment did not
	// state one, which is a stated fact, not a default class.
	ExecutionClass string
}

// Bridge is a gateway.Executor whose model turns are durable Model Gateway
// generations. Safe for concurrent use: per-attempt identity travels via
// context, the runtime and stores are stateless across calls.
type Bridge struct {
	files           *model.FileService
	childTokenLimit int64
	budgetLineage   BudgetLineage
	budgetCompleted func(context.Context, model.BudgetTask) (bool, error)
	gens            *model.GenerationStore
	budgets         *model.BudgetStore
	svc             *model.Service
	inf             model.Inference
	tools           *core.ToolRegistry
	artifacts       ArtifactExporter
	modelName       string
	maxSteps        int
	maxTokens       int
	taskTokenLimit  int64
	stepReserve     int64
	quotas          *model.QuotaStore
	router          *routing.Router
	// providerClients picks the physical client of the provider copy a generation
	// was routed to (nil when no routing table is mounted).
	providerClients *providerClients
	// taskLabels is the task/context label journal (nil when unwired).
	taskLabels TaskLabelJournal
	// deploymentLabels is the routing label source: the operator's record of what
	// data each deployment sends. Mounted together with the router, nil without it.
	deploymentLabels *model.DeploymentLabelStore
	executionClass   string
	unconfigured     bool
}

var _ gateway.Executor = (*Bridge)(nil)

// ArtifactExporter seals one turn's result into tenant-scoped object storage and
// returns the seal the store verified. *executor.ArtifactPlane implements it, and
// so can a test. The seal is the fact: its sha256 comes from the store's own
// verification of the uploaded bytes, never from a claim by the model or by this
// process.
type ArtifactExporter interface {
	Seal(ctx context.Context, scope contracts.Scope, b artifacts.Binding, name, mime, content string) (executor.SealedArtifact, error)
}

// New migrates the Model Gateway tables over pool and builds a bridge. It fails
// closed on a missing pool or missing inference handle rather than inventing a
// provider.
func New(ctx context.Context, pool *pgxpool.Pool, cfg Config) (*Bridge, error) {
	if pool == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "execbridge: nil pool")
	}
	if cfg.Inference == nil {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"execbridge: a model.Inference handle is required (use FromEnv to resolve the runtime secret)")
	}
	schema := cfg.Schema
	if schema == "" {
		schema = DefaultModelSchema
	}
	ledger, err := operations.New(pool, schema)
	if err != nil {
		return nil, fmt.Errorf("execbridge: effect ledger: %w", err)
	}
	gens := model.NewGenerationStore(pool)
	budgets := model.NewBudgetStore(pool)
	if err := gens.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("execbridge: generations migrate: %w", err)
	}
	if err := budgets.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("execbridge: budgets migrate: %w", err)
	}
	if err := ledger.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("execbridge: ledger migrate: %w", err)
	}

	b := &Bridge{
		gens:            gens,
		budgets:         budgets,
		svc:             model.NewService(gens, budgets, ledger, cfg.Inference),
		inf:             cfg.Inference,
		tools:           cfg.Tools,
		artifacts:       cfg.Artifacts,
		modelName:       strings.TrimSpace(cfg.ModelName),
		maxSteps:        positiveOr(cfg.MaxSteps, defaultMaxSteps),
		maxTokens:       positiveOr(cfg.MaxTokens, defaultMaxTokens),
		taskTokenLimit:  positiveInt64Or(cfg.TaskTokenLimit, defaultTaskTokenLimit),
		childTokenLimit: cfg.ChildTokenLimit,
		stepReserve:     positiveInt64Or(cfg.StepReserve, defaultStepReserve),
		taskLabels:      cfg.TaskLabels,
	}
	if b.modelName == "" {
		b.modelName = cfg.Inference.Model()
	}
	// Provider quotas (RPM/TPM) are mounted only when this deployment names the
	// upstream they apply to. When they are mounted, a missing or unreachable
	// quota ledger is a fail-closed condition: a gateway that believes it is
	// holding a tenant to a per-minute limit while it cannot read the counters is
	// not holding anything, and the operator would learn that from the provider's
	// own 429s.
	if provider := strings.TrimSpace(cfg.QuotaProvider); provider != "" {
		quotas := model.NewQuotaStore(pool)
		if err := quotas.Migrate(ctx); err != nil {
			return nil, fmt.Errorf("execbridge: provider quotas migrate: %w", err)
		}
		quotas.RequireExplicitLimits(cfg.QuotaRequired)
		b.svc.WithQuota(quotas, provider)
		b.quotas = quotas
		window := cfg.QuotaWindowSeconds
		if window <= 0 {
			window = model.DefaultQuotaWindowSeconds
		}
		slog.Info("execbridge: provider quota gate mounted",
			"provider", provider, "default_window_seconds", window,
			"limits_table", "ahp_model.provider_quotas",
			"explicit_limits_required", cfg.QuotaRequired)
	} else if cfg.QuotaRequired {
		// Fail closed: the operator declared quotas mandatory, and this
		// deployment cannot honour that claim. Starting anyway would advertise
		// bounded tenants while admitting everything.
		return nil, contracts.NewReasoned(contracts.ErrConflict, ReasonQuotaRequiredUnconfigured,
			"execbridge: quotas are declared mandatory (AHP_MODEL_QUOTA_REQUIRED) but no provider is named (AHP_MODEL_PROVIDER unset), so no tenant could be bounded -- refusing to mount instead of serving unlimited")
	} else {
		slog.Info("execbridge: provider quota gate not mounted (AHP_MODEL_PROVIDER unset); all tenants unlimited",
			"consequence", "generation admission is bounded by the task budget only",
			"operator_option", "AHP_MODEL_QUOTA_REQUIRED=1 refuses this state instead of allowing it")
	}
	// Label-aware routing. Mounted only when an operator pointed at a provider
	// table, and mounting one that cannot be read is fatal: a gateway that
	// believes it routes by label while it is serving from an env default is
	// worse than one that says it has no policy.
	if path := strings.TrimSpace(cfg.RoutingFile); path != "" {
		router, rerr := routing.New(path, slog.Default())
		if rerr != nil {
			return nil, fmt.Errorf("execbridge: provider routing: %w", rerr)
		}
		b.router = router
		// The label half of the router. A routing table with no label source is a
		// gate that is only ever asked about the empty set -- a decision, not an
		// oversight, so it gets said out loud. The source is the OPERATOR's record
		// of the deployment, addressed by the tenant+deployment the transport
		// attested; internal/labelwire is the leg that keeps it that way. Failing to
		// even create the records is fatal for the same reason an unreadable table
		// is: a gateway that believes it routes by label while it has nowhere to
		// read labels from is lying to its operator.
		labelStore := model.NewDeploymentLabelStore(pool)
		if merr := labelStore.Migrate(ctx); merr != nil {
			return nil, fmt.Errorf("execbridge: deployment label records: %w", merr)
		}
		classified, cerr := labelStore.RecordCount(ctx)
		if cerr != nil {
			return nil, fmt.Errorf("execbridge: deployment label records: %w", cerr)
		}
		b.deploymentLabels = labelStore
		// Physical provider selection. A routing table that does not choose the
		// client is a table of intentions: each declared copy gets its own client
		// from its own secret_ref, and the copy this runtime was configured with
		// keeps the client the environment mounted. A copy that cannot be resolved
		// is refused at admission rather than served by the default client.
		resolver, build := cfg.SecretResolver, cfg.InferenceFactory
		if resolver == nil {
			vr, rerr := vaultSecretResolver()
			if rerr != nil {
				slog.Warn("execbridge: no secret store mounted - only the bound provider copy can be served",
					"consequence", "a route to any other declared copy is refused at admission",
					"reason", rerr.Error())
			}
			resolver = vr
		}
		if build == nil {
			build = productionInferenceFactory()
		}
		clients := newProviderClients(router, resolver, build, slog.Default())
		// The client this process was configured with serves ONE named copy. The
		// name comes from the operator (AHP_MODEL_PROVIDER), and where the table
		// declares exactly one copy there is nothing to name: that copy is it.
		// With several copies and no name, no copy is assumed -- assuming is the
		// defect this file exists to remove.
		bound := strings.TrimSpace(cfg.QuotaProvider)
		if bound == "" && len(router.Table().Providers) == 1 {
			bound = router.Table().Providers[0].Name
			slog.Info("execbridge: the single declared copy is the one the configured client serves",
				"provider", bound)
		}
		clients.register(bound, cfg.Inference)
		b.providerClients = clients
		b.svc.WithProviderClients(clients)
		slog.Info("execbridge: per-provider clients mounted",
			"declared_copies", len(router.Table().Providers),
			"bound_provider", bound,
			"secret_source", resolverKind(resolver),
			"unresolved_copy_means", "refusal at admission, never the default client")
		slog.Info("execbridge: routing label source mounted",
			"labels_source", "ahp_model.deployment_labels",
			"labels_writer", "operator-plane-only-no-http-face",
			"classified_deployments", classified,
			"unclassified_meaning", "empty label set, still checked against the destination")
	}
	b.executionClass = strings.TrimSpace(cfg.ExecutionClass)
	_, b.unconfigured = cfg.Inference.(UnconfiguredInference)
	return b, nil
}

// ProviderQuotas is the operator/verifier handle on the quota gate: limits, the
// current window and the admission journal. Nil when this face runs without
// quotas, which is itself the answer to "is anything bounded here".
func (b *Bridge) ProviderQuotas() *model.QuotaStore {
	if b == nil {
		return nil
	}
	return b.quotas
}

// MaxStepsFromEnv is the operator's per-attempt step budget, in one place. The
// gateway needs the same number to validate a caller-stated max_steps against
// the deployment ceiling; reading the env twice with two defaults is how "the
// gateway allowed 8 and the executor ran 4" happens. Callers that configure a
// bridge explicitly pass Config.MaxSteps and do not go through here.
func MaxStepsFromEnv() int {
	return positiveOr(envInt("AHP_EXEC_MAX_STEPS", defaultMaxSteps), defaultMaxSteps)
}

// MaxTokensFromEnv is the operator's completion cap for one generation, in one
// place. The bridge enforces it, and anything that ASKS the bridge -- the
// as-runner's Core runtime above all -- must state the same number, because
// internal/core fills in its own private default (4096) when a caller states
// none. Reading the bound twice with two defaults is how a deployment ends up
// refusing its own turns: the number that broke a live Dot turn was chosen by a
// default two packages away from the cap it violated (DEF-DOTE2E-9).
func MaxTokensFromEnv() int {
	return positiveOr(envInt("AHP_EXEC_MAX_TOKENS", defaultMaxTokens), defaultMaxTokens)
}

// FromEnv resolves the bridge from process environment only. The provider
// handle comes from the secret store (Vault), never from a key in the
// environment or the binary; when that runtime secret is absent the bridge is
// still mounted, backed by UnconfiguredInference so tasks drain to an honest
// FAILED instead of the plane silently disappearing.
func FromEnv(ctx context.Context, pool *pgxpool.Pool) (*Bridge, error) {
	return FromEnvWithArtifacts(ctx, pool, nil)
}

// FromEnvWithArtifacts is FromEnv with the turn-result export mounted. Without
// it a bridged turn is model-only and can never deliver the artifact this face's
// own Agent Card advertises -- which is exactly how a plane ends up answering
// "COMPLETED" with nothing to download while its budget is really spent. Passing
// nil keeps the old behaviour and must be logged by the caller as "this face
// cannot serve the artifact skill".
func FromEnvWithArtifacts(ctx context.Context, pool *pgxpool.Pool, exporter ArtifactExporter) (*Bridge, error) {
	return fromEnv(ctx, pool, exporter, nil)
}

// FromEnvWithArtifactsAndTools is FromEnvWithArtifacts with the plane's
// model-callable tool registry mounted as well. This is the wiring a face that
// advertises an artifact skill has to use: the definitions then travel to the
// provider on every step, the model's write_artifact call is dispatched by the
// runtime, and the artifact the model wrote is what the task carries. Passing
// nil tools is exactly FromEnvWithArtifacts -- a model-only turn whose artifact
// can only come from the plane's own export, and the caller must log that fact.
func FromEnvWithArtifactsAndTools(ctx context.Context, pool *pgxpool.Pool, exporter ArtifactExporter, tools *core.ToolRegistry) (*Bridge, error) {
	return fromEnv(ctx, pool, exporter, tools)
}

func fromEnv(ctx context.Context, pool *pgxpool.Pool, exporter ArtifactExporter, tools *core.ToolRegistry) (*Bridge, error) {
	cfg := Config{
		Schema:          envOr("AHP_MODEL_SCHEMA", DefaultModelSchema),
		ModelName:       envOr("AHP_MODEL_NAME", ""),
		MaxSteps:        MaxStepsFromEnv(),
		Artifacts:       exporter,
		Tools:           tools,
		TaskTokenLimit:  TaskTokenLimitFromEnv(),
		ChildTokenLimit: ChildTokenLimitFromEnv(),
		StepReserve:     int64(envInt("AHP_EXEC_STEP_RESERVE", defaultStepReserve)),
		MaxTokens:       MaxTokensFromEnv(),
		// Quota gate: named by an operator, bounded by per-tenant rows. An unset
		// provider name is not a zero quota -- it is no quota, which is what keeps
		// every existing acceptance path behaving as before.
		QuotaProvider:      envOr("AHP_MODEL_PROVIDER", ""),
		QuotaRequired:      envBool("AHP_MODEL_QUOTA_REQUIRED"),
		QuotaWindowSeconds: envInt("AHP_MODEL_QUOTA_WINDOW_S", model.DefaultQuotaWindowSeconds),
		// Routing is opt-in the same way the quota gate is: an unset file is not
		// "no routes", it is "this deployment never asked for label routing".
		RoutingFile:    envOr("AHP_MODEL_PROVIDERS_FILE", ""),
		ExecutionClass: envOr("AHP_EXECUTION_CLASS", ""),
	}
	secret, err := secretstore.QwenFromEnv(ctx)
	switch {
	case errors.Is(err, secretstore.ErrNotConfigured):
		cfg.Inference = UnconfiguredInference{}
		if cfg.ModelName == "" {
			cfg.ModelName = UnconfiguredInference{}.Model()
		}
	case err != nil:
		// A configured-but-unreadable secret is a real fault: fail closed rather
		// than quietly downgrading a production plane to no-provider.
		return nil, fmt.Errorf("execbridge: provider runtime secret: %w", err)
	default:
		client, cerr := model.NewQwenClient(ctx, secret)
		if cerr != nil {
			return nil, fmt.Errorf("execbridge: model gateway client: %w", cerr)
		}
		// Bounded transport retry, INSIDE one provider call: a TLS handshake
		// timeout on a saturated node is node health, not a model answer, and the
		// effect key/reservation of this generation must not be spent twice.
		cfg.Inference = newRetryingInference(client)
		if cfg.ModelName == "" {
			cfg.ModelName = client.Model()
		}
	}
	return New(ctx, pool, cfg)
}

// WithTaskLabels attaches the conversation label journal the routing ceiling is
// merged from. Returns the bridge for chaining; nil journal leaves today's
// behaviour (deployment record only) and the routed line says so.
func (b *Bridge) WithTaskLabels(j TaskLabelJournal) *Bridge {
	if b != nil {
		b.taskLabels = j
		// Say what this journal will actually do. With a provider table mounted it
		// is the second half of the routing ceiling; without one there is no
		// routing decision to inform, so it is deliberately not consulted -- an
		// unreadable journal must not brick turns that cannot be routed either way
		// (DEF-DOTE2E-2). Either way an operator can tell which state is in force.
		if b.router != nil {
			slog.Info("execbridge: conversation label journal mounted",
				"task_label_journal", j != nil, "routing", "mounted", "consulted", true,
				"meaning", "routing ceiling is the union of the deployment record and this conversation's labels")
		} else {
			slog.Info("execbridge: conversation label journal mounted, routing not configured",
				"task_label_journal", j != nil, "routing", "not_mounted", "consulted", false,
				"consequence", "no provider table: the turn uses this deployment's single client and the label journal is not consulted",
				"operator_option", "AHP_MODEL_PROVIDERS_FILE enables label-aware routing")
		}
	}
	return b
}

// ProviderConfigured reports whether a real provider handle backs the bridge.
// The role logs it so an operator can see "mounted, no provider" in plain words.
func (b *Bridge) ProviderConfigured() bool { return !b.unconfigured }

// ToolsMounted reports whether the bridge carries a model-callable tool
// registry. It is the fact the execution role logs as tool_wire: with it false
// a bridged turn is model-only and the only artifact this face can ever produce
// is its own exported answer, which is not what the Agent Card advertises.
func (b *Bridge) ToolsMounted() bool { return b.tools != nil }

// RoutingMounted reports whether label-aware routing is in force. The execution
// role logs it as label_routing so an operator can tell, from the banner alone,
// whether "routes with label awareness" is a claim or a fact (PDF :1620-1630).
func (b *Bridge) RoutingMounted() bool { return b != nil && b.router != nil }

// DeploymentLabels is the operator/verifier handle on the routing label records:
// classify a deployment with Set, read a classification with LabelsFor, count how
// many are classified with RecordCount. Nil when routing is not mounted, which is
// itself the answer to "is anything labelled here". There is no HTTP face on it by
// design -- the writer is the operator plane, and an open writer would put the
// caller's own assertion back into the gate through a different door.
func (b *Bridge) DeploymentLabels() *model.DeploymentLabelStore {
	if b == nil {
		return nil
	}
	return b.deploymentLabels
}

// RoutingDigest is the identity of the provider table currently in force, empty
// when routing is not mounted. It answers "which bytes is this process routing
// by" without opening the file from the outside.
func (b *Bridge) RoutingDigest() string {
	if b == nil || b.router == nil {
		return ""
	}
	return b.router.Digest()
}

// Model reports the model identifier stamped on every bridged generation.
func (b *Bridge) Model() string { return b.modelName }

// TaskTokenLimit reports the per-task root budget ceiling in tokens. Exposed so
// a role log line and an evidence harness can state the admission bound a task
// is held to without reading process configuration.
func (b *Bridge) TaskTokenLimit() int64 { return b.taskTokenLimit }

// TaskTokenLimitFromEnv is shared by model admission and runtime token issuance.
func TaskTokenLimitFromEnv() int64 {
	return positiveInt64Or(int64(envInt("AHP_EXEC_TASK_TOKEN_LIMIT", defaultTaskTokenLimit)), defaultTaskTokenLimit)
}

// ChildTokenLimitFromEnv names the operator allocation cap. Zero chooses half
// of each parent ceiling, leaving space for sibling tasks and parent work.
func ChildTokenLimitFromEnv() int64 { return int64(envInt("AHP_EXEC_CHILD_TOKEN_LIMIT", 0)) }

// TaskBudget returns the persistent root or inherited child budget for a task.
// Exposed for evidence harnesses: it proves the ceiling exists in Postgres
// without reaching into the bridge's internals.
func (b *Bridge) TaskBudget(ctx context.Context, scope contracts.Scope, taskID string) (model.Budget, error) {
	chain, err := b.taskBudgetChain(ctx, scope, taskID)
	if err != nil {
		return model.Budget{}, err
	}
	if err = b.reconcileBudgetScope(ctx, scope); err != nil {
		return model.Budget{}, err
	}
	return b.budgets.EnsureTaskTree(ctx, chain, RootBudgetKey, b.taskTokenLimit, b.childTokenLimit)
}

// Execute runs one fenced attempt: a real core.Runtime turn whose every model
// call is a Model Gateway generation billed against its admitted task budget.
func (b *Bridge) Execute(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
	if err := in.Scope.Validate(); err != nil {
		return gateway.ExecResult{State: a2a.TaskStateFailed}, err
	}
	taskID := string(in.Task.ID)
	if taskID == "" {
		return gateway.ExecResult{State: a2a.TaskStateFailed},
			contracts.NewError(contracts.ErrInvalid, "execbridge: task id required")
	}
	// Two different facts, two different messages. "The task carries no inbound
	// user message" is a gateway/history problem; "the message has no text" is a
	// client problem. Collapsing both into "no usable text input" sent operators
	// looking for a wire bug while the text sat in the task's own history.
	if in.Input == nil {
		return failedResult("execbridge: task carries no inbound user message"), nil
	}
	input := messageText(in.Input)
	if strings.TrimSpace(input) == "" {
		return failedResult("execbridge: inbound message has no text part"), nil
	}
	// The context binding is checked BEFORE any spend, not at export time: the
	// task's run must be able to bind the artifacts it produces, or they leave
	// the deletion cascade. Failing here costs zero tokens and names the real
	// cause; failing at export would bill a turn and then hide the result. It
	// stays a deterministic FAILED (not an error) like every other ingress
	// fact: the attempt never started, nothing ambiguous left this process.
	if strings.TrimSpace(in.Task.ContextID) == "" {
		return failedResult("execbridge: task carries no context id, artifacts could not be bound to the deletion cascade"), nil
	}

	// Admission before any spend: reuse the run budget or allocate a child. If
	// this fails the turn is never started, so an over-budget task cannot reach
	// a provider at all.
	root, err := b.executionBudget(ctx, in)
	if err != nil {
		return gateway.ExecResult{}, fmt.Errorf("execbridge: task budget: %w", err)
	}

	// The tool layer does not receive the attempt's identity from the core loop,
	// so it is attached here: a sealed artifact must be written under the scope and
	// task that own it. Without this a mounted tool would write under an empty
	// scope -- which is a leak, not a failure.
	labelContext := string(in.Task.ContextID)
	labelScope := in.Scope
	grantTask := ""
	if b.taskLabels != nil {
		labelContext, _ = in.Task.Metadata[gateway.MetaGrantContext].(string)
		grantTask, _ = in.Task.Metadata[gateway.MetaGrantTask].(string)
		labelScope.Deployment, _ = in.Task.Metadata[gateway.MetaGrantDeployment].(string)
		if labelContext == "" || labelScope.Deployment == "" || grantTask == "" {
			return failedResult("execbridge: missing verified grant binding for model routing"), nil
		}
	}
	attempt := executor.WithTurn(withTurnInfo(ctx, turnInfo{scope: in.Scope, taskID: taskID,
		contextID: labelContext, labelScope: labelScope, grantTask: grantTask}),
		executor.Turn{Scope: in.Scope, TaskID: taskID, ContextID: in.Task.ContextID})
	runtime := core.NewRuntime(core.RuntimeConfig{
		Model: &gatewayModel{
			svc:         b.svc,
			budgetID:    root.ID,
			modelName:   b.modelName,
			stepReserve: b.stepReserve,
			maxTokens:   b.maxTokens,
			router:      b.router,
			labels:      b.deploymentLabels,
			clients:     b.providerClients,
			taskLabels:  b.taskLabels,
			class:       b.executionClass,
		},
		Tools:     b.tools,
		MaxTokens: b.maxTokens,
	})
	// Which bound is in force for THIS attempt, decided out loud. The caller's
	// budget wins when it stated one (the gateway validated it against the
	// operator's ceiling at the ingress); the deployment number applies only when
	// nobody stated a budget. Previously the deployment number always won, so a
	// caller that asked for 8 steps silently ran on 4 -- and nothing downstream
	// could tell a budget that was never given from a budget that was exhausted.
	stepBudget := b.maxSteps
	if in.MaxSteps > 0 {
		stepBudget = in.MaxSteps
	}
	req := &space.CoreRequest{Scope: in.Scope, Input: jsonString(turnPrompt(input)), MaxSteps: stepBudget}

	results := make(chan space.StepResult, 64)
	var runErr error
	go func() {
		runErr = runtime.Run(attempt, req, results)
		close(results)
	}()

	var (
		finalText string
		sawError  bool
		errText   string
		modelTurn int
		toolSeals []executor.SealedArtifact
	)
	for sr := range results {
		switch sr.StepType {
		case space.StepModel:
			modelTurn++
			if txt := contentText(sr.Content); txt != "" {
				finalText = txt
			}
		case space.StepTool:
			// A tool step whose content decodes as a seal is an artifact the
			// MODEL wrote. It is remembered here so the turn can end with the
			// model's own file instead of being overwritten by the plane's
			// fallback export of the answer text.
			if seal, ok := executor.ParseSealedArtifact(sr.Content); ok {
				toolSeals = append(toolSeals, seal)
			}
		case space.StepError:
			sawError = true
			if errText == "" {
				errText = contentText(sr.Content)
			}
		}
	}

	if cerr := ctx.Err(); cerr != nil {
		// The lease/attempt was cut short locally; let the gateway decide.
		return gateway.ExecResult{}, cerr
	}

	if runErr != nil || sawError || strings.TrimSpace(finalText) == "" {
		reason := "execbridge: model turn failed"
		switch {
		case runErr != nil:
			reason += ": " + runErr.Error()
			// The contract reason code travels next to the human text. A refusal
			// that only says "it failed" cannot be scripted on, metered, or
			// matched against an operator log line, and that silence is what made
			// two separate legs hunt one routing refusal by hand. Codes only --
			// no prompt, no arguments, no provider payload.
			if rc := contracts.ReasonOf(runErr); rc != "" {
				reason += " [reason=" + rc + "]"
			}
		case errText != "":
			reason += ": " + errText
		case strings.TrimSpace(finalText) == "":
			reason += ": empty model output"
		}
		// A deterministic model failure is terminal, not ambiguous: nothing left
		// the process unnoticed, the reservation was settled by the gateway, so
		// the task drains to FAILED with the real reason instead of being parked.
		res := failedResult(truncate(reason, 512))
		res.Stats = gateway.ExecStats{
			StepsConsumed: modelTurn,
			StepBudget:    stepBudget,
			CloseReason:   closeReason(runErr, errText, sawError, finalText, modelTurn, stepBudget),
		}
		return res, nil
	}

	// One artifact per task, and the model's own file wins. When a tool call
	// already sealed an artifact this turn, that seal is what the task carries:
	// exporting the answer text on top of it would hand the consumer a second,
	// different "artifact" for the same task and bury the model's effect.
	// Otherwise the plane exports its own result -- this face advertises an
	// exportable result, so a model-only turn still has to leave a seal, and an
	// export failure is a FAILED attempt with a reason, never a silently
	// artifact-free completion: the seal is the proof the result exists outside
	// this process.
	var produced []*a2a.Artifact
	if len(toolSeals) > 0 {
		produced = executor.ArtifactsFromSeals(toolSeals)
	} else if b.artifacts != nil {
		seal, serr := b.artifacts.Seal(ctx, in.Scope,
			artifacts.Binding{ContextID: in.Task.ContextID, TaskID: taskID},
			resultArtifactName(taskID), "text/plain", finalText)
		if serr != nil {
			// The model answered; the export did not. Say both: the steps were
			// consumed, and the reason the attempt closed is the export, not the
			// model running out of room.
			res := failedResult(truncate("execbridge: result export failed: "+serr.Error(), 512))
			res.Stats = gateway.ExecStats{
				StepsConsumed: modelTurn, StepBudget: stepBudget,
				CloseReason: gateway.CloseReasonError,
			}
			return res, nil
		}
		produced = executor.ArtifactsFromSeals([]executor.SealedArtifact{seal})
	}

	return gateway.ExecResult{
		State:     a2a.TaskStateCompleted,
		Message:   a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(finalText)),
		Artifacts: produced,
		Stats: gateway.ExecStats{
			StepsConsumed: modelTurn,
			StepBudget:    stepBudget,
			CloseReason:   gateway.CloseReasonModelFinished,
		},
	}, nil
}

// closeReason names, out loud, why the execution loop stopped for this attempt.
// Three genuinely different facts used to look identical from the outside: the
// loop ran out of steps, the model decided it was finished, or something failed.
// That distinction is what an acceptance rule needs in order to call a finished
// run that never wrote the requested artifact a FAIL for the right reason:
// `model_finished` means the model stopped early, `step_limit` means the bound
// was too small for the work, `error` means the turn did not complete at all.
//
// The step budget is per ATTEMPT: core.Runtime counts steps from zero on every
// Run and a checkpoint restores the conversation, not the consumed budget, so a
// task resumed after a crash gets the budget again. StepsConsumed is therefore
// this attempt's model turns, and the per-attempt history is in the effect ledger.
func closeReason(runErr error, errText string, sawError bool, finalText string, consumed, budget int) string {
	return gateway.ClassifyCloseReason(runErr, errText, sawError, finalText, consumed, budget)
}

func failedResult(reason string) gateway.ExecResult {
	return gateway.ExecResult{
		State:   a2a.TaskStateFailed,
		Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(reason)),
	}
}

// taskBudgetTask namespaces the budget task so a bridge budget can never collide
// with a budget opened for some other task id by another plane.
func taskBudgetTask(taskID string) string { return "a2a-task:" + taskID }

// resultArtifactName names the exported result of one task. The name carries the
// task id because the artifact is offered on that task: a consumer that
// downloaded an object from task A must never be able to mistake it for task B's
// answer, and "the sha is different" is not a naming policy.
func resultArtifactName(taskID string) string {
	id := strings.TrimSpace(taskID)
	if len(id) > 24 {
		id = id[len(id)-24:]
	}
	return "a2a-task-" + id + "-result.txt"
}

// messageText concatenates the text parts of an inbound A2A message.
func messageText(m *a2a.Message) string {
	if m == nil {
		return ""
	}
	var b strings.Builder
	for _, p := range m.Parts {
		if t := p.Text(); t != "" {
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(t)
		}
	}
	return b.String()
}

// contentText decodes a core step payload that is a bare JSON string; anything
// else is rendered verbatim.
func contentText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

func jsonString(s string) json.RawMessage {
	b, err := json.Marshal(s)
	if err != nil {
		return json.RawMessage(`""`)
	}
	return b
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// errorText renders an optional stored error, never an empty reason.
func errorText(s, fallback string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return fallback
	}
	return s
}

func itoa(n int) string { return strconv.Itoa(n) }

func positiveOr(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

func positiveInt64Or(v, def int64) int64 {
	if v <= 0 {
		return def
	}
	return v
}

// envBool reads an opt-in operator flag: 1/true/yes/on, any case.
func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// turnPrompt frames the inbound A2A text as one execution-agent turn. It does
// not ask the model for files or tool actions: this boundary is a text inference
// (internal/model carries no tool wire), and the result artifact is sealed by the
// plane after the turn, so the model must not be led to claim a file it cannot
// write.
func turnPrompt(user string) string {
	return "You are the AHP execution agent handling one durable task. Answer the " +
		"request directly and concisely. Do not invent tool actions or files: this " +
		"turn is a model turn routed through the Model Gateway.\n\nUser: " + user
}
