package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	artifactssdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

// Environment configuration for the real runtime wiring. There is NO fallback
// to any other provider and NO stub executor: the model client is built only
// from the variables below and fails closed when they are absent; a tool is
// registered only when its real backing store/endpoint is configured.
//
// Model (ahp model role = native Qwen; the launcher maps the Qwen role into
// AHP_LLM_* so the binary never references a foreign provider):
//
//	AHP_LLM_BASE_URL   OpenAI-compatible base URL (required)
//	AHP_LLM_API_KEY    bearer token (optional; in-memory only, never logged)
//	AHP_LLM_MODEL      model id (required)
//
// Artifact tool — AHP's own Tool GW -> Artifacts path (optional; the tool is
// registered only when the whole group is present, never as a fake sink):
//
//	AS_ARTIFACTS_URL     HTTPS origin of the Artifacts owner
//	AS_WORKLOAD_CERT / AS_WORKLOAD_KEY / AS_WORKLOAD_CA  workload mTLS
//
// Emil delegation adapter — our adapter to a reference core agent over A2A
// (OPTIONAL). Emil's own container/name/port is a reference integration used to
// understand and adapt the runtime, NOT the product's readiness gate; when unset
// the runner simply omits the delegation tool.
//
//	EMIL_BASE_URL      Emil core-agent base URL (optional)
//	EMIL_BEARER        A2A bearer token (optional; in-memory only)
//	EMIL_MCP           comma-separated connector refs: id|authType|secretEnv
//	CARD_CACHE_TTL_S   Agent Card cache TTL in seconds (optional, default 60)
//
// EMIL_MCP entries never embed secret values: the third field names an
// environment variable that holds the secret, so the config string stays
// safe to log and secrets live only in adapter memory (redacted from
// provenance).

// emilRunAuthority resolves the authority of the run that is issuing a delegate
// call. internal/core injects the run's scope + task token and its conversation
// binding into the tool context (internal/core/artifact_tool.go WithRunScope,
// internal/core/run_binding.go WithRunBinding) - the same credential the run
// persists its own records with is what the child deployment gets.
//
// Returning nil (no run in this context) is not a failure: the adapter then keeps
// its configured bearer. A run that HAS a grant never silently falls back to the
// deployment credential, because this function only declines when there is no run.
func emilRunAuthority(ctx context.Context) *emil.RunAuthority {
	scope, token, ok := core.RunScopeFromContext(ctx)
	if !ok {
		return nil
	}
	ra := &emil.RunAuthority{Bearer: token, Scope: scope}
	if b, ok := core.RunBindingFromContext(ctx); ok {
		ra.TaskID, ra.ContextID = b.TaskID, b.ContextID
		ra.OperationKey = b.OperationKey
		// The step budget is forwarded only when the caller actually named one.
		// MaxStepsStated is what keeps "the caller asked for 64" apart from
		// "nobody named anything": the loop's own fallback ceiling is a fact about
		// this life of the loop, and forwarding it would make the child journal
		// step_budget_source=caller for a bound the caller never gave.
		if b.MaxStepsStated {
			ra.MaxSteps = b.MaxSteps
		}
		// The budget tree, when this deployment runs with one. MaxSteps alone only
		// announces the parent's number; the grant below is the parent's REMAINDER,
		// which is what the child is allowed to spend. A run with no stated bound has
		// no remainder of the caller's making and gets no tree.
		//
		// The run's own task token travels to ForRun: the scope this leg files under
		// must be the scope the run is actually acting under, which for a browser
		// turn only the credential knows (DEF-H10-9).
		if b.MaxStepsStated && runTreeBudget != nil {
			if budget, berr := runTreeBudget.ForRun(scope, token, b.TaskID, b.MaxSteps); berr == nil {
				ra.Budget = budget
			} else {
				ra.Budget = rejectedDelegationBudget{err: berr}
				slog.Warn("emil: run budget authority unavailable; delegation refused", "run_task", b.TaskID, "err", berr)
			}
		}
	}
	// DEF-H10-15: the parent a delegated child declares must be named in the SAME
	// identifier namespace the receiving plane attests it in. A run has two ids and
	// they are not interchangeable: the State run id (b.TaskID, what artifacts and
	// the budget tree are keyed by) and the identity grant task the run's own task
	// credential carries (rtask-<subject>:<key>, what ahp_state.runs.task_id stores).
	// internal/gateway/outbox.go declares the SECOND canonical for ahp_parent_task,
	// and internal/gateway/delegation.go refuses a declared parent the presenting
	// credential does not attest (parent_attestation_task_mismatch). Declaring the
	// run id therefore made every real browser-initiated A->B hop refuse: measured on
	// VM2 with run 20261101T224632.048158540Z_66gqbbwlik5jyczrfrel4jnksq (run id)
	// against caller rtask-s-rootuser:k-e317c1 (the credential's own task).
	//
	// The grant task is read from the credential this hop is about to present, AFTER
	// verifying it -- never from the request, never decoded unverified. Where this
	// process cannot attest (no published keys, no pinned workload, a credential for
	// another workload) the run id is declared as before AND the refusal is announced
	// in advance: silently keeping the old value would reproduce the mismatch with no
	// clue for the operator, which is the DEF-H10-10 failure mode this family has
	// already paid for once.
	if ra.TaskID != "" {
		parent := ra.TaskID
		if grant, gerr := runAuthorityScope.attestedTask(ra.Bearer); gerr == nil {
			parent = grant
		} else {
			slog.Warn("emil: declaring the delegated parent by run id because the run credential attests no attestable task; the receiving plane can only audit this parent if it holds it locally",
				"run_task", ra.TaskID, "err", gerr)
		}
		ra.Labels = map[string]any{emil.MetaParentTask: parent}
		ra.DeclaredParent = parent
	}
	// Scope is not a secret and this is the line that makes the propagation
	// auditable: the subject the child will authorize is the subject of THIS run.
	slog.Info("emil: child hop will present the run's own task token",
		"tenant", scope.Tenant, "deployment", scope.Deployment, "subject", scope.Subject,
		"run_task", ra.TaskID, "context", ra.ContextID,
		"declared_parent", ra.DeclaredParent,
		"step_budget", ra.MaxSteps, "step_budget_stated", ra.MaxSteps > 0)
	return ra
}

// buildEmilClient returns an mTLS *http.Client for the emil.delegate hop when a
// client-certificate bundle is configured, else nil (emil keeps its default
// client, i.e. plaintext/TLS-server-auth only). Bundle is all-or-nothing:
// EMIL_CLIENT_CERT + EMIL_CLIENT_KEY (+ optional EMIL_CLIENT_CA to pin the child
// server CA). This lets the runner invoke a child deployment that requires a
// workload client certificate, with the material supplied by the environment --
// never edited into the source for a stand.
func buildEmilClient() (*http.Client, error) {
	certPEM := os.Getenv("EMIL_CLIENT_CERT")
	keyPEM := os.Getenv("EMIL_CLIENT_KEY")
	if certPEM == "" && keyPEM == "" {
		return tracedClient(&http.Client{Timeout: 120 * time.Second}), nil
	}
	if certPEM == "" || keyPEM == "" {
		return nil, fmt.Errorf("EMIL_CLIENT_CERT and EMIL_CLIENT_KEY must be set together")
	}
	cert, err := tls.LoadX509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("client keypair: %w", err)
	}
	tcfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if ca := os.Getenv("EMIL_CLIENT_CA"); ca != "" {
		pem, rerr := os.ReadFile(ca)
		if rerr != nil {
			return nil, fmt.Errorf("client ca read: %w", rerr)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("client ca: no certs in %s", ca)
		}
		tcfg.RootCAs = pool
	}
	return tracedClient(&http.Client{Transport: &http.Transport{TLSClientConfig: tcfg}, Timeout: 120 * time.Second}), nil
}

// emilToolDef is the Core tool definition exposed to the model for Emil
// delegation. The description biases the model to call it for report/table/
// file-generation requests, which is the artifact-producing journey.
func emilToolDef() core.ToolDef {
	return core.ToolDef{
		Name: emil.ToolName,
		Description: "Delegate a request to the Emil core agent. It runs an " +
			"independent agent turn and returns generated file artifacts plus " +
			"a provenance record. Call this tool whenever the user asks for a " +
			"report, table, or generated file.",
		Parameters: []byte(`{"type":"object","properties":{"prompt":{"type":"string","description":"the request to delegate to Emil"}},"required":["prompt"]}`),
	}
}

// buildModel constructs the real Core model client from the environment. It
// never substitutes a stub and never falls back to another provider: when the
// endpoint or model is not configured it returns an error so the caller can
// fail closed.
func buildModel() (core.ModelClient, string, error) {
	if origin := os.Getenv("AS_MODEL_URL"); origin != "" {
		client, err := buildStateClient(loadConfig())
		if err != nil {
			return nil, "", err
		}
		name := os.Getenv("AS_MODEL_NAME")
		remote, err := execbridge.NewRemoteModel(origin, name, client)
		if err != nil {
			return nil, "", err
		}
		authority, err := buildRunScopeAttester()
		if err != nil {
			return nil, "", err
		}
		if !authority.enabled() {
			return nil, "", fmt.Errorf("AS_MODEL_URL requires published task verification keys and own workload certificate")
		}
		return scopedGatewayModel{model: remote, authority: authority}, "ahp-model-gateway", nil
	}
	if os.Getenv("AS_ALLOW_DIRECT_MODEL") != "1" {
		return nil, "", fmt.Errorf("AS_MODEL_URL is required; direct provider access requires explicit AS_ALLOW_DIRECT_MODEL=1 for development")
	}
	c, err := core.NewOpenAICompatFromEnv()
	if err != nil {
		return nil, "", err
	}
	return c, c.Model(), nil
}

// buildTools constructs the real tool registry from the environment. Every tool
// registered here performs real work: save_artifact writes to the real Data/
// Artifacts store, emil.delegate performs a live A2A call. No echo stub is ever
// registered. An empty registry (no tools configured) is a valid model-only
// deployment and is returned with a nil error — the model itself is the only
// mandatory dependency, enforced by buildModel.
func buildTools() (*core.ToolRegistry, []string, error) {
	client, err := buildArtifactClient()
	if err != nil {
		return nil, nil, err
	}
	return buildToolsWithArtifactClient(client)
}

// buildToolsWithArtifactClient shares the task-neutral API client with Core's
// automatic output externalization; individual calls supply their own run token.
func buildToolsWithArtifactClient(client *artifactssdk.Client) (*core.ToolRegistry, []string, error) {
	mx := core.NewMultiExecutor()
	reg := core.NewToolRegistry(mx)
	var names []string

	// AHP's own artifact tool (Tool GW -> Artifacts -> Seal). Registered only
	// when its authenticated owner API is configured.
	sink, ok, err := artifactSinkFromClient(client)
	if err != nil {
		return nil, nil, err
	}
	if ok {
		tool := core.NewSaveArtifactTool(sink)
		mx.Register(core.ToolSaveArtifact, tool)
		reg.Register(tool.Def())
		names = append(names, core.ToolSaveArtifact)
	}

	// Emil delegation adapter (our adapter). The target is the child deployment
	// named by EMIL_BASE_URL; emil resolves the RPC endpoint + protocol binding
	// from the child's Agent Card, so nothing here hardcodes a host or path. When
	// a client-certificate bundle is configured the adapter presents it, which is
	// how it authenticates to a child execution plane that requires workload mTLS;
	// the bearer (EMIL_BEARER) is the scoped task token the plane authorizes with.
	if base := os.Getenv("EMIL_BASE_URL"); base != "" {
		cfg := emil.Config{
			BaseURL:      base,
			Bearer:       os.Getenv("EMIL_BEARER"),
			MCPServers:   parseMCPServers(os.Getenv("EMIL_MCP")),
			CardCacheTTL: cardCacheTTL(),
			// The child hop acts as the RUN, not as this deployment: the run's own
			// task token (whose subject Identity derived from the verified human
			// subject) authenticates it, so nothing re-labels the request as a
			// different principal on the way out. EMIL_BEARER stays only as the
			// fallback for a delegate issued outside a run.
			RunAuthorityFor: emilRunAuthority,
		}
		hc, merr := buildEmilClient()
		if merr != nil {
			return nil, nil, fmt.Errorf("as-runner: emil mTLS client: %w", merr)
		}
		cfg.HTTPClient = hc
		adapter, err := emil.New(cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("as-runner: emil adapter: %w", err)
		}
		mx.Register(emil.ToolName, adapter)
		reg.Register(emilToolDef())
		names = append(names, emil.ToolName)
	}

	if len(names) == 0 {
		return nil, nil, nil
	}
	return reg, names, nil
}

// buildArtifactClient wires the owner API without any storage credentials.
func buildArtifactClient() (*artifactssdk.Client, error) {
	cfg := loadConfig()
	if cfg.ArtifactsURL == "" {
		return nil, nil
	}
	hc, err := space.NewMTLSClient(cfg.WorkloadCert, cfg.WorkloadKey, cfg.WorkloadCA)
	if err != nil {
		return nil, fmt.Errorf("as-runner: artifacts workload mTLS: %w", err)
	}
	return artifactssdk.New(artifactssdk.Config{BaseURL: cfg.ArtifactsURL, HTTPClient: hc,
		RequestDecorator: artifactTraceDecorator})
}

// buildArtifactSink enables the tool when its owner API origin is configured.
// A partial mTLS configuration fails startup instead of silently omitting the tool.
func buildArtifactSink() (core.ArtifactSink, bool, error) {
	client, err := buildArtifactClient()
	if err != nil {
		return nil, false, err
	}
	return artifactSinkFromClient(client)
}

func artifactSinkFromClient(client *artifactssdk.Client) (core.ArtifactSink, bool, error) {
	if client == nil {
		return nil, false, nil
	}
	att, err := buildRunScopeAttester()
	if err != nil {
		return nil, false, err
	}
	return &apiArtifactSink{client: client, verifier: att.verifier, taskAudience: att.taskAudience, workload: att.workload}, true, nil
}

// artifactScopeVerifier builds the verifier used to read a run's real scope out of its own task
// token. Keys/issuer are the SAME published human/task keys the edge is verified with (Space holds no
// signing key). Task audience defaults to the platform task audience "ahp"; override with
// AS_TASK_AUDIENCE. Returns (nil, "", nil) when human auth is not configured at all - the sink then
// behaves exactly as before and only accepts a scope the runtime already carried.
func artifactScopeVerifier() (*auth.Verifier, string, error) {
	issuer := os.Getenv("AS_AUTH_ISSUER")
	raw := os.Getenv("AS_AUTH_KEYS")
	if issuer == "" || raw == "" {
		return nil, "", nil
	}
	keys, err := parseAuthKeys(raw)
	if err != nil {
		return nil, "", fmt.Errorf("as-runner: artifact sink verifier: %w", err)
	}
	aud := os.Getenv("AS_TASK_AUDIENCE")
	if aud == "" {
		aud = "ahp"
	}
	return auth.NewVerifier(issuer, keys), aud, nil
}

type apiArtifactSink struct {
	client       *artifactssdk.Client
	verifier     *auth.Verifier
	taskAudience string
	workload     string
}

// expectedWorkloadID returns this process's workload identity - the SPIFFE URI
// spiffe://ahp/tenant/{tenant}/deployment/{deployment} carried by its OWN workload
// certificate. It is read from the certificate and never from a token or an env var,
// so a value the caller controls cannot rename the identity a token is checked against.
func expectedWorkloadID(certPath string) (string, error) {
	if certPath == "" {
		return "", fmt.Errorf("AS_WORKLOAD_CERT must name the workload certificate that pins the artifact scope")
	}
	raw, err := os.ReadFile(certPath)
	if err != nil {
		return "", fmt.Errorf("read workload certificate: %w", err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return "", fmt.Errorf("workload certificate is not PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("parse workload certificate: %w", err)
	}
	for _, uri := range cert.URIs {
		id := uri.String()
		if strings.HasPrefix(id, auth.WorkloadScheme+"/tenant/") &&
			strings.Contains(strings.TrimPrefix(id, auth.WorkloadScheme), "/deployment/") {
			return id, nil
		}
	}
	return "", fmt.Errorf("workload certificate carries no %s SPIFFE URI", auth.WorkloadScheme)
}

// verifiedScope completes a run's artifact scope from the run's OWN task token.
//
// Measured failure this closes: a browser turn carries the HUMAN principal's scope, which
// has no deployment (a human token carries no azp), so BeginUpload refused every artifact
// with "scope deployment must not be empty" and the model could not deliver a file. The
// fix must not become a bypass: the deployment is taken only from a token that (a) really
// verifies against the published Identity keys, (b) is a task token bound to a task/root/
// ctx/grant, and (c) names THIS process's workload - the SPIFFE identity of our own
// workload certificate. An unverifiable token, a foreign workload or a tenant mismatch
// fails closed instead of writing bytes into someone else's scope.
func (s *apiArtifactSink) verifiedScope(scope contracts.Scope, taskToken string) (contracts.Scope, error) {
	if scope.Deployment != "" || s.verifier == nil || taskToken == "" {
		return scope, nil
	}
	// One canonical attester (cmd/as-runner/run_scope.go): the artifact path and the
	// budget-accounting path complete a run's scope from the same verified credential,
	// through the same checks, so the two can never disagree about which run a record
	// belongs to. Where no verifier is configured the sink keeps filing under the
	// scope the runtime carried -- the pre-attestation behaviour, unchanged.
	out, err := s.attester().complete(scope, taskToken)
	if err != nil {
		return contracts.Scope{}, fmt.Errorf("save_artifact: %w", err)
	}
	return out, nil
}

// attester exposes this sink's credential checks as the shared run-scope attester,
// so there is exactly one implementation of "which scope does this run own".
func (s *apiArtifactSink) attester() runScopeAttester {
	return runScopeAttester{verifier: s.verifier, taskAudience: s.taskAudience, workload: s.workload}
}

// artifactIdemKey binds a logical artifact to its producing conversation and
// run. Identical bytes in another run may share blob storage, never the logical
// record whose labels and deletion lifecycle belong to the first conversation.
func artifactIdemKey(name, contentSHA string, binding core.RunBinding) string {
	data, _ := json.Marshal([]string{binding.ContextID, binding.TaskID, name, contentSHA})
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])[:32]
}

func (s *apiArtifactSink) SaveArtifact(ctx context.Context, name, mime string, data []byte) (core.ArtifactHandle, error) {
	scope, taskToken, ok := core.RunScopeFromContext(ctx)
	if !ok || scope.Tenant == "" {
		return core.ArtifactHandle{}, fmt.Errorf("save_artifact: no task scope in run context")
	}
	// Measured failure this closes: a browser turn carries the HUMAN principal's scope, which has no
	// deployment (a human token has no azp), so BeginUpload refused every artifact with
	// "scope deployment must not be empty" and the model could not deliver a file. The run's task
	// token is the authoritative scope (Identity set deployment from Space's own workload certificate),
	// so the missing field is taken from the VERIFIED token; a tenant mismatch or an unverifiable
	// token fails closed rather than writing into another scope.
	scope, err := s.verifiedScope(scope, taskToken)
	if err != nil {
		return core.ArtifactHandle{}, err
	}
	sum := sha256.Sum256(data)
	wantSHA := hex.EncodeToString(sum[:])
	// The artifact is bound to the conversation and run that produced it: that binding
	// is what lets State cascade-delete a chat's artifacts with no agent hook, and it
	// is the same fact the read gate asks about. A retry in the same run replays
	// one record; another run receives its own logical artifact.
	binding, bound := core.RunBindingFromContext(ctx)
	if !bound || binding.TaskID == "" || binding.ContextID == "" {
		return core.ArtifactHandle{}, fmt.Errorf("save_artifact: durable run and context binding are required")
	}
	if s.verifier != nil {
		claims, err := s.verifier.Verify(taskToken, s.taskAudience, auth.KindTask, s.workload)
		if err != nil || claims.Context != binding.ContextID {
			return core.ArtifactHandle{}, fmt.Errorf("save_artifact: verified context does not match durable run")
		}
	}
	client := s.client.WithTokenSource(func(context.Context) (string, error) { return taskToken, nil })
	ref, err := client.Put(ctx, artifactssdk.PutRequest{Key: artifactIdemKey(name, wantSHA, binding), Name: name, MIME: mime, Data: data})
	if err != nil {
		return core.ArtifactHandle{}, err
	}

	return core.ArtifactHandle{ID: ref.ID, Version: ref.Version, SHA256: ref.SHA256, Size: ref.Size}, nil
}

// firstEnv returns the first non-empty value among the named variables.
func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// parseMCPServers parses EMIL_MCP entries of the form id|authType|secretEnv
// into in-memory MCPServers. The secretEnv field names an environment variable
// holding the secret value; the config string itself never contains a secret.
// authType defaults to "none"; when authType is not "none" but the named secret
// variable is empty, the entry is kept with no secret (auth reported as present
// false in provenance).
func parseMCPServers(raw string) []emil.MCPServer {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []emil.MCPServer
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		fields := strings.Split(entry, "|")
		id := strings.TrimSpace(fields[0])
		if id == "" {
			continue
		}
		authType := "none"
		if len(fields) > 1 && strings.TrimSpace(fields[1]) != "" {
			authType = strings.TrimSpace(fields[1])
		}
		var secret string
		if len(fields) > 2 && strings.TrimSpace(fields[2]) != "" {
			secret = os.Getenv(strings.TrimSpace(fields[2]))
		}
		out = append(out, emil.MCPServer{ID: id, AuthType: authType, Secret: secret})
	}
	return out
}

// cardCacheTTL reads CARD_CACHE_TTL_S seconds, defaulting to 60.
func cardCacheTTL() time.Duration {
	if v := strings.TrimSpace(os.Getenv("CARD_CACHE_TTL_S")); v != "" {
		var secs int
		if _, err := fmt.Sscanf(v, "%d", &secs); err == nil && secs >= 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return 60 * time.Second
}

func logWiring(logger *slog.Logger, model string, tools []string) {
	logger.Info("as-runner: runtime wired",
		"model", model,
		"model_kind", "openai-compat",
		"tools", strings.Join(tools, ","),
		"executor", "composite",
	)
}

// buildStateClient returns an mTLS http.Client for the Space->State hop when a
// workload certificate bundle is configured (so State sees Space's workload
// identity), or nil to use the default client (e.g. an isolated test network). It
// never carries any browser credential.
// buildDownloadProxy wires the human-edge artifact download route (PDF rev3.2 рис. 7:
// "скачивание через прокси: текущие права, точная версия").
//
// Unconfigured (AS_ARTIFACTS_URL unset) returns nil and the path is simply not a route -
// that is the state of every deployment that has not opted in.
//
// Configured WITHOUT Space's own workload certificate is a startup REFUSAL, not a
// degraded route. A proxy that cannot present a workload identity has exactly two ways
// to answer a browser: fail every single call, or reuse the human JWT it was handed.
// The second one is the defect this route exists to prevent, so the deployment does not
// start. Rights are never weakened here: Artifacts still decides them at grant-issue
// time, from the exchanged task token and Space's verified certificate.
func buildDownloadProxy(logger *slog.Logger, cfg config) (http.Handler, error) {
	if cfg.ArtifactsURL == "" {
		return nil, nil
	}
	if cfg.WorkloadCert == "" || cfg.WorkloadKey == "" || cfg.WorkloadCA == "" {
		return nil, fmt.Errorf("AS_ARTIFACTS_URL is set but AS_WORKLOAD_CERT/KEY/CA are not: " +
			"the download proxy must issue grants as Space, never as the browser")
	}
	var ttl time.Duration
	if raw := cfg.ArtifactGrantTTL; raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("AS_ARTIFACT_GRANT_TTL %q is not a duration: %w", raw, err)
		}
		ttl = parsed
	}
	client, err := space.NewMTLSClient(cfg.WorkloadCert, cfg.WorkloadKey, cfg.WorkloadCA)
	if err != nil {
		return nil, err
	}
	client = tracedClient(client)
	handler, err := space.NewDownloadProxy(space.DownloadProxyConfig{
		ArtifactsURL: cfg.ArtifactsURL,
		Client:       client,
		GrantTTL:     ttl,
	})
	if err != nil {
		return nil, err
	}
	logger.Info("as-runner: human artifact download proxy enabled",
		"route", space.UIDownloadPath, "artifacts", cfg.ArtifactsURL, "grant_ttl", ttl.String())
	return handler, nil
}

// buildCostProxy wires the human cost read (PDF rev3.2 :347 "стоимость задачи").
//
// Unconfigured is the default and means the route does not exist: the accepted surfaces stay
// byte-identical. Configured WITHOUT Space workload identity is a startup REFUSAL, because
// the Model Gateway authorizes a scoped task token bound to a workload -- a process that
// cannot present one would have to fake the authority or forward a browser credential, and
// both are the defects this route exists to avoid.
//
// The task the cost belongs to is NOT taken from the browser: it is read back from the
// thread's own durable runs (State, scoped by the exchanged token), so a caller can ask
// about a thread it owns and cannot ask about anyone else's budget tree.
func buildCostProxy(logger *slog.Logger, cfg config, coord *space.Coordinator) (http.Handler, error) {
	if strings.TrimSpace(cfg.ModelCostURL) == "" {
		return nil, nil
	}
	if cfg.WorkloadCert == "" || cfg.WorkloadKey == "" || cfg.WorkloadCA == "" {
		return nil, fmt.Errorf("AS_MODEL_COST_URL is set but AS_WORKLOAD_CERT/KEY/CA are not: " +
			"the cost read must ask the Model Gateway as Space, never as the browser")
	}
	if coord == nil {
		return nil, fmt.Errorf("AS_MODEL_COST_URL is set but no coordinator is wired to name the thread's run task")
	}
	client, err := space.NewMTLSClient(cfg.WorkloadCert, cfg.WorkloadKey, cfg.WorkloadCA)
	if err != nil {
		return nil, err
	}
	client = tracedClient(client)
	tasks := func(ctx context.Context, taskToken, threadID string) (string, error) {
		runs, err := coord.ListRunHistory(ctx, taskToken, threadID)
		if err != nil {
			return "", err
		}
		// The Model HTTP API bills the authenticated grant task; the older direct
		// runner tree used the durable State run ID. Select the actual owner for
		// this configured model path, never a browser-provided task identifier.
		task := ""
		for _, r := range runs {
			if os.Getenv("AS_MODEL_URL") != "" && strings.TrimSpace(r.TaskID) != "" {
				task = r.TaskID
			} else if strings.TrimSpace(r.ID) != "" {
				task = r.ID
			} else if strings.TrimSpace(r.TaskID) != "" {
				task = r.TaskID
			}
		}
		return task, nil
	}
	handler, err := space.NewCostProxy(space.CostProxyConfig{
		ModelCostURL: cfg.ModelCostURL,
		Client:       client,
		Tasks:        tasks,
	})
	if err != nil {
		return nil, err
	}
	logger.Info("as-runner: human cost read enabled (read-only, task named from the thread's own runs)",
		"route", space.UICostPath, "model_cost", cfg.ModelCostURL)
	return handler, nil
}

func buildStateClient(cfg config) (*http.Client, error) {
	if cfg.WorkloadCert == "" || cfg.WorkloadKey == "" || cfg.WorkloadCA == "" {
		return nil, nil
	}
	client, err := space.NewMTLSClient(cfg.WorkloadCert, cfg.WorkloadKey, cfg.WorkloadCA)
	if err != nil {
		return nil, err
	}
	return tracedClient(client), nil
}

// buildExchanger builds the production human->scoped-task exchanger: an explicit
// trusted Space->Identity operation over Space's own workload mTLS. It is REQUIRED
// whenever human browser auth is enabled so the browser's human JWT (aud ahp-web)
// is exchanged server-side for a scope-bound Task token and never forwarded to
// State. It fails closed if the boundary is not fully configured.
func buildExchanger(logger *slog.Logger, cfg config) (space.TaskExchanger, error) {
	if cfg.IdentityExchangeURL == "" {
		return nil, fmt.Errorf("AS_IDENTITY_EXCHANGE_URL is required when human auth is enabled")
	}
	if cfg.WorkloadCert == "" || cfg.WorkloadKey == "" || cfg.WorkloadCA == "" {
		return nil, fmt.Errorf("AS_WORKLOAD_CERT/KEY/CA (Space workload identity) are required for the human->task exchange")
	}
	client, err := space.NewMTLSClient(cfg.WorkloadCert, cfg.WorkloadKey, cfg.WorkloadCA)
	if err != nil {
		return nil, err
	}
	endpoint := cfg.IdentityExchangeURL
	if !strings.Contains(endpoint, space.IdentityExchangePath) {
		endpoint = strings.TrimRight(endpoint, "/") + space.IdentityExchangePath
	}
	logger.Info("as-runner: identity task exchange configured", "endpoint", endpoint)
	workload, err := expectedWorkloadID(cfg.WorkloadCert)
	if err != nil {
		return nil, fmt.Errorf("identity task exchange requires the server's workload identity: %w", err)
	}
	return space.NewIdentityGrantExchanger(space.GrantExchangerConfig{
		IdentityExchangeURL: cfg.IdentityExchangeURL,
		Client:              client,
		AuthorityNamespace:  workload,
	})
}

// buildRunGrantIssuer builds the Tier-2 per-run Identity Delegate issuer over Space's
// workload mTLS. It is REQUIRED whenever human browser auth is enabled: a browser run must
// execute under its OWN delegated child grant, never the shared ingress authority token.
// The Identity base is AS_IDENTITY_URL, else derived by trimming the exchange path from
// AS_IDENTITY_EXCHANGE_URL (same service). Fails closed if the boundary is not configured.
func buildRunGrantIssuer(logger *slog.Logger, cfg config) (space.RunGrantIssuer, error) {
	base := cfg.IdentityURL
	if base == "" {
		if cfg.IdentityExchangeURL == "" {
			return nil, fmt.Errorf("AS_IDENTITY_URL (or AS_IDENTITY_EXCHANGE_URL) is required when human auth is enabled (Tier-2 run grant)")
		}
		base = strings.TrimSuffix(cfg.IdentityExchangeURL, space.IdentityExchangePath)
	}
	if cfg.WorkloadCert == "" || cfg.WorkloadKey == "" || cfg.WorkloadCA == "" {
		return nil, fmt.Errorf("AS_WORKLOAD_CERT/KEY/CA (Space workload identity) are required for Tier-2 run grants")
	}
	client, err := space.NewMTLSClient(cfg.WorkloadCert, cfg.WorkloadKey, cfg.WorkloadCA)
	if err != nil {
		return nil, err
	}
	issuer, err := space.NewIdentityDelegateIssuer(base, client)
	if err != nil {
		return nil, err
	}
	logger.Info("as-runner: Tier-2 run-grant delegate issuer configured", "identity_base", base)
	return issuer, nil
}

// parseRunGrantTTL parses AS_RUN_GRANT_TTL (default 15m) as a Go duration.
func parseRunGrantTTL(v string) time.Duration {
	if v == "" {
		return 15 * time.Minute
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	return 15 * time.Minute
}

// splitCSV splits a comma-separated capability list, trimming spaces and dropping empties.
func splitCSV(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// buildResolveProxy is the human-edge digest resolver (space.UIResolvePath), or nil when
// the deployment has no Artifacts surface — then the path is not a route at all.
//
// It is mounted under the same condition as the download proxy and with the same
// workload identity: a digest lookup is a read of somebody's artifact metadata, so the
// process answering it must be able to say who it is. Rights are Artifacts' decision on
// every call; this wiring only decides whether the route exists.
func buildResolveProxy(logger *slog.Logger, cfg config) (http.Handler, error) {
	if cfg.ArtifactsURL == "" {
		return nil, nil
	}
	if cfg.WorkloadCert == "" || cfg.WorkloadKey == "" || cfg.WorkloadCA == "" {
		return nil, fmt.Errorf("AS_ARTIFACTS_URL is set but AS_WORKLOAD_CERT/KEY/CA are not: " +
			"the digest resolver must ask Artifacts as Space, never as the browser")
	}
	client, err := space.NewMTLSClient(cfg.WorkloadCert, cfg.WorkloadKey, cfg.WorkloadCA)
	if err != nil {
		return nil, err
	}
	handler, err := space.NewResolveProxy(space.ResolveProxyConfig{
		ArtifactsURL: cfg.ArtifactsURL,
		Client:       client,
	})
	if err != nil {
		return nil, err
	}
	logger.Info("as-runner: human artifact digest resolver enabled (read-only, tenant from the verified session)",
		"route", space.UIResolvePath, "artifacts", cfg.ArtifactsURL)
	return handler, nil
}

// buildPlaneProxies builds the human plane surfaces the browser is allowed to operate:
// registry, evals, triggers and memory. Each is opt-in by its origin variable, so a
// deployment that sets none of them mounts none and answers exactly as before. When one IS
// set, Space's own workload certificate is mandatory: the machine face authorizes a scoped
// task token bound to this workload, so a process that cannot present one has nothing honest
// to ask with, and pretending otherwise would turn an operator mistake into a plane 403 on
// someone else's screen.
func buildPlaneProxies(logger *slog.Logger, cfg config) (map[string]http.Handler, error) {
	origins := map[string]string{}
	for _, spec := range space.PlanePipes() {
		var raw string
		switch spec.RoutePrefix {
		case space.UIRegistryPath, space.UIProfilePath:
			raw = cfg.RegistryURL
		case space.UIEvalsPath:
			raw = cfg.EvalsURL
		case space.UITriggersPath:
			raw = cfg.TriggersURL
		case space.UIMemoryPath:
			raw = cfg.MemoryURL
		case space.UIComputePath:
			raw = cfg.ComputeURL
		case space.UIWorkspacePath:
			raw = cfg.WorkspaceURL
		case space.UIArtifactsPath:
			raw = cfg.ArtifactsURL
		}
		if raw != "" {
			origins[spec.RoutePrefix] = raw
		}
	}
	if cfg.ToolsURL != "" {
		origins[space.UIToolsPath] = cfg.ToolsURL
	}
	if len(origins) == 0 {
		return nil, nil
	}
	if cfg.WorkloadCert == "" || cfg.WorkloadKey == "" || cfg.WorkloadCA == "" {
		return nil, fmt.Errorf("a human plane surface is configured but AS_WORKLOAD_CERT/KEY/CA are not: " +
			"the planes must be addressed as Space, never as the browser")
	}
	client, err := space.NewMTLSClient(cfg.WorkloadCert, cfg.WorkloadKey, cfg.WorkloadCA)
	if err != nil {
		return nil, err
	}
	handlers := make(map[string]http.Handler, len(origins))
	for _, spec := range space.PlanePipes() {
		origin, ok := origins[spec.RoutePrefix]
		if !ok {
			continue
		}
		callTimeout := 15 * time.Second
		if spec.RoutePrefix == space.UIEvalsPath {
			callTimeout = 130 * time.Second
		}
		handler, perr := space.NewPlaneProxy(space.PlaneProxyConfig{Spec: spec, Origin: origin, Client: client, CallTimeout: callTimeout})
		if perr != nil {
			return nil, perr
		}
		handlers[spec.RoutePrefix] = handler
		logger.Info("as-runner: human plane surface enabled (operations named in code, plane answers pass through)",
			"route", spec.RoutePrefix, "origin", origin, "operations", strings.Join(spec.AllowedOps, ","))
	}
	if cfg.ArtifactsURL != "" {
		h, err := space.NewUploadProxy(space.UploadProxyConfig{ArtifactsURL: cfg.ArtifactsURL, Client: client})
		if err != nil {
			return nil, err
		}
		handlers[space.UIUploadPath] = h
	}
	if cfg.ToolsURL != "" {
		h, err := space.NewHumanToolsProxy(cfg.ToolsURL, client)
		if err != nil {
			return nil, err
		}
		handlers[space.UIToolsPath] = h
	}
	return handlers, nil
}
