package emil

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ToolName is the Core runtime tool name this adapter serves.
const ToolName = "emil.delegate"

// MetaParentTask is the A2A SendMessage metadata key the AHP execution plane
// reads as "the task that delegated this one". The name is the execution plane's
// wire contract (internal/gateway/outbox.go MetaParentTask); it is repeated here
// because this package is the producer, and a delegated task that arrives without
// its parent link is a task nobody can trace back up the tree.
const MetaParentTask = "ahp_parent_task"

// MCPServer is a configured Model Context Protocol server reference. Secret
// values live only here (in memory) and are NEVER copied into Provenance.
type MCPServer struct {
	ID       string
	AuthType string // "bearer", "api-key", "none", ...
	Secret   string // in-memory only; redacted from provenance
}

// MCPServerRef is the redacted provenance view of an MCPServer.
type MCPServerRef struct {
	ID            string `json:"id"`
	AuthType      string `json:"auth_type"`
	SecretPresent bool   `json:"secret_present"`
}

// RunAuthority is the authority emil presents to a child deployment for ONE
// delegate call, resolved from the turn that is making the call.
//
// This exists because a delegation must not change who the request is for. If
// the adapter carried only a deployment-wide credential, the child deployment
// would record the task under a SERVICE subject and the original human subject
// would be lost at the hop - the "от первого ко второму как от другого человека"
// substitution the architecture forbids. A host that wires a resolver hands over
// the run's own task token, whose scope Identity derived from the verified human
// subject, so the child authorizes and journals the ORIGINAL subject.
type RunAuthority struct {
	// Bearer is the run's own task token. In memory only; never logged.
	Bearer string
	// Scope is the scope that token carries (tenant/deployment/subject). It is not
	// a secret and is what makes the propagation auditable in the logs.
	Scope contracts.Scope
	// TaskID and ContextID name the run and the conversation it runs in.
	TaskID    string
	ContextID string
	// OperationKey is the durable effect coordinate (run, node, call index).
	OperationKey string
	// DeclaredParent is the task id this hop declares as the child's parent in
	// ahp_parent_task. It is the run's identity GRANT task, not its State run id:
	// the receiving plane attests a declared parent against the credential that
	// presents itself (internal/gateway/delegation.go), and that credential attests
	// the grant task. Kept beside TaskID so one log line can show both ids and an
	// operator can tell a lineage claim from a run bookkeeping id.
	DeclaredParent string
	// Labels are forwarded as A2A SendMessage metadata: the tree labels and
	// budget the child is supposed to inherit.
	Labels map[string]any
	// MaxSteps is THIS RUN's own per-attempt step bound, forwarded to the child as
	// metadata.max_steps so a delegated attempt is bounded by the budget the run
	// was given instead of by whatever the child deployment happens to default to.
	// Zero means nobody stated one and the child's deployment default stands (the
	// child then journals step_budget_source=deployment). The host fills this from
	// the run's own binding -- internal/core/run_binding.go MaxSteps, guarded by
	// MaxStepsStated (vm2-state-runner a44dd6d) -- so an unstated budget stays
	// unstated instead of becoming the loop's fallback ceiling.
	MaxSteps int
	// Budget is the delegating run's budget tree, when its host maintains one.
	//
	// Forwarding MaxSteps bounds the child by the parent's NUMBER; it accounts for
	// nothing, and that difference is the defect this field closes: a run admitted
	// with 3 steps that spent all 3 and then delegated forwarded 3 again, so one
	// subject ran 5 model steps under a budget of 3. With Budget set the child's
	// bound is carved from this run's REMAINDER before the child is admitted, and
	// what the child spent is converged back into the tree afterwards. An exhausted
	// remainder refuses the delegation ("исчерпанный резерв — отказ, а не
	// перерасход"). nil keeps today's behavior exactly.
	Budget DelegationBudget
}

// Config configures an Adapter.
type Config struct {
	BaseURL string
	// HTTPClient, Bearer, and MCPServers are optional. Bearer/MCPServer
	// secrets are held in memory only and excluded from provenance.
	HTTPClient *http.Client `json:"-"`
	Bearer     string       `json:"-"`
	MCPServers []MCPServer  `json:"-"`
	// RunAuthorityFor resolves the calling run's authority (see RunAuthority).
	// Optional: when nil the adapter keeps presenting the static Bearer, which is
	// the correct choice only for a caller that has no per-run credential.
	RunAuthorityFor func(context.Context) *RunAuthority `json:"-"`
	// CardCacheTTL caches the Agent Card across delegates. Zero disables cache.
	CardCacheTTL time.Duration
}

// ArtifactDigest records a content digest for one artifact part.
type ArtifactDigest struct {
	ArtifactID string `json:"artifact_id"`
	Name       string `json:"name,omitempty"`
	PartIndex  int    `json:"part_index"`
	Kind       string `json:"kind"`
	SHA256     string `json:"sha256"`
	Bytes      int    `json:"bytes"`
}

// Result is the extracted outcome of a delegated Emil turn.
type Result struct {
	Text      string     `json:"text"`
	Artifacts []Artifact `json:"artifacts,omitempty"`
	TaskID    string     `json:"task_id"`
	ContextID string     `json:"context_id"`
	State     string     `json:"state"`
}

// Provenance is the auditable record of a delegation. It contains NO secret
// values: auth is described by type and presence only.
type Provenance struct {
	EmilURL         string           `json:"emil_url"`
	AgentName       string           `json:"agent_name"`
	AgentVersion    string           `json:"agent_version"`
	AgentCardSHA256 string           `json:"agent_card_sha256"`
	TaskID          string           `json:"task_id"`
	ContextID       string           `json:"context_id"`
	TaskState       string           `json:"task_state"`
	ArtifactDigests []ArtifactDigest `json:"artifact_digests"`
	// ToolCalls lists the Emil-side tools the agent actually invoked during
	// the delegated turn (A2A history function_calls, e.g. core_artifact_save),
	// in first-seen order. Lets callers audit a real tool-hop rather than only
	// a generated reply.
	ToolCalls   []string       `json:"tool_calls"`
	MCPServers  []MCPServerRef `json:"mcp_servers"`
	AuthType    string         `json:"auth_type"`
	AuthPresent bool           `json:"auth_present"`
	// RunScoped records WHICH credential the child actually saw: true when the
	// hop carried the run's own credential, false when it fell back to the
	// deployment-wide one. RunSubject is the subject the child therefore
	// authorized - the original human subject when the hop was run-scoped.
	// Neither is a secret; both exist so a reviewer can audit the identity
	// propagation from the turn transcript instead of trusting the wiring.
	RunScoped  bool      `json:"run_scoped"`
	RunSubject string    `json:"run_subject,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}

// Adapter performs third-agent delegation to an Emil core-agent.
type Adapter struct {
	client        *Client
	mcp           []MCPServer
	authType      string
	authPresent   bool
	runScopedAuth bool

	cardCache   *AgentCard
	cardRaw     []byte
	cardFetched time.Time
	cardTTL     time.Duration
}

// New creates an Adapter from a Config.
func New(cfg Config) (*Adapter, error) {
	// Fail closed on a missing required dependency (no target URL): this is a
	// configuration error, surfaced as a stable contracts code rather than a
	// later nil dereference deep in a delegate call.
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "emil: base URL is required")
	}
	opts := []Option{}
	if cfg.HTTPClient != nil {
		opts = append(opts, WithHTTPClient(cfg.HTTPClient))
	}
	if cfg.Bearer != "" {
		opts = append(opts, WithBearer(cfg.Bearer))
	}
	if cfg.RunAuthorityFor != nil {
		opts = append(opts, WithRunAuthority(cfg.RunAuthorityFor))
	}
	c, err := NewClient(cfg.BaseURL, opts...)
	if err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, err.Error())
	}
	authType := "none"
	if cfg.Bearer != "" {
		authType = "bearer"
	}
	if cfg.RunAuthorityFor != nil {
		// The credential is per-run, so its presence is only knowable per call;
		// the type names the mechanism honestly rather than claiming a static one.
		authType = "run-task-token"
	}
	return &Adapter{
		client:        c,
		mcp:           cfg.MCPServers,
		authType:      authType,
		authPresent:   cfg.Bearer != "" || cfg.RunAuthorityFor != nil,
		runScopedAuth: cfg.RunAuthorityFor != nil,
		cardTTL:       cfg.CardCacheTTL,
	}, nil
}

// resolveCard returns the cached Agent Card when fresh, else re-fetches.
func (a *Adapter) resolveCard(ctx context.Context) (*AgentCard, []byte, error) {
	if a == nil || a.client == nil {
		return nil, nil, contracts.NewError(contracts.ErrInvalid, "emil: adapter has no configured client")
	}
	if a.cardCache != nil && a.cardTTL > 0 && time.Since(a.cardFetched) < a.cardTTL {
		return a.cardCache, a.cardRaw, nil
	}
	card, raw, err := a.client.AgentCard(ctx)
	if err != nil {
		return nil, nil, err
	}
	a.cardCache, a.cardRaw, a.cardFetched = card, raw, time.Now()
	return card, raw, nil
}

// Delegate runs one Emil turn for prompt and returns the extracted result plus
// provenance. On a failed/rejected task it returns the Emil status text as the
// error while still returning provenance, so callers can audit failed runs.
func (a *Adapter) Delegate(ctx context.Context, prompt string) (*Result, *Provenance, error) {
	// A nil adapter/client is a misconfiguration, not a delegate failure: return
	// a stable contracts error instead of dereferencing nil and crashing the
	// whole composed process (the adapter runs inside the Space turn goroutine).
	if a == nil || a.client == nil {
		return nil, baseProvenanceFor(a, time.Now()), contracts.NewError(contracts.ErrInvalid, "emil: adapter has no configured client")
	}
	start := time.Now()
	// Resolved ONCE per delegate call: the same authority authenticates the card
	// read, the submission and every poll/cancel of this child.
	ra := a.client.authorityFor(ctx)
	card, cardRaw, err := a.resolveCard(ctx)
	if err != nil {
		return nil, stampRunAuth(baseProvenanceFor(a, start), ra), err
	}

	binding := a.client.ResolveBinding(card)
	task, grant, err := a.acquireChild(ctx, binding, prompt, ra)
	if grant != nil && ra != nil {
		// Converge before anything else: the child is finished, its share of the
		// tree is held no longer than it has to be, and the remainder goes back to
		// this run (or to the next delegation of it) now.
		a.convergeDelegation(ctx, ra.Budget, *grant, task)
	}
	if err != nil {
		return nil, stampRunAuth(baseProvenanceFor(a, start), ra), err
	}

	prov := &Provenance{
		EmilURL:         a.client.baseURL,
		AgentName:       card.Name,
		AgentVersion:    card.Version,
		AgentCardSHA256: hexSHA256(cardRaw),
		TaskID:          task.ID,
		ContextID:       task.ContextID,
		TaskState:       task.Status.State,
		ArtifactDigests: digestArtifacts(task.Artifacts),
		ToolCalls:       extractToolCalls(task.History),
		MCPServers:      redactMCP(a.mcp),
		AuthType:        a.authType,
		AuthPresent:     a.authPresent,
		StartedAt:       start,
		FinishedAt:      time.Now(),
	}
	if ra != nil {
		// Which credential the child actually saw, and whose subject it carries:
		// the audit line for identity propagation across the delegation hop.
		prov.RunScoped = ra.Bearer != ""
		prov.RunSubject = ra.Scope.Subject
	}

	res := &Result{
		Artifacts: task.Artifacts,
		TaskID:    task.ID,
		ContextID: task.ContextID,
		State:     task.Status.State,
		Text:      extractText(task),
	}

	switch NormalizeState(task.Status.State) {
	case StateFailed, StateRejected, StateCanceled:
		msg := task.Status.Message.Text()
		if msg == "" {
			msg = "task " + task.Status.State
		}
		return res, prov, fmt.Errorf("emil task %s: %s", task.Status.State, msg)
	default:
		return res, prov, nil
	}
}

// delegationMetadata composes the A2A SendMessage metadata for one delegated
// call: the run's tree labels plus the run's own step budget.
//
// The budget is the half that used to be missing. A parent run admitted with N
// steps used to delegate with no number at all, so the child ran on its own
// deployment default -- which is not necessarily smaller (measured on the live
// stand: the child's canonical executor defaults to 16 while the gateway's
// caller ceiling for the same deploy is 4). Forwarding the number is what makes
// step_budget_source=caller reachable across the hop at all; the child still
// answers with a refusal rather than a clamp if the number exceeds its ceiling.
//
// Labels are copied, never mutated: the host may hand the same map to several
// delegations of one run.
func delegationMetadata(ra *RunAuthority) map[string]any {
	metadata := make(map[string]any, len(ra.Labels)+1)
	for k, v := range ra.Labels {
		metadata[k] = v
	}
	steps := ra.MaxSteps
	if steps > 0 {
		metadata[MetaStepBudget] = steps
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

// acquireChild runs one delegated child turn to a terminal state, honoring the
// binding the Agent Card advertised. It also returns the budget grant taken for the
// child (nil when the run has no budget tree, or when the child deployment speaks
// the legacy protocol and cannot be handed a bound at all), so the caller can
// converge the tree with what the child did -- including on the error paths, because
// a child that was reached and then failed still spent something.
//
// Canonical (a2a-go v2.7.0): the child is submitted non-blocking and then polled.
// Polling runs in THIS goroutine under the caller's context, so when the caller
// cancels -- the Space run coordinator cancelling the live turn, which is how a
// parent-subtree cancel must reach a child -- the adapter issues a real
// CancelTask to the child before returning. That is what makes "cancel the
// subtree cancels the child" true for a live model turn, not only for a CLI.
//
// Legacy (Emil Python 0.3): message/send blocks server-side to a terminal task;
// behavior is unchanged.
func (a *Adapter) acquireChild(ctx context.Context, b Binding, prompt string, ra *RunAuthority) (*Task, *DelegationGrant, error) {
	if !b.Canonical {
		if ra != nil && ra.Budget != nil {
			return nil, nil, contracts.NewError(contracts.ErrForbidden, "emil: legacy peer cannot carry a delegated budget bound")
		}
		// The legacy protocol carries no message metadata, so there is no way to
		// hand this child a bound. Taking a reserve that cannot be communicated
		// would be accounting theatre, so this path touches no tree.
		task, _, err := a.client.Send(ctx, SendRequest{Text: prompt})
		return task, nil, err
	}
	ref, refErr := delegationRef(ra)
	if refErr != nil {
		return nil, nil, refErr
	}
	var contextID string
	var metadata map[string]any
	var grantOut *DelegationGrant
	if ra != nil {
		contextID = ra.ContextID
		metadata = delegationMetadata(ra)
		if ra.Budget != nil {
			grant, rerr := ra.Budget.Reserve(ctx, ref, ra.MaxSteps)
			if rerr != nil {
				// A refusal, never a quieter number: an exhausted remainder means
				// this run may not delegate at all, and the model has to hear that.
				return nil, nil, fmt.Errorf("emil: delegation refused by the run budget tree: %w", rerr)
			}
			if metadata == nil {
				metadata = map[string]any{}
			}
			metadata[MetaStepBudget] = grant.Granted
			metadata[MetaDelegationRef] = grant.Ref
			grantOut = &grant
			slog.Info("emil: delegated bound carved from the run's budget tree",
				"delegation", grant.Ref, "run_task", ra.TaskID,
				"run_bound", ra.MaxSteps, "child_bound", grant.Granted)
		}
	}
	sub, _, err := a.client.SendV2WithID(ctx, b.RPCURL, prompt, contextID, ref, metadata)
	// The submission is the moment a task leaves this deployment, so its identity
	// is journalled here: which child task was created, in which conversation, and
	// under which parent link. Without it a delegated task can only be reconstructed
	// after the fact from the child's own store.
	if sub != nil {
		slog.Info("emil: delegated child task",
			"task_id", sub.ID, "state", sub.Status.State, "rpc_url", b.RPCURL,
			"context_id", sub.ContextID, "forwarded", metadata)
	}
	if err != nil {
		// AHP deduplicates SendMessage by the stable message ID and request digest.
		// Re-submit exactly that request to recover the task handle after a lost ACK.
		reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		sub, _, err = a.client.SendV2WithID(reconcileCtx, b.RPCURL, prompt, contextID, ref, metadata)
		cancel()
		if err != nil {
			return nil, grantOut, contracts.NewReasoned(contracts.ErrConflict, "SIDE_EFFECT_UNKNOWN",
				"emil: delegation acknowledgement unknown; reconcile operation "+ref+" with the same request")
		}
	}

	// cancelChild issues a real CancelTask under a DETACHED context so a live-turn
	// subtree cancel (the coordinator cancelling this turn's context) always drives
	// the child to its own terminal canceled state, even when the cancellation races
	// an in-flight poll RPC instead of landing between poll ticks.
	cancelChild := func() *Task {
		slog.Info("emil: live-turn subtree cancel — cancelling delegated child task",
			"task_id", sub.ID, "rpc_url", b.RPCURL)
		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		canceled, _, cerr := a.client.CancelV2(cancelCtx, b.RPCURL, sub.ID)
		if cerr != nil {
			slog.Warn("emil: child CancelTask failed", "task_id", sub.ID, "err", cerr.Error())
			return nil
		}
		return canceled
	}
	if IsTerminalState(sub.Status.State) {
		return sub, grantOut, nil
	}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Parent-subtree cancellation observed on the live turn: cancel the
			// child so it reaches its own terminal canceled state, then return.
			if c := cancelChild(); c != nil {
				return c, grantOut, ctx.Err()
			}
			return sub, grantOut, ctx.Err()
		case <-ticker.C:
			t, _, gerr := a.client.GetTaskV2(ctx, b.RPCURL, sub.ID)
			if gerr != nil {
				// Cancellation observed mid-RPC: still cancel the child before returning.
				if ctx.Err() != nil {
					if c := cancelChild(); c != nil {
						return c, grantOut, ctx.Err()
					}
					return sub, grantOut, ctx.Err()
				}
				return nil, grantOut, gerr
			}
			if IsTerminalState(t.Status.State) {
				return t, grantOut, nil
			}
		}
	}
}

// baseProvenanceFor builds the auditable, secret-free provenance skeleton returned
// alongside an early failure (no card / no task yet obtained). It is never nil,
// so the tool-call path below can marshal it without a nil dereference.
func baseProvenanceFor(a *Adapter, start time.Time) *Provenance {
	prov := &Provenance{StartedAt: start, FinishedAt: time.Now()}
	if a == nil {
		return prov
	}
	prov.AuthType = a.authType
	prov.AuthPresent = a.authPresent
	prov.MCPServers = redactMCP(a.mcp)
	if a.client != nil {
		prov.EmilURL = a.client.baseURL
	}
	prov.RunScoped = a.runScopedAuth
	return prov
}

// stampRunAuth records on an early-failure provenance which credential the hop
// was going to present. A nil authority means the run had none, which is exactly
// what a reviewer needs when the child rejected the call.
func stampRunAuth(p *Provenance, ra *RunAuthority) *Provenance {
	if p == nil {
		return nil
	}
	if ra != nil {
		p.RunScoped = ra.Bearer != ""
		p.RunSubject = ra.Scope.Subject
	}
	return p
}

// Execute satisfies core.ToolExecutor: register the Adapter as the
// "emil.delegate" tool. Args: {"prompt": "..."} (or {"text": "..."}). The
// returned payload carries the result text, artifact digests, and provenance.
//
// It is structurally compatible with internal/core.ToolExecutor without
// importing that package, avoiding an import cycle.
func (a *Adapter) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	// A tool executor must never panic the host runtime: a nil adapter or a nil
	// client is a misconfiguration and is returned as a contracts error. This is
	// the fail-closed seam the composed plane relies on -- one badly configured
	// adapter must degrade to a tool error, never an all-plane outage.
	if a == nil || a.client == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "emil: adapter has no configured client")
	}
	if name != ToolName {
		return nil, fmt.Errorf("emil: unknown tool %q", name)
	}
	var in struct {
		Prompt string `json:"prompt"`
		Text   string `json:"text"`
	}
	if len(args) > 0 {
		if err := json.Unmarshal(args, &in); err != nil {
			return nil, fmt.Errorf("emil: bad args: %w", err)
		}
	}
	prompt := in.Prompt
	if prompt == "" {
		prompt = in.Text
	}
	if prompt == "" {
		return nil, fmt.Errorf("emil: empty prompt")
	}
	res, prov, err := a.Delegate(ctx, prompt)
	payload := map[string]any{"provenance": prov}
	if prov != nil {
		payload["task_state"] = prov.TaskState
		payload["task_id"] = prov.TaskID
	}
	if res != nil && prov != nil {
		payload["text"] = res.Text
		payload["artifacts"] = prov.ArtifactDigests
	}
	if err != nil {
		// Surface the failure to the model as a structured tool error while
		// keeping provenance for auditing.
		payload["error"] = err.Error()
		out, _ := json.Marshal(payload)
		return out, err
	}
	out, mErr := json.Marshal(payload)
	if mErr != nil {
		return nil, mErr
	}
	return out, nil
}

// extractText prefers inline artifact answers. Sealed-file receipts are a
// fallback only: they must not replace the agent's actual answer message.
func extractText(task *Task) string {
	var receipt string
	for _, art := range task.Artifacts {
		var seal struct {
			SHA256 string `json:"sha256"`
			Size   *int64 `json:"size"`
		}
		isReference := json.Unmarshal(art.Metadata, &seal) == nil && art.ArtifactID != "" && seal.Size != nil && *seal.Size >= 0 && len(seal.SHA256) == 64
		if isReference {
			_, err := hex.DecodeString(seal.SHA256)
			isReference = err == nil
		}
		for _, p := range art.Parts {
			if (p.Kind == "" || p.Kind == "text") && p.Text != "" {
				// AHP's sealed file carries a human-readable receipt in Parts;
				// the actual answer remains in the task's agent message. Preserve
				// legacy inline artifact precedence, but never substitute that
				// receipt for a real answer. Metadata remains untrusted input;
				// downloading a file still requires the Artifacts owner checks.
				if isReference {
					if receipt == "" {
						receipt = p.Text
					}
					continue
				}
				return p.Text
			}
		}
	}
	if task.Status.Message != nil {
		if answer := task.Status.Message.Text(); answer != "" {
			return answer
		}
	}
	return receipt
}

func digestArtifacts(arts []Artifact) []ArtifactDigest {
	var out []ArtifactDigest
	for _, art := range arts {
		for i, p := range art.Parts {
			b := p.ContentBytes()
			out = append(out, ArtifactDigest{
				ArtifactID: art.ArtifactID,
				Name:       art.Name,
				PartIndex:  i,
				Kind:       firstNonEmpty(p.Kind, "text"),
				SHA256:     hexSHA256(b),
				Bytes:      len(b),
			})
		}
	}
	return out
}

// redactMCP drops secret values, keeping only id/auth_type/secret_present.
func redactMCP(servers []MCPServer) []MCPServerRef {
	var out []MCPServerRef
	for _, s := range servers {
		out = append(out, MCPServerRef{
			ID:            s.ID,
			AuthType:      s.AuthType,
			SecretPresent: s.Secret != "",
		})
	}
	return out
}

func hexSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
