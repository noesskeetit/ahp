package execbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/model/routing"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// turnInfo is the per-attempt identity a bridged model call must be billed to.
// core.ModelClient has no scope parameter, so (like internal/executor) the
// bridge threads it through context instead of storing it on the model.
type turnInfo struct {
	scope  contracts.Scope
	taskID string
	// contextID is the conversation this turn belongs to -- the key of the label
	// journal the routing ceiling is merged from.
	grantTask  string
	contextID  string
	labelScope contracts.Scope
}

type turnInfoKey struct{}

func withTurnInfo(ctx context.Context, t turnInfo) context.Context {
	return context.WithValue(ctx, turnInfoKey{}, t)
}

func turnInfoFrom(ctx context.Context) turnInfo {
	if v, ok := ctx.Value(turnInfoKey{}).(turnInfo); ok {
		return v
	}
	return turnInfo{}
}

// gatewayModel is the seam that makes every core.Runtime model call a durable
// Model Gateway generation. It holds no provider credential: the only thing it
// can do is ask internal/model to admit, journal, run and settle a generation.
type gatewayModel struct {
	svc         *model.Service
	budgetID    string
	modelName   string
	stepReserve int64
	maxTokens   int
	// router is nil when the deployment did not configure a provider table, in
	// which case routing is not performed at all and modelName is used as given.
	router *routing.Router
	// labels is the routing label source -- the operator's record of the
	// deployment (model.DeploymentLabelStore). Nil only when the router is nil.
	labels *model.DeploymentLabelStore
	// clients picks the physical client of the copy the router chose (nil when no
	// table is mounted). It is consulted BEFORE Submit: a copy that cannot be
	// served from here must not leave a budget hold or a quota booking behind.
	clients *providerClients
	// taskLabels is the current label journal of the conversation (Identity's).
	// The deployment record alone is a ceiling on the WORKER, not on the data the
	// task currently carries (ARCH-02: "ceiling из журнала меток задачи"), so the
	// two are merged and the union is what the destination is checked against.
	taskLabels TaskLabelJournal
	class      string
}

var _ core.ModelClient = (*gatewayModel)(nil)

// Generate implements core.ModelClient through the Model Gateway: budget
// admission first (Submit reserves before any spend), then the ledger-fenced
// execution, then a response built from the persisted generation only.
func (g *gatewayModel) Generate(ctx context.Context, req *core.ModelRequest) (*core.ModelResponse, error) {
	t := turnInfoFrom(ctx)
	if err := t.scope.Validate(); err != nil {
		return nil, errors.New("execbridge: model call outside an authenticated task turn")
	}
	if t.taskID == "" {
		return nil, errors.New("execbridge: model call without a task id")
	}
	if g.budgetID == "" {
		return nil, errors.New("execbridge: model call without a task budget")
	}

	// Routing happens BEFORE any reservation: a request this deployment may not
	// route must not open a budget hold, leave a generation row, or reach a
	// provider. A refusal is returned as it came -- the label gate and the
	// no-route refusal both name their reason, and nothing here converts them
	// into a retry against a different provider.
	modelName, provider, err := g.route(ctx, t, req)
	if err != nil {
		return nil, err
	}

	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = g.maxTokens
	}
	// The same bound the HTTP face enforces. The bridge used to substitute its
	// own number only when the caller stated none, so a stated over-cap value
	// walked straight through to the provider while the identical number on the
	// other leg was refused -- one deployment, two rules (DEF-DOTE2E-9). It is
	// refused, never clamped: quietly answering with fewer tokens than the caller
	// asked for is a silent behavior change in a confidentiality-sensitive plane.
	if g.maxTokens > 0 && maxTokens > g.maxTokens {
		return nil, g.refuse(ctx, "completion_cap", ReasonCompletionCapExceeded,
			contracts.ErrInvalid, t, req,
			fmt.Sprintf("step asked for %d completion tokens, this deployment caps %d", maxTokens, g.maxTokens))
	}
	request := model.ChatRequest{Model: modelName, Messages: gatewayMessages(req.Messages), Tools: gatewayTools(req.Tools), MaxTokens: maxTokens}
	reserve, err := tokenReserve(request, g.stepReserve)
	if err != nil {
		return nil, err
	}
	key := stepKey(t.taskID, req.Step, req.Attempt)
	gen, err := g.svc.Submit(ctx, model.SubmitArgs{
		Scope:         t.scope,
		Task:          t.taskID,
		Key:           key,
		Model:         modelName,
		Request:       request,
		BudgetID:      g.budgetID,
		ReserveKey:    key + ":reserve",
		ReserveAmount: reserve,
		// The quota gate charges the copy that was ROUTED to, not whatever the
		// environment happened to name: two copies of one upstream with different
		// limits are two windows, and charging the default for a turn the router
		// sent elsewhere would leave the restricted copy unlimited (PDF :323).
		Provider: provider,
	})
	if err != nil {
		return nil, fmt.Errorf("execbridge: gateway submit: %w", err)
	}
	gen, err = g.svc.Execute(ctx, t.scope, gen.ID)
	if err != nil {
		return nil, fmt.Errorf("execbridge: gateway execute: %w", err)
	}

	switch gen.Status {
	case model.GenCompleted:
		content, calls, err := decodeGenerationOutput(gen.Output)
		if err != nil {
			return nil, fmt.Errorf("execbridge: decode generation output: %w", err)
		}
		return &core.ModelResponse{
			Content:   content,
			ToolCalls: calls,
			Usage:     &core.Usage{Input: gen.Usage.PromptTokens, Output: gen.Usage.CompletionTokens},
		}, nil
	case model.GenCanceled:
		return nil, fmt.Errorf("execbridge: generation %s canceled", gen.ID)
	default:
		reason := errorText(gen.Error, "no provider error recorded")
		return nil, fmt.Errorf("execbridge: generation %s ended %s: %s", gen.ID, gen.Status, reason)
	}
}

// route decides which provider, model and provider-copy serve this step, and
// returns the copy's name so the quota gate can charge the window it actually
// spent. Without a configured table it is a no-op that reports the bridge's own
// model and no provider, so an unconfigured deployment is untouched.
//
// THE LABEL SOURCE IS THE SERVER, NOT THE CALLER (director's ruling, 2026-11-02).
// The label set is read from the operator's record of the DEPLOYMENT this turn
// runs in, addressed by the tenant and deployment the transport attested -- the
// verified certificate, never a body field. An earlier draft asked for labels on
// core.ModelRequest / gateway.ExecInput / transport.Principal instead; that was
// withdrawn, because the party asking to be routed to a less restricted upstream
// would be the party stating the labels. internal/labelwire keeps that ruling as a
// leg. A deployment with no record is the EMPTY set, which is still checked against
// the destination -- an absent classification never means "unrestricted".
// dialHash names, content-free, which socket a copy resolves to.
func (g *gatewayModel) dialHash(provider string) string {
	if g.clients == nil {
		return ""
	}
	return shortHash(g.clients.EndpointOf(provider))
}

// refuse is the single place a routing decision says "no" and, in the same
// motion, tells the operator why. A refusal that reaches the transport as an
// unclassified error becomes a bare 500 with no reason and no journal line --
// the shape that hid a bricked default mount and made the J4/Dot blockers
// unmeasurable (DEF-H25-1, DEF-DOTE2E-2). The journal line carries the check,
// the stable reason, the contract code and the identifiers an operator needs to
// find the row; the request's content never appears (PDF rev3.2 :347/:277).
// The client gets the code plus the stable reason and nothing more: naming the
// cause is not the same as publishing the internals that produced it.
func (g *gatewayModel) refuse(ctx context.Context, check, reason, code string, t turnInfo, req *core.ModelRequest, detail string) error {
	return g.refuseNamed(ctx, check, reason, code, t, req, detail, false)
}

// refuseWithDetail additionally surfaces the detail in the client message.
// Used only where the detail is itself an operator-level configuration name
// (which provider copy, which secret reference) and was already the text this
// face produced before it gained a contract code. It never carries a credential
// value or request content.
func (g *gatewayModel) refuseWithDetail(ctx context.Context, check, reason, code string, t turnInfo, req *core.ModelRequest, detail string) error {
	return g.refuseNamed(ctx, check, reason, code, t, req, detail, true)
}

func (g *gatewayModel) refuseNamed(ctx context.Context, check, reason, code string, t turnInfo, req *core.ModelRequest, detail string, toClient bool) error {
	var step, attempt int
	if req != nil {
		step, attempt = req.Step, req.Attempt
	}
	// A refusal is an auditable event, so it is recorded where an auditor looks
	// first: the operation's own span. The middleware only marks 5xx with the
	// generic HTTP text, which is how "Internal Server Error" became the entire
	// audit trail of a refusal. Identifiers and the stable reason only
	// (PDF rev3.2 :347/:277).
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		span.SetAttributes(
			attribute.String("ahp.refusal.check", check),
			attribute.String("ahp.refusal.reason", reason),
			attribute.String("ahp.refusal.code", code),
			attribute.String("ahp.tenant", t.scope.Tenant),
			attribute.String("ahp.deployment", t.scope.Deployment),
			attribute.String("ahp.task", t.taskID),
			attribute.Int("ahp.step", step),
			attribute.Int("ahp.attempt", attempt),
		)
		span.SetStatus(codes.Error, reason)
	}
	slog.Error("execbridge: generation refused before admission",
		"check", check, "reason", reason, "code", code,
		"tenant", t.scope.Tenant, "deployment", t.scope.Deployment, "subject", t.scope.Subject,
		"task", t.taskID, "step", step, "attempt", attempt,
		"detail", detail)
	message := "execbridge: generation refused: " + reason
	if toClient && detail != "" {
		message = "execbridge: generation refused: " + detail
	}
	return contracts.NewReasoned(code, reason, message)
}

func (g *gatewayModel) route(ctx context.Context, t turnInfo, req *core.ModelRequest) (string, string, error) {
	if g.taskLabels != nil {
		if t.contextID == "" {
			return "", "", g.refuse(ctx, "grant_context", ReasonGrantBindingMissing,
				contracts.ErrForbidden, t, req, "no verified grant context on this attempt")
		}
		if authority, ok := g.taskLabels.(TaskGrantAuthority); ok {
			scope := t.labelScope
			if scope.Tenant == "" {
				scope = t.scope
			}
			if t.grantTask == "" {
				return "", "", g.refuse(ctx, "grant_task", ReasonGrantBindingMissing,
					contracts.ErrForbidden, t, req, "no verified grant task on this attempt")
			}
			if err := authority.AuthorizeModelTask(ctx, scope, t.grantTask); err != nil {
				return "", "", err
			}
		}
	}
	if g.router == nil {
		// No provider table means there is exactly one client and no routing
		// decision to make, so there is nothing for the label ceiling to protect:
		// the destination cannot be widened because there is no second destination.
		// Refusing here instead bricked the DEFAULT composition -- the execution
		// role mounts the journal unconditionally, so every Tier-1 human turn died
		// with an unclassified 500 before a generation row existed (DEF-DOTE2E-2).
		// The mount says this out loud, and the grant authorization above still ran.
		return g.modelName, "", nil
	}
	set := labels.Set{}
	journalState := "not_mounted"
	if g.labels != nil {
		got, err := g.labels.LabelsFor(ctx, t.scope.Tenant, t.scope.Deployment)
		if err != nil {
			// Fail closed. A classification this gateway cannot read is one it
			// cannot vouch for, and routing it as unlabelled is precisely how
			// tainted data would leave a restricted tenant.
			return "", "", g.refuse(ctx, "deployment_label_record", ReasonDeploymentLabelRecordUnreadable,
				contracts.ErrUnavailable, t, req, err.Error())
		}
		set = got
	}
	if g.taskLabels != nil && t.contextID != "" {
		labelScope := t.labelScope
		if labelScope.Tenant == "" {
			labelScope = t.scope
		}
		journal, jerr := g.taskLabels.LabelsForContext(ctx, labelScope, t.contextID)
		if jerr != nil {
			if errors.Is(jerr, ErrTaskLabelJournalAbsent) {
				// The task journal is mandatory once production routing is mounted.
				return "", "", g.refuse(ctx, "task_label_journal", ReasonTaskLabelJournalAbsent,
					contracts.ErrConflict, t, req, jerr.Error())
			} else {
				// Fail closed, exactly as for the deployment record: labels this
				// gateway cannot read are labels it cannot vouch for.
				return "", "", g.refuse(ctx, "task_label_journal", ReasonTaskLabelJournalUnreadable,
					contracts.ErrUnavailable, t, req, jerr.Error())
			}
		} else {
			set = labels.Merge(set, journal)
			journalState = "applied"
		}
	}
	route, err := g.router.Resolve(t.scope, routing.Request{Class: g.class, Model: g.modelName, Labels: set})
	if err != nil {
		return "", "", fmt.Errorf("execbridge: %w", err)
	}
	// The copy the router chose must be reachable FROM HERE, physically, before
	// anything is booked. Resolving it here means a misconfiguration refuses the
	// generation instead of dialling the default upstream and recording a charge
	// against a copy that never saw the request.
	if g.clients != nil {
		if _, cerr := g.clients.Resolve(ctx, route.Provider); cerr != nil {
			// The copy's own reason travels in the text (which copy, which
			// secret_ref -- never a value); the contract code is what stops the
			// transport from flattening a configuration fact into a bare 500.
			//
			// A refused configuration is a permanent fact about this deployment, so
			// it keeps its own reason class and the conflict code: an operator
			// scripts on the branch, and a client is not told to retry something that
			// can never succeed. An unreadable secret stays UNAVAILABLE -- that one
			// may heal, and pretending otherwise would change a dependency answer.
			if reason := contracts.ReasonOf(cerr); model.IsBaseURLConfigReason(reason) {
				return "", "", g.refuseWithDetail(ctx, "provider_copy_config", reason,
					contracts.ErrConflict, t, req, cerr.Error())
			}
			return "", "", g.refuseWithDetail(ctx, "provider_copy", ReasonProviderCopyUnreachable,
				contracts.ErrUnavailable, t, req, cerr.Error())
		}
	}
	// Content-free by construction: rule, provider, model, the operator's own
	// classification terms, and the ids an operator needs to find the row. No
	// prompt, no messages, no tool arguments (PDF :347/:277).
	slog.Info("execbridge: generation routed",
		"rule", route.RuleName, "provider", route.Provider, "model", route.Model,
		"class", route.Class, "tenant", t.scope.Tenant, "task", t.taskID,
		"step", req.Step, "attempt", req.Attempt,
		"labels_source", "deployment_record", "deployment", t.scope.Deployment,
		"label_taints", strings.Join(set.Taints, ","),
		"label_subjects", len(set.Subjects), "label_workspaces", len(set.Workspaces),
		"task_label_journal", journalState,
		// The destination actually dialled, hashed: the line that lets an auditor
		// prove two different copies produced two different sockets (ARCH-02).
		"dial_host_sha256", g.dialHash(route.Provider))
	return route.Model, route.Provider, nil
}

// stepKey is the generation idempotency key for one attempt of one turn step.
// Re-running the SAME attempt of the same task step replays the stored
// generation instead of spending again — that is what makes crash recovery
// safe. It is deliberately not a retry key: a blank model turn is a finished,
// sealed attempt, and asking for its key again returns the sealed conflict
// rather than calling the provider. A physical retry therefore carries a
// non-zero attempt and gets its own key, while attempt 0 keeps the historical
// key so already-persisted generations stay replayable.
func stepKey(taskID string, step, attempt int) string {
	if attempt <= 0 {
		return "execbridge:" + taskID + ":step:" + itoa(step)
	}
	return "execbridge:" + taskID + ":step:" + itoa(step) + ":r" + itoa(attempt)
}

// gatewayMessages lowers the core conversation onto the inference boundary.
// Since the tool-wire contract (T1) the boundary is NOT text-only: assistant
// tool-call turns and tool-result turns travel in the provider's own shape
// (role "tool" + tool_call_id, role "assistant" + tool_calls). The folding into
// user prose is kept ONLY for legacy rows without a call id -- silently
// answering a call the provider never issued is how a conversation derails.
func gatewayMessages(msgs []core.ChatMessage) []model.Message {
	out := make([]model.Message, 0, len(msgs))
	for _, m := range msgs {
		text := contentText(m.Content)
		switch m.Role {
		case "assistant":
			calls, _ := decodeWireToolCalls(m.ToolCalls)
			if strings.TrimSpace(text) == "" && len(calls) == 0 {
				continue
			}
			out = append(out, model.Message{Role: "assistant", Content: text, ToolCalls: calls})
		case "tool":
			if strings.TrimSpace(text) == "" && m.ToolCallID == "" {
				continue
			}
			if m.ToolCallID == "" {
				out = append(out, model.Message{Role: "user", Content: "tool result: " + text})
			} else {
				out = append(out, model.Message{Role: "tool", Content: text, ToolCallID: m.ToolCallID})
			}
		case "system":
			if strings.TrimSpace(text) == "" {
				continue
			}
			out = append(out, model.Message{Role: "system", Content: text})
		default:
			if strings.TrimSpace(text) == "" {
				continue
			}
			out = append(out, model.Message{Role: "user", Content: text})
		}
	}
	if len(out) == 0 {
		out = append(out, model.Message{Role: "user", Content: "(no text input)"})
	}
	// The provider accepts system messages only at the beginning of the
	// conversation (measured refusal: HTTP 400 "System message must be at the
	// beginning."), so NO calling path -- core loop, this gateway seam or a
	// remote model leg -- may put one anywhere else on the wire (DEF-DOTE2E-11).
	// The hoist is positional only: nothing is dropped, folded or reordered
	// beyond pulling system entries into the leading block.
	out = systemFirst(out)
	return out
}

// systemFirst stably moves system-role messages into the leading block,
// preserving the relative order of everything else (tool-call turns and their
// results stay paired and in sequence).
func systemFirst(msgs []model.Message) []model.Message {
	leading := make([]model.Message, 0, len(msgs))
	rest := make([]model.Message, 0, len(msgs))
	for _, m := range msgs {
		if m.Role == "system" {
			leading = append(leading, m)
		} else {
			rest = append(rest, m)
		}
	}
	return append(leading, rest...)
}

// decodeWireToolCalls reads the core conversation's tool_calls payload (the
// core.ToolCall list the runtime recorded on the assistant turn) back into the
// provider wire shape. Unparsable or nameless entries are dropped: replaying a
// malformed call to the provider would poison the conversation, while dropping
// it degrades to "the model asked, got no result, asks again".
func decodeWireToolCalls(raw json.RawMessage) ([]model.WireToolCall, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	var calls []struct {
		ID   string          `json:"id"`
		Name string          `json:"name"`
		Args json.RawMessage `json:"args"`
	}
	if err := json.Unmarshal(raw, &calls); err != nil {
		return nil, false
	}
	var out []model.WireToolCall
	for _, c := range calls {
		if strings.TrimSpace(c.Name) == "" {
			continue
		}
		args := string(c.Args)
		if args == "" || args == "null" {
			args = "{}"
		}
		id := c.ID
		if id == "" {
			id = "call-" + itoa(len(out)+1)
		}
		out = append(out, model.WireToolCall{ID: id, Type: "function",
			Function: model.WireFunctionCall{Name: c.Name, Arguments: args}})
	}
	return out, len(out) > 0
}

// gatewayTools lowers the runtime's tool registry onto the request. Definitions
// carry no credential: a ToolDef is a name, a description and a schema, and the
// execution-side guardian is what actually authorizes a call.
func gatewayTools(defs []core.ToolDef) []model.ToolDef {
	if len(defs) == 0 {
		return nil
	}
	out := make([]model.ToolDef, 0, len(defs))
	for _, d := range defs {
		out = append(out, model.ToolDef{Name: d.Name, Description: d.Description, Parameters: d.Parameters})
	}
	return out
}

// decodeGenerationOutput turns one durable generation's output back into the
// runtime's response shape. Two shapes are honest history:
//   - plain text (every text-only generation, before and after T1) arrives as
//     the accumulated stream and is re-encoded as a JSON string exactly as the
//     runtime has always consumed it;
//   - a tool-call finish persists the provider envelope
//     {"text":...,"tool_calls":[...]}, which decodes to content plus the calls
//     the runtime must dispatch. An envelope with neither text nor calls is a
//     corrupt record, not an empty answer, and says so.
func decodeGenerationOutput(output string) (json.RawMessage, []core.ToolCall, error) {
	var env struct {
		Text      string               `json:"text"`
		ToolCalls []model.WireToolCall `json:"tool_calls"`
	}
	if err := json.Unmarshal([]byte(output), &env); err == nil && env.ToolCalls != nil {
		if env.Text == "" && len(env.ToolCalls) == 0 {
			return nil, nil, errors.New("execbridge: generation envelope is empty")
		}
		content, err := json.Marshal(env.Text)
		if err != nil {
			return nil, nil, err
		}
		var calls []core.ToolCall
		for _, tc := range env.ToolCalls {
			if strings.TrimSpace(tc.Function.Name) == "" {
				continue
			}
			args := json.RawMessage(tc.Function.Arguments)
			if !json.Valid(args) || len(args) == 0 {
				args = json.RawMessage(`{}`)
			}
			id := tc.ID
			if id == "" {
				id = "call-" + itoa(len(calls)+1)
			}
			calls = append(calls, core.ToolCall{ID: id, Name: tc.Function.Name, Args: args})
		}
		if len(calls) == 0 {
			return nil, nil, errors.New("execbridge: tool-call envelope carried no callable request")
		}
		return content, calls, nil
	}
	content, err := json.Marshal(output)
	if err != nil {
		return nil, nil, err
	}
	return content, nil, nil
}
