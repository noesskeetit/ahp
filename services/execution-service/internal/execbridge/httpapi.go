package execbridge

import (
	"net/http"
	"strings"

	"fmt"
	"log/slog"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// ModelAPI exposes the same durable, routed model service to custom runtimes.
// Submit executes synchronously; retrying its key reads the durable outcome.
// GetModel can observe streaming output while Submit is in flight.
func (b *Bridge) ModelAPI(verifier *auth.Verifier, authorize transport.Authorize) http.Handler {
	return transport.Task(verifier, "ahp", "model", authorize)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { b.serveModelAuthorized(w, r, authorize) }))
}

func (b *Bridge) serveModel(w http.ResponseWriter, r *http.Request) {
	b.serveModelAuthorized(w, r, nil)
}

func (b *Bridge) serveModelAuthorized(w http.ResponseWriter, r *http.Request, authorize transport.Authorize) {
	p, ok := transport.FromContext(r.Context())
	if !ok || p.Claims.Kind != auth.KindTask || p.Claims.Task == "" || p.Claims.Context == "" {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "model: a verified task and context are required"))
		return
	}
	op := strings.TrimPrefix(r.URL.Path, "/v1/model/")
	if r.Method != http.MethodPost {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "model: POST required"))
		return
	}
	if op == "CopyFile" || op == "GetFile" || op == "DeleteFile" {
		b.serveProviderFile(w, r, op)
		return
	}
	if op == "SubmitModel" || op == "StreamModel" {
		var in struct {
			Key       string          `json:"key"`
			After     int64           `json:"after,omitempty"`
			Model     string          `json:"model,omitempty"`
			Messages  []model.Message `json:"messages"`
			Tools     []model.ToolDef `json:"tools,omitempty"`
			MaxTokens int             `json:"max_tokens,omitempty"`
		}
		if err := transport.ReadJSON(w, r, &in, 1<<20); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		if strings.TrimSpace(in.Key) == "" || len(in.Key) > 256 || len(in.Messages) == 0 || in.MaxTokens < 0 || in.After < 0 {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "model: key, messages and bounded max_tokens are required"))
			return
		}
		// Over the completion cap is its own refusal with its own reason. The
		// combined bound message hid which number the caller broke, and the one
		// number that makes the mismatch fixable -- the cap -- never reached the
		// caller or the journal (DEF-DOTE2E-9). The refusal itself stays: this is
		// governance, and a silent clamp is forbidden.
		if in.MaxTokens > b.maxTokens {
			slog.Error("execbridge: generation refused before admission",
				"check", "completion_cap", "reason", ReasonCompletionCapExceeded,
				"code", contracts.ErrInvalid, "tenant", p.Scope.Tenant, "deployment", p.Scope.Deployment,
				"task", p.Claims.Task, "asked", in.MaxTokens, "cap", b.maxTokens)
			transport.WriteError(w, r, contracts.NewReasoned(contracts.ErrInvalid, ReasonCompletionCapExceeded,
				fmt.Sprintf("model: max_tokens %d exceeds this deployment's completion cap %d (AHP_EXEC_MAX_TOKENS)",
					in.MaxTokens, b.maxTokens)))
			return
		}
		// This deployment advertises one logical route. Provider names, credentials,
		// budget ownership and security classification are never caller-selectable.
		if in.Model != "" && in.Model != b.modelName {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "model: requested route is not configured"))
			return
		}
		gm := &gatewayModel{modelName: b.modelName, router: b.router, labels: b.deploymentLabels, clients: b.providerClients, taskLabels: b.taskLabels, class: b.executionClass}
		name, provider, err := gm.route(r.Context(), turnInfo{scope: p.Scope, taskID: p.Claims.Task, contextID: p.Claims.Context, labelScope: p.Scope, grantTask: p.Claims.Task}, &core.ModelRequest{})
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		budget, err := b.TaskBudget(r.Context(), p.Scope, p.Claims.Task)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		tokens := in.MaxTokens
		if tokens == 0 {
			tokens = b.maxTokens
		}
		request := model.ChatRequest{Model: name, Messages: in.Messages, Tools: in.Tools, MaxTokens: tokens}
		reserve, err := tokenReserve(request, b.stepReserve)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		key := contracts.OperationKey(p.Scope, p.Claims.Task, "model.http", in.Key)
		gen, err := b.svc.Submit(r.Context(), model.SubmitArgs{Scope: p.Scope, Task: p.Claims.Task, Key: key, Model: name, Provider: provider,
			Request: request, BudgetID: budget.ID, ReserveKey: key + ":reserve", ReserveAmount: reserve})
		if err == nil && op == "StreamModel" {
			b.streamModel(w, r, gen, in.After, authorize)
			return
		}
		if err == nil {
			gen, err = b.svc.Execute(r.Context(), p.Scope, gen.ID)
		}
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		gen.Request = nil
		transport.WriteJSON(w, http.StatusOK, gen)
		return
	}
	if op == "Reserve" {
		var in struct {
			Key    string `json:"key"`
			Amount int64  `json:"amount"`
		}
		if err := transport.ReadJSON(w, r, &in, 1<<20); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		if in.Key == "" || len(in.Key) > 256 || in.Amount <= 0 || in.Amount > b.taskTokenLimit {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "model: bounded reservation required"))
			return
		}
		budget, err := b.TaskBudget(r.Context(), p.Scope, p.Claims.Task)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		hold, err := b.budgets.Reserve(r.Context(), p.Scope, budget.ID, contracts.OperationKey(p.Scope, p.Claims.Task, "model.reserve", in.Key), in.Amount)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		transport.WriteJSON(w, http.StatusOK, hold)
		return
	}
	if op != "GetModel" && op != "CancelModel" {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "model: unknown operation"))
		return
	}
	var in struct {
		ID string `json:"id"`
	}
	if err := transport.ReadJSON(w, r, &in, 1<<20); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	gen, err := b.gens.Get(r.Context(), p.Scope, in.ID)
	if err == nil && gen.Task != p.Claims.Task {
		err = contracts.NewError(contracts.ErrNotFound, "model: generation not found")
	}
	if err == nil && op == "CancelModel" {
		gen, err = b.svc.Cancel(r.Context(), p.Scope, in.ID)
	}
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	gen.Request = nil
	transport.WriteJSON(w, http.StatusOK, gen)
}
