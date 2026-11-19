package registry

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// AgentTurnTransport is the gateway side of the Coordinator's control API.
// Registry supplies the selected artifact and target grant, never the caller.
// Remote task IDs are looked up from a durable link, not accepted from a body.
type AgentTurnTransport interface {
	SendAgentMessage(context.Context, InvokeRequest, json.RawMessage) (json.RawMessage, error)
	AgentTask(context.Context, InvokeRequest, string, string) (json.RawMessage, error)
}

func (h *HTTPHandler) agentTurnControl(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	principal, ok := transport.FromContext(r.Context())
	if !ok || principal.Claims.Kind != auth.KindTask || principal.Claims.Task == "" || principal.Claims.Context == "" {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "agent turn control requires an authenticated run task and conversation"))
		return
	}
	var in struct {
		InstallationID string          `json:"installation_id"`
		Input          json.RawMessage `json:"input"`
		Continue       bool            `json:"continue"`
		Key            string          `json:"key"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	op := strings.TrimPrefix(r.URL.Path, "/v1/registry/")
	if op != "SendAgentTurn" && op != "ReadAgentTurn" && op != "CancelAgentTurn" {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "unknown agent turn control"))
		return
	}
	rpc, ok := h.dispatcher.(AgentTurnTransport)
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrUnavailable, "agent conversation transport is not configured"))
		return
	}
	var parts []json.RawMessage
	if op == "SendAgentTurn" {
		if in.Continue && strings.TrimSpace(in.Key) == "" || !in.Continue && in.Key != "" {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "only a continuation takes an explicit durable message key"))
			return
		}
		parts, err = agentInputParts(in.Input)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
	} else if in.Continue || in.Key != "" || len(in.Input) > 0 {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "read/cancel takes only the installation"))
		return
	}
	pin := contracts.OperationKey(scope, principal.Claims.Task, "registry.agent-turn", principal.Claims.Context)
	if op == "SendAgentTurn" && !in.Continue {
		if _, err = h.store.PinForSubject(r.Context(), scope, in.InstallationID, pin); err != nil {
			transport.WriteError(w, r, err)
			return
		}
	}
	var link agentTaskLink
	if op != "SendAgentTurn" || in.Continue {
		link, err = h.store.agentTask(r.Context(), scope, pin)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
	}
	config, err := h.store.agentTurnSnapshot(r.Context(), scope, in.InstallationID, pin)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	grant := "agent-" + pin
	if link.GrantTask != "" && link.GrantTask != grant {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrConflict, "agent grant mapping differs from the authenticated run"))
		return
	}
	conversation := contracts.OperationKey(scope, principal.Claims.Context, "registry.agent-context", in.InstallationID)
	req := InvokeRequest{Scope: scope, Tenant: scope.Tenant, Deployment: scope.Deployment, TaskID: grant, AgentID: config.AgentID, VersionID: config.VersionID, VersionDigest: config.Digest, InstallationID: in.InstallationID, Environment: config.Environment, Capabilities: config.Capabilities, Config: config.Config, ConfigRevision: config.ConfigRevision, Token: bearerOf(r), Observe: op != "SendAgentTurn" || in.Continue, ConversationContext: conversation}
	var task json.RawMessage
	if op == "SendAgentTurn" {
		key := "start"
		if in.Continue {
			key = in.Key
		}
		message := map[string]any{"role": "ROLE_USER", "messageId": contracts.OperationKey(scope, principal.Claims.Task, "registry.agent-message", key), "contextId": conversation, "parts": parts}
		if in.Continue {
			message["taskId"] = link.RuntimeTask
		}
		data, _ := json.Marshal(message)
		task, err = rpc.SendAgentMessage(r.Context(), req, data)
	} else {
		method := "GetTask"
		if op == "CancelAgentTurn" {
			method = "CancelTask"
		}
		task, err = rpc.AgentTask(r.Context(), req, method, link.RuntimeTask)
	}
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var result struct {
		ID        string `json:"id"`
		ContextID string `json:"contextId"`
	}
	if json.Unmarshal(task, &result) != nil || result.ID == "" || result.ContextID != "" && result.ContextID != conversation {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrConflict, "agent runtime returned a mismatched task/context"))
		return
	}
	if op == "SendAgentTurn" {
		err = h.store.bindAgentTask(r.Context(), scope, pin, grant, result.ID)
	} else if result.ID != link.RuntimeTask {
		err = contracts.NewError(contracts.ErrConflict, "agent runtime returned another task")
	}
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"task": task, "grant_task_id": grant})
}

func agentInputParts(input json.RawMessage) ([]json.RawMessage, error) {
	var plain string
	if json.Unmarshal(input, &plain) == nil && strings.TrimSpace(plain) != "" {
		p, _ := json.Marshal(map[string]string{"kind": "text", "text": plain})
		return []json.RawMessage{p}, nil
	}
	var body struct {
		Content string            `json:"content"`
		Text    string            `json:"text"`
		Parts   []json.RawMessage `json:"parts"`
	}
	if json.Unmarshal(input, &body) != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "agent input must contain text or message parts")
	}
	if len(body.Parts) > 0 {
		if len(body.Parts) > 64 {
			return nil, contracts.NewError(contracts.ErrInvalid, "too many agent input parts")
		}
		for _, p := range body.Parts {
			var kind struct {
				Kind string `json:"kind"`
			}
			if json.Unmarshal(p, &kind) != nil || kind.Kind != "text" && kind.Kind != "file" && kind.Kind != "data" {
				return nil, contracts.NewError(contracts.ErrInvalid, "unsupported agent input part")
			}
		}
		return body.Parts, nil
	}
	text := body.Content
	if text == "" {
		text = body.Text
	}
	if strings.TrimSpace(text) == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "agent input is empty")
	}
	p, _ := json.Marshal(map[string]string{"kind": "text", "text": text})
	return []json.RawMessage{p}, nil
}
