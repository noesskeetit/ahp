package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// authorizeRuntime is shared with one-shot invocation. Readiness is admission
// for NEW tasks; it must not prevent observing/continuing an existing task on
// a draining version. Image and grant identity are checked in either case.
func (d *A2AInvokeDispatcher) authorizeRuntime(ctx context.Context, req InvokeRequest, needReady bool) (RuntimeBinding, string, error) {
	if d.store == nil || d.issuer == nil {
		return RuntimeBinding{}, "", contracts.NewError(contracts.ErrUnavailable, "immutable runtime routing and target issuer are required")
	}
	b, err := d.store.ResolveRuntime(ctx, req.Tenant, req.Environment, req.AgentID, req.VersionDigest)
	if err != nil {
		return b, "", err
	}
	v, err := d.store.GetVersion(ctx, req.VersionID)
	if err != nil {
		return b, "", err
	}
	if v.AgentID != req.AgentID || v.Digest != req.VersionDigest {
		return b, "", contracts.NewError(contracts.ErrConflict, "invoke version identity mismatch")
	}
	image := ""
	for _, component := range v.Manifest.Components {
		if component.A2A {
			image = component.Image
		}
	}
	if image == "" || image != b.Image {
		return b, "", contracts.NewError(contracts.ErrUnavailable, "selected version does not bind this immutable runtime image")
	}
	if needReady {
		if err = d.ready(ctx, b); err != nil {
			return b, "", err
		}
	}
	token, err := d.issuer.IssueRuntimeToken(ctx, req, b)
	if err != nil {
		return b, "", err
	}
	if token == "" {
		return b, "", contracts.NewError(contracts.ErrForbidden, "target credential missing")
	}
	return b, token, nil
}

func (d *A2AInvokeDispatcher) SendAgentMessage(ctx context.Context, req InvokeRequest, message json.RawMessage) (json.RawMessage, error) {
	var identity struct {
		ID      string `json:"messageId"`
		Task    string `json:"taskId"`
		Context string `json:"contextId"`
	}
	if json.Unmarshal(message, &identity) != nil || identity.ID == "" || identity.Context == "" || identity.Context != req.ConversationContext || req.Observe != (identity.Task != "") {
		return nil, contracts.NewError(contracts.ErrInvalid, "agent message requires a stable ID and its authorized conversation/task")
	}
	b, token, err := d.authorizeRuntime(ctx, req, identity.Task == "")
	if err != nil {
		return nil, err
	}
	params := map[string]any{"message": message, "configuration": map[string]any{"returnImmediately": true}, "metadata": map[string]any{"ahp": map[string]any{"task_id": req.TaskID, "tenant": req.Tenant, "agent_id": req.AgentID, "version_id": req.VersionID, "version_digest": req.VersionDigest, "installation_id": req.InstallationID, "config_revision": req.ConfigRevision, "config": req.Config}}}
	return d.agentRPC(ctx, b, token, "SendMessage", params, identity.Task, req.ConversationContext)
}

func (d *A2AInvokeDispatcher) AgentTask(ctx context.Context, req InvokeRequest, method, remote string) (json.RawMessage, error) {
	if method != "GetTask" && method != "CancelTask" || remote == "" || len(remote) > 512 || !req.Observe {
		return nil, contracts.NewError(contracts.ErrInvalid, "unsupported agent task control")
	}
	b, token, err := d.authorizeRuntime(ctx, req, false)
	if err != nil {
		return nil, err
	}
	return d.agentRPC(ctx, b, token, method, map[string]any{"id": remote}, remote, req.ConversationContext)
}

func (d *A2AInvokeDispatcher) agentRPC(ctx context.Context, b RuntimeBinding, token, method string, params any, expectedTask, expectedContext string) (json.RawMessage, error) {
	id := contracts.NewID()
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	if err != nil || len(data) > registryMaxBody {
		return nil, contracts.NewError(contracts.ErrInvalid, "agent request exceeds its body bound")
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, b.Endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("A2A-Version", "1.0")
	r.Header.Set("Authorization", "Bearer "+token)
	response, err := d.client.Do(r)
	if err != nil {
		return nil, contracts.NewReasoned(contracts.ErrUnavailable, "agent_reply_unknown", "agent reply was not confirmed; retain the same message key")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		code := contracts.ErrUnavailable
		switch response.StatusCode {
		case 401, 403:
			code = contracts.ErrForbidden
		case 404:
			code = contracts.ErrNotFound
		case 409:
			code = contracts.ErrConflict
		}
		return nil, contracts.NewReasoned(code, "agent_http_refusal", fmt.Sprintf("agent runtime answered HTTP %d", response.StatusCode))
	}
	data, err = io.ReadAll(io.LimitReader(response.Body, registryMaxBody+1))
	if err != nil || len(data) > registryMaxBody {
		return nil, contracts.NewReasoned(contracts.ErrUnavailable, "agent_reply_unknown", "agent response was incomplete or exceeded its bound")
	}
	var reply struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      string          `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(data, &reply) != nil || reply.JSONRPC != "2.0" || reply.ID != id {
		return nil, contracts.NewReasoned(contracts.ErrUnavailable, "agent_reply_unknown", "agent response did not match its RPC")
	}
	if reply.Error != nil {
		// These are the pinned a2a-go/v2 JSON-RPC sentinel codes. Preserve the
		// refusal class without forwarding arbitrary remote diagnostic content.
		code := contracts.ErrUnavailable
		reason := "agent_rpc_error"
		switch reply.Error.Code {
		case -32001:
			code = contracts.ErrNotFound
		case -32002:
			code = contracts.ErrConflict
		case -31401, -31403:
			code = contracts.ErrForbidden
		case -32600, -32602:
			code = contracts.ErrInvalid
		case -32601, -32004:
			code = contracts.ErrConflict
			reason = "agent_operation_unsupported"
		}
		return nil, contracts.NewReasoned(code, reason, fmt.Sprintf("agent runtime returned A2A error %d", reply.Error.Code))
	}
	var task struct {
		ID      string          `json:"id"`
		Context string          `json:"contextId"`
		Task    json.RawMessage `json:"task"`
	}
	if json.Unmarshal(reply.Result, &task) != nil {
		return nil, contracts.NewReasoned(contracts.ErrUnavailable, "agent_reply_unknown", "agent task response was not valid JSON")
	}
	if task.ID == "" && len(task.Task) > 0 {
		reply.Result = task.Task
		if json.Unmarshal(reply.Result, &task) != nil {
			return nil, contracts.NewReasoned(contracts.ErrUnavailable, "agent_reply_unknown", "agent task wrapper was invalid")
		}
	}
	if task.ID == "" || expectedTask != "" && task.ID != expectedTask || expectedContext != "" && task.Context != "" && task.Context != expectedContext {
		return nil, contracts.NewError(contracts.ErrConflict, "agent runtime returned another task or conversation")
	}
	return reply.Result, nil
}
